// Package worker
package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
	"worker/packages/config"
	"worker/packages/crawler"
	"worker/packages/db"
	"worker/packages/domain"
	"worker/packages/generated"

	"golang.org/x/sync/errgroup"
)

type Worker struct {
	cfg        config.Config
	storage    *db.Storage
	crawler    *crawler.Crawler
	httpClient *http.Client
}

func New(cfg config.Config, storage *db.Storage, crawler *crawler.Crawler) *Worker {
	return &Worker{
		cfg:        cfg,
		storage:    storage,
		crawler:    crawler,
		httpClient: &http.Client{},
	}
}

func (w *Worker) ProcessJobs(ctx context.Context, jobType string, fromStatus, toStatus generated.CrawlStatus) {
	lockStart := time.Now()
	jobsList, err := w.storage.LockJobs(ctx, fromStatus, toStatus, int32(w.cfg.BatchSize))
	lockDuration := time.Since(lockStart)

	if err != nil {
		slog.Error("Failed to lock and update jobs", "type", jobType, "error", err)
		return
	}

	if len(jobsList) == 0 {
		return
	}

	slog.Info("Locked new job batch",
		"event", slog.GroupValue(
			slog.String("name", "JOB_BATCH_LOCKED"),
			slog.String("stage", "end"),
			slog.Float64("duration_ms", float64(lockDuration.Microseconds())/1000.0),
		),
		"details", slog.GroupValue(
			slog.Any("input", map[string]interface{}{
				"job_type":       jobType,
				"requested_size": w.cfg.BatchSize,
			}),
			slog.Any("output", map[string]interface{}{
				"locked_count": len(jobsList),
			}),
		),
	)

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(w.cfg.MaxWorkers)

	for _, job := range jobsList {
		currentJob := job
		g.Go(func() error {
			jobLogger := slog.With("correlation_id", fmt.Sprintf("job_%d", currentJob.ID))

			domainJob := domain.URLRecord{
				ID:  currentJob.ID,
				URL: currentJob.Url,
			}
			var err error
			if jobType == "classification" {
				err = w.processClassificationTask(gCtx, domainJob, jobLogger)
			} else {
				err = w.processCrawlTask(gCtx, domainJob, jobLogger)
			}
			if err != nil {
				jobLogger.Error("Task processing failed unexpectedly", "error", err)
			}
			return nil
		})
	}
	_ = g.Wait()
	slog.Info("Finished processing batch", "type", jobType, "count", len(jobsList))
}

func (w *Worker) processClassificationTask(ctx context.Context, job domain.URLRecord, jobLogger *slog.Logger) error {
	jobLogger.Info("Starting classification task", "event", slog.GroupValue(
		slog.String("name", "CLASSIFICATION_TASK_STARTED"),
		slog.String("stage", "start"),
	))

	fetchStart := time.Now()
	content, err := w.crawler.FetchAndParseContent(ctx, job.URL)
	fetchDuration := time.Since(fetchStart)

	if err != nil {
		jobLogger.Warn("Content fetch failed",
			"event", slog.GroupValue(slog.String("name", "CONTENT_FETCH_COMPLETED"), slog.String("stage", "end"), slog.Float64("duration_ms", float64(fetchDuration.Microseconds())/1000.0)),
			"details", slog.GroupValue(slog.Any("input", map[string]interface{}{"url": job.URL}), slog.Any("output", map[string]interface{}{"error": err.Error()})),
		)
		w.storage.EnqueueStatusUpdate(domain.StatusUpdateResult{ID: job.ID, Status: domain.Failed, ErrorMsg: err.Error()})
		return nil
	}

	jobLogger.Info("Content fetch completed",
		"event", slog.GroupValue(slog.String("name", "CONTENT_FETCH_COMPLETED"), slog.String("stage", "end"), slog.Float64("duration_ms", float64(fetchDuration.Microseconds())/1000.0)),
		"details", slog.GroupValue(
			slog.Any("input", map[string]interface{}{"url": job.URL}),
			slog.Any("output", map[string]interface{}{
				"final_url":        content.FinalURL,
				"is_html":          !content.IsNonHTML,
				"is_csr":           content.IsCSR,
				"language":         content.Language,
				"html_content_len": len(content.HTMLContent),
				"text_content_len": len(content.TextContent),
			}),
		),
	)

	if content.IsNonHTML {
		w.storage.EnqueueStatusUpdate(domain.StatusUpdateResult{ID: job.ID, Status: domain.Irrelevant, ErrorMsg: "Content-Type was not HTML"})
		return nil
	}
	if content.IsCSR {
		w.storage.EnqueueStatusUpdate(domain.StatusUpdateResult{ID: job.ID, Status: domain.Irrelevant, Rendering: domain.CSR, ErrorMsg: "Detected Client-Side Rendering"})
		return nil
	}

	if content.Language != "eng" && content.Language != "" {
		errMsg := fmt.Sprintf("Non-English content detected (lang: %s)", content.Language)
		w.storage.EnqueueStatusUpdate(domain.StatusUpdateResult{ID: job.ID, Status: domain.Irrelevant, ErrorMsg: errMsg})
		return nil
	}

	reqBody := domain.PredictionRequest{URL: content.FinalURL, HTMLContent: content.HTMLContent, TextContent: content.TextContent}
	jsonBody, _ := json.Marshal(reqBody)

	apiCallStart := time.Now()
	httpReq, err := http.NewRequestWithContext(ctx, "POST", w.cfg.MLApiURL+"/predict", bytes.NewBuffer(jsonBody))
	if err != nil {
		jobLogger.Error("Failed to create prediction request object", "error", err)
		return fmt.Errorf("failed to create prediction request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := w.httpClient.Do(httpReq)
	apiCallDuration := time.Since(apiCallStart)

	if err != nil {
		jobLogger.Warn("ML API call failed", "event", slog.String("name", "ML_API_CALL_COMPLETED"), "duration_ms", float64(apiCallDuration.Microseconds())/1000.0, "error", err)
		w.storage.EnqueueStatusUpdate(domain.StatusUpdateResult{ID: job.ID, Status: domain.Failed, ErrorMsg: "Prediction API call failed: " + err.Error()})
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		errMsg := fmt.Sprintf("Prediction API returned non-200 status: %d - %s", resp.StatusCode, string(bodyBytes))
		w.storage.EnqueueStatusUpdate(domain.StatusUpdateResult{ID: job.ID, Status: domain.Failed, ErrorMsg: errMsg})
		return nil
	}

	var predResp domain.PredictionResponse
	if err := json.NewDecoder(resp.Body).Decode(&predResp); err != nil {
		w.storage.EnqueueStatusUpdate(domain.StatusUpdateResult{ID: job.ID, Status: domain.Failed, ErrorMsg: "Failed to decode prediction response: " + err.Error()})
		return nil
	}

	jobLogger.Info("ML API call completed", "event", slog.String("name", "ML_API_CALL_COMPLETED"), "is_personal_blog", predResp.IsPersonalBlog)

	if predResp.IsPersonalBlog {
		if _, err := w.handleCrawlLogic(ctx, job, content, jobLogger); err != nil {
			jobLogger.Error("Crawl logic failed, but enqueuing as complete anyway", "error", err)
		}
		w.storage.EnqueueContentInsert(domain.ContentInsertResult{
			ID: job.ID, Title: content.Title, Description: content.Description, TextContent: content.TextContent, Rendering: domain.SSR,
		})
	} else {
		w.storage.EnqueueStatusUpdate(domain.StatusUpdateResult{ID: job.ID, Status: domain.Irrelevant})
	}
	return nil
}

func (w *Worker) processCrawlTask(ctx context.Context, job domain.URLRecord, jobLogger *slog.Logger) error {
	jobLogger.Info("Starting crawl task", "event", slog.GroupValue(slog.String("name", "CRAWL_TASK_STARTED"), slog.String("stage", "start")))
	fetchStart := time.Now()
	content, err := w.crawler.FetchAndParseContent(ctx, job.URL)
	fetchDuration := time.Since(fetchStart)

	if err != nil {
		jobLogger.Warn("Content fetch failed", "event", slog.GroupValue(slog.String("name", "CONTENT_FETCH_COMPLETED"), slog.String("stage", "end"), slog.Float64("duration_ms", float64(fetchDuration.Microseconds())/1000.0)), "details", slog.GroupValue(slog.Any("error", err.Error())))
		w.storage.EnqueueStatusUpdate(domain.StatusUpdateResult{ID: job.ID, Status: domain.Failed, ErrorMsg: err.Error()})
		return nil
	}

	jobLogger.Info("Content fetch completed", "event", slog.GroupValue(slog.String("name", "CONTENT_FETCH_COMPLETED"), slog.String("stage", "end"), slog.Float64("duration_ms", float64(fetchDuration.Microseconds())/1000.0)))

	if content.IsNonHTML {
		w.storage.EnqueueContentInsert(domain.ContentInsertResult{ID: job.ID, Rendering: domain.SSR})
		return nil
	}
	if content.IsCSR {
		w.storage.EnqueueContentInsert(domain.ContentInsertResult{ID: job.ID, Rendering: domain.CSR})
		return nil
	}

	if _, err := w.handleCrawlLogic(ctx, job, content, jobLogger); err != nil {
		jobLogger.Error("Crawl logic failed, but enqueuing as complete anyway", "error", err)
	}
	w.storage.EnqueueContentInsert(domain.ContentInsertResult{
		ID: job.ID, Title: content.Title, Description: content.Description, TextContent: content.TextContent, Rendering: domain.SSR,
	})
	return nil
}

// =================================================================================
// MODIFIED SECTION: This logic has been completely rewritten for the new architecture.
// =================================================================================
func (w *Worker) handleCrawlLogic(ctx context.Context, job domain.URLRecord, content *domain.FetchedContent, jobLogger *slog.Logger) (int, error) {
	logicStart := time.Now()

	// Step 1: Extract all raw links from the page.
	newLinksRaw := w.crawler.ExtractLinks(content.GoqueryDoc, content.FinalURL, w.cfg.IgnoreExtensions)
	if len(newLinksRaw) == 0 {
		return 0, nil
	}

	// Step 2: Check against the bloom filter and database for existing URLs.
	// This is still a necessary step to prevent adding duplicates.
	// The bloom filter is used inside processLinkBatches, which is called by the linkWriter.
	// For the purpose of this function, we will rely on the DB check as the final arbiter.
	existingURLsRows, err := w.storage.Queries.GetExistingURLs(ctx, newLinksRaw)
	if err != nil {
		return 0, fmt.Errorf("crawl-logic: failed to check existing urls: %w", err)
	}
	existingURLs := make(map[string]struct{}, len(existingURLsRows))
	for _, urlStr := range existingURLsRows {
		existingURLs[urlStr] = struct{}{}
	}

	// Step 3: Pre-filter the links and group them by netloc.
	// We do this BEFORE calling Redis to avoid unnecessary network calls for links that are invalid
	// or have already been processed.
	candidatesByNetloc := make(map[string][]string)
	for _, link := range newLinksRaw {
		if _, exists := existingURLs[link]; exists {
			continue // Skip already known URLs.
		}

		parsed, err := url.Parse(link)
		if err != nil || parsed.Host == "" {
			continue
		}
		netloc := parsed.Host

		// This is the preserved business logic for restricted TLDs.
		isRestricted := false
		for _, tld := range w.cfg.RestrictedTLDs {
			if strings.HasSuffix(netloc, tld) {
				isRestricted = true
				break
			}
		}
		if isRestricted {
			path := getPath(link)
			isAllowed := false
			for _, prefix := range w.cfg.AllowedPathPrefixes {
				if strings.HasPrefix(path, prefix) || path == "/" || path == "" {
					isAllowed = true
					break
				}
			}
			if !isAllowed {
				continue
			}
		}
		candidatesByNetloc[netloc] = append(candidatesByNetloc[netloc], link)
	}

	if len(candidatesByNetloc) == 0 {
		return 0, nil
	}

	// Step 4: Atomically reserve slots for each netloc using the Redis Lua script.
	// This is done concurrently for maximum performance.
	var wg sync.WaitGroup
	var mu sync.Mutex
	var finalLinksToQueue []domain.NewLink

	for netloc, urls := range candidatesByNetloc {
		wg.Add(1)
		go func(n string, u []string) {
			defer wg.Done()
			// The ReserveNetlocSlots function now handles the atomic check and increment.
			acceptedURLs, err := w.storage.ReserveNetlocSlots(ctx, n, u)
			if err != nil {
				jobLogger.Error("Failed to reserve netloc slots from Redis", "netloc", n, "error", err)
				return
			}
			if len(acceptedURLs) > 0 {
				mu.Lock()
				for _, acceptedURL := range acceptedURLs {
					finalLinksToQueue = append(finalLinksToQueue, domain.NewLink{
						URL:    acceptedURL,
						Netloc: n,
						Status: domain.PendingClassification, // Always default to pending classification.
					})
				}
				mu.Unlock()
			}
		}(netloc, urls)
	}
	wg.Wait()

	// Step 5: Enqueue the batch of links that were successfully reserved.
	if len(finalLinksToQueue) > 0 {
		w.storage.EnqueueLinks(domain.LinkBatch{SourceURLID: job.ID, NewLinks: finalLinksToQueue})
	}

	logicDuration := time.Since(logicStart)
	jobLogger.Info("Link filtering and reservation completed",
		"event", slog.GroupValue(slog.String("name", "LINK_FILTERING_COMPLETED"), slog.String("stage", "end"), slog.Float64("duration_ms", float64(logicDuration.Microseconds())/1000.0)),
		"details", slog.GroupValue(
			slog.Any("input", map[string]interface{}{"raw_link_count": len(newLinksRaw)}),
			slog.Any("output", map[string]interface{}{"queued_link_count": len(finalLinksToQueue)}),
		),
	)

	return len(finalLinksToQueue), nil
}

func getPath(rawURL string) string {
	if parsed, err := url.Parse(rawURL); err == nil {
		return parsed.Path
	}
	return ""
}