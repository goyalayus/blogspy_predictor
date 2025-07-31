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
			// Create a logger with a persistent correlation_id for this specific job
			jobLogger := slog.With("correlation_id", fmt.Sprintf("job_%d", currentJob.ID))

			domainJob := domain.URLRecord{
				ID:  currentJob.ID,
				URL: currentJob.Url,
			}
			var err error
			// The logic is now combined, as both tasks do the same initial fetch.
			if jobType == "classification" {
				err = w.processClassificationTask(gCtx, domainJob, jobLogger)
			} else {
				err = w.processCrawlTask(gCtx, domainJob, jobLogger)
			}
			if err != nil {
				// Errors from the task itself (e.g. context canceled) are logged here.
				// Business logic errors (e.g. fetch failed) are handled inside the task by enqueuing a failed status.
				jobLogger.Error("Task processing failed unexpectedly", "error", err)
			}
			return nil // Always return nil to not cancel the whole group
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
			"details", slog.GroupValue(
				slog.Any("input", map[string]interface{}{"url": job.URL}),
				slog.Any("output", map[string]interface{}{"error": err.Error()}),
			),
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
				"language":         content.Language, // MODIFIED: Added language to log
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

	// --- NEW: Language check before calling the expensive ML API ---
	if content.Language != "eng" && content.Language != "" {
		errMsg := fmt.Sprintf("Non-English content detected (lang: %s)", content.Language)
		jobLogger.Info("Skipping job, content is not in English",
			"details", slog.GroupValue(
				slog.Any("input", map[string]interface{}{"url": job.URL}),
				slog.Any("output", map[string]interface{}{"reason": errMsg}),
			),
		)
		w.storage.EnqueueStatusUpdate(domain.StatusUpdateResult{ID: job.ID, Status: domain.Irrelevant, ErrorMsg: errMsg})
		return nil
	}
	// --- End of new logic ---

	reqBody := domain.PredictionRequest{URL: content.FinalURL, HTMLContent: content.HTMLContent, TextContent: content.TextContent}
	jsonBody, _ := json.Marshal(reqBody)

	apiCallStart := time.Now()
	httpReq, err := http.NewRequestWithContext(ctx, "POST", w.cfg.MLApiURL+"/predict", bytes.NewBuffer(jsonBody))
	if err != nil {
		// This is a programmer error, not a job failure. Log as an error and return it.
		jobLogger.Error("Failed to create prediction request object", "error", err)
		return fmt.Errorf("failed to create prediction request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := w.httpClient.Do(httpReq)
	apiCallDuration := time.Since(apiCallStart)

	if err != nil {
		jobLogger.Warn("ML API call failed",
			"event", slog.GroupValue(slog.String("name", "ML_API_CALL_COMPLETED"), slog.String("stage", "end"), slog.Float64("duration_ms", float64(apiCallDuration.Microseconds())/1000.0)),
			"details", slog.GroupValue(
				slog.Any("input", map[string]interface{}{"url": content.FinalURL}),
				slog.Any("output", map[string]interface{}{"error": "HTTP client error: " + err.Error()}),
			),
		)
		w.storage.EnqueueStatusUpdate(domain.StatusUpdateResult{ID: job.ID, Status: domain.Failed, ErrorMsg: "Prediction API call failed: " + err.Error()})
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		errMsg := fmt.Sprintf("Prediction API returned non-200 status: %d - %s", resp.StatusCode, string(bodyBytes))
		jobLogger.Warn("ML API call returned non-200 status",
			"event", slog.GroupValue(slog.String("name", "ML_API_CALL_COMPLETED"), slog.String("stage", "end"), slog.Float64("duration_ms", float64(apiCallDuration.Microseconds())/1000.0)),
			"details", slog.GroupValue(
				slog.Any("input", map[string]interface{}{"url": content.FinalURL}),
				slog.Any("output", map[string]interface{}{"http_status": resp.StatusCode, "error": errMsg}),
			),
		)
		w.storage.EnqueueStatusUpdate(domain.StatusUpdateResult{ID: job.ID, Status: domain.Failed, ErrorMsg: errMsg})
		return nil
	}

	var predResp domain.PredictionResponse
	if err := json.NewDecoder(resp.Body).Decode(&predResp); err != nil {
		jobLogger.Warn("Failed to decode ML API response", "error", err)
		w.storage.EnqueueStatusUpdate(domain.StatusUpdateResult{ID: job.ID, Status: domain.Failed, ErrorMsg: "Failed to decode prediction response: " + err.Error()})
		return nil
	}

	jobLogger.Info("ML API call completed",
		"event", slog.GroupValue(slog.String("name", "ML_API_CALL_COMPLETED"), slog.String("stage", "end"), slog.Float64("duration_ms", float64(apiCallDuration.Microseconds())/1000.0)),
		"details", slog.GroupValue(
			slog.Any("input", map[string]interface{}{"url": content.FinalURL}),
			slog.Any("output", map[string]interface{}{"http_status": resp.StatusCode, "is_personal_blog": predResp.IsPersonalBlog}),
		),
	)

	// If it's a blog, we crawl it and mark as completed.
	if predResp.IsPersonalBlog {
		if _, err := w.handleCrawlLogic(ctx, job, content, jobLogger); err != nil {
			jobLogger.Error("Crawl logic failed, but enqueuing as complete anyway", "error", err)
		}
		w.storage.EnqueueContentInsert(domain.ContentInsertResult{
			ID:          job.ID,
			Title:       content.Title,
			Description: content.Description,
			TextContent: content.TextContent,
			Rendering:   domain.SSR,
		})
	} else {
		// If not a blog, it's irrelevant.
		w.storage.EnqueueStatusUpdate(domain.StatusUpdateResult{ID: job.ID, Status: domain.Irrelevant})
	}
	return nil
}

func (w *Worker) processCrawlTask(ctx context.Context, job domain.URLRecord, jobLogger *slog.Logger) error {
	jobLogger.Info("Starting crawl task", "event", slog.GroupValue(
		slog.String("name", "CRAWL_TASK_STARTED"),
		slog.String("stage", "start"),
	))
	
	fetchStart := time.Now()
	content, err := w.crawler.FetchAndParseContent(ctx, job.URL)
	fetchDuration := time.Since(fetchStart)

	if err != nil {
		jobLogger.Warn("Content fetch failed",
			"event", slog.GroupValue(slog.String("name", "CONTENT_FETCH_COMPLETED"), slog.String("stage", "end"), slog.Float64("duration_ms", float64(fetchDuration.Microseconds())/1000.0)),
			"details", slog.GroupValue(
				slog.Any("input", map[string]interface{}{"url": job.URL}),
				slog.Any("output", map[string]interface{}{"error": err.Error()}),
			),
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
				"html_content_len": len(content.HTMLContent),
				"text_content_len": len(content.TextContent),
			}),
		),
	)

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
		ID:          job.ID,
		Title:       content.Title,
		Description: content.Description,
		TextContent: content.TextContent,
		Rendering:   domain.SSR,
	})
	return nil
}

// handleCrawlLogic now accepts the logger and returns the count of links queued, for logging purposes.
func (w *Worker) handleCrawlLogic(ctx context.Context, job domain.URLRecord, content *domain.FetchedContent, jobLogger *slog.Logger) (int, error) {
	logicStart := time.Now()
	
	extractStart := time.Now()
	newLinksRaw := w.crawler.ExtractLinks(content.GoqueryDoc, content.FinalURL, w.cfg.IgnoreExtensions)
	extractDuration := time.Since(extractStart)

	jobLogger.Info("Link extraction completed",
		"event", slog.GroupValue(slog.String("name", "LINK_EXTRACTION_COMPLETED"), slog.String("stage", "end"), slog.Float64("duration_ms", float64(extractDuration.Microseconds())/1000.0)),
		"details", slog.GroupValue(
			slog.Any("input", map[string]interface{}{"final_url": content.FinalURL}),
			slog.Any("output", map[string]interface{}{"raw_link_count": len(newLinksRaw)}),
		),
	)

	if len(newLinksRaw) == 0 {
		return 0, nil
	}

	linksByNetloc := make(map[string][]string)
	var allNetlocsFound []string
	netlocSet := make(map[string]struct{})
	for _, link := range newLinksRaw {
		parsed, err := url.Parse(link)
		if err != nil || parsed.Host == "" {
			continue
		}
		netloc := parsed.Host
		linksByNetloc[netloc] = append(linksByNetloc[netloc], link)
		if _, exists := netlocSet[netloc]; !exists {
			netlocSet[netloc] = struct{}{}
			allNetlocsFound = append(allNetlocsFound, netloc)
		}
	}
	if len(allNetlocsFound) == 0 {
		return 0, nil
	}

	// Perform all DB reads here in the worker, not in a transaction.
	dbReadStart := time.Now()
	netlocCountsRows, err := w.storage.Queries.GetNetlocCounts(ctx, allNetlocsFound)
	if err != nil {
		return 0, fmt.Errorf("crawl-logic: failed to get netloc counts: %w", err)
	}
	netlocCounts := make(map[string]int32)
	for _, row := range netlocCountsRows {
		netlocCounts[row.Netloc] = row.UrlCount
	}

	existingURLsRows, err := w.storage.Queries.GetExistingURLs(ctx, newLinksRaw)
	if err != nil {
		return 0, fmt.Errorf("crawl-logic: failed to check existing urls: %w", err)
	}
	existingURLs := make(map[string]struct{}, len(existingURLsRows))
	for _, urlStr := range existingURLsRows {
		existingURLs[urlStr] = struct{}{}
	}

	domainDecisionsRows, err := w.storage.Queries.GetDomainDecisions(ctx, allNetlocsFound)
	if err != nil {
		return 0, fmt.Errorf("crawl-logic: failed to get domain decisions: %w", err)
	}
	domainDecisions := make(map[string]generated.CrawlStatus)
	for _, row := range domainDecisionsRows {
		domainDecisions[row.Netloc] = row.Status
	}
	dbReadDuration := time.Since(dbReadStart)

	// Now call the pure filtering logic with all the data it needs.
	linksToQueue := w.filterAndPrepareLinks(job, linksByNetloc, netlocCounts, existingURLs, domainDecisions)

	if len(linksToQueue) > 0 {
		w.storage.EnqueueLinks(domain.LinkBatch{SourceURLID: job.ID, NewLinks: linksToQueue})
	}

	logicDuration := time.Since(logicStart)
	jobLogger.Info("Link filtering completed",
		"event", slog.GroupValue(slog.String("name", "LINK_FILTERING_COMPLETED"), slog.String("stage", "end"), slog.Float64("duration_ms", float64(logicDuration.Microseconds())/1000.0)),
		"details", slog.GroupValue(
			slog.Any("input", map[string]interface{}{
				"raw_link_count":           len(newLinksRaw),
				"db_read_duration_ms":      float64(dbReadDuration.Microseconds()) / 1000.0,
				"netloc_count_map_size":    len(netlocCounts),
				"existing_url_map_size":    len(existingURLs),
				"domain_decision_map_size": len(domainDecisions),
			}),
			slog.Any("output", map[string]interface{}{"queued_link_count": len(linksToQueue)}),
		),
	)

	return len(linksToQueue), nil
}

// filterAndPrepareLinks is the refactored, pure-function version of the old crawl logic.
// It takes data and returns a list of links to be queued, performing no I/O itself.
// MODIFIED: Corrected the syntax of the last parameter.
func (w *Worker) filterAndPrepareLinks(
	job domain.URLRecord,
	linksByNetloc map[string][]string,
	netlocCounts map[string]int32,
	existingURLs map[string]struct{},
	domainDecisions map[string]generated.CrawlStatus,
) []domain.NewLink {
	sourceNetloc, err := url.Parse(job.URL)
	if err != nil {
		// Can't use job logger here, but can use global logger.
		slog.Error("Could not parse source job URL", "url", job.URL, "error", err)
		return nil
	}

	var linksToQueue []domain.NewLink
	for netloc, links := range linksByNetloc {
		if int(netlocCounts[netloc]) >= w.cfg.MaxUrlsPerNetloc {
			continue
		}
		for _, link := range links {
			if _, exists := existingURLs[link]; exists {
				continue
			}

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

			linkStatus := domain.PendingClassification
			if netloc == sourceNetloc.Host {
				linkStatus = domain.PendingCrawl
			} else if decision, ok := domainDecisions[netloc]; ok {
				if decision == generated.CrawlStatusIrrelevant {
					// Rejection Path: This domain is known to be bad. IGNORE.
					continue
				} else if decision == generated.CrawlStatusPendingClassification || decision == generated.CrawlStatusClassifying {
					// Ignore Path: A job for this domain is already in the pipeline. IGNORE.
					continue
				} else {
					// Promotion Path: This domain is known to be good (completed, crawling, etc.). Promote to crawl.
					linkStatus = domain.PendingCrawl
				}
			}

			linksToQueue = append(linksToQueue, domain.NewLink{URL: link, Netloc: netloc, Status: linkStatus})
			netlocCounts[netloc]++
			if int(netlocCounts[netloc]) >= w.cfg.MaxUrlsPerNetloc {
				break
			}
		}
	}
	return linksToQueue
}

func getPath(rawURL string) string {
	if parsed, err := url.Parse(rawURL); err == nil {
		return parsed.Path
	}
	return ""
}
