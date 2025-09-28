// FILE: src/woker/packages/worker/worker.go

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
	"worker/packages/metrics"

	"golang.org/x/sync/errgroup"
)

type Worker struct {
	cfg        config.Config
	storage    *db.Storage
	crawler    *crawler.Crawler
	httpClient *http.Client
}

func normalizeNetloc(host string) string {
	return strings.TrimPrefix(host, "www.")
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
			slog.Any("input", map[string]any{
				"job_type":       jobType,
				"requested_size": w.cfg.BatchSize,
			}),
			slog.Any("output", map[string]any{
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
	jobStart := time.Now()
	metrics.JobQueueActiveWorkers.WithLabelValues("classification").Inc()
	defer func() {
		metrics.JobQueueActiveWorkers.WithLabelValues("classification").Dec()
		metrics.JobDurationSeconds.WithLabelValues("classification").Observe(time.Since(jobStart).Seconds())
	}()

	parsedURL, err := url.Parse(job.URL)
	if err != nil {
		jobLogger.Warn("Could not parse job URL", "url", job.URL, "error", err)
		w.storage.EnqueueStatusUpdate(domain.StatusUpdateResult{ID: job.ID, Status: domain.Failed, ErrorMsg: "Invalid URL"})
		metrics.JobsProcessedTotal.WithLabelValues("classification", "failure").Inc()
		return nil
	}
	netloc := normalizeNetloc(parsedURL.Host)

	jobLogger.Info("Starting classification task", "netloc", netloc)

	status, err := w.storage.GetNetlocClassificationStatus(ctx, netloc)
	if err != nil {
		jobLogger.Error("Failed to get netloc classification from Redis", "netloc", netloc, "error", err)
		w.storage.EnqueueStatusUpdate(domain.StatusUpdateResult{ID: job.ID, Status: domain.Failed, ErrorMsg: "Redis lookup failed"})
		metrics.JobsProcessedTotal.WithLabelValues("classification", "failure").Inc()
		return nil
	}

	switch {
	case status > 0:
		metrics.NetlocCacheTotal.WithLabelValues("hit").Inc()
		jobLogger.Info("Netloc classification cache HIT", "netloc", netloc, "status", "confirmed_blog")

		content, err := w.crawler.FetchAndParseContent(ctx, job.URL)
		if err != nil {
			jobLogger.Warn("Content fetch failed for known blog domain", "error", err)
			w.storage.EnqueueStatusUpdate(domain.StatusUpdateResult{ID: job.ID, Status: domain.Failed, ErrorMsg: err.Error()})
			metrics.JobsProcessedTotal.WithLabelValues("classification", "failure").Inc()
			return nil
		}

		if content.Language != "eng" && content.Language != "" {
			jobLogger.Info("Content is non-english on a confirmed blog domain, marking as irrelevant", "language", content.Language)
			w.storage.EnqueueStatusUpdate(domain.StatusUpdateResult{ID: job.ID, Status: domain.Irrelevant, ErrorMsg: "Content is non-english"})
			metrics.JobsProcessedTotal.WithLabelValues("classification", "classified_irrelevant").Inc()
			return nil
		}

		w.processContentAndLinks(ctx, job, content, jobLogger, "classification")
		metrics.JobsProcessedTotal.WithLabelValues("classification", "classified_blog").Inc()

	case status < 0:
		metrics.NetlocCacheTotal.WithLabelValues("hit").Inc()
		jobLogger.Info("Netloc classification cache HIT", "netloc", netloc, "status", "confirmed_irrelevant")
		w.storage.EnqueueStatusUpdate(domain.StatusUpdateResult{ID: job.ID, Status: domain.Irrelevant, ErrorMsg: "Domain previously classified as irrelevant"})
		metrics.JobsProcessedTotal.WithLabelValues("classification", "classified_irrelevant").Inc()

	default:
		metrics.NetlocCacheTotal.WithLabelValues("miss").Inc()
		jobLogger.Info("Netloc classification cache MISS", "netloc", netloc)

		homepageURL := fmt.Sprintf("%s://%s", parsedURL.Scheme, netloc)
		jobLogger.Info("Classifying homepage via ML API", "url", homepageURL)

		isBlog, err := w.callMLApi(ctx, homepageURL, jobLogger)
		if err != nil {
			jobLogger.Error("ML API call failed for homepage", "error", err)
			w.storage.EnqueueStatusUpdate(domain.StatusUpdateResult{ID: job.ID, Status: domain.Failed, ErrorMsg: "ML API call failed: " + err.Error()})
			metrics.JobsProcessedTotal.WithLabelValues("classification", "failure").Inc()
			return nil
		}

		if isBlog {
			jobLogger.Info("Homepage classified as BLOG. Caching as positive.", "netloc", netloc)
			w.storage.SetNetlocClassificationStatus(ctx, netloc, 1)

			originalContent, err := w.crawler.FetchAndParseContent(ctx, job.URL)
			if err != nil {
				jobLogger.Warn("Content fetch failed for original URL after positive homepage classification", "error", err)
				w.storage.EnqueueStatusUpdate(domain.StatusUpdateResult{ID: job.ID, Status: domain.Failed, ErrorMsg: err.Error()})
				metrics.JobsProcessedTotal.WithLabelValues("classification", "failure").Inc()
				return nil
			}

			w.processContentAndLinks(ctx, job, originalContent, jobLogger, "classification")
			metrics.JobsProcessedTotal.WithLabelValues("classification", "classified_blog").Inc()

		} else {
			jobLogger.Info("Homepage classified as NOT A BLOG. Caching as negative.", "netloc", netloc)
			w.storage.SetNetlocClassificationStatus(ctx, netloc, -1)
			w.storage.EnqueueStatusUpdate(domain.StatusUpdateResult{ID: job.ID, Status: domain.Irrelevant, ErrorMsg: "Domain classified as irrelevant via homepage"})
			metrics.JobsProcessedTotal.WithLabelValues("classification", "classified_irrelevant").Inc()
		}
	}
	return nil
}
// --- MODIFIED FUNCTION ---
func (w *Worker) callMLApi(ctx context.Context, urlToClassify string, jobLogger *slog.Logger) (bool, error) {
    // The request body is now much simpler.
	reqBody := domain.PredictionRequest{URL: urlToClassify}
	jsonBody, _ := json.Marshal(reqBody)

	apiCallStart := time.Now()
	httpReq, err := http.NewRequestWithContext(ctx, "POST", w.cfg.MLApiURL+"/predict", bytes.NewBuffer(jsonBody))
	if err != nil {
		return false, fmt.Errorf("failed to create prediction request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := w.httpClient.Do(httpReq)
	metrics.DependencyCallDurationSeconds.WithLabelValues("ml_api").Observe(time.Since(apiCallStart).Seconds())

	if err != nil {
		return false, fmt.Errorf("prediction API call failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		// If the Python API returns a 400 (e.g., fetch failed), we treat it as a non-blog.
		if resp.StatusCode == http.StatusBadRequest {
			jobLogger.Warn("ML API returned bad request, likely a fetch failure. Treating as non-blog.", "status_code", resp.StatusCode, "body", string(bodyBytes))
			return false, nil 
		}
		return false, fmt.Errorf("prediction API returned non-200 status: %d - %s", resp.StatusCode, string(bodyBytes))
	}

	var predResp domain.PredictionResponse
	if err := json.NewDecoder(resp.Body).Decode(&predResp); err != nil {
		return false, fmt.Errorf("failed to decode prediction response: %w", err)
	}

	jobLogger.Info("ML API call completed", "url_classified", urlToClassify, "is_personal_blog", predResp.IsPersonalBlog)
	return predResp.IsPersonalBlog, nil
}

func (w *Worker) processCrawlTask(ctx context.Context, job domain.URLRecord, jobLogger *slog.Logger) error {
	jobStart := time.Now()
	metrics.JobQueueActiveWorkers.WithLabelValues("crawling").Inc()
	defer func() {
		metrics.JobQueueActiveWorkers.WithLabelValues("crawling").Dec()
		metrics.JobDurationSeconds.WithLabelValues("crawling").Observe(time.Since(jobStart).Seconds())
	}()

	jobLogger.Info("Starting crawl task")
	content, err := w.crawler.FetchAndParseContent(ctx, job.URL)
	if err != nil {
		jobLogger.Warn("Content fetch failed", "error", err)
		w.storage.EnqueueStatusUpdate(domain.StatusUpdateResult{ID: job.ID, Status: domain.Failed, ErrorMsg: err.Error()})
		metrics.JobsProcessedTotal.WithLabelValues("crawling", "failure").Inc()
		return nil
	}

	w.processContentAndLinks(ctx, job, content, jobLogger, "crawling")

	return nil
}

func (w *Worker) processContentAndLinks(ctx context.Context, job domain.URLRecord, content *domain.FetchedContent, jobLogger *slog.Logger, jobType string) {
	if content.IsNonHTML || content.IsCSR {
		errMsg := "Content is non-HTML or client-side rendered"
		w.storage.EnqueueStatusUpdate(domain.StatusUpdateResult{ID: job.ID, Status: domain.Completed, ErrorMsg: errMsg})
		metrics.JobsProcessedTotal.WithLabelValues(jobType, "completed_unusable_content").Inc()
		return
	}

	isSubstantial, avgWords := w.crawler.AnalyzeParagraphs(content.GoqueryDoc, w.cfg.MinAvgParagraphWords)
	jobLogger.Info("Paragraph analysis complete", "avg_word_count", fmt.Sprintf("%.2f", avgWords), "threshold", w.cfg.MinAvgParagraphWords, "is_substantial", isSubstantial)

	if isSubstantial {
		metrics.ContentStorageDecisionsTotal.WithLabelValues("stored").Inc()
		jobLogger.Info("Content is substantial, enqueuing for storage.")
		w.storage.EnqueueContentInsert(domain.ContentInsertResult{
			ID:          job.ID,
			Title:       content.Title,
			Description: content.Description,
			TextContent: content.TextContent,
			Rendering:   domain.SSR,
		})

		parsedURL, err := url.Parse(job.URL)
    if err == nil {
            // We are processing a substantial page from a blog, so we can now safely
            // increment the count for this domain by exactly 1.
      if err := w.storage.IncrementNetlocCount(ctx, parsedURL.Host, 1); err != nil {
                jobLogger.Error("Failed to increment netloc count after processing substantial content", "netloc", parsedURL.Host, "error", err)
            }
					}
	} else {
		metrics.ContentStorageDecisionsTotal.WithLabelValues("skipped_thin_content").Inc()
		jobLogger.Info("Content is not substantial, skipping storage. Job will be marked completed.")
		errMsg := fmt.Sprintf("Content not substantial (avg words per <p> < %d)", w.cfg.MinAvgParagraphWords)
		w.storage.EnqueueStatusUpdate(domain.StatusUpdateResult{ID: job.ID, Status: domain.Completed, ErrorMsg: errMsg})
	}

	if _, err := w.handleCrawlLogic(ctx, job, content, jobLogger); err != nil {
		jobLogger.Error("Crawl logic failed, but job is already being marked as completed", "error", err)
	}

	// This is the generic success for a crawl job
	if jobType == "crawling" {
		metrics.JobsProcessedTotal.WithLabelValues(jobType, "success").Inc()
	}
}

func (w *Worker) handleCrawlLogic(ctx context.Context, job domain.URLRecord, content *domain.FetchedContent, jobLogger *slog.Logger) (int, error) {
	logicStart := time.Now()

	newLinksRaw := w.crawler.ExtractLinks(content.GoqueryDoc, content.FinalURL, w.cfg.IgnoreExtensions)

	// --- MODIFICATION: Add metric for discovered links ---
	metrics.LinksDiscoveredTotal.WithLabelValues("crawl").Add(float64(len(newLinksRaw)))

	if len(newLinksRaw) == 0 {
		return 0, nil
	}

	candidatesByNetloc := make(map[string][]string)
	for _, link := range newLinksRaw {
		parsed, err := url.Parse(link)
		if err != nil || parsed.Host == "" {
			continue
		}
		netloc := normalizeNetloc(parsed.Host)

		var isBlocked bool
		for _, blockedSuffix := range w.cfg.BlockedHostSuffixes {
			if strings.HasSuffix(netloc, blockedSuffix) {
				isBlocked = true
				break
			}
		}
		if isBlocked {
			jobLogger.Debug("Skipping link from blocked host", "url", link)
			metrics.LinksSkippedTotal.WithLabelValues("blocked_host").Inc()
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
		candidatesByNetloc[netloc] = append(candidatesByNetloc[netloc], link)
	}

	if len(candidatesByNetloc) == 0 {
		return 0, nil
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var finalLinksToQueue []domain.NewLink

	for netloc, urls := range candidatesByNetloc {
		wg.Add(1)
		go func(n string, u []string) {
			defer wg.Done()

			currentCount, err := w.storage.GetNetlocClassificationStatus(ctx, n)
			if err != nil {
				jobLogger.Error("Failed to get netloc count for rate-limiting", "netloc", n, "error", err)
				return
			}
			
			if currentCount < 0 {
				jobLogger.Debug("Netloc is classified as irrelevant, skipping link addition", "netloc", n)
				return
			}

      availableSlots := w.cfg.MaxUrlsPerNetloc - currentCount
			if availableSlots <= 0 {
				jobLogger.Debug("Netloc is full, skipping link addition", "netloc", n, "count", currentCount)
				return
			}

			if len(u) > availableSlots {
				jobLogger.Debug("Trimming link batch to fit available slots", "netloc", n, "original_size", len(u), "trimmed_size", availableSlots)
				u = u[:availableSlots]
			}

			if len(u) > 0 {
				mu.Lock()
				for _, acceptedURL := range u {
					finalLinksToQueue = append(finalLinksToQueue, domain.NewLink{
						URL:    acceptedURL,
						Netloc: n,
						Status: domain.PendingClassification,
					})
				}
				mu.Unlock()
			}
		}(netloc, urls)
	}
	wg.Wait()

	if len(finalLinksToQueue) > 0 {
		w.storage.EnqueueLinks(domain.LinkBatch{SourceURLID: job.ID, NewLinks: finalLinksToQueue})
	}

	logicDuration := time.Since(logicStart)
	jobLogger.Info("Link filtering and reservation completed",
		"event", slog.GroupValue(slog.String("name", "LINK_FILTERING_COMPLETED"), slog.String("stage", "end"), slog.Float64("duration_ms", float64(logicDuration.Microseconds())/1000.0)),
		"details", slog.GroupValue(
			slog.Any("input", map[string]any{"raw_link_count": len(newLinksRaw)}),
			slog.Any("output", map[string]any{"queued_link_count": len(finalLinksToQueue)}),
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
