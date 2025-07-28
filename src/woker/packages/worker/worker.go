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
	jobsList, err := w.storage.LockJobs(ctx, fromStatus, toStatus, int32(w.cfg.BatchSize))
	if err != nil {
		slog.Error("Failed to lock and update jobs", "type", jobType, "error", err)
		return
	}

	if len(jobsList) == 0 {
		return
	}

	slog.Info("Locked and dispatched jobs", "type", jobType, "count", len(jobsList))

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(w.cfg.MaxWorkers)

	for _, job := range jobsList {
		currentJob := job
		g.Go(func() error {
			domainJob := domain.URLRecord{
				ID:  currentJob.ID,
				URL: currentJob.Url,
			}
			var err error
			// The logic is now combined, as both tasks do the same initial fetch.
			if jobType == "classification" {
				err = w.processClassificationTask(gCtx, domainJob)
			} else {
				err = w.processCrawlTask(gCtx, domainJob)
			}
			if err != nil {
				// Errors from the task itself (e.g. context canceled) are logged here.
				// Business logic errors (e.g. fetch failed) are handled inside the task by enqueuing a failed status.
				slog.Error("Task processing failed unexpectedly", "job_id", currentJob.ID, "url", currentJob.Url, "error", err)
			}
			return nil // Always return nil to not cancel the whole group
		})
	}
	_ = g.Wait()
	slog.Info("Finished processing batch", "type", jobType, "count", len(jobsList))
}

func (w *Worker) processClassificationTask(ctx context.Context, job domain.URLRecord) error {
	content, err := w.crawler.FetchAndParseContent(ctx, job.URL)
	if err != nil {
		w.storage.EnqueueStatusUpdate(domain.StatusUpdateResult{ID: job.ID, Status: domain.Failed, ErrorMsg: err.Error()})
		return nil
	}
	if content.IsNonHTML {
		w.storage.EnqueueStatusUpdate(domain.StatusUpdateResult{ID: job.ID, Status: domain.Irrelevant, ErrorMsg: "Content-Type was not HTML"})
		return nil
	}
	if content.IsCSR {
		w.storage.EnqueueStatusUpdate(domain.StatusUpdateResult{ID: job.ID, Status: domain.Irrelevant, Rendering: domain.CSR, ErrorMsg: "Detected Client-Side Rendering"})
		return nil
	}

	reqBody := domain.PredictionRequest{URL: content.FinalURL, HTMLContent: content.HTMLContent, TextContent: content.TextContent}
	jsonBody, _ := json.Marshal(reqBody)
	httpReq, err := http.NewRequestWithContext(ctx, "POST", w.cfg.MLApiURL+"/predict", bytes.NewBuffer(jsonBody))
	if err != nil {
		return fmt.Errorf("failed to create prediction request: %w", err) // This is a programmer error
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := w.httpClient.Do(httpReq)
	if err != nil {
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

	// If it's a blog, we crawl it and mark as completed.
	if predResp.IsPersonalBlog {
		// Perform the read-heavy part of crawl logic here, before handing off to the pure filter function.
		if err := w.handleCrawlLogic(ctx, job, content); err != nil {
			slog.Error("Crawl logic failed, but enqueuing as complete anyway", "job_id", job.ID, "error", err)
		}
		w.storage.EnqueueContentInsert(domain.ContentInsertResult{
			ID:          job.ID,
			Title:       content.Title,
			Description: content.Description,
			TextContent: content.TextContent,
			Rendering:   domain.SSR,
		})
	} else {
		// If not a blog, it's irrelevant. We still "complete" it by inserting content.
		w.storage.EnqueueStatusUpdate(domain.StatusUpdateResult{ID: job.ID, Status: domain.Irrelevant})
	}
	return nil
}

func (w *Worker) processCrawlTask(ctx context.Context, job domain.URLRecord) error {
	content, err := w.crawler.FetchAndParseContent(ctx, job.URL)
	if err != nil {
		w.storage.EnqueueStatusUpdate(domain.StatusUpdateResult{ID: job.ID, Status: domain.Failed, ErrorMsg: err.Error()})
		return nil
	}

	if content.IsNonHTML {
		// We still mark it as completed, but with an error note. No new links are generated.
		w.storage.EnqueueContentInsert(domain.ContentInsertResult{ID: job.ID, Rendering: domain.SSR})
		return nil
	}
	if content.IsCSR {
		// Mark as completed, but note the CSR rendering. No new links.
		w.storage.EnqueueContentInsert(domain.ContentInsertResult{ID: job.ID, Rendering: domain.CSR})
		return nil
	}

	if err := w.handleCrawlLogic(ctx, job, content); err != nil {
		slog.Error("Crawl logic failed, but enqueuing as complete anyway", "job_id", job.ID, "error", err)
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

// handleCrawlLogic is a new helper that performs the READ parts of crawling and then calls the pure filter function.
func (w *Worker) handleCrawlLogic(ctx context.Context, job domain.URLRecord, content *domain.FetchedContent) error {
	newLinksRaw := w.crawler.ExtractLinks(content.GoqueryDoc, content.FinalURL, w.cfg.IgnoreExtensions)
	if len(newLinksRaw) == 0 {
		return nil
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
		return nil
	}

	// Perform all DB reads here in the worker, not in a transaction.
	netlocCountsRows, err := w.storage.Queries.GetNetlocCounts(ctx, allNetlocsFound)
	if err != nil {
		return fmt.Errorf("crawl-logic: failed to get netloc counts: %w", err)
	}
	netlocCounts := make(map[string]int32)
	for _, row := range netlocCountsRows {
		netlocCounts[row.Netloc] = row.UrlCount
	}

	existingURLsRows, err := w.storage.Queries.GetExistingURLs(ctx, newLinksRaw)
	if err != nil {
		return fmt.Errorf("crawl-logic: failed to check existing urls: %w", err)
	}
	existingURLs := make(map[string]struct{}, len(existingURLsRows))
	for _, urlStr := range existingURLsRows {
		existingURLs[urlStr] = struct{}{}
	}

	domainDecisionsRows, err := w.storage.Queries.GetDomainDecisions(ctx, allNetlocsFound)
	if err != nil {
		return fmt.Errorf("crawl-logic: failed to get domain decisions: %w", err)
	}
	domainDecisions := make(map[string]generated.CrawlStatus)
	for _, row := range domainDecisionsRows {
		domainDecisions[row.Netloc] = row.Status
	}

	// Now call the pure filtering logic with all the data it needs.
	linksToQueue := w.filterAndPrepareLinks(job, linksByNetloc, netlocCounts, existingURLs, domainDecisions)

	if len(linksToQueue) > 0 {
		w.storage.EnqueueLinks(domain.LinkBatch{SourceURLID: job.ID, NewLinks: linksToQueue})
	}
	return nil
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
					linkStatus = domain.Irrelevant
				} else {
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
