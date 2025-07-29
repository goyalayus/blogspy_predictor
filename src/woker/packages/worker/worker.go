// Package worker
package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"worker/packages/config"
	"worker/packages/crawler"
	"worker/packages/db"
	"worker/packages/domain"
	"worker/packages/generated"

	"github.com/jackc/pgx/v5"
	"golang.org/x/sync/errgroup"
)

type Worker struct {
	cfg     config.Config
	storage *db.Storage
	crawler *crawler.Crawler
}

func New(cfg config.Config, storage *db.Storage, crawler *crawler.Crawler) *Worker {
	return &Worker{
		cfg:     cfg,
		storage: storage,
		crawler: crawler,
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
			if jobType == "classification" {
				err = w.processClassificationTask(gCtx, domainJob)
			} else {
				err = w.processCrawlTask(gCtx, domainJob)
			}
			if err != nil {
				slog.Error("Task processing failed unexpectedly", "job_id", currentJob.ID, "url", currentJob.Url, "error", err)
			}
			return nil
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
		w.storage.EnqueueStatusUpdate(domain.StatusUpdateResult{
			ID:       job.ID,
			Status:   domain.Irrelevant,
			ErrorMsg: "Content-Type was not HTML",
		})
		return nil
	}
	if content.IsCSR {
		w.storage.EnqueueStatusUpdate(domain.StatusUpdateResult{
			ID:        job.ID,
			Status:    domain.Irrelevant,
			Rendering: domain.CSR,
			ErrorMsg:  "Detected Client-Side Rendering",
		})
		return nil
	}

	payload := map[string]any{
		"url":          content.FinalURL,
		"html_content": content.HTMLContent,
		"text_content": content.TextContent,
	}
	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		slog.Error("Failed to marshal classification payload to JSON", "job_id", job.ID, "error", err)
		w.storage.EnqueueStatusUpdate(domain.StatusUpdateResult{
			ID:       job.ID,
			Status:   domain.Failed,
			ErrorMsg: "JSON marshaling failed",
		})
		return nil
	}

	err = w.storage.WithTransaction(ctx, func(qtx *generated.Queries, tx pgx.Tx) error {
		updateSQL := "UPDATE urls SET status = $1, processed_at = NOW() WHERE id = $2"
		if _, err := tx.Exec(ctx, updateSQL, generated.CrawlStatusClassifying, job.ID); err != nil {
			return err
		}

		_, err := qtx.EnqueueClassificationJob(ctx, generated.EnqueueClassificationJobParams{
			UrlID:   job.ID,
			Payload: jsonPayload,
		})
		return err
	})

	if err != nil {
		slog.Error("Failed to enqueue classification job transaction", "job_id", job.ID, "error", err)
		return err
	}

	slog.Info("Successfully enqueued classification job", "job_id", job.ID, "url", job.URL)
	return nil
}

func (w *Worker) processCrawlTask(ctx context.Context, job domain.URLRecord) error {
	content, err := w.crawler.FetchAndParseContent(ctx, job.URL)
	if err != nil {
		w.storage.EnqueueStatusUpdate(domain.StatusUpdateResult{ID: job.ID, Status: domain.Failed, ErrorMsg: err.Error()})
		return nil
	}

	if content.IsNonHTML {
		w.storage.EnqueueContentInsert(domain.ContentInsertResult{ID: job.ID, Rendering: domain.SSR})
		return nil
	}
	if content.IsCSR {
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

	linksToQueue := w.filterAndPrepareLinks(job, linksByNetloc, netlocCounts, existingURLs, domainDecisions)

	if len(linksToQueue) > 0 {
		w.storage.EnqueueLinks(domain.LinkBatch{SourceURLID: job.ID, NewLinks: linksToQueue})
	}
	return nil
}

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
