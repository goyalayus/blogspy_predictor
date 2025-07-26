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

	"github.com/jackc/pgx/v5"
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

func (w *Worker) ProcessJobs(ctx context.Context, jobType string, fromStatus, toStatus domain.CrawlStatus) {
	jobsList, err := w.storage.LockJobs(ctx, fromStatus, toStatus, w.cfg.BatchSize)
	if err != nil {
		slog.Error("Failed to lock jobs", "type", jobType, "error", err)
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
			var err error
			if jobType == "classification" {
				err = w.processClassificationTask(gCtx, currentJob)
			} else {
				err = w.processCrawlTask(gCtx, currentJob)
			}
			if err != nil {
				slog.Error("Task failed", "job_id", currentJob.ID, "url", currentJob.URL, "error", err)
			}
			return nil
		})
	}
	_ = g.Wait()
	slog.Info("Finished processing batch", "type", jobType, "count", len(jobsList))
}

func (w *Worker) processClassificationTask(ctx context.Context, job domain.URLRecord) error {
	return w.storage.WithTransaction(ctx, func(tx pgx.Tx) error {
		content, err := w.crawler.FetchAndParseContent(ctx, job.URL)
		if err != nil {
			return w.storage.UpdateStatus(ctx, tx, job.ID, domain.Failed, err.Error())
		}
		if content.IsNonHTML {
			return w.storage.UpdateStatus(ctx, tx, job.ID, domain.Irrelevant, "Content-Type was not HTML")
		}
		if content.IsCSR {
			return w.storage.UpdateStatusAndRendering(ctx, tx, job.ID, domain.Irrelevant, domain.CSR, "Detected Client-Side Rendering")
		}

		reqBody := domain.PredictionRequest{URL: content.FinalURL, HTMLContent: content.HTMLContent, TextContent: content.TextContent}
		jsonBody, _ := json.Marshal(reqBody)
		httpReq, _ := http.NewRequestWithContext(ctx, "POST", w.cfg.MLApiURL+"/predict", bytes.NewBuffer(jsonBody))
		httpReq.Header.Set("Content-Type", "application/json")

		resp, err := w.httpClient.Do(httpReq)
		if err != nil {
			return w.storage.UpdateStatus(ctx, tx, job.ID, domain.Failed, "Prediction API call failed: "+err.Error())
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(resp.Body)
			errMsg := fmt.Sprintf("Prediction API returned non-200 status: %d - %s", resp.StatusCode, string(bodyBytes))
			return w.storage.UpdateStatus(ctx, tx, job.ID, domain.Failed, errMsg)
		}

		var predResp domain.PredictionResponse
		if err := json.NewDecoder(resp.Body).Decode(&predResp); err != nil {
			return w.storage.UpdateStatus(ctx, tx, job.ID, domain.Failed, "Failed to decode prediction response: "+err.Error())
		}

		if predResp.IsPersonalBlog {
			if err := w.performCrawlLogic(ctx, tx, job, content); err != nil {
				return err
			}
			return w.storage.UpdateURLAsCompleted(ctx, tx, job.ID, content, domain.Completed)
		}
		return w.storage.UpdateURLAsCompleted(ctx, tx, job.ID, content, domain.Irrelevant)
	})
}

func (w *Worker) processCrawlTask(ctx context.Context, job domain.URLRecord) error {
	return w.storage.WithTransaction(ctx, func(tx pgx.Tx) error {
		content, err := w.crawler.FetchAndParseContent(ctx, job.URL)
		if err != nil {
			return w.storage.UpdateStatus(ctx, tx, job.ID, domain.Failed, err.Error())
		}
		if content.IsNonHTML {
			return w.storage.UpdateStatus(ctx, tx, job.ID, domain.Completed, "Content-Type was not HTML")
		}
		if content.IsCSR {
			return w.storage.UpdateStatusAndRendering(ctx, tx, job.ID, domain.Completed, domain.CSR, "Detected CSR during crawl")
		}

		if err := w.performCrawlLogic(ctx, tx, job, content); err != nil {
			return err
		}
		return w.storage.UpdateURLAsCompleted(ctx, tx, job.ID, content, domain.Completed)
	})
}

func (w *Worker) performCrawlLogic(ctx context.Context, tx pgx.Tx, job domain.URLRecord, content *domain.FetchedContent) error {
	newLinks := w.crawler.ExtractLinks(content.GoqueryDoc, content.FinalURL, w.cfg.IgnoreExtensions)
	if len(newLinks) == 0 {
		return nil
	}

	linksByNetloc := make(map[string][]string)
	var allNetlocsFound []string
	netlocSet := make(map[string]struct{})
	for _, link := range newLinks {
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

	netlocCounts, err := w.storage.GetNetlocCounts(ctx, tx, allNetlocsFound)
	if err != nil {
		return fmt.Errorf("crawl-logic: failed to get netloc counts: %w", err)
	}

	existingURLs, err := w.storage.GetExistingURLs(ctx, tx, newLinks)
	if err != nil {
		return fmt.Errorf("crawl-logic: failed to check existing urls: %w", err)
	}

	domainDecisions, err := w.storage.GetDomainDecisions(ctx, tx, allNetlocsFound)
	if err != nil {
		return fmt.Errorf("crawl-logic: failed to get domain decisions: %w", err)
	}

	sourceNetloc, err := url.Parse(job.URL)
	if err != nil {
		return fmt.Errorf("crawl-logic: could not parse source job URL %q: %w", job.URL, err)
	}

	var linksToQueue []domain.NewLink
	for netloc, links := range linksByNetloc {
		if netlocCounts[netloc] >= w.cfg.MaxUrlsPerNetloc {
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
				if decision == domain.Irrelevant {
					linkStatus = domain.Irrelevant
				} else {
					linkStatus = domain.PendingCrawl
				}
			}

			linksToQueue = append(linksToQueue, domain.NewLink{URL: link, Netloc: netloc, Status: linkStatus})
			netlocCounts[netloc]++
			if netlocCounts[netloc] >= w.cfg.MaxUrlsPerNetloc {
				break
			}
		}
	}

	if len(linksToQueue) > 0 {
		w.storage.EnqueueLinks(domain.LinkBatch{SourceURLID: job.ID, NewLinks: linksToQueue})
	}
	return nil
}

func getPath(rawURL string) string {
	if parsed, err := url.Parse(rawURL); err == nil {
		return parsed.Path
	}
	return ""
}
