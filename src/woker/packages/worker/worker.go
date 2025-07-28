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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
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
				ID: currentJob.ID,
				URL: currentJob.Url,
			}
			var err error
			if jobType == "classification" {
				err = w.processClassificationTask(gCtx, domainJob)
			} else {
				err = w.processCrawlTask(gCtx, domainJob)
			}
			if err != nil {
				slog.Error("Task failed", "job_id", currentJob.ID, "url", currentJob.Url, "error", err)
			}
			return nil
		})
	}
	_ = g.Wait()
	slog.Info("Finished processing batch", "type", jobType, "count", len(jobsList))
}

func (w *Worker) processClassificationTask(ctx context.Context, job domain.URLRecord) error {
	return w.storage.WithTransaction(ctx, func(qtx *generated.Queries, tx pgx.Tx) error {
		content, err := w.crawler.FetchAndParseContent(ctx, job.URL)
		if err != nil {
			return qtx.UpdateStatus(ctx, generated.UpdateStatusParams{
				ID:           job.ID,
				Status:       generated.CrawlStatusFailed,
				ErrorMessage: pgtype.Text{String: err.Error(), Valid: true},
			})
		}
		if content.IsNonHTML {
			return qtx.UpdateStatus(ctx, generated.UpdateStatusParams{
				ID:           job.ID,
				Status:       generated.CrawlStatusIrrelevant,
				ErrorMessage: pgtype.Text{String: "Content-Type was not HTML", Valid: true},
			})
		}
		if content.IsCSR {
			return qtx.UpdateStatusAndRendering(ctx, generated.UpdateStatusAndRenderingParams{
				ID:           job.ID,
				Status:       generated.CrawlStatusIrrelevant,
				Rendering:    generated.NullRenderingType{RenderingType: generated.RenderingTypeCSR, Valid: true},
				ErrorMessage: pgtype.Text{String: "Detected Client-Side Rendering", Valid: true},
			})
		}

		reqBody := domain.PredictionRequest{URL: content.FinalURL, HTMLContent: content.HTMLContent, TextContent: content.TextContent}
		jsonBody, _ := json.Marshal(reqBody)
		httpReq, _ := http.NewRequestWithContext(ctx, "POST", w.cfg.MLApiURL+"/predict", bytes.NewBuffer(jsonBody))
		httpReq.Header.Set("Content-Type", "application/json")

		resp, err := w.httpClient.Do(httpReq)
		if err != nil {
			return qtx.UpdateStatus(ctx, generated.UpdateStatusParams{
				ID:           job.ID,
				Status:       generated.CrawlStatusFailed,
				ErrorMessage: pgtype.Text{String: "Prediction API call failed: " + err.Error(), Valid: true},
			})
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(resp.Body)
			errMsg := fmt.Sprintf("Prediction API returned non-200 status: %d - %s", resp.StatusCode, string(bodyBytes))
			return qtx.UpdateStatus(ctx, generated.UpdateStatusParams{
				ID:           job.ID,
				Status:       generated.CrawlStatusFailed,
				ErrorMessage: pgtype.Text{String: errMsg, Valid: true},
			})
		}

		var predResp domain.PredictionResponse
		if err := json.NewDecoder(resp.Body).Decode(&predResp); err != nil {
			return qtx.UpdateStatus(ctx, generated.UpdateStatusParams{
				ID:           job.ID,
				Status:       generated.CrawlStatusFailed,
				ErrorMessage: pgtype.Text{String: "Failed to decode prediction response: " + err.Error(), Valid: true},
			})
		}

		if predResp.IsPersonalBlog {
			if err := w.performCrawlLogic(ctx, qtx, job, content); err != nil {
				return err
			}
			return qtx.UpdateURLAsCompleted(ctx, generated.UpdateURLAsCompletedParams{
				ID:          job.ID,
				Status:      generated.CrawlStatusCompleted,
				Title:       pgtype.Text{String: content.Title, Valid: content.Title != ""},
				Description: pgtype.Text{String: content.Description, Valid: content.Description != ""},
				Content:     pgtype.Text{String: content.TextContent, Valid: content.TextContent != ""},
				Rendering:   generated.NullRenderingType{RenderingType: generated.RenderingTypeSSR, Valid: true},
			})
		}
		return qtx.UpdateURLAsCompleted(ctx, generated.UpdateURLAsCompletedParams{
			ID:          job.ID,
			Status:      generated.CrawlStatusIrrelevant,
			Title:       pgtype.Text{String: content.Title, Valid: content.Title != ""},
			Description: pgtype.Text{String: content.Description, Valid: content.Description != ""},
			Content:     pgtype.Text{String: content.TextContent, Valid: content.TextContent != ""},
			Rendering:   generated.NullRenderingType{RenderingType: generated.RenderingTypeSSR, Valid: true},
		})
	})
}

func (w *Worker) processCrawlTask(ctx context.Context, job domain.URLRecord) error {
	return w.storage.WithTransaction(ctx, func(qtx *generated.Queries, tx pgx.Tx) error {
		content, err := w.crawler.FetchAndParseContent(ctx, job.URL)
		if err != nil {
			return qtx.UpdateStatus(ctx, generated.UpdateStatusParams{
				ID:           job.ID,
				Status:       generated.CrawlStatusFailed,
				ErrorMessage: pgtype.Text{String: err.Error(), Valid: true},
			})
		}

		if content.IsNonHTML {
			return qtx.UpdateStatus(ctx, generated.UpdateStatusParams{
				ID:           job.ID,
				Status:       generated.CrawlStatusCompleted,
				ErrorMessage: pgtype.Text{String: "Content-Type was not HTML", Valid: true},
			})
		}

		if content.IsCSR {
			return qtx.UpdateStatusAndRendering(ctx, generated.UpdateStatusAndRenderingParams{
				ID:           job.ID,
				Status:       generated.CrawlStatusCompleted,
				Rendering:    generated.NullRenderingType{RenderingType: generated.RenderingTypeCSR, Valid: true},
				ErrorMessage: pgtype.Text{String: "Detected CSR during crawl", Valid: true},
			})
		}

		if err := w.performCrawlLogic(ctx, qtx, job, content); err != nil {
			return err
		}
		return qtx.UpdateURLAsCompleted(ctx, generated.UpdateURLAsCompletedParams{
			ID:          job.ID,
			Status:      generated.CrawlStatusCompleted,
			Title:       pgtype.Text{String: content.Title, Valid: content.Title != ""},
			Description: pgtype.Text{String: content.Description, Valid: content.Description != ""},
			Content:     pgtype.Text{String: content.TextContent, Valid: content.TextContent != ""},
			Rendering:   generated.NullRenderingType{RenderingType: generated.RenderingTypeSSR, Valid: true},
		})
	})
}

func (w *Worker) performCrawlLogic(ctx context.Context, qtx *generated.Queries, job domain.URLRecord, content *domain.FetchedContent) error {
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

	netlocCountsRows, err := qtx.GetNetlocCounts(ctx, allNetlocsFound)
	if err != nil {
		return fmt.Errorf("crawl-logic: failed to get netloc counts: %w", err)
	}
	netlocCounts := make(map[string]int32)
	for _, row := range netlocCountsRows {
		// MODIFIED: The generated struct field is now UrlCount, not Count.
		netlocCounts[row.Netloc] = row.UrlCount
	}

	existingURLsRows, err := qtx.GetExistingURLs(ctx, newLinks)
	if err != nil {
		return fmt.Errorf("crawl-logic: failed to check existing urls: %w", err)
	}
	existingURLs := make(map[string]struct{}, len(existingURLsRows))
	for _, urlStr := range existingURLsRows {
		existingURLs[urlStr] = struct{}{}
	}

	domainDecisionsRows, err := qtx.GetDomainDecisions(ctx, allNetlocsFound)
	if err != nil {
		return fmt.Errorf("crawl-logic: failed to get domain decisions: %w", err)
	}
	domainDecisions := make(map[string]generated.CrawlStatus)
	for _, row := range domainDecisionsRows {
		domainDecisions[row.Netloc] = row.Status
	}

	sourceNetloc, err := url.Parse(job.URL)
	if err != nil {
		return fmt.Errorf("crawl-logic: could not parse source job URL %q: %w", job.URL, err)
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
