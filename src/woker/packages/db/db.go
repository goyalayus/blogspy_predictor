// Package db
package db

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"
	"worker/packages/domain"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Storage struct {
	DB        *pgxpool.Pool
	linkQueue chan domain.LinkBatch
	cfg       Config
}

type Config struct {
	BatchWriteInterval  time.Duration
	BatchWriteQueueSize int
	JobTimeout          time.Duration
}

func New(ctx context.Context, databaseURL string, cfg Config) (*Storage, error) {
	db, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("unable to create connection pool: %w", err)
	}

	s := &Storage{
		DB:        db,
		linkQueue: make(chan domain.LinkBatch, cfg.BatchWriteQueueSize),
		cfg:       cfg,
	}

	go s.databaseWriter(ctx)
	slog.Info("Database writer goroutine started")

	return s, nil
}

func (s *Storage) Close() {
	close(s.linkQueue)
	s.DB.Close()
}

func (s *Storage) EnqueueLinks(batch domain.LinkBatch) {
	select {
	case s.linkQueue <- batch:
	default:
		slog.Warn("Link queue is full. Dropping links.", "count", len(batch.NewLinks))
	}
}

func (s *Storage) WithTransaction(ctx context.Context, fn func(tx pgx.Tx) error) (err error) {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback(ctx)
			panic(p)
		} else if err != nil {
			if rbErr := tx.Rollback(ctx); rbErr != nil {
				slog.Error("Error during transaction rollback", "original_error", err, "rollback_error", rbErr)
			}
		} else {
			err = tx.Commit(ctx)
		}
	}()

	err = fn(tx)
	return err
}

func (s *Storage) databaseWriter(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.BatchWriteInterval)
	defer ticker.Stop()
	var batches []domain.LinkBatch

	for {
		select {
		case <-ctx.Done():
			if len(batches) > 0 {
				slog.Info("DB Writer: Final write on shutdown...")
				s.processLinkBatches(context.Background(), batches)
			}
			slog.Info("DB Writer: Shutdown.")
			return
		case batch, ok := <-s.linkQueue:
			if !ok {
				if len(batches) > 0 {
					s.processLinkBatches(context.Background(), batches)
				}
				slog.Info("DB Writer: Link queue closed, exiting.")
				return
			}
			batches = append(batches, batch)
		case <-ticker.C:
			if len(batches) > 0 {
				s.processLinkBatches(ctx, batches)
				batches = nil
			}
		}
	}
}

func (s *Storage) processLinkBatches(ctx context.Context, batches []domain.LinkBatch) {
	allNewLinks := make(map[string]domain.NewLink)
	for _, batch := range batches {
		for _, link := range batch.NewLinks {
			allNewLinks[link.URL] = link
		}
	}
	if len(allNewLinks) == 0 {
		return
	}

	err := s.WithTransaction(ctx, func(tx pgx.Tx) error {
		var urlRows [][]any
		for _, link := range allNewLinks {
			urlRows = append(urlRows, []any{link.URL, link.Netloc, link.Status})
		}

		if _, err := tx.Exec(ctx, `CREATE TEMP TABLE new_urls (url TEXT, netloc TEXT, status crawl_status) ON COMMIT DROP`); err != nil {
			return fmt.Errorf("failed to create temp table: %w", err)
		}

		_, err := tx.CopyFrom(ctx, pgx.Identifier{"new_urls"}, []string{"url", "netloc", "status"}, pgx.CopyFromRows(urlRows))
		if err != nil {
			return fmt.Errorf("failed to copy to temp table: %w", err)
		}

		if _, err := tx.Exec(ctx, `INSERT INTO urls (url, netloc, status) SELECT url, netloc, status FROM new_urls ON CONFLICT (url) DO NOTHING`); err != nil {
			return fmt.Errorf("failed to insert from temp table: %w", err)
		}

		urlStrings := make([]string, 0, len(allNewLinks))
		for urlStr := range allNewLinks {
			urlStrings = append(urlStrings, urlStr)
		}
		rows, err := tx.Query(ctx, `SELECT id, url FROM urls WHERE url = ANY($1)`, urlStrings)
		if err != nil {
			return fmt.Errorf("failed to query for new URL IDs: %w", err)
		}

		urlToIDMap := make(map[string]int64)
		idPtr := new(int64)
		urlPtr := new(string)
		if _, err := pgx.ForEachRow(rows, []any{idPtr, urlPtr}, func() error {
			urlToIDMap[*urlPtr] = *idPtr
			return nil
		}); err != nil {
			return fmt.Errorf("failed to iterate new URL rows: %w", err)
		}

		var edgeRows [][]any
		for _, batch := range batches {
			for _, link := range batch.NewLinks {
				if destID, ok := urlToIDMap[link.URL]; ok {
					edgeRows = append(edgeRows, []any{batch.SourceURLID, destID})
				}
			}
		}
		if len(edgeRows) > 0 {
			if _, err := tx.CopyFrom(ctx, pgx.Identifier{"url_edges"}, []string{"source_url_id", "dest_url_id"}, pgx.CopyFromRows(edgeRows)); err != nil {
				if !strings.Contains(err.Error(), "23505") {
					return fmt.Errorf("failed to bulk insert edges: %w", err)
				}
			}
		}
		return nil
	})

	if err != nil {
		slog.Error("DB Writer: Transaction failed", "error", err)
	} else {
		slog.Info("DB Writer: Successfully committed batch", "potential_new_urls", len(allNewLinks))
	}
}

func (s *Storage) ResetStalledJobs(ctx context.Context) {
	query := `
        UPDATE urls
        SET
            status = CASE
                WHEN status = 'classifying'::crawl_status THEN 'pending_classification'::crawl_status
                WHEN status = 'crawling'::crawl_status THEN 'pending_crawl'::crawl_status
            END,
            locked_at = NULL
        WHERE
            status IN ('classifying'::crawl_status, 'crawling'::crawl_status)
            AND locked_at < NOW() - $1::interval
    `
	interval := fmt.Sprintf("%f minutes", s.cfg.JobTimeout.Minutes())
	res, err := s.DB.Exec(ctx, query, interval)
	if err != nil {
		slog.Error("Reaper: Failed to reset stalled jobs", "error", err)
	} else if res.RowsAffected() > 0 {
		slog.Warn("Reaper: Reset stalled jobs", "count", res.RowsAffected())
	}
}

func (s *Storage) GetNetlocCounts(ctx context.Context, tx pgx.Tx, netlocs []string) (map[string]int, error) {
	counts := make(map[string]int)
	if len(netlocs) == 0 {
		return counts, nil
	}
	query := `SELECT netloc, count(id) FROM urls WHERE netloc = ANY($1) GROUP BY netloc`
	rows, err := tx.Query(ctx, query, netlocs)
	if err != nil {
		return nil, err
	}
	netlocPtr := new(string)
	countPtr := new(int64)
	_, err = pgx.ForEachRow(rows, []any{netlocPtr, countPtr}, func() error {
		counts[*netlocPtr] = int(*countPtr)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return counts, nil
}

func (s *Storage) GetExistingURLs(ctx context.Context, tx pgx.Tx, urls []string) (map[string]struct{}, error) {
	existing := make(map[string]struct{})
	if len(urls) == 0 {
		return existing, nil
	}
	query := `SELECT url FROM urls WHERE url = ANY($1)`
	rows, err := tx.Query(ctx, query, urls)
	if err != nil {
		return nil, err
	}
	urlPtr := new(string)
	_, err = pgx.ForEachRow(rows, []any{urlPtr}, func() error {
		existing[*urlPtr] = struct{}{}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return existing, nil
}

func (s *Storage) GetDomainDecisions(ctx context.Context, tx pgx.Tx, netlocs []string) (map[string]domain.CrawlStatus, error) {
	decisions := make(map[string]domain.CrawlStatus)
	if len(netlocs) == 0 {
		return decisions, nil
	}
	query := `SELECT DISTINCT ON (netloc) netloc, status FROM urls WHERE netloc = ANY($1) AND status IN ($2, $3, $4, $5)`
	rows, err := tx.Query(ctx, query, netlocs, domain.PendingCrawl, domain.Crawling, domain.Completed, domain.Irrelevant)
	if err != nil {
		return nil, err
	}
	netlocPtr := new(string)
	statusPtr := new(domain.CrawlStatus)
	_, err = pgx.ForEachRow(rows, []any{netlocPtr, statusPtr}, func() error {
		decisions[*netlocPtr] = *statusPtr
		return nil
	})
	if err != nil {
		return nil, err
	}
	return decisions, nil
}

func (s *Storage) UpdateStatus(ctx context.Context, tx pgx.Tx, id int64, status domain.CrawlStatus, errMsg string) error {
	query := `UPDATE urls SET status = $1, error_message = $2, processed_at = NOW() WHERE id = $3`
	_, err := tx.Exec(ctx, query, status, errMsg, id)
	if err != nil {
		slog.Error("Failed to update status", "id", id, "error", err)
	}
	return err
}

func (s *Storage) UpdateStatusAndRendering(ctx context.Context, tx pgx.Tx, id int64, status domain.CrawlStatus, rendering domain.RenderingType, errMsg string) error {
	query := `UPDATE urls SET status = $1, rendering = $2, error_message = $3, processed_at = NOW() WHERE id = $4`
	_, err := tx.Exec(ctx, query, status, rendering, errMsg, id)
	if err != nil {
		slog.Error("Failed to update status and rendering", "id", id, "error", err)
	}
	return err
}

func (s *Storage) UpdateURLAsCompleted(ctx context.Context, tx pgx.Tx, id int64, content *domain.FetchedContent, status domain.CrawlStatus) error {
	query := `UPDATE urls SET status = $1, processed_at = NOW(), title = $2, description = $3, content = $4, rendering = $5 WHERE id = $6`
	_, err := tx.Exec(ctx, query, status, content.Title, content.Description, content.TextContent, domain.SSR, id)
	if err != nil {
		slog.Error("Failed to update URL as completed", "id", id, "error", err)
	}
	return err
}

func (s *Storage) LockJobs(ctx context.Context, fromStatus, toStatus domain.CrawlStatus, limit int) ([]domain.URLRecord, error) {
	var jobs []domain.URLRecord

	err := s.WithTransaction(ctx, func(tx pgx.Tx) error {
		query := `SELECT id, url FROM urls WHERE status = $1 FOR UPDATE SKIP LOCKED LIMIT $2`
		rows, err := tx.Query(ctx, query, fromStatus, limit)
		if err != nil {
			return fmt.Errorf("failed to query for jobs: %w", err)
		}

		jobs, err = pgx.CollectRows(rows, pgx.RowToStructByName[domain.URLRecord])
		if err != nil {
			return fmt.Errorf("failed to scan job rows: %w", err)
		}

		if len(jobs) == 0 {
			return nil
		}

		jobIDs := make([]int64, len(jobs))
		for i, job := range jobs {
			jobIDs[i] = job.ID
		}

		updateQuery := `UPDATE urls SET status = $1, locked_at = NOW() WHERE id = ANY($2)`
		if _, err := tx.Exec(ctx, updateQuery, toStatus, jobIDs); err != nil {
			return fmt.Errorf("failed to update job status: %w", err)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return jobs, nil
}
