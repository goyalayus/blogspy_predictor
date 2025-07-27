// Package db
package db

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"
	"worker/packages/domain"
	"worker/packages/generated"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Storage struct {
	DB        *pgxpool.Pool
	Queries   *generated.Queries
	cfg       Config
	linkQueue chan domain.LinkBatch
}

type Config struct {
	JobTimeout          time.Duration
	BatchWriteInterval  time.Duration
	BatchWriteQueueSize int
}

func New(ctx context.Context, databaseURL string, cfg Config) (*Storage, error) {
	db, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("unable to create connection pool: %w", err)
	}

	s := &Storage{
		DB:        db,
		Queries:   generated.New(db),
		cfg:       cfg,
		linkQueue: make(chan domain.LinkBatch, cfg.BatchWriteQueueSize),
	}

	go s.databaseWriter(ctx)
	slog.Info("Database writer goroutine started")

	return s, nil
}

func (s *Storage) Close() {
	close(s.linkQueue)
	s.DB.Close()
}

func (s *Storage) WithTransaction(ctx context.Context, fn func(qtx *generated.Queries, tx pgx.Tx) error) (err error) {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback(ctx)
			panic(p)
		} else if err != nil {
			_ = tx.Rollback(ctx)
		} else {
			err = tx.Commit(ctx)
		}
	}()

	qtx := s.Queries.WithTx(tx)
	err = fn(qtx, tx)
	return err
}

func (s *Storage) EnqueueLinks(batch domain.LinkBatch) {
	select {
	case s.linkQueue <- batch:
	default:
		slog.Warn("Link queue is full. Dropping links.", "count", len(batch.NewLinks))
	}
}

func (s *Storage) ResetStalledJobs(ctx context.Context) {
	interval := pgtype.Interval{
		Microseconds: s.cfg.JobTimeout.Microseconds(),
		Valid:        true,
	}

	err := s.Queries.ResetStalledJobs(ctx, interval)
	if err != nil {
		slog.Error("Reaper: Failed to reset stalled jobs", "error", err)
	}
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

	err := s.WithTransaction(ctx, func(qtx *generated.Queries, tx pgx.Tx) error {
		var urlRows [][]any
		for _, link := range allNewLinks {
			urlRows = append(urlRows, []any{link.URL, link.Netloc, string(link.Status)})
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

		idRows, err := tx.Query(ctx, `SELECT id, url FROM urls WHERE url = ANY($1)`, urlStrings)
		if err != nil {
			return fmt.Errorf("failed to re-query for IDs: %w", err)
		}

		urlToIDMap := make(map[string]int64)
		idPtr := new(int64)
		urlPtr := new(string)
		if _, err := pgx.ForEachRow(idRows, []any{idPtr, urlPtr}, func() error {
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

func (s *Storage) LockJobs(ctx context.Context, fromStatus, toStatus generated.CrawlStatus, limit int32) ([]generated.LockJobsForUpdateRow, error) {
	var jobs []generated.LockJobsForUpdateRow

	err := s.WithTransaction(ctx, func(qtx *generated.Queries, tx pgx.Tx) error {
		var err error
		jobs, err = qtx.LockJobsForUpdate(ctx, generated.LockJobsForUpdateParams{
			Status: fromStatus,
			Limit:  limit,
		})
		if err != nil {
			return fmt.Errorf("failed to lock jobs: %w", err)
		}
		if len(jobs) == 0 {
			return nil
		}

		jobIDs := make([]int64, len(jobs))
		for i, job := range jobs {
			jobIDs[i] = job.ID
		}

		return qtx.UpdateJobStatusToInProgress(ctx, generated.UpdateJobStatusToInProgressParams{
			Status: toStatus,
			JobIds: jobIDs,
		})
	})

	if err != nil {
		return nil, err
	}

	return jobs, nil
}
