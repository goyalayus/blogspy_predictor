// woker/packages/db/db.go

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
	// 1. Aggregate all unique candidate links from all in-memory batches
	allCandidateLinks := make(map[string]domain.NewLink)
	for _, batch := range batches {
		for _, link := range batch.NewLinks {
			allCandidateLinks[link.URL] = link
		}
	}
	if len(allCandidateLinks) == 0 {
		return
	}

	candidateURLStrings := make([]string, 0, len(allCandidateLinks))
	for urlStr := range allCandidateLinks {
		candidateURLStrings = append(candidateURLStrings, urlStr)
	}

	err := s.WithTransaction(ctx, func(qtx *generated.Queries, tx pgx.Tx) error {
		// 2. Pre-filter to find which URLs already exist and fetch their IDs in a single query.
		// This avoids the expensive INSERT...ON CONFLICT lookup and a redundant SELECT.
		existingRows, err := tx.Query(ctx, `SELECT id, url FROM urls WHERE url = ANY($1)`, candidateURLStrings)
		if err != nil {
			return fmt.Errorf("failed to query for existing URLs: %w", err)
		}

		urlToIDMap := make(map[string]int64)
		var idPtr int64
		var urlPtr string
		if _, err := pgx.ForEachRow(existingRows, []any{&idPtr, &urlPtr}, func() error {
			urlToIDMap[urlPtr] = idPtr
			return nil
		}); err != nil {
			return fmt.Errorf("failed to iterate existing URL rows: %w", err)
		}
		existingRows.Close()

		// 3. Identify the subset of URLs that are truly new.
		var newURLsToInsert []domain.NewLink
		for urlStr, link := range allCandidateLinks {
			if _, exists := urlToIDMap[urlStr]; !exists {
				newURLsToInsert = append(newURLsToInsert, link)
			}
		}

		// 4. Batch insert ONLY the new URLs using a single INSERT with multiple VALUES.
		// Use RETURNING to get the new IDs back without a second query.
		if len(newURLsToInsert) > 0 {
			// Note: pgx has a parameter limit of 65535. With 3 columns, we can insert
			// ~21,845 rows per batch, which is more than enough for our queue size.
			sql := "INSERT INTO urls (url, netloc, status) VALUES "
			var args []interface{}
			paramIdx := 1
			for i, link := range newURLsToInsert {
				if i > 0 {
					sql += ", "
				}
				sql += fmt.Sprintf("($%d, $%d, $%d)", paramIdx, paramIdx+1, paramIdx+2)
				args = append(args, link.URL, link.Netloc, string(link.Status))
				paramIdx += 3
			}
			sql += " RETURNING id, url"

			insertedRows, err := tx.Query(ctx, sql, args...)
			if err != nil {
				return fmt.Errorf("failed to batch insert new urls: %w", err)
			}

			// Populate the map with the newly created IDs
			if _, err := pgx.ForEachRow(insertedRows, []any{&idPtr, &urlPtr}, func() error {
				urlToIDMap[urlPtr] = idPtr
				return nil
			}); err != nil {
				insertedRows.Close()
				return fmt.Errorf("failed to iterate newly inserted URL rows: %w", err)
			}
			insertedRows.Close()
		}

		// 5. Now that urlToIDMap is complete, build and bulk insert all edges using COPY.
		var edgeRows [][]any
		for _, batch := range batches {
			sourceURLID := batch.SourceURLID
			for _, link := range batch.NewLinks {
				if destID, ok := urlToIDMap[link.URL]; ok {
					edgeRows = append(edgeRows, []any{sourceURLID, destID})
				} else {
					// This should theoretically not happen if logic is correct, but good to log.
					slog.Warn("DB Writer: Could not find ID for a link to create an edge", "url", link.URL)
				}
			}
		}

		if len(edgeRows) > 0 {
			_, err := tx.CopyFrom(ctx, pgx.Identifier{"url_edges"}, []string{"source_url_id", "dest_url_id"}, pgx.CopyFromRows(edgeRows))
			// A unique_violation (23505) can happen if two workers process pages that link
			// to each other at the same time. This is safe to ignore.
			if err != nil && !strings.Contains(err.Error(), "23505") {
				return fmt.Errorf("failed to bulk insert edges: %w", err)
			}
		}
		return nil
	})

	if err != nil {
		slog.Error("DB Writer: Transaction failed", "error", err)
	} else {
		slog.Info("DB Writer: Successfully committed batch", "candidate_urls", len(allCandidateLinks))
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
