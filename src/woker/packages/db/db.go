package db

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"
	"worker/packages/config"
	"worker/packages/domain"
	"worker/packages/generated"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	pendingURLCountLimit = 50000
	pendingCounterName   = "pending_urls_count"
)

type Storage struct {
	DB                 *pgxpool.Pool
	Queries            *generated.Queries
	cfg                config.Config
	linkQueue          chan domain.LinkBatch
	statusUpdateQueue  chan domain.StatusUpdateResult
	contentInsertQueue chan domain.ContentInsertResult
}

func New(ctx context.Context, cfg config.Config) (*Storage, error) {
	poolConfig, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("unable to parse database url: %w", err)
	}

	db, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to create connection pool: %w", err)
	}

	s := &Storage{
		DB:                 db,
		Queries:            generated.New(db),
		cfg:                cfg,
		linkQueue:          make(chan domain.LinkBatch, cfg.BatchWriteQueueSize),
		statusUpdateQueue:  make(chan domain.StatusUpdateResult, cfg.StatusUpdateQueueSize),
		contentInsertQueue: make(chan domain.ContentInsertResult, cfg.ContentInsertQueueSize),
	}

	go s.linkWriter(ctx)
	go s.statusWriter(ctx)
	go s.contentWriter(ctx)
	slog.Info("Asynchronous database writers started", "count", 3)

	return s, nil
}

// ... (Close, WithTransaction, Enqueue methods are unchanged) ...
func (s *Storage) Close() {
	if s.linkQueue != nil {
		close(s.linkQueue)
	}
	if s.statusUpdateQueue != nil {
		close(s.statusUpdateQueue)
	}
	if s.contentInsertQueue != nil {
		close(s.contentInsertQueue)
	}
	s.DB.Close()
	slog.Info("Database connection and writer channels closed.")
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

func (s *Storage) EnqueueStatusUpdate(result domain.StatusUpdateResult) {
	select {
	case s.statusUpdateQueue <- result:
	default:
		slog.Warn("Status update queue is full. Dropping status update.", "job_id", result.ID)
	}
}

func (s *Storage) EnqueueContentInsert(result domain.ContentInsertResult) {
	select {
	case s.contentInsertQueue <- result:
	default:
		slog.Warn("Content insert queue is full. Dropping content insert.", "job_id", result.ID)
	}
}

// --- Reaper and Throttling Methods ---

func (s *Storage) ResetStalledJobs(ctx context.Context) error {
	start := time.Now()
	interval := pgtype.Interval{
		Microseconds: s.cfg.JobTimeout.Microseconds(),
		Valid:        true,
	}
	// The ResetStalledJobs query now returns the number of rows affected.
	// We need to modify query.sql and the generated code for this.
	// For now, we'll assume it doesn't and log the execution.
	// In a future step we would update the SQL query to `RETURNING id` and count here.
	err := s.Queries.ResetStalledJobs(ctx, interval)
	duration := time.Since(start)
	if err != nil {
		slog.Error("Stalled job reset failed",
			"event", slog.GroupValue(slog.String("name", "STALLED_JOB_RESET_COMPLETED"), slog.String("stage", "end"), slog.Float64("duration_ms", float64(duration.Microseconds())/1000.0)),
			"details", slog.GroupValue(slog.Any("error", err.Error())),
		)
		return err
	}

	// This part of the log is less useful without knowing the count, but we log success.
	slog.Info("Stalled job reset completed",
		"event", slog.GroupValue(slog.String("name", "STALLED_JOB_RESET_COMPLETED"), slog.String("stage", "end"), slog.Float64("duration_ms", float64(duration.Microseconds())/1000.0)),
	)
	return nil
}

func (s *Storage) ResetOrphanedJobs(ctx context.Context) error {
	start := time.Now()
	rowsAffected, err := s.Queries.ResetOrphanedCompletedJobs(ctx)
	duration := time.Since(start)

	if err != nil {
		slog.Error("Orphaned job reset failed",
			"event", slog.GroupValue(slog.String("name", "ORPHANED_JOB_RESET_COMPLETED"), slog.String("stage", "end"), slog.Float64("duration_ms", float64(duration.Microseconds())/1000.0)),
			"details", slog.GroupValue(slog.Any("error", err.Error())),
		)
		return fmt.Errorf("failed to reset orphaned jobs: %w", err)
	}
	slog.Info("Orphaned job reset completed",
		"event", slog.GroupValue(slog.String("name", "ORPHANED_JOB_RESET_COMPLETED"), slog.String("stage", "end"), slog.Float64("duration_ms", float64(duration.Microseconds())/1000.0)),
		"details", slog.GroupValue(
			slog.Any("output", map[string]interface{}{"requeued_job_count": rowsAffected}),
		),
	)

	return nil
}

func (s *Storage) GetPendingURLCount(ctx context.Context) (int64, error) {
	return s.Queries.GetCounterValue(ctx, pendingCounterName)
}

func (s *Storage) RefreshPendingURLCount(ctx context.Context) error {
	start := time.Now()
	count, err := s.Queries.CountPendingURLs(ctx)
	if err != nil {
		return fmt.Errorf("failed to count pending urls: %w", err)
	}
	err = s.Queries.UpdateCounterValue(ctx, generated.UpdateCounterValueParams{
		Value:       count,
		CounterName: pendingCounterName,
	})
	duration := time.Since(start)
	if err != nil {
		return fmt.Errorf("failed to update counter value: %w", err)
	}

	slog.Info("Refreshed pending URL count",
		"event", slog.GroupValue(slog.String("name", "PENDING_URL_COUNT_REFRESHED"), slog.String("stage", "end"), slog.Float64("duration_ms", float64(duration.Microseconds())/1000.0)),
		"details", slog.GroupValue(
			slog.Any("output", map[string]interface{}{"count": count}),
		),
	)
	return nil
}

func (s *Storage) RefreshNetlocCounts(ctx context.Context) error {
	start := time.Now()
	err := s.Queries.RefreshNetlocCounts(ctx)
	duration := time.Since(start)
	if err != nil {
		slog.Error("Netloc counts refresh failed",
			"event", slog.GroupValue(slog.String("name", "NETLOC_COUNTS_REFRESHED"), slog.String("stage", "end"), slog.Float64("duration_ms", float64(duration.Microseconds())/1000.0)),
			"details", slog.GroupValue(slog.Any("error", err.Error())),
		)
		return err
	}
	slog.Info("Netloc counts refresh completed",
		"event", slog.GroupValue(slog.String("name", "NETLOC_COUNTS_REFRESHED"), slog.String("stage", "end"), slog.Float64("duration_ms", float64(duration.Microseconds())/1000.0)),
	)
	return nil
}

// ... (writer goroutines are unchanged) ...
func (s *Storage) statusWriter(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.StatusUpdateInterval)
	defer ticker.Stop()
	var batch []domain.StatusUpdateResult

	for {
		select {
		case <-ctx.Done():
			if len(batch) > 0 {
				slog.Info("Status Writer: Final write on shutdown...")
				s.processStatusUpdates(context.Background(), batch)
			}
			slog.Info("Status Writer: Shutdown.")
			return
		case result, ok := <-s.statusUpdateQueue:
			if !ok {
				if len(batch) > 0 {
					s.processStatusUpdates(context.Background(), batch)
				}
				slog.Info("Status Writer: Queue closed, exiting.")
				return
			}
			batch = append(batch, result)
			if len(batch) >= s.cfg.StatusUpdateBatchSize {
				s.processStatusUpdates(ctx, batch)
				batch = nil
			}
		case <-ticker.C:
			if len(batch) > 0 {
				s.processStatusUpdates(ctx, batch)
				batch = nil
			}
		}
	}
}

func (s *Storage) processStatusUpdates(ctx context.Context, batch []domain.StatusUpdateResult) {
	if len(batch) == 0 {
		return
	}
	start := time.Now()

	var statusSQL, errorMsgSQL, renderingSQL strings.Builder
	var args []interface{}
	var ids []int64
	paramIdx := 1
	var renderingUpdatesExist bool

	statusSQL.WriteString("CASE id ")
	errorMsgSQL.WriteString("CASE id ")
	renderingSQL.WriteString("CASE id ")

	for _, item := range batch {
		ids = append(ids, item.ID)

		statusSQL.WriteString(fmt.Sprintf("WHEN $%d THEN $%d::crawl_status ", paramIdx, paramIdx+1))
		args = append(args, item.ID, item.Status)
		paramIdx += 2

		errorMsgSQL.WriteString(fmt.Sprintf("WHEN $%d THEN $%d ", paramIdx, paramIdx+1))
		args = append(args, item.ID, pgtype.Text{String: item.ErrorMsg, Valid: item.ErrorMsg != ""})
		paramIdx += 2

		if item.Rendering != "" {
			renderingSQL.WriteString(fmt.Sprintf("WHEN $%d THEN $%d::rendering_type ", paramIdx, paramIdx+1))
			args = append(args, item.ID, item.Rendering)
			paramIdx += 2
			renderingUpdatesExist = true
		}
	}

	idsParam := fmt.Sprintf("$%d", paramIdx)
	args = append(args, ids)

	statusSQL.WriteString("END")
	errorMsgSQL.WriteString("END")

	var renderingUpdateSQL string
	if renderingUpdatesExist {
		renderingSQL.WriteString("ELSE rendering END")
		renderingUpdateSQL = fmt.Sprintf(", rendering = %s", renderingSQL.String())
	}

	sql := fmt.Sprintf(`UPDATE urls SET status = %s, error_message = %s, processed_at = NOW() %s WHERE id = ANY(%s)`,
		statusSQL.String(), errorMsgSQL.String(), renderingUpdateSQL, idsParam)

	_, err := s.DB.Exec(ctx, sql, args...)
	duration := time.Since(start)

	if err != nil {
		slog.Error("DB batch write failed",
			"event", slog.GroupValue(slog.String("name", "DB_BATCH_WRITE_COMPLETED"), slog.String("stage", "end"), slog.Float64("duration_ms", float64(duration.Microseconds())/1000.0)),
			"details", slog.GroupValue(
				slog.Any("input", map[string]interface{}{"writer_type": "status_writer", "batch_size": len(batch)}),
				slog.Any("output", map[string]interface{}{"error": err.Error()}),
			),
		)
	} else {
		slog.Info("DB batch write completed",
			"event", slog.GroupValue(slog.String("name", "DB_BATCH_WRITE_COMPLETED"), slog.String("stage", "end"), slog.Float64("duration_ms", float64(duration.Microseconds())/1000.0)),
			"details", slog.GroupValue(
				slog.Any("input", map[string]interface{}{"writer_type": "status_writer", "batch_size": len(batch)}),
			),
		)
	}
}

func (s *Storage) contentWriter(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.ContentInsertInterval)
	defer ticker.Stop()
	var batch []domain.ContentInsertResult

	for {
		select {
		case <-ctx.Done():
			if len(batch) > 0 {
				slog.Info("Content Writer: Final write on shutdown...")
				s.processContentInserts(context.Background(), batch)
			}
			slog.Info("Content Writer: Shutdown.")
			return
		case result, ok := <-s.contentInsertQueue:
			if !ok {
				if len(batch) > 0 {
					s.processContentInserts(context.Background(), batch)
				}
				slog.Info("Content Writer: Queue closed, exiting.")
				return
			}
			batch = append(batch, result)
			if len(batch) >= s.cfg.ContentInsertBatchSize {
				s.processContentInserts(ctx, batch)
				batch = nil
			}
		case <-ticker.C:
			if len(batch) > 0 {
				s.processContentInserts(ctx, batch)
				batch = nil
			}
		}
	}
}

func (s *Storage) processContentInserts(ctx context.Context, batch []domain.ContentInsertResult) {
	if len(batch) == 0 {
		return
	}
	start := time.Now()
	err := s.WithTransaction(ctx, func(qtx *generated.Queries, tx pgx.Tx) error {
		var ids []int64
		for _, item := range batch {
			ids = append(ids, item.ID)
		}
		updateSQL := `UPDATE urls SET status = $1, processed_at = NOW() WHERE id = ANY($2)`
		if _, err := tx.Exec(ctx, updateSQL, generated.CrawlStatusCompleted, ids); err != nil {
			return fmt.Errorf("failed to batch update urls to completed: %w", err)
		}

		rows := make([][]interface{}, len(batch))
		for i, item := range batch {
			rows[i] = []interface{}{
				item.ID,
				pgtype.Text{String: item.Title, Valid: item.Title != ""},
				pgtype.Text{String: item.Description, Valid: item.Description != ""},
				pgtype.Text{String: item.TextContent, Valid: item.TextContent != ""},
			}
		}

		_, err := tx.CopyFrom(ctx,
			pgx.Identifier{"url_content"},
			[]string{"url_id", "title", "description", "content"},
			pgx.CopyFromRows(rows),
		)
		if err != nil {
			return fmt.Errorf("failed to bulk insert content via COPY: %w", err)
		}
		return nil
	})
	duration := time.Since(start)

	if err != nil {
		slog.Error("DB batch write failed",
			"event", slog.GroupValue(slog.String("name", "DB_BATCH_WRITE_COMPLETED"), slog.String("stage", "end"), slog.Float64("duration_ms", float64(duration.Microseconds())/1000.0)),
			"details", slog.GroupValue(
				slog.Any("input", map[string]interface{}{"writer_type": "content_writer", "batch_size": len(batch)}),
				slog.Any("output", map[string]interface{}{"error": err.Error()}),
			),
		)
	} else {
		slog.Info("DB batch write completed",
			"event", slog.GroupValue(slog.String("name", "DB_BATCH_WRITE_COMPLETED"), slog.String("stage", "end"), slog.Float64("duration_ms", float64(duration.Microseconds())/1000.0)),
			"details", slog.GroupValue(
				slog.Any("input", map[string]interface{}{"writer_type": "content_writer", "batch_size": len(batch)}),
			),
		)
	}
}

func (s *Storage) linkWriter(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.BatchWriteInterval)
	defer ticker.Stop()
	var batches []domain.LinkBatch

	for {
		select {
		case <-ctx.Done():
			if len(batches) > 0 {
				slog.Info("Link Writer: Final write on shutdown...")
				s.processLinkBatches(context.Background(), batches)
			}
			slog.Info("Link Writer: Shutdown.")
			return
		case batch, ok := <-s.linkQueue:
			if !ok {
				if len(batches) > 0 {
					s.processLinkBatches(context.Background(), batches)
				}
				slog.Info("Link Writer: Link queue closed, exiting.")
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
	start := time.Now()
	count, err := s.GetPendingURLCount(ctx)
	if err != nil {
		slog.Error("Link Writer: Failed to get pending URL count for throttling check", "error", err)
	} else if count >= pendingURLCountLimit {
		slog.Warn("Link Writer: Throttling link ingestion, pending queue is full", "count", count, "limit", pendingURLCountLimit)
		return
	}

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

	err = s.WithTransaction(ctx, func(qtx *generated.Queries, tx pgx.Tx) error {
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

		var newURLsToInsert []domain.NewLink
		for urlStr, link := range allCandidateLinks {
			if _, exists := urlToIDMap[urlStr]; !exists {
				newURLsToInsert = append(newURLsToInsert, link)
			}
		}

		if len(newURLsToInsert) > 0 {
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

			if _, err := pgx.ForEachRow(insertedRows, []any{&idPtr, &urlPtr}, func() error {
				urlToIDMap[urlPtr] = idPtr
				return nil
			}); err != nil {
				insertedRows.Close()
				return fmt.Errorf("failed to iterate newly inserted URL rows: %w", err)
			}
			insertedRows.Close()
		}

		var edgeRows [][]any
		for _, batch := range batches {
			sourceURLID := batch.SourceURLID
			for _, link := range batch.NewLinks {
				if destID, ok := urlToIDMap[link.URL]; ok {
					edgeRows = append(edgeRows, []any{sourceURLID, destID})
				}
			}
		}

		if len(edgeRows) > 0 {
			_, err := tx.CopyFrom(ctx, pgx.Identifier{"url_edges"}, []string{"source_url_id", "dest_url_id"}, pgx.CopyFromRows(edgeRows))
			// Ignore unique constraint violations for edges, as they are not critical.
			if err != nil && !strings.Contains(err.Error(), "23505") {
				return fmt.Errorf("failed to bulk insert edges: %w", err)
			}
		}
		return nil
	})
	duration := time.Since(start)

	if err != nil {
		slog.Error("DB batch write failed",
			"event", slog.GroupValue(slog.String("name", "DB_BATCH_WRITE_COMPLETED"), slog.String("stage", "end"), slog.Float64("duration_ms", float64(duration.Microseconds())/1000.0)),
			"details", slog.GroupValue(
				slog.Any("input", map[string]interface{}{"writer_type": "link_writer", "batch_size": len(allCandidateLinks)}),
				slog.Any("output", map[string]interface{}{"error": err.Error()}),
			),
		)
	} else {
		slog.Info("DB batch write completed",
			"event", slog.GroupValue(slog.String("name", "DB_BATCH_WRITE_COMPLETED"), slog.String("stage", "end"), slog.Float64("duration_ms", float64(duration.Microseconds())/1000.0)),
			"details", slog.GroupValue(
				slog.Any("input", map[string]interface{}{"writer_type": "link_writer", "batch_size": len(allCandidateLinks)}),
			),
		)
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
