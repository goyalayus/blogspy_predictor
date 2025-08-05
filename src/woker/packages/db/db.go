package db

import (
	"context"
	"errors"
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
	"github.com/redis/go-redis/v9"
)

const (
	pendingURLCountLimit = 50000
	pendingCounterName   = "pending_urls_count"
)

type Storage struct {
	DB                 *pgxpool.Pool
	Queries            *generated.Queries
	RedisClient        *redis.Client // NEW: Redis client
	cfg                config.Config
	linkQueue          chan domain.LinkBatch
	statusUpdateQueue  chan domain.StatusUpdateResult
	contentInsertQueue chan domain.ContentInsertResult
}

func New(ctx context.Context, cfg config.Config) (*Storage, error) {
	// Connect to PostgreSQL (existing logic)
	poolConfig, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("unable to parse database url: %w", err)
	}

	db, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to create connection pool: %w", err)
	}

	// NEW: Connect to Redis
	redisOpts := &redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	}
	redisClient := redis.NewClient(redisOpts)
	if _, err := redisClient.Ping(ctx).Result(); err != nil {
		return nil, fmt.Errorf("unable to connect to redis: %w", err)
	}
	slog.Info("Successfully connected to Redis", "addr", cfg.RedisAddr)

	s := &Storage{
		DB:                 db,
		Queries:            generated.New(db),
		RedisClient:        redisClient, // NEW: Assign client
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
	// NEW: Close Redis client
	if s.RedisClient != nil {
		if err := s.RedisClient.Close(); err != nil {
			slog.Error("Failed to close Redis client", "error", err)
		}
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

// Enqueue methods are unchanged
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

// Reaper and Throttling Methods are mostly unchanged
// ... (ResetStalledJobs, ResetOrphanedJobs, GetPendingURLCount, RefreshPendingURLCount, RefreshNetlocCounts are unchanged) ...
func (s *Storage) ResetStalledJobs(ctx context.Context) error {
	start := time.Now()
	interval := pgtype.Interval{
		Microseconds: s.cfg.JobTimeout.Microseconds(),
		Valid:        true,
	}
	err := s.Queries.ResetStalledJobs(ctx, interval)
	duration := time.Since(start)
	if err != nil {
		slog.Error("Stalled job reset failed",
			"event", slog.GroupValue(slog.String("name", "STALLED_JOB_RESET_COMPLETED"), slog.String("stage", "end"), slog.Float64("duration_ms", float64(duration.Microseconds())/1000.0)),
			"details", slog.GroupValue(slog.Any("error", err.Error())),
		)
		return err
	}
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

// Writer goroutines (statusWriter, contentWriter) are unchanged
// ...
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

// =================================================================================
// REWRITTEN: processLinkBatches now uses a Bloom Filter
// =================================================================================
// =================================================================================
// REWRITTEN: processLinkBatches now uses a Bloom Filter
// =================================================================================
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

	// ======================== START OF CORRECTED SEGMENT ========================

	// Step 1: Prepare arguments and check existence in Bloom Filter
	// CORRECTION: Convert []string to []interface{} for the variadic function.
	candidateURLInterfaces := make([]interface{}, len(candidateURLStrings))
	for i, v := range candidateURLStrings {
		candidateURLInterfaces[i] = v
	}

	existsResult, err := s.RedisClient.BFMExists(ctx, s.cfg.BloomFilterKey, candidateURLInterfaces...).Result()
	if err != nil {
		// If bloom filter fails, fallback to old behavior to avoid data loss
		slog.Error("Bloom filter check failed, falling back to full DB check.", "error", err)
		// For this implementation, we will proceed assuming the DB will catch duplicates.
	}

	// Step 2: Partition URLs based on Bloom Filter results
	var urlsToCheckInDB []string
	definitelyNewLinks := make(map[string]domain.NewLink)
	var bloomHits int
	if existsResult != nil {
		// CORRECTION: The result is a []bool, so we check for `true`, not `1`.
		for i, exists := range existsResult {
			urlStr := candidateURLStrings[i]
			if exists {
				urlsToCheckInDB = append(urlsToCheckInDB, urlStr)
				bloomHits++
			} else {
				definitelyNewLinks[urlStr] = allCandidateLinks[urlStr]
			}
		}
	} else {
		// Fallback case if Redis call failed: check all URLs in the DB
		urlsToCheckInDB = candidateURLStrings
	}
	slog.Debug("Bloom filter check complete", "candidates", len(candidateURLStrings), "hits", bloomHits, "db_check_required", len(urlsToCheckInDB))

	// ========================= END OF CORRECTED SEGMENT =========================

	// Step 3: The actual processing within a transaction
	var newlyAddedURLs []string
	err = s.WithTransaction(ctx, func(qtx *generated.Queries, tx pgx.Tx) error {
		// Step 3a: Check the "might exist" URLs against the database
		urlToIDMap := make(map[string]int64)
		if len(urlsToCheckInDB) > 0 {
			existingRows, err := tx.Query(ctx, `SELECT id, url FROM urls WHERE url = ANY($1)`, urlsToCheckInDB)
			if err != nil {
				return fmt.Errorf("failed to query for existing URLs: %w", err)
			}
			defer existingRows.Close() // Ensure rows are closed
			var idPtr int64
			var urlPtr string
			if _, err := pgx.ForEachRow(existingRows, []any{&idPtr, &urlPtr}, func() error {
				urlToIDMap[urlPtr] = idPtr
				return nil
			}); err != nil {
				return fmt.Errorf("failed to iterate existing URL rows: %w", err)
			}
		}

		// Step 3b: Consolidate all truly new URLs
		var newURLsToInsert []domain.NewLink
		// Add URLs that were definitely new from the bloom filter
		for _, link := range definitelyNewLinks {
			newURLsToInsert = append(newURLsToInsert, link)
		}
		// Add URLs that the bloom filter hit on, but were not in the DB (false positives)
		for _, urlStr := range urlsToCheckInDB {
			if _, exists := urlToIDMap[urlStr]; !exists {
				newURLsToInsert = append(newURLsToInsert, allCandidateLinks[urlStr])
			}
		}

		// Step 3c: Insert the new URLs
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
			defer insertedRows.Close() // Ensure rows are closed

			// Add newly inserted URLs to our map and a slice for bloom filter update
			var idPtr int64
			var urlPtr string
			if _, err := pgx.ForEachRow(insertedRows, []any{&idPtr, &urlPtr}, func() error {
				urlToIDMap[urlPtr] = idPtr
				newlyAddedURLs = append(newlyAddedURLs, urlPtr)
				return nil
			}); err != nil {
				return fmt.Errorf("failed to iterate newly inserted URL rows: %w", err)
			}
		}

		// Step 3d: Insert edges (existing logic)
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
				slog.Any("input", map[string]interface{}{"writer_type": "link_writer", "candidates": len(allCandidateLinks), "new_urls_added": len(newlyAddedURLs)}),
			),
		)
	}

	// Step 4: Update the Bloom Filter with newly added URLs
	if len(newlyAddedURLs) > 0 {
		// Interface slice conversion
		newlyAddedInterfaces := make([]interface{}, len(newlyAddedURLs))
		for i, v := range newlyAddedURLs {
			newlyAddedInterfaces[i] = v
		}

		if _, err := s.RedisClient.BFMAdd(ctx, s.cfg.BloomFilterKey, newlyAddedInterfaces...).Result(); err != nil {
			slog.Error("Failed to add new URLs to Bloom filter", "error", err, "count", len(newlyAddedURLs))
		} else {
			slog.Debug("Added new URLs to Bloom filter", "count", len(newlyAddedURLs))
		}
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

// =================================================================================
// NEW: RehydrateBloomFilter for the reaper to build the filter on startup
// =================================================================================
func (s *Storage) RehydrateBloomFilter(ctx context.Context) error {
	slog.Info("Checking state of Bloom filter...")
	exists, err := s.RedisClient.Exists(ctx, s.cfg.BloomFilterKey).Result()
	if err != nil {
		return fmt.Errorf("failed to check if bloom filter exists: %w", err)
	}
	if exists == 1 {
		slog.Info("Bloom filter already exists. Skipping rehydration.")
		return nil
	}

	slog.Warn("Bloom filter not found. Starting rehydration process. This may take a while...")

	// Create (Reserve) the filter first. This is a critical step.
	_, err = s.RedisClient.BFReserve(ctx, s.cfg.BloomFilterKey, s.cfg.BloomFilterErrorRate, s.cfg.BloomFilterCapacity).Result()
	if err != nil {
		// It might error if another process created it in the meantime.
		if !strings.Contains(strings.ToLower(err.Error()), "item exists") {
			return fmt.Errorf("failed to reserve bloom filter in redis: %w", err)
		}
		slog.Info("Bloom filter was created by another process. Skipping rehydration.")
		return nil
	}

	var totalAdded int64
	batchSize := 50000
	offset := 0

	for {
		select {
		case <-ctx.Done():
			return errors.New("bloom filter rehydration cancelled")
		default:
			slog.Info("Fetching URL batch from database for rehydration...", "offset", offset, "batch_size", batchSize)
			rows, err := s.DB.Query(ctx, `SELECT url FROM urls LIMIT $1 OFFSET $2`, batchSize, offset)
			if err != nil {
				return fmt.Errorf("rehydration failed: could not query urls table: %w", err)
			}

			var urlBatch []interface{}
			var url string
			if _, err := pgx.ForEachRow(rows, []any{&url}, func() error {
				urlBatch = append(urlBatch, url)
				return nil
			}); err != nil {
				return fmt.Errorf("rehydration failed: could not scan url row: %w", err)
			}

			if len(urlBatch) == 0 {
				slog.Info("Bloom filter rehydration complete.", "total_urls_added", totalAdded)
				return nil // No more rows
			}

			// Add the batch to the filter
			if _, err := s.RedisClient.BFMAdd(ctx, s.cfg.BloomFilterKey, urlBatch...).Result(); err != nil {
				return fmt.Errorf("rehydration failed: could not add batch to filter: %w", err)
			}

			totalAdded += int64(len(urlBatch))
			slog.Info("Added batch to Bloom filter", "batch_size", len(urlBatch), "total_added", totalAdded)
			offset += batchSize
		}
	}
}
