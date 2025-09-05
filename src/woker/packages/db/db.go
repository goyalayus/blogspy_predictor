// FILE: src/woker/packages/db/db.go

// Package db
package db

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"
	"worker/packages/config"
	"worker/packages/domain"
	"worker/packages/generated"
	"worker/packages/metrics"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

const (
	pendingURLCountLimit = 50000
	pendingCounterName   = "pending_urls_count"
	netlocCountsKey      = "blogspy:netloc_counts"
)

type Storage struct {
	DB                 *pgxpool.Pool
	Queries            *generated.Queries
	RedisClient        *redis.Client
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
		RedisClient:        redisClient,
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

func (s *Storage) GetNetlocClassificationStatus(ctx context.Context, netloc string) (int, error) {
	result, err := s.RedisClient.HGet(ctx, netlocCountsKey, netloc).Result()
	if err != nil {
		if err == redis.Nil {
			return 0, nil // Not found is an unclassified state, not an error.
		}
		return 0, err // A real error occurred.
	}

	status, err := strconv.Atoi(result)
	if err != nil {
		slog.Error("Could not parse netloc count from Redis, treating as unclassified", "netloc", netloc, "value", result)
		return 0, nil
	}

	return status, nil
}

func (s *Storage) SetNetlocClassificationStatus(ctx context.Context, netloc string, status int) error {
	return s.RedisClient.HSet(ctx, netlocCountsKey, netloc, status).Err()
}

func (s *Storage) IncrementNetlocCount(ctx context.Context, netloc string, count int) error {
	return s.RedisClient.HIncrBy(ctx, netlocCountsKey, netloc, int64(count)).Err()
}

func (s *Storage) GetTotalURLCount(ctx context.Context) (int64, error) {
	start := time.Now()
	defer func() {
		metrics.DBQueryDuration.WithLabelValues("GetTotalURLCount").Observe(time.Since(start).Seconds())
	}()
	return s.Queries.GetTotalURLCount(ctx)
}

func (s *Storage) RefreshTotalURLCount(ctx context.Context) error {
	count, err := s.GetTotalURLCount(ctx)
	if err != nil {
		slog.Error("Failed to get total URL count from DB", "error", err)
		return err
	}
	metrics.TotalURLs.Set(float64(count))
	slog.Info("Refreshed total URL count metric", "count", count)
	return nil
}

func (s *Storage) RehydrateNetlocCounts(ctx context.Context) error {
	slog.Info("Checking state of netloc counts in Redis...")
	exists, err := s.RedisClient.Exists(ctx, netlocCountsKey).Result()
	if err != nil {
		return fmt.Errorf("failed to check if netloc counts key exists: %w", err)
	}
	if exists == 1 {
		slog.Info("Netloc counts key already exists in Redis. Skipping rehydration.")
		return nil
	}

	slog.Warn("Netloc counts not found in Redis. Starting rehydration from PostgreSQL with CORRECTED logic.")
	start := time.Now()

    // --- THIS IS THE CORRECTED QUERY ---
	const correctedQuery = `
		SELECT
			netloc,
			CASE
				WHEN bool_or(status IN ('completed', 'pending_crawl')) THEN 1
				WHEN bool_and(status IN ('irrelevant', 'failed')) THEN -1
				ELSE 0
			END as classification_status
		FROM urls
		GROUP BY netloc
	`
	rows, err := s.DB.Query(ctx, correctedQuery)
	if err != nil {
		return fmt.Errorf("rehydration failed: could not query urls table: %w", err)
	}
	defer rows.Close()

	pipe := s.RedisClient.Pipeline()
	var rowsScanned int
	for rows.Next() {
		var netloc string
		var status int // Changed from count to status
		if err := rows.Scan(&netloc, &status); err != nil {
			return fmt.Errorf("rehydration failed: could not scan row: %w", err)
		}
        // Only set non-zero statuses to keep the cache clean.
        if status != 0 {
		    pipe.HSet(ctx, netlocCountsKey, netloc, status)
        }
		rowsScanned++
		if rowsScanned%10000 == 0 {
			if _, err := pipe.Exec(ctx); err != nil {
				return fmt.Errorf("rehydration failed: could not exec redis pipeline: %w", err)
			}
		}
	}
	if rows.Err() != nil {
		return fmt.Errorf("rehydration failed: error during row iteration: %w", rows.Err())
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("rehydration failed: could not exec final redis pipeline: %w", err)
	}

	duration := time.Since(start)
	slog.Info("Netloc classification decision rehydration complete.", "duration", duration, "netlocs_processed", rowsScanned)
	return nil
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

func (s *Storage) ResetStalledJobs(ctx context.Context) error {
	start := time.Now()
	interval := pgtype.Interval{
		Microseconds: s.cfg.JobTimeout.Microseconds(),
		Valid:        true,
	}
	// --- MODIFICATION: Capture rows affected and increment metric ---
	rowsAffected, err := s.Queries.ResetStalledJobs(ctx, interval)
	if err == nil {
		metrics.ReaperJobsResetTotal.WithLabelValues("stalled").Add(float64(rowsAffected))
	}
	duration := time.Since(start)
	metrics.DBQueryDuration.WithLabelValues("ResetStalledJobs").Observe(duration.Seconds())

	if err != nil {
		slog.Error("Stalled job reset failed",
			"event", slog.GroupValue(slog.String("name", "STALLED_JOB_RESET_COMPLETED"), slog.String("stage", "end"), slog.Float64("duration_ms", float64(duration.Microseconds())/1000.0)),
			"details", slog.GroupValue(slog.Any("error", err.Error())),
		)
		return err
	}
	slog.Info("Stalled job reset completed",
		"event", slog.GroupValue(slog.String("name", "STALLED_JOB_RESET_COMPLETED"), slog.String("stage", "end"), slog.Float64("duration_ms", float64(duration.Microseconds())/1000.0)),
		"details", slog.GroupValue(
			slog.Any("output", map[string]any{"reset_count": rowsAffected}),
		),
	)
	return nil
}

func (s *Storage) GetPendingURLCount(ctx context.Context) (int64, error) {
	start := time.Now()
	defer func() {
		metrics.DBQueryDuration.WithLabelValues("GetPendingURLCount").Observe(time.Since(start).Seconds())
	}()
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
	metrics.DBQueryDuration.WithLabelValues("RefreshPendingURLCount").Observe(duration.Seconds())

	if err != nil {
		return fmt.Errorf("failed to update counter value: %w", err)
	}

	slog.Info("Refreshed pending URL count",
		"event", slog.GroupValue(slog.String("name", "PENDING_URL_COUNT_REFRESHED"), slog.String("stage", "end"), slog.Float64("duration_ms", float64(duration.Microseconds())/1000.0)),
		"details", slog.GroupValue(
			slog.Any("output", map[string]any{"count": count}),
		),
	)
	return nil
}

func (s *Storage) processLinkBatches(ctx context.Context, batches []domain.LinkBatch) {
	start := time.Now()
	defer func() {
		metrics.DBQueryDuration.WithLabelValues("processLinkBatches").Observe(time.Since(start).Seconds())
	}()
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

	candidateURLInterfaces := make([]any, len(candidateURLStrings))
	for i, v := range candidateURLStrings {
		candidateURLInterfaces[i] = v
	}

	redisStart := time.Now()
	existsResult, err := s.RedisClient.BFMExists(ctx, s.cfg.BloomFilterKey, candidateURLInterfaces...).Result()
	metrics.DependencyCallDurationSeconds.WithLabelValues("redis_bloom_exists").Observe(time.Since(redisStart).Seconds())

	if err != nil {
		slog.Error("Bloom filter check failed, falling back to full DB check.", "error", err)
	}

	var urlsToCheckInDB []string
	definitelyNewLinks := make(map[string]domain.NewLink)
	if existsResult != nil {
		for i, exists := range existsResult {
			urlStr := candidateURLStrings[i]
			if exists {
				urlsToCheckInDB = append(urlsToCheckInDB, urlStr)
				metrics.BloomFilterChecksTotal.WithLabelValues("hit").Inc()
			} else {
				definitelyNewLinks[urlStr] = allCandidateLinks[urlStr]
				metrics.BloomFilterChecksTotal.WithLabelValues("miss").Inc()
			}
		}
	} else {
		urlsToCheckInDB = candidateURLStrings
	}
	slog.Debug("Bloom filter check complete", "candidates", len(candidateURLStrings), "hits", len(urlsToCheckInDB), "misses", len(definitelyNewLinks))

	var newlyAddedURLs []string
	err = s.WithTransaction(ctx, func(qtx *generated.Queries, tx pgx.Tx) error {
		var currentPendingCount int64
		err := tx.QueryRow(ctx, "SELECT value FROM system_counters WHERE counter_name = $1 FOR UPDATE", pendingCounterName).Scan(&currentPendingCount)
		if err != nil {
			return fmt.Errorf("failed to get and lock pending URL count: %w", err)
		}

		if currentPendingCount >= pendingURLCountLimit {
			slog.Warn("Link Writer: Throttling link ingestion, pending queue is full inside transaction", "count", currentPendingCount, "limit", pendingURLCountLimit)
			return nil // Exit the transaction gracefully, inserting nothing.
		}

		urlToIDMap := make(map[string]int64)
		if len(urlsToCheckInDB) > 0 {
			existingRows, err := tx.Query(ctx, `SELECT id, url FROM urls WHERE url = ANY($1)`, urlsToCheckInDB)
			if err != nil {
				return fmt.Errorf("failed to query for existing URLs: %w", err)
			}
			defer existingRows.Close()
			var idPtr int64
			var urlPtr string
			if _, err := pgx.ForEachRow(existingRows, []any{&idPtr, &urlPtr}, func() error {
				urlToIDMap[urlPtr] = idPtr
				return nil
			}); err != nil {
				return fmt.Errorf("failed to iterate existing URL rows: %w", err)
			}
		}

		// We do this *after* querying the DB for ground truth.
		for _, urlThatWasAHit := range urlsToCheckInDB {
			if _, actuallyExists := urlToIDMap[urlThatWasAHit]; !actuallyExists {
				// The filter said "hit", but the DB says it doesn't exist. This is a false positive.
				metrics.BloomFilterChecksTotal.WithLabelValues("false_positive").Inc()
			}
		}

		var newURLsToInsert []domain.NewLink
		for _, link := range definitelyNewLinks {
			newURLsToInsert = append(newURLsToInsert, link)
		}
		for _, urlStr := range urlsToCheckInDB {
			if _, exists := urlToIDMap[urlStr]; !exists {
				newURLsToInsert = append(newURLsToInsert, allCandidateLinks[urlStr])
			}
		}

		// Enforce a hard cap on pending URLs based on the locked counter.
		availableSlots := pendingURLCountLimit - currentPendingCount
		if int64(len(newURLsToInsert)) > availableSlots {
			slog.Warn("Trimming new URL batch to respect pending URL limit",
				"original_size", len(newURLsToInsert),
				"trimmed_size", availableSlots,
				"limit", pendingURLCountLimit,
			)
			newURLsToInsert = newURLsToInsert[:availableSlots]
		}

		if len(newURLsToInsert) > 0 {
			sql := "INSERT INTO urls (url, netloc, status) VALUES "
			var args []any
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
			defer insertedRows.Close()

			var idPtr int64
			var urlPtr string
			if _, err := pgx.ForEachRow(insertedRows, []any{&idPtr, &urlPtr}, func() error {
				urlToIDMap[urlPtr] = idPtr
				newlyAddedURLs = append(newlyAddedURLs, urlPtr)
				return nil
			}); err != nil {
				return fmt.Errorf("failed to iterate newly inserted URL rows: %w", err)
			}

			// Atomically update the counter with the number of URLs we just added.
			updateCounterSQL := `
				UPDATE system_counters 
				SET value = value + $1 
				WHERE counter_name = $2
			`
			if _, err := tx.Exec(ctx, updateCounterSQL, len(newlyAddedURLs), pendingCounterName); err != nil {
				return fmt.Errorf("failed to increment pending URL counter: %w", err)
			}
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
				slog.Any("input", map[string]any{"writer_type": "link_writer", "batch_size": len(allCandidateLinks)}),
				slog.Any("output", map[string]any{"error": err.Error()}),
			),
		)
	} else {
		slog.Info("DB batch write completed",
			"event", slog.GroupValue(slog.String("name", "DB_BATCH_WRITE_COMPLETED"), slog.String("stage", "end"), slog.Float64("duration_ms", float64(duration.Microseconds())/1000.0)),
			"details", slog.GroupValue(
				slog.Any("input", map[string]any{"writer_type": "link_writer", "candidates": len(allCandidateLinks), "new_urls_added": len(newlyAddedURLs)}),
			),
		)
	}

	if len(newlyAddedURLs) > 0 {
		newlyAddedInterfaces := make([]any, len(newlyAddedURLs))
		for i, v := range newlyAddedURLs {
			newlyAddedInterfaces[i] = v
		}
		redisAddStart := time.Now()
		_, err := s.RedisClient.BFMAdd(ctx, s.cfg.BloomFilterKey, newlyAddedInterfaces...).Result()
		metrics.DependencyCallDurationSeconds.WithLabelValues("redis_bloom_add").Observe(time.Since(redisAddStart).Seconds())

		if err != nil {
			slog.Error("Failed to add new URLs to Bloom filter", "error", err, "count", len(newlyAddedURLs))
		} else {
			slog.Debug("Added new URLs to Bloom filter", "count", len(newlyAddedURLs))
		}
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
func (s *Storage) LockJobs(ctx context.Context, fromStatus, toStatus generated.CrawlStatus, limit int32) ([]generated.LockJobsForUpdateRow, error) {
	var jobs []generated.LockJobsForUpdateRow

	err := s.WithTransaction(ctx, func(qtx *generated.Queries, tx pgx.Tx) error {
		var err error
		start := time.Now()
		jobs, err = qtx.LockJobsForUpdate(ctx, generated.LockJobsForUpdateParams{
			Status: fromStatus,
			Limit:  limit,
		})
		duration := time.Since(start)
		metrics.DBQueryDuration.WithLabelValues("LockJobsForUpdate").Observe(duration.Seconds())
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

	_, err = s.RedisClient.BFReserve(ctx, s.cfg.BloomFilterKey, s.cfg.BloomFilterErrorRate, s.cfg.BloomFilterCapacity).Result()
	if err != nil {
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
			dbReadStart := time.Now()
			rows, err := s.DB.Query(ctx, `SELECT url FROM urls LIMIT $1 OFFSET $2`, batchSize, offset)
			metrics.DBQueryDuration.WithLabelValues("RehydrateBloomFilter_ReadBatch").Observe(time.Since(dbReadStart).Seconds())

			if err != nil {
				return fmt.Errorf("rehydration failed: could not query urls table: %w", err)
			}

			var urlBatch []any
			var url string
			if _, err := pgx.ForEachRow(rows, []any{&url}, func() error {
				urlBatch = append(urlBatch, url)
				return nil
			}); err != nil {
				return fmt.Errorf("rehydration failed: could not scan url row: %w", err)
			}

			if len(urlBatch) == 0 {
				slog.Info("Bloom filter rehydration complete.", "total_urls_added", totalAdded)
				return nil
			}
			redisAddStart := time.Now()
			_, err = s.RedisClient.BFMAdd(ctx, s.cfg.BloomFilterKey, urlBatch...).Result()
			metrics.DependencyCallDurationSeconds.WithLabelValues("redis_bloom_add").Observe(time.Since(redisAddStart).Seconds())

			if err != nil {
				return fmt.Errorf("rehydration failed: could not add batch to filter: %w", err)
			}

			totalAdded += int64(len(urlBatch))
			slog.Info("Added batch to Bloom filter", "batch_size", len(urlBatch), "total_added", totalAdded)
			offset += batchSize
		}
	}
}
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
	defer func() {
		metrics.DBQueryDuration.WithLabelValues("processStatusUpdates").Observe(time.Since(start).Seconds())
	}()

	var statusSQL, errorMsgSQL, renderingSQL strings.Builder
	var args []any
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
				slog.Any("input", map[string]any{"writer_type": "status_writer", "batch_size": len(batch)}),
				slog.Any("output", map[string]any{"error": err.Error()}),
			),
		)
	} else {
		slog.Info("DB batch write completed",
			"event", slog.GroupValue(slog.String("name", "DB_BATCH_WRITE_COMPLETED"), slog.String("stage", "end"), slog.Float64("duration_ms", float64(duration.Microseconds())/1000.0)),
			"details", slog.GroupValue(
				slog.Any("input", map[string]any{"writer_type": "status_writer", "batch_size": len(batch)}),
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
	defer func() {
		metrics.DBQueryDuration.WithLabelValues("processContentInserts").Observe(time.Since(start).Seconds())
	}()

	err := s.WithTransaction(ctx, func(qtx *generated.Queries, tx pgx.Tx) error {
		var ids []int64
		for _, item := range batch {
			ids = append(ids, item.ID)
		}
		updateSQL := `UPDATE urls SET status = $1, processed_at = NOW() WHERE id = ANY($2)`
		if _, err := tx.Exec(ctx, updateSQL, generated.CrawlStatusCompleted, ids); err != nil {
			return fmt.Errorf("failed to batch update urls to completed: %w", err)
		}

		rows := make([][]any, len(batch))
		for i, item := range batch {
			rows[i] = []any{
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
				slog.Any("input", map[string]any{"writer_type": "content_writer", "batch_size": len(batch)}),
				slog.Any("output", map[string]any{"error": err.Error()}),
			),
		)
	} else {
		slog.Info("DB batch write completed",
			"event", slog.GroupValue(slog.String("name", "DB_BATCH_WRITE_COMPLETED"), slog.String("stage", "end"), slog.Float64("duration_ms", float64(duration.Microseconds())/1000.0)),
			"details", slog.GroupValue(
				slog.Any("input", map[string]any{"writer_type": "content_writer", "batch_size": len(batch)}),
			),
		)
	}
}
