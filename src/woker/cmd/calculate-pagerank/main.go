package main

import (
	"context"
	"fmt"
	"log/slog"
	"math" // <-- NEW IMPORT
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

// --- PageRank Algorithm Configuration ---
const (
	DampingFactor   = 0.85
	MaxIterations   = 30
	BatchUpdateSize = 10000
)

// --- Main Application Logic ---

func main() {
	// 1. Setup structured logging and load environment variables.
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	slog.SetDefault(logger)

	// Assumes you've copied the .env file into the 'src/woker' directory
	if err := godotenv.Load(); err != nil {
		slog.Info("No .env file found in current directory, relying on system environment variables.")
	}

	// 2. Get database URL from environment.
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		slog.Error("FATAL: DATABASE_URL environment variable must be set.")
		os.Exit(1)
	}

	// 3. Create a cancellable context for graceful shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// 4. Connect to PostgreSQL.
	dbpool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		slog.Error("Failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer dbpool.Close()

	slog.Info("--- Starting PageRank Calculation Process ---")

	// Step 1/4: Fetch all data needed to build the graph.
	slog.Info("Step 1/4: Fetching URLs and edges from database to build graph...")
	graph, _, indexToUrlId, outDegrees, err := fetchGraphData(ctx, dbpool)
	if err != nil {
		slog.Error("Failed to fetch graph data", "error", err)
		os.Exit(1)
	}
	slog.Info("Finished fetching graph data", "total_urls", len(indexToUrlId), "total_edges", len(graph))

	// Step 2/4: Run the iterative PageRank algorithm to get RAW scores.
	slog.Info("Step 2/4: Calculating raw PageRank scores...")
	rawPagerankScores := calculatePageRank(graph, outDegrees, len(indexToUrlId))
	slog.Info("Finished calculating raw PageRank scores.")

	// --- NORMALIZATION LOGIC START ---

	// Step 3/4: Normalize the raw scores to a 0-1 range.
	slog.Info("Step 3/4: Normalizing scores using Min-Max scaling...")
	var minScore float64 = math.MaxFloat64
	var maxScore float64 = 0.0
	for _, score := range rawPagerankScores {
		if score < minScore {
			minScore = score
		}
		if score > maxScore {
			maxScore = score
		}
	}
	slog.Info("Calculated raw score stats", "min_score", minScore, "max_score", maxScore)

	normalizedUpdates := make(map[int64]float32)
	scoreRange := maxScore - minScore
	// Prevent division by zero if all scores are identical
	if scoreRange == 0 {
		scoreRange = 1.0
	}

	for i, rawScore := range rawPagerankScores {
		urlID := indexToUrlId[i]
		normalizedScore := (rawScore - minScore) / scoreRange
		normalizedUpdates[urlID] = float32(normalizedScore)
	}
	slog.Info("Score normalization complete.")

	// --- NORMALIZATION LOGIC END ---

	// Step 4/4: Update the database with the FINAL NORMALIZED scores.
	slog.Info("Step 4/4: Updating pagerank_score in the database with normalized values...")
	err = updatePageRankScoresInDB(ctx, dbpool, normalizedUpdates)
	if err != nil {
		slog.Error("Failed to update PageRank scores in DB", "error", err)
		os.Exit(1)
	}

	slog.Info("--- PageRank Calculation Process Completed Successfully ---")
}

// fetchGraphData remains unchanged
func fetchGraphData(ctx context.Context, db *pgxpool.Pool) (graph map[int][]int, urlIdToIndex map[int64]int, indexToUrlId []int64, outDegrees []int, err error) {
	rows, err := db.Query(ctx, `SELECT id FROM urls ORDER BY id`)
	if err != nil { return nil, nil, nil, nil, fmt.Errorf("failed to query url ids: %w", err) }
	defer rows.Close()
	var tempIds []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil { return nil, nil, nil, nil, fmt.Errorf("failed to scan url id: %w", err) }
		tempIds = append(tempIds, id)
	}
	numURLs := len(tempIds)
	urlIdToIndex = make(map[int64]int, numURLs)
	indexToUrlId = make([]int64, numURLs)
	for i, id := range tempIds {
		urlIdToIndex[id] = i
		indexToUrlId[i] = id
	}
	graph = make(map[int][]int)
	outDegrees = make([]int, numURLs)
	edgeRows, err := db.Query(ctx, `SELECT source_url_id, dest_url_id FROM url_edges`)
	if err != nil { return nil, nil, nil, nil, fmt.Errorf("failed to query url_edges: %w", err) }
	defer edgeRows.Close()
	for edgeRows.Next() {
		var sourceID, destID int64
		if err := edgeRows.Scan(&sourceID, &destID); err != nil {
			slog.Warn("Failed to scan an edge row, skipping", "error", err)
			continue
		}
		sourceIdx, okS := urlIdToIndex[sourceID]
		destIdx, okD := urlIdToIndex[destID]
		if okS && okD {
			graph[sourceIdx] = append(graph[sourceIdx], destIdx)
			outDegrees[sourceIdx]++
		}
	}
	return graph, urlIdToIndex, indexToUrlId, outDegrees, nil
}

// calculatePageRank remains unchanged
func calculatePageRank(graph map[int][]int, outDegrees []int, numURLs int) []float64 {
	initialScore := 1.0 / float64(numURLs)
	pagerank := make([]float64, numURLs)
	for i := range pagerank { pagerank[i] = initialScore }
	for i := 0; i < MaxIterations; i++ {
		slog.Debug("Running PageRank iteration", "iteration", i+1)
		newPagerank := make([]float64, numURLs)
		danglingSum := 0.0
		for j := 0; j < numURLs; j++ {
			if outDegrees[j] == 0 { danglingSum += pagerank[j] }
		}
		for j := 0; j < numURLs; j++ {
			newPagerank[j] = (1.0-DampingFactor)/float64(numURLs) + DampingFactor*danglingSum/float64(numURLs)
		}
		for sourceIdx, destIndices := range graph {
			if outDegrees[sourceIdx] > 0 {
				contribution := DampingFactor * pagerank[sourceIdx] / float64(outDegrees[sourceIdx])
				for _, destIdx := range destIndices {
					newPagerank[destIdx] += contribution
				}
			}
		}
		pagerank = newPagerank
	}
	return pagerank
}

// updatePageRankScoresInDB remains unchanged
func updatePageRankScoresInDB(ctx context.Context, db *pgxpool.Pool, updates map[int64]float32) error {
	tx, err := db.Begin(ctx)
	if err != nil { return fmt.Errorf("failed to begin transaction: %w", err) }
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, `CREATE TEMPORARY TABLE pagerank_updates (id BIGINT PRIMARY KEY, score REAL) ON COMMIT DROP;`)
	if err != nil { return fmt.Errorf("failed to create temporary table: %w", err) }
	var rows [][]interface{}
	totalUpdates := 0
	for id, score := range updates {
		rows = append(rows, []interface{}{id, score})
		if len(rows) >= BatchUpdateSize {
			slog.Debug("Copying batch of scores to temporary table", "count", len(rows))
			_, err = tx.CopyFrom(ctx, pgx.Identifier{"pagerank_updates"}, []string{"id", "score"}, pgx.CopyFromRows(rows))
			if err != nil { return fmt.Errorf("failed to copy data to temp table: %w", err) }
			totalUpdates += len(rows)
			rows = nil
		}
	}
	if len(rows) > 0 {
		slog.Debug("Copying final batch of scores to temporary table", "count", len(rows))
		_, err = tx.CopyFrom(ctx, pgx.Identifier{"pagerank_updates"}, []string{"id", "score"}, pgx.CopyFromRows(rows))
		if err != nil { return fmt.Errorf("failed to copy final batch to temp table: %w", err) }
		totalUpdates += len(rows)
	}
	slog.Info("Staged all PageRank scores for update", "count", totalUpdates)
	slog.Info("Executing final UPDATE command...")
	startTime := time.Now()
	cmdTag, err := tx.Exec(ctx, `UPDATE urls SET pagerank_score = u.score FROM pagerank_updates u WHERE urls.id = u.id;`)
	if err != nil { return fmt.Errorf("failed to execute update from temporary table: %w", err) }
	slog.Info("UPDATE command finished", "rows_affected", cmdTag.RowsAffected(), "duration", time.Since(startTime))
	if err := tx.Commit(ctx); err != nil { return fmt.Errorf("failed to commit transaction: %w", err) }
	return nil
}
