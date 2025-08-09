package main

import (
	"context"
	"fmt"
	"log/slog"
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
	// DampingFactor is the probability that a user will continue clicking links.
	// A standard value is 0.85.
	DampingFactor = 0.85
	// MaxIterations is the number of times to run the PageRank calculation.
	// 20-30 iterations are usually sufficient for convergence.
	MaxIterations = 30
	// BatchUpdateSize is the number of rows to send to the database in each
	// COPY operation when updating scores.
	BatchUpdateSize = 10000
)

// --- Main Application Logic ---

func main() {
	// 1. Setup structured logging and load environment variables.
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	slog.SetDefault(logger)

	// Assumes you run the script from the project root, e.g., `go run ./tools/calculate-pagerank/main.go`
	if err := godotenv.Load("../../../../.env"); err != nil {
		slog.Info("No .env file found, relying on system environment variables.")
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

	// 5. Fetch all data needed to build the graph.
	slog.Info("Step 1/4: Fetching URLs and edges from database to build graph...")
	// --- FIX: Replaced 'urlIdToIndex' with '_' to ignore the unused variable ---
	graph, _, indexToUrlId, outDegrees, err := fetchGraphData(ctx, dbpool)
	if err != nil {
		slog.Error("Failed to fetch graph data", "error", err)
		os.Exit(1)
	}
	slog.Info("Finished fetching graph data", "total_urls", len(indexToUrlId), "total_edges", len(graph))

	// 6. Run the iterative PageRank algorithm.
	slog.Info("Step 2/4: Calculating PageRank scores...")
	pagerankScores := calculatePageRank(graph, outDegrees, len(indexToUrlId))
	slog.Info("Finished calculating PageRank scores.")

	// 7. Map the calculated scores back to their URL IDs.
	slog.Info("Step 3/4: Mapping scores to URL IDs...")
	updates := make(map[int64]float32)
	for i, score := range pagerankScores {
		urlID := indexToUrlId[i]
		updates[urlID] = float32(score)
	}

	// 8. Update the database with the new scores.
	slog.Info("Step 4/4: Updating pagerank_score in the database...")
	err = updatePageRankScoresInDB(ctx, dbpool, updates)
	if err != nil {
		slog.Error("Failed to update PageRank scores in DB", "error", err)
		os.Exit(1)
	}

	slog.Info("--- PageRank Calculation Process Completed Successfully ---")
}

// fetchGraphData retrieves all URLs and their links from the database
// and builds in-memory data structures to represent the graph for calculation.
func fetchGraphData(ctx context.Context, db *pgxpool.Pool) (
	graph map[int][]int,
	urlIdToIndex map[int64]int,
	indexToUrlId []int64,
	outDegrees []int,
	err error,
) {
	// Fetch all URL IDs to create mappings between DB ID and slice index.
	rows, err := db.Query(ctx, `SELECT id FROM urls ORDER BY id`)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("failed to query url ids: %w", err)
	}
	defer rows.Close()

	var tempIds []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, nil, nil, nil, fmt.Errorf("failed to scan url id: %w", err)
		}
		tempIds = append(tempIds, id)
	}

	numURLs := len(tempIds)
	urlIdToIndex = make(map[int64]int, numURLs)
	indexToUrlId = make([]int64, numURLs)
	for i, id := range tempIds {
		urlIdToIndex[id] = i
		indexToUrlId[i] = id
	}

	// Initialize graph structures.
	graph = make(map[int][]int)
	outDegrees = make([]int, numURLs)

	// Fetch all edges and build the graph.
	edgeRows, err := db.Query(ctx, `SELECT source_url_id, dest_url_id FROM url_edges`)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("failed to query url_edges: %w", err)
	}
	defer edgeRows.Close()

	for edgeRows.Next() {
		var sourceID, destID int64
		if err := edgeRows.Scan(&sourceID, &destID); err != nil {
			slog.Warn("Failed to scan an edge row, skipping", "error", err)
			continue
		}

		sourceIdx, okS := urlIdToIndex[sourceID]
		destIdx, okD := urlIdToIndex[destID]

		// Only add edges where both source and destination URLs exist in our map.
		if okS && okD {
			graph[sourceIdx] = append(graph[sourceIdx], destIdx)
			outDegrees[sourceIdx]++
		}
	}

	return graph, urlIdToIndex, indexToUrlId, outDegrees, nil
}

// calculatePageRank performs the iterative PageRank calculation.
func calculatePageRank(graph map[int][]int, outDegrees []int, numURLs int) []float64 {
	// Initialize scores uniformly.
	initialScore := 1.0 / float64(numURLs)
	pagerank := make([]float64, numURLs)
	for i := range pagerank {
		pagerank[i] = initialScore
	}

	for i := 0; i < MaxIterations; i++ {
		slog.Debug("Running PageRank iteration", "iteration", i+1)
		newPagerank := make([]float64, numURLs)
		danglingSum := 0.0

		// Calculate contribution from dangling nodes (pages with no outgoing links).
		for j := 0; j < numURLs; j++ {
			if outDegrees[j] == 0 {
				danglingSum += pagerank[j]
			}
		}

		// Distribute PageRank.
		for j := 0; j < numURLs; j++ {
			// Base score + distribution from dangling nodes.
			newPagerank[j] = (1.0-DampingFactor)/float64(numURLs) + DampingFactor*danglingSum/float64(numURLs)
		}

		// Add contribution from inbound links.
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

// updatePageRankScoresInDB updates the `urls` table with the calculated scores
// using a highly efficient temporary table and COPY FROM strategy.
func updatePageRankScoresInDB(ctx context.Context, db *pgxpool.Pool, updates map[int64]float32) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx) // Rollback is a no-op if committed.

	// 1. Create a temporary table to hold the updates. It will be dropped automatically on commit.
	_, err = tx.Exec(ctx, `CREATE TEMPORARY TABLE pagerank_updates (id BIGINT PRIMARY KEY, score REAL) ON COMMIT DROP;`)
	if err != nil {
		return fmt.Errorf("failed to create temporary table: %w", err)
	}

	// 2. Stream the updates into the temporary table using COPY protocol for high performance.
	var rows [][]interface{}
	totalUpdates := 0
	for id, score := range updates {
		rows = append(rows, []interface{}{id, score})
		if len(rows) >= BatchUpdateSize {
			slog.Debug("Copying batch of scores to temporary table", "count", len(rows))
			_, err = tx.CopyFrom(ctx, pgx.Identifier{"pagerank_updates"}, []string{"id", "score"}, pgx.CopyFromRows(rows))
			if err != nil {
				return fmt.Errorf("failed to copy data to temp table: %w", err)
			}
			totalUpdates += len(rows)
			rows = nil // Reset the batch
		}
	}
	// Copy any remaining rows.
	if len(rows) > 0 {
		slog.Debug("Copying final batch of scores to temporary table", "count", len(rows))
		_, err = tx.CopyFrom(ctx, pgx.Identifier{"pagerank_updates"}, []string{"id", "score"}, pgx.CopyFromRows(rows))
		if err != nil {
			return fmt.Errorf("failed to copy final batch to temp table: %w", err)
		}
		totalUpdates += len(rows)
	}

	slog.Info("Staged all PageRank scores for update", "count", totalUpdates)

	// 3. Perform the actual update by joining the main table with the temporary table.
	slog.Info("Executing final UPDATE command...")
	startTime := time.Now()
	cmdTag, err := tx.Exec(ctx, `UPDATE urls SET pagerank_score = u.score FROM pagerank_updates u WHERE urls.id = u.id;`)
	if err != nil {
		return fmt.Errorf("failed to execute update from temporary table: %w", err)
	}

	slog.Info("UPDATE command finished", "rows_affected", cmdTag.RowsAffected(), "duration", time.Since(startTime))

	// 4. Commit the transaction.
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}
