package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	pgvector "github.com/vladimir-ch/go-pgvector/pgvector"
	"golang.org/x/sync/errgroup"
)

// --- Configuration ---

const (
	// GeminiAPIURL is the endpoint for batch embedding generation.
	GeminiAPIURL = "https://generativelanguage.googleapis.com/v1beta/models/text-embedding-004:batchEmbedContents"
	// GeminiModel is the specific model to use for embedding.
	GeminiModel = "models/text-embedding-004"

	// TaskType instructs the model to optimize embeddings for document retrieval.
	TaskType = "RETRIEVAL_DOCUMENT"

	// OutputDimensionality is the desired size of the embedding vector.
	OutputDimensionality = 256

	// MaxContentLength is the number of characters to take from the main content.
	MaxContentLength = 1500

	// DBBatchSize is the number of rows to fetch from PostgreSQL in one go.
	DBBatchSize = 500
	// APIBatchSize is the number of texts to send to the Gemini API in a single request.
	// Google's API has a limit, 100 is a safe and effective number.
	APIBatchSize = 100
	// ConcurrencyLimit limits how many parallel requests are sent to the Gemini API.
	ConcurrencyLimit = 8
)

// --- API Request and Response Structures ---

type GeminiRequest struct {
	Requests []EmbedRequest `json:"requests"`
}

type EmbedRequest struct {
	Model   string  `json:"model"`
	Content Content `json:"content"`
	Task    string  `json:"task_type"`
	Output  int     `json:"output_dimensionality"`
}

type Content struct {
	Parts []Part `json:"parts"`
}

type Part struct {
	Text string `json:"text"`
}

type GeminiResponse struct {
	Embeddings []Embedding `json:"embeddings"`
}

type Embedding struct {
	Values []float32 `json:"values"`
}

// --- Database and Worker Structures ---

// DBRecord holds the data fetched from the url_content table.
type DBRecord struct {
	URLID       int64
	Title       pgtype.Text
	Description pgtype.Text
	Content     pgtype.Text
}

// Embedder holds the necessary clients and configuration for the backfill process.
type Embedder struct {
	db     *pgxpool.Pool
	client *http.Client
	apiKey string
}

func NewEmbedder(db *pgxpool.Pool, apiKey string) *Embedder {
	return &Embedder{
		db:     db,
		client: &http.Client{Timeout: 60 * time.Second},
		apiKey: apiKey,
	}
}

// --- Main Execution Logic ---

func main() {
	// 1. Setup structured logging.
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	// 2. Load environment variables from a .env file (if it exists).
	if err := godotenv.Load(); err != nil {
		slog.Info("No .env file found, relying on system environment variables.")
	}

	// 3. Get required configuration from the environment.
	databaseURL := os.Getenv("DATABASE_URL")
	geminiAPIKey := os.Getenv("GEMINI_API_KEY")
	if databaseURL == "" || geminiAPIKey == "" {
		slog.Error("FATAL: DATABASE_URL and GEMINI_API_KEY must be set.")
		os.Exit(1)
	}

	// 4. Create a cancellable context for graceful shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// 5. Connect to the PostgreSQL database with pgvector support.
	dbConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		slog.Error("Failed to parse database URL", "error", err)
		os.Exit(1)
	}
	dbConfig.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		return pgvector.Register(conn.TypeMap())
	}
	dbpool, err := pgxpool.NewWithConfig(ctx, dbConfig)
	if err != nil {
		slog.Error("Failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer dbpool.Close()

	// 6. Run the backfill process.
	embedder := NewEmbedder(dbpool, geminiAPIKey)
	slog.Info("--- Starting Embedding Backfill Process ---")
	if err := embedder.Run(ctx); err != nil {
		slog.Error("Backfill process failed", "error", err)
		os.Exit(1)
	}
	slog.Info("--- Embedding Backfill Process Completed Successfully ---")
}

// Run orchestrates the entire backfill process, fetching and processing in batches.
func (e *Embedder) Run(ctx context.Context) error {
	var offset int
	var totalProcessed int
	for {
		select {
		case <-ctx.Done():
			return errors.New("process cancelled by user")
		default:
			slog.Info("Fetching next batch from database", "limit", DBBatchSize, "offset", offset)
			records, err := e.fetchBatchFromDB(ctx, DBBatchSize, offset)
			if err != nil {
				return fmt.Errorf("failed to fetch from database: %w", err)
			}
			if len(records) == 0 {
				slog.Info("No more records to process.")
				return nil // We're done.
			}

			err = e.processDBBatch(ctx, records)
			if err != nil {
				return fmt.Errorf("failed to process database batch: %w", err)
			}

			totalProcessed += len(records)
			slog.Info("Successfully processed a batch", "batch_size", len(records), "total_processed", totalProcessed)
			offset += DBBatchSize
		}
	}
}

// fetchBatchFromDB retrieves a slice of records that need embedding.
func (e *Embedder) fetchBatchFromDB(ctx context.Context, limit, offset int) ([]DBRecord, error) {
	query := `
		SELECT url_id, title, description, content
		FROM url_content
		WHERE embedding IS NULL AND content IS NOT NULL AND content != ''
		ORDER BY url_id
		LIMIT $1 OFFSET $2
	`
	rows, err := e.db.Query(ctx, query, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []DBRecord
	for rows.Next() {
		var r DBRecord
		if err := rows.Scan(&r.URLID, &r.Title, &r.Description, &r.Content); err != nil {
			return nil, err
		}
		records = append(records, r)
	}
	return records, rows.Err()
}

// processDBBatch handles the embedding generation and database update for a set of records.
func (e *Embedder) processDBBatch(ctx context.Context, records []DBRecord) error {
	// This map will store the final results: url_id -> normalized_embedding.
	results := new(sync.Map)

	// Use an errgroup to manage concurrent API calls.
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(ConcurrencyLimit)

	// Chunk the database records into smaller batches suitable for the API.
	for i := 0; i < len(records); i += APIBatchSize {
		end := i + APIBatchSize
		if end > len(records) {
			end = len(records)
		}
		chunk := records[i:end]

		g.Go(func() error {
			// Get embeddings for the current chunk.
			embeddings, err := e.getEmbeddingsForChunk(gCtx, chunk)
			if err != nil {
				return fmt.Errorf("failed to get embeddings for chunk: %w", err)
			}
			if len(embeddings) != len(chunk) {
				return fmt.Errorf("mismatch in embedding count: got %d, want %d", len(embeddings), len(chunk))
			}

			// Normalize and store the results.
			for j, embedding := range embeddings {
				normalizedVector := normalize(embedding.Values)
				record := chunk[j]
				results.Store(record.URLID, normalizedVector)
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	return e.updateDBWithEmbeddings(ctx, results)
}

// getEmbeddingsForChunk sends a batch of texts to the Gemini API.
func (e *Embedder) getEmbeddingsForChunk(ctx context.Context, chunk []DBRecord) ([]Embedding, error) {
	apiRequests := make([]EmbedRequest, len(chunk))
	for i, record := range chunk {
		var builder strings.Builder
		if record.Title.Valid {
			builder.WriteString(record.Title.String)
			builder.WriteString("\n\n")
		}
		if record.Description.Valid {
			builder.WriteString(record.Description.String)
			builder.WriteString("\n\n")
		}
		if record.Content.Valid {
			content := record.Content.String
			if len(content) > MaxContentLength {
				content = content[:MaxContentLength]
			}
			builder.WriteString(content)
		}

		apiRequests[i] = EmbedRequest{
			Model:  GeminiModel,
			Task:   TaskType,
			Output: OutputDimensionality,
			Content: Content{
				Parts: []Part{{Text: builder.String()}},
			},
		}
	}

	reqBody, err := json.Marshal(GeminiRequest{Requests: apiRequests})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request body: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", GeminiAPIURL, bytes.NewBuffer(reqBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create http request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("x-goog-api-key", e.apiKey)

	resp, err := e.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("api request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var errBody bytes.Buffer
		io.Copy(&errBody, resp.Body)
		return nil, fmt.Errorf("api returned non-200 status: %d - %s", resp.StatusCode, errBody.String())
	}

	var apiResponse GeminiResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResponse); err != nil {
		return nil, fmt.Errorf("failed to decode api response: %w", err)
	}

	return apiResponse.Embeddings, nil
}

// updateDBWithEmbeddings writes the new embeddings to the database in a single transaction.
func (e *Embedder) updateDBWithEmbeddings(ctx context.Context, results *sync.Map) error {
	tx, err := e.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx) // Rollback is a no-op if the transaction is committed.

	updateQuery := `UPDATE url_content SET embedding = $1 WHERE url_id = $2`
	var updateCount int

	var errs []error
	results.Range(func(key, value interface{}) bool {
		urlID := key.(int64)
		embedding := value.([]float32)

		_, err := tx.Exec(ctx, updateQuery, pgvector.NewVector(embedding), urlID)
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to update url_id %d: %w", urlID, err))
			// Stop iterating on the first error.
			return false
		}
		updateCount++
		return true
	})

	if len(errs) > 0 {
		return fmt.Errorf("encountered %d errors during update, rolling back. First error: %w", len(errs), errs[0])
	}

	slog.Debug("Committing transaction", "update_count", updateCount)
	return tx.Commit(ctx)
}

// normalize converts a vector to a unit vector (magnitude of 1).
func normalize(v []float32) []float32 {
	var sumOfSquares float64
	for _, val := range v {
		sumOfSquares += float64(val * val)
	}
	norm := float32(math.Sqrt(sumOfSquares))

	if norm == 0 {
		return v // Cannot normalize a zero vector.
	}

	normalized := make([]float32, len(v))
	for i, val := range v {
		normalized[i] = val / norm
	}
	return normalized
}
