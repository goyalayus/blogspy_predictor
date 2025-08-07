package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/pgvector/pgvector-go"
	pgxvec "github.com/pgvector/pgvector-go/pgx"
	"golang.org/x/sync/errgroup"
)

const (
	GeminiAPIURL         = "https://generativelanguage.googleapis.com/v1beta/models/text-embedding-004:batchEmbedContents"
	GeminiModel          = "models/text-embedding-004"
	TaskType             = "RETRIEVAL_DOCUMENT"
	OutputDimensionality = 256
	MaxContentLength     = 1500
	DBBatchSize          = 500
	APIBatchSize         = 100
	ConcurrencyLimit     = 8
)

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

type DBRecord struct {
	URLID       int64
	Title       pgtype.Text
	Description pgtype.Text
	Content     pgtype.Text
}

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

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)
	if err := godotenv.Load("../../../../.env"); err != nil {
		slog.Info("Could not load .env file from project root", "error", err)
	}

	databaseURL := os.Getenv("DATABASE_URL")
	geminiAPIKey := os.Getenv("GEMINI_API_KEY")
	if databaseURL == "" || geminiAPIKey == "" {
		slog.Error("FATAL: DATABASE_URL and GEMINI_API_KEY must be set.")
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	dbConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		slog.Error("Failed to parse database URL", "error", err)
		os.Exit(1)
	}
	dbConfig.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		return pgxvec.RegisterTypes(ctx, conn)
	}
	dbpool, err := pgxpool.NewWithConfig(ctx, dbConfig)
	if err != nil {
		slog.Error("Failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer dbpool.Close()

	embedder := NewEmbedder(dbpool, geminiAPIKey)
	slog.Info("--- Starting Embedding Backfill Process ---")
	if err := embedder.Run(ctx); err != nil {
		slog.Error("Backfill process failed", "error", err)
		os.Exit(1)
	}
	slog.Info("--- Embedding Backfill Process Completed Successfully ---")
}

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
				return nil
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

func (e *Embedder) processDBBatch(ctx context.Context, records []DBRecord) error {
	results := new(sync.Map)
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(ConcurrencyLimit)

	for i := 0; i < len(records); i += APIBatchSize {
		end := i + APIBatchSize
		end = min(end, len(records))
		chunk := records[i:end]

		g.Go(func() error {
			embeddings, err := e.getEmbeddingsForChunk(gCtx, chunk)
			if err != nil {
				return fmt.Errorf("failed to get embeddings for chunk: %w", err)
			}
			if len(embeddings) != len(chunk) {
				return fmt.Errorf("mismatch in embedding count: got %d, want %d", len(embeddings), len(chunk))
			}

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

func (e *Embedder) updateDBWithEmbeddings(ctx context.Context, results *sync.Map) error {
	tx, err := e.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	updateQuery := `UPDATE url_content SET embedding = $1 WHERE url_id = $2`
	var updateCount int

	var errs []error
	results.Range(func(key, value any) bool {
		urlID := key.(int64)
		embedding := value.([]float32)

		_, err := tx.Exec(ctx, updateQuery, pgvector.NewVector(embedding), urlID)
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to update url_id %d: %w", urlID, err))
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

func normalize(v []float32) []float32 {
	var sumOfSquares float64
	for _, val := range v {
		sumOfSquares += float64(val * val)
	}
	norm := float32(math.Sqrt(sumOfSquares))

	if norm == 0 {
		return v
	}

	normalized := make([]float32, len(v))
	for i, val := range v {
		normalized[i] = val / norm
	}
	return normalized
}
