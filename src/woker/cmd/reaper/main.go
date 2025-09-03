// FILE: src/woker/cmd/reaper/main.go
// MODIFIED: The 'refreshContentURLCount' function now uses the fast PostgreSQL
// statistics estimator (reltuples) instead of a slow COUNT(*) to avoid database load.

package main

import (
	"context"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
	"worker/packages/config"
	"worker/packages/db"
	"worker/packages/metrics"

	"gopkg.in/natefinch/lumberjack.v2"
)

func setupLogger(cfg config.Config) {
	var level slog.Level
	switch strings.ToLower(cfg.LogLevel) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	logDir := filepath.Dir(cfg.LogFile)
	if err := os.MkdirAll(logDir, 0750); err != nil {
		slog.New(slog.NewJSONHandler(os.Stderr, nil)).Error(
			"Failed to create log directory", "path", logDir, "error", err,
		)
	}

	logRotator := &lumberjack.Logger{
		Filename:   cfg.LogFile,
		MaxSize:    5,
		MaxBackups: 3,
		MaxAge:     30,
		Compress:   true,
	}

	multiWriter := io.MultiWriter(os.Stdout, logRotator)

	handler := slog.NewJSONHandler(multiWriter, &slog.HandlerOptions{
		Level: level,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				a.Value = slog.StringValue(a.Value.Time().Format(time.RFC3339Nano))
			}
			return a
		},
	}).WithAttrs([]slog.Attr{slog.String("service", "go-reaper")})

	logger := slog.New(handler)
	slog.SetDefault(logger)
}

// --- MODIFIED FUNCTION TO USE FAST ESTIMATOR ---
func refreshContentURLCount(ctx context.Context, storage *db.Storage) {
	// Use the fast PostgreSQL estimator instead of a slow COUNT(*).
	// This provides a nearly instantaneous, but approximate, row count suitable for a gauge.
	var estimatedCount int64
	query := `SELECT reltuples::bigint FROM pg_class WHERE relname = 'url_content'`
	err := storage.DB.QueryRow(ctx, query).Scan(&estimatedCount)

	if err != nil {
		slog.Error("Failed to get estimated content URL count from pg_class", "error", err)
		return
	}
	metrics.URLsInContentTableTotal.Set(float64(estimatedCount))
	// Update log to clarify it's an estimate.
	slog.Info("Refreshed estimated total content URL count metric", "estimated_count", estimatedCount)
}

func main() {
	tempCfg, err := config.Load()
	if err != nil {
		slog.New(slog.NewJSONHandler(os.Stderr, nil)).Error("FATAL: Failed to load configuration for logger setup", "error", err)
		os.Exit(1)
	}
	setupLogger(tempCfg)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	slog.Info("--- Starting BlogSpy Go Reaper ---")

	cfg, err := config.Load()
	if err != nil {
		slog.Error("Failed to load configuration", "error", err)
		os.Exit(1)
	}

	go metrics.ExposeMetrics("0.0.0.0:9093")

	storage, err := db.New(ctx, cfg)
	if err != nil {
		slog.Error("Failed to initialize database", "error", err)
		os.Exit(1)
	}
	defer storage.Close()

	mainTicker := time.NewTicker(10 * time.Second)
	defer mainTicker.Stop()

	stalledJobTicker := time.NewTicker(15 * time.Minute)
	defer stalledJobTicker.Stop()

	slog.Info("Reaper tasks scheduled",
		"url_counts_refresh", "10s",
		"stalled_job_reset", "15m",
	)

	go func() {
		if err := storage.RehydrateNetlocCounts(ctx); err != nil {
			slog.Error("FATAL: Netloc count rehydration failed on startup. Workers may behave incorrectly.", "error", err)
		}

		if err := storage.RehydrateBloomFilter(context.Background()); err != nil {
			slog.Error("Bloom filter rehydration failed on startup", "error", err)
		}

		_ = storage.RefreshPendingURLCount(ctx)
		_ = storage.RefreshTotalURLCount(ctx)
		_ = storage.ResetStalledJobs(ctx)
		refreshContentURLCount(ctx, storage) // Also run on startup
	}()

	for {
		select {
		case <-ctx.Done():
			slog.Info("Shutdown signal received. Exiting...")
			return
		case <-mainTicker.C:
			if err := storage.RefreshPendingURLCount(ctx); err != nil {
				slog.Error("Failed to refresh pending URL count", "error", err)
			}
			if err := storage.RefreshTotalURLCount(ctx); err != nil {
				slog.Error("Failed to refresh total URL count", "error", err)
			}
			// Call the modified refresh function periodically
			refreshContentURLCount(ctx, storage)
		case <-stalledJobTicker.C:
			if err := storage.ResetStalledJobs(ctx); err != nil {
				slog.Error("Failed to reset stalled jobs", "error", err)
			}
		}
	}
}
