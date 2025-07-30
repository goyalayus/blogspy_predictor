// woker/cmd/reaper/main.go

package main

import (
	"context"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
	"worker/packages/config"
	"worker/packages/db"

	"gopkg.in/natefinch/lumberjack.v2"
)

const (
	// DELETED: This is now handled within the db package directly.
	pendingCounterName = "pending_urls_count"
)

// NOTE: Ideally, this function would live in a shared 'logging' package.
// It is duplicated here to adhere to the response constraints.
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

	// Configure log rotation
	logRotator := &lumberjack.Logger{
		Filename:   cfg.LogFile,
		MaxSize:    5, // megabytes
		MaxBackups: 3,
		MaxAge:     30, // days
		Compress:   true,
	}

	// MultiWriter to log to both file and stdout (for container logs)
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

func main() {
	tempCfg, err := config.Load()
	if err != nil {
		// Use a basic logger for this fatal error since the main one isn't set up.
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

	storage, err := db.New(ctx, cfg)
	if err != nil {
		slog.Error("Failed to initialize database", "error", err)
		os.Exit(1)
	}
	defer storage.Close()

	// Use separate tickers for each distinct task for clarity and future flexibility.
	mainTicker := time.NewTicker(20 * time.Second) // Ticker for frequent tasks
	defer mainTicker.Stop()

	netlocCacheTicker := time.NewTicker(cfg.NetlocCountRefreshInterval)
	defer netlocCacheTicker.Stop()

	stalledJobTicker := time.NewTicker(15 * time.Minute)
	defer stalledJobTicker.Stop()

	// NEW: Ticker for the less frequent orphan cleanup task.
	orphanCheckTicker := time.NewTicker(30 * time.Minute)
	defer orphanCheckTicker.Stop()

	slog.Info("Reaper tasks scheduled",
		"pending_count_refresh", "20s",
		"netloc_count_refresh", cfg.NetlocCountRefreshInterval,
		"stalled_job_reset", "15m",
		"orphan_job_check", "30m",
	)

	// Run tasks once on startup for immediate feedback
	go func() {
		_ = storage.RefreshPendingURLCount(ctx)
		_ = storage.RefreshNetlocCounts(ctx)
		_ = storage.ResetStalledJobs(ctx)
		_ = storage.ResetOrphanedJobs(ctx) // Run orphan check on startup too
	}()

	for {
		select {
		case <-ctx.Done():
			slog.Info("Shutdown signal received. Exiting...")
			return
		case <-mainTicker.C:
			// Task 1: Refresh the pending URL count (every 20s)
			if err := storage.RefreshPendingURLCount(ctx); err != nil {
				slog.Error("Failed to refresh pending URL count", "error", err)
			}
		case <-netlocCacheTicker.C:
			// Task 2: Refresh the netloc counts cache
			if err := storage.RefreshNetlocCounts(ctx); err != nil {
				slog.Error("Failed to refresh netloc counts cache", "error", err)
			}
		case <-stalledJobTicker.C:
			// Task 3: Reset stalled jobs
			if err := storage.ResetStalledJobs(ctx); err != nil {
				slog.Error("Failed to reset stalled jobs", "error", err)
			}
		case <-orphanCheckTicker.C: // NEW
			// Task 4: Reset orphaned completed jobs
			if err := storage.ResetOrphanedJobs(ctx); err != nil {
				slog.Error("Failed to reset orphaned jobs", "error", err)
			}
		}
	}
}
