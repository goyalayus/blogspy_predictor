// woker/cmd/reaper/main.go

package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
	"worker/packages/config"
	"worker/packages/db"
)

const (
	// DELETED: These constants are no longer needed here or are in config
	pendingCounterName = "pending_urls_count"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)
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

	// Run tasks once on startup
	go func() {
		storage.RefreshPendingURLCount(ctx, pendingCounterName)
		storage.RefreshNetlocCounts(ctx)
		storage.ResetStalledJobs(ctx)
		storage.ResetOrphanedJobs(ctx) // Run orphan check on startup too
	}()

	for {
		select {
		case <-ctx.Done():
			slog.Info("Shutdown signal received. Exiting...")
			return
		case <-mainTicker.C:
			// Task 1: Refresh the pending URL count (every 20s)
			if err := storage.RefreshPendingURLCount(ctx, pendingCounterName); err != nil {
				slog.Error("Failed to refresh pending URL count", "error", err)
			}
		case <-netlocCacheTicker.C:
			// Task 2: Refresh the netloc counts cache
			slog.Info("Refreshing netloc counts cache")
			if err := storage.RefreshNetlocCounts(ctx); err != nil {
				slog.Error("Failed to refresh netloc counts cache", "error", err)
			}
		case <-stalledJobTicker.C:
			// Task 3: Reset stalled jobs
			slog.Info("Resetting stalled jobs")
			if err := storage.ResetStalledJobs(ctx); err != nil {
				slog.Error("Failed to reset stalled jobs", "error", err)
			}
		case <-orphanCheckTicker.C: // NEW
			// Task 4: Reset orphaned completed jobs
			slog.Info("Checking for orphaned completed jobs")
			if err := storage.ResetOrphanedJobs(ctx); err != nil {
				slog.Error("Failed to reset orphaned jobs", "error", err)
			}
		}
	}
}
