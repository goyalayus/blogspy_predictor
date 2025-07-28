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
	// DELETED: These constants are now derived from config or used directly
	reaperInterval             = 20 * time.Second
	resetStalledJobsInterval   = 15 * time.Minute
	pendingCounterName         = "pending_urls_count"
	resetStalledJobsLoopCycles = int(resetStalledJobsInterval / reaperInterval)
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

	// The Reaper only needs a subset of the DB config
	dbCfg := db.Config{JobTimeout: cfg.JobTimeout}
	storage, err := db.New(ctx, cfg.DatabaseURL, dbCfg)
	if err != nil {
		slog.Error("Failed to initialize database", "error", err)
		os.Exit(1)
	}
	defer storage.Close()

	// MODIFIED: Use separate tickers for each distinct task for clarity and future flexibility.
	mainTicker := time.NewTicker(20 * time.Second) // Ticker for frequent tasks
	defer mainTicker.Stop()

	netlocCacheTicker := time.NewTicker(cfg.NetlocCountRefreshInterval) // NEW: Dedicated ticker for netloc cache
	defer netlocCacheTicker.Stop()

	stalledJobTicker := time.NewTicker(15 * time.Minute) // NEW: Dedicated ticker for resetting stalled jobs
	defer stalledJobTicker.Stop()

	slog.Info("Reaper tasks scheduled",
		"pending_count_refresh", "20s",
		"netloc_count_refresh", cfg.NetlocCountRefreshInterval,
		"stalled_job_reset", "15m",
	)

	// Run tasks once on startup
	go func() {
		storage.RefreshPendingURLCount(ctx, "pending_urls_count")
		storage.RefreshNetlocCounts(ctx)
		storage.ResetStalledJobs(ctx)
	}()

	for {
		select {
		case <-ctx.Done():
			slog.Info("Shutdown signal received. Exiting...")
			return
		case <-mainTicker.C:
			// Task 1: Refresh the pending URL count (every 20s)
			if err := storage.RefreshPendingURLCount(ctx, "pending_urls_count"); err != nil {
				slog.Error("Failed to refresh pending URL count", "error", err)
			}
		case <-netlocCacheTicker.C: // NEW: Handle the netloc cache refresh
			// Task 2: Refresh the netloc counts cache (every cfg.NetlocCountRefreshInterval)
			slog.Info("Refreshing netloc counts cache")
			if err := storage.RefreshNetlocCounts(ctx); err != nil {
				slog.Error("Failed to refresh netloc counts cache", "error", err)
			}
		case <-stalledJobTicker.C: // NEW: Handle resetting stalled jobs
			// Task 3: Reset stalled jobs (every 15m)
			slog.Info("Resetting stalled jobs")
			if err := storage.ResetStalledJobs(ctx); err != nil {
				slog.Error("Failed to reset stalled jobs", "error", err)
			}
		}
	}
}
