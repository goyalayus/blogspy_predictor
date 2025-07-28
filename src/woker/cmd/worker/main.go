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

	// MODIFIED: db.New now takes the main config struct directly.
	// The reaper doesn't use all the fields, but this standardizes the initialization.
	storage, err := db.New(ctx, cfg)
	if err != nil {
		slog.Error("Failed to initialize database", "error", err)
		os.Exit(1)
	}
	defer storage.Close()

	mainTicker := time.NewTicker(20 * time.Second)
	defer mainTicker.Stop()

	netlocCacheTicker := time.NewTicker(cfg.NetlocCountRefreshInterval)
	defer netlocCacheTicker.Stop()

	stalledJobTicker := time.NewTicker(15 * time.Minute)
	defer stalledJobTicker.Stop()

	slog.Info("Reaper tasks scheduled",
		"pending_count_refresh", "20s",
		"netloc_count_refresh", cfg.NetlocCountRefreshInterval,
		"stalled_job_reset", "15m",
	)

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
			if err := storage.RefreshPendingURLCount(ctx, "pending_urls_count"); err != nil {
				slog.Error("Failed to refresh pending URL count", "error", err)
			}
		case <-netlocCacheTicker.C:
			slog.Info("Refreshing netloc counts cache")
			if err := storage.RefreshNetlocCounts(ctx); err != nil {
				slog.Error("Failed to refresh netloc counts cache", "error", err)
			}
		case <-stalledJobTicker.C:
			slog.Info("Resetting stalled jobs")
			if err := storage.ResetStalledJobs(ctx); err != nil {
				slog.Error("Failed to reset stalled jobs", "error", err)
			}
		}
	}
}
