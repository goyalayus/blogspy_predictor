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

	mainTicker := time.NewTicker(20 * time.Second)
	defer mainTicker.Stop()

	netlocCacheTicker := time.NewTicker(cfg.NetlocCountRefreshInterval)
	defer netlocCacheTicker.Stop()

	stalledJobTicker := time.NewTicker(15 * time.Minute)
	defer stalledJobTicker.Stop()

	orphanCheckTicker := time.NewTicker(30 * time.Minute)
	defer orphanCheckTicker.Stop()

	stalledClassifyTicker := time.NewTicker(5 * time.Minute)
	defer stalledClassifyTicker.Stop()

	slog.Info("Reaper tasks scheduled",
		"pending_count_refresh", "20s",
		"netloc_count_refresh", cfg.NetlocCountRefreshInterval.String(),
		"stalled_crawl_reset", "15m",
		"orphan_job_check", "30m",
		"stalled_classify_reset", "5m",
	)

	go func() {
		slog.Info("Performing initial startup maintenance run...")
		if err := storage.RefreshPendingURLCount(ctx, pendingCounterName); err != nil {
			slog.Error("Startup: Failed to refresh pending URL count", "error", err)
		}
		if err := storage.RefreshNetlocCounts(ctx); err != nil {
			slog.Error("Startup: Failed to refresh netloc counts cache", "error", err)
		}
		if err := storage.ResetStalledJobs(ctx); err != nil {
			slog.Error("Startup: Failed to reset stalled jobs", "error", err)
		}
		if err := storage.ResetOrphanedJobs(ctx); err != nil {
			slog.Error("Startup: Failed to reset orphaned jobs", "error", err)
		}
		if err := storage.ResetStalledClassificationJobs(ctx, "5m"); err != nil {
			slog.Error("Startup: Failed to reset stalled classification jobs", "error", err)
		}
		slog.Info("Initial startup maintenance run complete.")
	}()

	for {
		select {
		case <-ctx.Done():
			slog.Info("Shutdown signal received. Exiting...")
			return

		case <-mainTicker.C:
			if err := storage.RefreshPendingURLCount(ctx, pendingCounterName); err != nil {
				slog.Error("Failed to refresh pending URL count", "error", err)
			}

		case <-netlocCacheTicker.C:
			slog.Info("Refreshing netloc counts cache")
			if err := storage.RefreshNetlocCounts(ctx); err != nil {
				slog.Error("Failed to refresh netloc counts cache", "error", err)
			}

		case <-stalledJobTicker.C:
			slog.Info("Resetting stalled crawl/classify jobs in 'urls' table")
			if err := storage.ResetStalledJobs(ctx); err != nil {
				slog.Error("Failed to reset stalled jobs", "error", err)
			}

		case <-orphanCheckTicker.C:
			slog.Info("Checking for orphaned completed jobs")
			if err := storage.ResetOrphanedJobs(ctx); err != nil {
				slog.Error("Failed to reset orphaned jobs", "error", err)
			}

		case <-stalledClassifyTicker.C:
			slog.Info("Resetting stalled classification jobs in 'classification_queue' table")
			if err := storage.ResetStalledClassificationJobs(ctx, "5m"); err != nil {
				slog.Error("Failed to reset stalled classification jobs", "error", err)
			}
		}
	}
}
