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

	// The Reaper only needs a subset of the DB config
	dbCfg := db.Config{JobTimeout: cfg.JobTimeout}
	storage, err := db.New(ctx, cfg.DatabaseURL, dbCfg)
	if err != nil {
		slog.Error("Failed to initialize database", "error", err)
		os.Exit(1)
	}
	defer storage.Close()

	ticker := time.NewTicker(reaperInterval)
	defer ticker.Stop()

	loopCounter := 0
	for {
		select {
		case <-ctx.Done():
			slog.Info("Shutdown signal received. Exiting...")
			return
		case <-ticker.C:
			loopCounter++
			slog.Info("Reaper cycle starting")

			// Task 1: Refresh the pending URL count (every cycle)
			if err := storage.RefreshPendingURLCount(ctx, pendingCounterName); err != nil {
				slog.Error("Failed to refresh pending URL count", "error", err)
			}

			// Task 2: Reset stalled jobs (every N cycles)
			if loopCounter%resetStalledJobsLoopCycles == 0 {
				slog.Info("Resetting stalled jobs")
				if err := storage.ResetStalledJobs(ctx); err != nil {
					slog.Error("Failed to reset stalled jobs", "error", err)
				}
			}
		}
	}
}
