// Package main
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
	"worker/packages/config"
	"worker/packages/crawler"
	"worker/packages/db"
	"worker/packages/domain"
	"worker/packages/worker"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	slog.Info("--- Starting BlogSpy Go Worker ---")

	cfg, err := config.Load()
	if err != nil {
		slog.Error("Failed to load configuration", "error", err)
		os.Exit(1)
	}

	dbCfg := db.Config{
		BatchWriteInterval:  cfg.BatchWriteInterval,
		BatchWriteQueueSize: cfg.BatchWriteQueueSize,
		JobTimeout:          cfg.JobTimeout,
	}
	storage, err := db.New(ctx, cfg.DatabaseURL, dbCfg)
	if err != nil {
		slog.Error("Failed to initialize database", "error", err)
		os.Exit(1)
	}
	defer storage.Close()

	crawler := crawler.New(cfg.FetchTimeout)

	appWorker := worker.New(cfg, storage, crawler)

	ticker := time.NewTicker(cfg.SleepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("Shutdown signal received. Exiting...")
			return
		case <-ticker.C:
			slog.Debug("Worker cycle starting")
			appWorker.ProcessJobs(ctx, "classification", domain.PendingClassification, domain.Classifying)
			appWorker.ProcessJobs(ctx, "crawling", domain.PendingCrawl, domain.Crawling)
		}
	}
}
