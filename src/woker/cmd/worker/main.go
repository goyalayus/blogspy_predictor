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
	"worker/packages/crawler"
	"worker/packages/db"
	"worker/packages/generated"
	"worker/packages/worker"

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

	// Configure log rotation
	logRotator := &lumberjack.Logger{
		Filename:   cfg.LogFile,
		MaxSize:    10, // megabytes
		MaxBackups: 5,
		MaxAge:     30, // days
		Compress:   true,
	}

	// MultiWriter to log to both file and stdout (for container logs)
	multiWriter := io.MultiWriter(os.Stdout, logRotator)

	handler := slog.NewJSONHandler(multiWriter, &slog.HandlerOptions{
		Level: level,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			// Add a 'T' to the time format
			if a.Key == slog.TimeKey {
				a.Value = slog.StringValue(a.Value.Time().Format(time.RFC3339Nano))
			}
			return a
		},
	}).WithAttrs([]slog.Attr{slog.String("service", "go-worker")})

	logger := slog.New(handler)
	slog.SetDefault(logger)
}

func main() {
	// NOTE: Logger is configured before any other operation.
	// We load config here first for the logger, then it's loaded again in the main flow.
	// This is acceptable to ensure logging is immediately available.
	tempCfg, err := config.Load()
	if err != nil {
		slog.Error("FATAL: Failed to load configuration for logger setup", "error", err)
		os.Exit(1)
	}
	setupLogger(tempCfg)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	slog.Info("--- Starting BlogSpy Go Worker ---")

	cfg, err := config.Load()
	if err != nil {
		slog.Error("Failed to load configuration", "error", err)
		os.Exit(1)
	}

	// db.New now takes the main config struct directly.
	storage, err := db.New(ctx, cfg)
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
			appWorker.ProcessJobs(ctx, "classification", generated.CrawlStatusPendingClassification, generated.CrawlStatusClassifying)
			appWorker.ProcessJobs(ctx, "crawling", generated.CrawlStatusPendingCrawl, generated.CrawlStatusCrawling)
		}
	}
}
