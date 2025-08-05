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

	logDir := filepath.Dir(cfg.LogFile)
	if err := os.MkdirAll(logDir, 0750); err != nil {
		slog.New(slog.NewJSONHandler(os.Stderr, nil)).Error(
			"Failed to create log directory", "path", logDir, "error", err,
		)
	}

	logRotator := &lumberjack.Logger{
		Filename:   cfg.LogFile,
		MaxSize:    10,
		MaxBackups: 5,
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
	}).WithAttrs([]slog.Attr{slog.String("service", "go-worker")})

	logger := slog.New(handler)
	slog.SetDefault(logger)
}

func main() {
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
