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

	orphanCheckTicker := time.NewTicker(30 * time.Minute)
	defer orphanCheckTicker.Stop()

	slog.Info("Reaper tasks scheduled",
		"url_counts_refresh", "10s",
		"stalled_job_reset", "15m",
		"orphan_job_check", "30m",
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
		_ = storage.ResetOrphanedJobs(ctx)
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
		case <-stalledJobTicker.C:
			if err := storage.ResetStalledJobs(ctx); err != nil {
				slog.Error("Failed to reset stalled jobs", "error", err)
			}
		case <-orphanCheckTicker.C:
			if err := storage.ResetOrphanedJobs(ctx); err != nil {
				slog.Error("Failed to reset orphaned jobs", "error", err)
			}
		}
	}
}