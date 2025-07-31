// Package config
package config

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	DatabaseURL                string
	MLApiURL                   string
	BatchSize                  int
	MaxWorkers                 int
	SleepInterval              time.Duration
	JobTimeout                 time.Duration
	FetchTimeout               time.Duration
	MaxUrlsPerNetloc           int
	BatchWriteInterval         time.Duration
	BatchWriteQueueSize        int
	NetlocCountRefreshInterval time.Duration
	RestrictedTLDs             []string
	AllowedPathPrefixes        []string
	IgnoreExtensions           []string
	// NEW: Configuration for the asynchronous writers
	StatusUpdateBatchSize  int
	StatusUpdateInterval   time.Duration
	ContentInsertBatchSize int
	ContentInsertInterval  time.Duration
	ContentInsertQueueSize int
	StatusUpdateQueueSize  int
	// NEW: Logging configuration
	LogFile  string
	LogLevel string
	// NEW: Redis and Bloom Filter Configuration
	RedisAddr            string
	RedisPassword        string
	RedisDB              int
	BloomFilterKey       string
	BloomFilterCapacity  int64
	BloomFilterErrorRate float64
}

func Load() (Config, error) {
	cfg := Config{}
	var missingVars []string

	cfg.DatabaseURL = getEnv("DATABASE_URL", "")
	cfg.MLApiURL = getEnv("ML_API_URL", "")

	if cfg.DatabaseURL == "" {
		missingVars = append(missingVars, "DATABASE_URL")
	}
	if cfg.MLApiURL == "" {
		missingVars = append(missingVars, "ML_API_URL")
	}
	if len(missingVars) > 0 {
		return cfg, fmt.Errorf("missing required environment variables: %s", strings.Join(missingVars, ", "))
	}

	var err error
	cfg.BatchSize, err = strconv.Atoi(getEnv("BATCH_SIZE", "500"))
	if err != nil {
		slog.Warn("Invalid BATCH_SIZE", "value", getEnv("BATCH_SIZE", "500"), "error", err)
		cfg.BatchSize = 500
	}
	cfg.MaxWorkers, _ = strconv.Atoi(getEnv("MAX_WORKERS", "50"))
	cfg.SleepInterval, _ = time.ParseDuration(getEnv("SLEEP_INTERVAL", "5s"))
	cfg.JobTimeout, _ = time.ParseDuration(getEnv("JOB_TIMEOUT", "15m"))
	cfg.FetchTimeout, _ = time.ParseDuration(getEnv("FETCH_TIMEOUT", "6s"))
	cfg.MaxUrlsPerNetloc, _ = strconv.Atoi(getEnv("MAX_URLS_PER_NETLOC", "130"))

	// Legacy batching config, still used for link insertion
	cfg.BatchWriteInterval, _ = time.ParseDuration(getEnv("BATCH_WRITE_INTERVAL", "10s"))
	cfg.BatchWriteQueueSize, _ = strconv.Atoi(getEnv("BATCH_WRITE_QUEUE_SIZE", "1000"))

	// NEW: Config for async writers
	cfg.NetlocCountRefreshInterval, _ = time.ParseDuration(getEnv("NETLOC_COUNT_REFRESH_INTERVAL", "20s"))
	cfg.StatusUpdateBatchSize, _ = strconv.Atoi(getEnv("STATUS_UPDATE_BATCH_SIZE", "500"))
	cfg.StatusUpdateInterval, _ = time.ParseDuration(getEnv("STATUS_UPDATE_INTERVAL", "2s"))
	cfg.StatusUpdateQueueSize, _ = strconv.Atoi(getEnv("STATUS_UPDATE_QUEUE_SIZE", "2000"))
	cfg.ContentInsertBatchSize, _ = strconv.Atoi(getEnv("CONTENT_INSERT_BATCH_SIZE", "250"))
	cfg.ContentInsertInterval, _ = time.ParseDuration(getEnv("CONTENT_INSERT_INTERVAL", "5s"))
	cfg.ContentInsertQueueSize, _ = strconv.Atoi(getEnv("CONTENT_INSERT_QUEUE_SIZE", "1000"))

	cfg.RestrictedTLDs = strings.Split(getEnv("RESTRICTED_TLDS", ".org,.edu"), ",")
	cfg.AllowedPathPrefixes = strings.Split(getEnv("ALLOWED_PATH_PREFIXES", "/blog"), ",")
	cfg.IgnoreExtensions = strings.Split(getEnv("IGNORE_EXTENSIONS", ".pdf,.jpg,.jpeg,.png,.gif,.zip,.rar,.exe,.mp3,.mp4,.avi,.mov,.dmg,.iso,.css,.js,.xml,.json,.gz,.tar,.tgz"), ",")

	// NEW: Load logging configuration from environment
	cfg.LogFile = getEnv("LOG_FILE", "logs/worker.log")
	cfg.LogLevel = getEnv("LOG_LEVEL", "info")

	// NEW: Load Redis and Bloom Filter configuration
	cfg.RedisAddr = getEnv("REDIS_ADDR", "localhost:6379")
	cfg.RedisPassword = getEnv("REDIS_PASSWORD", "") // No password by default
	cfg.RedisDB, _ = strconv.Atoi(getEnv("REDIS_DB", "0"))
	cfg.BloomFilterKey = getEnv("BLOOM_FILTER_KEY", "blogspy:urls_bloom")
	cfg.BloomFilterCapacity, _ = strconv.ParseInt(getEnv("BLOOM_FILTER_CAPACITY", "20000000"), 10, 64)   // Default: 20 million
	cfg.BloomFilterErrorRate, _ = strconv.ParseFloat(getEnv("BLOOM_FILTER_ERROR_RATE", "0.0001"), 64) // Default: 1 in 10,000

	return cfg, nil
}

func getEnv(key, defaultVal string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultVal
}
