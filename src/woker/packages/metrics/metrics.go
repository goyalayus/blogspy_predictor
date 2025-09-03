// FILE: src/woker/packages/metrics/metrics.go

// Package metrics
package metrics

import (
	"log/slog"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	DBQueryDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "db_query_duration_seconds",
			Help:    "Duration of database queries in seconds.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"query_name"},
	)
	TotalURLs = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "blogspy_urls_total",
			Help: "Total number of URLs in the urls table.",
		},
	)
	BackfillerRecordsProcessed = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "backfiller_records_processed_total",
			Help: "Total number of records processed by the backfiller.",
		},
	)
	BackfillerAPIRequests = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "backfiller_api_requests_total",
			Help: "Total number of API requests made by the backfiller, labeled by status code.",
		},
		[]string{"status_code"},
	)
	BackfillerLastProcessedID = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "backfiller_last_processed_url_id",
			Help: "The last url_id processed by the backfiller, to track progress.",
		},
	)

	JobsProcessedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "blogspy_jobs_processed_total",
			Help: "Total number of jobs processed, labeled by type and outcome.",
		},
		[]string{"type", "outcome"}, // type="classification"|"crawling", outcome="success"|"failure"
	)

	JobDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "blogspy_job_duration_seconds",
			Help:    "End-to-end duration of jobs in seconds.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"type"}, // type="classification"|"crawling"
	)

	DependencyCallDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "blogspy_dependency_call_duration_seconds",
			Help:    "Duration of calls to external dependencies in seconds.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"dependency"}, // dependency="ml_api"|"redis_bloom_filter"|etc.
	)

	JobQueueActiveWorkers = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "blogspy_job_queue_active_workers",
			Help: "Number of goroutines currently processing jobs.",
		},
		[]string{"type"}, // type="classification"|"crawling"
	)

	NetlocCacheTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "blogspy_netloc_cache_total",
			Help: "Total number of netloc classification cache attempts.",
		},
		[]string{"status"}, // status="hit"|"miss"
	)

	ContentStorageDecisionsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "blogspy_content_storage_decisions_total",
			Help: "Total number of content storage decisions made based on quality filters.",
		},
		[]string{"decision"}, // decision="stored"|"skipped_thin_content"
	)

	LinksSkippedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "blogspy_links_skipped_total",
			Help: "Total number of discovered links skipped for various reasons.",
		},
		[]string{"reason"}, // reason="blocked_host"
	)

	// --- NEW METRIC DEFINITIONS START HERE ---

	LinksDiscoveredTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "blogspy_links_discovered_total",
			Help: "Total number of new links discovered, labeled by source.",
		},
		[]string{"source_type"}, // source_type="crawl"
	)

	URLsInContentTableTotal = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "blogspy_urls_in_content_table_total",
			Help: "Total number of URLs with content stored in the url_content table.",
		},
	)

	ReaperJobsResetTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "reaper_jobs_reset_total",
			Help: "Total number of jobs reset by the reaper, labeled by type.",
		},
		[]string{"type"}, // type="stalled"
	)

	// --- NEW METRIC DEFINITIONS END HERE ---
)

func init() {
	// promauto handles registration automatically.
}

func ExposeMetrics(addr string) {
	slog.Info("Exposing Prometheus metrics", "address", addr)
	http.Handle("/metrics", promhttp.Handler())
	if err := http.ListenAndServe(addr, nil); err != nil {
		slog.Error("Failed to start Prometheus metrics server", "error", err)
	}
}
