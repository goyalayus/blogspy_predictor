// Package metrics
package metrics

import (
	"log/slog"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	DBQueryDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "db_query_duration_seconds",
			Help:    "Duration of database queries in seconds.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"query_name"},
	)
	TotalURLs = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "blogspy_urls_total",
			Help: "Total number of URLs in the urls table.",
		},
	)
	BackfillerRecordsProcessed = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "backfiller_records_processed_total",
			Help: "Total number of records processed by the backfiller.",
		},
	)
	BackfillerAPIRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "backfiller_api_requests_total",
			Help: "Total number of API requests made by the backfiller, labeled by status code.",
		},
		[]string{"status_code"},
	)
	BackfillerLastProcessedID = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "backfiller_last_processed_url_id",
			Help: "The last url_id processed by the backfiller, to track progress.",
		},
	)
)

func init() {
	prometheus.MustRegister(DBQueryDuration)
	prometheus.MustRegister(TotalURLs)
	prometheus.MustRegister(BackfillerRecordsProcessed)
	prometheus.MustRegister(BackfillerAPIRequests)
	prometheus.MustRegister(BackfillerLastProcessedID)
}

func ExposeMetrics(addr string) {
	slog.Info("Exposing Prometheus metrics", "address", addr)
	http.Handle("/metrics", promhttp.Handler())
	if err := http.ListenAndServe(addr, nil); err != nil {
		slog.Error("Failed to start Prometheus metrics server", "error", err)
	}
}
