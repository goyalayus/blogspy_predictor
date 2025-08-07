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
)

func init() {
	prometheus.MustRegister(DBQueryDuration)
	prometheus.MustRegister(TotalURLs)
}

func ExposeMetrics(addr string) {
	slog.Info("Exposing Prometheus metrics", "address", addr)
	http.Handle("/metrics", promhttp.Handler())
	if err := http.ListenAndServe(addr, nil); err != nil {
		slog.Error("Failed to start Prometheus metrics server", "error", err)
	}
}
