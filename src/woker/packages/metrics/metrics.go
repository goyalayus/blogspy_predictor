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
	JobsProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "blogspy_jobs_processed_total",
		Help: "Total number of jobs processed by the worker.",
	}, []string{"type", "status"})

	JobDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "blogspy_job_duration_seconds",
		Help:    "Histogram of job processing durations.",
		Buckets: prometheus.DefBuckets,
	}, []string{"type"})

	PendingUrls = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "blogspy_pending_urls",
		Help: "Current number of URLs waiting to be processed.",
	})
)

func ExposeMetrics(addr string) {
	go func() {
		slog.Info("Metrics server starting", "address", addr)
		http.Handle("/metrics", promhttp.Handler())
		if err := http.ListenAndServe(addr, nil); err != nil {
			slog.Error("Metrics server failed", "error", err)
		}
	}()
}
