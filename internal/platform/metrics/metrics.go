// Package metrics exposes Prometheus counters/histograms for the SMS Gateway.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	MessagesAccepted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sms_messages_accepted_total",
		Help: "Messages accepted at the API gateway",
	}, []string{"priority"})

	MessagesRejected = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sms_messages_rejected_total",
		Help: "Messages rejected at the API gateway",
	}, []string{"reason"})

	CampaignsAccepted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sms_campaigns_accepted_total",
		Help: "Campaigns accepted at the API gateway",
	})

	DispatchTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sms_dispatch_total",
		Help: "Dispatch outcomes",
	}, []string{"mode", "status", "operator"})

	DispatchLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "sms_dispatch_latency_seconds",
		Help:    "Time from accepted_at to dispatch result",
		Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5, 15, 30, 60, 120},
	}, []string{"mode", "priority"})
)

// Handler returns the Prometheus scrape handler.
func Handler() http.Handler {
	return promhttp.Handler()
}
