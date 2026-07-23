// Package metrics exposes Prometheus business and technical metrics for the SMS Gateway.
package metrics

import (
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func init() {
	// Best-effort: ignore AlreadyRegistered when multiple binaries share a process in tests.
	_ = prometheus.Register(collectors.NewGoCollector())
	_ = prometheus.Register(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
}

// --- Business / domain metrics ---

var (
	AccountsCreated = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sms_accounts_created_total",
		Help: "Tenant accounts successfully created",
	})

	TopupsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sms_topups_total",
		Help: "Successful prepaid top-ups",
	})

	TopupCredits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sms_topup_credits_total",
		Help: "Sum of credits added via top-ups",
	})

	MessagesAccepted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sms_messages_accepted_total",
		Help: "Single messages accepted at the API gateway (balance debited + outbox written)",
	}, []string{"priority"})

	MessagesRejected = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sms_messages_rejected_total",
		Help: "Single messages rejected at the API gateway",
	}, []string{"reason"})

	CreditsSpent = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sms_credits_spent_total",
		Help: "Credits reserved/spent at accept time (1 credit per message)",
	}, []string{"priority", "source"}) // source: single|campaign

	CampaignsAccepted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sms_campaigns_accepted_total",
		Help: "Campaigns accepted at the API gateway (all-or-nothing reserve)",
	})

	CampaignsRejected = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sms_campaigns_rejected_total",
		Help: "Campaigns rejected at the API gateway",
	}, []string{"reason"})

	CampaignRecipientsAccepted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sms_campaign_recipients_accepted_total",
		Help: "Recipient count on accepted campaigns (reserved credits)",
	})

	CampaignsExpanded = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sms_campaigns_expanded_total",
		Help: "Campaigns fully expanded into per-recipient Kafka messages",
	})

	CampaignMessagesExpanded = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sms_campaign_messages_expanded_total",
		Help: "Per-recipient messages published after campaign expansion",
	})

	DispatchTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sms_dispatch_total",
		Help: "Dispatch outcomes (sent, failed, expired_sla_missed)",
	}, []string{"mode", "status", "operator"})

	DispatchLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "sms_dispatch_latency_seconds",
		Help:    "Wall time from accept (accepted_at) to dispatch result",
		Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5, 15, 30, 60, 120},
	}, []string{"mode", "priority"})

	ExpressSLAMissed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sms_express_sla_missed_total",
		Help: "Express messages dropped for hard SLA deadline (expired_sla_missed)",
	})

	LedgerDebits = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sms_ledger_debits_total",
		Help: "Durable ledger debit rows written by billing-consumer",
	}, []string{"priority"})

	LedgerRefunds = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sms_ledger_refunds_total",
		Help: "Durable ledger refund rows written by billing-consumer",
	}, []string{"reason"}) // failed|expired_sla_missed

	CreditsRefunded = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sms_credits_refunded_total",
		Help: "Credits returned to Redis/ledger on failure or SLA miss",
	}, []string{"reason"})

	ReportEvents = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sms_report_events_total",
		Help: "Dispatch results sunk to Postgres + ClickHouse",
	}, []string{"status"})

	ReconcilerDrift = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sms_reconciler_drift_total",
		Help: "Accounts where Redis balance diverged from ledger sum",
	}, []string{"direction"}) // redis_gt_ledger|redis_lt_ledger

	ReconcilerHeals = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sms_reconciler_heals_total",
		Help: "Auto-heals applied (Redis > ledger only; Redis set down to ledger)",
	})

	RateLimited = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sms_rate_limited_total",
		Help: "Requests rejected by token-bucket rate limits",
	}, []string{"scope"}) // signup|ingest
)

// --- Technical / pipeline metrics ---

var (
	HTTPRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "sms_http_request_duration_seconds",
		Help:    "API HTTP handler latency",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5},
	}, []string{"method", "path", "status"})

	AcceptDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "sms_accept_duration_seconds",
		Help:    "Time spent in single-message Accept (Redis Lua debit + outbox)",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
	}, []string{"priority", "result"}) // result: accepted|rejected|error

	OutboxRelayed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sms_outbox_relayed_total",
		Help: "Redis outbox entries successfully published to Kafka",
	}, []string{"priority"})

	OutboxRelayErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sms_outbox_relay_errors_total",
		Help: "Failures publishing or acking Redis outbox entries",
	}, []string{"stage"}) // publish|ack|claim_publish

	InboxDuplicates = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sms_inbox_duplicates_total",
		Help: "Kafka/consumer events skipped because Inbox already processed them",
	}, []string{"consumer"})

	InboxProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sms_inbox_processed_total",
		Help: "Events newly processed through the Inbox (first-time side effects)",
	}, []string{"consumer"})

	ConsumerHandleErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sms_consumer_handle_errors_total",
		Help: "Consumer handle failures that will be retried (no commit)",
	}, []string{"consumer"})

	OperatorSendDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "sms_operator_send_duration_seconds",
		Help:    "Latency of operator adapter HTTP send",
		Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5},
	}, []string{"operator", "result"}) // result: ok|error|skipped_sla
)

// Handler returns the Prometheus scrape handler (includes Go/process collectors when registered).
func Handler() http.Handler {
	return promhttp.Handler()
}

// Serve starts a background HTTP server exposing GET /metrics on addr (e.g. ":9090").
// No-op when addr is empty or "-".
func Serve(addr string) {
	if addr == "" || addr == "-" {
		return
	}
	go func() {
		mux := http.NewServeMux()
		mux.Handle("GET /metrics", Handler())
		log.Printf("metrics: listening on %s", addr)
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Printf("metrics: server stopped: %v", err)
		}
	}()
}

// InstrumentHTTP wraps an http.Handler to record sms_http_request_duration_seconds.
// pathLabel should be a low-cardinality route template (e.g. "/v1/messages"), not raw URL.
func InstrumentHTTP(pathLabel string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rw, r)
		HTTPRequestDuration.WithLabelValues(r.Method, pathLabel, strconv.Itoa(rw.status)).
			Observe(time.Since(start).Seconds())
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}
