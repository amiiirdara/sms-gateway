// Command api-gateway is the REST entry point (ARCHITECTURE.md section 8.1).
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/amiri/sms-gateway/internal/config"
	"github.com/amiri/sms-gateway/internal/domain/billing"
	"github.com/amiri/sms-gateway/internal/domain/campaigns"
	"github.com/amiri/sms-gateway/internal/domain/messaging"
	"github.com/amiri/sms-gateway/internal/platform/httpx"
	"github.com/amiri/sms-gateway/internal/platform/httpx/auth"
	"github.com/amiri/sms-gateway/internal/platform/httpx/ratelimit"
	"github.com/amiri/sms-gateway/internal/platform/lifecycle"
	"github.com/amiri/sms-gateway/internal/platform/metrics"
	"github.com/amiri/sms-gateway/internal/platform/postgres"
	platredis "github.com/amiri/sms-gateway/internal/platform/redis"
)

func main() {
	cfg := config.Load("api-gateway")
	ctx, stop := lifecycle.WithShutdownSignal(context.Background())
	defer stop()

	pool, err := postgres.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("api-gateway: postgres: %v", err)
	}
	defer pool.Close()

	rdb, err := platredis.NewClient(ctx, cfg.RedisAddr)
	if err != nil {
		log.Fatalf("api-gateway: redis: %v", err)
	}
	defer rdb.Close()

	billingSvc := billing.New(pool, rdb)
	msgSvc := messaging.New(rdb)
	campSvc := campaigns.New(rdb)
	authMw := auth.Middleware(billingSvc.Queries())
	signupLimit := ratelimit.ByIP(rdb, ratelimit.Config{
		Capacity:     cfg.SignupRateCapacity,
		RefillPerSec: cfg.SignupRateRefill,
	}, "signup")
	ingestLimit := ratelimit.ByAccount(rdb, ratelimit.Config{
		Capacity:     cfg.IngestRateCapacity,
		RefillPerSec: cfg.IngestRateRefill,
	}, "ingest")

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", metrics.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.Handle("POST /v1/accounts", metrics.InstrumentHTTP("/v1/accounts", signupLimit(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Name string `json:"name"`
		}
		if err := httpx.DecodeJSON(r, &body); err != nil {
			httpx.Error(w, http.StatusBadRequest, "invalid JSON body")
			return
		}
		res, err := billingSvc.CreateAccount(r.Context(), body.Name)
		if err != nil {
			httpx.Error(w, http.StatusBadRequest, err.Error())
			return
		}
		metrics.AccountsCreated.Inc()
		httpx.WriteJSON(w, http.StatusCreated, map[string]any{
			"accountId": res.AccountID.String(),
			"apiKey":    res.APIKey,
		})
	}))))

	mux.Handle("POST /v1/topups", metrics.InstrumentHTTP("/v1/topups", authMw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		acc, _ := auth.FromContext(r.Context())
		var body struct {
			Amount int64 `json:"amount"`
		}
		if err := httpx.DecodeJSON(r, &body); err != nil {
			httpx.Error(w, http.StatusBadRequest, "invalid JSON body")
			return
		}
		bal, err := billingSvc.TopUp(r.Context(), acc.ID, body.Amount)
		if err != nil {
			httpx.Error(w, http.StatusBadRequest, err.Error())
			return
		}
		metrics.TopupsTotal.Inc()
		metrics.TopupCredits.Add(float64(body.Amount))
		httpx.WriteJSON(w, http.StatusOK, map[string]any{"balance": bal})
	}))))

	mux.Handle("GET /v1/balance", metrics.InstrumentHTTP("/v1/balance", authMw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		acc, _ := auth.FromContext(r.Context())
		bal, err := billingSvc.Balance(r.Context(), acc.ID)
		if err != nil {
			httpx.Error(w, http.StatusInternalServerError, "failed to read balance")
			return
		}
		httpx.WriteJSON(w, http.StatusOK, map[string]any{"balance": bal})
	}))))

	mux.Handle("POST /v1/messages", metrics.InstrumentHTTP("/v1/messages", authMw(ingestLimit(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		acc, _ := auth.FromContext(r.Context())
		var body struct {
			To       string `json:"to"`
			Text     string `json:"text"`
			Priority string `json:"priority"`
		}
		if err := httpx.DecodeJSON(r, &body); err != nil {
			httpx.Error(w, http.StatusBadRequest, "invalid JSON body")
			return
		}
		priority := body.Priority
		if priority == "" {
			priority = messaging.PriorityNormal
		}
		start := time.Now()
		resp, err := msgSvc.Accept(r.Context(), messaging.AcceptRequest{
			AccountID:      acc.ID,
			To:             body.To,
			Text:           body.Text,
			Priority:       body.Priority,
			IdempotencyKey: r.Header.Get("Idempotency-Key"),
		})
		if errors.Is(err, messaging.ErrInsufficientFunds) {
			metrics.AcceptDuration.WithLabelValues(priority, "rejected").Observe(time.Since(start).Seconds())
			metrics.MessagesRejected.WithLabelValues("insufficient_funds").Inc()
			httpx.Error(w, http.StatusPaymentRequired, "insufficient funds")
			return
		}
		if err != nil {
			metrics.AcceptDuration.WithLabelValues(priority, "error").Observe(time.Since(start).Seconds())
			metrics.MessagesRejected.WithLabelValues("validation").Inc()
			httpx.Error(w, http.StatusBadRequest, err.Error())
			return
		}
		metrics.AcceptDuration.WithLabelValues(priority, "accepted").Observe(time.Since(start).Seconds())
		metrics.MessagesAccepted.WithLabelValues(priority).Inc()
		metrics.CreditsSpent.WithLabelValues(priority, "single").Add(float64(resp.Cost))
		httpx.WriteJSON(w, http.StatusAccepted, resp)
	})))))

	mux.Handle("POST /v1/campaigns", metrics.InstrumentHTTP("/v1/campaigns", authMw(ingestLimit(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		acc, _ := auth.FromContext(r.Context())
		var body struct {
			Text       string   `json:"text"`
			Recipients []string `json:"recipients"`
		}
		if err := httpx.DecodeJSON(r, &body); err != nil {
			httpx.Error(w, http.StatusBadRequest, "invalid JSON body")
			return
		}
		resp, err := campSvc.Accept(r.Context(), campaigns.AcceptRequest{
			AccountID:      acc.ID,
			Text:           body.Text,
			Recipients:     body.Recipients,
			IdempotencyKey: r.Header.Get("Idempotency-Key"),
		})
		var insuf *campaigns.InsufficientFundsError
		if errors.As(err, &insuf) {
			metrics.CampaignsRejected.WithLabelValues("insufficient_funds").Inc()
			httpx.WriteJSON(w, http.StatusPaymentRequired, map[string]any{
				"error":     "insufficient funds",
				"required":  insuf.Required,
				"available": insuf.Available,
			})
			return
		}
		if err != nil {
			metrics.CampaignsRejected.WithLabelValues("validation").Inc()
			httpx.Error(w, http.StatusBadRequest, err.Error())
			return
		}
		metrics.CampaignsAccepted.Inc()
		metrics.CampaignRecipientsAccepted.Add(float64(resp.TotalRecipients))
		metrics.CreditsSpent.WithLabelValues(messaging.PriorityNormal, "campaign").Add(float64(resp.Cost))
		httpx.WriteJSON(w, http.StatusAccepted, resp)
	})))))

	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: mux}
	go func() {
		log.Printf("api-gateway listening on %s", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("api-gateway: listen: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("api-gateway: shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}
