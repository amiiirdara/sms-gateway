// Command reporting-api serves message status and reports (ARCHITECTURE.md section 8.9).
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/amiri/sms-gateway/internal/config"
	"github.com/amiri/sms-gateway/internal/db/sqlc"
	"github.com/amiri/sms-gateway/internal/domain/billing"
	platch "github.com/amiri/sms-gateway/internal/platform/clickhouse"
	"github.com/amiri/sms-gateway/internal/platform/httpx"
	"github.com/amiri/sms-gateway/internal/platform/httpx/auth"
	"github.com/amiri/sms-gateway/internal/platform/lifecycle"
	"github.com/amiri/sms-gateway/internal/platform/postgres"
	platredis "github.com/amiri/sms-gateway/internal/platform/redis"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

func main() {
	cfg := config.Load("reporting-api")
	ctx, stop := lifecycle.WithShutdownSignal(context.Background())
	defer stop()

	pool, err := postgres.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("reporting-api: postgres: %v", err)
	}
	defer pool.Close()
	rdb, err := platredis.NewClient(ctx, cfg.RedisAddr)
	if err != nil {
		log.Fatalf("reporting-api: redis: %v", err)
	}
	defer rdb.Close()
	ch, err := platch.New(ctx, cfg.ClickHouseAddr)
	if err != nil {
		log.Fatalf("reporting-api: clickhouse: %v", err)
	}
	defer ch.Close()

	billingSvc := billing.New(pool, rdb)
	q := billingSvc.Queries()
	authMw := auth.Middleware(q)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.Handle("GET /v1/messages/{id}", authMw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		acc, _ := auth.FromContext(r.Context())
		id, err := uuid.Parse(r.PathValue("id"))
		if err != nil {
			httpx.Error(w, http.StatusBadRequest, "invalid message id")
			return
		}
		msg, err := q.GetMessageByIDForAccount(r.Context(), sqlc.GetMessageByIDForAccountParams{
			ID:        id,
			AccountID: acc.ID,
		})
		if errors.Is(err, pgx.ErrNoRows) {
			httpx.Error(w, http.StatusNotFound, "not found")
			return
		}
		if err != nil {
			httpx.Error(w, http.StatusInternalServerError, "internal error")
			return
		}
		httpx.WriteJSON(w, http.StatusOK, map[string]any{
			"id":           msg.ID.String(),
			"recipient":    msg.Recipient,
			"priority":     msg.Priority,
			"cost":         msg.Cost,
			"status":       msg.Status,
			"operator":     textOrEmpty(msg.Operator.String, msg.Operator.Valid),
			"campaignId":   uuidPtr(msg.CampaignID),
			"createdAt":    msg.CreatedAt.Time,
			"dispatchedAt": dispatchedAtOrNil(msg.DispatchedAt),
		})
	})))

	mux.Handle("GET /v1/reports", authMw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		acc, _ := auth.FromContext(r.Context())
		from, to := parseRange(r)
		page, _ := strconv.Atoi(r.URL.Query().Get("page"))
		if page < 1 {
			page = 1
		}
		limit := 50
		filter := platch.ReportFilter{
			AccountID: acc.ID,
			From:      from,
			To:        to,
			Status:    r.URL.Query().Get("status"),
			Limit:     limit,
			Offset:    (page - 1) * limit,
		}
		if cid := r.URL.Query().Get("campaignId"); cid != "" {
			id, err := uuid.Parse(cid)
			if err != nil {
				httpx.Error(w, http.StatusBadRequest, "invalid campaignId")
				return
			}
			filter.CampaignID = &id
		}
		rows, err := ch.QueryReports(r.Context(), filter)
		if err != nil {
			httpx.Error(w, http.StatusInternalServerError, "query failed")
			return
		}
		httpx.WriteJSON(w, http.StatusOK, map[string]any{"items": rows, "page": page})
	})))

	mux.Handle("GET /v1/campaigns", authMw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		acc, _ := auth.FromContext(r.Context())
		page, _ := strconv.Atoi(r.URL.Query().Get("page"))
		if page < 1 {
			page = 1
		}
		limit := int32(50)
		rows, err := q.ListCampaignsByAccount(r.Context(), sqlc.ListCampaignsByAccountParams{
			AccountID: acc.ID,
			Limit:     limit,
			Offset:    int32((page - 1) * 50),
		})
		if err != nil {
			httpx.Error(w, http.StatusInternalServerError, "query failed")
			return
		}
		httpx.WriteJSON(w, http.StatusOK, map[string]any{"items": rows, "page": page})
	})))

	mux.Handle("GET /v1/campaigns/{id}", authMw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		acc, _ := auth.FromContext(r.Context())
		id, err := uuid.Parse(r.PathValue("id"))
		if err != nil {
			httpx.Error(w, http.StatusBadRequest, "invalid campaign id")
			return
		}
		camp, err := q.GetCampaignByID(r.Context(), sqlc.GetCampaignByIDParams{ID: id, AccountID: acc.ID})
		if errors.Is(err, pgx.ErrNoRows) {
			httpx.Error(w, http.StatusNotFound, "not found")
			return
		}
		if err != nil {
			httpx.Error(w, http.StatusInternalServerError, "internal error")
			return
		}
		httpx.WriteJSON(w, http.StatusOK, camp)
	})))

	mux.Handle("GET /v1/campaigns/{id}/report", authMw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		acc, _ := auth.FromContext(r.Context())
		id, err := uuid.Parse(r.PathValue("id"))
		if err != nil {
			httpx.Error(w, http.StatusBadRequest, "invalid campaign id")
			return
		}
		if _, err := q.GetCampaignByID(r.Context(), sqlc.GetCampaignByIDParams{ID: id, AccountID: acc.ID}); errors.Is(err, pgx.ErrNoRows) {
			httpx.Error(w, http.StatusNotFound, "not found")
			return
		} else if err != nil {
			httpx.Error(w, http.StatusInternalServerError, "internal error")
			return
		}
		agg, err := ch.AggregateCampaign(r.Context(), acc.ID, id)
		if err != nil {
			httpx.Error(w, http.StatusInternalServerError, "aggregate failed")
			return
		}
		httpx.WriteJSON(w, http.StatusOK, map[string]any{
			"totalRecipients":  agg.TotalRecipients,
			"sent":             agg.Sent,
			"failed":           agg.Failed,
			"expiredSlaMissed": agg.ExpiredSlaMissed,
			"pending":          agg.Pending,
			"totalCost":        agg.TotalCost,
			"refundedAmount":   agg.RefundedAmount,
		})
	})))

	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: mux}
	go func() {
		log.Printf("reporting-api listening on %s", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("reporting-api: listen: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("reporting-api: shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}

func parseRange(r *http.Request) (time.Time, time.Time) {
	now := time.Now().UTC()
	from := now.Add(-24 * time.Hour)
	to := now.Add(time.Hour)
	if v := r.URL.Query().Get("from"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			from = t
		}
	}
	if v := r.URL.Query().Get("to"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			to = t
		}
	}
	return from, to
}

func uuidPtr(id *uuid.UUID) any {
	if id == nil {
		return nil
	}
	return id.String()
}

func textOrEmpty(s string, valid bool) string {
	if !valid {
		return ""
	}
	return s
}

func dispatchedAtOrNil(t pgtype.Timestamptz) any {
	if !t.Valid {
		return nil
	}
	return t.Time
}
