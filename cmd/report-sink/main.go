// Command report-sink writes message lifecycle events to Postgres history and ClickHouse.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/amiri/sms-gateway/internal/config"
	"github.com/amiri/sms-gateway/internal/db/sqlc"
	"github.com/amiri/sms-gateway/internal/domain/messaging"
	platch "github.com/amiri/sms-gateway/internal/platform/clickhouse"
	"github.com/amiri/sms-gateway/internal/platform/inbox"
	platkafka "github.com/amiri/sms-gateway/internal/platform/kafka"
	"github.com/amiri/sms-gateway/internal/platform/lifecycle"
	"github.com/amiri/sms-gateway/internal/platform/metrics"
	"github.com/amiri/sms-gateway/internal/platform/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	kafkago "github.com/segmentio/kafka-go"
)

func main() {
	cfg := config.Load("report-sink")
	ctx, stop := lifecycle.WithShutdownSignal(context.Background())
	defer stop()

	pool, err := postgres.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("report-sink: postgres: %v", err)
	}
	defer pool.Close()

	ch, err := platch.NewWithPassword(ctx, cfg.ClickHouseAddr, cfg.ClickHousePassword)
	if err != nil {
		log.Fatalf("report-sink: clickhouse: %v", err)
	}
	defer ch.Close()

	inboxStore := inbox.New(pool)
	q := sqlc.New(pool)
	reader := platkafka.NewReader(cfg.KafkaBrokers, platkafka.TopicDispatchResults, "report-sink")
	defer reader.Close()
	dlqW := platkafka.NewWriter(cfg.KafkaBrokers, platkafka.TopicDLQ)
	defer dlqW.Close()

	metrics.Serve(env("METRICS_ADDR", ":9090"))

	log.Println("report-sink: started")
	platkafka.ConsumeLoop(ctx, reader, dlqW, "report-sink", platkafka.DefaultMaxAttempts,
		func(ctx context.Context, msg kafkago.Message) (platkafka.HandleOutcome, error) {
			err := handle(ctx, msg, inboxStore, q, ch)
			if err == nil {
				return platkafka.OutcomeOK, nil
			}
			var pe *json.SyntaxError
			if errors.As(err, &pe) {
				return platkafka.OutcomePoison, err
			}
			metrics.ConsumerHandleErrors.WithLabelValues("report-sink").Inc()
			return platkafka.OutcomeRetry, err
		},
		func(err error) { log.Printf("report-sink: %v", err) },
	)
	log.Println("report-sink: shutting down")
}

func handle(ctx context.Context, msg kafkago.Message, inboxStore *inbox.Store, q *sqlc.Queries, ch *platch.Client) error {
	var ev messaging.DispatchResult
	if err := json.Unmarshal(msg.Value, &ev); err != nil {
		return err
	}

	eventID := ev.MessageID + ":report"
	done, err := inboxStore.IsProcessed(ctx, "report-sink", eventID)
	if err != nil {
		return err
	}
	if done {
		metrics.InboxDuplicates.WithLabelValues("report-sink").Inc()
		return nil
	}

	messageID, err := uuid.Parse(ev.MessageID)
	if err != nil {
		return fmt.Errorf("message id: %w", err)
	}
	accountID, err := uuid.Parse(ev.AccountID)
	if err != nil {
		return fmt.Errorf("account id: %w", err)
	}

	createdAt := pgtype.Timestamptz{Time: ev.CreatedAt, Valid: !ev.CreatedAt.IsZero()}
	if !createdAt.Valid {
		createdAt = pgtype.Timestamptz{Time: ev.AcceptedAt, Valid: true}
	}
	op := pgtype.Text{String: ev.Operator, Valid: ev.Operator != ""}
	dispatchedAt := pgtype.Timestamptz{Time: ev.DispatchedAt, Valid: !ev.DispatchedAt.IsZero()}

	// 1) Postgres writes (retry-safe) without Inbox mark yet.
	tx, err := inboxStore.Pool().Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	qtx := q.WithTx(tx)

	n, err := qtx.UpdateMessageStatus(ctx, sqlc.UpdateMessageStatusParams{
		ID:           messageID,
		CreatedAt:    createdAt,
		Status:       ev.Status,
		Operator:     op,
		DispatchedAt: dispatchedAt,
	})
	if err != nil {
		return fmt.Errorf("update status: %w", err)
	}
	if n == 0 {
		// Message row may not exist yet if dispatcher raced; retry.
		return fmt.Errorf("update status: no row for %s", messageID)
	}
	if err := qtx.InsertMessageStatusEvent(ctx, sqlc.InsertMessageStatusEventParams{
		MessageID:  messageID,
		Status:     ev.Status,
		OccurredAt: pgtype.Timestamptz{Time: time.Now().UTC(), Valid: true},
	}); err != nil {
		return fmt.Errorf("status event: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}

	var campaignID *uuid.UUID
	if ev.CampaignID != "" {
		if cid, err := uuid.Parse(ev.CampaignID); err == nil {
			campaignID = &cid
		}
	}
	// 2) ClickHouse (idempotent ensure).
	if err := ch.EnsureMessageEvent(ctx, platch.MessageEvent{
		EventTime:  ev.DispatchedAt,
		MessageID:  messageID,
		AccountID:  accountID,
		CampaignID: campaignID,
		Recipient:  ev.To,
		Priority:   ev.Priority,
		Status:     ev.Status,
		Cost:       ev.Cost,
		Operator:   ev.Operator,
	}); err != nil {
		return err
	}

	// 3) Mark Inbox only after PG + CH succeeded.
	if err := inboxStore.Mark(ctx, "report-sink", eventID); err != nil {
		return err
	}
	metrics.InboxProcessed.WithLabelValues("report-sink").Inc()
	metrics.ReportEvents.WithLabelValues(ev.Status).Inc()
	return nil
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
