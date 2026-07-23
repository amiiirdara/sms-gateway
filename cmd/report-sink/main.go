// Command report-sink writes message lifecycle events to Postgres history and ClickHouse.
package main

import (
	"context"
	"encoding/json"
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

	ch, err := platch.New(ctx, cfg.ClickHouseAddr)
	if err != nil {
		log.Fatalf("report-sink: clickhouse: %v", err)
	}
	defer ch.Close()

	inboxStore := inbox.New(pool)
	reader := platkafka.NewReader(cfg.KafkaBrokers, platkafka.TopicDispatchResults, "report-sink")
	defer reader.Close()

	metrics.Serve(env("METRICS_ADDR", ":9090"))

	log.Println("report-sink: started")
	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Println("report-sink: shutting down")
				return
			}
			log.Printf("report-sink: fetch: %v", err)
			time.Sleep(time.Second)
			continue
		}
		if err := handle(ctx, msg, inboxStore, ch); err != nil {
			metrics.ConsumerHandleErrors.WithLabelValues("report-sink").Inc()
			log.Printf("report-sink: handle: %v", err)
			continue
		}
		_ = reader.CommitMessages(ctx, msg)
	}
}

func handle(ctx context.Context, msg kafkago.Message, inboxStore *inbox.Store, ch *platch.Client) error {
	var ev messaging.DispatchResult
	if err := json.Unmarshal(msg.Value, &ev); err != nil {
		return err
	}

	tx, qtx, err := inboxStore.TryBegin(ctx, "report-sink", ev.MessageID+":report")
	if inbox.IsAlreadyProcessed(err) {
		metrics.InboxDuplicates.WithLabelValues("report-sink").Inc()
		return nil
	}
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	messageID, err := uuid.Parse(ev.MessageID)
	if err != nil {
		return err
	}
	accountID, err := uuid.Parse(ev.AccountID)
	if err != nil {
		return err
	}

	createdAt := pgtype.Timestamptz{Time: ev.CreatedAt, Valid: !ev.CreatedAt.IsZero()}
	if !createdAt.Valid {
		createdAt = pgtype.Timestamptz{Time: ev.AcceptedAt, Valid: true}
	}
	op := pgtype.Text{String: ev.Operator, Valid: ev.Operator != ""}
	dispatchedAt := pgtype.Timestamptz{Time: ev.DispatchedAt, Valid: !ev.DispatchedAt.IsZero()}

	_ = qtx.UpdateMessageStatus(ctx, sqlc.UpdateMessageStatusParams{
		ID:           messageID,
		CreatedAt:    createdAt,
		Status:       ev.Status,
		Operator:     op,
		DispatchedAt: dispatchedAt,
	})
	_ = qtx.InsertMessageStatusEvent(ctx, sqlc.InsertMessageStatusEventParams{
		MessageID:  messageID,
		Status:     ev.Status,
		OccurredAt: pgtype.Timestamptz{Time: time.Now().UTC(), Valid: true},
	})

	var campaignID *uuid.UUID
	if ev.CampaignID != "" {
		cid, err := uuid.Parse(ev.CampaignID)
		if err == nil {
			campaignID = &cid
		}
	}
	if err := ch.InsertMessageEvent(ctx, platch.MessageEvent{
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
	if err := tx.Commit(ctx); err != nil {
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
