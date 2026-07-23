// Command dispatcher consumes outbound SMS topics and calls routed operators.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/amiri/sms-gateway/internal/config"
	"github.com/amiri/sms-gateway/internal/db/sqlc"
	"github.com/amiri/sms-gateway/internal/domain/messaging"
	"github.com/amiri/sms-gateway/internal/domain/messaging/operator"
	"github.com/amiri/sms-gateway/internal/platform/inbox"
	platkafka "github.com/amiri/sms-gateway/internal/platform/kafka"
	"github.com/amiri/sms-gateway/internal/platform/lifecycle"
	"github.com/amiri/sms-gateway/internal/platform/metrics"
	"github.com/amiri/sms-gateway/internal/platform/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	kafkago "github.com/segmentio/kafka-go"
)

func main() {
	mode := flag.String("mode", "normal", "dispatcher mode: normal|express")
	flag.Parse()
	if *mode != "normal" && *mode != "express" {
		log.Fatalf("dispatcher: invalid mode %q", *mode)
	}

	cfg := config.Load("dispatcher-" + *mode)
	ctx, stop := lifecycle.WithShutdownSignal(context.Background())
	defer stop()

	pool, err := postgres.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("dispatcher: postgres: %v", err)
	}
	defer pool.Close()
	inboxStore := inbox.New(pool)
	q := sqlc.New(pool)

	topic := platkafka.TopicOutboundNormal
	if *mode == "express" {
		topic = platkafka.TopicOutboundExpress
	}
	reader := platkafka.NewReader(cfg.KafkaBrokers, topic, "dispatcher-"+*mode)
	defer reader.Close()
	resultsW := platkafka.NewWriter(cfg.KafkaBrokers, platkafka.TopicDispatchResults)
	defer resultsW.Close()

	// Local demo: all named MNOs point at the same operator-mock; swap URLs in prod.
	mockURL := env("OPERATOR_URL", "http://operator-mock:8080")
	fallback := operator.NewHTTPAdapter("default", mockURL)
	router := operator.NewRouter(fallback, []operator.Adapter{
		fallback,
		operator.NewHTTPAdapter("mci", env("OPERATOR_URL_MCI", mockURL)),
		operator.NewHTTPAdapter("irancell", env("OPERATOR_URL_IRANCELL", mockURL)),
		operator.NewHTTPAdapter("rightel", env("OPERATOR_URL_RIGHTEL", mockURL)),
	}, operator.DefaultIranRules())

	metrics.Serve(env("METRICS_ADDR", ":9090"))

	log.Printf("dispatcher: started mode=%s topic=%s", *mode, topic)
	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Println("dispatcher: shutting down")
				return
			}
			log.Printf("dispatcher: fetch: %v", err)
			time.Sleep(time.Second)
			continue
		}
		if err := handle(ctx, *mode, msg, inboxStore, q, resultsW, router); err != nil {
			metrics.ConsumerHandleErrors.WithLabelValues("dispatcher-"+*mode).Inc()
			log.Printf("dispatcher: handle: %v", err)
			continue
		}
		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("dispatcher: commit: %v", err)
		}
	}
}

func handle(
	ctx context.Context,
	mode string,
	msg kafkago.Message,
	inboxStore *inbox.Store,
	q *sqlc.Queries,
	resultsW *kafkago.Writer,
	router *operator.Router,
) error {
	var ev messaging.AcceptedMessage
	if err := json.Unmarshal(msg.Value, &ev); err != nil {
		return err
	}
	eventID := ev.MessageID + ":dispatch"

	consumer := "dispatcher-" + mode
	tx, qtx, err := inboxStore.TryBegin(ctx, consumer, eventID)
	if inbox.IsAlreadyProcessed(err) {
		metrics.InboxDuplicates.WithLabelValues(consumer).Inc()
		return nil
	}
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	now := time.Now().UTC()
	status := "sent"
	operatorName := ""

	if mode == "express" && messaging.ExpressExpired(ev.Deadline, now) {
		status = "expired_sla_missed"
		metrics.ExpressSLAMissed.Inc()
		metrics.OperatorSendDuration.WithLabelValues("none", "skipped_sla").Observe(0)
	} else {
		opStart := time.Now()
		name, sendErr := router.Send(ctx, ev.To, ev.Text, ev.Priority)
		operatorName = name
		if sendErr != nil {
			status = "failed"
			metrics.OperatorSendDuration.WithLabelValues(operatorName, "error").Observe(time.Since(opStart).Seconds())
		} else {
			metrics.OperatorSendDuration.WithLabelValues(operatorName, "ok").Observe(time.Since(opStart).Seconds())
		}
	}

	messageID, err := uuid.Parse(ev.MessageID)
	if err != nil {
		return err
	}
	accountID, err := uuid.Parse(ev.AccountID)
	if err != nil {
		return err
	}

	createdAt := pgtype.Timestamptz{Time: ev.AcceptedAt, Valid: true}
	var campaignID *uuid.UUID
	if ev.CampaignID != "" {
		cid, err := uuid.Parse(ev.CampaignID)
		if err == nil {
			campaignID = &cid
		}
	}
	var deadlineAt pgtype.Timestamptz
	if ev.Deadline != "" {
		if d, err := time.Parse(time.RFC3339Nano, ev.Deadline); err == nil {
			deadlineAt = pgtype.Timestamptz{Time: d, Valid: true}
		}
	}

	_, err = qtx.CreateMessage(ctx, sqlc.CreateMessageParams{
		ID:         messageID,
		AccountID:  accountID,
		CampaignID: campaignID,
		Recipient:  ev.To,
		Priority:   ev.Priority,
		Cost:       ev.Cost,
		Status:     "accepted",
		DeadlineAt: deadlineAt,
		CreatedAt:  createdAt,
	})
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("create message: %w", err)
	}

	opText := pgtype.Text{String: operatorName, Valid: operatorName != ""}
	dispatchedAt := pgtype.Timestamptz{Time: now, Valid: true}
	if err := qtx.UpdateMessageStatus(ctx, sqlc.UpdateMessageStatusParams{
		ID:           messageID,
		CreatedAt:    createdAt,
		Status:       status,
		Operator:     opText,
		DispatchedAt: dispatchedAt,
	}); err != nil {
		return fmt.Errorf("update status: %w", err)
	}
	if err := qtx.InsertMessageStatusEvent(ctx, sqlc.InsertMessageStatusEventParams{
		MessageID:  messageID,
		Status:     status,
		OccurredAt: pgtype.Timestamptz{Time: now, Valid: true},
	}); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}

	metrics.InboxProcessed.WithLabelValues(consumer).Inc()
	metrics.DispatchTotal.WithLabelValues(mode, status, operatorName).Inc()
	if !ev.AcceptedAt.IsZero() {
		metrics.DispatchLatency.WithLabelValues(mode, ev.Priority).Observe(now.Sub(ev.AcceptedAt).Seconds())
	}

	result := messaging.DispatchResult{
		MessageID:    ev.MessageID,
		AccountID:    ev.AccountID,
		CampaignID:   ev.CampaignID,
		To:           ev.To,
		Text:         ev.Text,
		Priority:     ev.Priority,
		Cost:         ev.Cost,
		Status:       status,
		Operator:     operatorName,
		AcceptedAt:   ev.AcceptedAt,
		DispatchedAt: now,
		CreatedAt:    ev.AcceptedAt,
	}
	payload, err := json.Marshal(result)
	if err != nil {
		return err
	}
	return platkafka.Publish(ctx, resultsW, []byte(ev.AccountID), payload)
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
