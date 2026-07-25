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
	dlqW := platkafka.NewWriter(cfg.KafkaBrokers, platkafka.TopicDLQ)
	defer dlqW.Close()

	mockURL := env("OPERATOR_URL", "http://operator-mock:8080")
	fallback := operator.NewHTTPAdapter("default", mockURL)
	router := operator.NewRouter(fallback, []operator.Adapter{
		fallback,
		operator.NewHTTPAdapter("mci", env("OPERATOR_URL_MCI", mockURL)),
		operator.NewHTTPAdapter("irancell", env("OPERATOR_URL_IRANCELL", mockURL)),
		operator.NewHTTPAdapter("rightel", env("OPERATOR_URL_RIGHTEL", mockURL)),
	}, operator.DefaultIranRules())

	metrics.Serve(env("METRICS_ADDR", ":9090"))
	consumer := "dispatcher-" + *mode
	log.Printf("dispatcher: started mode=%s topic=%s", *mode, topic)

	platkafka.ConsumeLoop(ctx, reader, dlqW, consumer, platkafka.DefaultMaxAttempts,
		func(ctx context.Context, msg kafkago.Message) (platkafka.HandleOutcome, error) {
			err := handle(ctx, *mode, msg, inboxStore, q, resultsW, router)
			if err == nil {
				return platkafka.OutcomeOK, nil
			}
			var pe *json.SyntaxError
			if errors.As(err, &pe) || errors.Is(err, errPoison) {
				return platkafka.OutcomePoison, err
			}
			metrics.ConsumerHandleErrors.WithLabelValues(consumer).Inc()
			return platkafka.OutcomeRetry, err
		},
		func(err error) { log.Printf("dispatcher: %v", err) },
	)
	log.Println("dispatcher: shutting down")
}

var errPoison = errors.New("poison message")

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
		return fmt.Errorf("%w: %v", errPoison, err)
	}
	if ev.MessageID == "" || ev.AccountID == "" {
		return fmt.Errorf("%w: missing messageId/accountId", errPoison)
	}

	consumer := "dispatcher-" + mode
	eventID := ev.MessageID + ":dispatch"

	// Already finished: republish dispatch-results (fixes commit-then-publish crash).
	done, err := inboxStore.IsProcessed(ctx, consumer, eventID)
	if err != nil {
		return err
	}
	if done {
		metrics.InboxDuplicates.WithLabelValues(consumer).Inc()
		return republishFromDB(ctx, q, resultsW, ev)
	}

	// Operator call OUTSIDE any Postgres transaction.
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
		return fmt.Errorf("%w: message id: %v", errPoison, err)
	}
	accountID, err := uuid.Parse(ev.AccountID)
	if err != nil {
		return fmt.Errorf("%w: account id: %v", errPoison, err)
	}

	createdAt := pgtype.Timestamptz{Time: ev.AcceptedAt, Valid: true}
	var campaignID *uuid.UUID
	if ev.CampaignID != "" {
		if cid, err := uuid.Parse(ev.CampaignID); err == nil {
			campaignID = &cid
		}
	}
	var deadlineAt pgtype.Timestamptz
	if ev.Deadline != "" {
		if d, err := time.Parse(time.RFC3339Nano, ev.Deadline); err == nil {
			deadlineAt = pgtype.Timestamptz{Time: d, Valid: true}
		}
	}

	tx, qtx, err := inboxStore.TryBegin(ctx, consumer, eventID)
	if inbox.IsAlreadyProcessed(err) {
		metrics.InboxDuplicates.WithLabelValues(consumer).Inc()
		return republishFromDB(ctx, q, resultsW, ev)
	}
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

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
	n, err := qtx.UpdateMessageStatus(ctx, sqlc.UpdateMessageStatusParams{
		ID:           messageID,
		CreatedAt:    createdAt,
		Status:       status,
		Operator:     opText,
		DispatchedAt: dispatchedAt,
	})
	if err != nil {
		return fmt.Errorf("update status: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("update status: no row for message %s created_at=%s", messageID, ev.AcceptedAt)
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
	return publishResult(ctx, resultsW, result)
}

func republishFromDB(ctx context.Context, q *sqlc.Queries, resultsW *kafkago.Writer, ev messaging.AcceptedMessage) error {
	messageID, err := uuid.Parse(ev.MessageID)
	if err != nil {
		return err
	}
	row, err := q.GetMessageByID(ctx, messageID)
	if err != nil {
		return fmt.Errorf("republish load message: %w", err)
	}
	var campaignID string
	if row.CampaignID != nil {
		campaignID = row.CampaignID.String()
	}
	dispatchedAt := time.Now().UTC()
	if row.DispatchedAt.Valid {
		dispatchedAt = row.DispatchedAt.Time
	}
	op := ""
	if row.Operator.Valid {
		op = row.Operator.String
	}
	result := messaging.DispatchResult{
		MessageID:    ev.MessageID,
		AccountID:    ev.AccountID,
		CampaignID:   campaignID,
		To:           row.Recipient,
		Text:         ev.Text,
		Priority:     row.Priority,
		Cost:         row.Cost,
		Status:       row.Status,
		Operator:     op,
		AcceptedAt:   ev.AcceptedAt,
		DispatchedAt: dispatchedAt,
		CreatedAt:    row.CreatedAt.Time,
	}
	return publishResult(ctx, resultsW, result)
}

func publishResult(ctx context.Context, resultsW *kafkago.Writer, result messaging.DispatchResult) error {
	payload, err := json.Marshal(result)
	if err != nil {
		return err
	}
	return platkafka.Publish(ctx, resultsW, []byte(result.AccountID), payload)
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
