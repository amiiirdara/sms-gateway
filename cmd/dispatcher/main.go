// Command dispatcher consumes outbound SMS topics and calls the operator mock.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/amiri/sms-gateway/internal/config"
	"github.com/amiri/sms-gateway/internal/db/sqlc"
	"github.com/amiri/sms-gateway/internal/domain/messaging"
	"github.com/amiri/sms-gateway/internal/platform/inbox"
	platkafka "github.com/amiri/sms-gateway/internal/platform/kafka"
	"github.com/amiri/sms-gateway/internal/platform/lifecycle"
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

	operatorURL := env("OPERATOR_URL", "http://operator-mock:8080")
	httpClient := &http.Client{Timeout: 5 * time.Second}

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
		if err := handle(ctx, *mode, msg, inboxStore, q, resultsW, operatorURL, httpClient); err != nil {
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
	operatorURL string,
	httpClient *http.Client,
) error {
	var ev messaging.AcceptedMessage
	if err := json.Unmarshal(msg.Value, &ev); err != nil {
		return err
	}
	eventID := ev.MessageID + ":dispatch"

	tx, qtx, err := inboxStore.TryBegin(ctx, "dispatcher-"+mode, eventID)
	if inbox.IsAlreadyProcessed(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	now := time.Now().UTC()
	status := "sent"
	operator := "mock"

	if mode == "express" && messaging.ExpressExpired(ev.Deadline, now) {
		status = "expired_sla_missed"
		operator = ""
	}

	if status != "expired_sla_missed" {
		if err := callOperator(ctx, httpClient, operatorURL, ev); err != nil {
			status = "failed"
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

	opText := pgtype.Text{String: operator, Valid: operator != ""}
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

	result := messaging.DispatchResult{
		MessageID:    ev.MessageID,
		AccountID:    ev.AccountID,
		CampaignID:   ev.CampaignID,
		To:           ev.To,
		Text:         ev.Text,
		Priority:     ev.Priority,
		Cost:         ev.Cost,
		Status:       status,
		Operator:     operator,
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

func callOperator(ctx context.Context, client *http.Client, baseURL string, ev messaging.AcceptedMessage) error {
	body, _ := json.Marshal(map[string]string{
		"to":       ev.To,
		"text":     ev.Text,
		"priority": ev.Priority,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/send", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("operator status %d", resp.StatusCode)
	}
	return nil
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
