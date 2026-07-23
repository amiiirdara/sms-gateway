// Command billing-consumer records durable ledger debits and refunds.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/amiri/sms-gateway/internal/config"
	"github.com/amiri/sms-gateway/internal/domain/billing"
	"github.com/amiri/sms-gateway/internal/domain/messaging"
	"github.com/amiri/sms-gateway/internal/platform/inbox"
	platkafka "github.com/amiri/sms-gateway/internal/platform/kafka"
	"github.com/amiri/sms-gateway/internal/platform/lifecycle"
	"github.com/amiri/sms-gateway/internal/platform/postgres"
	platredis "github.com/amiri/sms-gateway/internal/platform/redis"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	kafkago "github.com/segmentio/kafka-go"
)

func main() {
	cfg := config.Load("billing-consumer")
	ctx, stop := lifecycle.WithShutdownSignal(context.Background())
	defer stop()

	pool, err := postgres.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("billing-consumer: postgres: %v", err)
	}
	defer pool.Close()
	rdb, err := platredis.NewClient(ctx, cfg.RedisAddr)
	if err != nil {
		log.Fatalf("billing-consumer: redis: %v", err)
	}
	defer rdb.Close()

	billingSvc := billing.New(pool, rdb)
	inboxStore := inbox.New(pool)

	// Debits come from accepted messages on outbound topics; refunds from dispatch-results.
	normalR := platkafka.NewReader(cfg.KafkaBrokers, platkafka.TopicOutboundNormal, "billing-debit")
	expressR := platkafka.NewReader(cfg.KafkaBrokers, platkafka.TopicOutboundExpress, "billing-debit")
	resultsR := platkafka.NewReader(cfg.KafkaBrokers, platkafka.TopicDispatchResults, "billing-refund")
	defer normalR.Close()
	defer expressR.Close()
	defer resultsR.Close()

	log.Println("billing-consumer: started")
	go consumeDebits(ctx, normalR, inboxStore, billingSvc, "billing-debit-normal")
	go consumeDebits(ctx, expressR, inboxStore, billingSvc, "billing-debit-express")
	consumeRefunds(ctx, resultsR, inboxStore, billingSvc)
}

func consumeDebits(ctx context.Context, reader *kafkago.Reader, inboxStore *inbox.Store, billingSvc *billing.Service, consumerName string) {
	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("billing-consumer: debit fetch: %v", err)
			time.Sleep(time.Second)
			continue
		}
		var ev messaging.AcceptedMessage
		if err := json.Unmarshal(msg.Value, &ev); err != nil {
			log.Printf("billing-consumer: unmarshal: %v", err)
			_ = reader.CommitMessages(ctx, msg)
			continue
		}
		if err := recordDebit(ctx, inboxStore, billingSvc, consumerName, ev); err != nil {
			log.Printf("billing-consumer: debit: %v", err)
			continue
		}
		_ = reader.CommitMessages(ctx, msg)
	}
}

func recordDebit(ctx context.Context, inboxStore *inbox.Store, billingSvc *billing.Service, consumerName string, ev messaging.AcceptedMessage) error {
	tx, qtx, err := inboxStore.TryBegin(ctx, consumerName, ev.MessageID+":debit")
	if inbox.IsAlreadyProcessed(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	accountID, err := uuid.Parse(ev.AccountID)
	if err != nil {
		return err
	}
	messageID, err := uuid.Parse(ev.MessageID)
	if err != nil {
		return err
	}
	if err := billingSvc.RecordDebit(ctx, qtx, accountID, messageID, ev.Cost); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func consumeRefunds(ctx context.Context, reader *kafkago.Reader, inboxStore *inbox.Store, billingSvc *billing.Service) {
	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Println("billing-consumer: shutting down")
				return
			}
			log.Printf("billing-consumer: refund fetch: %v", err)
			time.Sleep(time.Second)
			continue
		}
		var ev messaging.DispatchResult
		if err := json.Unmarshal(msg.Value, &ev); err != nil {
			_ = reader.CommitMessages(ctx, msg)
			continue
		}
		if ev.Status != "failed" && ev.Status != "expired_sla_missed" {
			_ = reader.CommitMessages(ctx, msg)
			continue
		}
		if err := recordRefund(ctx, inboxStore, billingSvc, ev); err != nil && !errors.Is(err, pgx.ErrNoRows) {
			log.Printf("billing-consumer: refund: %v", err)
			continue
		}
		_ = reader.CommitMessages(ctx, msg)
	}
}

func recordRefund(ctx context.Context, inboxStore *inbox.Store, billingSvc *billing.Service, ev messaging.DispatchResult) error {
	tx, qtx, err := inboxStore.TryBegin(ctx, "billing-refund", ev.MessageID+":refund")
	if inbox.IsAlreadyProcessed(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	accountID, err := uuid.Parse(ev.AccountID)
	if err != nil {
		return err
	}
	messageID, err := uuid.Parse(ev.MessageID)
	if err != nil {
		return err
	}
	if err := billingSvc.RecordRefund(ctx, qtx, accountID, messageID, ev.Cost); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
