// Command billing-consumer records durable ledger debits and refunds.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/amiri/sms-gateway/internal/config"
	"github.com/amiri/sms-gateway/internal/domain/billing"
	"github.com/amiri/sms-gateway/internal/domain/messaging"
	"github.com/amiri/sms-gateway/internal/platform/inbox"
	platkafka "github.com/amiri/sms-gateway/internal/platform/kafka"
	"github.com/amiri/sms-gateway/internal/platform/lifecycle"
	"github.com/amiri/sms-gateway/internal/platform/metrics"
	"github.com/amiri/sms-gateway/internal/platform/postgres"
	platredis "github.com/amiri/sms-gateway/internal/platform/redis"
	"github.com/google/uuid"
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

	metrics.Serve(env("METRICS_ADDR", ":9090"))

	dlqW := platkafka.NewWriter(cfg.KafkaBrokers, platkafka.TopicDLQ)
	defer dlqW.Close()

	normalR := platkafka.NewReader(cfg.KafkaBrokers, platkafka.TopicOutboundNormal, "billing-debit")
	expressR := platkafka.NewReader(cfg.KafkaBrokers, platkafka.TopicOutboundExpress, "billing-debit")
	resultsR := platkafka.NewReader(cfg.KafkaBrokers, platkafka.TopicDispatchResults, "billing-refund")
	defer normalR.Close()
	defer expressR.Close()
	defer resultsR.Close()

	log.Println("billing-consumer: started")
	onErr := func(err error) { log.Printf("billing-consumer: %v", err) }
	go platkafka.ConsumeLoopWithStore(ctx, normalR, dlqW, "billing-debit-normal", platkafka.DefaultMaxAttempts,
		platkafka.NewRedisAttemptStore(rdb.Raw(), "billing-debit-normal"),
		debitHandler(inboxStore, billingSvc, "billing-debit-normal"), onErr,
	)
	go platkafka.ConsumeLoopWithStore(ctx, expressR, dlqW, "billing-debit-express", platkafka.DefaultMaxAttempts,
		platkafka.NewRedisAttemptStore(rdb.Raw(), "billing-debit-express"),
		debitHandler(inboxStore, billingSvc, "billing-debit-express"), onErr,
	)
	platkafka.ConsumeLoopWithStore(ctx, resultsR, dlqW, "billing-refund", platkafka.DefaultMaxAttempts,
		platkafka.NewRedisAttemptStore(rdb.Raw(), "billing-refund"),
		refundHandler(inboxStore, billingSvc), onErr,
	)
	log.Println("billing-consumer: shutting down")
}

func debitHandler(inboxStore *inbox.Store, billingSvc *billing.Service, consumerName string) func(context.Context, kafkago.Message) (platkafka.HandleOutcome, error) {
	return func(ctx context.Context, msg kafkago.Message) (platkafka.HandleOutcome, error) {
		var ev messaging.AcceptedMessage
		if err := json.Unmarshal(msg.Value, &ev); err != nil {
			return platkafka.OutcomePoison, err
		}
		if err := recordDebit(ctx, inboxStore, billingSvc, consumerName, ev); err != nil {
			metrics.ConsumerHandleErrors.WithLabelValues(consumerName).Inc()
			return platkafka.OutcomeRetry, err
		}
		return platkafka.OutcomeOK, nil
	}
}

func refundHandler(inboxStore *inbox.Store, billingSvc *billing.Service) func(context.Context, kafkago.Message) (platkafka.HandleOutcome, error) {
	return func(ctx context.Context, msg kafkago.Message) (platkafka.HandleOutcome, error) {
		var ev messaging.DispatchResult
		if err := json.Unmarshal(msg.Value, &ev); err != nil {
			return platkafka.OutcomePoison, err
		}
		if ev.Status != "failed" && ev.Status != "expired_sla_missed" {
			return platkafka.OutcomeOK, nil
		}
		if err := recordRefund(ctx, inboxStore, billingSvc, ev); err != nil {
			metrics.ConsumerHandleErrors.WithLabelValues("billing-refund").Inc()
			return platkafka.OutcomeRetry, err
		}
		return platkafka.OutcomeOK, nil
	}
}

func recordDebit(ctx context.Context, inboxStore *inbox.Store, billingSvc *billing.Service, consumerName string, ev messaging.AcceptedMessage) error {
	tx, qtx, err := inboxStore.TryBegin(ctx, consumerName, ev.MessageID+":debit")
	if inbox.IsAlreadyProcessed(err) {
		metrics.InboxDuplicates.WithLabelValues(consumerName).Inc()
		return nil
	}
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	accountID, err := uuid.Parse(ev.AccountID)
	if err != nil {
		return fmt.Errorf("account id: %w", err)
	}
	messageID, err := uuid.Parse(ev.MessageID)
	if err != nil {
		return fmt.Errorf("message id: %w", err)
	}
	if err := billingSvc.RecordDebit(ctx, qtx, accountID, messageID, ev.Cost); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	priority := ev.Priority
	if priority == "" {
		priority = messaging.PriorityNormal
	}
	metrics.InboxProcessed.WithLabelValues(consumerName).Inc()
	metrics.LedgerDebits.WithLabelValues(priority).Inc()
	return nil
}

func recordRefund(ctx context.Context, inboxStore *inbox.Store, billingSvc *billing.Service, ev messaging.DispatchResult) error {
	const consumer = "billing-refund"
	accountID, err := uuid.Parse(ev.AccountID)
	if err != nil {
		return err
	}
	messageID, err := uuid.Parse(ev.MessageID)
	if err != nil {
		return err
	}

	tx, qtx, err := inboxStore.TryBegin(ctx, consumer, ev.MessageID+":refund")
	if inbox.IsAlreadyProcessed(err) {
		metrics.InboxDuplicates.WithLabelValues(consumer).Inc()
		// Commit-then-Redis-crash recovery: align Redis to ledger (includes refund).
		_, alignErr := billingSvc.AlignRedisToLedger(ctx, accountID)
		return alignErr
	}
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if err := billingSvc.RecordRefundLedger(ctx, qtx, accountID, messageID, ev.Cost); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	// Redis credit only after Inbox+ledger commit (never before).
	if err := billingSvc.CreditRedisAfterRefund(ctx, accountID, ev.Cost); err != nil {
		return err
	}
	metrics.InboxProcessed.WithLabelValues(consumer).Inc()
	metrics.LedgerRefunds.WithLabelValues(ev.Status).Inc()
	metrics.CreditsRefunded.WithLabelValues(ev.Status).Add(float64(ev.Cost))
	return nil
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}