// Command campaign-expander fans out accepted campaigns into per-recipient messages.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/amiri/sms-gateway/internal/config"
	"github.com/amiri/sms-gateway/internal/db/sqlc"
	"github.com/amiri/sms-gateway/internal/domain/campaigns"
	"github.com/amiri/sms-gateway/internal/domain/messaging"
	platkafka "github.com/amiri/sms-gateway/internal/platform/kafka"
	"github.com/amiri/sms-gateway/internal/platform/lifecycle"
	"github.com/amiri/sms-gateway/internal/platform/metrics"
	"github.com/amiri/sms-gateway/internal/platform/postgres"
	platredis "github.com/amiri/sms-gateway/internal/platform/redis"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"
	kafkago "github.com/segmentio/kafka-go"
)

func main() {
	cfg := config.Load("campaign-expander")
	ctx, stop := lifecycle.WithShutdownSignal(context.Background())
	defer stop()

	pool, err := postgres.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("campaign-expander: postgres: %v", err)
	}
	defer pool.Close()
	rdb, err := platredis.NewClient(ctx, cfg.RedisAddr)
	if err != nil {
		log.Fatalf("campaign-expander: redis: %v", err)
	}
	defer rdb.Close()

	normalW := platkafka.NewWriter(cfg.KafkaBrokers, platkafka.TopicOutboundNormal)
	defer normalW.Close()
	q := sqlc.New(pool)

	hostname, _ := os.Hostname()
	consumer, err := platredis.NewOutboxConsumer(ctx, rdb, platredis.OutboxCampaignsStream, "campaign-expander", hostname)
	if err != nil {
		log.Fatalf("campaign-expander: consumer: %v", err)
	}

	log.Println("campaign-expander: started")
	metrics.Serve(env("METRICS_ADDR", ":9090"))
	for {
		select {
		case <-ctx.Done():
			log.Println("campaign-expander: shutting down")
			return
		default:
		}

		if msgs, err := consumer.ClaimStale(ctx, 30*time.Second, 5); err == nil {
			for _, m := range msgs {
				if err := expandOne(ctx, pool, q, normalW, m); err != nil {
					log.Printf("campaign-expander: claim expand: %v", err)
					continue
				}
				_ = consumer.Ack(ctx, m.ID)
			}
		}

		msgs, err := consumer.Read(ctx, 5, 2*time.Second)
		if err != nil {
			log.Printf("campaign-expander: read: %v", err)
			time.Sleep(time.Second)
			continue
		}
		for _, m := range msgs {
			if err := expandOne(ctx, pool, q, normalW, m); err != nil {
				log.Printf("campaign-expander: expand: %v", err)
				continue
			}
			_ = consumer.Ack(ctx, m.ID)
		}
	}
}

func field(m goredis.XMessage, k string) string {
	if v, ok := m.Values[k]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func expandOne(ctx context.Context, pool *pgxpool.Pool, q *sqlc.Queries, normalW *kafkago.Writer, m goredis.XMessage) error {
	campaignID, err := uuid.Parse(field(m, "campaign_id"))
	if err != nil {
		return err
	}
	accountID, err := uuid.Parse(field(m, "account_id"))
	if err != nil {
		return err
	}
	text := field(m, "text")
	acceptedAt, _ := time.Parse(time.RFC3339Nano, field(m, "accepted_at"))
	costPerMsg, _ := strconv.ParseInt(field(m, "cost_per_message"), 10, 64)
	totalCost, _ := strconv.ParseInt(field(m, "total_cost"), 10, 64)

	var recipients []string
	if err := json.Unmarshal([]byte(field(m, "recipients")), &recipients); err != nil {
		return err
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	qtx := q.WithTx(tx)

	_, err = qtx.CreateCampaign(ctx, sqlc.CreateCampaignParams{
		ID:              campaignID,
		AccountID:       accountID,
		Text:            text,
		TotalRecipients: int32(len(recipients)),
		CostPerMessage:  costPerMsg,
		TotalCost:       totalCost,
		Status:          "accepted",
	})
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}

	createdAt := pgtype.Timestamptz{Time: acceptedAt, Valid: true}
	cid := campaignID
	for i, recipient := range recipients {
		msgID := campaigns.DeterministicMessageID(campaignID, i)
		_, err := qtx.CreateMessage(ctx, sqlc.CreateMessageParams{
			ID:         msgID,
			AccountID:  accountID,
			CampaignID: &cid,
			Recipient:  recipient,
			Priority:   messaging.PriorityNormal,
			Cost:       costPerMsg,
			Status:     "accepted",
			CreatedAt:  createdAt,
		})
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
	}
	if err := qtx.UpdateCampaignStatus(ctx, sqlc.UpdateCampaignStatusParams{
		ID:     campaignID,
		Status: "expanded",
	}); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}

	for i, recipient := range recipients {
		msgID := campaigns.DeterministicMessageID(campaignID, i)
		ev := messaging.AcceptedMessage{
			MessageID:  msgID.String(),
			AccountID:  accountID.String(),
			CampaignID: campaignID.String(),
			To:         recipient,
			Text:       text,
			Priority:   messaging.PriorityNormal,
			Cost:       costPerMsg,
			AcceptedAt: acceptedAt,
		}
		payload, err := json.Marshal(ev)
		if err != nil {
			return err
		}
		if err := platkafka.Publish(ctx, normalW, []byte(accountID.String()), payload); err != nil {
			return err
		}
	}
	metrics.CampaignsExpanded.Inc()
	metrics.CampaignMessagesExpanded.Add(float64(len(recipients)))
	return nil
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
