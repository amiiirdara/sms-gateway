// Command outbox-relay drains Redis outbox streams into Kafka (ARCHITECTURE.md section 5.1).
package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/amiri/sms-gateway/internal/config"
	"github.com/amiri/sms-gateway/internal/domain/messaging"
	platkafka "github.com/amiri/sms-gateway/internal/platform/kafka"
	"github.com/amiri/sms-gateway/internal/platform/lifecycle"
	platredis "github.com/amiri/sms-gateway/internal/platform/redis"
	goredis "github.com/redis/go-redis/v9"
	kafkago "github.com/segmentio/kafka-go"
)

func main() {
	cfg := config.Load("outbox-relay")
	ctx, stop := lifecycle.WithShutdownSignal(context.Background())
	defer stop()

	rdb, err := platredis.NewClient(ctx, cfg.RedisAddr)
	if err != nil {
		log.Fatalf("outbox-relay: redis: %v", err)
	}
	defer rdb.Close()

	if err := platkafka.EnsureTopics(ctx, cfg.KafkaBrokers,
		platkafka.TopicOutboundNormal, platkafka.TopicOutboundExpress,
		platkafka.TopicDispatchResults, platkafka.TopicDLQ,
	); err != nil {
		log.Printf("outbox-relay: ensure topics (continuing): %v", err)
	}

	normalW := platkafka.NewWriter(cfg.KafkaBrokers, platkafka.TopicOutboundNormal)
	expressW := platkafka.NewWriter(cfg.KafkaBrokers, platkafka.TopicOutboundExpress)
	defer normalW.Close()
	defer expressW.Close()

	hostname, _ := os.Hostname()
	consumer, err := platredis.NewOutboxConsumer(ctx, rdb, platredis.OutboxMessagesStream, "outbox-relay", hostname)
	if err != nil {
		log.Fatalf("outbox-relay: consumer: %v", err)
	}

	log.Println("outbox-relay: started")
	for {
		select {
		case <-ctx.Done():
			log.Println("outbox-relay: shutting down")
			return
		default:
		}

		if msgs, err := consumer.ClaimStale(ctx, 30*time.Second, 10); err == nil {
			for _, m := range msgs {
				if err := publishOne(ctx, m, normalW, expressW); err != nil {
					log.Printf("outbox-relay: claim publish: %v", err)
					continue
				}
				_ = consumer.Ack(ctx, m.ID)
			}
		}

		msgs, err := consumer.Read(ctx, 10, 2*time.Second)
		if err != nil {
			log.Printf("outbox-relay: read: %v", err)
			time.Sleep(time.Second)
			continue
		}
		for _, m := range msgs {
			if err := publishOne(ctx, m, normalW, expressW); err != nil {
				log.Printf("outbox-relay: publish: %v", err)
				continue
			}
			if err := consumer.Ack(ctx, m.ID); err != nil {
				log.Printf("outbox-relay: ack: %v", err)
			}
		}
	}
}

func publishOne(ctx context.Context, m goredis.XMessage, normalW, expressW *kafkago.Writer) error {
	f := m.Values
	get := func(k string) string {
		if v, ok := f[k]; ok {
			if s, ok := v.(string); ok {
				return s
			}
		}
		return ""
	}
	cost, _ := strconv.ParseInt(get("cost"), 10, 64)
	acceptedAt, _ := time.Parse(time.RFC3339Nano, get("accepted_at"))
	ev := messaging.AcceptedMessage{
		MessageID:  get("message_id"),
		AccountID:  get("account_id"),
		CampaignID: get("campaign_id"),
		To:         get("to"),
		Text:       get("text"),
		Priority:   get("priority"),
		Cost:       cost,
		Deadline:   get("deadline"),
		AcceptedAt: acceptedAt,
	}
	payload, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	w := normalW
	if ev.Priority == messaging.PriorityExpress {
		w = expressW
	}
	return platkafka.Publish(ctx, w, []byte(ev.AccountID), payload)
}
