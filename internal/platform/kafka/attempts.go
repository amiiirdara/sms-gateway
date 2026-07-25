package kafka

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/amiri/sms-gateway/internal/platform/metrics"
	goredis "github.com/redis/go-redis/v9"
	kafkago "github.com/segmentio/kafka-go"
)

const attemptKeyTTL = 24 * time.Hour

// AttemptStore persists per-offset retry counts (survives process restart).
type AttemptStore interface {
	Inc(ctx context.Context, msg kafkago.Message) (int, error)
	Clear(ctx context.Context, msg kafkago.Message) error
}

// RedisAttemptStore stores attempt counts in Redis.
type RedisAttemptStore struct {
	rdb    *goredis.Client
	prefix string
}

// NewRedisAttemptStore creates a durable attempt counter.
func NewRedisAttemptStore(rdb *goredis.Client, consumerName string) *RedisAttemptStore {
	return &RedisAttemptStore{rdb: rdb, prefix: "kafka:attempts:" + consumerName + ":"}
}

func (s *RedisAttemptStore) key(msg kafkago.Message) string {
	return s.prefix + msg.Topic + ":" + strconv.Itoa(msg.Partition) + ":" + strconv.FormatInt(msg.Offset, 10)
}

// Inc increments and returns the new attempt count.
func (s *RedisAttemptStore) Inc(ctx context.Context, msg kafkago.Message) (int, error) {
	k := s.key(msg)
	n, err := s.rdb.Incr(ctx, k).Result()
	if err != nil {
		return 0, err
	}
	_ = s.rdb.Expire(ctx, k, attemptKeyTTL).Err()
	return int(n), nil
}

// Clear removes the counter after success or DLQ.
func (s *RedisAttemptStore) Clear(ctx context.Context, msg kafkago.Message) error {
	return s.rdb.Del(ctx, s.key(msg)).Err()
}

// MemoryAttemptStore adapts AttemptTracker to AttemptStore for tests / no-Redis.
type MemoryAttemptStore struct {
	t *AttemptTracker
}

// NewMemoryAttemptStore wraps a process-local tracker.
func NewMemoryAttemptStore() *MemoryAttemptStore {
	return &MemoryAttemptStore{t: NewAttemptTracker()}
}

func (s *MemoryAttemptStore) Inc(_ context.Context, msg kafkago.Message) (int, error) {
	return s.t.Inc(msg), nil
}

func (s *MemoryAttemptStore) Clear(_ context.Context, msg kafkago.Message) error {
	s.t.Clear(msg)
	return nil
}

// ConsumeLoopWithStore is ConsumeLoop with a pluggable attempt store.
func ConsumeLoopWithStore(
	ctx context.Context,
	reader *kafkago.Reader,
	dlq *kafkago.Writer,
	consumerName string,
	maxAttempts int,
	store AttemptStore,
	handle func(context.Context, kafkago.Message) (HandleOutcome, error),
	onError func(error),
) {
	if maxAttempts < 1 {
		maxAttempts = DefaultMaxAttempts
	}
	if store == nil {
		store = NewMemoryAttemptStore()
	}
	topic := reader.Config().Topic
	for {
		metrics.KafkaReaderQueue.WithLabelValues(consumerName).Set(float64(reader.Stats().QueueLength))
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if onError != nil {
				onError(fmt.Errorf("fetch: %w", err))
			}
			time.Sleep(time.Second)
			continue
		}
		outcome, herr := handle(ctx, msg)
		switch outcome {
		case OutcomeOK:
			_ = store.Clear(ctx, msg)
			_ = reader.CommitMessages(ctx, msg)
		case OutcomePoison:
			reason := "poison"
			if herr != nil {
				reason = herr.Error()
			}
			if err := PublishDLQ(ctx, dlq, topic, consumerName, reason, msg, 1); err != nil {
				if onError != nil {
					onError(fmt.Errorf("dlq: %w", err))
				}
				continue
			}
			metrics.DLQPublished.WithLabelValues(consumerName).Inc()
			_ = store.Clear(ctx, msg)
			_ = reader.CommitMessages(ctx, msg)
		case OutcomeRetry:
			n, err := store.Inc(ctx, msg)
			if err != nil {
				if onError != nil {
					onError(fmt.Errorf("attempt store: %w", err))
				}
				n = maxAttempts // fail closed to DLQ path on store errors after sleep
			}
			if onError != nil && herr != nil {
				onError(herr)
			}
			if n >= maxAttempts {
				reason := "max attempts exceeded"
				if herr != nil {
					reason = herr.Error()
				}
				if err := PublishDLQ(ctx, dlq, topic, consumerName, reason, msg, n); err != nil {
					if onError != nil {
						onError(fmt.Errorf("dlq: %w", err))
					}
					continue
				}
				metrics.DLQPublished.WithLabelValues(consumerName).Inc()
				_ = store.Clear(ctx, msg)
				_ = reader.CommitMessages(ctx, msg)
				continue
			}
			time.Sleep(Backoff(n))
		}
	}
}
