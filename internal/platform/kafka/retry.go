package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// DefaultMaxAttempts before a message is published to sms.dlq.
const DefaultMaxAttempts = 5

// DLQPayload is written to sms.dlq when retries are exhausted.
type DLQPayload struct {
	OriginalTopic string          `json:"originalTopic"`
	Consumer      string          `json:"consumer"`
	Reason        string          `json:"reason"`
	Attempts      int             `json:"attempts"`
	Key           string          `json:"key,omitempty"`
	Value         json.RawMessage `json:"value"`
	FailedAt      time.Time       `json:"failedAt"`
}

// PublishDLQ writes a poison / exhausted message to sms.dlq.
func PublishDLQ(ctx context.Context, dlq *kafkago.Writer, originalTopic, consumer, reason string, msg kafkago.Message, attempts int) error {
	body, err := json.Marshal(DLQPayload{
		OriginalTopic: originalTopic,
		Consumer:      consumer,
		Reason:        reason,
		Attempts:      attempts,
		Key:           string(msg.Key),
		Value:         json.RawMessage(msg.Value),
		FailedAt:      time.Now().UTC(),
	})
	if err != nil {
		return err
	}
	key := msg.Key
	if len(key) == 0 {
		key = []byte(consumer)
	}
	return Publish(ctx, dlq, key, body)
}

// Backoff returns a capped exponential delay for attempt n (1-based).
func Backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := time.Duration(attempt*attempt) * 200 * time.Millisecond
	if d > 5*time.Second {
		return 5 * time.Second
	}
	return d
}

// HandleOutcome classifies consumer handle results for the retry loop.
type HandleOutcome int

const (
	OutcomeOK HandleOutcome = iota
	OutcomeRetry
	OutcomePoison // non-retryable (e.g. bad JSON)
)

type attemptKey struct {
	topic     string
	partition int
	offset    int64
}

// AttemptTracker counts consecutive handle failures per Kafka offset (process-local).
type AttemptTracker struct {
	mu   sync.Mutex
	data map[attemptKey]int
}

// NewAttemptTracker creates an empty tracker.
func NewAttemptTracker() *AttemptTracker {
	return &AttemptTracker{data: make(map[attemptKey]int)}
}

func (t *AttemptTracker) key(msg kafkago.Message) attemptKey {
	return attemptKey{topic: msg.Topic, partition: msg.Partition, offset: msg.Offset}
}

// Inc returns the new attempt count after a failure.
func (t *AttemptTracker) Inc(msg kafkago.Message) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	k := t.key(msg)
	t.data[k]++
	return t.data[k]
}

// Clear drops the counter after success or DLQ.
func (t *AttemptTracker) Clear(msg kafkago.Message) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.data, t.key(msg))
}

// ConsumeLoop runs Fetch → handle → commit / retry / DLQ.
func ConsumeLoop(
	ctx context.Context,
	reader *kafkago.Reader,
	dlq *kafkago.Writer,
	consumerName string,
	maxAttempts int,
	handle func(context.Context, kafkago.Message) (HandleOutcome, error),
	onError func(error),
) {
	if maxAttempts < 1 {
		maxAttempts = DefaultMaxAttempts
	}
	tracker := NewAttemptTracker()
	topic := reader.Config().Topic
	for {
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
			tracker.Clear(msg)
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
			tracker.Clear(msg)
			_ = reader.CommitMessages(ctx, msg)
		case OutcomeRetry:
			n := tracker.Inc(msg)
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
				tracker.Clear(msg)
				_ = reader.CommitMessages(ctx, msg)
				continue
			}
			time.Sleep(Backoff(n))
			// Leave uncommitted so the same record is redelivered.
		}
	}
}
