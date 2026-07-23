package redis

import (
	"context"
	"fmt"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// OutboxConsumer reads a Redis Stream via a consumer group.
type OutboxConsumer struct {
	rdb      *goredis.Client
	stream   string
	group    string
	consumer string
}

// NewOutboxConsumer ensures the consumer group exists and returns a consumer.
func NewOutboxConsumer(ctx context.Context, c *Client, stream, group, consumer string) (*OutboxConsumer, error) {
	err := c.rdb.XGroupCreateMkStream(ctx, stream, group, "0").Err()
	if err != nil && !isBusyGroup(err) {
		return nil, fmt.Errorf("redis: create group: %w", err)
	}
	return &OutboxConsumer{rdb: c.rdb, stream: stream, group: group, consumer: consumer}, nil
}

func isBusyGroup(err error) bool {
	return err != nil && strings.Contains(err.Error(), "BUSYGROUP")
}

// Read reads up to count new messages, blocking up to block.
func (o *OutboxConsumer) Read(ctx context.Context, count int64, block time.Duration) ([]goredis.XMessage, error) {
	streams, err := o.rdb.XReadGroup(ctx, &goredis.XReadGroupArgs{
		Group:    o.group,
		Consumer: o.consumer,
		Streams:  []string{o.stream, ">"},
		Count:    count,
		Block:    block,
	}).Result()
	if err == goredis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(streams) == 0 {
		return nil, nil
	}
	return streams[0].Messages, nil
}

// ClaimStale reclaims pending entries idle longer than minIdle.
func (o *OutboxConsumer) ClaimStale(ctx context.Context, minIdle time.Duration, count int64) ([]goredis.XMessage, error) {
	start := "0-0"
	msgs, _, err := o.rdb.XAutoClaim(ctx, &goredis.XAutoClaimArgs{
		Stream:   o.stream,
		Group:    o.group,
		Consumer: o.consumer,
		MinIdle:  minIdle,
		Start:    start,
		Count:    count,
	}).Result()
	if err == goredis.Nil {
		return nil, nil
	}
	return msgs, err
}

// Ack acknowledges a stream entry after successful Kafka publish.
func (o *OutboxConsumer) Ack(ctx context.Context, ids ...string) error {
	if len(ids) == 0 {
		return nil
	}
	return o.rdb.XAck(ctx, o.stream, o.group, ids...).Err()
}
