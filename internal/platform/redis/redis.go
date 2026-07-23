// Package redis holds the go-redis client plus the atomic Lua scripts that
// underpin balance correctness (ARCHITECTURE.md section 5).
package redis

import (
	"context"
	"fmt"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

const (
	OutboxMessagesStream  = "outbox:messages"
	OutboxCampaignsStream = "outbox:campaigns"
)

// Client wraps go-redis with domain helpers.
type Client struct {
	rdb *goredis.Client
}

// NewClient connects to Redis and verifies connectivity.
func NewClient(ctx context.Context, addr string) (*Client, error) {
	rdb := goredis.NewClient(&goredis.Options{
		Addr:         addr,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("redis: ping: %w", err)
	}
	return &Client{rdb: rdb}, nil
}

// Raw exposes the underlying go-redis client for advanced Stream ops.
func (c *Client) Raw() *goredis.Client { return c.rdb }

// Close closes the Redis connection.
func (c *Client) Close() error { return c.rdb.Close() }

// BalanceKey returns the Redis key for an account's hot-path balance.
func BalanceKey(accountID string) string { return "balance:" + accountID }

// IdempotencyKey returns the Redis key for a cached idempotent response.
func IdempotencyKey(accountID, key string) string {
	return "idem:" + accountID + ":" + key
}

// RateLimitKey returns the Redis key for a tenant's ingestion token bucket.
func RateLimitKey(accountID string) string { return "ratelimit:" + accountID }

// GetBalance returns the current Redis balance, or 0 if the key is missing.
func (c *Client) GetBalance(ctx context.Context, accountID string) (int64, error) {
	val, err := c.rdb.Get(ctx, BalanceKey(accountID)).Result()
	if err == goredis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(val, 10, 64)
}

// SetBalance overwrites the Redis balance (used by topup, refund, reconciler).
func (c *Client) SetBalance(ctx context.Context, accountID string, balance int64) error {
	return c.rdb.Set(ctx, BalanceKey(accountID), balance, 0).Err()
}

// IncrBalance increments Redis balance by delta (topup/refund).
func (c *Client) IncrBalance(ctx context.Context, accountID string, delta int64) (int64, error) {
	return c.rdb.IncrBy(ctx, BalanceKey(accountID), delta).Result()
}

// Lua: atomic check-and-decrement + outbox append for a single message.
// KEYS[1] = balance key
// KEYS[2] = outbox stream
// ARGV[1] = cost
// ARGV[2..] = field/value pairs for the stream entry
var checkAndDebitScript = goredis.NewScript(`
local bal = tonumber(redis.call('GET', KEYS[1]) or '0')
local cost = tonumber(ARGV[1])
if bal < cost then
  return {-1, bal}
end
local newBal = bal - cost
redis.call('SET', KEYS[1], newBal)
local id = redis.call('XADD', KEYS[2], '*',
  'account_id', ARGV[2],
  'message_id', ARGV[3],
  'to', ARGV[4],
  'text', ARGV[5],
  'priority', ARGV[6],
  'cost', ARGV[7],
  'deadline', ARGV[8],
  'campaign_id', ARGV[9],
  'accepted_at', ARGV[10]
)
return {1, newBal, id}
`)

// MessageOutboxFields carries the payload written to outbox:messages.
type MessageOutboxFields struct {
	AccountID  string
	MessageID  string
	To         string
	Text       string
	Priority   string
	Cost       int64
	Deadline   string // RFC3339 or empty for normal
	CampaignID string // empty if not part of a campaign
	AcceptedAt string // RFC3339
}

// CheckAndDebitResult is the outcome of the atomic Lua debit.
type CheckAndDebitResult struct {
	OK         bool
	NewBalance int64
	StreamID   string
}

// CheckAndDebit atomically decrements balance and appends an outbox entry.
func (c *Client) CheckAndDebit(ctx context.Context, fields MessageOutboxFields) (CheckAndDebitResult, error) {
	res, err := checkAndDebitScript.Run(ctx, c.rdb, []string{
		BalanceKey(fields.AccountID),
		OutboxMessagesStream,
	},
		fields.Cost,
		fields.AccountID,
		fields.MessageID,
		fields.To,
		fields.Text,
		fields.Priority,
		strconv.FormatInt(fields.Cost, 10),
		fields.Deadline,
		fields.CampaignID,
		fields.AcceptedAt,
	).Slice()
	if err != nil {
		return CheckAndDebitResult{}, fmt.Errorf("redis: CheckAndDebit: %w", err)
	}
	okFlag, _ := toInt64(res[0])
	newBal, _ := toInt64(res[1])
	if okFlag < 0 {
		return CheckAndDebitResult{OK: false, NewBalance: newBal}, nil
	}
	streamID, _ := res[2].(string)
	return CheckAndDebitResult{OK: true, NewBalance: newBal, StreamID: streamID}, nil
}

// Lua: atomic check-and-decrement for a whole campaign + outbox append.
var checkAndDebitCampaignScript = goredis.NewScript(`
local bal = tonumber(redis.call('GET', KEYS[1]) or '0')
local cost = tonumber(ARGV[1])
if bal < cost then
  return {-1, bal}
end
local newBal = bal - cost
redis.call('SET', KEYS[1], newBal)
local id = redis.call('XADD', KEYS[2], '*',
  'account_id', ARGV[2],
  'campaign_id', ARGV[3],
  'text', ARGV[4],
  'total_cost', ARGV[5],
  'cost_per_message', ARGV[6],
  'recipients', ARGV[7],
  'accepted_at', ARGV[8]
)
return {1, newBal, id}
`)

// CampaignOutboxFields carries the payload written to outbox:campaigns.
type CampaignOutboxFields struct {
	AccountID      string
	CampaignID     string
	Text           string
	TotalCost      int64
	CostPerMessage int64
	RecipientsJSON string
	AcceptedAt     string
}

// CheckAndDebitCampaign atomically reserves campaign cost and appends outbox.
func (c *Client) CheckAndDebitCampaign(ctx context.Context, fields CampaignOutboxFields) (CheckAndDebitResult, error) {
	res, err := checkAndDebitCampaignScript.Run(ctx, c.rdb, []string{
		BalanceKey(fields.AccountID),
		OutboxCampaignsStream,
	},
		fields.TotalCost,
		fields.AccountID,
		fields.CampaignID,
		fields.Text,
		strconv.FormatInt(fields.TotalCost, 10),
		strconv.FormatInt(fields.CostPerMessage, 10),
		fields.RecipientsJSON,
		fields.AcceptedAt,
	).Slice()
	if err != nil {
		return CheckAndDebitResult{}, fmt.Errorf("redis: CheckAndDebitCampaign: %w", err)
	}
	okFlag, _ := toInt64(res[0])
	newBal, _ := toInt64(res[1])
	if okFlag < 0 {
		return CheckAndDebitResult{OK: false, NewBalance: newBal}, nil
	}
	streamID, _ := res[2].(string)
	return CheckAndDebitResult{OK: true, NewBalance: newBal, StreamID: streamID}, nil
}

// CacheIdempotentResponse stores a JSON response under an idempotency key.
func (c *Client) CacheIdempotentResponse(ctx context.Context, accountID, key, body string, ttl time.Duration) error {
	return c.rdb.Set(ctx, IdempotencyKey(accountID, key), body, ttl).Err()
}

// GetIdempotentResponse returns a previously cached idempotent response body.
func (c *Client) GetIdempotentResponse(ctx context.Context, accountID, key string) (string, bool, error) {
	val, err := c.rdb.Get(ctx, IdempotencyKey(accountID, key)).Result()
	if err == goredis.Nil {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return val, true, nil
}

func toInt64(v interface{}) (int64, error) {
	switch t := v.(type) {
	case int64:
		return t, nil
	case string:
		return strconv.ParseInt(t, 10, 64)
	default:
		return 0, fmt.Errorf("unexpected type %T", v)
	}
}
