package redis

import (
	"context"
	"fmt"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// SignupRateLimitKey returns the Redis key for open signup abuse control (by client IP).
func SignupRateLimitKey(clientIP string) string { return "ratelimit:signup:" + clientIP }

// Token-bucket: refill tokens over time up to capacity; consume `need` or deny.
// KEYS[1] bucket hash
// ARGV[1] capacity, ARGV[2] refill_per_sec, ARGV[3] now_ms, ARGV[4] need
var takeTokenScript = goredis.NewScript(`
local capacity = tonumber(ARGV[1])
local refill = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local need = tonumber(ARGV[4])

local data = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts = tonumber(data[2])
if tokens == nil then
  tokens = capacity
  ts = now
end

local elapsed = (now - ts) / 1000.0
if elapsed < 0 then
  elapsed = 0
end
tokens = math.min(capacity, tokens + elapsed * refill)

if tokens < need then
  redis.call('HMSET', KEYS[1], 'tokens', tostring(tokens), 'ts', tostring(now))
  local ttl = math.ceil(capacity / math.max(refill, 0.001)) + 60
  redis.call('EXPIRE', KEYS[1], ttl)
  return 0
end

tokens = tokens - need
redis.call('HMSET', KEYS[1], 'tokens', tostring(tokens), 'ts', tostring(now))
local ttl = math.ceil(capacity / math.max(refill, 0.001)) + 60
redis.call('EXPIRE', KEYS[1], ttl)
return 1
`)

// TakeToken applies a Redis token-bucket rate limit. Returns allowed=false when denied.
// Invalid config (non-positive capacity/refill/need) denies the request (fail closed).
func (c *Client) TakeToken(ctx context.Context, key string, capacity int64, refillPerSec float64, need int64) (bool, error) {
	if capacity <= 0 || refillPerSec <= 0 || need <= 0 {
		return false, fmt.Errorf("redis: TakeToken: invalid bucket config capacity=%d refill=%v need=%d", capacity, refillPerSec, need)
	}
	nowMS := time.Now().UnixMilli()
	res, err := takeTokenScript.Run(ctx, c.rdb, []string{key},
		capacity,
		strconv.FormatFloat(refillPerSec, 'f', 6, 64),
		nowMS,
		need,
	).Int64()
	if err != nil {
		return false, fmt.Errorf("redis: TakeToken: %w", err)
	}
	return res == 1, nil
}
