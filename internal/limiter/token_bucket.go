package limiter

import (
	"context"
	_ "embed"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed token_bucket.lua
var tokenBucketScript string

type TokenBucket struct {
	rdb    *redis.Client
	script *redis.Script
}

func NewTokenBucket(rdb *redis.Client) *TokenBucket {
	return &TokenBucket{
		rdb:    rdb,
		script: redis.NewScript(tokenBucketScript),
	}
}

// Check decides whether to allow a request.
// capacity = max burst size (the bucket's ceiling)
// refillRate = tokens added per second (can be < 1, e.g. 0.5 = 1 token per 2s)
func (t *TokenBucket) Check(ctx context.Context, key string, capacity int, refillRate float64) (*Result, error) {
	redisKey := fmt.Sprintf("ratelimit:tb:%s", key)
	now := time.Now().UnixMicro()

	// TTL: enough time for an idle bucket to fully refill, plus buffer.
	ttl := int(float64(capacity)/refillRate) + 60

	res, err := t.script.Run(
		ctx, t.rdb,
		[]string{redisKey},
		capacity, refillRate, now, ttl,
	).Result()
	if err != nil {
		return nil, fmt.Errorf("token bucket: %w", err)
	}

	arr, ok := res.([]interface{})
	if !ok || len(arr) != 3 {
		return nil, fmt.Errorf("unexpected result: %v", res)
	}

	allowed := arr[0].(int64) == 1
	tokens := arr[1].(int64)
	needForOneUs := arr[2].(int64)

	// TTL = seconds until next token if currently empty
	ttlSeconds := needForOneUs / 1_000_000

	return &Result{
		Allowed:   allowed,
		Current:   int64(capacity) - tokens,
		Remaining: tokens,
		TTL:       ttlSeconds,
	}, nil
}
