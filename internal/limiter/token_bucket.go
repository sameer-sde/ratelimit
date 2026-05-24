package limiter

import (
	"context"
	_ "embed"
	"fmt"
	"time"

	"github.com/sameer-sde/ratelimit/internal/rediscluster"
	"github.com/redis/go-redis/v9"
)

//go:embed token_bucket.lua
var tokenBucketScript string

type TokenBucket struct {
	cluster *rediscluster.Cluster
	script  *redis.Script
}

func NewTokenBucket(c *rediscluster.Cluster) *TokenBucket {
	return &TokenBucket{
		cluster: c,
		script:  redis.NewScript(tokenBucketScript),
	}
}

func (t *TokenBucket) Check(ctx context.Context, key string, capacity int, refillRate float64) (*Result, error) {
	redisKey := fmt.Sprintf("ratelimit:tb:%s", key)
	now := time.Now().UnixMicro()
	ttl := int(float64(capacity)/refillRate) + 60

	rdb := t.cluster.ClientFor(key)

	res, err := t.script.Run(ctx, rdb, []string{redisKey}, capacity, refillRate, now, ttl).Result()
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
	ttlSeconds := needForOneUs / 1_000_000

	return &Result{
		Allowed: allowed, Current: int64(capacity) - tokens,
		Remaining: tokens, TTL: ttlSeconds,
	}, nil
}
