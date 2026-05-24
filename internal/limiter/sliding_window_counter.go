package limiter

import (
	"context"
	_ "embed"
	"fmt"
	"time"

	"github.com/sameer-sde/ratelimit/internal/rediscluster"
	"github.com/redis/go-redis/v9"
)

//go:embed sliding_window_counter.lua
var slidingWindowCounterScript string

type SlidingWindowCounter struct {
	cluster *rediscluster.Cluster
	script  *redis.Script
}

func NewSlidingWindowCounter(c *rediscluster.Cluster) *SlidingWindowCounter {
	return &SlidingWindowCounter{
		cluster: c,
		script:  redis.NewScript(slidingWindowCounterScript),
	}
}

func (s *SlidingWindowCounter) Check(ctx context.Context, key string, limit, windowSeconds int) (*Result, error) {
	redisKey := fmt.Sprintf("ratelimit:swc:%s", key)
	now := time.Now().Unix()
	rdb := s.cluster.ClientFor(key)

	res, err := s.script.Run(ctx, rdb, []string{redisKey}, limit, windowSeconds, now).Result()
	if err != nil {
		return nil, fmt.Errorf("sliding window counter: %w", err)
	}
	arr, ok := res.([]interface{})
	if !ok || len(arr) != 3 {
		return nil, fmt.Errorf("unexpected result: %v", res)
	}
	allowed := arr[0].(int64) == 1
	estimate := arr[1].(int64)
	resetIn := arr[2].(int64)

	remaining := int64(limit) - estimate
	if remaining < 0 {
		remaining = 0
	}
	return &Result{Allowed: allowed, Current: estimate, Remaining: remaining, TTL: resetIn}, nil
}
