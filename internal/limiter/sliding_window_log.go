package limiter

import (
	"context"
	"crypto/rand"
	_ "embed"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed sliding_window_log.lua
var slidingWindowLogScript string

type SlidingWindowLog struct {
	rdb    *redis.Client
	script *redis.Script
}

func NewSlidingWindowLog(rdb *redis.Client) *SlidingWindowLog {
	return &SlidingWindowLog{
		rdb:    rdb,
		script: redis.NewScript(slidingWindowLogScript),
	}
}

func (s *SlidingWindowLog) Check(ctx context.Context, key string, limit, windowSeconds int) (*Result, error) {
	redisKey := fmt.Sprintf("ratelimit:slog:%s", key)

	now := time.Now().UnixMicro()
	reqID := fmt.Sprintf("%d-%s", now, randomHex(4))

	res, err := s.script.Run(
		ctx, s.rdb,
		[]string{redisKey},
		limit, windowSeconds, now, reqID,
	).Result()
	if err != nil {
		return nil, fmt.Errorf("sliding window log: %w", err)
	}

	arr, ok := res.([]interface{})
	if !ok || len(arr) != 3 {
		return nil, fmt.Errorf("unexpected result: %v", res)
	}

	allowed := arr[0].(int64) == 1
	current := arr[1].(int64)
	oldestUs := arr[2].(int64)

	remaining := int64(limit) - current
	if remaining < 0 {
		remaining = 0
	}

	// TTL = when oldest entry slides out (that's when a slot frees up)
	var ttl int64
	if !allowed && oldestUs > 0 {
		expiresAtUs := oldestUs + int64(windowSeconds)*1_000_000
		ttl = (expiresAtUs - now) / 1_000_000
		if ttl < 0 {
			ttl = 0
		}
	}

	return &Result{
		Allowed:   allowed,
		Current:   current,
		Remaining: remaining,
		TTL:       ttl,
	}, nil
}

// randomHex disambiguates requests arriving in the same microsecond.
func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
