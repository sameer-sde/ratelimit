package limiter

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"time"

	"github.com/sameer-sde/ratelimit/internal/cache"
	"github.com/sameer-sde/ratelimit/internal/rediscluster"
	"github.com/redis/go-redis/v9"
)

//go:embed fixed_window.lua
var fixedWindowScript string

type FixedWindow struct {
	cluster *rediscluster.Cluster
	script  *redis.Script
	cache   *cache.LRU
}

func NewFixedWindow(c *rediscluster.Cluster) *FixedWindow {
	return &FixedWindow{
		cluster: c,
		script:  redis.NewScript(fixedWindowScript),
	}
}

func (f *FixedWindow) WithCache(c *cache.LRU) *FixedWindow {
	f.cache = c
	return f
}

// Cluster exposes the underlying cluster (used by /inspect).
func (f *FixedWindow) Cluster() *rediscluster.Cluster {
	return f.cluster
}

type cachedDenial struct {
	ResetAtUnix int64 `json:"r"`
}

func (f *FixedWindow) Check(ctx context.Context, key string, limit, windowSeconds int) (*Result, error) {
	redisKey := fmt.Sprintf("ratelimit:fixed:%s", key)
	cacheKey := fmt.Sprintf("fixed:%s:%d:%d", key, limit, windowSeconds)

	if f.cache != nil {
		if raw, ok := f.cache.Get(cacheKey); ok {
			var cd cachedDenial
			if err := json.Unmarshal(raw, &cd); err == nil {
				if time.Now().Unix() < cd.ResetAtUnix {
					return &Result{
						Allowed: false, Current: int64(limit), Remaining: 0,
						TTL: cd.ResetAtUnix - time.Now().Unix(),
					}, nil
				}
			}
		}
	}

	// Route to the shard that owns this key.
	rdb := f.cluster.ClientFor(key)

	res, err := f.script.Run(ctx, rdb, []string{redisKey}, limit, windowSeconds).Result()
	if err != nil {
		return nil, fmt.Errorf("fixed window: %w", err)
	}
	arr, ok := res.([]interface{})
	if !ok || len(arr) != 3 {
		return nil, fmt.Errorf("unexpected result: %v", res)
	}
	allowed := arr[0].(int64) == 1
	current := arr[1].(int64)
	ttl := arr[2].(int64)

	remaining := int64(limit) - current
	if remaining < 0 {
		remaining = 0
	}

	if !allowed && f.cache != nil {
		resetAt := time.Now().Unix() + ttl
		raw, _ := json.Marshal(cachedDenial{ResetAtUnix: resetAt})
		cacheTTL := 100 * time.Millisecond
		if t := time.Duration(ttl) * time.Second; t < cacheTTL {
			cacheTTL = t
		}
		f.cache.Put(cacheKey, raw, cacheTTL)
	}

	return &Result{Allowed: allowed, Current: current, Remaining: remaining, TTL: ttl}, nil
}
