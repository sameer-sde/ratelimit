package limiter

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"time"

	"github.com/sameer-sde/ratelimit/internal/cache"
	"github.com/redis/go-redis/v9"
)

//go:embed fixed_window.lua
var fixedWindowScript string

type FixedWindow struct {
	rdb    *redis.Client
	script *redis.Script
	cache  *cache.LRU // optional; may be nil
}

// RDB returns the underlying Redis client. Used by the /inspect endpoint.
func (f *FixedWindow) RDB() *redis.Client {
	return f.rdb
}
func NewFixedWindow(rdb *redis.Client) *FixedWindow {
	return &FixedWindow{
		rdb:    rdb,
		script: redis.NewScript(fixedWindowScript),
	}
}

// WithCache attaches a decision cache. Returns the limiter for chaining.
func (f *FixedWindow) WithCache(c *cache.LRU) *FixedWindow {
	f.cache = c
	return f
}

// cachedDenial is the JSON-serialized shape we store in the LRU.
type cachedDenial struct {
	ResetAtUnix int64 `json:"r"`
}

func (f *FixedWindow) Check(ctx context.Context, key string, limit, windowSeconds int) (*Result, error) {
	redisKey := fmt.Sprintf("ratelimit:fixed:%s", key)
	cacheKey := fmt.Sprintf("fixed:%s:%d:%d", key, limit, windowSeconds)

	// Cache fast path: if we recently denied this key and the deny is still
	// valid (window hasn't reset), short-circuit without hitting Redis.
	if f.cache != nil {
		if raw, ok := f.cache.Get(cacheKey); ok {
			var cd cachedDenial
			if err := json.Unmarshal(raw, &cd); err == nil {
				if time.Now().Unix() < cd.ResetAtUnix {
					return &Result{
						Allowed:   false,
						Current:   int64(limit),
						Remaining: 0,
						TTL:       cd.ResetAtUnix - time.Now().Unix(),
					}, nil
				}
			}
		}
	}

	res, err := f.script.Run(ctx, f.rdb, []string{redisKey}, limit, windowSeconds).Result()
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

	// Cache denials only. Allows must keep flowing to Redis so the count grows.
	if !allowed && f.cache != nil {
		resetAt := time.Now().Unix() + ttl
		raw, _ := json.Marshal(cachedDenial{ResetAtUnix: resetAt})
		// Cache TTL is bounded by 100ms OR the remaining window TTL, whichever
		// is shorter. We don't want stale denials past the window reset.
		cacheTTL := 100 * time.Millisecond
		if t := time.Duration(ttl) * time.Second; t < cacheTTL {
			cacheTTL = t
		}
		f.cache.Put(cacheKey, raw, cacheTTL)
	}

	return &Result{
		Allowed:   allowed,
		Current:   current,
		Remaining: remaining,
		TTL:       ttl,
	}, nil
}
