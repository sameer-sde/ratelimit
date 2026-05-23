package limiter

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/redis/go-redis/v9"
)

//go:embed fixed_window.lua
var fixedWindowScript string

// FixedWindow implements the fixed-window rate-limiting algorithm.
type FixedWindow struct {
	rdb    *redis.Client
	script *redis.Script
}

// NewFixedWindow returns a fixed-window limiter ready to use.
// We wrap the script in redis.Script so the client can use EVALSHA
// (sends a hash instead of the whole script after the first call — faster).
func NewFixedWindow(rdb *redis.Client) *FixedWindow {
	return &FixedWindow{
		rdb:    rdb,
		script: redis.NewScript(fixedWindowScript),
	}
}

// Result is the outcome of a rate-limit check.
type Result struct {
	Allowed   bool
	Current   int64
	Remaining int64
	TTL       int64 // seconds until the window resets
}

// Check runs the rate-limit decision atomically.
func (f *FixedWindow) Check(ctx context.Context, key string, limit, windowSeconds int) (*Result, error) {
	redisKey := fmt.Sprintf("ratelimit:fixed:%s", key)

	// Run the Lua script. Returns []interface{} = {allowed, current, ttl}.
	res, err := f.script.Run(ctx, f.rdb, []string{redisKey}, limit, windowSeconds).Result()
	if err != nil {
		return nil, fmt.Errorf("running fixed window script: %w", err)
	}

	arr, ok := res.([]interface{})
	if !ok || len(arr) != 3 {
		return nil, fmt.Errorf("unexpected script result: %v", res)
	}

	allowed := arr[0].(int64) == 1
	current := arr[1].(int64)
	ttl := arr[2].(int64)

	remaining := int64(limit) - current
	if remaining < 0 {
		remaining = 0
	}

	return &Result{
		Allowed:   allowed,
		Current:   current,
		Remaining: remaining,
		TTL:       ttl,
	}, nil
}
