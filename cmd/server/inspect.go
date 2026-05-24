package main

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// InspectResult is everything we can find about a key across all algorithms.
type InspectResult struct {
	Key   string                 `json:"key"`
	Fixed *FixedState            `json:"fixed,omitempty"`
	SLog  *SLogState             `json:"slog,omitempty"`
	SWC   *SWCState              `json:"swc,omitempty"`
	TB    *TBState               `json:"token_bucket,omitempty"`
}

type FixedState struct {
	RedisKey   string `json:"redis_key"`
	Count      int64  `json:"count"`
	TTLSeconds int64  `json:"ttl_seconds"`
}

type SLogEntry struct {
	TimestampUs int64  `json:"ts_us"`
	RequestID   string `json:"req_id"`
}

type SLogState struct {
	RedisKey   string      `json:"redis_key"`
	Count      int64       `json:"count"`
	TTLSeconds int64       `json:"ttl_seconds"`
	Recent     []SLogEntry `json:"recent"` // up to 10 most recent entries
}

type SWCBucket struct {
	RedisKey string `json:"redis_key"`
	Bucket   int64  `json:"bucket_id"`
	Count    int64  `json:"count"`
}

type SWCState struct {
	Current *SWCBucket `json:"current"`
	Prev    *SWCBucket `json:"previous"`
}

type TBState struct {
	RedisKey   string  `json:"redis_key"`
	Tokens     float64 `json:"tokens"`
	LastUs     int64   `json:"last_us"`
	TTLSeconds int64   `json:"ttl_seconds"`
}

// handleInspect returns the raw state of a key across all 4 algorithms.
// Caller passes the *application* key (e.g. "user_42"); we look up every
// algorithm's prefixed form behind the scenes.
func (s *Server) handleInspect(w http.ResponseWriter, r *http.Request) {
	// Path is /inspect/<key>; extract the part after the prefix.
	key := strings.TrimPrefix(r.URL.Path, "/inspect/")
	if key == "" {
		http.Error(w, "key required: /inspect/<key>", http.StatusBadRequest)
		return
	}

	// Also accept ?window= and ?bucket_window= for swc inspection.
	swcWindow := 60
	if v := r.URL.Query().Get("swc_window"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			swcWindow = n
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	rdb := s.fixed.RDB() // small helper we'll add next
	result := InspectResult{Key: key}

	// --- FIXED WINDOW ---
	// Fixed uses bucket = unix / window, so we don't know which bucket without
	// the window. The reasonable thing: scan for any ratelimit:fixed:<key>:*
	// keys still alive and report them.
	if k, count, ttl := scanFixedLike(ctx, rdb, "ratelimit:fixed:"+key); k != "" {
		result.Fixed = &FixedState{RedisKey: k, Count: count, TTLSeconds: ttl}
	}

	// --- SLIDING WINDOW LOG ---
	slogKey := "ratelimit:slog:" + key
	if zcard, _ := rdb.ZCard(ctx, slogKey).Result(); zcard > 0 {
		ttl, _ := rdb.TTL(ctx, slogKey).Result()
		// last 10 entries by score
		members, _ := rdb.ZRangeWithScores(ctx, slogKey, -10, -1).Result()
		entries := make([]SLogEntry, 0, len(members))
		for _, m := range members {
			id, _ := m.Member.(string)
			entries = append(entries, SLogEntry{
				TimestampUs: int64(m.Score),
				RequestID:   id,
			})
		}
		result.SLog = &SLogState{
			RedisKey:   slogKey,
			Count:      zcard,
			TTLSeconds: int64(ttl.Seconds()),
			Recent:     entries,
		}
	}

	// --- SLIDING WINDOW COUNTER ---
	// Two buckets: current and previous. Bucket id = unix / window.
	nowBucket := time.Now().Unix() / int64(swcWindow)
	swcCur := "ratelimit:swc:" + key + ":" + strconv.FormatInt(nowBucket, 10)
	swcPrev := "ratelimit:swc:" + key + ":" + strconv.FormatInt(nowBucket-1, 10)
	if cur, err := rdb.Get(ctx, swcCur).Int64(); err == nil {
		if result.SWC == nil {
			result.SWC = &SWCState{}
		}
		result.SWC.Current = &SWCBucket{RedisKey: swcCur, Bucket: nowBucket, Count: cur}
	}
	if prev, err := rdb.Get(ctx, swcPrev).Int64(); err == nil {
		if result.SWC == nil {
			result.SWC = &SWCState{}
		}
		result.SWC.Prev = &SWCBucket{RedisKey: swcPrev, Bucket: nowBucket - 1, Count: prev}
	}

	// --- TOKEN BUCKET ---
	tbKey := "ratelimit:tb:" + key
	if vals, err := rdb.HMGet(ctx, tbKey, "tokens", "last_us").Result(); err == nil {
		if vals[0] != nil && vals[1] != nil {
			tokensStr, _ := vals[0].(string)
			lastStr, _ := vals[1].(string)
			tokens, _ := strconv.ParseFloat(tokensStr, 64)
			last, _ := strconv.ParseInt(lastStr, 10, 64)
			ttl, _ := rdb.TTL(ctx, tbKey).Result()
			result.TB = &TBState{
				RedisKey:   tbKey,
				Tokens:     tokens,
				LastUs:     last,
				TTLSeconds: int64(ttl.Seconds()),
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

// scanFixedLike finds the first ratelimit:fixed:<key>:* match and returns
// its count and TTL. Fixed uses bucketed keys so we use SCAN — fine here
// because we expect at most 1–2 matching keys per app key at any time.
func scanFixedLike(ctx context.Context, rdb *redis.Client, prefix string) (string, int64, int64) {
	iter := rdb.Scan(ctx, 0, prefix+"*", 10).Iterator()
	for iter.Next(ctx) {
		k := iter.Val()
		if count, err := rdb.Get(ctx, k).Int64(); err == nil {
			ttl, _ := rdb.TTL(ctx, k).Result()
			return k, count, int64(ttl.Seconds())
		}
	}
	return "", 0, 0
}
