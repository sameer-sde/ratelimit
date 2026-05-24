package main

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type InspectResult struct {
	Key   string      `json:"key"`
	Shard string      `json:"shard"`
	Fixed *FixedState `json:"fixed,omitempty"`
	SLog  *SLogState  `json:"slog,omitempty"`
	SWC   *SWCState   `json:"swc,omitempty"`
	TB    *TBState    `json:"token_bucket,omitempty"`
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
	Recent     []SLogEntry `json:"recent"`
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

func (s *Server) handleInspect(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/inspect/")
	if key == "" {
		http.Error(w, "key required: /inspect/<key>", http.StatusBadRequest)
		return
	}

	swcWindow := 60
	if v := r.URL.Query().Get("swc_window"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			swcWindow = n
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	// All rate-limit state for this key lives on this one shard.
	rdb := s.cluster.ClientFor(key)
	result := InspectResult{Key: key, Shard: s.cluster.NodeFor(key)}

	// FIXED
	iter := rdb.Scan(ctx, 0, "ratelimit:fixed:"+key+"*", 10).Iterator()
	for iter.Next(ctx) {
		k := iter.Val()
		if count, err := rdb.Get(ctx, k).Int64(); err == nil {
			ttl, _ := rdb.TTL(ctx, k).Result()
			result.Fixed = &FixedState{RedisKey: k, Count: count, TTLSeconds: int64(ttl.Seconds())}
			break
		}
	}

	// SLOG
	slogKey := "ratelimit:slog:" + key
	if zcard, _ := rdb.ZCard(ctx, slogKey).Result(); zcard > 0 {
		ttl, _ := rdb.TTL(ctx, slogKey).Result()
		members, _ := rdb.ZRangeWithScores(ctx, slogKey, -10, -1).Result()
		entries := make([]SLogEntry, 0, len(members))
		for _, m := range members {
			id, _ := m.Member.(string)
			entries = append(entries, SLogEntry{TimestampUs: int64(m.Score), RequestID: id})
		}
		result.SLog = &SLogState{
			RedisKey: slogKey, Count: zcard,
			TTLSeconds: int64(ttl.Seconds()), Recent: entries,
		}
	}

	// SWC
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

	// TB
	tbKey := "ratelimit:tb:" + key
	if vals, err := rdb.HMGet(ctx, tbKey, "tokens", "last_us").Result(); err == nil {
		if vals[0] != nil && vals[1] != nil {
			tokensStr, _ := vals[0].(string)
			lastStr, _ := vals[1].(string)
			tokens, _ := strconv.ParseFloat(tokensStr, 64)
			last, _ := strconv.ParseInt(lastStr, 10, 64)
			ttl, _ := rdb.TTL(ctx, tbKey).Result()
			result.TB = &TBState{
				RedisKey: tbKey, Tokens: tokens, LastUs: last,
				TTLSeconds: int64(ttl.Seconds()),
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}
