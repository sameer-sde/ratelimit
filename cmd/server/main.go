package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/sameer-sde/ratelimit/internal/cache"
	"github.com/sameer-sde/ratelimit/internal/limiter"
	"github.com/redis/go-redis/v9"
)

type CheckRequest struct {
	Key       string  `json:"key"`
	Limit     int     `json:"limit"`
	Window    int     `json:"window"`
	Capacity  int     `json:"capacity"`
	Refill    float64 `json:"refill"`
	Algorithm string  `json:"algorithm"`
}

type CheckResponse struct {
	Allowed   bool  `json:"allowed"`
	Remaining int64 `json:"remaining"`
	Current   int64 `json:"current"`
	ResetIn   int64 `json:"reset_in_seconds"`
}

type Metrics struct {
	totalRequests atomic.Uint64
	allowed       atomic.Uint64
	denied        atomic.Uint64
	startedAt     time.Time
}

type Server struct {
	fixed   *limiter.FixedWindow
	slog    *limiter.SlidingWindowLog
	bucket  *limiter.TokenBucket
	counter *limiter.SlidingWindowCounter
	lru     *cache.LRU
	metrics *Metrics
}

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()

	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("redis ping failed: %v", err)
	}
	log.Println("✓ Connected to Redis")

	lru := cache.New(10000)

	s := &Server{
		fixed:   limiter.NewFixedWindow(rdb).WithCache(lru),
		slog:    limiter.NewSlidingWindowLog(rdb),
		bucket:  limiter.NewTokenBucket(rdb),
		counter: limiter.NewSlidingWindowCounter(rdb),
		lru:     lru,
		metrics: &Metrics{startedAt: time.Now()},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/check", s.handleCheck)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("/cache/stats", s.handleCacheStats)
	mux.HandleFunc("/metrics", s.handleMetrics)

	addr := ":8080"
	log.Printf("✓ Listening on http://localhost%s", addr)
	log.Printf("✓ LRU cache capacity: 10000 entries, decision TTL: 100ms")
	srv := &http.Server{
		Addr:         addr,
		Handler:      withCORS(mux),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) handleCacheStats(w http.ResponseWriter, r *http.Request) {
	hits, misses, size := s.lru.Stats()
	total := hits + misses
	hitRate := 0.0
	if total > 0 {
		hitRate = float64(hits) / float64(total) * 100
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"hits":         hits,
		"misses":       misses,
		"size":         size,
		"hit_rate_pct": hitRate,
	})
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	total := s.metrics.totalRequests.Load()
	allowed := s.metrics.allowed.Load()
	denied := s.metrics.denied.Load()
	hits, misses, cacheSize := s.lru.Stats()

	uptime := time.Since(s.metrics.startedAt).Seconds()
	rps := 0.0
	if uptime > 0 {
		rps = float64(total) / uptime
	}

	cacheTotal := hits + misses
	cacheHitRate := 0.0
	if cacheTotal > 0 {
		cacheHitRate = float64(hits) / float64(cacheTotal) * 100
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"total_requests": total,
		"allowed":        allowed,
		"denied":         denied,
		"avg_rps":        rps,
		"uptime_seconds": uptime,
		"cache_hits":     hits,
		"cache_misses":   misses,
		"cache_size":     cacheSize,
		"cache_hit_rate": cacheHitRate,
	})
}

func (s *Server) handleCheck(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	var req CheckRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	if req.Key == "" {
		http.Error(w, "key required", http.StatusBadRequest)
		return
	}
	if req.Algorithm == "" {
		req.Algorithm = "fixed"
	}

	var (
		result *limiter.Result
		err    error
	)
	switch req.Algorithm {
	case "fixed":
		if req.Limit <= 0 || req.Window <= 0 {
			http.Error(w, "fixed needs limit and window", http.StatusBadRequest)
			return
		}
		result, err = s.fixed.Check(r.Context(), req.Key, req.Limit, req.Window)
	case "slog":
		if req.Limit <= 0 || req.Window <= 0 {
			http.Error(w, "slog needs limit and window", http.StatusBadRequest)
			return
		}
		result, err = s.slog.Check(r.Context(), req.Key, req.Limit, req.Window)
	case "swc":
		if req.Limit <= 0 || req.Window <= 0 {
			http.Error(w, "swc needs limit and window", http.StatusBadRequest)
			return
		}
		result, err = s.counter.Check(r.Context(), req.Key, req.Limit, req.Window)
	case "bucket":
		if req.Capacity <= 0 || req.Refill <= 0 {
			http.Error(w, "bucket needs capacity and refill", http.StatusBadRequest)
			return
		}
		result, err = s.bucket.Check(r.Context(), req.Key, req.Capacity, req.Refill)
	default:
		http.Error(w, "unknown algorithm: 'fixed', 'slog', 'swc', or 'bucket'", http.StatusBadRequest)
		return
	}

	if err != nil {
		log.Printf("check error: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	s.metrics.totalRequests.Add(1)
	if result.Allowed {
		s.metrics.allowed.Add(1)
	} else {
		s.metrics.denied.Add(1)
	}

	status := http.StatusOK
	if !result.Allowed {
		status = http.StatusTooManyRequests
	}
	writeJSON(w, status, CheckResponse{
		Allowed:   result.Allowed,
		Remaining: result.Remaining,
		Current:   result.Current,
		ResetIn:   result.TTL,
	})
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
