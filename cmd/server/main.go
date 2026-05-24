package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sameer-sde/ratelimit/internal/cache"
	"github.com/sameer-sde/ratelimit/internal/limiter"
	"github.com/sameer-sde/ratelimit/internal/rediscluster"
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
	Allowed   bool   `json:"allowed"`
	Remaining int64  `json:"remaining"`
	Current   int64  `json:"current"`
	ResetIn   int64  `json:"reset_in_seconds"`
	Shard     string `json:"shard"`
}

type AlgoStats struct {
	total   atomic.Uint64
	allowed atomic.Uint64
	denied  atomic.Uint64
}

type Metrics struct {
	totalRequests atomic.Uint64
	allowed       atomic.Uint64
	denied        atomic.Uint64
	startedAt     time.Time
	mu            sync.RWMutex
	algo          map[string]*AlgoStats
}

func (m *Metrics) record(alg string, allowed bool) {
	m.totalRequests.Add(1)
	if allowed {
		m.allowed.Add(1)
	} else {
		m.denied.Add(1)
	}
	m.mu.RLock()
	stats, ok := m.algo[alg]
	m.mu.RUnlock()
	if !ok {
		m.mu.Lock()
		stats = m.algo[alg]
		if stats == nil {
			stats = &AlgoStats{}
			m.algo[alg] = stats
		}
		m.mu.Unlock()
	}
	stats.total.Add(1)
	if allowed {
		stats.allowed.Add(1)
	} else {
		stats.denied.Add(1)
	}
}

type Server struct {
	cluster *rediscluster.Cluster
	fixed   *limiter.FixedWindow
	slog    *limiter.SlidingWindowLog
	bucket  *limiter.TokenBucket
	counter *limiter.SlidingWindowCounter
	lru     *cache.LRU
	metrics *Metrics
}

func main() {
	cluster, err := rediscluster.New(map[string]string{
		"redis-0": "localhost:6382",
		"redis-1": "localhost:6380",
		"redis-2": "localhost:6381",
	})
	if err != nil {
		log.Fatalf("cluster init: %v", err)
	}
	defer cluster.Close()

	log.Printf("✓ Connected to Redis cluster (%d shards): %v", len(cluster.Nodes()), cluster.Nodes())

	lru := cache.New(10000)

	s := &Server{
		cluster: cluster,
		fixed:   limiter.NewFixedWindow(cluster).WithCache(lru),
		slog:    limiter.NewSlidingWindowLog(cluster),
		bucket:  limiter.NewTokenBucket(cluster),
		counter: limiter.NewSlidingWindowCounter(cluster),
		lru:     lru,
		metrics: &Metrics{
			startedAt: time.Now(),
			algo:      make(map[string]*AlgoStats),
		},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/check", s.handleCheck)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("/cache/stats", s.handleCacheStats)
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/load-test", s.handleLoadTest)
	mux.HandleFunc("/inspect/", s.handleInspect)
	mux.HandleFunc("/cluster/info", s.handleClusterInfo)

	addr := ":8080"
	log.Printf("✓ Listening on http://localhost%s", addr)
	srv := &http.Server{
		Addr:         addr,
		Handler:      withCORS(mux),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
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
		"hits": hits, "misses": misses, "size": size, "hit_rate_pct": hitRate,
	})
}

func (s *Server) handleClusterInfo(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key != "" {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"key":   key,
			"shard": s.cluster.NodeFor(key),
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"shards": s.cluster.Nodes(),
		"count":  len(s.cluster.Nodes()),
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
	s.metrics.mu.RLock()
	perAlgo := make(map[string]map[string]uint64, len(s.metrics.algo))
	for name, st := range s.metrics.algo {
		perAlgo[name] = map[string]uint64{
			"total": st.total.Load(), "allowed": st.allowed.Load(), "denied": st.denied.Load(),
		}
	}
	s.metrics.mu.RUnlock()
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"total_requests": total, "allowed": allowed, "denied": denied,
		"avg_rps": rps, "uptime_seconds": uptime,
		"cache_hits": hits, "cache_misses": misses, "cache_size": cacheSize, "cache_hit_rate": cacheHitRate,
		"per_algorithm": perAlgo,
		"shards":        s.cluster.Nodes(),
	})
}

type LoadTestRequest struct {
	Algorithm   string  `json:"algorithm"`
	Requests    int     `json:"requests"`
	Concurrency int     `json:"concurrency"`
	Key         string  `json:"key"`
	Limit       int     `json:"limit"`
	Window      int     `json:"window"`
	Capacity    int     `json:"capacity"`
	Refill      float64 `json:"refill"`
}

type LoadTestResult struct {
	Sent       int     `json:"sent"`
	Allowed    int     `json:"allowed"`
	Denied     int     `json:"denied"`
	Errors     int     `json:"errors"`
	DurationMs int64   `json:"duration_ms"`
	RPS        float64 `json:"rps"`
	Shard      string  `json:"shard"`
}

func (s *Server) handleLoadTest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var req LoadTestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	if req.Requests <= 0 || req.Requests > 50000 {
		http.Error(w, "requests must be 1..50000", http.StatusBadRequest)
		return
	}
	if req.Concurrency <= 0 {
		req.Concurrency = 10
	}
	if req.Concurrency > 200 {
		req.Concurrency = 200
	}
	if req.Key == "" {
		req.Key = "loadtest_default"
	}
	var allowed, denied, errs atomic.Uint64
	start := time.Now()
	jobs := make(chan int, req.Concurrency*2)
	var wg sync.WaitGroup

	worker := func() {
		defer wg.Done()
		ctx := context.Background()
		for range jobs {
			var (
				result *limiter.Result
				err    error
			)
			switch req.Algorithm {
			case "fixed":
				result, err = s.fixed.Check(ctx, req.Key, req.Limit, req.Window)
			case "slog":
				result, err = s.slog.Check(ctx, req.Key, req.Limit, req.Window)
			case "swc":
				result, err = s.counter.Check(ctx, req.Key, req.Limit, req.Window)
			case "bucket":
				result, err = s.bucket.Check(ctx, req.Key, req.Capacity, req.Refill)
			default:
				errs.Add(1)
				continue
			}
			if err != nil {
				errs.Add(1)
				continue
			}
			s.metrics.record(req.Algorithm, result.Allowed)
			if result.Allowed {
				allowed.Add(1)
			} else {
				denied.Add(1)
			}
		}
	}
	for i := 0; i < req.Concurrency; i++ {
		wg.Add(1)
		go worker()
	}
	for i := 0; i < req.Requests; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	dur := time.Since(start)
	rps := 0.0
	if dur > 0 {
		rps = float64(req.Requests) / dur.Seconds()
	}
	writeJSON(w, http.StatusOK, LoadTestResult{
		Sent: req.Requests, Allowed: int(allowed.Load()), Denied: int(denied.Load()),
		Errors: int(errs.Load()), DurationMs: dur.Milliseconds(), RPS: rps,
		Shard: s.cluster.NodeFor(req.Key),
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
		http.Error(w, "unknown algorithm", http.StatusBadRequest)
		return
	}
	if err != nil {
		log.Printf("check error: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	s.metrics.record(req.Algorithm, result.Allowed)
	status := http.StatusOK
	if !result.Allowed {
		status = http.StatusTooManyRequests
	}
	writeJSON(w, status, CheckResponse{
		Allowed: result.Allowed, Remaining: result.Remaining,
		Current: result.Current, ResetIn: result.TTL,
		Shard: s.cluster.NodeFor(req.Key),
	})
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

var _ = fmt.Sprintf
var _ = strings.HasPrefix
