package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"
)

// CheckRequest is what clients send to /check
type CheckRequest struct {
	Key    string `json:"key"`    // e.g. "user_123:login"
	Limit  int    `json:"limit"`  // max requests in window
	Window int    `json:"window"` // window size in seconds
}

// CheckResponse is what we send back
type CheckResponse struct {
	Allowed   bool  `json:"allowed"`
	Remaining int   `json:"remaining"`
	ResetAt   int64 `json:"reset_at"` // unix timestamp when window resets
}

// Server holds shared dependencies. In larger Go apps, this struct
// gets passed around or has methods hung off it.
type Server struct {
	rdb *redis.Client
}

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()

	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("redis ping failed: %v", err)
	}
	log.Println("✓ Connected to Redis")

	s := &Server{rdb: rdb}

	http.HandleFunc("/check", s.handleCheck)
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})

	addr := ":8080"
	log.Printf("✓ Listening on http://localhost%s", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatal(err)
	}
}

// handleCheck is our rate-limit decision endpoint.
// THIS VERSION HAS A RACE CONDITION ON PURPOSE. We'll fix it Day 2.
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
	if req.Key == "" || req.Limit <= 0 || req.Window <= 0 {
		http.Error(w, "key, limit, window all required and positive", http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	// Build the Redis key: "ratelimit:fixed:<userkey>:<window-bucket>"
	// We bucket by the current window so each window gets a fresh counter
	// that Redis auto-expires.
	now := time.Now().Unix()
	bucket := now / int64(req.Window) // integer division → which window we're in
	redisKey := fmt.Sprintf("ratelimit:fixed:%s:%d", req.Key, bucket)

	// === THE RACE CONDITION LIVES HERE ===
	// Step A: read current count
	countStr, err := s.rdb.Get(ctx, redisKey).Result()
	count := 0
	if err == redis.Nil {
		// key doesn't exist yet — first request in this window
		count = 0
	} else if err != nil {
		http.Error(w, "redis error", http.StatusInternalServerError)
		return
	} else {
		fmt.Sscanf(countStr, "%d", &count)
	}

	// Step B: decide
	if count >= req.Limit {
		// over the limit, deny
		resp := CheckResponse{
			Allowed:   false,
			Remaining: 0,
			ResetAt:   (bucket + 1) * int64(req.Window),
		}
		writeJSON(w, http.StatusOK, resp)
		return
	}

	// Step C: increment (and set TTL if it's the first hit in this window)
	pipe := s.rdb.Pipeline()
	pipe.Incr(ctx, redisKey)
	pipe.Expire(ctx, redisKey, time.Duration(req.Window)*time.Second)
	if _, err := pipe.Exec(ctx); err != nil {
		http.Error(w, "redis error", http.StatusInternalServerError)
		return
	}

	resp := CheckResponse{
		Allowed:   true,
		Remaining: req.Limit - (count + 1),
		ResetAt:   (bucket + 1) * int64(req.Window),
	}
	writeJSON(w, http.StatusOK, resp)
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
