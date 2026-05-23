package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/sameer-sde/ratelimit/internal/limiter"
	"github.com/redis/go-redis/v9"
)

type CheckRequest struct {
	Key       string `json:"key"`
	Limit     int    `json:"limit"`
	Window    int    `json:"window"`
	Algorithm string `json:"algorithm"`
}

type CheckResponse struct {
	Allowed   bool  `json:"allowed"`
	Remaining int64 `json:"remaining"`
	Current   int64 `json:"current"`
	ResetIn   int64 `json:"reset_in_seconds"`
}

type Server struct {
	fixed *limiter.FixedWindow
	slog  *limiter.SlidingWindowLog
}

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()

	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("redis ping failed: %v", err)
	}
	log.Println("✓ Connected to Redis")

	s := &Server{
		fixed: limiter.NewFixedWindow(rdb),
		slog:  limiter.NewSlidingWindowLog(rdb),
	}

	http.HandleFunc("/check", s.handleCheck)
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})

	addr := ":8080"
	log.Printf("✓ Listening on http://localhost%s", addr)
	srv := &http.Server{
		Addr:         addr,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
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
	if req.Key == "" || req.Limit <= 0 || req.Window <= 0 {
		http.Error(w, "key, limit, window all required and positive", http.StatusBadRequest)
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
		result, err = s.fixed.Check(r.Context(), req.Key, req.Limit, req.Window)
	case "slog":
		result, err = s.slog.Check(r.Context(), req.Key, req.Limit, req.Window)
	default:
		http.Error(w, "unknown algorithm: 'fixed' or 'slog'", http.StatusBadRequest)
		return
	}

	if err != nil {
		log.Printf("check error: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
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
