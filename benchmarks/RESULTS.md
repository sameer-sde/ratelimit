# Benchmark Results

**Hardware:** MacBook Air (Apple Silicon)
**Setup:** Go server (single instance) + 3 Redis containers (Docker), all on localhost
**Tool:** [k6](https://k6.io) v0.50+
**Date:** May 2026

---

## 1. Peak throughput — Fixed Window (after Day 14 tuning)
*Script: `peak_fixed.js`. Ramp to 200 VUs, hammer a single key.*

| Metric           | Value        |
|------------------|--------------|
| **Sustained RPS**| **18,769**   |
| p95 latency      | 15.93 ms     |
| p90 latency      | 13.55 ms     |
| avg latency      | 8.42 ms      |
| Total requests   | 844,619 in 45s |
| Error rate       | 0.00%        |

## 2. Latency at sustained 2000 RPS
*Script: `latency.js`. 2000 req/s for 30s across 1000 distinct user keys via hash ring.*

| Metric           | Value        |
|------------------|--------------|
| **p50 (median)** | **343 µs**   |
| p95              | 724 µs       |
| **p99**          | **1.93 ms**  |
| avg              | 559 µs       |
| Error rate       | 0.00%        |

Sub-millisecond p95 at production-realistic load.

## 3. Day 14 performance tuning — before/after

Two config changes, measured back-to-back on the same machine.

**Changes:**
- Redis connection pool: 10 → **50 per shard** (3 shards = 150 total)
- HTTP server: added `IdleTimeout: 120s` for keep-alive reuse, tightened `ReadTimeout`/`WriteTimeout`, capped `MaxHeaderBytes`

| Metric        | Before    | After     | Delta     |
|---------------|----------:|----------:|----------:|
| RPS           | 11,657    | **18,769**| **+61%**  |
| p95 latency   | 28.08 ms  | 15.93 ms  | -43%      |
| p90 latency   | 22.23 ms  | 13.55 ms  | -39%      |
| avg latency   | 13.34 ms  | 8.42 ms   | -37%      |

The connection pool was the bottleneck. With only 10 connections per shard and up to 200 concurrent goroutines competing, requests queued. Bumping to 50 eliminated the queue.

## 4. Reference: in-process load tester
For sanity. Measures the limiter directly with no HTTP/JSON overhead.
- 47,297 RPS (fixed window via dashboard `/load-test`)

## Methodology notes
- 429 responses treated as expected (rate limiter correctly rejecting overflow), not as failures.
- LRU decision cache enabled (10k entries, 100ms TTL).
- Hash ring: 150 virtual nodes per Redis shard.
- All tests on localhost; production over network would add ~1-5ms per request.
