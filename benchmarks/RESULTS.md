# Benchmark Results

**Hardware:** MacBook Air (Apple Silicon)
**Setup:** Go server (single instance) + 3 Redis containers (Docker), all on localhost
**Tool:** [k6](https://k6.io) v0.50+
**Date:** May 2026

---

## 1. Peak throughput — Fixed Window
*Script: `peak_fixed.js`. Ramp to 200 VUs, hammer a single key, very high limit so we measure server ceiling.*

| Metric                | Value           |
|-----------------------|-----------------|
| **Sustained RPS**     | **37,553**      |
| Total requests        | 1,689,886 in 45s |
| avg latency           | 4.03 ms         |
| p90 latency           | 8.23 ms         |
| p95 latency           | 10.24 ms        |
| Error rate            | 0.00%           |

---

## 2. Latency at sustained 2000 RPS
*Script: `latency.js`. 2000 req/s for 30s across 1000 distinct user keys, routed through the hash ring to 3 Redis shards.*

| Metric                | Value           |
|-----------------------|-----------------|
| **p50 (median)**      | **343 µs**      |
| p95                   | 724 µs          |
| **p99**               | **1.93 ms**     |
| avg                   | 559 µs          |
| Total requests        | 59,979          |
| Error rate            | 0.00%           |

Sub-millisecond p95 latency confirms the system has headroom: at production-realistic load it's nowhere near saturation.

---

## 3. Algorithm comparison
*Script: `algo_compare.js`. 50 VUs × 15s per algorithm, run sequentially.*

| Algorithm                | RPS         | Notes                                  |
|--------------------------|------------:|----------------------------------------|
| Fixed Window             | ~43,000     | INCR + EXPIRE — cheapest               |
| Sliding Window Log       | TBD         | sorted-set ZADD/ZREMRANGEBYSCORE       |
| Sliding Window Counter   | TBD         | two integer reads + math               |
| Token Bucket             | TBD         | HGET/HSET + lazy refill                |

Combined across all four: **1,304,921 requests in 75s = 17,400 RPS avg**, p95 5.38ms, **0 errors**.

---

## 4. Reference: in-process load tester
For sanity-check only. This measures the limiter directly with no HTTP overhead — same Go process, no JSON, no TCP.

- **47,297 RPS** (fixed window, 1000 req @ 50 concurrency)

The HTTP-level number (37,553) is ~80% of the in-process ceiling — a tight, well-tuned HTTP
