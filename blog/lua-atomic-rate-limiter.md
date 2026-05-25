# Race Conditions in a Rate Limiter, and the Five-Line Lua Fix

I built a distributed rate limiter in Go over the last six weeks.
The hardest bug I hit wasn't an algorithm bug — it was a race
condition I didn't see until I tried hard to provoke it. This is
that story, and how Redis Lua scripts make it disappear.

## The naive implementation

Here's the first version of fixed-window rate limiting I wrote.
Read it; it looks reasonable.

```go
func (f *FixedWindow) Check(ctx context.Context, key string, limit, window int) (*Result, error) {
    redisKey := "ratelimit:fixed:" + key

    // Step 1: increment the counter
    current, err := f.rdb.Incr(ctx, redisKey).Result()
    if err != nil {
        return nil, err
    }

    // Step 2: if this is the first request, set the TTL
    if current == 1 {
        f.rdb.Expire(ctx, redisKey, time.Duration(window)*time.Second)
    }

    // Step 3: decide
    allowed := current <= int64(limit)
    return &Result{Allowed: allowed, Current: current}, nil
}
```

Three Redis commands, in order. Counter goes up. If it's the first
request, set expiry. Allowed if under limit. Seems fine.

The first 10,000 sequential requests it served, it served correctly.
Then I parallelized.

## The race condition

Picture this. Two requests, R1 and R2, hit the server at the exact
same moment for a brand new key (no counter exists in Redis yet).

```
Time   R1                              R2
─────  ──────────────────────────────  ──────────────────────────────
T0                                     INCR ratelimit:fixed:user_42   → 1
T1     INCR ratelimit:fixed:user_42 → 2
T2     EXPIRE ratelimit:fixed:user_42, 60s
T3                                     (does NOT set EXPIRE — current is 2, not 1)
```

R2 incremented first and saw `current = 1`. R2's job was to set the
TTL. But before R2 could do that, R1 incremented and saw `current = 2`.
Now R2 thinks "I'm not the first request, someone else will set TTL."
R1 also thinks "current is 2, not 1, I'm not the first either."

**Neither sets the TTL.** The key now lives forever. Every subsequent
request to that key adds to a counter that will never reset. The rate
limit silently breaks for that user, possibly for hours, until someone
manually inspects Redis.

This is the classic **check-then-act race condition** — a pattern that
shows up everywhere in concurrent code. The check (`if current == 1`)
and the act (`Expire`) are separate operations. Anything can happen
between them.

## I forced it to happen

I didn't believe it at first. The race condition needs both requests to
land in Redis within microseconds of each other. Surely in practice
that's vanishingly rare?

I wrote a script that fired 100 parallel `curl` requests against a
fresh key:

```bash
for i in {1..100}; do
  (curl -s -X POST localhost:8080/check \
    -H 'Content-Type: application/json' \
    -d '{"key":"race_demo","limit":5,"window":60,"algorithm":"fixed"}' &)
done
wait
```

Then I checked Redis:

```bash
docker exec ratelimit-redis-0 redis-cli TTL ratelimit:fixed:race_demo
(integer) -1
```

`-1` means "no expiry." The race fired on the very first try. The
counter went to 100. Then I waited 60 seconds and checked again — still
`-1`. Still 100. The key would have lived until Redis was restarted.

In production, with a thousand servers and a million concurrent users,
this race fires constantly. Most rate limiters built by people who
don't know about this bug ship to production with it. Users get
silently un-rate-limited. The team finds out months later when someone
hits the database with 50,000 req/s and brings it down.

## The fix: atomicity, not luck

The bug exists because two Redis commands can interleave with two other
Redis commands. The fix is to make them not two commands — make them
one operation that runs atomically.

Redis has exactly the tool for this: **Lua scripts via EVAL**.
Crucially, **Redis executes Lua scripts single-threaded.** While a
script is running, no other command — from any client — can interleave
with it. This isn't "locking" in the usual sense; Redis is just
single-threaded by design, and Lua extends that guarantee to multi-step
logic.

Here's the same algorithm in five lines of Lua:

```lua
-- KEYS[1] = the rate-limit key
-- ARGV[1] = limit
-- ARGV[2] = window in seconds

local current = redis.call('INCR', KEYS[1])
if current == 1 then
    redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
end
local ttl = redis.call('TTL', KEYS[1])
local allowed = current <= tonumber(ARGV[1]) and 1 or 0
return {allowed, current, ttl}
```

The check (`if current == 1`) and the act (`EXPIRE`) now happen inside
the same script invocation. No other command can squeeze between them.
The race is structurally impossible.

In Go, you load the script once at startup:

```go
//go:embed fixed_window.lua
var fixedWindowScript string

type FixedWindow struct {
    rdb    *redis.Client
    script *redis.Script
}

func NewFixedWindow(rdb *redis.Client) *FixedWindow {
    return &FixedWindow{
        rdb:    rdb,
        script: redis.NewScript(fixedWindowScript),
    }
}
```

And invoke it per request:

```go
result, err := f.script.Run(ctx, f.rdb, []string{redisKey}, limit, window).Result()
```

`redis.NewScript` is smart about this — it uses `EVALSHA` under the
hood, which sends only the script's SHA1 hash after the first call.
The script body is cached on the Redis server. Bandwidth-wise, calling
a Lua script costs about the same as calling INCR.

## I forced the race again — it didn't happen

Same test, 100 parallel curls against a fresh key, this time against
the Lua-based implementation:

```bash
docker exec ratelimit-redis-0 redis-cli TTL ratelimit:fixed:race_demo
(integer) 58
```

TTL of 58 seconds. The expiry was set. After 60 seconds the key
disappears, the counter resets, the rate limit works correctly.

I ran the test 50 more times. TTL was set every time.

## The pattern, more generally

The Lua-atomicity pattern is reusable. I ended up using it for all four
algorithms in the limiter:

1. **Fixed window** — INCR + EXPIRE (the example above)
2. **Sliding window log** — ZREMRANGEBYSCORE old entries + ZCARD count + ZADD new entry, all in one script
3. **Sliding window counter** — read current bucket + previous bucket + compute weighted estimate + INCR if allowed
4. **Token bucket** — read tokens & last_us + compute refill via elapsed time + decrement & save if allowed

Every one of these has a multi-step "check the current state, decide
based on it, then write" pattern. Every one of these is a race
condition waiting to happen. Lua makes every one of them atomic.

## The performance numbers

I was worried Lua would be slow. It isn't.

After this fix landed and three Redis shards came online behind a
consistent hash ring, the limiter benchmarked at:

- **18,769 RPS** over real HTTP (k6, 200 concurrent virtual users)
- **343 µs p50 latency** at sustained 2,000 RPS over 1,000 distinct keys
- **1.93 ms p99 latency** at the same sustained load
- **Zero errors** across 1.7M+ test requests

Lua isn't a slow path inside Redis — it's running compiled Lua bytecode
inside an embedded interpreter that's been tuned for this workload
since 2011. A typical script like the fixed-window one above adds
about 20–50 microseconds over a single command. Negligible compared to
the alternative (subtle production bugs and the months of debugging
they cause).

## Takeaways

1. **Concurrency bugs hide.** The naive version of my rate limiter
   passed 10,000 sequential test requests before I tried parallelism.
   Sequential tests are not concurrency tests.

2. **Reproduce, don't theorize.** Until I wrote the 100-curl
   parallel script and saw TTL = -1 with my own eyes, I half-believed
   the race was rare enough to ignore. It isn't.

3. **Redis Lua scripting is underused.** Most engineers who reach for
   Redis use it as a key-value store and stop there. Atomicity across
   multiple operations is a superpower it gives you for almost no
   added complexity.

4. **Single-threaded execution is a feature, not a bug.** People
   sometimes complain Redis is "only single-threaded." But that single
   thread is what makes Lua scripts atomic. The same property that
   limits Redis's CPU parallelism is the property that makes the
   correctness story easy.

The full source is at
[sameer-sde/ratelimit](https://github.com/sameer-sde/ratelimit).
The Lua scripts live under `internal/limiter/`.

---

*Want to read more? The "Designing Data-Intensive Applications" chapter
on linearizability and the
[Redis documentation on scripting](https://redis.io/docs/latest/develop/interact/programmability/eval-intro/)
are the two best deep-dives I know on this topic.*
