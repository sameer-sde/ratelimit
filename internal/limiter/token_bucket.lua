-- Token bucket rate limiter
-- KEYS[1] = redis key (stores tokens + last_refill_us as a hash)
-- ARGV[1] = capacity (max tokens the bucket holds)
-- ARGV[2] = refill_rate (tokens per second, can be fractional)
-- ARGV[3] = now in microseconds
-- ARGV[4] = TTL seconds (auto-cleanup when bucket idle)
--
-- Returns: {allowed (1/0), tokens_remaining_floor, refill_time_us_for_one_token}

local key          = KEYS[1]
local capacity     = tonumber(ARGV[1])
local refill_rate  = tonumber(ARGV[2])    -- tokens per SECOND
local now_us       = tonumber(ARGV[3])
local ttl          = tonumber(ARGV[4])

-- Read current state. HMGET returns {nil, nil} if the key doesn't exist.
local state = redis.call('HMGET', key, 'tokens', 'last_us')
local tokens  = tonumber(state[1])
local last_us = tonumber(state[2])

-- First-ever request for this key → start with a full bucket.
if tokens == nil then
    tokens  = capacity
    last_us = now_us
end

-- Lazy refill: add tokens proportional to elapsed time.
local elapsed_us  = now_us - last_us
local refilled    = (elapsed_us / 1000000) * refill_rate
tokens = math.min(capacity, tokens + refilled)

-- Decide.
local allowed = 0
if tokens >= 1 then
    tokens  = tokens - 1
    allowed = 1
end

-- Write state back.
redis.call('HMSET', key, 'tokens', tokens, 'last_us', now_us)
redis.call('EXPIRE', key, ttl)

-- How long until 1 full token is available (microseconds)?
-- Useful for the client to know when to retry.
local need_for_one = 0
if tokens < 1 then
    need_for_one = math.ceil(((1 - tokens) / refill_rate) * 1000000)
end

return {allowed, math.floor(tokens), need_for_one}
