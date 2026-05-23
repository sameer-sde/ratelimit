-- Fixed window rate limiter
-- KEYS[1] = the Redis key (e.g. "ratelimit:fixed:user_123:567890")
-- ARGV[1] = limit (max requests in window)
-- ARGV[2] = window size in seconds
--
-- Returns: { allowed (1 or 0), current_count, ttl_seconds }

local key    = KEYS[1]
local limit  = tonumber(ARGV[1])
local window = tonumber(ARGV[2])

-- Atomically increment. If the key didn't exist, this creates it at 1.
local current = redis.call('INCR', key)

-- If this is the first hit in the window, set the TTL so the key auto-expires.
if current == 1 then
    redis.call('EXPIRE', key, window)
end

-- Get remaining TTL so we can tell the client when the window resets.
local ttl = redis.call('TTL', key)

if current > limit then
    -- Over the limit. We still incremented (which is fine — it just decays out
    -- with the TTL). Return 0 = denied.
    return {0, current, ttl}
end

return {1, current, ttl}
