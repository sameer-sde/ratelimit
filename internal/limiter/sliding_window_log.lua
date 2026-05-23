-- Sliding window log rate limiter
-- KEYS[1] = redis key
-- ARGV[1] = limit
-- ARGV[2] = window in seconds
-- ARGV[3] = current time in microseconds
-- ARGV[4] = unique request id
-- Returns: {allowed (1/0), current_count, oldest_microseconds_in_window}

local key      = KEYS[1]
local limit    = tonumber(ARGV[1])
local window   = tonumber(ARGV[2])
local now_us   = tonumber(ARGV[3])
local req_id   = ARGV[4]

local window_us = window * 1000000
local cutoff_us = now_us - window_us

-- 1. Evict everything older than the window
redis.call('ZREMRANGEBYSCORE', key, '-inf', '(' .. cutoff_us)

-- 2. Count what's left
local count = redis.call('ZCARD', key)

-- 3. Already at or over the limit? Deny without adding.
if count >= limit then
    local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
    local oldest_us = 0
    if #oldest > 0 then oldest_us = tonumber(oldest[2]) end
    return {0, count, oldest_us}
end

-- 4. Allowed — log this request
redis.call('ZADD', key, now_us, req_id)
redis.call('EXPIRE', key, window)

return {1, count + 1, now_us}
