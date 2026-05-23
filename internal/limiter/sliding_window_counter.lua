-- Sliding window counter rate limiter
-- KEYS[1] = redis key prefix (we append :curr and :prev)
-- ARGV[1] = limit
-- ARGV[2] = window in seconds
-- ARGV[3] = current time in seconds (unix)
--
-- Returns: {allowed (1/0), approximate_count_floor, seconds_until_reset}

local key_prefix = KEYS[1]
local limit      = tonumber(ARGV[1])
local window     = tonumber(ARGV[2])
local now        = tonumber(ARGV[3])

-- Which bucket are we in? Integer division by window length.
local curr_bucket = math.floor(now / window)
local prev_bucket = curr_bucket - 1

local curr_key = key_prefix .. ':' .. curr_bucket
local prev_key = key_prefix .. ':' .. prev_bucket

-- Read both bucket counts (nil if doesn't exist → treat as 0).
local curr_count = tonumber(redis.call('GET', curr_key)) or 0
local prev_count = tonumber(redis.call('GET', prev_key)) or 0

-- How far into the current bucket are we, as a fraction [0, 1)?
local elapsed_in_bucket = now - (curr_bucket * window)
local elapsed_fraction  = elapsed_in_bucket / window

-- The estimate.
local sliding_estimate = prev_count * (1 - elapsed_fraction) + curr_count

if sliding_estimate >= limit then
    -- Denied. Don't increment.
    local reset_in = window - elapsed_in_bucket
    return {0, math.floor(sliding_estimate), reset_in}
end

-- Allowed — increment current bucket and set TTL.
-- We keep prev bucket alive for 2 windows so it's still readable next window.
redis.call('INCR', curr_key)
redis.call('EXPIRE', curr_key, window * 2)

local reset_in = window - elapsed_in_bucket
return {1, math.floor(sliding_estimate) + 1, reset_in}
