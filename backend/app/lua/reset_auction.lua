-- ============================================================================
-- Atomic Auction Initialization / Reset Script
-- Pre-loads auction metadata and clears ephemeral streams/lists
-- ============================================================================
--
-- KEYS:
--   KEYS[1] : auction:{auction_id}:meta
--   KEYS[2] : auction_bids:{auction_id}
--   KEYS[3] : auction_stream:{auction_id}
--
-- ARGV:
--   ARGV[1] : auction_id
--   ARGV[2] : starting_price
--   ARGV[3] : reserve_price
--   ARGV[4] : min_increment
--   ARGV[5] : duration_seconds
--   ARGV[6] : now_epoch_ms
--   ARGV[7] : soft_close_window_sec
--   ARGV[8] : extension_sec

local meta_key      = KEYS[1]
local bids_list_key = KEYS[2]
local stream_key    = KEYS[3]

local auction_id            = ARGV[1]
local starting_price        = tonumber(ARGV[2]) or 100000
local reserve_price         = tonumber(ARGV[3]) or 120000
local min_increment         = tonumber(ARGV[4]) or 2500
local duration_seconds      = tonumber(ARGV[5]) or 1800
local now_ms                = tonumber(ARGV[6]) or 0
local soft_close_window_sec = tonumber(ARGV[7]) or 15
local extension_sec         = tonumber(ARGV[8]) or 30

local end_time_ms = now_ms + (duration_seconds * 1000)

-- Clean previous lists and streams
redis.call("DEL", bids_list_key)
redis.call("DEL", stream_key)

-- Set fresh metadata hash
redis.call("HMSET", meta_key,
    "status", "ACTIVE",
    "current_price", string.format("%.2f", starting_price),
    "reserve_price", string.format("%.2f", reserve_price),
    "min_increment", string.format("%.2f", min_increment),
    "highest_bidder_id", "",
    "highest_bidder_name", "",
    "bid_count", "0",
    "sequence_id", "0",
    "end_time_ms", tostring(end_time_ms),
    "soft_close_window_sec", tostring(soft_close_window_sec),
    "extension_sec", tostring(extension_sec)
)

-- Broadcast reset event
local reset_event = cjson.encode({
    event = "AUCTION_RESET",
    auction_id = auction_id,
    current_price = starting_price,
    sequence_id = 0,
    end_time_ms = end_time_ms,
    timestamp = now_ms
})
redis.call("PUBLISH", "channel:auction:" .. auction_id, reset_event)

return cjson.encode({
    ok = true,
    code = "RESET_SUCCESS",
    auction_id = auction_id,
    current_price = starting_price,
    end_time_ms = end_time_ms
})
