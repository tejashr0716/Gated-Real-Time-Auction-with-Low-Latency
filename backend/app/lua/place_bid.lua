-- ============================================================================
-- Atomic High-Frequency Bid Placement Engine
-- Enforces Zero Race Conditions, Anti-Sniping Soft-Close, Gated Escrow Locking,
-- Monotonic Sequence Numbers, and Event Telemetry Broadcasting.
-- ============================================================================
--
-- KEYS:
--   KEYS[1] : auction:{auction_id}:meta        (Hash: status, current_price, etc.)
--   KEYS[2] : escrow:{bidder_id}              (Hash: total_balance, locked_collateral)
--   KEYS[3] : auction_bids:{auction_id}       (List: recent bids history)
--   KEYS[4] : auction_stream:{auction_id}     (Stream: durable write-behind to DB)
--   KEYS[5] : escrow:{prev_bidder_id}         (Optional: previous highest bidder escrow)
--
-- ARGV:
--   ARGV[1] : auction_id                      (string)
--   ARGV[2] : bidder_id                       (string UUID)
--   ARGV[3] : bidder_name                     (string)
--   ARGV[4] : bid_amount                      (number as string)
--   ARGV[5] : now_epoch_ms                    (integer timestamp in milliseconds)
--   ARGV[6] : required_escrow                 (number as string)

local meta_key      = KEYS[1]
local escrow_key    = KEYS[2]
local bids_list_key = KEYS[3]
local stream_key    = KEYS[4]

local auction_id      = ARGV[1]
local bidder_id       = ARGV[2]
local bidder_name     = ARGV[3]
local bid_amount      = tonumber(ARGV[4])
local now_ms          = tonumber(ARGV[5])
local required_escrow = tonumber(ARGV[6]) or 0

-- ----------------------------------------------------------------------------
-- 1. Verify Auction Existence
-- ----------------------------------------------------------------------------
if redis.call("EXISTS", meta_key) == 0 then
    return cjson.encode({
        ok = false,
        code = "ERR_AUCTION_NOT_FOUND",
        msg = "Auction " .. auction_id .. " does not exist in active memory."
    })
end

-- ----------------------------------------------------------------------------
-- 2. Fetch Current Auction State Atomically
-- ----------------------------------------------------------------------------
local meta = redis.call("HMGET", meta_key,
    "status",                  -- 1
    "current_price",           -- 2
    "min_increment",           -- 3
    "end_time_ms",             -- 4
    "highest_bidder_id",       -- 5
    "highest_bidder_name",     -- 6
    "bid_count",               -- 7
    "sequence_id",             -- 8
    "soft_close_window_sec",   -- 9
    "extension_sec",           -- 10
    "reserve_price"            -- 11
)

local status                = meta[1] or "INACTIVE"
local current_price         = tonumber(meta[2]) or 0
local min_increment         = tonumber(meta[3]) or 1
local end_time_ms           = tonumber(meta[4]) or 0
local highest_bidder_id     = meta[5] or ""
local highest_bidder_name   = meta[6] or ""
local bid_count             = tonumber(meta[7]) or 0
local sequence_id           = tonumber(meta[8]) or 0
local soft_close_window_sec = tonumber(meta[9]) or 15
local extension_sec         = tonumber(meta[10]) or 30
local reserve_price         = tonumber(meta[11]) or 0

-- ----------------------------------------------------------------------------
-- 3. Auction Lifecycle Validation
-- ----------------------------------------------------------------------------
if status ~= "ACTIVE" then
    return cjson.encode({
        ok = false,
        code = "ERR_AUCTION_NOT_ACTIVE",
        msg = "Auction is currently " .. status .. ". Bids are prohibited.",
        current_status = status
    })
end

if now_ms >= end_time_ms then
    redis.call("HSET", meta_key, "status", "ENDED")
    return cjson.encode({
        ok = false,
        code = "ERR_AUCTION_EXPIRED",
        msg = "Hammer has dropped! Auction concluded at " .. end_time_ms .. " ms.",
        end_time_ms = end_time_ms,
        now_ms = now_ms
    })
end

-- ----------------------------------------------------------------------------
-- 4. Self-Outbid Guard
-- ----------------------------------------------------------------------------
if highest_bidder_id == bidder_id then
    return cjson.encode({
        ok = false,
        code = "ERR_SELF_OUTBID",
        msg = "Bidder " .. bidder_name .. " is already the winning high bidder.",
        current_price = current_price
    })
end

-- ----------------------------------------------------------------------------
-- 5. Bid Increment Validation
-- ----------------------------------------------------------------------------
local min_required = current_price + min_increment
if bid_amount < min_required then
    return cjson.encode({
        ok = false,
        code = "ERR_BID_TOO_LOW",
        msg = string.format("Bid $%.2f is below minimum required $%.2f (Increment: $%.2f)", bid_amount, min_required, min_increment),
        current_price = current_price,
        min_required = min_required,
        min_increment = min_increment
    })
end

-- ----------------------------------------------------------------------------
-- 6. Gated Escrow Collateral Verification
-- ----------------------------------------------------------------------------
if redis.call("EXISTS", escrow_key) == 1 then
    local esc_balance = tonumber(redis.call("HGET", escrow_key, "total_balance") or 0)
    local esc_locked  = tonumber(redis.call("HGET", escrow_key, "locked_collateral") or 0)
    local esc_avail   = esc_balance - esc_locked

    if esc_avail < required_escrow then
        return cjson.encode({
            ok = false,
            code = "ERR_INSUFFICIENT_ESCROW",
            msg = string.format("Gated verification failed: Unencumbered escrow ($%.2f) is below required collateral ($%.2f)", esc_avail, required_escrow),
            available = esc_avail,
            required = required_escrow
        })
    end
else
    return cjson.encode({
        ok = false,
        code = "ERR_ESCROW_NOT_REGISTERED",
        msg = "Bidder has no active escrow collateral account on record."
    })
end

-- ----------------------------------------------------------------------------
-- 7. Anti-Sniping (Soft Close) Window Extension
-- ----------------------------------------------------------------------------
local extended = false
local remaining_ms = end_time_ms - now_ms
local soft_close_window_ms = soft_close_window_sec * 1000

if remaining_ms <= soft_close_window_ms then
    end_time_ms = end_time_ms + (extension_sec * 1000)
    extended = true
end

-- ----------------------------------------------------------------------------
-- 8. Collateral Realignment (Release Previous, Lock Incoming)
-- ----------------------------------------------------------------------------
if highest_bidder_id ~= "" and highest_bidder_id ~= bidder_id then
    local prev_escrow_key = nil
    if #KEYS >= 5 and KEYS[5] and KEYS[5] ~= "" and KEYS[5] ~= "nil" then
        prev_escrow_key = KEYS[5]
    else
        prev_escrow_key = "escrow:" .. highest_bidder_id
    end

    if redis.call("EXISTS", prev_escrow_key) == 1 then
        local prev_locked = tonumber(redis.call("HGET", prev_escrow_key, "locked_collateral") or 0)
        local prev_auc_hold = tonumber(redis.call("HGET", prev_escrow_key, "locked_for_auc_" .. auction_id) or 0)
        local new_prev_locked = prev_locked - prev_auc_hold
        if new_prev_locked < 0 then new_prev_locked = 0 end
        redis.call("HSET", prev_escrow_key, "locked_collateral", tostring(new_prev_locked))
        redis.call("HDEL", prev_escrow_key, "locked_for_auc_" .. auction_id)
    end
end

-- Lock collateral on winning bidder
local current_locked = tonumber(redis.call("HGET", escrow_key, "locked_collateral") or 0)
redis.call("HSET", escrow_key, "locked_collateral", tostring(current_locked + required_escrow))
redis.call("HSET", escrow_key, "locked_for_auc_" .. auction_id, tostring(required_escrow))

-- ----------------------------------------------------------------------------
-- 9. Monotonic State Update
-- ----------------------------------------------------------------------------
sequence_id = sequence_id + 1
bid_count = bid_count + 1
current_price = bid_amount
highest_bidder_id = bidder_id
highest_bidder_name = bidder_name

redis.call("HMSET", meta_key,
    "current_price", string.format("%.2f", current_price),
    "highest_bidder_id", highest_bidder_id,
    "highest_bidder_name", highest_bidder_name,
    "bid_count", tostring(bid_count),
    "sequence_id", tostring(sequence_id),
    "end_time_ms", tostring(end_time_ms)
)

-- ----------------------------------------------------------------------------
-- 10. Telemetry Payload & Event Broadcasting
-- ----------------------------------------------------------------------------
local event_data = {
    event = "BID_ACCEPTED",
    auction_id = auction_id,
    sequence_id = sequence_id,
    bid_amount = current_price,
    bidder_id = bidder_id,
    bidder_name = bidder_name,
    timestamp = now_ms,
    end_time_ms = end_time_ms,
    extended = extended,
    bid_count = bid_count,
    min_next_bid = current_price + min_increment,
    reserve_met = (current_price >= reserve_price)
}
local event_json = cjson.encode(event_data)

-- Cache recent bids list (LIFO window of last 100 bids)
redis.call("LPUSH", bids_list_key, event_json)
redis.call("LTRIM", bids_list_key, 0, 99)

-- Append to Stream for write-behind persistence
redis.call("XADD", stream_key, "*",
    "auction_id", auction_id,
    "bidder_id", bidder_id,
    "bidder_name", bidder_name,
    "sequence_id", tostring(sequence_id),
    "amount", string.format("%.2f", current_price),
    "timestamp", tostring(now_ms),
    "extended", tostring(extended)
)

-- Publish to Pub/Sub Channel for zero-latency fanout to WebSockets
local pubsub_channel = "channel:auction:" .. auction_id
redis.call("PUBLISH", pubsub_channel, event_json)

return cjson.encode({
    ok = true,
    code = "SUCCESS",
    data = event_data
})
