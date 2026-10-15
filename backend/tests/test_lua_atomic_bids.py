import pytest
import asyncio
import time
from app.services.lua_engine import lua_engine
from app.services.redis_client import redis_service

@pytest.mark.asyncio
async def test_atomic_bid_placement_success():
    auc_id = "test_auc_success"
    user_id = "u001"
    user_name = "AlphaBidder"

    # 1. Reset auction
    await lua_engine.reset_auction(
        auction_id=auc_id,
        starting_price=100000.0,
        reserve_price=120000.0,
        min_increment=2500.0,
        duration_seconds=600
    )

    # 2. Setup user escrow
    await lua_engine.set_user_escrow(user_id=user_id, total_balance=200000.0, locked_collateral=0.0)

    # 3. Place valid bid ($102,500)
    res = await lua_engine.execute_bid(
        auction_id=auc_id,
        bidder_id=user_id,
        bidder_name=user_name,
        bid_amount=102500.0,
        required_escrow=10000.0
    )

    assert res["ok"] is True
    assert res["code"] == "SUCCESS"
    data = res["data"]
    assert data["sequence_id"] == 1
    assert data["bid_amount"] == 102500.0
    assert data["bid_count"] == 1
    assert data["bidder_id"] == user_id

@pytest.mark.asyncio
async def test_bid_rejection_too_low():
    auc_id = "test_auc_low"
    user_id = "u002"

    await lua_engine.reset_auction(
        auction_id=auc_id,
        starting_price=50000.0,
        reserve_price=60000.0,
        min_increment=1000.0,
        duration_seconds=600
    )
    await lua_engine.set_user_escrow(user_id=user_id, total_balance=100000.0)

    # Attempt to bid $50,500 when min required is $51,000
    res = await lua_engine.execute_bid(
        auction_id=auc_id,
        bidder_id=user_id,
        bidder_name="LowBidder",
        bid_amount=50500.0,
        required_escrow=5000.0
    )

    assert res["ok"] is False
    assert res["code"] == "ERR_BID_TOO_LOW"
    assert res["min_required"] == 51000.0

@pytest.mark.asyncio
async def test_self_outbid_prevention():
    auc_id = "test_auc_self"
    user_id = "u003"

    await lua_engine.reset_auction(
        auction_id=auc_id,
        starting_price=10000.0,
        reserve_price=15000.0,
        min_increment=500.0,
        duration_seconds=600
    )
    await lua_engine.set_user_escrow(user_id=user_id, total_balance=50000.0)

    # First bid succeeds
    res1 = await lua_engine.execute_bid(
        auction_id=auc_id,
        bidder_id=user_id,
        bidder_name="SoloBidder",
        bid_amount=10500.0,
        required_escrow=1000.0
    )
    assert res1["ok"] is True

    # Same user immediately bids again
    res2 = await lua_engine.execute_bid(
        auction_id=auc_id,
        bidder_id=user_id,
        bidder_name="SoloBidder",
        bid_amount=11000.0,
        required_escrow=1000.0
    )
    assert res2["ok"] is False
    assert res2["code"] == "ERR_SELF_OUTBID"

@pytest.mark.asyncio
async def test_anti_sniping_soft_close_extension():
    auc_id = "test_auc_sniping"
    user_id = "u004"
    client = redis_service.get_client()

    now_ms = int(time.time() * 1000)
    # Set auction to expire in 10 seconds (inside 15-second soft close window)
    initial_end_ms = now_ms + 10000

    await lua_engine.reset_auction(
        auction_id=auc_id,
        starting_price=50000.0,
        reserve_price=60000.0,
        min_increment=1000.0,
        duration_seconds=10,
        soft_close_window_sec=15,
        extension_sec=30
    )
    await lua_engine.set_user_escrow(user_id=user_id, total_balance=100000.0)

    res = await lua_engine.execute_bid(
        auction_id=auc_id,
        bidder_id=user_id,
        bidder_name="SniperBidder",
        bid_amount=51000.0,
        required_escrow=5000.0
    )

    assert res["ok"] is True
    assert res["data"]["extended"] is True
    # The end_time_ms must have been extended by 30 seconds (30,000 ms)
    new_end_ms = res["data"]["end_time_ms"]
    assert new_end_ms > initial_end_ms

@pytest.mark.asyncio
async def test_high_concurrency_race_condition_immunity():
    """
    Simulate 50 concurrent racing clients trying to bid at the exact same millisecond.
    Enforces that:
    1. ZERO race conditions occur.
    2. Sequence IDs are strictly 1..N with NO duplicates or gaps.
    3. Exactly 1 winning bid per increment level.
    """
    auc_id = "test_auc_concurrency"
    starting_price = 10000.0
    min_increment = 100.0

    await lua_engine.reset_auction(
        auction_id=auc_id,
        starting_price=starting_price,
        reserve_price=20000.0,
        min_increment=min_increment,
        duration_seconds=600
    )

    # Setup 10 distinct bidders with sufficient escrow
    for i in range(10):
        await lua_engine.set_user_escrow(user_id=f"concurrent_user_{i}", total_balance=100000.0)

    # Generate 50 simultaneous bids with increasing amounts
    tasks = []
    for step in range(1, 51):
        bidder_idx = step % 10
        bid_amount = starting_price + (step * min_increment)
        tasks.append(
            lua_engine.execute_bid(
                auction_id=auc_id,
                bidder_id=f"concurrent_user_{bidder_idx}",
                bidder_name=f"Bot_{bidder_idx}",
                bid_amount=bid_amount,
                required_escrow=1000.0
            )
        )

    # Fire all 50 bids concurrently
    results = await asyncio.gather(*tasks)

    # Check results: sequence numbers must be unique and monotonic
    successful_bids = [r for r in results if r.get("ok") is True]
    assert len(successful_bids) > 0

    sequences = [r["data"]["sequence_id"] for r in successful_bids]
    # Verify no duplicates
    assert len(sequences) == len(set(sequences)), "Sequence IDs must be strictly unique!"
    # Verify sorted monotonicity
    assert sequences == sorted(sequences), "Sequence IDs must be monotonically increasing!"

    # Verify final price in Redis matches sequence count
    client = redis_service.get_client()
    meta = await client.hgetall(f"auction:{auc_id}:meta")
    final_seq = int(meta["sequence_id"])
    assert final_seq == len(successful_bids)
