import pytest
from app.services.lua_engine import lua_engine
from app.services.redis_client import redis_service

@pytest.mark.asyncio
async def test_gated_escrow_insufficient_funds():
    auc_id = "test_auc_escrow_fail"
    user_id = "poor_bidder"

    await lua_engine.reset_auction(
        auction_id=auc_id,
        starting_price=100000.0,
        reserve_price=120000.0,
        min_increment=5000.0,
        duration_seconds=600
    )

    # User only has $2,000 balance, but auction requires $15,000 collateral
    await lua_engine.set_user_escrow(user_id=user_id, total_balance=2000.0, locked_collateral=0.0)

    res = await lua_engine.execute_bid(
        auction_id=auc_id,
        bidder_id=user_id,
        bidder_name="BrokeBidder",
        bid_amount=105000.0,
        required_escrow=15000.0
    )

    assert res["ok"] is False
    assert res["code"] == "ERR_INSUFFICIENT_ESCROW"
    assert res["available"] == 2000.0
    assert res["required"] == 15000.0

@pytest.mark.asyncio
async def test_gated_escrow_lock_and_release():
    auc_id = "test_auc_collateral_transfer"
    user_a = "bidder_a"
    user_b = "bidder_b"
    client = redis_service.get_client()

    await lua_engine.reset_auction(
        auction_id=auc_id,
        starting_price=50000.0,
        reserve_price=60000.0,
        min_increment=1000.0,
        duration_seconds=600
    )

    # Fund both bidders with $50,000 each
    await lua_engine.set_user_escrow(user_id=user_a, total_balance=50000.0, locked_collateral=0.0)
    await lua_engine.set_user_escrow(user_id=user_b, total_balance=50000.0, locked_collateral=0.0)

    # 1. Bidder A places bid -> $5,000 collateral locked on Bidder A
    res_a = await lua_engine.execute_bid(
        auction_id=auc_id,
        bidder_id=user_a,
        bidder_name="BidderA",
        bid_amount=51000.0,
        required_escrow=5000.0
    )
    assert res_a["ok"] is True

    locked_a = float(await client.hget(f"escrow:{user_a}", "locked_collateral"))
    assert locked_a == 5000.0

    # 2. Bidder B outbids Bidder A -> Bidder A's collateral released, Bidder B's collateral locked!
    res_b = await lua_engine.execute_bid(
        auction_id=auc_id,
        bidder_id=user_b,
        bidder_name="BidderB",
        bid_amount=52000.0,
        required_escrow=5000.0
    )
    assert res_b["ok"] is True

    # Check Bidder A collateral is back to 0.0
    locked_a_after = float(await client.hget(f"escrow:{user_a}", "locked_collateral"))
    assert locked_a_after == 0.0

    # Check Bidder B collateral is now 5000.0
    locked_b = float(await client.hget(f"escrow:{user_b}", "locked_collateral"))
    assert locked_b == 5000.0
