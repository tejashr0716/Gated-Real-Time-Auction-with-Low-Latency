import asyncio
import pytest
import uuid
import httpx
from fastapi.testclient import TestClient
from app.main import app
from app.services.redis_client import redis_service
from app.services.database import db_service
from app.services.lua_engine import lua_engine
from worker import WriteBehindSyncWorker

@pytest.mark.asyncio
async def test_escrow_contention_releases_all_outbid_collateral():
    """
    Regression Test for Audit Issue #3:
    In a high-contention burst of bids across distinct users,
    ensure 100% of outbid accounts have their collateral unlocked,
    and only the final winning bidder has collateral locked.
    """
    client = redis_service.get_client()
    aid = f"test_contention_{uuid.uuid4().hex[:8]}"
    await lua_engine.reset_auction(aid, 1000.0, 5000.0, 10.0, 3600)

    num_bidders = 20
    bidders = [str(uuid.uuid4()) for _ in range(num_bidders)]
    for uid in bidders:
        await lua_engine.set_user_escrow(uid, 50000.0, 0.0)

    # Dispatch concurrent bids
    tasks = [
        lua_engine.execute_bid(
            auction_id=aid,
            bidder_id=uid,
            bidder_name=f"Trader_{i}",
            bid_amount=1010.0 + (i * 15.0),
            required_escrow=5000.0
        )
        for i, uid in enumerate(bidders)
    ]
    results = await asyncio.gather(*tasks)

    winner = await client.hget(f"auction:{aid}:meta", "highest_bidder_id")
    assert winner is not None, "Winning bidder must be recorded in Redis"

    # Verify that every single outbid user has ZERO locked collateral
    stale_locks = []
    for uid in bidders:
        locked = float(await client.hget(f"escrow:{uid}", "locked_collateral") or 0.0)
        if uid == winner:
            assert locked == 5000.0, f"Winning bidder {uid} must have exactly $5,000 locked, got {locked}"
        else:
            if locked > 0:
                stale_locks.append((uid, locked))

    assert len(stale_locks) == 0, f"Outbid accounts remained locked after contention: {stale_locks}"

    # Cleanup
    await client.delete(f"auction:{aid}:meta", f"auction_bids:{aid}", f"auction_stream:{aid}", *[f"escrow:{uid}" for uid in bidders])


@pytest.mark.asyncio
async def test_multi_auction_escrow_isolation():
    """
    Verifies that releasing an outbid hold in Auction A does not
    inadvertently release or corrupt active collateral locked in Auction B.
    """
    client = redis_service.get_client()
    auc_a = f"auc_a_{uuid.uuid4().hex[:8]}"
    auc_b = f"auc_b_{uuid.uuid4().hex[:8]}"
    user1 = str(uuid.uuid4())
    user2 = str(uuid.uuid4())

    await lua_engine.reset_auction(auc_a, 1000.0, 5000.0, 10.0, 3600)
    await lua_engine.reset_auction(auc_b, 2000.0, 6000.0, 10.0, 3600)

    # User 1 has 100k balance
    await lua_engine.set_user_escrow(user1, 100000.0, 0.0)
    await lua_engine.set_user_escrow(user2, 100000.0, 0.0)

    # User 1 bids on Auc A (locks 5000) and Auc B (locks 5000) -> total locked 10000
    res1 = await lua_engine.execute_bid(auc_a, user1, "User1", 1020.0, 5000.0)
    assert res1.get("ok") is True
    res2 = await lua_engine.execute_bid(auc_b, user1, "User1", 2020.0, 5000.0)
    assert res2.get("ok") is True

    locked_initial = float(await client.hget(f"escrow:{user1}", "locked_collateral"))
    assert locked_initial == 10000.0, f"User 1 should have 10,000 locked across two auctions, got {locked_initial}"

    # User 2 outbids User 1 in Auction A only
    res3 = await lua_engine.execute_bid(auc_a, user2, "User2", 1050.0, 5000.0)
    assert res3.get("ok") is True

    # User 1's lock in Auction A should be released, but lock in Auction B must REMAIN!
    locked_after = float(await client.hget(f"escrow:{user1}", "locked_collateral"))
    assert locked_after == 5000.0, f"User 1 should still have 5,000 locked in Auction B, got {locked_after}"

    # Cleanup
    await client.delete(
        f"auction:{auc_a}:meta", f"auction:{auc_b}:meta",
        f"auction_bids:{auc_a}", f"auction_bids:{auc_b}",
        f"escrow:{user1}", f"escrow:{user2}"
    )


@pytest.mark.asyncio
async def test_worker_retains_stream_message_on_db_failure():
    """
    Regression Test for Audit Issue #4:
    If database persistence raises an error, the sync worker MUST NOT
    delete or acknowledge the stream message; it must retain it for retry.
    """
    client = redis_service.get_client()
    aid = "auc_shelby_gt500"
    stream_key = f"auction_stream:{aid}"
    bidder_id = "a0000001-0000-0000-0000-000000000001"
    test_seq = 88888

    # Clean any stale stream for test isolation
    await client.delete(stream_key)

    # Push 1 message to stream
    msg_id = await client.xadd(
        stream_key,
        {
            "auction_id": aid,
            "bidder_id": bidder_id,
            "bidder_name": "AuditTester",
            "sequence_id": str(test_seq),
            "amount": "999.0"
        }
    )

    initial_len = await client.xlen(stream_key)
    assert initial_len == 1

    # Inject database failure
    original_execute = db_service.execute
    async def forced_failure(*args, **kwargs):
        raise RuntimeError("AUDIT_INJECTED_DATABASE_FAILURE")

    db_service.execute = forced_failure
    try:
        worker = WriteBehindSyncWorker()
        await worker.process_streams()
    finally:
        db_service.execute = original_execute

    remaining_len = await client.xlen(stream_key)
    assert remaining_len == 1, (
        f"Message was dropped! Stream length should be 1 after failed DB execution, but got {remaining_len}"
    )

    # Now run worker normally without failure; message should persist and be removed
    worker = WriteBehindSyncWorker()
    await worker.process_streams()
    clean_len = await client.xlen(stream_key)
    assert clean_len == 0, f"Message should be processed and trimmed on success, got {clean_len}"

    # Cleanup DB row and stream
    await db_service.execute("DELETE FROM bids WHERE auction_id = $1 AND sequence_id = $2", aid, test_seq)
    await client.delete(stream_key)


@pytest.mark.asyncio
async def test_bid_endpoint_rejects_unauthenticated_request():
    """
    Regression Test for Audit Issue #2:
    POST /api/auctions/{id}/bid without credentials must return 401 Unauthorized.
    """
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        resp = await ac.post(
            "/api/auctions/auc_shelby_gt500/bid",
            json={
                "auction_id": "auc_shelby_gt500",
                "bidder_id": "a0000001-0000-0000-0000-000000000001",
                "bidder_name": "SpoofedBidder",
                "bid_amount": 150000.0
            }
        )
        assert resp.status_code == 401, f"Expected HTTP 401 Unauthorized, got {resp.status_code}"


@pytest.mark.asyncio
async def test_bid_endpoint_accepts_valid_authenticated_user_and_derives_identity():
    """
    Verifies that a request with valid Authorization Bearer token is accepted,
    and the bidder identity is derived from verified token claims, NOT the body.
    """
    from app.services.auth_security import create_access_token
    real_uid = "a0000001-0000-0000-0000-000000000001"
    token = create_access_token(real_uid, "ApexWhale")

    # Reset auction and escrow
    await lua_engine.reset_auction("auc_shelby_gt500", 100000.0, 200000.0, 25.0, 3600)
    await lua_engine.set_user_escrow(real_uid, 500000.0, 0.0)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        # Attempt to spoof bidder_id as another user in body
        spoofed_uid = "b0000002-0000-0000-0000-000000000002"
        resp = await ac.post(
            "/api/auctions/auc_shelby_gt500/bid",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "auction_id": "auc_shelby_gt500",
                "bidder_id": spoofed_uid,
                "bidder_name": "SpoofedAttacker",
                "bid_amount": 105000.0
            }
        )
        assert resp.status_code == 200, f"Expected 200 OK with valid token, got {resp.status_code}: {resp.text}"
        body = resp.json()
        assert body["ok"] is True
        # MUST match token identity, NOT the spoofed body
        assert body["bidder_id"] == real_uid, f"Identity was not derived from token! Got {body['bidder_id']}"
        assert body["bidder_name"] == "ApexWhale"


@pytest.mark.asyncio
async def test_websocket_bid_authentication_enforcement():
    """
    Verifies WebSocket bid action:
    1. Unauthenticated bid receives BID_REJECTED with ERR_UNAUTHENTICATED.
    2. Authenticated bid with valid token succeeds and derives identity from token.
    """
    import json
    from unittest.mock import AsyncMock
    from fastapi import WebSocketDisconnect
    from app.routers.websocket import websocket_auction_endpoint
    from app.services.auth_security import create_access_token

    auc_id = "auc_shelby_gt500"
    real_uid = "a0000001-0000-0000-0000-000000000001"
    token = create_access_token(real_uid, "ApexWhale")

    await lua_engine.reset_auction(auc_id, 100000.0, 200000.0, 25.0, 3600)
    await lua_engine.set_user_escrow(real_uid, 500000.0, 0.0)

    # 1. Test unauthenticated rejection
    mock_ws = AsyncMock()
    mock_ws.accept = AsyncMock()
    mock_ws.send_text = AsyncMock()

    # Yield unauthenticated bid message, then disconnect
    mock_ws.receive_text = AsyncMock(side_effect=[
        json.dumps({"action": "BID", "amount": 105000.0}),
        WebSocketDisconnect(1000)
    ])

    await websocket_auction_endpoint(mock_ws, auc_id)

    # Handshake is call 0, rejection is call 1
    rejection_call = json.loads(mock_ws.send_text.call_args_list[1][0][0])
    assert rejection_call["event"] == "BID_REJECTED"
    assert rejection_call["code"] == "ERR_UNAUTHENTICATED"

    # 2. Test authenticated acceptance with token
    mock_ws2 = AsyncMock()
    mock_ws2.accept = AsyncMock()
    mock_ws2.send_text = AsyncMock()
    mock_ws2.receive_text = AsyncMock(side_effect=[
        json.dumps({
            "action": "BID",
            "token": token,
            "amount": 105000.0,
            "bidder_id": "spoofed_id",
            "bidder_name": "SpoofedName"
        }),
        WebSocketDisconnect(1000)
    ])

    await websocket_auction_endpoint(mock_ws2, auc_id)

    receipt_call = json.loads(mock_ws2.send_text.call_args_list[1][0][0])
    assert receipt_call["event"] == "BID_RECEIPT"
    assert receipt_call["result"]["ok"] is True
    # Identity must be derived from token!
    assert receipt_call["result"]["data"]["bidder_id"] == real_uid
    assert receipt_call["result"]["data"]["bidder_name"] == "ApexWhale"

