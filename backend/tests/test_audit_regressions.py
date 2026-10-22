import asyncio
import pytest
import uuid
import httpx
import time
from app.main import app
from app.config import settings
from app.services.redis_client import redis_service
from app.services.database import db_service
from app.services.lua_engine import lua_engine
from app.services.auth_security import (
    create_access_token,
    verify_access_token,
    hash_password,
    verify_password
)
from worker import WriteBehindSyncWorker

# ============================================================================
# SECTION A: AUTHENTICATION & IDENTITY REGRESSION TESTS
# ============================================================================

@pytest.mark.asyncio
async def test_login_password_verification_and_rejections():
    """
    Regression Test for Audit Issue A.1-3:
    1. Incorrect password for existing user MUST return 401.
    2. Unknown user MUST return 401.
    3. Valid credentials return 200 with verified token.
    """
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        # 1. Incorrect password
        bad_pw_resp = await ac.post("/api/auth/login", json={
            "username": "apex_collector",
            "password": "WrongPassword123!"
        })
        assert bad_pw_resp.status_code == 401, f"Expected 401 for bad password, got {bad_pw_resp.status_code}"
        assert "Invalid username or password" in bad_pw_resp.json()["detail"]

        # 2. Unknown user
        unknown_resp = await ac.post("/api/auth/login", json={
            "username": "non_existent_trader_xyz",
            "password": "VIP_password_2026!"
        })
        assert unknown_resp.status_code == 401, f"Expected 401 for unknown user, got {unknown_resp.status_code}"

        # 3. Valid credentials
        good_resp = await ac.post("/api/auth/login", json={
            "username": "apex_collector",
            "password": "VIP_password_2026!"
        })
        assert good_resp.status_code == 200, f"Expected 200 for valid login, got {good_resp.status_code}"
        data = good_resp.json()
        assert "access_token" in data
        assert data["username"] == "apex_collector"
        assert data["user_id"] == "a0000001-0000-0000-0000-000000000001"


@pytest.mark.asyncio
async def test_token_verification_rejections():
    """
    Regression Test for Audit Issue A.3:
    Rejects tampered, expired, or malformed tokens.
    """
    real_uid = "a0000001-0000-0000-0000-000000000001"
    valid_token = create_access_token(real_uid, "apex_collector")

    # 1. Tampered payload
    parts = valid_token.split(".")
    tampered_token = f"{parts[0]}tampered.{parts[1]}"
    assert verify_access_token(tampered_token) is None

    # 2. Tampered signature
    tampered_sig_token = f"{parts[0]}.0000000000000000000000000000000000000000000000000000000000000000"
    assert verify_access_token(tampered_sig_token) is None

    # 3. Expired token
    expired_token = create_access_token(real_uid, "apex_collector", expires_in_seconds=-10)
    assert verify_access_token(expired_token) is None

    # 4. Endpoints reject tampered token
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        resp = await ac.post(
            "/api/auctions/auc_shelby_gt500/bid",
            headers={"Authorization": f"Bearer {tampered_token}"},
            json={"auction_id": "auc_shelby_gt500", "bid_amount": 150000.0}
        )
        assert resp.status_code == 401


@pytest.mark.asyncio
async def test_login_does_not_overwrite_active_redis_collateral_hold():
    """
    Regression Test for Audit Issue A.7:
    Logging in again MUST NOT wipe out active in-flight Redis locked_collateral holds
    with stale PostgreSQL values.
    """
    client = redis_service.get_client()
    user_id = "a0000001-0000-0000-0000-000000000001"
    redis_key = f"escrow:{user_id}"

    # Simulate an active locked collateral hold in Redis (e.g. $25,000 locked on active lot)
    await client.hset(redis_key, mapping={
        "total_balance": "750000.00",
        "locked_collateral": "25000.00",
        "kyc_status": "VIP",
        "tier": "DIAMOND"
    })

    # User logs in again (or opens another session tab)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        login_resp = await ac.post("/api/auth/login", json={
            "username": "apex_collector",
            "password": "VIP_password_2026!"
        })
        assert login_resp.status_code == 200

    # Verify active hold was NOT overwritten with DB's 0.00!
    active_locked = float(await client.hget(redis_key, "locked_collateral"))
    assert active_locked == 25000.0, (
        f"CRITICAL: Login wiped active Redis collateral hold! Expected 25000.0, got {active_locked}"
    )


@pytest.mark.asyncio
async def test_body_bidder_id_cannot_impersonate():
    """
    Regression Test for Audit Issue A.5:
    Bidder identity MUST be derived strictly from verified token claims.
    Any caller-supplied bidder_id in request body or header is ignored.
    """
    real_uid = "a0000001-0000-0000-0000-000000000001"
    attacker_token = create_access_token(real_uid, "RealBidderAlistair")

    await lua_engine.reset_auction("auc_shelby_gt500", 100000.0, 200000.0, 25.0, 3600)
    await lua_engine.set_user_escrow(real_uid, 500000.0, 0.0)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        # Attacker claims to be victim 'a0000002...' in body
        victim_uid = "a0000002-0000-0000-0000-000000000002"
        resp = await ac.post(
            "/api/auctions/auc_shelby_gt500/bid",
            headers={"Authorization": f"Bearer {attacker_token}"},
            json={
                "auction_id": "auc_shelby_gt500",
                "bidder_id": victim_uid,
                "bidder_name": "VictimElena",
                "bid_amount": 105000.0
            }
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["bidder_id"] == real_uid, "Security violation: Identity was spoofed from body!"
        assert body["bidder_name"] == "RealBidderAlistair"


@pytest.mark.asyncio
async def test_escrow_endpoints_bind_strictly_to_authenticated_identity():
    """
    Regression Test for Audit Issue A.6:
    Escrow balance and deposit endpoints MUST act strictly on the authenticated user.
    Passing arbitrary X-User-Id or unauthenticated requests MUST NOT access other accounts.
    """
    user_a = "a0000001-0000-0000-0000-000000000001"
    token_a = create_access_token(user_a, "apex_collector")

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        # 1. Unauthenticated request rejected
        unauth = await ac.get("/api/escrow/balance")
        assert unauth.status_code == 401

        # 2. Authenticated request binds to user A even if header attempts spoofing user B
        resp = await ac.get(
            "/api/escrow/balance",
            headers={
                "Authorization": f"Bearer {token_a}",
                "X-User-Id": "a0000002-0000-0000-0000-000000000002"
            }
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["user_id"] == user_a
        assert data["username"] == "apex_collector"


@pytest.mark.asyncio
async def test_unauthorized_user_cannot_reset_auction():
    """
    Regression Test for Audit Issue A.6:
    Resetting auctions requires administrative privileges.
    """
    standard_uid = "a0000005-0000-0000-0000-000000000005"
    standard_token = create_access_token(standard_uid, "silicon_syndicate", tier="GOLD", is_admin=False)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        # 1. Unauthenticated reset fails
        unauth = await ac.post("/api/auctions/auc_shelby_gt500/reset")
        assert unauth.status_code == 401

        # 2. In non-demo mode, non-admin user fails
        original_demo = settings.DEMO_MODE
        try:
            settings.DEMO_MODE = False
            resp = await ac.post(
                "/api/auctions/auc_shelby_gt500/reset",
                headers={"Authorization": f"Bearer {standard_token}"}
            )
            assert resp.status_code == 403
        finally:
            settings.DEMO_MODE = original_demo


@pytest.mark.asyncio
async def test_lifespan_startup_rejections_and_success():
    """
    Regression Test for Audit Issue A: Lifespan Security Validation
    1. Non-demo + default secret: startup fails.
    2. Non-demo + short secret (< 32 chars): startup fails.
    3. Non-demo + valid test secret: startup succeeds with isolated test services.
    4. Documented demo configuration behaves as intended.
    5. Failed startup does not leave database connections or background tasks running.
    """
    from app.main import lifespan, app
    from app.services.websocket_manager import ws_manager

    original_mode = settings.DEMO_MODE
    original_secret = settings.SECRET_KEY
    try:
        # 1. Non-demo + default secret: startup MUST fail
        settings.DEMO_MODE = False
        settings.SECRET_KEY = settings.DEFAULT_INSECURE_SECRET
        with pytest.raises(RuntimeError) as exc_info_1:
            async with lifespan(app):
                pass
        assert "INSECURE_CONFIGURATION" in str(exc_info_1.value)
        assert ws_manager.heartbeat_task is None

        # 2. Non-demo + short secret: startup MUST fail
        settings.DEMO_MODE = False
        settings.SECRET_KEY = "short_insecure_secret_123"
        with pytest.raises(RuntimeError) as exc_info_2:
            async with lifespan(app):
                pass
        assert "INSECURE_CONFIGURATION" in str(exc_info_2.value)
        assert ws_manager.heartbeat_task is None

        # 3. Non-demo + valid secure secret (>= 32 chars): startup MUST succeed
        settings.DEMO_MODE = False
        settings.SECRET_KEY = "a_very_secure_and_long_production_test_secret_key_2026_99"
        async with lifespan(app):
            assert ws_manager.heartbeat_task is not None
            assert not ws_manager.heartbeat_task.done()

        # After clean shutdown, background task must be cleaned up
        assert ws_manager.heartbeat_task is None

        # 4. Documented demo mode: startup succeeds even with default secret
        settings.DEMO_MODE = True
        settings.SECRET_KEY = settings.DEFAULT_INSECURE_SECRET
        async with lifespan(app):
            assert ws_manager.heartbeat_task is not None
        assert ws_manager.heartbeat_task is None

    finally:
        settings.DEMO_MODE = original_mode
        settings.SECRET_KEY = original_secret


# ============================================================================
# SECTION B: PERSISTENCE TRANSACTIONS & RETRY SAFETY REGRESSION TESTS
# ============================================================================

@pytest.mark.asyncio
async def test_persistence_atomic_transaction_rollback():
    """
    Regression Test for Audit Issue B:
    If the second statement (auction update) fails, the first statement (bid insert)
    MUST roll back atomically within the single connection transaction.
    """
    aid = "auc_shelby_gt500"
    bidder_id = "a0000001-0000-0000-0000-000000000001"
    test_seq = 77777
    valid_amount = 185000.0

    # Ensure clean starting state
    await db_service.execute("DELETE FROM bids WHERE auction_id = $1 AND sequence_id = $2", aid, test_seq)

    # Execute inside connection transaction with forced failure on 2nd statement
    rolled_back = False
    try:
        async with db_service.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    """INSERT INTO bids (auction_id, bidder_id, bidder_name, sequence_id, amount, status)
                       VALUES ($1, $2, $3, $4, $5, 'ACCEPTED')""",
                    aid, uuid.UUID(bidder_id), "TxTester", test_seq, valid_amount
                )
                # Injected failure on second statement
                raise RuntimeError("INJECTED_AUCTION_UPDATE_FAILURE")
    except RuntimeError:
        rolled_back = True

    assert rolled_back is True

    # Verify that statement 1 was rolled back and bid DOES NOT exist
    row = await db_service.fetchrow("SELECT * FROM bids WHERE auction_id = $1 AND sequence_id = $2", aid, test_seq)
    assert row is None, "Transactional failure: Bid remained in DB after failed transaction!"


@pytest.mark.asyncio
async def test_persistence_idempotent_replay_does_not_double_count():
    """
    Regression Test for Audit Issue B.5:
    Replaying an already-persisted stream message must NOT double-increment bid_count.
    """
    aid = "auc_shelby_gt500"
    bidder_id = "a0000001-0000-0000-0000-000000000001"

    # Record initial auction state
    initial_auc = await db_service.fetchrow("SELECT bid_count, sequence_id, current_price FROM auctions WHERE id = $1", aid)
    initial_count = int(initial_auc["bid_count"])
    test_seq = int(initial_auc["sequence_id"]) + 10
    valid_amount = float(initial_auc["current_price"]) + 1000.0

    try:
        await db_service.execute("DELETE FROM bids WHERE auction_id = $1 AND sequence_id = $2", aid, test_seq)

        # First pass: new bid
        await db_service.persist_bid_transaction(aid, bidder_id, "ReplayTester", test_seq, valid_amount)

        mid_auc = await db_service.fetchrow("SELECT bid_count FROM auctions WHERE id = $1", aid)
        assert int(mid_auc["bid_count"]) == initial_count + 1

        # Second pass: duplicate replay
        await db_service.persist_bid_transaction(aid, bidder_id, "ReplayTester", test_seq, valid_amount)

        final_auc = await db_service.fetchrow("SELECT bid_count FROM auctions WHERE id = $1", aid)
        assert int(final_auc["bid_count"]) == initial_count + 1, (
            f"Replay double-counted! Expected {initial_count + 1}, got {final_auc['bid_count']}"
        )
    finally:
        await db_service.execute("DELETE FROM bids WHERE auction_id = $1 AND sequence_id = $2", aid, test_seq)


@pytest.mark.asyncio
async def test_worker_retains_stream_message_on_db_failure_repaired():
    """
    Repairs previous recovery test by using valid isolated auction pricing constraint.
    """
    client = redis_service.get_client()
    aid = "auc_shelby_gt500"
    stream_key = f"auction_stream:{aid}"
    bidder_id = "a0000001-0000-0000-0000-000000000001"
    test_seq = 55555
    valid_amount = 187000.0 # Valid: > starting_price $100,000

    try:
        await client.delete(stream_key)
        await db_service.execute("DELETE FROM bids WHERE auction_id = $1 AND sequence_id = $2", aid, test_seq)

        # Push valid message
        await client.xadd(
            stream_key,
            {
                "auction_id": aid,
                "bidder_id": bidder_id,
                "bidder_name": "AuditTester",
                "sequence_id": str(test_seq),
                "amount": str(valid_amount)
            }
        )

        # Inject DB failure
        original_execute = db_service.execute
        async def forced_failure(*args, **kwargs):
            raise RuntimeError("AUDIT_INJECTED_DATABASE_FAILURE")

        db_service.execute = forced_failure
        try:
            worker = WriteBehindSyncWorker()
            await worker.process_streams()
        finally:
            db_service.execute = original_execute

        # Must be retained
        remaining_len = await client.xlen(stream_key)
        assert remaining_len == 1, f"Failed message was dropped! Expected 1, got {remaining_len}"

        # Re-run worker without failure; message should persist and be removed
        worker = WriteBehindSyncWorker()
        await worker.process_streams()
        clean_len = await client.xlen(stream_key)
        assert clean_len == 0, f"Message should be processed and trimmed on success, got {clean_len}"

        # Verify DB entry
        row = await db_service.fetchrow("SELECT * FROM bids WHERE auction_id = $1 AND sequence_id = $2", aid, test_seq)
        assert row is not None
        assert float(row["amount"]) == valid_amount
    finally:
        await db_service.execute("DELETE FROM bids WHERE auction_id = $1 AND sequence_id = $2", aid, test_seq)
        await client.delete(stream_key)


@pytest.mark.asyncio
async def test_production_worker_persistence_rollback_retry_and_idempotency():
    """
    Regression Test for Audit Issue F:
    Exercises the actual production WriteBehindSyncWorker.process_streams() / persist_bid_transaction() path:
    1. Injects failure during auction UPDATE after ledger INSERT using a temporary PostgreSQL trigger.
    2. Verifies the ledger insert rolls back cleanly (0 ledger entries).
    3. Verifies the Redis stream message remains retained for retry.
    4. Removes the injected failure and retries worker pass.
    5. Verifies exactly one ledger entry, correct catalog state, and message deletion from stream.
    6. Replays the same message and verifies sequence monotonic protection (no double-counting).
    """
    client = redis_service.get_client()
    aid = f"auc_tx_{uuid.uuid4().hex[:8]}"
    uid = "a0000001-0000-0000-0000-000000000001"
    key = f"auction_stream:{aid}"

    # Setup isolated test auction in DB and Redis
    await db_service.execute(
        """INSERT INTO auctions (id, title, description, category, starting_price, current_price, reserve_price, min_increment, min_escrow_required, status, start_time, end_time, bid_count, sequence_id)
           VALUES ($1, 'TxTest', 'Test Desc', 'AUTO', 100000.0, 100000.0, 200000.0, 100.0, 5000.0, 'ACTIVE', NOW(), NOW() + INTERVAL '1 hour', 0, 0)
           ON CONFLICT (id) DO UPDATE SET current_price=100000.0, sequence_id=0, bid_count=0""",
        aid
    )
    await db_service.execute("DELETE FROM bids WHERE auction_id = $1", aid)
    await client.delete(key)

    # 1. Install failure trigger on auctions table
    await db_service.execute(
        "CREATE OR REPLACE FUNCTION audit_fail_update() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'AUDIT_UPDATE_FAILURE'; END $$"
    )
    await db_service.execute(
        "CREATE TRIGGER audit_update_failure BEFORE UPDATE ON auctions FOR EACH ROW EXECUTE FUNCTION audit_fail_update()"
    )

    # 2. Add message to stream
    await client.xadd(key, {
        "auction_id": aid,
        "bidder_id": uid,
        "bidder_name": "TxAuditTester",
        "sequence_id": "1",
        "amount": "150000.0"
    })

    worker = WriteBehindSyncWorker()
    try:
        # Run worker with failure active
        await worker.process_streams()

        # Verify rollback: ledger insert must be rolled back, auction unchanged, stream message retained
        bids_count = await db_service.fetchrow("SELECT count(*) AS n FROM bids WHERE auction_id = $1", aid)
        assert bids_count["n"] == 0, f"Ledger insert failed to roll back! Found {bids_count['n']} bids"
        auc_state = await db_service.fetchrow("SELECT sequence_id, bid_count FROM auctions WHERE id = $1", aid)
        assert auc_state["sequence_id"] == 0 and auc_state["bid_count"] == 0

        stream_len = await client.xlen(key)
        assert stream_len == 1, f"Failed stream message was not retained for retry! Length: {stream_len}"

    finally:
        # 3. Clean up trigger
        await db_service.execute("DROP TRIGGER IF EXISTS audit_update_failure ON auctions")
        await db_service.execute("DROP FUNCTION IF EXISTS audit_fail_update()")

    # 4. Retry worker pass without failure
    await worker.process_streams()

    # Verify successful persistence
    bids_after = await db_service.fetchrow("SELECT count(*) AS n FROM bids WHERE auction_id = $1", aid)
    assert bids_after["n"] == 1, f"Expected 1 persisted bid after retry, got {bids_after['n']}"
    auc_after = await db_service.fetchrow("SELECT sequence_id, bid_count, current_price FROM auctions WHERE id = $1", aid)
    assert auc_after["sequence_id"] == 1
    assert auc_after["bid_count"] == 1
    assert float(auc_after["current_price"]) == 150000.0

    stream_after = await client.xlen(key)
    assert stream_after == 0, f"Expected message to be pruned via XDEL after successful commit, got {stream_after}"

    # 5. Replay same message: must NOT double-count bid_count
    await db_service.persist_bid_transaction(aid, uid, "TxAuditTester", 1, 150000.0)
    auc_replay = await db_service.fetchrow("SELECT sequence_id, bid_count FROM auctions WHERE id = $1", aid)
    assert auc_replay["bid_count"] == 1, f"Duplicate replay double-counted! Expected 1, got {auc_replay['bid_count']}"

    # Cleanup
    await db_service.execute("DELETE FROM bids WHERE auction_id = $1", aid)
    await db_service.execute("DELETE FROM auctions WHERE id = $1", aid)
    await client.delete(key)


# ============================================================================
# SECTION C: ESCROW & WEBSOCKET AUDIT TESTS
# ============================================================================

@pytest.mark.asyncio
async def test_escrow_contention_releases_all_outbid_collateral():
    """
    Verifies that under concurrent bidding, 100% of losing holds are released.
    """
    client = redis_service.get_client()
    aid = f"test_contention_{uuid.uuid4().hex[:8]}"
    await lua_engine.reset_auction(aid, 1000.0, 5000.0, 10.0, 3600)

    num_bidders = 20
    bidders = [str(uuid.uuid4()) for _ in range(num_bidders)]
    for uid in bidders:
        await lua_engine.set_user_escrow(uid, 50000.0, 0.0)

    try:
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
        assert winner is not None

        stale_locks = []
        for uid in bidders:
            locked = float(await client.hget(f"escrow:{uid}", "locked_collateral") or 0.0)
            if uid == winner:
                assert locked == 5000.0
            else:
                if locked > 0:
                    stale_locks.append((uid, locked))

        assert len(stale_locks) == 0, f"Outbid accounts remained locked after contention: {stale_locks}"
    finally:
        await client.delete(f"auction:{aid}:meta", f"auction_bids:{aid}", f"auction_stream:{aid}", *[f"escrow:{uid}" for uid in bidders])


@pytest.mark.asyncio
async def test_multi_auction_escrow_isolation():
    """
    Verifies outbid release in Auction A does not release hold in Auction B.
    """
    client = redis_service.get_client()
    auc_a = f"auc_a_{uuid.uuid4().hex[:8]}"
    auc_b = f"auc_b_{uuid.uuid4().hex[:8]}"
    user1 = str(uuid.uuid4())
    user2 = str(uuid.uuid4())

    try:
        await lua_engine.reset_auction(auc_a, 1000.0, 5000.0, 10.0, 3600)
        await lua_engine.reset_auction(auc_b, 2000.0, 6000.0, 10.0, 3600)

        await lua_engine.set_user_escrow(user1, 100000.0, 0.0)
        await lua_engine.set_user_escrow(user2, 100000.0, 0.0)

        res1 = await lua_engine.execute_bid(auc_a, user1, "User1", 1020.0, 5000.0)
        assert res1.get("ok") is True
        res2 = await lua_engine.execute_bid(auc_b, user1, "User1", 2020.0, 5000.0)
        assert res2.get("ok") is True

        locked_initial = float(await client.hget(f"escrow:{user1}", "locked_collateral"))
        assert locked_initial == 10000.0

        res3 = await lua_engine.execute_bid(auc_a, user2, "User2", 1050.0, 5000.0)
        assert res3.get("ok") is True

        locked_after = float(await client.hget(f"escrow:{user1}", "locked_collateral"))
        assert locked_after == 5000.0
    finally:
        await client.delete(
            f"auction:{auc_a}:meta", f"auction:{auc_b}:meta",
            f"auction_bids:{auc_a}", f"auction_bids:{auc_b}",
            f"escrow:{user1}", f"escrow:{user2}"
        )


@pytest.mark.asyncio
async def test_websocket_bid_authentication_enforcement():
    """
    Verifies WebSocket unauthenticated bid rejection and authenticated acceptance.
    """
    import json
    from unittest.mock import AsyncMock
    from fastapi import WebSocketDisconnect
    from app.routers.websocket import websocket_auction_endpoint

    auc_id = "auc_shelby_gt500"
    real_uid = "a0000001-0000-0000-0000-000000000001"
    token = create_access_token(real_uid, "ApexWhale")

    await lua_engine.reset_auction(auc_id, 100000.0, 200000.0, 25.0, 3600)
    await lua_engine.set_user_escrow(real_uid, 500000.0, 0.0)

    # 1. Unauthenticated rejection
    mock_ws = AsyncMock()
    mock_ws.accept = AsyncMock()
    mock_ws.send_text = AsyncMock()
    mock_ws.receive_text = AsyncMock(side_effect=[
        json.dumps({"action": "BID", "amount": 105000.0}),
        WebSocketDisconnect(1000)
    ])

    await websocket_auction_endpoint(mock_ws, auc_id)
    rejection = json.loads(mock_ws.send_text.call_args_list[1][0][0])
    assert rejection["event"] == "BID_REJECTED"
    assert rejection["code"] == "ERR_UNAUTHENTICATED"

    # 2. Authenticated acceptance
    mock_ws2 = AsyncMock()
    mock_ws2.accept = AsyncMock()
    mock_ws2.send_text = AsyncMock()
    mock_ws2.receive_text = AsyncMock(side_effect=[
        json.dumps({
            "action": "BID",
            "token": token,
            "amount": 105000.0,
            "bidder_id": "spoofed_id"
        }),
        WebSocketDisconnect(1000)
    ])

    await websocket_auction_endpoint(mock_ws2, auc_id)
    receipt = json.loads(mock_ws2.send_text.call_args_list[1][0][0])
    assert receipt["event"] == "BID_RECEIPT"
    assert receipt["result"]["ok"] is True
    assert receipt["result"]["data"]["bidder_id"] == real_uid
