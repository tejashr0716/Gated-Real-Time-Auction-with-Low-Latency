import pytest
import time
import uuid
from worker import WriteBehindSyncWorker
from app.services.redis_client import redis_service
from app.services.database import db_service

@pytest.mark.asyncio
async def test_worker_persists_stream_to_postgres():
    worker = WriteBehindSyncWorker()
    client = redis_service.get_client()

    auc_id = "auc_shelby_gt500"
    bidder_id = "a0000001-0000-0000-0000-000000000001"
    bidder_name = "apex_collector"
    test_seq = 99999
    test_amount = 188888.0
    now_ms = int(time.time() * 1000)

    # Push to stream
    await client.xadd(
        f"auction_stream:{auc_id}",
        {
            "auction_id": auc_id,
            "bidder_id": bidder_id,
            "bidder_name": bidder_name,
            "sequence_id": str(test_seq),
            "amount": str(test_amount),
            "timestamp": str(now_ms),
            "extended": "false"
        }
    )

    # Run one pass of worker
    await worker.process_streams()

    # Verify bid exists in PostgreSQL
    row = await db_service.fetchrow(
        "SELECT * FROM bids WHERE auction_id = $1 AND sequence_id = $2",
        auc_id, test_seq
    )
    assert row is not None
    assert float(row["amount"]) == test_amount
    assert row["bidder_name"] == bidder_name

    # Clean up test row
    await db_service.execute(
        "DELETE FROM bids WHERE auction_id = $1 AND sequence_id = $2",
        auc_id, test_seq
    )
