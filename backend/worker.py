import asyncio
import logging
import signal
import sys
import uuid
from typing import Dict, Any
from app.config import settings
from app.services.redis_client import redis_service
from app.services.database import db_service
from app.services.telemetry_metrics import telemetry_metrics

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [SYNC_WORKER] %(message)s"
)
logger = logging.getLogger("sync_worker")

class WriteBehindSyncWorker:
    def __init__(self):
        self.running = False
        self.batch_size = 50
        self.poll_interval = 0.25 # 250ms batching window

    async def start(self):
        self.running = True
        await redis_service.connect()
        await db_service.connect()
        logger.info("Connected to Redis and PostgreSQL. Starting write-behind sync loop...")

        # Find all active auctions to track
        while self.running:
            try:
                await self.process_streams()
                await self.record_telemetry_snapshot()
                await asyncio.sleep(self.poll_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in sync loop: {e}", exc_info=True)
                await asyncio.sleep(1.0)

    async def process_streams(self):
        client = redis_service.get_client()

        # Discover all active auction stream keys
        stream_keys = await client.keys("auction_stream:*")
        if not stream_keys:
            return

        # Prepare XREAD arguments
        streams_dict = {key: "0" for key in stream_keys}
        try:
            entries = await client.xread(streams_dict, count=self.batch_size, block=100)
            if not entries:
                return

            for stream_name, messages in entries:
                auction_id = stream_name.split("auction_stream:")[-1]
                for msg_id, fields in messages:
                    try:
                        await self._persist_bid(auction_id, fields)
                        # Trim stream message ONLY upon verified, transactional database persistence
                        await client.xdel(stream_name, msg_id)
                    except Exception as e:
                        logger.error(f"Persistence error for {stream_name} msg {msg_id}: {e}. Retaining message in stream for retry.")
                        # Break out of this stream to preserve strict FIFO sequence ordering
                        break

        except Exception as e:
            logger.error(f"Error reading stream: {e}")

    async def _persist_bid(self, auction_id: str, fields: Dict[str, Any]):
        bidder_id = fields.get("bidder_id")
        bidder_name = fields.get("bidder_name")
        seq_id = int(fields.get("sequence_id", 0))
        amount = float(fields.get("amount", 0.0))

        # 1. Idempotently persist bid record into ledger
        await db_service.execute(
            """INSERT INTO bids (auction_id, bidder_id, bidder_name, sequence_id, amount, status)
               VALUES ($1, $2, $3, $4, $5, 'ACCEPTED')
               ON CONFLICT (auction_id, sequence_id) DO NOTHING""",
            auction_id, uuid.UUID(bidder_id), bidder_name, seq_id, amount
        )

        # 2. Update auction record with monotonic sequence constraint
        await db_service.execute(
            """UPDATE auctions
               SET current_price = $1, highest_bidder_id = $2, highest_bidder_name = $3,
                   bid_count = bid_count + 1, sequence_id = $4
               WHERE id = $5 AND sequence_id < $4""",
            amount, uuid.UUID(bidder_id), bidder_name, seq_id, auction_id
        )

        logger.info(f"Persisted bid seq #{seq_id} for {auction_id}: ${amount:,.2f} by {bidder_name}")

    async def record_telemetry_snapshot(self):
        try:
            snapshot = telemetry_metrics.get_snapshot()
            if snapshot["total_bids_processed"] > 0:
                await db_service.execute(
                    """INSERT INTO telemetry_metrics (auction_id, active_ws_clients, bids_per_second, p50_latency_ms, p95_latency_ms, p99_latency_ms)
                       VALUES ('global', $1, $2, $3, $4, $5)""",
                    snapshot["active_ws_connections"],
                    snapshot["bids_per_second"],
                    snapshot["p50_latency_ms"],
                    snapshot["p95_latency_ms"],
                    snapshot["p99_latency_ms"]
                )
        except Exception as e:
            logger.debug(f"Telemetry snapshot skipped: {e}")

    async def stop(self):
        self.running = False
        await redis_service.close()
        await db_service.close()
        logger.info("Sync worker shutdown cleanly.")

async def main():
    worker = WriteBehindSyncWorker()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(worker.stop()))

    await worker.start()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
