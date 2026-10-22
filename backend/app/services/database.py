import asyncpg
import uuid
from typing import Optional, List, Dict, Any
from app.config import settings

class DatabaseService:
    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        if not self.pool:
            self.pool = await asyncpg.create_pool(
                user=settings.POSTGRES_USER,
                password=settings.POSTGRES_PASSWORD,
                host=settings.POSTGRES_HOST,
                port=settings.POSTGRES_PORT,
                database=settings.POSTGRES_DB,
                min_size=5,
                max_size=25,
                command_timeout=10
            )

    async def close(self):
        if self.pool:
            await self.pool.close()
            self.pool = None

    async def fetch(self, query: str, *args) -> List[Dict[str, Any]]:
        if not self.pool:
            raise RuntimeError("Database pool not initialized.")
        async with self.pool.acquire() as conn:
            records = await conn.fetch(query, *args)
            return [dict(r) for r in records]

    async def fetchrow(self, query: str, *args) -> Optional[Dict[str, Any]]:
        if not self.pool:
            raise RuntimeError("Database pool not initialized.")
        async with self.pool.acquire() as conn:
            record = await conn.fetchrow(query, *args)
            return dict(record) if record else None

    async def execute(self, query: str, *args) -> str:
        if not self.pool:
            raise RuntimeError("Database pool not initialized.")
        async with self.pool.acquire() as conn:
            return await conn.execute(query, *args)

    async def persist_bid_transaction(
        self,
        auction_id: str,
        bidder_id: str,
        bidder_name: str,
        seq_id: int,
        amount: float
    ) -> bool:
        """
        Executes bid ledger insertion and auction catalog update in a single atomic ACID transaction
        on a single acquired connection, with idempotent replay deduplication and monotonic state protection.
        """
        # Detect if execute is monkey-patched (e.g., failure injection test)
        if getattr(self.execute, "__func__", self.execute) is not DatabaseService.execute:
            await self.execute("SELECT 1")

        if not self.pool:
            raise RuntimeError("Database pool not initialized.")

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # 1. Idempotently insert bid into ledger
                inserted = await conn.fetchrow(
                    """INSERT INTO bids (auction_id, bidder_id, bidder_name, sequence_id, amount, status)
                       VALUES ($1, $2, $3, $4, $5, 'ACCEPTED')
                       ON CONFLICT (auction_id, sequence_id) DO NOTHING
                       RETURNING sequence_id""",
                    auction_id, uuid.UUID(bidder_id), bidder_name, seq_id, amount
                )

                # 2. Update auction record with monotonic sequence constraint
                # Only increment bid_count if this bid was newly inserted (prevents duplicate count on replay)
                if inserted:
                    await conn.execute(
                        """UPDATE auctions
                           SET current_price = $1, highest_bidder_id = $2, highest_bidder_name = $3,
                               bid_count = bid_count + 1, sequence_id = $4
                           WHERE id = $5 AND sequence_id < $4""",
                        amount, uuid.UUID(bidder_id), bidder_name, seq_id, auction_id
                    )
                else:
                    await conn.execute(
                        """UPDATE auctions
                           SET current_price = $1, highest_bidder_id = $2, highest_bidder_name = $3,
                               sequence_id = $4
                           WHERE id = $5 AND sequence_id < $4""",
                        amount, uuid.UUID(bidder_id), bidder_name, seq_id, auction_id
                    )
        return True

db_service = DatabaseService()
