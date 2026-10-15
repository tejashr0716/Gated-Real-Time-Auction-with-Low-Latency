import asyncpg
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

db_service = DatabaseService()
