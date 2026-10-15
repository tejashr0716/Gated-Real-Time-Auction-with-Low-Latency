import redis.asyncio as aioredis
from typing import Optional
from app.config import settings

class RedisService:
    def __init__(self):
        self.pool: Optional[aioredis.ConnectionPool] = None
        self.client: Optional[aioredis.Redis] = None

    async def connect(self):
        if not self.client:
            self.pool = aioredis.ConnectionPool(
                host=settings.REDIS_HOST,
                port=settings.REDIS_PORT,
                db=settings.REDIS_DB,
                password=settings.REDIS_PASSWORD,
                decode_responses=True,
                max_connections=settings.REDIS_MAX_CONNECTIONS
            )
            self.client = aioredis.Redis(connection_pool=self.pool)

    async def close(self):
        if self.client:
            await self.client.close()
            await self.pool.disconnect()
            self.client = None
            self.pool = None

    def get_client(self) -> aioredis.Redis:
        if not self.client:
            raise RuntimeError("Redis client is not initialized. Call connect() first.")
        return self.client

redis_service = RedisService()
