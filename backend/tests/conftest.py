import pytest
import os
import sys

backend_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if backend_path not in sys.path:
    sys.path.insert(0, backend_path)

from app.services.redis_client import redis_service
from app.services.database import db_service
from app.services.lua_engine import lua_engine

@pytest.fixture(autouse=True)
async def setup_test_services():
    # Fresh connection for current test's event loop with graceful service availability checks
    try:
        await redis_service.connect()
        await lua_engine.register_scripts()
    except Exception as e:
        pytest.skip(f"Redis service unavailable on localhost:6379: {e}")

    try:
        await db_service.connect()
    except Exception as e:
        pytest.skip(f"PostgreSQL service unavailable: {e}")

    yield
    await redis_service.close()
    await db_service.close()
