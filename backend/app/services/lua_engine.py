import os
import time
import json
import logging
from typing import Dict, Any, Optional
from app.services.redis_client import redis_service
from app.services.telemetry_metrics import telemetry_metrics

logger = logging.getLogger("auction_lua")

class LuaEngine:
    def __init__(self):
        self.place_bid_sha: Optional[str] = None
        self.reset_auction_sha: Optional[str] = None
        self.place_bid_script_content: str = ""
        self.reset_auction_script_content: str = ""

    def load_scripts(self):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        lua_dir = os.path.join(os.path.dirname(current_dir), "lua")

        place_bid_path = os.path.join(lua_dir, "place_bid.lua")
        with open(place_bid_path, "r", encoding="utf-8") as f:
            self.place_bid_script_content = f.read()

        reset_path = os.path.join(lua_dir, "reset_auction.lua")
        with open(reset_path, "r", encoding="utf-8") as f:
            self.reset_auction_script_content = f.read()

    async def register_scripts(self):
        self.load_scripts()
        client = redis_service.get_client()
        self.place_bid_sha = await client.script_load(self.place_bid_script_content)
        self.reset_auction_sha = await client.script_load(self.reset_auction_script_content)
        logger.info(f"Loaded Lua scripts. Bid SHA: {self.place_bid_sha}, Reset SHA: {self.reset_auction_sha}")

    async def execute_bid(
        self,
        auction_id: str,
        bidder_id: str,
        bidder_name: str,
        bid_amount: float,
        required_escrow: float = 5000.0
    ) -> Dict[str, Any]:
        client = redis_service.get_client()
        if not self.place_bid_sha:
            await self.register_scripts()

        now_ms = int(time.time() * 1000)

        # Keys - Atomic evaluation resolves previous bidder directly inside Redis memory
        meta_key = f"auction:{auction_id}:meta"
        escrow_key = f"escrow:{bidder_id}"
        bids_list_key = f"auction_bids:{auction_id}"
        stream_key = f"auction_stream:{auction_id}"

        keys = [meta_key, escrow_key, bids_list_key, stream_key]
        args = [
            auction_id,
            bidder_id,
            bidder_name,
            str(bid_amount),
            str(now_ms),
            str(required_escrow)
        ]

        t0 = time.perf_counter()
        raw_res = await client.evalsha(self.place_bid_sha, len(keys), *keys, *args)
        t1 = time.perf_counter()
        latency_ms = round((t1 - t0) * 1000.0, 3)

        telemetry_metrics.record_bid(latency_ms)

        if isinstance(raw_res, str):
            res = json.loads(raw_res)
        else:
            res = raw_res

        res["latency_ms"] = latency_ms
        return res

    async def reset_auction(
        self,
        auction_id: str,
        starting_price: float,
        reserve_price: float,
        min_increment: float,
        duration_seconds: int = 1800,
        soft_close_window_sec: int = 15,
        extension_sec: int = 30
    ) -> Dict[str, Any]:
        client = redis_service.get_client()
        if not self.reset_auction_sha:
            await self.register_scripts()

        now_ms = int(time.time() * 1000)
        keys = [
            f"auction:{auction_id}:meta",
            f"auction_bids:{auction_id}",
            f"auction_stream:{auction_id}"
        ]
        args = [
            auction_id,
            str(starting_price),
            str(reserve_price),
            str(min_increment),
            str(duration_seconds),
            str(now_ms),
            str(soft_close_window_sec),
            str(extension_sec)
        ]

        raw_res = await client.evalsha(self.reset_auction_sha, len(keys), *keys, *args)
        if isinstance(raw_res, str):
            return json.loads(raw_res)
        return raw_res

    async def set_user_escrow(
        self,
        user_id: str,
        total_balance: float,
        locked_collateral: float = 0.0,
        tier: str = "VIP",
        kyc_status: str = "VERIFIED"
    ):
        client = redis_service.get_client()
        escrow_key = f"escrow:{user_id}"
        await client.hset(escrow_key, mapping={
            "total_balance": str(total_balance),
            "locked_collateral": str(locked_collateral),
            "tier": tier,
            "kyc_status": kyc_status
        })

lua_engine = LuaEngine()
