import time
from datetime import datetime, timezone
from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from app.schemas.auction import AuctionCreate, AuctionResponse
from app.services.database import db_service
from app.services.redis_client import redis_service
from app.services.lua_engine import lua_engine

router = APIRouter(prefix="/api/auctions", tags=["Auctions"])

async def get_auction_live_overlay(row: dict) -> AuctionResponse:
    auction_id = row["id"]
    client = redis_service.get_client()
    meta = await client.hgetall(f"auction:{auction_id}:meta")

    current_price = float(meta.get("current_price", row["current_price"]))
    highest_bidder_id = meta.get("highest_bidder_id") or (str(row["highest_bidder_id"]) if row.get("highest_bidder_id") else None)
    highest_bidder_name = meta.get("highest_bidder_name") or row.get("highest_bidder_name")
    bid_count = int(meta.get("bid_count", row["bid_count"]))
    sequence_id = int(meta.get("sequence_id", row["sequence_id"]))
    status = meta.get("status", row["status"])
    reserve_price = float(row["reserve_price"])

    now_ms = int(time.time() * 1000)
    end_time_ms = int(meta.get("end_time_ms", int(row["end_time"].timestamp() * 1000) if row.get("end_time") else now_ms + 1800000))
    remaining_seconds = max(0.0, round((end_time_ms - now_ms) / 1000.0, 1))

    return AuctionResponse(
        id=row["id"],
        title=row["title"],
        description=row["description"],
        category=row["category"],
        starting_price=float(row["starting_price"]),
        reserve_price=reserve_price,
        current_price=current_price,
        min_increment=float(row["min_increment"]),
        min_escrow_required=float(row["min_escrow_required"]),
        highest_bidder_id=highest_bidder_id,
        highest_bidder_name=highest_bidder_name,
        bid_count=bid_count,
        sequence_id=sequence_id,
        status=status,
        soft_close_window_seconds=row["soft_close_window_seconds"],
        extension_seconds=row["extension_seconds"],
        image_url=row.get("image_url"),
        start_time=row.get("start_time"),
        end_time=row.get("end_time"),
        end_time_ms=end_time_ms,
        remaining_seconds=remaining_seconds,
        reserve_met=(current_price >= reserve_price),
        created_at=row.get("created_at")
    )

@router.get("", response_model=List[AuctionResponse])
async def list_auctions():
    rows = await db_service.fetch("SELECT * FROM auctions ORDER BY created_at DESC")
    client = redis_service.get_client()

    results = []
    now_ms = int(time.time() * 1000)

    for row in rows:
        auc_id = row["id"]
        meta_key = f"auction:{auc_id}:meta"
        exists = await client.exists(meta_key)
        if not exists:
            # Pre-load into Redis from PostgreSQL
            end_ms = int(row["end_time"].timestamp() * 1000) if row.get("end_time") else now_ms + 1800000
            await client.hset(meta_key, mapping={
                "status": row["status"],
                "current_price": str(row["current_price"]),
                "reserve_price": str(row["reserve_price"]),
                "min_increment": str(row["min_increment"]),
                "highest_bidder_id": str(row["highest_bidder_id"]) if row.get("highest_bidder_id") else "",
                "highest_bidder_name": row.get("highest_bidder_name") or "",
                "bid_count": str(row["bid_count"]),
                "sequence_id": str(row["sequence_id"]),
                "end_time_ms": str(end_ms),
                "soft_close_window_sec": str(row["soft_close_window_seconds"]),
                "extension_sec": str(row["extension_seconds"])
            })
        item = await get_auction_live_overlay(row)
        results.append(item)

    return results

@router.get("/{auction_id}", response_model=AuctionResponse)
async def get_auction(auction_id: str):
    row = await db_service.fetchrow("SELECT * FROM auctions WHERE id = $1", auction_id)
    if not row:
        raise HTTPException(status_code=404, detail="Auction not found")
    return await get_auction_live_overlay(row)

@router.post("/{auction_id}/reset")
async def reset_auction_endpoint(auction_id: str):
    row = await db_service.fetchrow("SELECT * FROM auctions WHERE id = $1", auction_id)
    if not row:
        raise HTTPException(status_code=404, detail="Auction not found")

    starting_price = float(row["starting_price"])
    res = await lua_engine.reset_auction(
        auction_id=auction_id,
        starting_price=starting_price,
        reserve_price=float(row["reserve_price"]),
        min_increment=float(row["min_increment"]),
        duration_seconds=1800,
        soft_close_window_sec=row["soft_close_window_seconds"],
        extension_sec=row["extension_seconds"]
    )

    # Sync back reset in postgres
    await db_service.execute(
        """UPDATE auctions 
           SET current_price = $1, highest_bidder_id = NULL, highest_bidder_name = NULL,
               bid_count = 0, sequence_id = 0, status = 'ACTIVE'
           WHERE id = $2""",
        starting_price, auction_id
    )

    return res

@router.get("/{auction_id}/history")
async def get_auction_bid_history(auction_id: str, limit: int = Query(50, ge=1, le=200)):
    records = await db_service.fetch(
        """SELECT id, sequence_id, bidder_name, amount, latency_ms, status, created_at
           FROM bids
           WHERE auction_id = $1
           ORDER BY sequence_id DESC
           LIMIT $2""",
        auction_id, limit
    )
    return [
        {
            "id": str(r["id"]),
            "sequence_id": r["sequence_id"],
            "bidder_name": r["bidder_name"],
            "amount": float(r["amount"]),
            "latency_ms": float(r["latency_ms"]),
            "status": r["status"],
            "created_at": r["created_at"].isoformat() if r["created_at"] else None
        }
        for r in records
    ]
