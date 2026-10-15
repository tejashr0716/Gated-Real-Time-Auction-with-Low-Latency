from fastapi import APIRouter, HTTPException, Depends
from app.schemas.bid import BidRequest, BidResponse
from app.services.lua_engine import lua_engine
from app.services.database import db_service

router = APIRouter(prefix="/api/auctions", tags=["Bids"])

@router.post("/{auction_id}/bid", response_model=BidResponse)
async def place_bid(auction_id: str, bid: BidRequest):
    if auction_id != bid.auction_id:
        raise HTTPException(status_code=400, detail="Mismatched auction_id in path and body")

    # Get minimum required escrow collateral for this auction
    auction = await db_service.fetchrow("SELECT min_escrow_required FROM auctions WHERE id = $1", auction_id)
    required_escrow = float(auction["min_escrow_required"]) if auction else 5000.0

    # Execute atomic bid via Lua Script in Redis
    result = await lua_engine.execute_bid(
        auction_id=bid.auction_id,
        bidder_id=bid.bidder_id,
        bidder_name=bid.bidder_name,
        bid_amount=bid.bid_amount,
        required_escrow=required_escrow
    )

    if not result.get("ok"):
        code = result.get("code", "UNKNOWN_ERROR")
        msg = result.get("msg", "Bid placement rejected")

        if code == "ERR_BID_TOO_LOW":
            raise HTTPException(status_code=409, detail=f"Conflict: {msg}")
        elif code == "ERR_AUCTION_EXPIRED":
            raise HTTPException(status_code=410, detail=f"Auction Concluded: {msg}")
        elif code == "ERR_INSUFFICIENT_ESCROW":
            raise HTTPException(status_code=403, detail=f"Gated Violation: {msg}")
        elif code == "ERR_SELF_OUTBID":
            raise HTTPException(status_code=400, detail=f"Invalid Action: {msg}")
        else:
            raise HTTPException(status_code=400, detail=msg)

    data = result.get("data", {})
    return BidResponse(
        ok=True,
        code="SUCCESS",
        message="Bid validated and placed atomically into orderbook.",
        auction_id=bid.auction_id,
        sequence_id=data.get("sequence_id"),
        bid_amount=data.get("bid_amount"),
        bidder_id=data.get("bidder_id"),
        bidder_name=data.get("bidder_name"),
        timestamp=data.get("timestamp"),
        end_time_ms=data.get("end_time_ms"),
        extended=data.get("extended"),
        latency_ms=result.get("latency_ms")
    )
