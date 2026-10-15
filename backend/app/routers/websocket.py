import json
import logging
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from app.services.websocket_manager import ws_manager
from app.services.lua_engine import lua_engine
from app.services.database import db_service

logger = logging.getLogger("auction_ws_route")
router = APIRouter(tags=["WebSocket Real-Time Telemetry"])

@router.websocket("/ws/auctions/{auction_id}")
async def websocket_auction_endpoint(websocket: WebSocket, auction_id: str):
    await ws_manager.connect(auction_id, websocket)
    try:
        while True:
            data_text = await websocket.receive_text()
            try:
                payload = json.loads(data_text)
            except Exception:
                if data_text.strip().upper() == "PING":
                    await websocket.send_text(json.dumps({"event": "PONG"}))
                continue

            action = payload.get("action")
            if action == "PING":
                await websocket.send_text(json.dumps({"event": "PONG", "echo_ts": payload.get("ts")}))
            elif action == "BID":
                # Execute bid over WebSocket low-latency path
                bidder_id = payload.get("bidder_id", "a0000001-0000-0000-0000-000000000001")
                bidder_name = payload.get("bidder_name", "VIP_Bidder")
                bid_amount = float(payload.get("amount", 0.0))

                auction = await db_service.fetchrow("SELECT min_escrow_required FROM auctions WHERE id = $1", auction_id)
                required_escrow = float(auction["min_escrow_required"]) if auction else 5000.0

                result = await lua_engine.execute_bid(
                    auction_id=auction_id,
                    bidder_id=bidder_id,
                    bidder_name=bidder_name,
                    bid_amount=bid_amount,
                    required_escrow=required_escrow
                )
                await websocket.send_text(json.dumps({
                    "event": "BID_RECEIPT",
                    "result": result
                }))

    except WebSocketDisconnect:
        await ws_manager.disconnect(auction_id, websocket)
    except Exception as e:
        logger.error(f"WebSocket client error: {e}")
        await ws_manager.disconnect(auction_id, websocket)
