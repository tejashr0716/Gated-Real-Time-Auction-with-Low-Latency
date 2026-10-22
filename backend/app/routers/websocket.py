import json
import logging
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from app.services.websocket_manager import ws_manager
from app.services.lua_engine import lua_engine
from app.services.database import db_service
from app.services.auth_security import verify_access_token

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
                # Enforce authentication and derive identity from verified claims
                token = payload.get("token") or payload.get("auth_token")
                claims = verify_access_token(token) if token else None
                if not claims or "sub" not in claims:
                    await websocket.send_text(json.dumps({
                        "event": "BID_REJECTED",
                        "code": "ERR_UNAUTHENTICATED",
                        "msg": "Authentication required for WebSocket bid placement. Provide valid bearer token."
                    }))
                    continue

                bidder_id = claims["sub"]
                bidder_name = claims.get("username", "VIP_Bidder")
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
