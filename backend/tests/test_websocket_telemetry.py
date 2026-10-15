import pytest
import json
import asyncio
from unittest.mock import AsyncMock
from app.services.websocket_manager import WebSocketManager
from app.services.redis_client import redis_service

@pytest.mark.asyncio
async def test_websocket_manager_connect_and_broadcast():
    manager = WebSocketManager()
    auc_id = "auc_shelby_gt500"

    # Pre-populate some auction data in Redis
    client = redis_service.get_client()
    await client.hset(f"auction:{auc_id}:meta", mapping={
        "status": "ACTIVE",
        "current_price": "145000.00",
        "sequence_id": "18"
    })

    # Create mock WebSocket
    mock_ws = AsyncMock()
    mock_ws.accept = AsyncMock()
    mock_ws.send_text = AsyncMock()

    # 1. Connect client
    await manager.connect(auc_id, mock_ws)
    assert auc_id in manager.active_rooms
    assert mock_ws in manager.active_rooms[auc_id]
    assert manager.total_connections == 1

    # Verify handshake was sent
    mock_ws.accept.assert_awaited_once()
    assert mock_ws.send_text.await_count >= 1
    handshake_call = mock_ws.send_text.call_args_list[0][0][0]
    handshake_data = json.loads(handshake_call)
    assert handshake_data["event"] == "ROOM_HANDSHAKE"
    assert handshake_data["auction_id"] == auc_id

    # 2. Broadcast high-frequency telemetry event to room
    event_payload = json.dumps({
        "event": "BID_ACCEPTED",
        "auction_id": auc_id,
        "sequence_id": 19,
        "bid_amount": 147500.00
    })
    await manager.broadcast_to_room(auc_id, event_payload)

    # Verify mock received broadcast
    assert mock_ws.send_text.await_count >= 2
    broadcast_call = mock_ws.send_text.call_args_list[1][0][0]
    assert "BID_ACCEPTED" in broadcast_call

    # 3. Disconnect client
    await manager.disconnect(auc_id, mock_ws)
    assert auc_id not in manager.active_rooms
    assert manager.total_connections == 0
