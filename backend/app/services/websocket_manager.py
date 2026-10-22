import asyncio
import json
import logging
from typing import Dict, Set, Optional
from fastapi import WebSocket
from app.services.redis_client import redis_service
from app.services.telemetry_metrics import telemetry_metrics

logger = logging.getLogger("auction_ws")

class WebSocketManager:
    def __init__(self):
        # auction_id -> set of WebSockets
        self.active_rooms: Dict[str, Set[WebSocket]] = {}
        # Keep track of active Redis pub/sub listener tasks
        self.pubsub_tasks: Dict[str, asyncio.Task] = {}
        self.total_connections: int = 0
        self.heartbeat_task: Optional[asyncio.Task] = None

    async def connect(self, auction_id: str, websocket: WebSocket):
        await websocket.accept()
        if auction_id not in self.active_rooms:
            self.active_rooms[auction_id] = set()
            # Start Redis Pub/Sub listener for this auction room
            self.pubsub_tasks[auction_id] = asyncio.create_task(
                self._listen_redis_channel(auction_id)
            )

        self.active_rooms[auction_id].add(websocket)
        self.total_connections += 1
        telemetry_metrics.set_ws_connections(self.total_connections)

        # Send initial room state & history
        await self._send_initial_state(auction_id, websocket)

    async def disconnect(self, auction_id: str, websocket: WebSocket):
        if auction_id in self.active_rooms:
            self.active_rooms[auction_id].discard(websocket)
            self.total_connections = max(0, self.total_connections - 1)
            telemetry_metrics.set_ws_connections(self.total_connections)

            if len(self.active_rooms[auction_id]) == 0:
                del self.active_rooms[auction_id]
                # Cancel Redis Pub/Sub listener if room is empty
                if auction_id in self.pubsub_tasks:
                    task = self.pubsub_tasks.pop(auction_id)
                    task.cancel()

    async def _send_initial_state(self, auction_id: str, websocket: WebSocket):
        try:
            client = redis_service.get_client()
            meta = await client.hgetall(f"auction:{auction_id}:meta")
            recent_bids_raw = await client.lrange(f"auction_bids:{auction_id}", 0, 24)
            recent_bids = [json.loads(b) for b in recent_bids_raw]

            handshake = {
                "event": "ROOM_HANDSHAKE",
                "auction_id": auction_id,
                "meta": meta,
                "recent_bids": recent_bids,
                "active_room_clients": len(self.active_rooms.get(auction_id, set())),
                "server_time_ms": int(asyncio.get_event_loop().time() * 1000)
            }
            await websocket.send_text(json.dumps(handshake))
        except Exception as e:
            logger.error(f"Error sending room handshake: {e}")

    async def _listen_redis_channel(self, auction_id: str):
        channel_name = f"channel:auction:{auction_id}"
        client = redis_service.get_client()
        pubsub = client.pubsub()
        await pubsub.subscribe(channel_name)

        try:
            async for message in pubsub.listen():
                if message["type"] == "message":
                    raw_data = message["data"]
                    await self.broadcast_to_room(auction_id, raw_data)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"PubSub exception on {channel_name}: {e}")
        finally:
            await pubsub.unsubscribe(channel_name)
            await pubsub.close()

    async def broadcast_to_room(self, auction_id: str, message_str: str):
        room = self.active_rooms.get(auction_id)
        if not room:
            return

        dead_sockets = set()
        # Non-blocking async fan-out across all 500+ clients
        send_coros = []
        for ws in room:
            send_coros.append(self._safe_send(ws, message_str, dead_sockets))

        await asyncio.gather(*send_coros, return_exceptions=True)

        # Prune disconnected clients
        for dead_ws in dead_sockets:
            room.discard(dead_ws)
            self.total_connections = max(0, self.total_connections - 1)
        telemetry_metrics.set_ws_connections(self.total_connections)

    async def _safe_send(self, websocket: WebSocket, data: str, dead_sockets: Set[WebSocket]):
        try:
            await asyncio.wait_for(websocket.send_text(data), timeout=0.5)
        except Exception:
            dead_sockets.add(websocket)

    async def start_heartbeat_loop(self):
        if not self.heartbeat_task:
            self.heartbeat_task = asyncio.create_task(self._heartbeat_worker())

    async def stop_heartbeat_loop(self):
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
            try:
                await self.heartbeat_task
            except asyncio.CancelledError:
                pass
            self.heartbeat_task = None
        for task in list(self.pubsub_tasks.values()):
            task.cancel()
        self.pubsub_tasks.clear()

    async def _heartbeat_worker(self):
        while True:
            try:
                await asyncio.sleep(1.0)
                snapshot = telemetry_metrics.get_snapshot(len(self.active_rooms))
                tick_msg = json.dumps({
                    "event": "TELEMETRY_TICK",
                    "telemetry": snapshot
                })
                # Broadcast telemetry tick to all active rooms
                for room_id in list(self.active_rooms.keys()):
                    await self.broadcast_to_room(room_id, tick_msg)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in telemetry heartbeat worker: {e}")

ws_manager = WebSocketManager()
