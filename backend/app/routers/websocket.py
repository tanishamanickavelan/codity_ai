"""
WebSocket live updates for the dashboard (bonus feature).

The dashboard already works fine with periodic polling (see frontend's
setInterval), but a websocket push means the Overview page updates the
instant something changes rather than up to 8 seconds later. This is kept
intentionally simple: one broadcast loop pushes a fresh health snapshot to
every connected client every few seconds - no per-client subscriptions or
topic filtering, since the dashboard only has one thing to watch (system
health). A production system with many independent widgets would want a
pub/sub layer (Redis, etc.) instead of this in-process broadcast list.
"""
import asyncio
import json

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.database import SessionLocal
from app.logging_config import logger
from app.routers.dashboard import _compute_health

router = APIRouter(tags=["websocket"])


class ConnectionManager:
    def __init__(self):
        self.active: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            self.active.remove(ws)

    async def broadcast(self, message: dict):
        dead = []
        for ws in self.active:
            try:
                await ws.send_text(json.dumps(message))
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)


manager = ConnectionManager()
_broadcaster_started = False


async def _broadcast_loop():
    while True:
        if manager.active:
            db = SessionLocal()
            try:
                health = _compute_health(db)
                await manager.broadcast({"type": "health", "data": health})
            except Exception as e:  # noqa: BLE001
                logger.error(f"websocket broadcast error: {e}")
            finally:
                db.close()
        await asyncio.sleep(3)


@router.websocket("/ws/dashboard")
async def dashboard_ws(websocket: WebSocket):
    global _broadcaster_started
    await manager.connect(websocket)
    if not _broadcaster_started:
        _broadcaster_started = True
        asyncio.create_task(_broadcast_loop())
    try:
        while True:
            # We don't expect inbound messages, but need to await something
            # so the server notices a disconnect.
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
