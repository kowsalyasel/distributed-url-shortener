from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from typing import List, Dict
import asyncio
import aioredis
import json

app = FastAPI()
REDIS_URL = "redis://localhost:6379"

class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, List[WebSocket]] = {}

    async def connect(self, room_id: str, websocket: WebSocket):
        await websocket.accept()
        if room_id not in self.active_connections:
            self.active_connections[room_id] = []
        self.active_connections[room_id].append(websocket)

    def disconnect(self, room_id: str, websocket: WebSocket):
        if room_id in self.active_connections:
            self.active_connections[room_id].remove(websocket)

    async def broadcast_to_room(self, room_id: str, message: str):
        if room_id in self.active_connections:
            for connection in self.active_connections[room_id]:
                try:
                    await connection.send_text(message)
                except Exception:
                    pass # Handle stale connections gracefully

manager = ConnectionManager()

@app.websocket("/ws/{room_id}")
async def websocket_endpoint(websocket: WebSocket, room_id: str):
    await manager.connect(room_id, websocket)
    redis = await aioredis.from_url(REDIS_URL, decode_responses=True)
    pubsub = redis.pubsub()
    await pubsub.subscribe(room_id)

    # Listen to horizontal scaling broadcast events from Redis
    async def redis_listener():
        try:
            async for message in pubsub.listen():
                if message['type'] == 'message':
                    await manager.broadcast_to_room(room_id, message['data'])
        except asyncio.CancelledError:
            pass

    listener_task = asyncio.create_task(redis_listener())

    try:
        while True:
            data = await websocket.receive_text()
            # Publish event to Redis so ALL backend instances pick it up
            await redis.publish(room_id, data)
    except WebSocketDisconnect:
        manager.disconnect(room_id, websocket)
        listener_task.cancel()
        await pubsub.unsubscribe(room_id)
