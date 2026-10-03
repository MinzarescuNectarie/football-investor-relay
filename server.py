"""Ephemeral, host-authoritative Football Investor WebSocket relay."""
import asyncio
import hashlib
import hmac
import json
import os
import secrets
import time
from dataclasses import dataclass, field
from aiohttp import web, WSMsgType

MAX_MESSAGE = 262144
MAX_CLIENTS = 200
MAX_ROOMS = 50

@dataclass(eq=False)
class Client:
    ws: web.WebSocketResponse
    transport: object = None
    room: object = None
    id: int = 0
    tokens: float = 60
    checked: float = field(default_factory=time.monotonic)

@dataclass
class Room:
    code: str
    name: str
    capacity: int
    budget: int
    salt: bytes
    password: bytes
    members: dict = field(default_factory=dict)
    next_id: int = 2
    status: str = 'lobby'

def create_app():
    app = web.Application(client_max_size=MAX_MESSAGE)
    rooms, clients = {}, set()

    async def send(client, data):
        if client.ws.closed: return
        if client.transport.get_write_buffer_size() > MAX_MESSAGE * 4:
            await client.ws.close(code=1013, message=b'Slow connection')
            return
        try:
            await asyncio.wait_for(client.ws.send_json(data), 5)
        except (ConnectionResetError, asyncio.TimeoutError):
            await client.ws.close(code=1013, message=b'Connection unavailable')

    async def error(client, text):
        await send(client, {'type': 'error', 'message': text})

    async def detach(client):
        room = client.room
        if room is None: return
        client.room = None
        room.members.pop(client.id, None)
        if client.id == 1:
            rooms.pop(room.code, None)
            for guest in list(room.members.values()):
                guest.room = None
                await send(guest, {'type': 'room_closed'})
            room.members.clear()
        elif 1 in room.members:
            await send(room.members[1], {'type': 'peer_left', 'id': client.id})

    async def dispatch(client, data):
        kind = data.get('type')
        if kind == 'list':
            listing = [{'name': r.name, 'code': r.code, 'private': bool(r.password),
                        'players': len(r.members), 'capacity': r.capacity,
                        'budget': r.budget, 'status': r.status} for r in rooms.values()]
            await send(client, {'type': 'rooms', 'rooms': listing})
        elif kind == 'create':
            if client.room: return await error(client, 'Leave your current room first.')
            name = str(data.get('name', '')).strip()
            capacity, budget = data.get('capacity', 4), data.get('budget', 100)
            password = data.get('password', '')
            if not 2 <= len(name) <= 48 or not isinstance(capacity, int) or not 2 <= capacity <= 5:
                return await error(client, 'Invalid lobby name or capacity.')
            if not isinstance(budget, int) or not 1 <= budget <= 100000:
                return await error(client, 'Invalid budget.')
            if not isinstance(password, str) or len(password) > 128 or (password and len(password) < 4):
                return await error(client, 'Private rooms need a password of 4–128 characters.')
            if any(r.name.casefold() == name.casefold() for r in rooms.values()):
                return await error(client, 'That lobby name already exists. Choose another.')
            if len(rooms) >= MAX_ROOMS: return await error(client, 'Relay is full. Try later.')
            salt = secrets.token_bytes(16)
            digest = await asyncio.to_thread(hashlib.scrypt, password.encode(), salt=salt, n=16384, r=8, p=1) if password else b''
            # Recheck after the password hash, before allocating the room atomically.
            if any(r.name.casefold() == name.casefold() for r in rooms.values()):
                return await error(client, 'That lobby name already exists. Choose another.')
            if len(rooms) >= MAX_ROOMS: return await error(client, 'Relay is full. Try later.')
            while True:
                code = 'FI-R-' + ''.join(secrets.choice('ABCDEFGHJKLMNPQRSTUVWXYZ23456789') for _ in range(10)) + ('-P' if password else '-U')
                if code not in rooms: break
            room = Room(code, name, capacity, budget, salt, digest)
            rooms[code] = room
            client.room, client.id = room, 1
            room.members[1] = client
            await send(client, {'type': 'created', 'code': code, 'id': 1})
        elif kind == 'join':
            if client.room: return await error(client, 'Leave your current room first.')
            room = rooms.get(str(data.get('code', '')).strip().upper())
            if not room: return await error(client, 'Room not found or closed.')
            if room.status != 'lobby' or len(room.members) >= room.capacity:
                return await error(client, 'Room is full or the game has already started.')
            password = data.get('password', '')
            if not isinstance(password, str) or len(password) > 128: return await error(client, 'Invalid password.')
            if room.password:
                digest = await asyncio.to_thread(hashlib.scrypt, password.encode(), salt=room.salt, n=16384, r=8, p=1)
                if not hmac.compare_digest(digest, room.password): return await error(client, 'Incorrect room password.')
            if rooms.get(room.code) is not room or room.status != 'lobby' or len(room.members) >= room.capacity:
                return await error(client, 'Room closed, full, or already started.')
            client.room, client.id = room, room.next_id
            room.next_id += 1
            room.members[client.id] = client
            await send(client, {'type': 'joined', 'id': client.id, 'code': room.code})
            await send(room.members[1], {'type': 'peer_joined', 'id': client.id, 'name': str(data.get('name', 'Guest')).strip()[:40] or 'Guest'})
        elif kind == 'state':
            room = client.room
            if not room or client.id != 1: return await error(client, 'Only the host may send game state.')
            state = data.get('state')
            if not isinstance(state, dict) or state.get('phase') not in ('lobby', 'draft', 'results'):
                return await error(client, 'Invalid game state.')
            room.status = state['phase']
            for id, guest in list(room.members.items()):
                if id != 1: await send(guest, {'type': 'state', 'state': state})
        elif kind == 'action':
            room = client.room
            if not room or client.id == 1: return await error(client, 'Join a room first.')
            action, pick = data.get('action'), data.get('pick')
            if action not in ('pick', 'lock') or not isinstance(pick, int): return await error(client, 'Invalid player action.')
            await send(room.members[1], {'type': 'action', 'id': client.id, 'action': action, 'pick': pick})
        elif kind == 'leave':
            await detach(client)
        else:
            await error(client, 'Unknown request.')

    async def websocket(request):
        if len(clients) >= MAX_CLIENTS: raise web.HTTPServiceUnavailable(text='Relay at capacity')
        ws = web.WebSocketResponse(heartbeat=20, receive_timeout=90, max_msg_size=MAX_MESSAGE, compress=False)
        await ws.prepare(request)
        if len(clients) >= MAX_CLIENTS:
            await ws.close(code=1013, message=b'Relay at capacity')
            return ws
        client = Client(ws, request.transport)
        clients.add(client)
        try:
            async for msg in ws:
                if msg.type != WSMsgType.TEXT: continue
                now = time.monotonic()
                client.tokens = min(60, client.tokens + (now - client.checked) * 20)
                client.checked = now
                client.tokens -= 1
                if client.tokens < 0:
                    await ws.close(code=1008, message=b'Rate limit'); break
                try:
                    data = json.loads(msg.data)
                    if not isinstance(data, dict): raise ValueError()
                    cost = 9 if data.get('type') in ('join', 'create') else 0
                    if client.tokens < cost:
                        await ws.close(code=1008, message=b'Rate limit'); break
                    client.tokens -= cost
                    await dispatch(client, data)
                except (ValueError, TypeError, KeyError):
                    await error(client, 'Invalid request.')
        finally:
            await detach(client)
            clients.discard(client)
        return ws

    async def health(request):
        return web.json_response({'service': 'Football Investor relay', 'protocol': 1, 'rooms': len(rooms)})

    async def shutdown(app):
        await asyncio.gather(*(c.ws.close(code=1001, message=b'Server restarting') for c in list(clients)))

    app.router.add_get('/ws', websocket)
    app.router.add_get('/health', health)
    app.router.add_get('/', health)
    app.on_shutdown.append(shutdown)
    return app

if __name__ == '__main__':
    web.run_app(create_app(), host=os.environ.get('BIND_ADDRESS', '127.0.0.1'), port=int(os.environ.get('PORT', 8080)), access_log=None)
