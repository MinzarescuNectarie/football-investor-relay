import asyncio
import unittest
from aiohttp import ClientSession, web
from server import create_app

class RelayTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.runner = web.AppRunner(create_app())
        await self.runner.setup()
        site = web.TCPSite(self.runner, '127.0.0.1', 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        self.url = f'http://127.0.0.1:{port}/ws'
        self.session = ClientSession()
        self.connections = []

    async def asyncTearDown(self):
        for connection in self.connections: await connection.close()
        await self.session.close()
        await self.runner.cleanup()

    async def connect(self):
        connection = await self.session.ws_connect(self.url)
        self.connections.append(connection)
        return connection

    async def request(self, connection, data):
        await connection.send_json(data)
        return await asyncio.wait_for(connection.receive_json(), 3)

    async def create(self, connection, name='Test room', password='', capacity=3):
        return await self.request(connection, {'type':'create', 'name':name, 'password':password, 'capacity':capacity, 'budget':100})

    async def test_rooms_passwords_authority_and_cleanup(self):
        host, guest, stranger, other_host = [await self.connect() for _ in range(4)]
        created = await self.create(host, password='private-test', capacity=2)
        self.assertEqual(created['type'], 'created')
        duplicate = await self.create(other_host, name=' TEST ROOM ')
        self.assertIn('already exists', duplicate['message'])
        wrong = await self.request(guest, {'type':'join', 'code':created['code'], 'password':'wrong'})
        self.assertIn('Incorrect', wrong['message'])
        listing = await self.request(stranger, {'type':'list'})
        self.assertTrue(listing['rooms'][0]['private'])
        self.assertNotIn('password', listing['rooms'][0])
        self.assertNotIn('salt', listing['rooms'][0])
        joined = await self.request(guest, {'type':'join', 'code':created['code'], 'password':'private-test', 'name':'Guest'})
        self.assertEqual(joined['id'], 2)
        notice = await host.receive_json()
        self.assertEqual(notice['type'], 'peer_joined')
        full = await self.request(stranger, {'type':'join', 'code':created['code'], 'password':'private-test'})
        self.assertIn('full', full['message'])
        denied = await self.request(guest, {'type':'state', 'state':{'phase':'results'}})
        self.assertIn('Only the host', denied['message'])
        await guest.send_json({'type':'action', 'id':1, 'action':'pick', 'pick':3})
        action = await host.receive_json()
        self.assertEqual(action['id'], 2)  # Claimed sender IDs cannot override the socket identity.
        other = await self.create(other_host, name='Other room')
        self.assertEqual(other['type'], 'created')
        await host.send_json({'type':'state', 'state':{'phase':'draft', 'marker':'room-one'}})
        state = await guest.receive_json()
        self.assertEqual(state['state']['marker'], 'room-one')
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(other_host.receive_json(), 0.1)
        await host.close()
        self.assertEqual((await guest.receive_json())['type'], 'room_closed')
        remaining = await self.request(stranger, {'type':'list'})
        self.assertEqual([r['code'] for r in remaining['rooms']], [other['code']])

    async def test_started_rooms_invalid_messages_and_guest_leave(self):
        host, guest, late = [await self.connect() for _ in range(3)]
        created = await self.create(host)
        await self.request(guest, {'type':'join', 'code':created['code']})
        await host.receive_json()
        await host.send_json({'type':'state', 'state':{'phase':'draft'}})
        await guest.receive_json()
        result = await self.request(late, {'type':'join', 'code':created['code']})
        self.assertIn('started', result['message'])
        result = await self.request(guest, {'type':'action', 'action':'advance', 'pick':-1})
        self.assertIn('Invalid', result['message'])
        await late.send_str('not-json')
        self.assertEqual((await late.receive_json())['type'], 'error')
        await guest.close()
        self.assertEqual((await host.receive_json())['type'], 'peer_left')

if __name__ == '__main__': unittest.main()
