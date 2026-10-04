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

    async def ready_pair(self, host, guest):
        await guest.send_json({'type':'ready','ready':True})
        await host.receive_json()
        await self.request(host,{'type':'ready','ready':True})

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
        await self.ready_pair(host,guest)
        await host.send_json({'type':'state', 'state':{'phase':'draft', 'marker':'room-one'}})
        state = await guest.receive_json()
        self.assertEqual(state['state']['marker'], 'room-one')
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(other_host.receive_json(), 0.1)
        await host.send_json({"type":"leave"})
        self.assertEqual((await guest.receive_json())['type'], 'room_closed')
        remaining = await self.request(stranger, {'type':'list'})
        self.assertEqual([r['code'] for r in remaining['rooms']], [other['code']])

    async def test_started_rooms_invalid_messages_and_guest_leave(self):
        host, guest, late = [await self.connect() for _ in range(3)]
        created = await self.create(host)
        await self.request(guest, {'type':'join', 'code':created['code']})
        await host.receive_json()
        await self.ready_pair(host,guest)
        await host.send_json({'type':'state', 'state':{'phase':'draft'}})
        await guest.receive_json()
        result = await self.request(late, {'type':'join', 'code':created['code']})
        self.assertIn('started', result['message'])
        result = await self.request(guest, {'type':'action', 'action':'advance', 'pick':-1})
        self.assertIn('Invalid', result['message'])
        await late.send_str('not-json')
        self.assertEqual((await late.receive_json())['type'], 'error')
        await guest.send_json({"type":"leave"})
        self.assertEqual((await host.receive_json())['type'], 'peer_left')

    async def test_recover_host_and_guest_seats(self):
        host,guest=await self.connect(),await self.connect()
        created=await self.create(host)
        joined=await self.request(guest,{'type':'join','code':created['code'],'name':'Guest'})
        await host.receive_json()
        await self.ready_pair(host,guest)
        await host.send_json({'type':'state','state':{'phase':'draft','players':{'2':{'picks':[4,7]}},'marker':'preserved'}})
        await guest.receive_json()
        await guest.close()
        self.assertEqual((await host.receive_json())['type'],'peer_disconnected')
        restored=await self.connect()
        resume=await self.request(restored,{'type':'resume','code':created['code'],'resume_token':joined['resume_token']})
        self.assertEqual(resume['id'],2)
        self.assertEqual((await restored.receive_json())['state']['players']['2']['picks'],[4,7])
        self.assertEqual((await host.receive_json())['type'],'peer_reconnected')
        await host.close()
        self.assertEqual((await restored.receive_json())['type'],'peer_disconnected')
        paused=await self.request(restored,{'type':'action','action':'pick','pick':8})
        self.assertIn('paused',paused['message'])
        new_host=await self.connect()
        result=await self.request(new_host,{'type':'resume','code':created['code'],'resume_token':created['resume_token']})
        self.assertEqual(result['id'],1)
        self.assertEqual((await restored.receive_json())['type'],'peer_reconnected')
        impostor=await self.connect()
        result=await self.request(impostor,{'type':'resume','code':created['code'],'resume_token':'invalid'})
        self.assertEqual(result['type'],'error')

if __name__ == '__main__': unittest.main()
