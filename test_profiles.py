import asyncio,base64,io,json,os,tempfile
from pathlib import Path
from aiohttp import ClientSession,web
from PIL import Image
from server import create_app

async def main():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ['PROFILE_DB_PATH']=str(Path(tmp)/'profiles.db')
        runner=web.AppRunner(create_app());await runner.setup()
        site=web.TCPSite(runner,'127.0.0.1',0);await site.start()
        base='http://127.0.0.1:'+str(site._server.sockets[0].getsockname()[1])
        async with ClientSession() as client:
            async with client.post(base+'/api/profiles',json={'id':'Profile_Test'}) as response:
                assert response.status==201;profile=await response.json()
            token=profile['token'];headers={'Authorization':'Bearer '+token}
            async with client.post(base+'/api/profiles',json={'id':'PROFILE_TEST'}) as response: assert response.status==409
            async with client.post(base+'/api/profiles',json={'id':'bad id!'}) as response: assert response.status==400
            out=io.BytesIO();Image.new('RGB',(50,50),'blue').save(out,'PNG')
            image=base64.b64encode(out.getvalue()).decode()
            async with client.put(base+'/api/profiles/profile_test/avatar',json={'image':image}) as response: assert response.status==401
            async with client.put(base+'/api/profiles/profile_test/avatar',headers=headers,json={'image':image}) as response:
                assert response.status==200;assert (await response.json())['avatar']
            async with client.post(base+'/api/profiles/profile_test/restore',headers=headers,json={}) as response: assert response.status==200
            async with client.ws_connect(base+'/ws') as ws:
                await ws.send_json({'type':'create','name':'Profile Test Room','capacity':2,'budget':100,'profile_id':'profile_test','profile_token':token})
                created=await ws.receive_json();assert created['type']=='created'
                async with client.post(base+'/api/profiles',json={'id':'profile_guest'}) as response: guest_profile=await response.json()
                async with client.put(base+'/api/profiles/profile_guest/avatar',headers={'Authorization':'Bearer '+guest_profile['token']},json={'image':image}) as response: assert response.status==200
                async with client.ws_connect(base+'/ws') as guest:
                    await guest.send_json({'type':'join','code':created['code'],'profile_id':'profile_guest','profile_token':guest_profile['token']})
                    assert (await guest.receive_json())['type']=='joined'
                    joined=await ws.receive_json()
                    assert joined['type']=='peer_joined' and joined['avatar'] and joined['name']=='profile_guest'
                assert (await ws.receive_json())['type']=='peer_disconnected'
                data=json.loads(Path(__file__).with_name('market_catalog.json').read_text())
                roster=[];used=set()
                for index,(year,role) in enumerate(zip([2016,2017,2018,2020,2021,2022,2024],['Centre-Back','winger','Defensive Midfield','Goalkeeper','Centre-Forward','Attacking Midfield','Centre-Back'])):
                    for key,item in data.items():
                        pid,y=key.split('|')
                        if int(y)==year and item['role']==role and pid not in used:
                            roster.append({'id':index,'slot':index,'player_id':pid,'buy_year':year});used.add(pid);break
                expected=sum(data[p['player_id']+'|'+str(p['buy_year'])]['future']-data[p['player_id']+'|'+str(p['buy_year'])]['price'] for p in roster)
                await ws.send_json({'type':'state','state':{'phase':'results','match_id':'profile_test_001','players':{'1':{'picks':list(range(7))}},'roster':roster,'sales':[]}})
                await asyncio.sleep(0.5)
                async with client.get(base+'/api/leaderboard') as response:
                    entries=(await response.json())['entries'];assert entries[0]['id']=='profile_test';assert abs(entries[0]['profit']-expected)<0.000001
        await runner.cleanup()
        # Recreate the entire application; the profile and score must survive.
        app=create_app();runner=web.AppRunner(app);await runner.setup()
        assert app['profile_store'].auth('profile_test',token)
        rows=app['profile_store'].run([('SELECT profit FROM results WHERE profile_id=?',('profile_test',))])[0]
        assert len(rows)==1
        await runner.cleanup()
        print('PASS unique IDs, recovery, authenticated image upload, relay-derived score and restart persistence')

if __name__=='__main__':asyncio.run(main())
