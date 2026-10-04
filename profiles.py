"""Persistent profiles and best completed-match profits. Shared Postgres in production."""
import asyncio, base64, hashlib, hmac, io, json, os, re, secrets, sqlite3, threading, time
from pathlib import Path
from aiohttp import web
from PIL import Image

class Store:
    def __init__(self):
        self.url = os.environ.get('DATABASE_URL', '')
        self.lock = threading.RLock()
        self.path = os.environ.get('PROFILE_DB_PATH', 'profiles.sqlite3')
        self.ready = False
    def run(self, statements):
        with self.lock:
            if self.url:
                import psycopg
                conn = psycopg.connect(self.url, connect_timeout=12)
            else: conn = sqlite3.connect(self.path, timeout=12)
            try:
                results = []
                for sql, args in statements:
                    cur = conn.execute(sql.replace('?', '%s') if self.url else sql, args)
                    results.append(cur.fetchall() if cur.description else [])
                conn.commit()
                return results
            finally: conn.close()
    def initialize(self):
        self.run([('CREATE TABLE IF NOT EXISTS profiles (id TEXT PRIMARY KEY, token_hash TEXT NOT NULL, avatar TEXT NOT NULL DEFAULT \'\', created BIGINT NOT NULL)',()),
                  ('CREATE TABLE IF NOT EXISTS results (profile_id TEXT NOT NULL, match_id TEXT NOT NULL, profit BIGINT NOT NULL, created BIGINT NOT NULL, PRIMARY KEY(profile_id,match_id))',()),
                  ('CREATE INDEX IF NOT EXISTS profit_index ON results(profit DESC)',())])
        try: self.run([('ALTER TABLE results ADD COLUMN budget INTEGER',())])
        except Exception: pass
        self.ready = True
    def auth(self, uid, token):
        if not isinstance(uid,str) or not isinstance(token,str): return False
        rows = self.run([('SELECT token_hash FROM profiles WHERE id=?',(uid.lower(),))])[0]
        return bool(rows) and hmac.compare_digest(rows[0][0],hashlib.sha256(token.encode()).hexdigest())

def install(app):
    store = Store()
    app['profile_store'] = store
    rates = {}
    async def startup(app):
        await asyncio.to_thread(store.initialize)
    app.on_startup.append(startup)
    def public(uid):
        rows=store.run([('SELECT id,avatar FROM profiles WHERE id=?',(uid,))])[0]
        if not rows: return None
        return {'id':rows[0][0], 'avatar':rows[0][1]}
    def bearer(request):
        return request.headers.get('Authorization','').removeprefix('Bearer ')
    @web.middleware
    async def errors(request, handler):
        if not request.path.startswith('/api/'): return await handler(request)
        key=request.remote or 'unknown'; now=time.monotonic()
        bucket=[t for t in rates.get(key,[]) if now-t<60]
        if len(bucket)>=60: return web.json_response({'error':'Too many requests. Try in a minute.'},status=429)
        if len(rates)>10000: rates.clear()
        bucket.append(now);rates[key]=bucket
        try: return await handler(request)
        except web.HTTPException as exc: return web.json_response({'error':exc.reason},status=exc.status)
        except (ValueError,TypeError,KeyError): return web.json_response({'error':'Invalid request.'},status=400)
        except Exception: return web.json_response({'error':'Profile service temporarily unavailable.'},status=503)
    app.middlewares.append(errors)
    async def create(request):
	    # Render's filesystem is temporary; never silently reserve IDs there.
        if os.environ.get('RENDER') and not store.url:
            return web.json_response({'error':'Persistent profile storage is not configured yet.'},status=503)
        data=await request.json();uid=str(data.get('id','')).strip().lower()
        if not re.fullmatch(r'[a-z0-9_]{3,24}',uid):
            return web.json_response({'error':'Use 3–24 letters, numbers or underscores.'},status=400)
        token=secrets.token_urlsafe(32)
        try:
            await asyncio.to_thread(store.run,[('INSERT INTO profiles(id,token_hash,created) VALUES(?,?,?)',(uid,hashlib.sha256(token.encode()).hexdigest(),int(time.time())))])
        except Exception as exc:
            if await asyncio.to_thread(public,uid): return web.json_response({'error':'That ID is already taken.'},status=409)
            raise exc
        return web.json_response({'id':uid,'token':token,'avatar':''},status=201)
    async def profile(request):
        result=await asyncio.to_thread(public,request.match_info['uid'].lower())
        if not result: raise web.HTTPNotFound()
        return web.json_response(result)
    async def restore(request):
        uid=request.match_info['uid'].lower()
        if not await asyncio.to_thread(store.auth,uid,bearer(request)): raise web.HTTPUnauthorized()
        return web.json_response(await asyncio.to_thread(public,uid))
    def sanitize(raw):
        content=base64.b64decode(raw,validate=True)
        if len(content)>1000000: raise ValueError()
        with Image.open(io.BytesIO(content)) as img:
            if img.width*img.height>16000000: raise ValueError()
            img.thumbnail((256,256))
            clean=img.convert('RGB');out=io.BytesIO();clean.save(out,'JPEG',quality=85)
        return base64.b64encode(out.getvalue()).decode()
    async def avatar(request):
        uid=request.match_info['uid'].lower()
        if not await asyncio.to_thread(store.auth,uid,bearer(request)): raise web.HTTPUnauthorized()
        data=await request.json()
        try: photo=await asyncio.to_thread(sanitize,data['image'])
        except Exception: return web.json_response({'error':'Choose a valid JPG, PNG or WebP image.'},status=400)
        await asyncio.to_thread(store.run,[('UPDATE profiles SET avatar=? WHERE id=?',(photo,uid))])
        return web.json_response(await asyncio.to_thread(public,uid))
    async def leaderboard(request):
        rows=(await asyncio.to_thread(store.run,[('SELECT p.id,p.avatar,MAX(r.profit) AS best FROM profiles p JOIN results r ON p.id=r.profile_id WHERE r.budget=100 GROUP BY p.id,p.avatar ORDER BY best DESC,p.id ASC LIMIT 50',())]))[0]
        return web.json_response({'entries':[{'id':uid,'avatar':avatar,'profit':profit/1000000} for uid,avatar,profit in rows], 'metric':'Highest completed online-match net profit (€M)'})
    app.router.add_post('/api/profiles',create)
    app.router.add_get('/api/profiles/{uid}',profile)
    app.router.add_post('/api/profiles/{uid}/restore',restore)
    app.router.add_put('/api/profiles/{uid}/avatar',avatar)
    app.router.add_get('/api/leaderboard',leaderboard)
    return store

def record_results(store, clients, room, state):
    """Never accept a client-supplied score; derive it from dated archive prices."""
    if room.budget != 100: return
    catalog_path=Path(__file__).with_name('market_catalog.json')
    if not catalog_path.exists(): return
    catalog=json.loads(catalog_path.read_text(encoding='utf-8'))
    assets={int(asset['id']):asset for asset in state.get('roster',[])}
    match=str(state.get('match_id',''))
    if not re.fullmatch(r'[A-Za-z0-9_-]{8,100}',match): return
    buy_years={2016,2017,2018,2020,2021,2022,2024}
    def verified(asset):
        key=str(asset.get('player_id',''))+'|'+str(asset.get('buy_year',''))
        item=catalog.get(key)
        if not item: raise ValueError()
        return item
    for cid,client in clients.items():
        if not client.profile_id: continue
        try:
            person=state['players'].get(str(cid),state['players'].get(cid))
            picks=[assets[int(pid)] for pid in person['picks']]
            if len(picks)!=7 or sorted(int(p['slot']) for p in picks)!=list(range(7)): continue
            if any(int(p['buy_year']) not in buy_years | {2019,2025} for p in picks): continue
            if len({p['player_id'] for p in picks})!=7: continue
            roles=sorted(verified(p)['role'] for p in picks)
            if roles!=sorted(['Centre-Back','Centre-Back','winger','Goalkeeper','Defensive Midfield','Attacking Midfield','Centre-Forward']): continue
            profit=sum(verified(p)['future']-verified(p)['price'] for p in picks)
            for sale in state.get('sales',[]):
                if int(sale['owner'])!=cid: continue
                item=verified(assets[int(sale['pick'])]);year=str(sale['year'])
                profit+=item['windows'][year]-item['price']
            store.run([('INSERT INTO results(profile_id,match_id,profit,created,budget) VALUES(?,?,?,?,?) ON CONFLICT(profile_id,match_id) DO NOTHING',(client.profile_id,room.code+'-'+match,round(profit*1000000),int(time.time()),100))])
        except (ValueError,TypeError,KeyError): continue

