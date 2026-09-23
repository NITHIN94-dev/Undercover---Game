import asyncio, json, os, random, re, time
from pathlib import Path
from aiohttp import web, WSMsgType

ROOT = Path(__file__).parent
PUBLIC = ROOT / 'public'
rooms = {}

WORD_PAIRS = [
    ('BEACH','POOL'), ('COFFEE','TEA'), ('PIZZA','BURGER'), ('CAT','DOG'),
    ('SUMMER','WINTER'), ('MOVIE','SERIES'), ('MOUNTAIN','HILL'), ('PILLOW','BLANKET'),
    ('CAKE','ICE CREAM'), ('TRAIN','BUS'), ('SUN','MOON'), ('APPLE','ORANGE'),
    ('GUITAR','PIANO'), ('SCHOOL','COLLEGE'), ('THUNDER','LIGHTNING'), ('FOREST','JUNGLE'),
    ('SHOES','SANDALS'), ('RIVER','LAKE'), ('PENCIL','PEN'), ('DOCTOR','NURSE')
]
BLIND_WORDS = ['CAMPING','BIRTHDAY','AIRPORT','CHOCOLATE','ROBOT','CIRCUS','WEDDING','KITCHEN','OCEAN','LIBRARY']


def gen_code():
    chars='ABCDEFGHJKLMNPQRSTUVWXYZ23456789'
    while True:
        c=''.join(random.choice(chars) for _ in range(5))
        if c not in rooms: return c


def gen_id(): return ''.join(random.choice('abcdefghijklmnopqrstuvwxyz0123456789') for _ in range(9))


def player_view(p):
    return {'id':p['id'],'name':p['name'],'score':p['score'],'connected':not p.get('offline',False)}


def base_state(room):
    return {
        'type':'state','phase':room['phase'],'roomCode':room['code'],'hostId':room['hostId'],
        'players':[player_view(p) for p in room['players'].values()],
        'round':room['round'],'totalRounds':room['totalRounds'],'mode':room.get('mode'),
        'endsAt':room.get('endsAt'),'runoffIds':room.get('runoffIds',[]),'clueSubmitted':list(room['clues'].keys()),'votes':room['votes']
    }


def state_for(room,p):
    s=base_state(room)
    if room['phase']=='playing':
        sec=room['secret']
        if p['role']=='undercover':
            word=None if room['mode']=='blind' else sec['undercoverWord']
        else:
            word=sec['mainWord']
        s['secret']={'role':p['role'],'word':word,'blind':room['mode']=='blind'}
    if room['phase'] in ('voting','runoff','results'):
        s['clues']=room['clues'].copy()
    if room['phase']=='results': s['result']=room['result']
    return s


async def send(ws, data):
    if ws is not None and not ws.closed:
        try: await ws.send_json(data)
        except Exception: pass


async def sync(room):
    await asyncio.gather(*(send(p.get('ws'),state_for(room,p)) for p in room['players'].values()))


def clear_timer(room):
    t=room.get('timer')
    cur=asyncio.current_task()
    if t and t is not cur and not t.done(): t.cancel()
    room['timer']=None


def create_room():
    code=gen_code()
    r={'code':code,'hostId':None,'players':{},'phase':'lobby','round':0,'totalRounds':5,'mode':None,'secret':None,
       'clues':{},'votes':{},'result':None,'runoffCount':0,'runoffIds':[],'endsAt':None,'timer':None}
    rooms[code]=r
    return r


def add_player(room, name, ws, is_host=False):
    pid=gen_id(); p={'id':pid,'name':(name or 'Player').strip()[:18] or 'Player','ws':ws,'score':0,'role':'innocent','clue':None,'vote':None,'offline':False}
    room['players'][pid]=p
    if is_host: room['hostId']=pid
    return p


def setup_round(room):
    clear_timer(room)
    if room['round'] >= room['totalRounds']:
        room['phase']='gameover'; room['endsAt']=None; return
    room['round']+=1
    blind=(room['round']%2==0)
    room['mode']='blind' if blind else 'related'
    if blind: room['secret']={'mainWord':random.choice(BLIND_WORDS),'undercoverWord':None}
    else:
        a,b=random.choice(WORD_PAIRS); room['secret']={'mainWord':a,'undercoverWord':b}
    ps=list(room['players'].values())
    for p in ps:
        p['role']='innocent'; p['clue']=None; p['vote']=None
    if ps: random.choice(ps)['role']='undercover'
    room['clues']={}; room['votes']={}; room['result']=None; room['runoffCount']=0; room['runoffIds']=[]
    room['phase']='playing'; room['endsAt']=int(time.time()*1000)+30000
    room['timer']=asyncio.create_task(phase_after(room,30,'playing'))


async def phase_after(room, secs, expected_phase):
    try:
        await asyncio.sleep(secs)
        if room['phase'] != expected_phase: return
        if expected_phase=='playing': await finish_clues(room)
        elif expected_phase in ('voting','runoff'): await finish_voting(room)
        elif expected_phase=='results': await next_or_end(room)
    except asyncio.CancelledError: pass


async def finish_clues(room):
    if room['phase']!='playing': return
    clear_timer(room); room['phase']='voting'; room['endsAt']=int(time.time()*1000)+30000
    room['timer']=asyncio.create_task(phase_after(room,30,'voting')); await sync(room)


def vote_counts(room, only=None):
    c={}
    for v in room['votes'].values():
        if only is None or v in only: c[v]=c.get(v,0)+1
    return c


def leaders(room, only=None):
    counts=vote_counts(room,only)
    if not counts: return []
    mx=max(counts.values()); return [pid for pid,n in counts.items() if n==mx]


async def finish_voting(room):
    if room['phase'] not in ('voting','runoff'): return
    clear_timer(room)
    if room['phase']=='voting':
        tops=leaders(room)
        if len(tops)>1:
            room['phase']='runoff'; room['runoffCount']=1; room['runoffIds']=tops; room['votes']={}
            for p in room['players'].values(): p['vote']=None
            room['endsAt']=int(time.time()*1000)+20000
            room['timer']=asyncio.create_task(phase_after(room,20,'runoff')); await sync(room); return
        await resolve_vote(room,tops[0] if len(tops)==1 else None)
    else:
        tops=leaders(room,room['runoffIds'])
        await resolve_vote(room,tops[0] if len(tops)==1 else None)


async def resolve_vote(room, accused):
    room['phase']='results'; room['endsAt']=int(time.time()*1000)+12000
    undercover=next((p for p in room['players'].values() if p['role']=='undercover'),None)
    caught=bool(accused and undercover and accused==undercover['id'])
    if caught:
        for p in room['players'].values():
            if p['id']!=undercover['id'] and p.get('vote')==undercover['id']:
                p['score']+=2
    else:
        if undercover: undercover['score']+=4
    room['result']={'undercoverId':undercover['id'] if undercover else None,'caught':caught,
                    'mainWord':room['secret']['mainWord'],'undercoverWord':room['secret']['undercoverWord'],
                    'accusedId':accused,'runoff':room['runoffCount']>0,'wordBonus':0}
    room['timer']=asyncio.create_task(phase_after(room,12,'results')); await sync(room)


async def next_or_end(room):
    if room['round']>=room['totalRounds']:
        room['phase']='gameover'; room['endsAt']=None; await sync(room)
    else:
        setup_round(room); await sync(room)


async def handler(request):
    ws=web.WebSocketResponse(heartbeat=20); await ws.prepare(request)
    player=None; room=None
    async for msg in ws:
        if msg.type!=WSMsgType.TEXT: continue
        try: m=json.loads(msg.data)
        except Exception: continue
        typ=m.get('type')
        if typ=='create':
            room=create_room(); player=add_player(room,m.get('name'),ws,True)
            await send(ws,{'type':'joined','playerId':player['id'],'roomCode':room['code']}); await sync(room)
        elif typ=='join':
            code=str(m.get('roomCode','')).upper(); room=rooms.get(code)
            if not room: await send(ws,{'type':'error','message':'Room not found.'}); continue
            if room['phase']!='lobby': await send(ws,{'type':'error','message':'That game has already started.'}); continue
            if len(room['players'])>=8: await send(ws,{'type':'error','message':'Room is full.'}); continue
            player=add_player(room,m.get('name'),ws,False)
            await send(ws,{'type':'joined','playerId':player['id'],'roomCode':room['code']}); await sync(room)
        elif room and player:
            if typ=='setRounds' and player['id']==room['hostId'] and room['phase']=='lobby':
                room['totalRounds']=int(m.get('rounds')) if int(m.get('rounds',5)) in (3,5,10) else 5; await sync(room)
            elif typ=='start' and player['id']==room['hostId'] and room['phase']=='lobby':
                if len(room['players'])<2: await send(ws,{'type':'error','message':'Need at least 2 players to start.'}); continue
                setup_round(room); await sync(room)
            elif typ=='clue' and room['phase']=='playing' and player['id'] not in room['clues']:
                raw=str(m.get('clue','')).strip(); first=re.split(r'\s+',raw)[0]
                clue=re.sub(r'[^A-Za-z0-9\'\-]','',first)[:24]
                if not clue: continue
                room['clues'][player['id']]=clue; player['clue']=clue; await sync(room)
                if len(room['clues'])==len(room['players']): await finish_clues(room)
            elif typ=='vote' and room['phase'] in ('voting','runoff'):
                target=str(m.get('targetId',''))
                if target==player['id'] or target not in room['players']: continue
                if room['phase']=='runoff' and target not in room['runoffIds']: continue
                player['vote']=target; room['votes'][player['id']]=target; await sync(room)
                eligible_voters=max(0,len(room['players'])-1)
                if len(room['votes'])>=eligible_voters: await finish_voting(room)
            elif typ=='guess' and room['phase']=='results' and player['role']=='undercover' and room['result'] and not room['result']['caught'] and room['result']['wordBonus']==0:
                if str(m.get('word','')).strip().upper()==room['secret']['mainWord']:
                    player['score']+=1; room['result']['wordBonus']=1; await sync(room)
            elif typ=='restart' and player['id']==room['hostId'] and room['phase']=='gameover':
                for p in room['players'].values(): p['score']=0
                room['round']=0; setup_round(room); await sync(room)
    if room and player:
        player['offline']=True; await sync(room)
    return ws

app=web.Application()
app.router.add_get('/ws',handler)
async def index(request): return web.FileResponse(PUBLIC/'index.html')
app.router.add_get('/', index)
app.router.add_static('/assets', path=PUBLIC, show_index=False)


if __name__=='__main__':
    web.run_app(app,host='0.0.0.0',port=int(os.environ.get('PORT','3001')))
