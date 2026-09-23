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


# Frontend is embedded directly in this server so deployment does not depend on a public/ folder.
_INDEX_HTML = __import__('base64').b64decode("PCFkb2N0eXBlIGh0bWw+PGh0bWw+PGhlYWQ+PG1ldGEgY2hhcnNldD0idXRmLTgiPjxtZXRhIG5hbWU9InZpZXdwb3J0IiBjb250ZW50PSJ3aWR0aD1kZXZpY2Utd2lkdGgsaW5pdGlhbC1zY2FsZT0xIj48bWV0YSBuYW1lPSJ0aGVtZS1jb2xvciIgY29udGVudD0iIzBiMGExMiI+PHRpdGxlPldobydzIHRoZSBVbmRlcmNvdmVyPzwvdGl0bGU+PGxpbmsgcmVsPSJzdHlsZXNoZWV0IiBocmVmPSIvYXNzZXRzL3N0eWxlLmNzcyI+PC9oZWFkPjxib2R5PjxkaXYgaWQ9ImFwcCI+PC9kaXY+PHNjcmlwdCBzcmM9Ii9hc3NldHMvYXBwLmpzIj48L3NjcmlwdD48L2JvZHk+PC9odG1sPgo=").decode('utf-8')
_APP_JS = __import__('base64').b64decode("Y29uc3QgYXBwPWRvY3VtZW50LmdldEVsZW1lbnRCeUlkKCdhcHAnKTsKbGV0IHdzPW51bGwsc3RhdGU9bnVsbCxwbGF5ZXJJZD1udWxsLGNvbm5lY3RlZE9uY2U9ZmFsc2Usc2VsZWN0ZWRWb3RlPW51bGwsZ3Vlc3NTZW50PWZhbHNlOwpjb25zdCBlc2M9cz0+U3RyaW5nKHM/PycnKS5yZXBsYWNlKC9bJjw+IiddL2csYz0+KHsnJic6JyZhbXA7JywnPCc6JyZsdDsnLCc+JzonJmd0OycsJyInOicmcXVvdDsnLCInIjonJiMzOTsnfVtjXSkpOwpmdW5jdGlvbiBjb25uZWN0KCl7aWYod3MgJiYgKHdzLnJlYWR5U3RhdGU9PT0wfHx3cy5yZWFkeVN0YXRlPT09MSkpcmV0dXJuOyB3cz1uZXcgV2ViU29ja2V0KGAke2xvY2F0aW9uLnByb3RvY29sPT09J2h0dHBzOic/J3dzcyc6J3dzJ306Ly8ke2xvY2F0aW9uLmhvc3R9L3dzYCk7IHdzLm9ub3Blbj0oKT0+e2Nvbm5lY3RlZE9uY2U9dHJ1ZTt9OyB3cy5vbm1lc3NhZ2U9ZT0+e2NvbnN0IG09SlNPTi5wYXJzZShlLmRhdGEpOyBpZihtLnR5cGU9PT0nam9pbmVkJyl7cGxheWVySWQ9bS5wbGF5ZXJJZDt9IGVsc2UgaWYobS50eXBlPT09J2Vycm9yJykgYWxlcnQobS5tZXNzYWdlKTsgZWxzZSBpZihtLnR5cGU9PT0nc3RhdGUnKXtzdGF0ZT1tOyByZW5kZXIoKTt9fTsgd3Mub25jbG9zZT0oKT0+e3NldFRpbWVvdXQoY29ubmVjdCwxMDAwKX07fQpmdW5jdGlvbiBzZW5kKG0pe2lmKHdzPy5yZWFkeVN0YXRlPT09MSl3cy5zZW5kKEpTT04uc3RyaW5naWZ5KG0pKTt9CmZ1bmN0aW9uIHNoZWxsKGNvbnRlbnQpe2FwcC5pbm5lckhUTUw9YDxkaXYgY2xhc3M9IndyYXAiPjxkaXYgY2xhc3M9InJvdyIgc3R5bGU9Imp1c3RpZnktY29udGVudDpzcGFjZS1iZXR3ZWVuO21hcmdpbi1ib3R0b206MTRweCI+PGRpdj48ZGl2IGNsYXNzPSJzbWFsbCBtdXRlZCI+V0hPJ1MgVEhFIFVOREVSQ09WRVI/PC9kaXY+JHtzdGF0ZT9gPGRpdiBjbGFzcz0ic21hbGwgbXV0ZWQiPlJvb20gJHtlc2Moc3RhdGUucm9vbUNvZGUpfTwvZGl2PmA6Jyd9PC9kaXY+JHtzdGF0ZT9gPGRpdiBjbGFzcz0ic21hbGwgbXV0ZWQiPlJvdW5kICR7c3RhdGUucm91bmR9LyR7c3RhdGUudG90YWxSb3VuZHN9PC9kaXY+YDonJ308L2Rpdj4ke2NvbnRlbnR9PC9kaXY+YH0KZnVuY3Rpb24gcmVuZGVyKCl7aWYoIXN0YXRlKXtob21lKCk7cmV0dXJufWlmKHN0YXRlLnBoYXNlPT09J2xvYmJ5Jylsb2JieSgpO2Vsc2UgaWYoc3RhdGUucGhhc2U9PT0ncGxheWluZycpcGxheWluZygpO2Vsc2UgaWYoc3RhdGUucGhhc2U9PT0ndm90aW5nJ3x8c3RhdGUucGhhc2U9PT0ncnVub2ZmJyl2b3RpbmcoKTtlbHNlIGlmKHN0YXRlLnBoYXNlPT09J3Jlc3VsdHMnKXJlc3VsdHMoKTtlbHNlIGdhbWVvdmVyKCk7fQpmdW5jdGlvbiBob21lKCl7Y29ubmVjdCgpO2FwcC5pbm5lckhUTUw9YDxkaXYgY2xhc3M9IndyYXAiPjxkaXYgY2xhc3M9ImNhcmQgZ3JpZCIgc3R5bGU9Im1hcmdpbi10b3A6OHZoIj48ZGl2IGNsYXNzPSJicmFuZCI+V2hvJ3MgdGhlPGJyPlVuZGVyY292ZXI/PC9kaXY+PHAgY2xhc3M9Im11dGVkIj5BIGxpdmUgYmx1ZmYtYW5kLWRlZHVjdGlvbiBwYXJ0eSBnYW1lIGZvciAy4oCTOCBwZW9wbGUgb24gc2VwYXJhdGUgZGV2aWNlcy48L3A+PGRpdiBjbGFzcz0iZ3JpZCB0d28iPjxkaXYgY2xhc3M9ImNhcmQiIHN0eWxlPSJwYWRkaW5nOjE2cHgiPjxoMz5DcmVhdGUgYSByb29tPC9oMz48aW5wdXQgaWQ9Im5hbWUiIHBsYWNlaG9sZGVyPSJZb3VyIG5hbWUiIG1heGxlbmd0aD0iMTgiPjxidXR0b24gY2xhc3M9ImJ0biIgc3R5bGU9IndpZHRoOjEwMCU7bWFyZ2luLXRvcDoxMHB4IiBvbmNsaWNrPSJjcmVhdGUoKSI+Q3JlYXRlIHJvb208L2J1dHRvbj48L2Rpdj48ZGl2IGNsYXNzPSJjYXJkIiBzdHlsZT0icGFkZGluZzoxNnB4Ij48aDM+Sm9pbiBhIHJvb208L2gzPjxpbnB1dCBpZD0iam9pbk5hbWUiIHBsYWNlaG9sZGVyPSJZb3VyIG5hbWUiIG1heGxlbmd0aD0iMTgiPjxpbnB1dCBpZD0iam9pbkNvZGUiIHBsYWNlaG9sZGVyPSJSb29tIGNvZGUiIG1heGxlbmd0aD0iNSIgc3R5bGU9Im1hcmdpbi10b3A6MTBweDt0ZXh0LXRyYW5zZm9ybTp1cHBlcmNhc2UiPjxidXR0b24gY2xhc3M9ImJ0biBzZWNvbmRhcnkiIHN0eWxlPSJ3aWR0aDoxMDAlO21hcmdpbi10b3A6MTBweCIgb25jbGljaz0iam9pbigpIj5Kb2luIHJvb208L2J1dHRvbj48L2Rpdj48L2Rpdj48ZGl2IGNsYXNzPSJtdXRlZCBzbWFsbCI+UnVsZXM6IG9uZS13b3JkIGNsdWVzIOKGkiBwdWJsaWMgdm90ZSDihpIgcmV2ZWFsLiBVbmRlcmNvdmVyIHJvdW5kcyBhbHRlcm5hdGUgYmV0d2VlbiByZWxhdGVkLXdvcmQgYW5kIGJsaW5kIG1vZGUuPC9kaXY+PC9kaXY+PC9kaXY+YH0KZnVuY3Rpb24gY3JlYXRlKCl7Y29uc3Qgbj1kb2N1bWVudC5nZXRFbGVtZW50QnlJZCgnbmFtZScpLnZhbHVlLnRyaW0oKTtpZighbilyZXR1cm4gYWxlcnQoJ0VudGVyIHlvdXIgbmFtZS4nKTtzZW5kKHt0eXBlOidjcmVhdGUnLG5hbWU6bn0pO30KZnVuY3Rpb24gam9pbigpe2NvbnN0IG49ZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoJ2pvaW5OYW1lJykudmFsdWUudHJpbSgpLGM9ZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoJ2pvaW5Db2RlJykudmFsdWUudHJpbSgpLnRvVXBwZXJDYXNlKCk7aWYoIW58fCFjKXJldHVybiBhbGVydCgnRW50ZXIgeW91ciBuYW1lIGFuZCByb29tIGNvZGUuJyk7c2VuZCh7dHlwZTonam9pbicsbmFtZTpuLHJvb21Db2RlOmN9KTt9CmZ1bmN0aW9uIGxvYmJ5KCl7Y29uc3QgaG9zdD1wbGF5ZXJJZD09PXN0YXRlLmhvc3RJZDtzaGVsbChgPGRpdiBjbGFzcz0iZ3JpZCB0d28iPjxkaXYgY2xhc3M9ImNhcmQgZ3JpZCI+PGRpdj48ZGl2IGNsYXNzPSJzbWFsbCBtdXRlZCI+Uk9PTSBDT0RFPC9kaXY+PGRpdiBjbGFzcz0iY29kZSI+JHtlc2Moc3RhdGUucm9vbUNvZGUpfTwvZGl2PjwvZGl2PjxkaXYgY2xhc3M9Im11dGVkIj5TaGFyZSB0aGlzIGNvZGUgd2l0aCBmcmllbmRzLiAke3N0YXRlLnBsYXllcnMubGVuZ3RofS84IHBsYXllcnMuPC9kaXY+JHtob3N0P2A8ZGl2PjxkaXYgY2xhc3M9InNtYWxsIG11dGVkIj5ST1VORFM8L2Rpdj48c2VsZWN0IGlkPSJyb3VuZHMiIG9uY2hhbmdlPSJzZW5kKHt0eXBlOidzZXRSb3VuZHMnLHJvdW5kczpOdW1iZXIodGhpcy52YWx1ZSl9KSI+PG9wdGlvbiB2YWx1ZT0iMyIgJHtzdGF0ZS50b3RhbFJvdW5kcz09PTM/J3NlbGVjdGVkJzonJ30+MyByb3VuZHM8L29wdGlvbj48b3B0aW9uIHZhbHVlPSI1IiAke3N0YXRlLnRvdGFsUm91bmRzPT09NT8nc2VsZWN0ZWQnOicnfT41IHJvdW5kczwvb3B0aW9uPjxvcHRpb24gdmFsdWU9IjEwIiAke3N0YXRlLnRvdGFsUm91bmRzPT09MTA/J3NlbGVjdGVkJzonJ30+MTAgcm91bmRzPC9vcHRpb24+PC9zZWxlY3Q+PC9kaXY+PGJ1dHRvbiBjbGFzcz0iYnRuIiBvbmNsaWNrPSJzZW5kKHt0eXBlOidzdGFydCd9KSIgJHtzdGF0ZS5wbGF5ZXJzLmxlbmd0aDwyPydkaXNhYmxlZCc6Jyd9PlN0YXJ0IGdhbWU8L2J1dHRvbj5gOmA8ZGl2IGNsYXNzPSJwaWxsIj5XYWl0aW5nIGZvciB0aGUgaG9zdCB0byBzdGFydOKApjwvZGl2PmB9PC9kaXY+PGRpdiBjbGFzcz0iY2FyZCI+PGgzPlBsYXllcnM8L2gzPjxkaXYgY2xhc3M9InBsYXllcnMiPiR7c3RhdGUucGxheWVycy5tYXAocD0+YDxkaXYgY2xhc3M9InBpbGwiPjxiPiR7ZXNjKHAubmFtZSl9PC9iPjxkaXYgY2xhc3M9InNtYWxsIG11dGVkIj4ke3AuaWQ9PT1zdGF0ZS5ob3N0SWQ/J0hvc3QgwrcgJzonJ30ke3Auc2NvcmV9IHB0czwvZGl2PjwvZGl2PmApLmpvaW4oJycpfTwvZGl2PjxociBzdHlsZT0iYm9yZGVyLWNvbG9yOiMyYjMxM2M7bWFyZ2luOjE4cHggMCI+PGgzPlJ1bGVzPC9oMz48cCBjbGFzcz0ic21hbGwgbXV0ZWQiPkV2ZXJ5b25lIGdldHMgYSBzZWNyZXQgd29yZC4gVGhlIFVuZGVyY292ZXIgZWl0aGVyIGdldHMgYSByZWxhdGVkIHdvcmQgb3Igbm8gd29yZCBhdCBhbGwuIFN1Ym1pdCBvbmUgY2x1ZSBhdCB0aGUgc2FtZSB0aW1lLCB0aGVuIHZvdGUgcHVibGljbHkuIENvcnJlY3Qgdm90ZXJzIGdldCAyIHBvaW50czsgYSBzdXJ2aXZpbmcgVW5kZXJjb3ZlciBnZXRzIDQsIHBsdXMgMSBmb3IgZ3Vlc3NpbmcgdGhlIG1haW4gd29yZC48L3A+PC9kaXY+PC9kaXY+YCl9CmZ1bmN0aW9uIGNvdW50ZG93bigpe3JldHVybiBzdGF0ZT8uZW5kc0F0P01hdGgubWF4KDAsTWF0aC5jZWlsKChzdGF0ZS5lbmRzQXQtRGF0ZS5ub3coKSkvMTAwMCkpOjB9CnNldEludGVydmFsKCgpPT57Y29uc3QgdD1kb2N1bWVudC5xdWVyeVNlbGVjdG9yKCcudGltZXInKTtpZih0ICYmIHN0YXRlICYmIFsncGxheWluZycsJ3ZvdGluZycsJ3J1bm9mZicsJ3Jlc3VsdHMnXS5pbmNsdWRlcyhzdGF0ZS5waGFzZSkpdC50ZXh0Q29udGVudD1jb3VudGRvd24oKTt9LDI1MCk7CmZ1bmN0aW9uIHBsYXlpbmcoKXtjb25zdCBzPXN0YXRlLnNlY3JldCxzZW50PXN0YXRlLmNsdWVTdWJtaXR0ZWQuaW5jbHVkZXMocGxheWVySWQpO3NoZWxsKGA8ZGl2IGNsYXNzPSJncmlkIHR3byI+PGRpdiBjbGFzcz0iY2FyZCBncmlkIj48ZGl2IGNsYXNzPSJyb3ciIHN0eWxlPSJqdXN0aWZ5LWNvbnRlbnQ6c3BhY2UtYmV0d2VlbiI+PHNwYW4gY2xhc3M9InBpbGwiPlJvdW5kICR7c3RhdGUucm91bmR9PC9zcGFuPjxzcGFuIGNsYXNzPSJwaWxsIj4ke3N0YXRlLm1vZGU9PT0nYmxpbmQnPydCTElORCBST1VORCc6J1JFTEFURUQtV09SRCBST1VORCd9PC9zcGFuPjwvZGl2PjxoMj5Zb3VyIHNlY3JldDwvaDI+PGRpdiBjbGFzcz0id29yZCI+JHtzPy53b3JkP2VzYyhzLndvcmQpOidZT1UgQVJFIFVOREVSQ09WRVInfTwvZGl2PjxwIGNsYXNzPSJtdXRlZCBzbWFsbCI+JHtzPy5ibGluZCYmcz8ud29yZD09PW51bGw/J1lvdSBoYXZlIG5vIHdvcmQuIEluZmVyIHRoZSB0b3BpYyBmcm9tIHRoZSBjbHVlcyB3aGVuIHZvdGluZy4nOnM/LnJvbGU9PT0ndW5kZXJjb3Zlcic/J1lvdSBoYXZlIHRoZSByZWxhdGVkIHdvcmQuIEJsZW5kIGluLic6J0dpdmUgYSBjbHVlIHRoYXQgZml0cyB5b3VyIHdvcmQgd2l0aG91dCBtYWtpbmcgaXQgdG9vIG9idmlvdXMuJ308L3A+PGRpdj48ZGl2IGNsYXNzPSJzbWFsbCBtdXRlZCI+Q2x1ZSB0aW1lcjwvZGl2PjxkaXYgY2xhc3M9InRpbWVyIj4ke2NvdW50ZG93bigpfTwvZGl2PjwvZGl2Pjxmb3JtIG9uc3VibWl0PSJzdWJtaXRDbHVlKGV2ZW50KSIgY2xhc3M9ImdyaWQiPjxpbnB1dCBpZD0iY2x1ZSIgbWF4bGVuZ3RoPSIyNCIgcGxhY2Vob2xkZXI9Ik9uZSB3b3JkIG9ubHkiICR7c2VudD8nZGlzYWJsZWQnOicnfT48YnV0dG9uIGNsYXNzPSJidG4iICR7c2VudD8nZGlzYWJsZWQnOicnfT4ke3NlbnQ/J0NsdWUgc3VibWl0dGVkJzonU3VibWl0IGNsdWUnfTwvYnV0dG9uPjwvZm9ybT48ZGl2IGNsYXNzPSJtdXRlZCBzbWFsbCI+U3VibWl0dGVkOiAke3N0YXRlLmNsdWVTdWJtaXR0ZWQubGVuZ3RofS8ke3N0YXRlLnBsYXllcnMubGVuZ3RofTwvZGl2PjwvZGl2PjxkaXYgY2xhc3M9ImNhcmQiPjxoMz5TY29yZWJvYXJkPC9oMz48ZGl2IGNsYXNzPSJncmlkIj4ke1suLi5zdGF0ZS5wbGF5ZXJzXS5zb3J0KChhLGIpPT5iLnNjb3JlLWEuc2NvcmUpLm1hcCgocCxpKT0+YDxkaXYgY2xhc3M9InJvdyIgc3R5bGU9Imp1c3RpZnktY29udGVudDpzcGFjZS1iZXR3ZWVuIj48c3Bhbj4ke2krMX0uICR7ZXNjKHAubmFtZSl9PC9zcGFuPjxiPiR7cC5zY29yZX08L2I+PC9kaXY+YCkuam9pbignJyl9PC9kaXY+PC9kaXY+PC9kaXY+YCl9CmZ1bmN0aW9uIHN1Ym1pdENsdWUoZSl7ZS5wcmV2ZW50RGVmYXVsdCgpO2NvbnN0IHY9ZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoJ2NsdWUnKS52YWx1ZS50cmltKCkuc3BsaXQoL1xzKy8pWzBdO2lmKHYpc2VuZCh7dHlwZTonY2x1ZScsY2x1ZTp2fSk7fQpmdW5jdGlvbiB2b3RpbmcoKXtjb25zdCBydW5vZmY9c3RhdGUucGhhc2U9PT0ncnVub2ZmJzsgc2hlbGwoYDxkaXYgY2xhc3M9ImdyaWQiPjxkaXYgY2xhc3M9ImNhcmQiPjxkaXYgY2xhc3M9InJvdyIgc3R5bGU9Imp1c3RpZnktY29udGVudDpzcGFjZS1iZXR3ZWVuIj48aDIgc3R5bGU9Im1hcmdpbjowIj4ke3J1bm9mZj8nUnVub2ZmIHZvdGUnOidXaG8gaXMgdGhlIFVuZGVyY292ZXI/J308L2gyPjxzcGFuIGNsYXNzPSJwaWxsIj4ke3J1bm9mZj8nVGllZCBwbGF5ZXJzIG9ubHknOicnfTwvc3Bhbj48L2Rpdj48cCBjbGFzcz0ibXV0ZWQgc21hbGwiPiR7cnVub2ZmPydPbmx5IHBsYXllcnMgdGllZCBmb3IgdGhlIHRvcCB2b3RlIGFyZSBlbGlnaWJsZS4nOidEaXNjdXNzIHRoZSBjbHVlcywgdGhlbiB2b3RlLiBWb3RlcyBhcmUgcHVibGljLid9PC9wPjxkaXYgY2xhc3M9InRpbWVyIj4ke2NvdW50ZG93bigpfTwvZGl2PjxkaXYgY2xhc3M9ImNsdWVzIiBzdHlsZT0ibWFyZ2luLXRvcDoxNHB4Ij4ke09iamVjdC5lbnRyaWVzKHN0YXRlLmNsdWVzfHx7fSkubWFwKChbaWQsY2x1ZV0pPT5gPGRpdiBjbGFzcz0iY2x1ZSI+PGRpdiBjbGFzcz0ic21hbGwgbXV0ZWQiPiR7ZXNjKHN0YXRlLnBsYXllcnMuZmluZChwPT5wLmlkPT09aWQpPy5uYW1lfHwnUGxheWVyJyl9PC9kaXY+PGI+JHtlc2MoY2x1ZSl9PC9iPjxkaXYgY2xhc3M9InNtYWxsIG11dGVkIj5Wb3RlczogJHtPYmplY3QudmFsdWVzKHN0YXRlLnZvdGVzfHx7fSkuZmlsdGVyKHg9Png9PT1pZCkubGVuZ3RofTwvZGl2PjwvZGl2PmApLmpvaW4oJycpfTwvZGl2PjxkaXYgY2xhc3M9ImdyaWQiIHN0eWxlPSJtYXJnaW4tdG9wOjE2cHgiPiR7c3RhdGUucGxheWVycy5maWx0ZXIocD0+cC5pZCE9PXBsYXllcklkICYmICghcnVub2ZmIHx8IHN0YXRlLnJ1bm9mZklkcy5pbmNsdWRlcyhwLmlkKSkpLm1hcChwPT5gPGJ1dHRvbiBjbGFzcz0idm90ZSAke3NlbGVjdGVkVm90ZT09PXAuaWQ/J3NlbGVjdGVkJzonJ30iIG9uY2xpY2s9InZvdGUoJyR7cC5pZH0nKSI+PHNwYW4+JHtlc2MocC5uYW1lKX08L3NwYW4+PHN0cm9uZz4ke09iamVjdC52YWx1ZXMoc3RhdGUudm90ZXN8fHt9KS5maWx0ZXIoeD0+eD09PXAuaWQpLmxlbmd0aH08L3N0cm9uZz48L2J1dHRvbj5gKS5qb2luKCcnKX08L2Rpdj4ke3J1bm9mZj9gPGRpdiBjbGFzcz0ibXV0ZWQgc21hbGwiPlJ1bm9mZiBjYW5kaWRhdGVzOiAke3N0YXRlLnBsYXllcnMuZmlsdGVyKHA9PnN0YXRlLnJ1bm9mZklkcy5pbmNsdWRlcyhwLmlkKSkubWFwKHA9PmVzYyhwLm5hbWUpKS5qb2luKCcsICcpfTwvZGl2PmA6Jyd9PGRpdiBjbGFzcz0ic21hbGwgbXV0ZWQiIHN0eWxlPSJtYXJnaW4tdG9wOjEwcHgiPkNob29zZSBvbmUgcGxheWVyLjwvZGl2PjwvZGl2PjwvZGl2PmApfQpmdW5jdGlvbiB2b3RlKGlkKXtzZWxlY3RlZFZvdGU9aWQ7c2VuZCh7dHlwZTondm90ZScsdGFyZ2V0SWQ6aWR9KTt9CmZ1bmN0aW9uIHJlc3VsdHMoKXtjb25zdCByPXN0YXRlLnJlc3VsdCxtZT1zdGF0ZS5wbGF5ZXJzLmZpbmQocD0+cC5pZD09PXBsYXllcklkKSx1bmRlcj1zdGF0ZS5wbGF5ZXJzLmZpbmQocD0+cC5pZD09PXIudW5kZXJjb3ZlcklkKTtjb25zdCBjYW5HdWVzcz1tZT8uaWQ9PT1yLnVuZGVyY292ZXJJZCYmIXIuY2F1Z2h0JiYhZ3Vlc3NTZW50O3NoZWxsKGA8ZGl2IGNsYXNzPSJncmlkIHR3byI+PGRpdiBjbGFzcz0iY2FyZCBncmlkIj48ZGl2IGNsYXNzPSJwaWxsIj5Sb3VuZCByZXN1bHQ8L2Rpdj48aDI+JHtyLmNhdWdodD8nVGhlIFVuZGVyY292ZXIgd2FzIGNhdWdodCEnOidUaGUgVW5kZXJjb3ZlciBzdXJ2aXZlZCEnfTwvaDI+PGRpdiBjbGFzcz0id29yZCI+JHtlc2Moci5tYWluV29yZCl9PC9kaXY+PHAgY2xhc3M9Im11dGVkIj5VbmRlcmNvdmVyOiA8Yj4ke2VzYyh1bmRlcj8ubmFtZXx8JycpfTwvYj48L3A+JHtyLnVuZGVyY292ZXJXb3JkP2A8cCBjbGFzcz0ibXV0ZWQiPlRoZWlyIHJlbGF0ZWQgd29yZDogPGI+JHtlc2Moci51bmRlcmNvdmVyV29yZCl9PC9iPjwvcD5gOic8cCBjbGFzcz0ibXV0ZWQiPlRoaXMgd2FzIGEgYmxpbmQgcm91bmQuPC9wPid9PGRpdiBjbGFzcz0iY2x1ZXMiPiR7T2JqZWN0LmVudHJpZXMoc3RhdGUuY2x1ZXN8fHt9KS5tYXAoKFtpZCxjbHVlXSk9PmA8ZGl2IGNsYXNzPSJjbHVlIj48ZGl2IGNsYXNzPSJzbWFsbCBtdXRlZCI+JHtlc2Moc3RhdGUucGxheWVycy5maW5kKHA9PnAuaWQ9PT1pZCk/Lm5hbWV8fCdQbGF5ZXInKX08L2Rpdj48Yj4ke2VzYyhjbHVlKX08L2I+PC9kaXY+YCkuam9pbignJyl9PC9kaXY+JHtjYW5HdWVzcz9gPGRpdiBjbGFzcz0iZ3JpZCI+PHAgY2xhc3M9Im9rIj5Zb3Ugc3Vydml2ZWQuIEd1ZXNzIHRoZSBtYWluIHdvcmQgZm9yICsxIHBvaW50LjwvcD48ZGl2IGNsYXNzPSJyb3ciPjxpbnB1dCBpZD0iZ3Vlc3MiIHBsYWNlaG9sZGVyPSJNYWluIHdvcmQiPjxidXR0b24gY2xhc3M9ImJ0biIgb25jbGljaz0iZ3Vlc3NXb3JkKCkiPkd1ZXNzPC9idXR0b24+PC9kaXY+PC9kaXY+YDonJ308L2Rpdj48ZGl2IGNsYXNzPSJjYXJkIj48aDM+U2NvcmVib2FyZDwvaDM+JHtbLi4uc3RhdGUucGxheWVyc10uc29ydCgoYSxiKT0+Yi5zY29yZS1hLnNjb3JlKS5tYXAocD0+YDxkaXYgY2xhc3M9InJvdyIgc3R5bGU9Imp1c3RpZnktY29udGVudDpzcGFjZS1iZXR3ZWVuO3BhZGRpbmc6OXB4IDA7Ym9yZGVyLWJvdHRvbToxcHggc29saWQgIzI2MmMzNyI+PHNwYW4+JHtlc2MocC5uYW1lKX08L3NwYW4+PGI+JHtwLnNjb3JlfTwvYj48L2Rpdj5gKS5qb2luKCcnKX08L2Rpdj48L2Rpdj5gKX0KZnVuY3Rpb24gZ3Vlc3NXb3JkKCl7Y29uc3Qgdj1kb2N1bWVudC5nZXRFbGVtZW50QnlJZCgnZ3Vlc3MnKS52YWx1ZS50cmltKCk7aWYoIXYpcmV0dXJuO2d1ZXNzU2VudD10cnVlO3NlbmQoe3R5cGU6J2d1ZXNzJyx3b3JkOnZ9KTt9CmZ1bmN0aW9uIGdhbWVvdmVyKCl7Y29uc3Qgc29ydGVkPVsuLi5zdGF0ZS5wbGF5ZXJzXS5zb3J0KChhLGIpPT5iLnNjb3JlLWEuc2NvcmUpO3NoZWxsKGA8ZGl2IGNsYXNzPSJncmlkIHR3byI+PGRpdiBjbGFzcz0iY2FyZCI+PGRpdiBjbGFzcz0icGlsbCI+R2FtZSBvdmVyPC9kaXY+PGgxPkZpbmFsIHNjb3JlczwvaDE+PGRpdiBjbGFzcz0iZ3JpZCI+JHtzb3J0ZWQubWFwKChwLGkpPT5gPGRpdiBjbGFzcz0icm93IiBzdHlsZT0ianVzdGlmeS1jb250ZW50OnNwYWNlLWJldHdlZW47Zm9udC1zaXplOjE4cHgiPjxzcGFuPiR7aSsxfS4gJHtlc2MocC5uYW1lKX08L3NwYW4+PGI+JHtwLnNjb3JlfSBwdHM8L2I+PC9kaXY+YCkuam9pbignJyl9PC9kaXY+JHtwbGF5ZXJJZD09PXN0YXRlLmhvc3RJZD9gPGJ1dHRvbiBjbGFzcz0iYnRuIiBzdHlsZT0ibWFyZ2luLXRvcDoxNnB4O3dpZHRoOjEwMCUiIG9uY2xpY2s9InNlbmQoe3R5cGU6J3Jlc3RhcnQnfSkiPlBsYXkgYWdhaW48L2J1dHRvbj5gOicnfTwvZGl2PjxkaXYgY2xhc3M9ImNhcmQiPjxoMz5Ib3cgeW91IHBsYXllZDwvaDM+PHAgY2xhc3M9Im11dGVkIj5UcnVzdCB5b3VyIGluc3RpbmN0cywga2VlcCBjbHVlcyBzdWJ0bGUsIGFuZCB3YXRjaCB0aGUgcHVibGljIHZvdGUuIEV2ZXJ5IHBvaW50IGNvbWVzIGZyb20gdGhlIGNob2ljZXMgcGxheWVycyBtYWtlLjwvcD48L2Rpdj48L2Rpdj5gKX0KY29ubmVjdCgpO3JlbmRlcigpOwo=").decode('utf-8')
_STYLE_CSS = __import__('base64').b64decode("OnJvb3R7CiAgLS1iZzojMGIwYTEyOwogIC0tYmctMjojMTIwZDFkOwogIC0tcGFuZWw6IzE3MTIyMjsKICAtLXBhbmVsLTI6IzFlMTcyYzsKICAtLWxpbmU6IzNhMmY0YjsKICAtLWxpbmUtc3Ryb25nOiM1MTQxNjc7CiAgLS10ZXh0OiNmZmZhZjI7CiAgLS1tdXRlZDojYjhhZWM0OwogIC0tbXV0ZWQtMjojOGQ4MTljOwogIC0tYWNjZW50OiNiNzg2ZmY7CiAgLS1hY2NlbnQtMjojZTVjOWZmOwogIC0td2FybTojZmZkMTY2OwogIC0tb2s6IzY1ZTZhZDsKICAtLWRhbmdlcjojZmY3ZjllOwogIGZvbnQtZmFtaWx5OkludGVyLHVpLXNhbnMtc2VyaWYsc3lzdGVtLXVpLC1hcHBsZS1zeXN0ZW0sQmxpbmtNYWNTeXN0ZW1Gb250LCJTZWdvZSBVSSIsc2Fucy1zZXJpZjsKICBjb2xvcjp2YXIoLS10ZXh0KTsKICBiYWNrZ3JvdW5kOnZhcigtLWJnKTsKfQoKKntib3gtc2l6aW5nOmJvcmRlci1ib3h9Cmh0bWx7bWluLXdpZHRoOjMyMHB4O2JhY2tncm91bmQ6dmFyKC0tYmcpfQpib2R5ewogIG1hcmdpbjowOwogIG1pbi1oZWlnaHQ6MTAwdmg7CiAgY29sb3I6dmFyKC0tdGV4dCk7CiAgYmFja2dyb3VuZDoKICAgIHJhZGlhbC1ncmFkaWVudCg5MDBweCA1MjBweCBhdCAxMCUgLTEwJSxyZ2JhKDE4MywxMzQsMjU1LC4xOCksdHJhbnNwYXJlbnQgNjIlKSwKICAgIHJhZGlhbC1ncmFkaWVudCg3NjBweCA1MjBweCBhdCAxMDAlIDk1JSxyZ2JhKDI1NSwyMDksMTAyLC4wOSksdHJhbnNwYXJlbnQgNjIlKSwKICAgIGxpbmVhci1ncmFkaWVudCgxODBkZWcsdmFyKC0tYmctMikgMCUsdmFyKC0tYmcpIDY4JSk7Cn0KYm9keTpiZWZvcmV7CiAgY29udGVudDoiIjsKICBwb3NpdGlvbjpmaXhlZDsKICBpbnNldDowOwogIHBvaW50ZXItZXZlbnRzOm5vbmU7CiAgb3BhY2l0eTouMDM1OwogIGJhY2tncm91bmQtaW1hZ2U6bGluZWFyLWdyYWRpZW50KHJnYmEoMjU1LDI1NSwyNTUsLjgpIDFweCx0cmFuc3BhcmVudCAxcHgpLGxpbmVhci1ncmFkaWVudCg5MGRlZyxyZ2JhKDI1NSwyNTUsMjU1LC44KSAxcHgsdHJhbnNwYXJlbnQgMXB4KTsKICBiYWNrZ3JvdW5kLXNpemU6MzJweCAzMnB4OwogIG1hc2staW1hZ2U6bGluZWFyLWdyYWRpZW50KHRvIGJvdHRvbSxibGFjayx0cmFuc3BhcmVudCA4NSUpOwp9CmJ1dHRvbixpbnB1dCxzZWxlY3R7Zm9udDppbmhlcml0fQpidXR0b257dG91Y2gtYWN0aW9uOm1hbmlwdWxhdGlvbn0KCi53cmFwe21heC13aWR0aDoxMDYwcHg7bWFyZ2luOjAgYXV0bztwYWRkaW5nOjI4cHggMThweCA1MnB4O3Bvc2l0aW9uOnJlbGF0aXZlfQoKLndyYXAgPiAucm93OmZpcnN0LWNoaWxkewogIG1pbi1oZWlnaHQ6NTRweDsKICBwYWRkaW5nOjJweCAycHggOHB4Owp9Ci53cmFwID4gLnJvdzpmaXJzdC1jaGlsZCA+IGRpdjpmaXJzdC1jaGlsZCAuc21hbGw6Zmlyc3QtY2hpbGR7CiAgY29sb3I6dmFyKC0tYWNjZW50LTIpOwogIGZvbnQtd2VpZ2h0OjkwMDsKICBsZXR0ZXItc3BhY2luZzouMThlbTsKICBmb250LXNpemU6MTFweDsKfQoKLmNhcmR7CiAgcG9zaXRpb246cmVsYXRpdmU7CiAgb3ZlcmZsb3c6aGlkZGVuOwogIGJhY2tncm91bmQ6bGluZWFyLWdyYWRpZW50KDE4MGRlZyxyZ2JhKDI5LDIyLDQyLC45NykscmdiYSgxNywxMywyNiwuOTcpKTsKICBib3JkZXI6MXB4IHNvbGlkIHJnYmEoMTgzLDEzNCwyNTUsLjE4KTsKICBib3JkZXItcmFkaXVzOjI2cHg7CiAgcGFkZGluZzoyNHB4OwogIGJveC1zaGFkb3c6MCAyNHB4IDcwcHggcmdiYSgwLDAsMCwuNDIpLGluc2V0IDAgMXB4IDAgcmdiYSgyNTUsMjU1LDI1NSwuMDMpOwp9Ci5jYXJkOmJlZm9yZXsKICBjb250ZW50OiIiOwogIHBvc2l0aW9uOmFic29sdXRlOwogIHRvcDowO2xlZnQ6MDtyaWdodDowOwogIGhlaWdodDoycHg7CiAgYmFja2dyb3VuZDpsaW5lYXItZ3JhZGllbnQoOTBkZWcsdHJhbnNwYXJlbnQsdmFyKC0tYWNjZW50KSx0cmFuc3BhcmVudCk7CiAgb3BhY2l0eTouNzsKfQoKaDEsaDIsaDMscHttYXJnaW4tdG9wOjB9CmgxLGgyLGgze2xldHRlci1zcGFjaW5nOi0uMDNlbX0KaDF7Zm9udC1zaXplOmNsYW1wKDMycHgsN3Z3LDYycHgpO2xpbmUtaGVpZ2h0Oi45OH0KaDJ7Zm9udC1zaXplOmNsYW1wKDI0cHgsNXZ3LDM4cHgpfQpoM3tmb250LXNpemU6MThweH0KCi5icmFuZHsKICBmb250LXNpemU6Y2xhbXAoNDJweCw5dncsODZweCk7CiAgZm9udC13ZWlnaHQ6OTUwOwogIGxldHRlci1zcGFjaW5nOi0uMDU1ZW07CiAgbGluZS1oZWlnaHQ6Ljk7CiAgbWF4LXdpZHRoOjc4MHB4OwogIHRleHQtd3JhcDpiYWxhbmNlOwp9Ci5icmFuZDphZnRlcnsKICBjb250ZW50OiIiOwogIGRpc3BsYXk6YmxvY2s7CiAgd2lkdGg6MTA4cHg7CiAgaGVpZ2h0OjVweDsKICBib3JkZXItcmFkaXVzOjk5OXB4OwogIG1hcmdpbi10b3A6MjBweDsKICBiYWNrZ3JvdW5kOmxpbmVhci1ncmFkaWVudCg5MGRlZyx2YXIoLS1hY2NlbnQpLHZhcigtLXdhcm0pKTsKfQoKLm11dGVke2NvbG9yOnZhcigtLW11dGVkKX0KLnNtYWxse2ZvbnQtc2l6ZToxM3B4fQoub2t7Y29sb3I6dmFyKC0tb2spfQouZGFuZ2Vye2NvbG9yOnZhcigtLWRhbmdlcil9CgouZ3JpZHtkaXNwbGF5OmdyaWQ7Z2FwOjE4cHh9Ci50d297Z3JpZC10ZW1wbGF0ZS1jb2x1bW5zOnJlcGVhdChhdXRvLWZpdCxtaW5tYXgoMjgwcHgsMWZyKSl9Ci5yb3d7ZGlzcGxheTpmbGV4O2dhcDoxMnB4O2FsaWduLWl0ZW1zOmNlbnRlcjtmbGV4LXdyYXA6d3JhcH0KCi5idG57CiAgYm9yZGVyOjFweCBzb2xpZCByZ2JhKDI1NSwyNTUsMjU1LC4wOCk7CiAgYm9yZGVyLXJhZGl1czoxNXB4OwogIHBhZGRpbmc6MTRweCAxOHB4OwogIGJhY2tncm91bmQ6bGluZWFyLWdyYWRpZW50KDE4MGRlZywjYzU5YmZmLCNhNjZkZjUpOwogIGNvbG9yOiMxNzBkMjM7CiAgZm9udC13ZWlnaHQ6OTAwOwogIGN1cnNvcjpwb2ludGVyOwogIGJveC1zaGFkb3c6MCAxMHB4IDI0cHggcmdiYSgxNjcsMTA5LDI0NSwuMjIpOwogIHRyYW5zaXRpb246dHJhbnNmb3JtIC4xNXMgZWFzZSxmaWx0ZXIgLjE1cyBlYXNlLGJveC1zaGFkb3cgLjE1cyBlYXNlOwp9Ci5idG46aG92ZXJ7ZmlsdGVyOmJyaWdodG5lc3MoMS4wNik7dHJhbnNmb3JtOnRyYW5zbGF0ZVkoLTFweCk7Ym94LXNoYWRvdzowIDE0cHggMzBweCByZ2JhKDE2NywxMDksMjQ1LC4yNyl9Ci5idG46YWN0aXZle3RyYW5zZm9ybTp0cmFuc2xhdGVZKDApfQouYnRuLnNlY29uZGFyeXtiYWNrZ3JvdW5kOmxpbmVhci1ncmFkaWVudCgxODBkZWcsIzMwMjY0MiwjMjQxZDM0KTtjb2xvcjp2YXIoLS10ZXh0KTtib3gtc2hhZG93Om5vbmU7Ym9yZGVyLWNvbG9yOnZhcigtLWxpbmUpfQouYnRuLmdob3N0e2JhY2tncm91bmQ6dHJhbnNwYXJlbnQ7Y29sb3I6dmFyKC0tYWNjZW50LTIpO2JvcmRlci1jb2xvcjp2YXIoLS1saW5lLXN0cm9uZyk7Ym94LXNoYWRvdzpub25lfQouYnRuOmRpc2FibGVke29wYWNpdHk6LjQyO2N1cnNvcjpub3QtYWxsb3dlZDt0cmFuc2Zvcm06bm9uZTtib3gtc2hhZG93Om5vbmV9CgppbnB1dCxzZWxlY3R7CiAgd2lkdGg6MTAwJTsKICBwYWRkaW5nOjE0cHggMTVweDsKICBtaW4taGVpZ2h0OjQ4cHg7CiAgYm9yZGVyLXJhZGl1czoxNHB4OwogIGJvcmRlcjoxcHggc29saWQgdmFyKC0tbGluZSk7CiAgYmFja2dyb3VuZDojMGYwYzE3OwogIGNvbG9yOnZhcigtLXRleHQpOwogIG91dGxpbmU6bm9uZTsKICBmb250LXNpemU6MTZweDsKICB0cmFuc2l0aW9uOmJvcmRlci1jb2xvciAuMTVzIGVhc2UsYm94LXNoYWRvdyAuMTVzIGVhc2U7Cn0KaW5wdXQ6OnBsYWNlaG9sZGVye2NvbG9yOiM3NTZiODN9CmlucHV0OmZvY3VzLHNlbGVjdDpmb2N1c3tib3JkZXItY29sb3I6dmFyKC0tYWNjZW50KTtib3gtc2hhZG93OjAgMCAwIDNweCByZ2JhKDE4MywxMzQsMjU1LC4xNCl9CgouY29kZXsKICBmb250LXNpemU6Y2xhbXAoMzhweCw4dncsNThweCk7CiAgbGV0dGVyLXNwYWNpbmc6LjE4ZW07CiAgZm9udC13ZWlnaHQ6OTUwOwogIGxpbmUtaGVpZ2h0OjE7CiAgbWFyZ2luLXRvcDo1cHg7CiAgY29sb3I6dmFyKC0tYWNjZW50LTIpOwogIHRleHQtc2hhZG93OjAgMCAyOHB4IHJnYmEoMTgzLDEzNCwyNTUsLjE4KTsKfQoKLnBsYXllcnN7ZGlzcGxheTpncmlkO2dyaWQtdGVtcGxhdGUtY29sdW1uczpyZXBlYXQoYXV0by1maXQsbWlubWF4KDE4MHB4LDFmcikpO2dhcDoxMnB4fQoucGlsbHsKICBkaXNwbGF5OmlubGluZS1mbGV4OwogIGFsaWduLWl0ZW1zOmNlbnRlcjsKICBnYXA6OHB4OwogIHdpZHRoOm1heC1jb250ZW50OwogIGJhY2tncm91bmQ6IzIwMTgyYzsKICBib3JkZXI6MXB4IHNvbGlkIHZhcigtLWxpbmUpOwogIGJvcmRlci1yYWRpdXM6OTk5cHg7CiAgcGFkZGluZzo4cHggMTJweDsKICBjb2xvcjojZDhjY2U0Owp9Ci5waWxsIGJ7Y29sb3I6dmFyKC0tdGV4dCl9CgouY2x1ZXN7ZGlzcGxheTpncmlkO2dyaWQtdGVtcGxhdGUtY29sdW1uczpyZXBlYXQoYXV0by1maXQsbWlubWF4KDE1MHB4LDFmcikpO2dhcDoxMnB4fQouY2x1ZXsKICBiYWNrZ3JvdW5kOmxpbmVhci1ncmFkaWVudCgxODBkZWcsIzIxMTkyZSwjMTcxMjIzKTsKICBib3JkZXI6MXB4IHNvbGlkIHZhcigtLWxpbmUpOwogIGJvcmRlci1yYWRpdXM6MThweDsKICBwYWRkaW5nOjE3cHggMTRweDsKICB0ZXh0LWFsaWduOmNlbnRlcjsKICBtaW4taGVpZ2h0Ojk2cHg7Cn0KLmNsdWUgYntkaXNwbGF5OmJsb2NrO2ZvbnQtc2l6ZToyNHB4O21hcmdpbi10b3A6NnB4O2NvbG9yOnZhcigtLWFjY2VudC0yKTt3b3JkLWJyZWFrOmJyZWFrLXdvcmR9Cgoud29yZHsKICBmb250LXNpemU6Y2xhbXAoMzBweCw3dncsNDZweCk7CiAgZm9udC13ZWlnaHQ6OTUwOwogIHBhZGRpbmc6MjRweCAxOHB4OwogIGJvcmRlci1yYWRpdXM6MjBweDsKICBiYWNrZ3JvdW5kOmxpbmVhci1ncmFkaWVudCgxMzVkZWcsIzIwMTcyZCwjMTIwZTFiKTsKICBib3JkZXI6MXB4IHNvbGlkIHZhcigtLWxpbmUtc3Ryb25nKTsKICB0ZXh0LWFsaWduOmNlbnRlcjsKICBjb2xvcjp2YXIoLS13YXJtKTsKICBsZXR0ZXItc3BhY2luZzouMDJlbTsKICBib3gtc2hhZG93Omluc2V0IDAgMXB4IDAgcmdiYSgyNTUsMjU1LDI1NSwuMDMpLDAgMThweCAzNnB4IHJnYmEoMCwwLDAsLjIyKTsKfQoKLnRpbWVyewogIGZvbnQtc2l6ZTpjbGFtcCg0NnB4LDEwdncsNjhweCk7CiAgbGluZS1oZWlnaHQ6MTsKICBmb250LXdlaWdodDo5NTA7CiAgdGV4dC1hbGlnbjpjZW50ZXI7CiAgY29sb3I6dmFyKC0tYWNjZW50LTIpOwogIHRleHQtc2hhZG93OjAgMCAyNHB4IHJnYmEoMTgzLDEzNCwyNTUsLjE4KTsKICBmb250LXZhcmlhbnQtbnVtZXJpYzp0YWJ1bGFyLW51bXM7Cn0KCi52b3RlewogIGRpc3BsYXk6ZmxleDsKICBqdXN0aWZ5LWNvbnRlbnQ6c3BhY2UtYmV0d2VlbjsKICBhbGlnbi1pdGVtczpjZW50ZXI7CiAgd2lkdGg6MTAwJTsKICBtaW4taGVpZ2h0OjUycHg7CiAgcGFkZGluZzoxNHB4IDE1cHg7CiAgYm9yZGVyLXJhZGl1czoxNXB4OwogIGJvcmRlcjoxcHggc29saWQgdmFyKC0tbGluZSk7CiAgYmFja2dyb3VuZDojMTAwYzE4OwogIGNvbG9yOnZhcigtLXRleHQpOwogIGN1cnNvcjpwb2ludGVyOwogIHRyYW5zaXRpb246dHJhbnNmb3JtIC4xNXMgZWFzZSxib3JkZXItY29sb3IgLjE1cyBlYXNlLGJhY2tncm91bmQgLjE1cyBlYXNlOwp9Ci52b3RlOmhvdmVye3RyYW5zZm9ybTp0cmFuc2xhdGVZKC0xcHgpO2JvcmRlci1jb2xvcjojNmE1NTgxO2JhY2tncm91bmQ6IzE3MTAyMX0KLnZvdGUuc2VsZWN0ZWR7Ym9yZGVyLWNvbG9yOnZhcigtLWFjY2VudCk7b3V0bGluZToycHggc29saWQgcmdiYSgxODMsMTM0LDI1NSwuMzUpO2JhY2tncm91bmQ6IzI1MTgzYX0KLnZvdGUgc3Ryb25ne2NvbG9yOnZhcigtLWFjY2VudC0yKTtmb250LXNpemU6MThweH0KCmhye2JvcmRlcjowO2JvcmRlci10b3A6MXB4IHNvbGlkIHZhcigtLWxpbmUpIWltcG9ydGFudH0KCmZvcm0uZ3JpZHtnYXA6MTJweH0KCi8qIElubGluZSBsZWdhY3kgY2FyZHMvYnV0dG9ucyBmcm9tIHRoZSBhcHAgYXJlIG5vcm1hbGl6ZWQgaGVyZS4gKi8KLmNhcmQgLmNhcmR7cGFkZGluZzoyMHB4O2JvcmRlci1yYWRpdXM6MjBweDtiYWNrZ3JvdW5kOnJnYmEoMjAsMTUsMzEsLjcyKTtib3JkZXItY29sb3I6dmFyKC0tbGluZSl9Ci5jYXJkIC5idG5bc3R5bGUqPSJ3aWR0aDoxMDAlIl17d2lkdGg6MTAwJSFpbXBvcnRhbnR9CgpAbWVkaWEobWF4LXdpZHRoOjcwMHB4KXsKICAud3JhcHtwYWRkaW5nOjE2cHggMTJweCAzNnB4fQogIC5jYXJke3BhZGRpbmc6MThweDtib3JkZXItcmFkaXVzOjIycHh9CiAgLmJyYW5ke2ZvbnQtc2l6ZTpjbGFtcCg0MHB4LDE1dncsNjhweCl9CiAgLnR3b3tncmlkLXRlbXBsYXRlLWNvbHVtbnM6MWZyfQogIC5wbGF5ZXJze2dyaWQtdGVtcGxhdGUtY29sdW1uczoxZnIgMWZyfQogIC5jbHVlc3tncmlkLXRlbXBsYXRlLWNvbHVtbnM6MWZyIDFmcn0KICAuYnRue3dpZHRoOjEwMCU7bWluLWhlaWdodDo1MHB4fQp9CkBtZWRpYShtYXgtd2lkdGg6NDMwcHgpewogIC53cmFwe3BhZGRpbmctbGVmdDoxMHB4O3BhZGRpbmctcmlnaHQ6MTBweH0KICAucGxheWVycywuY2x1ZXN7Z3JpZC10ZW1wbGF0ZS1jb2x1bW5zOjFmcn0KICAuY29kZXtmb250LXNpemU6MzRweH0KICAuY2FyZHtwYWRkaW5nOjE2cHh9Cn0K").decode('utf-8')

app=web.Application()
app.router.add_get('/ws',handler)
async def index(request):
    return web.Response(text=_INDEX_HTML, content_type='text/html', charset='utf-8')
async def app_js(request):
    return web.Response(text=_APP_JS, content_type='application/javascript', charset='utf-8')
async def style_css(request):
    return web.Response(text=_STYLE_CSS, content_type='text/css', charset='utf-8')
app.router.add_get('/', index)
app.router.add_get('/assets/app.js', app_js)
app.router.add_get('/assets/style.css', style_css)

if __name__=='__main__':
    web.run_app(app,host='0.0.0.0',port=int(os.environ.get('PORT','3001')))
