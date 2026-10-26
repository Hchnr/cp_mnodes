import sys,pathlib,json
sys.path.insert(0,str(pathlib.Path(__file__).parent));from migration_io import read_json
import migration_launch as t
cfg=read_json(pathlib.Path(__file__).parent/'active_migration.json');t.JOB=cfg['job']
code='''import pathlib,sys,json,sqlite3,collections,urllib.request,time
c=pathlib.Path(CONTROL);sys.path.insert(0,str(c));from migration_io import read_json
cfg=read_json(c/'config.json');b=read_json(c/'broker.json');pid=b['pid'];p=pathlib.Path('/proc')/str(pid)
dbpath='/tmp/'+cfg['run_id']+'-broker.sqlite';db=sqlite3.connect('file:'+dbpath+'?mode=ro',uri=True,timeout=1)
indexes=db.execute('PRAGMA index_list(claims)').fetchall();plan=db.execute("EXPLAIN QUERY PLAN SELECT id,lease,payload,running FROM claims WHERE stage=? AND owner=? AND status='leased'",('p1-r1','sample-0')).fetchall()
ws=collections.Counter()
for t in (p/'task').iterdir():
 try:ws[(t/'wchan').read_text()]+=1
 except OSError:pass
print(json.dumps({'pid':pid,'threads':sum(ws.values()),'wchan':dict(ws),'io':(p/'io').read_text(),'db_bytes':pathlib.Path(dbpath).stat().st_size,'indexes':indexes,'queryplan':plan,'stop':(c/'STOP').exists(),'state':{k:read_json(c/'state.json').get(k) for k in ('status','stage','updated')},'brokerlog':next(c.glob('migration_broker-scaled-*.log')).read_text()[-2000:]}))
'''.replace('CONTROL',repr(cfg['control']))
print(t.ssh('master-0',code)[1])
