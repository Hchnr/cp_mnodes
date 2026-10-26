import sys,pathlib,json,concurrent.futures
sys.path.insert(0,str(pathlib.Path(__file__).parent));from migration_io import read_json
import migration_launch as t
cfg=read_json(pathlib.Path(__file__).parent/'active_migration.json');t.JOB=cfg['job'];d=read_json(pathlib.Path(__file__).parent/'scale_deploy.json')
code='''import pathlib,sys,json,socket,time
c=pathlib.Path(CONTROL);sys.path.insert(0,str(c));from migration_io import read_json
n=read_json(c/'nodes'/(socket.gethostname()+'.json'));p=pathlib.Path('/proc')/str(n['pid'])
print(json.dumps({'pid':n['pid'],'threads':n['threads'],'streamed_bytes':n.get('streamed_bytes'),'active_sizes':[a.get('size') for a in n.get('active',{}).values()],'io':(p/'io').read_text(),'wchan':(p/'wchan').read_text()}))
'''.replace('CONTROL',repr(cfg['control']))
with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
 for name,out in pool.map(lambda n:t.ssh(n,code),d['remaining']):print(name,out)
