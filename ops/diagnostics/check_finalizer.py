import sys,pathlib,json
sys.path.insert(0,str(pathlib.Path(__file__).parent));from migration_io import read_json
import migration_launch as t
cfg=read_json(pathlib.Path(__file__).parent/'active_migration.json');t.JOB=cfg['job']
code='''import pathlib,sys,json,os,time,stat
c=pathlib.Path(CONTROL);sys.path.insert(0,str(c));from migration_io import read_json
s=read_json(c/'state.json');found=[]
for p in pathlib.Path('/proc').iterdir():
 if not p.name.isdigit():continue
 try:
  a=(p/'cmdline').read_bytes().decode().split('\\0')
  if str(c/'migration_run.py') in a and 'coordinator' in a:found.append(p)
 except OSError:pass
out={'state':{k:s.get(k) for k in ('status','stage','updated')},'processes':[]}
for p in found:
 def io():return {k:int(v) for k,v in (x.split(':') for x in (p/'io').read_text().splitlines())}
 before=io();at=time.monotonic();time.sleep(2);after=io();secs=time.monotonic()-at;fds=[]
 for f in (p/'fd').iterdir():
  try:
   target=os.readlink(f);st=f.stat()
   if stat.S_ISREG(st.st_mode):
    pos=next((x for x in (p/'fdinfo'/f.name).read_text().splitlines() if x.startswith('pos:')),None)
    fds.append({'fd':f.name,'path':target,'bytes':st.st_size,'position':pos})
  except OSError:pass
 out['processes'].append({'pid':int(p.name),'wchan':(p/'wchan').read_text(),'rchar_per_s':(after['rchar']-before['rchar'])/secs,'wchar_per_s':(after['wchar']-before['wchar'])/secs,'files':fds})
print(json.dumps(out))
'''.replace('CONTROL',repr(cfg['control']))
print(t.ssh('master-0',code)[1])
