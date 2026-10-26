import sys,pathlib,json
sys.path.insert(0,str(pathlib.Path(__file__).parent));from migration_io import read_json
import migration_launch as t
base=pathlib.Path(__file__).parent;cfg=read_json(base/'active_migration.json');t.JOB=cfg['job'];info=json.loads((base/'benchmark_active.json').read_text())
code='''import pathlib,os,json,signal,time
b=json.loads(INFO);p=pathlib.Path('/proc')/str(b['pid']);owned=[]
if p.exists() and (p/'stat').read_text().split(') ')[1].split()[0]!='Z':
 a=(p/'cmdline').read_bytes().decode().split('\\0');assert b['work']+'/benchmark_copy.py' in a and b['root'] in a
 # Include only descendants of the exact benchmark process, never migration/training.
 procmap={}
 for q in pathlib.Path('/proc').iterdir():
  if not q.name.isdigit():continue
  try:parts=(q/'stat').read_text().split(') ')[1].split();procmap[int(q.name)]=(int(parts[1]),parts[0])
  except (OSError,IndexError):pass
 descendants={b['pid']}
 while True:
  more={pid for pid,(parent,_) in procmap.items() if parent in descendants};new=descendants|more
  if new==descendants:break
  descendants=new
 for pid in sorted(descendants,reverse=True):
  if procmap.get(pid,(None,'Z'))[1]=='Z':continue
  try:os.kill(pid,signal.SIGTERM);owned.append(pid)
  except ProcessLookupError:pass
print(json.dumps({'benchmark_stopped_pids':owned,'migration_untouched':True,'root':b['root']}))
'''.replace('INFO',repr(json.dumps(info)))
print(t.ssh(info['node'],code)[1])
