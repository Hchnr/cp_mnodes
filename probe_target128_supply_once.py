"""Read only existing control files and /proc for producer supply evidence."""
import json
from pathlib import Path
from migration_io import read_json, write_json
import migration_launch as transport

BASE = Path(__file__).resolve().parent
active = read_json(BASE / 'active_migration.json')
transport.JOB = active['job']
code = r'''import collections,json,os,pathlib,sys,time
c=pathlib.Path(CONTROL);sys.path.insert(0,str(c))
from migration_io import read_json
warnings=[]
def optional(name):
 try:return read_json(c/name)
 except (OSError,ValueError) as exc:warnings.append(str(name)+':'+type(exc).__name__);return {}
def proc(pid):
 p=pathlib.Path('/proc')/str(pid)
 try:
  fields=(p/'stat').read_text().rsplit(') ',1)[1].split()
  result={'pid':pid,'state':fields[0],'starttime_ticks':fields[19],'argv':(p/'cmdline').read_bytes().rstrip(b'\0').decode().split('\0'),'wchan':(p/'wchan').read_text().strip(),'threads':int(fields[17]),'utime':int(fields[11]),'stime':int(fields[12])}
  try:result['stack']=(p/'stack').read_text()[:4000]
  except OSError:pass
  by_wchan=collections.Counter();details=[]
  for task in(p/'task').iterdir():
   try:wait=(task/'wchan').read_text().strip();by_wchan[wait]+=1
   except OSError:continue
  result['thread_wchan_counts']=dict(by_wchan)
  fds=[]
  for fd in(p/'fd').iterdir():
   try:target=os.readlink(fd)
   except OSError:continue
   if str(c) in target and ('manifest' in target or 'assigned-tasks' in target or 'producer.log' in target):
    info={'fd':int(fd.name),'target':target}
    try:
     rows=dict(line.split(':',1) for line in(p/'fdinfo'/fd.name).read_text().splitlines() if ':'in line)
     info['position']=int(rows.get('pos','0'));info['flags']=rows.get('flags','').strip();info['bytes']=fd.stat().st_size
    except OSError:pass
    fds.append(info)
  result['control_fds']=fds
  result['fd_count']=len(list((p/'fd').iterdir()))
  result['io']=dict((key,int(value)) for key,value in(line.split(':',1) for line in(p/'io').read_text().splitlines()))
  return result
 except OSError as exc:return {'pid':pid,'error':type(exc).__name__}
def sample():
 state=optional('state.json');deployment=optional('pipeline-deployment.json');progress=optional('progress.json')
 return {'time':time.time(),'stop':(c/'STOP').exists(),'state':{key:state.get(key) for key in('updated','status','stage','recovery_warm','recovery_label','active_manifest','foreground_handover','target_filter','next_task_id','parallel_target_threads','parallel_scan_threads','scan_complete','warm_missing_scan','resume_index_building','resume_index_error')},'current_scan':progress.get('current_scan'),'producer':proc(deployment.get('pid')),'deployment':{key:deployment.get(key) for key in('pid','startticks','entrypoint','bundle','log','handover','parallel_target_threads','parallel_scan_threads')}}
first=sample();time.sleep(5);second=sample();deployment=second['deployment'];extra={}
try:
 with open(deployment['log'],'rb') as log:log.seek(0,2);length=log.tell();log.seek(max(0,length-8000));extra['log']={'bytes':length,'tail':log.read().decode(errors='replace')}
except (OSError,KeyError):pass
try:
 handover=read_json(pathlib.Path(deployment['handover']));extra['handover']={key:handover.get(key) for key in('created','preserved_task_count','next_task_id','warm_manifest','missing_manifest','assigned_tasks_path')}
 for label in ('warm_manifest','missing_manifest','assigned_tasks_path'):
  path=pathlib.Path(handover[label]);extra.setdefault('control_file_sizes',{})[label]={'path':str(path),'bytes':path.stat().st_size}
 bundle=pathlib.Path(deployment['bundle']);entry=bundle/deployment['entrypoint']
 lines=entry.read_text().splitlines();extra['entry_excerpt']=['%d:%s'%(n+1,line) for n,line in enumerate(lines) if any(token in line for token in('def records','def supplement_missing','resume_records=records','self.filter_workers','self.scan_workers','target_filter_inflight','filter_limit','submit([dict(entry'))]
except (OSError,KeyError,ValueError) as exc:warnings.append('extra:'+type(exc).__name__)
print(json.dumps({'control':str(c),'first':first,'second':second,'extra':extra,'warnings':warnings}))
'''.replace('CONTROL', repr(active['control']), 1)
_, raw = transport.ssh('master-0', code, owner=active.get('ssh_owner'))
value = json.loads(raw)
path = BASE / 'target128_supply_probe.json'
write_json(path, value)
print(json.dumps({'saved':str(path),'first':value['first'],'second':value['second'],'extra':value['extra'],'warnings':value['warnings']},ensure_ascii=False))
