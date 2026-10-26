"""Read only /proc and immutable state; no source tree traversal."""
import json
from pathlib import Path
from migration_io import read_json, write_json
import migration_launch as t
base=Path(__file__).resolve().parent
active=read_json(base/'active_migration.json');t.JOB=active['job']
code=r'''import collections,json,os,pathlib,sys,time
c=pathlib.Path(CONTROL);sys.path.insert(0,str(c));from migration_io import read_json
role=read_json(c/'pipeline-deployment.json');pid=role['pid'];root=pathlib.Path('/proc')/str(pid)
def sample():
 out={'time':time.time(),'pid':pid,'state':read_json(c/'state.json')};fdmap={};kinds=collections.Counter();controls=[];byparent=collections.Counter()
 for fd in(root/'fd').iterdir():
  try:target=os.readlink(fd)
  except OSError:continue
  fdmap[int(fd.name)]=target
  kind='control' if target.startswith(str(c)+'/') else 'destination' if target.startswith('/share-mixed/project/eai_pwm') else 'source' if target.startswith('/share/project/eai_pwm') else 'other'
  kinds[kind]+=1
  if kind=='destination':byparent[target]+=1
  if kind=='control':
   try:info=(root/'fdinfo'/fd.name).read_text();controls.append({'fd':int(fd.name),'target':target,'info':info})
   except OSError:pass
 out['fd_categories']=dict(kinds);out['control_fds']=controls;out['destination_fd_top']=byparent.most_common(16)
 threads=[];summary=collections.Counter()
 for task in(root/'task').iterdir():
  try:
   w=(task/'wchan').read_text().strip();sc=(task/'syscall').read_text().strip();pieces=sc.split();nr=pieces[0] if pieces else''
   summary[(w,nr)]+=1
   item={'tid':int(task.name),'wchan':w,'syscall':sc}
   if len(pieces)>1:
    try:fd=int(pieces[1],16);item['first_arg_fd_target']=fdmap.get(fd)
    except ValueError:pass
   if int(task.name)==pid or w not in ('futex_wait_queue_me','0') and len(threads)<24:threads.append(item)
  except OSError:continue
 out['thread_syscalls']=[{'wchan':key[0],'syscall_number':key[1],'count':count} for key,count in summary.items()];out['sampled_threads']=threads
 return out
first=sample();time.sleep(5);second=sample();print(json.dumps({'first':first,'second':second}))
'''.replace('CONTROL',repr(active['control']),1)
_,out=t.ssh('master-0',code,owner=active.get('ssh_owner'));value=json.loads(out)
path=base/'target128_supply_probe.json';prior=read_json(path);prior['extra_probe']=value;write_json(path,prior)
for key,sample in value.items():
 state=sample['state'];print(json.dumps({'sample':key,'time':sample['time'],'target_filter':state.get('target_filter'),'updated':state.get('updated'),'fd_categories':sample['fd_categories'],'control_fds':sample['control_fds'],'thread_syscalls':sample['thread_syscalls'],'main_thread':next((i for i in sample['sampled_threads'] if i['tid']==sample['pid']),{})},ensure_ascii=False))
