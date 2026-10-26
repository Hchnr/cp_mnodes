"""Add disjoint owner lanes after all primaries acknowledge their I/O share."""
import concurrent.futures
import datetime
import json
import pathlib
import time

from migration_io import read_json, write_json
import migration_launch as transport

BASE = pathlib.Path(__file__).resolve().parent
HOSTS = ['master-0'] + ['worker-' + str(i) for i in range(11)]


def main():
    active = read_json(BASE / 'active_migration.json'); transport.JOB = active['job']
    control = active['control']
    progress_path = BASE / 'double_worker_launch.json'
    checkpoint = {'phase': 'waiting_primary_budget', 'started': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                  'control': control, 'completed': {}}
    write_json(progress_path, checkpoint)
    probe = r'''import pathlib,sys,json,time,datetime
c=pathlib.Path(CONTROL);sys.path.insert(0,str(c));from migration_io import read_json
assert not (c/'STOP').exists(),'STOP protects training'
cfg=read_json(c/'config.json');assert cfg['threads']==64 and cfg['copy_threads_per_node']==128
assert cfg['io_bytes_per_second']==10_000_000_000 and cfg['supplement_io_bytes_per_second']==10_000_000_000
state=read_json(c/'state.json');assert state['status'] not in ('failed','stopped','complete','complete_with_changes'),state['status']
nodes=[]
for p in (c/'nodes').glob('*.json'):
 n=read_json(p);age=time.time()-datetime.datetime.fromisoformat(n['updated']).timestamp()
 expected=10_000_000_000-(32*1024**2 if n['host'].endswith('master-0') else 0)
 nodes.append({'host':n['host'],'pid':n['pid'],'rate':n.get('io_bytes_per_second'),'expected':expected,
               'age':age,'acked':n.get('io_bytes_per_second')==expected and -5<=age<=180
                        and n.get('threads')==64 and not n.get('ended') and not n.get('thread_error')})
print(json.dumps({'nodes':nodes,'bundle':read_json(c/'pipeline-deployment.json')['bundle']}))
'''.replace('CONTROL', repr(control))
    began = time.monotonic()
    while True:
        report = json.loads(transport.ssh('master-0', probe)[1])
        waiting = [n for n in report['nodes'] if not n['acked']]
        if len(report['nodes']) == 12 and not waiting:
            break
        print('WAIT_PRIMARY_BUDGET ' + json.dumps(waiting), flush=True)
        if time.monotonic() - began > 600:
            raise RuntimeError('primary acknowledgement incomplete; no new supplement was started')
        time.sleep(5)
    checkpoint.update(phase='starting_supplements', primary_ack=report['nodes'], bundle=report['bundle'])
    write_json(progress_path, checkpoint)
    print('ALL_PRIMARY_BUDGETS_ACKNOWLEDGED', flush=True)

    def launch(node):
        code = r'''import pathlib,sys,json,subprocess,socket,os,resource,time
c=pathlib.Path(CONTROL);bundle=pathlib.Path(BUNDLE);sys.path.insert(0,str(bundle))
from migration_io import read_json
from migration_worker128_run import supplement_config
supplement_config(c,require_primary=True)
script=str(bundle/'migration_worker128_run.py');existing=[]
for q in pathlib.Path('/proc').iterdir():
 if not q.name.isdigit():continue
 try:
  fields=(q/'stat').read_text().split(') ')[1].split()
  a=(q/'cmdline').read_bytes().decode().split('\0')
  if fields[0]!='Z' and script in a and '--control' in a and a[a.index('--control')+1]==str(c):existing.append(int(q.name))
 except (FileNotFoundError,ProcessLookupError):pass
assert len(existing)<=1,'duplicate supplement process'
if existing:pid=existing[0];reused=True
else:
 os.umask(0o077)
 soft,hard=resource.getrlimit(resource.RLIMIT_NOFILE)
 limit=65536 if hard==resource.RLIM_INFINITY else min(65536,hard)
 if soft<limit:resource.setrlimit(resource.RLIMIT_NOFILE,(limit,hard))
 log=c/('worker-extra64-'+socket.gethostname()+'.log')
 with open(log,'ab',buffering=0) as output:
  p=subprocess.Popen(['nice','-n','15','ionice','-c','3',sys.executable,'-u',script,'--control',str(c),
                      '--threads','64','--nice','0'],stdin=subprocess.DEVNULL,stdout=output,
                     stderr=subprocess.STDOUT,start_new_session=True,close_fds=True)
 pid=p.pid;reused=False
time.sleep(.3)
q=pathlib.Path('/proc')/str(pid);assert q.exists() and (q/'stat').read_text().split(') ')[1].split()[0]!='Z'
print(json.dumps({'host':socket.gethostname(),'pid':pid,'extra_threads':64,'reused':reused}))
'''.replace('CONTROL', repr(control)).replace('BUNDLE', repr(report['bundle']))
        return node, json.loads(transport.ssh(node, code)[1])

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        futures = [pool.submit(launch, host) for host in HOSTS]
        for future in concurrent.futures.as_completed(futures):
            host, record = future.result()
            checkpoint['completed'][host] = record
            write_json(progress_path, checkpoint)
            print('STARTED ' + host + ' ' + json.dumps(record), flush=True)
    checkpoint.update(phase='processes_started', updated=datetime.datetime.now(datetime.timezone.utc).isoformat())
    write_json(progress_path, checkpoint)
    print('ALL_12_SUPPLEMENTS_STARTED', flush=True)


if __name__ == '__main__':
    main()
