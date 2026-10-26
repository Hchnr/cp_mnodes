"""Read-only idle-lane/controller preflight; never drain or restart a worker."""
import argparse
import concurrent.futures
import json
from pathlib import Path
import shlex
import subprocess

from migration_io import read_json, write_json
import migration_launch as transport


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--node', choices=['master-0'] + ['worker-' + str(i) for i in range(11)], required=True)
    parser.add_argument('--lane', choices=['primary', 'supplement'], required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    active = read_json(Path(__file__).parent / 'active_migration.json')
    transport.JOB = active['job']
    script = r'''import sys,os,pathlib,json,time
c=pathlib.Path(CONTROL);sys.path.insert(0,str(c))
from migration_io import read_json
n=read_json(c/('nodes' if LANE=='primary' else 'nodes-extra')/(os.uname().nodename+'.json'))
p=pathlib.Path('/proc')/str(n['pid']);f=(p/'stat').read_text().rsplit(') ',1)[1].split()
argv=(p/'cmdline').read_bytes().decode().strip('\0').split('\0');payload=[]
for fd in (p/'fd').iterdir():
 try:
  name=os.readlink(fd)
  if name.startswith('/share/project/') or name.startswith('/share-mixed/project/eai_pwm/'):
   payload.append(name)
 except OSError:pass
helpers=[]
for q in pathlib.Path('/proc').iterdir():
 if not q.name.isdigit():continue
 try:
  a=(q/'cmdline').read_bytes().decode().strip('\0').split('\0')
  if any(pathlib.Path(x).name=='rollout_cache_age.py' for x in a):helpers.append({'pid':q.name,'argv':a})
 except (OSError,UnicodeError):pass
print(json.dumps({'now':time.time(),'updated':n.get('updated'),'pid':n['pid'],'start_ticks':f[19],
 'state':f[0],'argv':argv,'active':len(n.get('active',{})),'payload_fd_count':len(payload),
 'helpers':helpers,'ended':n.get('ended'),'thread_error':n.get('thread_error')}))
'''.replace('CONTROL', repr(active['control']), 1).replace('LANE', repr(args.lane), 1)
    master_script = r'''import pathlib,json
helpers=[]
for q in pathlib.Path('/proc').iterdir():
 if not q.name.isdigit():continue
 try:
  a=(q/'cmdline').read_bytes().decode().strip('\0').split('\0')
  if any(pathlib.Path(x).name=='rollout_cache_age.py' for x in a):helpers.append({'pid':q.name,'argv':a})
 except (OSError,UnicodeError):pass
print(json.dumps({'helpers':helpers}))
'''
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(transport.ssh, args.node, script, owner=active.get('ssh_owner')),
                   pool.submit(transport.ssh, 'master-0', master_script, owner=active.get('ssh_owner'))]
        lane, master = [json.loads(f.result()[1]) for f in futures]
    controllers = []
    for line in subprocess.check_output(['ps', '-axo', 'pid=,command='], text=True).splitlines():
        parts = line.strip().split(None, 1)
        if len(parts) != 2:
            continue
        try:
            argv = shlex.split(parts[1])
        except ValueError:
            continue
        if any(Path(value).name == 'rollout_cache_age.py' for value in argv):
            controllers.append(int(parts[0]))
    result = {'node': args.node, 'lane': args.lane, 'worker': lane,
              'master': master, 'local_controller_pids': controllers,
              'mutation_performed': False}
    write_json(args.output, result)
    print(json.dumps(result))


if __name__ == '__main__':
    main()
