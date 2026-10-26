"""Publish merged-node readers and replace only the lightweight sampler."""
import json
import pathlib

from migration_io import read_json
import migration_launch as transport

base = pathlib.Path(__file__).resolve().parent
active = read_json(base / 'active_migration.json'); transport.JOB = active['job']
code = r'''import pathlib,sys,json,shutil,subprocess,signal,os,time
c=pathlib.Path(CONTROL);sys.path.insert(0,str(c));from migration_io import read_json,write_json
assert not (c/'STOP').exists()
d=read_json(c/'pipeline-deployment.json');bundle=pathlib.Path(d['bundle'])
assert (bundle/'migration_worker128_run.py').is_file()
m=pathlib.Path('/share-mixed/project/.hcr-copy-monitor')
for name in ('migration_nodes.py','migration_progress.py'):
 shutil.copyfile(bundle/name,c/name)
 shutil.copyfile(bundle/name,m/name)
shutil.copyfile(bundle/'migration_progress.py',m/'progress.py')
old=read_json(c/'metrics.json')['pid'];q=pathlib.Path('/proc')/str(old)
if q.exists():
 a=(q/'cmdline').read_bytes().decode().split('\0')
 scripts=[v for v in a if v.endswith('migration_metrics.py') and v.startswith(str(c)+'/')]
 assert scripts and '--control' in a and a[a.index('--control')+1]==str(c)
 start=(q/'stat').read_text().split(') ')[1].split()[19]
 os.kill(old,signal.SIGTERM)
 for _ in range(200):
  try:fields=(q/'stat').read_text().split(') ')[1].split()
  except FileNotFoundError:break
  if fields[0]=='Z' or fields[19]!=start:break
  time.sleep(.1)
 else:raise RuntimeError('old metrics sampler still exiting; do not duplicate it')
log=c/'metrics-double-concurrency.log'
with open(log,'ab',buffering=0) as output:
 p=subprocess.Popen(['nice','-n','15','ionice','-c','3',sys.executable,'-u',str(bundle/'migration_metrics.py'),
                     '--control',str(c),'--interval','5'],stdin=subprocess.DEVNULL,
                    stdout=output,stderr=subprocess.STDOUT,start_new_session=True,close_fds=True)
time.sleep(.5);assert p.poll() is None
out={'metrics_old_pid':old,'metrics_pid':p.pid,'bundle':str(bundle),'monitor':str(m/'progress.py')}
write_json(c/'double-readers-deployment.json',out)
print(json.dumps(out))
'''.replace('CONTROL', repr(active['control']))
print(transport.ssh('master-0', code)[1])
