import pathlib,sys,json,base64,uuid,datetime
sys.path.insert(0,str(pathlib.Path(__file__).parent));from migration_io import read_json
import migration_launch as t
base=pathlib.Path(__file__).parent;cfg=read_json(base/'active_migration.json');t.JOB=cfg['job']
selection=json.loads((base/'benchmark_selection.json').read_text());tool=json.loads((base/'benchmark_tools.json').read_text())
selection['executables']={'cp':'/usr/bin/cp','rsync':tool['path']};selection['library_path']=tool['library_path'];selection['tools']['rsync']={'version':tool['version'],'flags':['-aAXS','--whole-file','--from0','--files-from']}
selection={k:selection[k] for k in ('node','source','control','workloads','tools','executables','library_path')}
selection['timed_source_sha256']=True
run_id=datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uuid.uuid4().hex[:8]
root='/share-mixed/project/.hcr-copy-benchmark-'+run_id;work='/tmp/hcr-copy-benchmark-run-'+run_id
bundle={'root':root,'work':work,'selection':selection,'script':base64.b64encode((base/'benchmark_copy.py').read_bytes()).decode()}
code='''import pathlib,sys,json,base64,subprocess,shutil,os,resource
os.umask(0o077);b=json.loads(BUNDLE);w=pathlib.Path(b['work']);w.mkdir();(w/'benchmark_copy.py').write_bytes(base64.b64decode(b['script']));(w/'selection.json').write_text(json.dumps(b['selection']))
compile((w/'benchmark_copy.py').read_bytes(),'benchmark_copy.py','exec')
soft,hard=resource.getrlimit(resource.RLIMIT_NOFILE);limit=65536 if hard==resource.RLIM_INFINITY else min(65536,hard);resource.setrlimit(resource.RLIMIT_NOFILE,(max(soft,limit),hard))
args=['nice','-n','15',sys.executable,'-u',str(w/'benchmark_copy.py'),'--selection',str(w/'selection.json'),'--root',b['root']]
if shutil.which('ionice'):args=['ionice','-c','3']+args
env=os.environ.copy();env['PYTHONDONTWRITEBYTECODE']='1';log=w/'benchmark.log';f=open(log,'ab',buffering=0)
p=subprocess.Popen(args,env=env,stdin=subprocess.DEVNULL,stdout=f,stderr=subprocess.STDOUT,start_new_session=True,close_fds=True)
print(json.dumps({'pid':p.pid,'node':b['selection']['node'],'root':b['root'],'work':str(w),'log':str(log),'control':b['selection']['control']}))
'''.replace('BUNDLE',repr(json.dumps(bundle)),1)
info=json.loads(t.ssh(selection['node'],code)[1]);(base/'benchmark_active.json').write_text(json.dumps(info,indent=2)+'\n');print(json.dumps(info))
