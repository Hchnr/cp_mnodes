import sys,pathlib,json,uuid
sys.path.insert(0,str(pathlib.Path(__file__).parent));from migration_io import read_json
import migration_launch as t
cfg=read_json(pathlib.Path(__file__).parent/'active_migration.json');t.JOB=cfg['job']
root='/tmp/hcr-copy-benchmark-tools-'+uuid.uuid4().hex[:12]
code='''import pathlib,subprocess,os,json,shutil
os.umask(0o077);root=pathlib.Path(ROOT);root.mkdir();packages=root/'packages';packages.mkdir();unpacked=root/'unpacked';unpacked.mkdir()
existing=shutil.which('rsync')
if existing:print(json.dumps({'path':existing,'library_path':None,'version':subprocess.check_output([existing,'--version'],text=True).splitlines()[0]}))
else:
 r=subprocess.run(['apt-get','download','rsync','libpopt0'],cwd=packages,text=True,capture_output=True,timeout=60)
 (root/'download.log').write_text(r.stdout+'\\n'+r.stderr)
 if r.returncode:raise RuntimeError('isolated package download failed; private log '+str(root/'download.log'))
 files=list(packages.glob('*.deb'));assert files
 for package in files:subprocess.run(['dpkg-deb','-x',str(package),str(unpacked)],check=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
 binary=unpacked/'usr/bin/rsync';assert binary.is_file();env=os.environ.copy();libs=[str(unpacked/'lib/x86_64-linux-gnu'),str(unpacked/'usr/lib/x86_64-linux-gnu')];env['LD_LIBRARY_PATH']=':'.join(libs+([env['LD_LIBRARY_PATH']] if env.get('LD_LIBRARY_PATH') else []))
 r=subprocess.run([str(binary),'--version'],env=env,text=True,capture_output=True,timeout=10);(root/'version.log').write_text(r.stdout+'\\n'+r.stderr)
 if r.returncode:raise RuntimeError('isolated rsync dependencies unavailable; private log '+str(root/'version.log'))
 print(json.dumps({'path':str(binary),'library_path':env['LD_LIBRARY_PATH'],'version':r.stdout.splitlines()[0],'packages':[p.name for p in files],'tools_root':str(root)}))
'''.replace('ROOT',repr(root))
print(t.ssh('worker-10',code)[1])
