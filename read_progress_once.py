import sys,pathlib
sys.path.insert(0,str(pathlib.Path(__file__).parent))
from migration_io import read_json
import migration_launch as t
cfg=read_json(pathlib.Path(__file__).parent/'active_migration.json');t.JOB=cfg['job']
code="import subprocess;print(subprocess.check_output(['python3','/share-mixed/project/.hcr-copy-monitor/progress.py'],text=True))"
print(t.ssh('master-0',code,owner=cfg.get('ssh_owner'))[1])
