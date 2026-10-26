import sys,pathlib,json
sys.path.insert(0,str(pathlib.Path(__file__).parent))
from migration_io import read_json
import migration_launch as t
cfg=read_json(pathlib.Path(__file__).parent/'active_migration.json');t.JOB=cfg['job']
code='''import pathlib,sys,json,time,datetime
c=pathlib.Path(CONTROL);sys.path.insert(0,str(c));from migration_io import read_json
m=read_json(c/'metrics.json');inv=read_json(c/'inventory.json');pid=m['pid'];p=pathlib.Path('/proc')/str(pid)
ss=m.get('samples',[])
print(json.dumps({'now':time.time(),'metricsupdated':m.get('updated'),'sample_time':m.get('sample_time'),'sample_count':len(ss),'last_samples':ss[-3:],'warnings':m.get('warnings'),'inventoryupdated':inv.get('updated'),'inventoryunits':[inv.get('completed_units'),inv.get('unit_count')],'inventorytotals':inv.get('totals'),'pid':pid,'pstate':(p/'stat').read_text().split(') ')[1].split()[0],'wchan':(p/'wchan').read_text(),'io':(p/'io').read_text(),'logtail':next(c.glob('metrics-*.log')).read_text()[-2000:],'stop':(c/'STOP').exists()}))
'''.replace('CONTROL',repr(cfg['control']))
print(t.ssh('master-0',code)[1])
