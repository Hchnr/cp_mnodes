"""Register the authorized successor target without changing live copy work."""
import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent))
from migration_io import read_json, write_json
import migration_launch as transport

base = pathlib.Path(__file__).parent
active = read_json(base / 'active_migration.json')
transport.JOB = active['job']
code = r'''import datetime,json,pathlib,sys,uuid
c=pathlib.Path(CONTROL);sys.path.insert(0,str(c))
from migration_io import read_json,write_json
cfg=read_json(c/'config.json')
assert cfg['source']=='/share/project/eai_pwm' and cfg['destination']=='/share-mixed/project/eai_pwm'
path=c/'extension-plan.json'
try:plan=read_json(path)
except FileNotFoundError:plan=None
if plan is not None:
 assert (plan.get('priority'),plan.get('source'),plan.get('destination'))==(4,'/share/project','/share-mixed/project'),'different extension plan already registered'
 assert plan.get('exclude')==['eai_pwm'],'extension overlap policy changed'
else:
 now=datetime.datetime.now(datetime.timezone.utc).isoformat()
 plan={'version':1,'id':'priority4-'+uuid.uuid4().hex,'priority':4,'status':'queued',
       'registered_at':now,'updated':now,'source':'/share/project','destination':'/share-mixed/project',
       'exclude':['eai_pwm'],'covered_by_priorities':[1,2,3],
       'coverage_note':'eai_pwm is already covered by priorities 1-3; reuse their final verification evidence, do not copy that tree again.',
       'depends_on':{'control':str(c),'latest_stages':['p1-r3','p2-r3','p3-r2'],
                    'allowed_terminal_states':['complete','complete_with_changes'],
                    'require_issue_review':True,'require_all_summaries_and_finalizers':True,
                    'require_metadata_complete':True,'require_all_queues_empty':True,
                    'require_old_workers_and_helpers_quiescent':True,'require_no_STOP':True},
       'execution':{'mode':'separate_successor_run','dispatcher':'existing_thread_heartbeat',
                    'copy_threads_per_node':128,'parallel_scan_threads':64,
                    'data_io_total_per_node_bytes_per_second':20000000000,
                    'copy_only':True,'delete_source':False,'restart_training':False,
                    'conflicts':'report; never overwrite an unowned differing destination',
                    'destination_mount_root_metadata':'preserve existing mount root; copy child metadata',
                    'resume_index':'independent; do not reuse eai_pwm-relative keys without explicit prefix mapping'},
       'inventory_complete':False,'discovered_bytes':None,'discovered_files':None,
       'completion_note':'The expanded task is not complete until priority 4 has also been copied and verified.'}
if 'successor' not in plan:
 stamp=''.join(ch for ch in plan['registered_at'].split('+')[0] if ch.isdigit())[:14]
 run_id='hcr-copy-p4-'+stamp+'-'+plan['id'][-6:]
 plan['successor']={'run_id':run_id,'control':'/share-mixed/project/.'+run_id,
                    'state':'not_started','phase':4,'master_local_lock':'/tmp/'+run_id+'-dispatch.lock',
                    'launch_policy':'idempotent; inspect local lock, process identity and persisted state before launch'}
 plan['updated']=datetime.datetime.now(datetime.timezone.utc).isoformat()
 write_json(path,plan)
print(json.dumps({'registered':True,'plan_path':str(path),'plan':plan,'current_state':{k:read_json(c/'state.json').get(k) for k in ('status','stage','updated')}}))
'''.replace('CONTROL', repr(active['control']), 1)
_, output = transport.ssh('master-0', code)
result = json.loads(output)
write_json(base / 'priority4_registration.json', result)
write_json(base / 'extension-plan.json', result['plan'])
print(json.dumps({'registered': result['registered'], 'priority': result['plan']['priority'],
    'status': result['plan']['status'], 'plan_path': result['plan_path'],
    'current_state': result['current_state']}, ensure_ascii=False))
