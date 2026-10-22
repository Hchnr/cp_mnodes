import json,pathlib
from migration_io import read_json,write_json
import migration_launch as t
B=pathlib.Path(__file__).parent;r=read_json(B/'active_migration.json');t.JOB=r['job'];t.OWNER=r['ssh_owner']
profiles=[]
for name,label in [('post_target128_profile.json','128_scanner_128_single_node_target'),('distributed_target_post_profile.json','distributed_256_target_adoption'),('distributed_target_new_data_profile.json','distributed_256_target_fresh_scan')]:
 v=read_json(B/name);profiles.append({'file':name,'phase':label,'started_at':v['started_at'],'finished_at':v['finished_at'],'aggregate':v['aggregate'],'notes':v['notes'],'errors':v['errors']})
samples={'profiles':profiles,'throughput_is_logical_process_io':True,'sample_comparisons_are_not_controlled_benchmarks':True,'different_file_mixes_and_pipeline_phases':True,'probe_observation':read_json(B/'distributed_target_after_30s.json'),'source_payload_modified':False}
payload={'control':r['control'],'records':{'distributed-target-services-deployment.json':read_json(B/'distributed_target_services_deployment.json'),'distributed-target-filter-deployment.json':read_json(B/'distributed_target_filter_deployment.json'),'distributed-target-optimization-samples.json':samples}}
code=r'''import pathlib,sys,json
b=json.loads(BODY);c=pathlib.Path(b['control']);sys.dont_write_bytecode=True;sys.path.insert(0,str(c));from migration_io import write_json
for name,value in b['records'].items():write_json(c/name,value)
print(json.dumps({'archived':list(b['records']),'source_data_touched':False}))
'''.replace('BODY',repr(json.dumps(payload)),1)
x=json.loads(t.ssh('master-0',code)[1]);write_json(B/'distributed_evidence_archive.json',x);print(json.dumps(x))
