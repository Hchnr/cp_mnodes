#!/usr/bin/env python3
"""Continue role deployment; every remote launch has exact identity adoption."""
import concurrent.futures
import fcntl
import json
import pathlib
from migration_io import read_json,write_json
import migration_launch as t

BASE=pathlib.Path(__file__).resolve().parent

def main():
    lock=open(BASE/'new-cluster-roles.lock','a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    r=read_json(BASE/'new_cluster_recovery.json');t.JOB=r['job'];t.OWNER=r['ssh_owner']
    def launch(item):
        node,role=item
        code='''import sys,pathlib,json
c=pathlib.Path(CONTROL);sys.path.insert(0,str(c));from migration_recovery_role import launch
print(json.dumps(launch(c,ROLE)))
'''.replace('CONTROL',repr(r['control'])).replace('ROLE',repr(role))
        return node,role,json.loads(t.ssh(node,code)[1])
    def batch(items):
        errors=[]
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
            fs={pool.submit(launch,item):item for item in items}
            for f in concurrent.futures.as_completed(fs):
                try:
                    node,role,result=f.result();r['roles'][node+':'+role]=result
                    write_json(BASE/'new_cluster_recovery.json',r)
                    print(node,role,result['pid'],'adopted' if result.get('adopted') else 'launched',flush=True)
                except Exception as exc:
                    item=fs[f];errors.append({'node':item[0],'role':item[1],'error':str(exc)})
                    print('ERROR',item,type(exc).__name__,str(exc)[:250],flush=True)
        if errors:
            r['deployment_errors']=errors;write_json(BASE/'new_cluster_recovery.json',r)
            raise RuntimeError('role deployment incomplete; exact-identity retry supported')
    batch([('master-0',role) for role in ('broker','metrics','watchdog')])
    batch([(node,'primary') for node in t.HOSTS])
    batch([(node,'supplement') for node in t.HOSTS])
    r['status']='roles_running_index_restoring';r.pop('deployment_errors',None)
    write_json(BASE/'new_cluster_recovery.json',r)
    print('all 24 copy daemons launched',flush=True)

if __name__=='__main__':main()
