#!/usr/bin/env python3
"""Inspect the locally selected copy run using immutable status snapshots."""
import argparse
import json
import pathlib
import subprocess

import migration_launch as transport
from migration_io import read_json


REMOTE_CODE = '''import pathlib,json,time,datetime,sys,socket
c=pathlib.Path(CONTROL_PATH)
sys.path.insert(0,str(c))
from migration_io import read_json
from migration_nodes import load_nodes
warnings=[]
def optional(path):
 try:return read_json(path)
 except (OSError,ValueError,TypeError) as exc:
  warnings.append(str(path)+': '+type(exc).__name__)
  return None
s=optional(c/'state.json')
if not isinstance(s,dict):
 print(json.dumps({'ok':False,'control':str(c),'error':'state snapshot unavailable','read_warnings':warnings}))
else:
 nodes=[]
 for n in load_nodes(c,warnings):
  try:
   u=datetime.datetime.fromisoformat(n.get('updated',n.get('started','')).replace('Z','+00:00')).timestamp()
   heartbeat_age=round(max(0,time.time()-u),1)
  except (ValueError,TypeError,AttributeError):heartbeat_age=None
  active=n.get('active',{})
  nodes.append({'host':n.get('host'),'bytes':n.get('bytes',0),'counts':n.get('counts',{}),'active_threads':len(active),'threads':n.get('threads'),'active_files':list(active.values())[:2],'heartbeat_age_s':heartbeat_age,'error':n.get('last_error'),'ended':n.get('ended'),'components':n.get('components',[])})
 output={'ok':True,'control':str(c),'state':s,'nodes':nodes,'total_bytes':sum(n['bytes'] for n in nodes),'active_threads':sum(n['active_threads'] for n in nodes),'progress':optional(c/'progress.json'),'training_health':optional(c/'training-health.json'),'read_warnings':warnings}
 if s.get('status')=='failed':output['coordinator_log_path']=str(c/('coordinator-'+socket.gethostname()+'.log'))
 print(json.dumps(output,ensure_ascii=True))
'''


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--active', default=str(pathlib.Path(__file__).parent / 'active_migration.json'),
                        help='local active-run JSON pointer')
    args = parser.parse_args(argv)
    try:
        cfg = read_json(args.active)
        transport.JOB = cfg['job']
        code = REMOTE_CODE.replace('CONTROL_PATH', repr(cfg['control']), 1)
        _node, output = transport.ssh('master-0', code, owner=cfg.get('ssh_owner'))
        value = json.loads(output)
        if not isinstance(value, dict):
            raise ValueError('status response must be an object')
        rendered = json.dumps(value, ensure_ascii=False, indent=2)
        token = cfg.get('broker_token')
        if isinstance(token, str) and token:
            rendered = rendered.replace(token, '[redacted]')
        print(rendered)
        return 0 if value.get('ok') else 1
    except (OSError, ValueError, KeyError, TypeError, RuntimeError, subprocess.SubprocessError) as exc:
        print(json.dumps({'ok': False, 'error': 'migration status unavailable',
                          'diagnostic': type(exc).__name__}, ensure_ascii=False))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
