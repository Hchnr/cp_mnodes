"""Publish read-only observations and rollout evidence to the control archive."""
import json
import pathlib

from migration_io import read_json
import migration_launch as transport

base = pathlib.Path(__file__).absolute().parent
active = read_json(base / 'active_migration.json')
transport.JOB = active['job']
values = {}
for local, remote in [('state_cache_rollout.json', 'state-cache-deployment.json'),
                      ('state_cache_canary.json', 'state-cache-canary.json'),
                      ('state_cache_post_profile.json', 'state-cache-post-profile.json'),
                      ('state_cache_broker_probe.json', 'state-cache-broker-probe.json'),
                      ('state_cache_health.json', 'state-cache-health.json'),
                      ('state_cache_summary.json', 'state-cache-summary.json')]:
    try: values[remote] = read_json(base / local)
    except FileNotFoundError: pass
code = '''import json,pathlib,sys
c=pathlib.Path(CONTROL);sys.path.insert(0,str(c))
from migration_io import write_json
for name,value in VALUES.items():write_json(c/name,value)
print(json.dumps({'published':list(VALUES)}))
'''.replace('CONTROL', repr(active['control'])).replace('VALUES', repr(values))
_, result = transport.ssh('master-0', code)
print(result)
