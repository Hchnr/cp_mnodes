#!/usr/bin/env python3
"""Small read-only comparison of existing and spawned batch metadata clients."""
import argparse
import concurrent.futures
import json
from pathlib import Path
import time

from migration_io import read_json, write_json
from migration_probe_processes import ProcessProbePool, _Client


def main():
    p=argparse.ArgumentParser();p.add_argument('--control',required=True);p.add_argument('--spool',required=True)
    p.add_argument('--output',required=True);p.add_argument('--count',type=int,default=4096)
    a=p.parse_args();c=Path(a.control);cfg=read_json(c/'config.json')
    entries=[]
    with open(a.spool) as handle:
        for line in handle:
            entry=json.loads(line);entry['_refresh_source']=True;entries.append(entry)
            if len(entries)>=a.count:break
    if (c/'STOP').exists():raise RuntimeError('STOP requested')
    results={}
    for mode in ['individual_threads','spawned_batches']:
        start=time.monotonic();cpu=time.process_time()
        if mode=='individual_threads':
            client=_Client(c,cfg)
            with concurrent.futures.ThreadPoolExecutor(max_workers=128) as pool:
                checked=list(pool.map(client._probe_missing,entries))
            counts=client._probe_snapshot()['counts'];child_cpu=0
        else:
            pool=ProcessProbePool(c,cfg,processes=8,threads=64)
            try:
                futures=[pool.submit(entries[i:i+64]) for i in range(0,len(entries),64)]
                batches=[future.result() for future in futures]
            finally:pool.shutdown()
            checked=[entry for batch in batches for entry in batch['entries']]
            counts={};child_cpu=sum(batch['cpu_seconds'] for batch in batches)
            for batch in batches:
                for key,value in batch['probe_delta']['counts'].items():counts[key]=counts.get(key,0)+value
        elapsed=time.monotonic()-start
        results[mode]={'entries':len(checked),'seconds':elapsed,'entries_per_second':len(checked)/elapsed,
                       'parent_cpu_seconds':time.process_time()-cpu,'child_cpu_seconds':child_cpu,
                       'counts':counts,'missing':sum(e is not None for e in checked)}
        print(json.dumps({mode:results[mode]}),flush=True)
    write_json(Path(a.output),results)


if __name__=='__main__':main()
