#!/usr/bin/env python3
"""Compare immutable old/new missing manifests, without opening data payloads."""
import argparse
import collections
import datetime
import gzip
import json
import os
from pathlib import Path
import time

from migration_reaudit import atomic


def rows(path):
    previous=None
    with gzip.open(path,'rt',encoding='ascii') as stream:
        for line in stream:
            row=json.loads(line);key=os.fsencode(row['rel'])
            if previous is not None and key<=previous:raise ValueError('manifest not uniquely sorted')
            previous=key;yield key,row


def compare(old,new,cutoff,output):
    olditer=iter(rows(old));prior=next(olditer,None);counts=collections.Counter();groups={};examples={}
    started=time.time()
    for key,row in rows(new):
        while prior is not None and prior[0]<key:prior=next(olditer,None)
        same=prior is not None and prior[0]==key
        source=row['source'];size=source['size'];counts['files']+=1;counts['bytes']+=size
        overlap='in_previous_missing_manifest' if same else 'not_in_previous_missing_manifest'
        ns=int(cutoff*1e9)
        if source.get('ctime_ns',ns+1)<=ns and source.get('mtime_ns',ns+1)<=ns:
            age='ctime_and_mtime_not_after_previous_audit'
        else:age='metadata_timestamp_after_previous_audit'
        for tag in (overlap,age):
            counts[tag+'_files']+=1;counts[tag+'_bytes']+=size
        parts=row['rel'].split('/');group='/'.join(parts[:4] if parts[0]=='eai_pwm' else parts[:3])
        value=groups.setdefault(group,collections.Counter());value['files']+=1;value['bytes']+=size
        value[age+'_files']+=1;value[age+'_bytes']+=size;value[overlap+'_files']+=1
        if len(examples.setdefault(age,[]))<8:examples[age].append(row)
        if counts['files']%250000==0:
            atomic(Path(output).with_name('provenance-progress.json'),dict(updated=time.time(),processed=counts['files'],elapsed=time.time()-started))
    result=dict(status='manifest_comparison_complete',started=started,finished=time.time(),cutoff=cutoff,
                old_manifest=str(old),new_manifest=str(new),counts=dict(counts),
                groups=sorted((dict(path=k,**v) for k,v in groups.items()),key=lambda x:-x['bytes']),examples=examples,
                caveat='ctime is not creation time. A later timestamp does not prove a file was newly created. An absent old missing-manifest entry does not prove the old scanner visited the path.')
    atomic(output,result);return result


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--old',type=Path,required=True);p.add_argument('--new',type=Path,required=True)
    p.add_argument('--cutoff',type=float,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    r=compare(a.old,a.new,a.cutoff,a.output);print(json.dumps(dict(counts=r['counts'],groups=r['groups'][:12])),flush=True)
