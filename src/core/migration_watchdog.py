#!/usr/bin/env python3
"""Observe training locally; stop this copy run on a new fatal training error."""
import argparse
import datetime
import json
import os
import pathlib
import re
import time

from migration_io import read_json, write_json


def write(path,data):
    return write_json(path, data)


def elastic_processes():
    found={}
    for p in pathlib.Path('/proc').iterdir():
        if not p.name.isdigit():continue
        try:
            if (p/'comm').read_text().strip()=='pt_elastic':
                found[p.name]=(p/'stat').read_text().split()[21]
        except OSError:pass
    return found


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--control',required=True)
    c=pathlib.Path(parser.parse_args().control)
    cfg=read_json(c/'config.json')
    if cfg.get('training_watchdog_enabled') is False:
        write(c/'training-health.json', {'state':'disabled_by_user',
              'updated':datetime.datetime.now(datetime.timezone.utc).isoformat()})
        return
    log=pathlib.Path(cfg['training_log'])
    pids=elastic_processes()
    offset=log.stat().st_size if log.exists() else 0
    baseline=offset
    fatal=re.compile(r'filedescriptor out of range in select\(\)|ChildFailedError|CUDA out of memory',re.I)
    status={'started':datetime.datetime.now(datetime.timezone.utc).isoformat(),
            'training_pids':list(pids),'baseline_log_bytes':baseline,
            'training_log':str(log),'state':'watching','checks':0}
    carry=''
    while True:
        status['updated']=datetime.datetime.now(datetime.timezone.utc).isoformat()
        status['checks']+=1
        reason=None
        try:
            if read_json(c/'config.json').get('training_watchdog_enabled') is False:
                status['state']='disabled_by_user';write(c/'training-health.json',status);return
            state=read_json(c/'state.json')
            if state['status'] in ('complete','complete_with_changes','failed','stopped'):
                status['state']='finished';write(c/'training-health.json',status);return
            current=elastic_processes()
            if pids and any(current.get(pid)!=start for pid,start in pids.items()):
                reason='a baseline training launcher has exited or restarted'
            if log.exists():
                size=log.stat().st_size
                if size<offset:offset=0
                with open(log,'rb') as f:
                    f.seek(offset);data=f.read(256*1024);offset=f.tell()
                text=data.decode('utf-8','replace')
                combined=carry+text
                match=fatal.search(combined)
                if match:reason='new training error: '+match.group(0)
                carry=combined[-1024:]
                status.update(log_bytes=size,read_offset=offset,log_mtime=log.stat().st_mtime)
                timings=re.findall(r'avg_time_per_step_s[\"\x27\s]*[:=]\s*([0-9.]+)',combined)
                if timings:status['observed_avg_step_seconds']=float(timings[-1])
            if reason:
                status.update(state='copy_stopped_to_protect_training',reason=reason)
                (c/'STOP').write_text('Training watchdog: '+reason+'\n')
                write(c/'training-health.json',status)
                return
            write(c/'training-health.json',status)
        except (OSError,ValueError) as exc:
            status['inspection_error']=str(exc)
            write(c/'training-health.json',status)
        time.sleep(15)


if __name__=='__main__':main()
