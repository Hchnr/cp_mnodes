#!/usr/bin/env python3
"""One private low-frequency SwanLab reporter; independent of copying."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import time
import uuid

from projset_overview import overview_metrics, overview_rows


def save(path, value):
    tmp = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    tmp.write_text(json.dumps(value, ensure_ascii=True))
    tmp.replace(path)


def capacity_metrics(counts):
    values = {}
    for key, value in counts.items():
        if key.endswith('_bytes'):
            values['copy/' + key[:-6] + '_TB'] = value / 1e12
        elif key == 'payload_written':
            values['copy/payload_written_TB'] = value / 1e12
        else:
            values['copy/' + key] = value
    return values


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--run', type=Path, required=True)
    p.add_argument('--interval', type=int, default=120)
    args = p.parse_args()
    if args.interval < 120:
        p.error('minimum upload interval is 120 seconds')
    root = args.run/'swanlab'
    root.mkdir(exist_ok=True, mode=0o700)
    lock = open(root/'reporter.lock', 'a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    for name in ('https_proxy', 'http_proxy', 'HTTPS_PROXY', 'HTTP_PROXY', 'ALL_PROXY', 'all_proxy'):
        os.environ.pop(name, None)
    secrets = Path('/share-mixed/project/eai_pwm/.secrets')
    proxy = (secrets/'swanlab_proxy').read_text().strip()
    os.environ.update({k: proxy for k in ('https_proxy', 'http_proxy', 'HTTPS_PROXY', 'HTTP_PROXY')})
    os.environ['SWANLAB_API_KEY'] = (secrets/'swanlab_api_key').read_text().strip()
    import swanlab
    swanlab.merge_settings(swanlab.Settings(interactive=False, probe=swanlab.Settings.Probe(
        hardware=False, runtime=False, requirements=False, conda=False, git=False, swanlab=False, monitor=False)))
    statepath = root/'state.json'
    state = json.loads(statepath.read_text()) if statepath.exists() else {}
    run, backoff = None, args.interval
    next_confirm = 0
    while True:
        try:
            # A reporter restart also respects the configured upload cadence.
            remaining = state.get('enqueued_at', 0) + args.interval - time.time()
            if remaining > 0:
                time.sleep(remaining)
            if run is None:
                swanlab.login(api_key=os.environ['SWANLAB_API_KEY'], save=False, timeout=10)
                run = swanlab.init(mode='online', project='data-migration',
                    name=args.run.name, id=state.get('run_id'), resume='allow' if state.get('run_id') else 'never',
                    public=False, log_dir=str(root/'sdk'),
                    config=dict(source='/share-mixed/projset', target='/share-mixed/project',
                                nodes=16, upload_interval_seconds=args.interval,
                                existing_conflict_policy='preserve', verification='SHA256+readback+fsync'))
                swanlab.define_metric("overview/*", section_name="总览")
                state.update(run_id=run.id, url=run.url, run_path=run.path)
                save(statepath, state)
            status = json.loads((args.run/'status.json').read_text())
            step = max(int(state.get('step', -1)) + 1, int((status['updated']-status['started']) // args.interval))
            counts = status['counts']
            values = capacity_metrics(counts)
            values.update({'nodes/ready':status['nodes_ready'], 'queue/active':status['active'],
                           'queue/pending_copy':status['pending'].get('copy', 0),
                           'queue/pending_scan':status['pending'].get('scan', 0),
                           'jobs/discovered':status['discovered_jobs'], 'jobs/finished':status['finished_jobs'],
                           'time/elapsed_hours':(status['updated']-status['started'])/3600,
                           'time/status_age_seconds':time.time()-status['updated'],
                           'health/node_failures':len(status['failures']),
                           'health/copying':int(status['state']=='copying')})
            for window, rates in status['rates'].items():
                for key in ('write_Bps','copied_Bps'):
                    values['rate_'+window+'s/'+key[:-4]+'_TB_per_second'] = rates[key] / 1e12
                values['rate_'+window+'s/files_ps'] = rates['files_ps']
            for node, info in status['workers'].items():
                values['node/'+node+'/copied_TB'] = info['counts'].get('copied_bytes', 0)/1e12
                values['node/'+node+'/heartbeat_age_seconds'] = info['age']
            values.update(overview_metrics(status))
            values["overview/总进度看板"] = swanlab.echarts.Table().add(
                ["指标", "当前值", "口径说明"], overview_rows(status))
            # One combined batch per interval; overview table has no hardware probes.
            run.log(values, step=step)
            state.update(step=step, enqueued_at=time.time(), state='streaming', pid=os.getpid(),
                         interval_seconds=args.interval, source_status_updated=status['updated'],
                         capacity_unit='TB', bytes_per_TB=10**12,
                         overview_enabled=True, total_scope_known=False)
            state.pop('error_type', None)
            state.pop('retry_seconds', None)
            save(statepath, state)
            if time.time() >= next_confirm:
                try:
                    summary = swanlab.Api().run(run.path.strip('/')).summary(keys=['jobs/finished','copy/copied_TB'])
                    state.update(server_confirmed_steps={k:int(v['step']) for k,v in summary.items()
                        if isinstance(v,dict) and 'step' in v}, confirmed_at=time.time())
                    state.pop('confirmation_error_type', None)
                except Exception as error:
                    state['confirmation_error_type'] = type(error).__name__
                save(statepath, state)
                next_confirm = time.time()+900
            backoff = args.interval
            if status['state'] in ('copy_pass_finished','copied_with_attention','workers_unavailable','stopped','failed'):
                run.finish()
                state.update(state='finished', finished_at=time.time())
                save(statepath, state)
                return
        except Exception as error:
            # No exception messages/tracebacks: proxy/credential values may appear there.
            state.update(state='retrying', error_type=type(error).__name__, updated=time.time(),
                         retry_seconds=backoff, pid=os.getpid())
            save(statepath, state)
            backoff = min(3600, backoff*2)
        time.sleep(backoff)


if __name__ == '__main__':
    main()
