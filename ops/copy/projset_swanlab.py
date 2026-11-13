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


def recovery_log_progress(run, status, proc_root=Path('/proc'), now=None):
    """Measure the current journal stage, not the whole copy progress."""
    if status.get('state') != 'recovering':
        return None
    stage = status.get('recovery_stage')
    kind = {'completed-index': 'completed', 'tasks': 'tasks'}.get(stage)
    if not kind and stage != 'receipts-parallel':
        return None
    try:
        supervisor = json.loads((run/'recovery'/'supervisor.json').read_text())
        now = time.time() if now is None else now
        if supervisor.get('state') != 'running' or not 0 <= now-supervisor['updated'] <= 60:
            return None
        pid = int(supervisor['controller_pid'])
        proc = proc_root/str(pid)
        argv = (proc/'cmdline').read_bytes().split(b'\0')
        if (b'controller' not in argv or str(run).encode() not in argv or
                not any(x.endswith(b'/projset_recovery_copy.py') for x in argv)):
            return None
        if stage == 'receipts-parallel':
            attempt = supervisor['attempt']
            if not isinstance(attempt, str) or not attempt or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_' for c in attempt):
                return None
            folder = run/'recovery'/('receipt-shards-'+attempt)
            manifest = json.loads((folder/'manifest.json').read_text())
            if manifest['attempt'] != attempt:
                return None
            node_totals = manifest['node_total_bytes']
            total = int(manifest['total_bytes'])
            if total <= 0 or any(int(x) < 0 for x in node_totals.values()) or sum(int(x) for x in node_totals.values()) != total:
                return None
            done = 0
            for node, limit in node_totals.items():
                if not node or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_' for c in node):
                    return None
                for suffix in ('.result.json', '.progress.json'):
                    try:
                        item = json.loads((folder/(node+suffix)).read_text())
                        if item['node'] != node or int(item['total_bytes']) != int(limit):
                            return None
                        n = int(item['done_bytes'])
                        if not 0 <= n <= int(limit):
                            return None
                        done += n
                        break
                    except FileNotFoundError:
                        continue
            return {'stage': stage, 'done_TB': done/1e12, 'total_TB': total/1e12,
                    'percent': 100*done/total}
        paths = [run/(kind+'.jsonl')] + sorted((run/'recovery').glob(kind+'.*.jsonl'))
        sizes = [(path, path.stat().st_size) for path in paths]
        sizes = [(path, size) for path, size in sizes if size > 0]
        total = sum(size for _, size in sizes)
        if not total:
            return None
        for fd in (proc/'fd').iterdir():
            try:
                target = Path(os.readlink(fd))
                if target.name != status.get('recovery_file'):
                    continue
                info = (proc/'fdinfo'/fd.name).read_text().splitlines()
                pos = int(next(line.split()[1] for line in info if line.startswith('pos:')))
                preceding = 0
                for path, size in sizes:
                    if path == target:
                        done = preceding + min(max(pos, 0), size)
                        return {'stage': status['recovery_stage'], 'done_TB': done/1e12,
                                'total_TB': total/1e12, 'percent': 100*done/total}
                    preceding += size
            except (OSError, ValueError, StopIteration):
                continue
    except (OSError, ValueError, KeyError, TypeError, OverflowError, AttributeError):
        pass
    return None


def prefetch_progress(run, now=None):
    """Report read-only receipt prefetch separately from copied data."""
    try:
        state = json.loads((run/'recovery'/'prefetch-status.json').read_text())
        name = state['state']
        if name not in ('running', 'ready', 'error'):
            return None
        try:
            reuse = json.loads((run/'recovery'/'prefetch-reuse.json').read_text())
            if reuse.get('attempt') == state.get('parent_attempt'):
                if reuse.get('state') == 'used':
                    name = 'used'
                elif reuse.get('state') == 'rejected':
                    name = 'rejected'
        except (OSError, ValueError, TypeError, AttributeError):
            pass
        now = time.time() if now is None else now
        progress = {'state': name, 'age_seconds': max(0, now-float(state['updated']))}
        if name in ('running', 'ready', 'used'):
            total = int(state['total_bytes'])
            done = total if name in ('ready', 'used') else int(state['done_bytes'])
            if total <= 0 or not 0 <= done <= total:
                return None
            progress.update(done_TB=done/1e12, total_TB=total/1e12, percent=100*done/total)
        return progress
    except (OSError, ValueError, KeyError, TypeError, OverflowError, AttributeError):
        return None


def supplemental_metrics(run):
    """Summarize independent file retries without changing main-copy totals."""
    records = []
    for status_path in (run / 'recovery').glob('*retry*/status.json'):
        try:
            status = json.loads(status_path.read_text())
            manifest = json.loads((status_path.parent / 'manifest.json').read_text())
            counts = status['counts']
            fields = ('verified_bytes', 'copied', 'skipped', 'error', 'changed', 'conflict', 'unsupported')
            numbers = {key: int(counts.get(key, 0)) for key in fields}
            total_files = int(status['total_files'])
            entries = manifest['entries']
            total_bytes = sum(int(entry['size']) for entry in entries)
            if (total_files != len(entries) or total_files < 0 or total_bytes < 0 or
                    any(value < 0 for value in numbers.values()) or
                    numbers['copied'] + numbers['skipped'] > total_files or
                    numbers['verified_bytes'] > total_bytes):
                continue
            records.append((status.get('state'), numbers, total_files, total_bytes))
        except (OSError, ValueError, KeyError, TypeError, AttributeError):
            continue
    if not records:
        return {}
    return {
        'retry/verified_TB': sum(counts['verified_bytes'] for _, counts, _, _ in records) / 1e12,
        'retry/total_TB': sum(total for _, _, _, total in records) / 1e12,
        'retry/verified_files': sum(counts['copied'] + counts['skipped']
                                    for _, counts, _, _ in records),
        'retry/total_files': sum(total for _, _, total, _ in records),
        'retry/attention_files': sum(sum(counts.get(key, 0) for key in ('error', 'changed', 'conflict', 'unsupported'))
                                     for _, counts, _, _ in records),
        'retry/running': sum(state == 'running' for state, _, _, _ in records),
    }


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
            recovery_progress = recovery_log_progress(args.run, status)
            if recovery_progress is not None:
                status['recovery_log_progress'] = recovery_progress
            prefetch = prefetch_progress(args.run)
            if prefetch is not None:
                status['prefetch_progress'] = prefetch
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
            if recovery_progress is not None:
                values.update({'recovery/当前阶段日志已读_TB': recovery_progress['done_TB'],
                               'recovery/当前阶段日志总量_TB': recovery_progress['total_TB'],
                               'recovery/当前阶段日志读取百分比': recovery_progress['percent']})
            if prefetch is not None:
                values['recovery/回执预读状态'] = {'running': 1, 'ready': 2, 'used': 3,
                                                'error': -1, 'rejected': -1}[prefetch['state']]
                values['recovery/回执预读状态延迟秒'] = prefetch['age_seconds']
                if 'percent' in prefetch:
                    values.update({'recovery/回执预读已读_TB': prefetch['done_TB'],
                                   'recovery/回执预读总量_TB': prefetch['total_TB'],
                                   'recovery/回执预读百分比': prefetch['percent']})
            values.update(supplemental_metrics(args.run))
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
