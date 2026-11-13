#!/usr/bin/env python3
"""Correct archive dashboard semantics without erasing earlier metric history."""
import argparse
import fcntl
import importlib.util
import json
import os
from pathlib import Path
import time
import uuid


def atomic(path, value):
    tmp = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    with tmp.open('x') as stream:
        json.dump(value, stream, ensure_ascii=True); stream.flush(); os.fsync(stream.fileno())
    os.replace(tmp, path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--stale-retry', type=Path)
    args = parser.parse_args()
    work = args.work
    with (work / 'reporter.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        status = json.loads((work / 'status.json').read_text())
        state_path = work / 'swanlab.json'
        state = json.loads(state_path.read_text())
        if status.get('state') != 'finished' or status['done'] != status['total'] or state.get('state') != 'finished':
            raise ValueError('work or reporter not finished')
        mode = status.get('mode', 'review')
        if mode == 'archive':
            if status['results'].get('archived_source') != status['total']:
                raise ValueError('archive has unresolved results')
            metrics = {'attention/当前需决策项': status['total'],
                       'archive/原位版本待决定项': status['total']}
            name = 'projset-冲突源版本归档-' + work.name[-8:]
            reason = 'saving the source does not decide the destination version'
        elif mode == 'review' and args.stale_retry is not None:
            retry = json.loads((args.stale_retry / 'status.json').read_text())
            if (retry.get('state') != 'finished' or retry['done'] != retry['total'] or
                    retry['results'].get('scan_verified') != retry['total']):
                raise ValueError('stale directory retry is not verified')
            old_rows, new_rows = [], []
            for path in (work / 'workers').glob('*/receipts.jsonl'):
                with path.open() as stream:
                    old_rows.extend(json.loads(line) for line in stream if 'manual_both_directories' in line)
            for path in (args.stale_retry / 'workers').glob('*/receipts.jsonl'):
                with path.open() as stream:
                    new_rows.extend(json.loads(line) for line in stream if 'scan_verified' in line)
            old_keys = {(x['rel'], x['receipt_path'], str(x['receipt_offset'])) for x in old_rows}
            new_keys = {(x['rel'], x['receipt_path'], str(x['receipt_offset'])) for x in new_rows}
            if len(old_keys) != retry['total'] or old_keys != new_keys:
                raise ValueError('retry does not cover the stale directory conflict')
            metrics = {'attention/当前需决策项': status['conflicts_total'] - len(old_keys),
                       'attention/NFS目录错误已重试': len(old_keys)}
            name = 'projset-历史错误与冲突复核-' + work.name[-8:]
            reason = 'the stale-handle directory was rescanned and checked'
        else:
            raise ValueError('unsupported correction scope')
        remaining = state['enqueued_at'] + 120 - time.time()
        if remaining > 0:
            time.sleep(remaining)
        for key in ('https_proxy', 'http_proxy', 'HTTPS_PROXY', 'HTTP_PROXY', 'ALL_PROXY', 'all_proxy'):
            os.environ.pop(key, None)
        secrets = Path('/share-mixed/project/eai_pwm/.secrets')
        proxy = (secrets / 'swanlab_proxy').read_text().strip()
        os.environ.update({key: proxy for key in ('https_proxy', 'http_proxy', 'HTTPS_PROXY', 'HTTP_PROXY')})
        os.environ['SWANLAB_API_KEY'] = (secrets / 'swanlab_api_key').read_text().strip()
        import swanlab
        swanlab.merge_settings(swanlab.Settings(interactive=False, probe=swanlab.Settings.Probe(
            hardware=False, runtime=False, requirements=False, conda=False, git=False, swanlab=False, monitor=False)))
        swanlab.login(api_key=os.environ['SWANLAB_API_KEY'], save=False, timeout=10)
        run = swanlab.init(mode='online', project='data-migration',
                           name=name, id=state['run_id'],
                           resume='allow', public=False, log_dir=str(work / 'swanlab-sdk'))
        if run.id != state['run_id']:
            raise ValueError('resumed unexpected SwanLab run')
        step = int(state['step']) + 1
        source = Path(__file__).resolve().with_name('projset_attention_recheck.py')
        spec = importlib.util.spec_from_file_location('attention_cadence', source)
        module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
        with module.swanlab_upload_slot(work) as reserved:
            state.update(state='sending_correction', step=step, enqueued_at=reserved)
            atomic(state_path, state)
            run.log(metrics, step=step)
        run.finish()
        state.update(state='finished', finished_at=time.time(), correction=reason)
        atomic(state_path, state)
        atomic(work / 'swanlab-correction.json', {'state': 'finished', 'step': step,
               'updated': time.time(), 'reason': state['correction']})


if __name__ == '__main__':
    main()
