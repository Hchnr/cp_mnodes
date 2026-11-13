#!/usr/bin/env python3
"""Read historical receipts ahead of queue rebuild without copying source data."""
import argparse
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import socket
import time
import uuid


def load(name):
    path = Path(__file__).with_name(name+'.py')
    spec = importlib.util.spec_from_file_location('prefetch_'+name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


copy = load('projset_recovery_copy')
parallel = load('projset_parallel_receipts')


def last_complete_task_id(path):
    size = path.stat().st_size
    if not size:
        return None
    start = max(0, size-4*1024*1024)
    with path.open('rb') as stream:
        stream.seek(start)
        data = stream.read()
    rows = data.split(b'\n')
    if start:
        rows.pop(0)  # The first line can start before the tail window.
    rows.pop()  # Empty after a newline, otherwise an incomplete final append.
    for line in reversed(rows):
        if line:
            value = json.loads(line)['id']
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise ValueError('invalid task ID in '+str(path))
            return value
    raise ValueError('no complete task found in tail: '+str(path))


def candidate_jobs(run, snapshot):
    """Conservative candidate superset; actual pending IDs are checked later."""
    if snapshot.get('state') != 'copying' or snapshot['pending'].get('copy') != 0:
        raise ValueError('snapshot has an unknown queued-copy frontier')
    last = snapshot['discovered_jobs']
    if not isinstance(last, int) or isinstance(last, bool) or last < 0:
        raise ValueError('invalid snapshot task ID')
    all_active, copies = set(), set()
    for worker in snapshot['workers'].values():
        for raw, kind in worker.get('active', {}).items():
            job = int(raw)
            if job in all_active or job < 0 or job > last or kind not in ('copy', 'scan'):
                raise ValueError('inconsistent active snapshot')
            all_active.add(job)
            if kind == 'copy':
                copies.add(job)
    if len(all_active) != snapshot['active']:
        raise ValueError('active snapshot does not match its total')
    paths = [run/'tasks.jsonl'] + sorted((run/'recovery').glob('tasks.*.jsonl'))
    ends = [value for value in (last_complete_task_id(path) for path in paths) if value is not None]
    if not ends:
        raise ValueError('no task logs')
    final = max(ends)
    if final < last or final-last > 100_000:
        raise ValueError('unbounded or inconsistent post-snapshot task range')
    return copies | set(range(last+1, final+1)), last, final


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', type=Path, required=True)
    parser.add_argument('--parent-attempt', required=True)
    args = parser.parse_args()
    run = args.run.resolve()
    root = run/'recovery'
    lock = open('/var/tmp/'+run.name+'-receipt-prefetch.lock', 'a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    if copy.stop_requested(run):
        raise RuntimeError('cluster fence unavailable or stop requested')
    supervisor = json.loads((root/'supervisor.json').read_text())
    if (supervisor.get('state') != 'running' or supervisor.get('attempt') != args.parent_attempt or
            time.time()-supervisor['updated'] > 30):
        raise RuntimeError('parent controller is not the current recovery')
    cfg = json.loads((root/'ssh.json').read_text())
    if socket.gethostname() != cfg['hosts']['cpu-master-0']:
        raise RuntimeError('prefetch must run on the fixed recovery anchor')
    parent = json.loads((root/'controller-child.json').read_text())
    pid = int(supervisor['controller_pid'])
    proc = Path('/proc')/str(pid)
    stat = (proc/'stat').read_text().rsplit(') ', 1)[1].split()
    argv = (proc/'cmdline').read_bytes().split(b'\0')
    if (parent['pid'] != pid or parent['host'] != socket.gethostname() or
            parent['start'] != stat[19] or stat[0] == 'Z' or b'controller' not in argv or
            str(root/'code/projset_recovery_copy.py').encode() not in argv):
        raise RuntimeError('parent controller identity differs')
    fence = json.loads((root/'legacy-cluster-fence.json').read_text())
    handover = Path(fence['handover_dir'])
    snapshot_bytes = (handover/'status.json').read_bytes()
    manifest = json.loads((handover/'manifest.json').read_text())
    snapshot_sha = hashlib.sha256(snapshot_bytes).hexdigest()
    if snapshot_sha != manifest['files']['status.json']['sha256']:
        raise ValueError('handover snapshot digest differs')
    snapshot = json.loads(snapshot_bytes)
    candidate, snapshot_id, final_id = candidate_jobs(run, snapshot)
    original_path = root/'original-counts.json'
    original_bytes = original_path.read_bytes()
    original = json.loads(original_bytes)
    if set(original) != set(cfg['hosts']):
        raise ValueError('prefetch node set differs')
    attempt = 'prefetch-'+time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())+'-'+uuid.uuid4().hex[:8]
    output = root/('receipt-shards-'+attempt)
    os.environ.update(PROJSET_ATTEMPT=attempt, PROJSET_PREFETCH_MODE='1',
                      PROJSET_SSH_TOPOLOGY=str(root/'ssh.json'))
    base = {'attempt': attempt, 'parent_attempt': args.parent_attempt,
            'snapshot_id': snapshot_id, 'final_task_id': final_id,
            'candidate_jobs': len(candidate), 'pid': os.getpid(), 'host': socket.gethostname()}
    status_path = root/'prefetch-status.json'
    def report(stage, path='', seen=0, force=False):
        if copy.stop_requested(run):
            raise RuntimeError('stop requested during prefetch')
        current = json.loads((run/'status.json').read_text())
        current_supervisor = json.loads((root/'supervisor.json').read_text())
        if (current['state'] != 'recovering' or current_supervisor.get('attempt') != args.parent_attempt or
                current.get('recovery_stage') not in ('completed-index', 'tasks', 'validate-completions')):
            raise RuntimeError('main recovery moved past the prefetch window')
        result = dict(base, state='running', stage=stage, records=seen, updated=time.time())
        try:
            m = json.loads((output/'manifest.json').read_text())
            done, finished = 0, 0
            for node in original:
                for suffix in ('.result.json', '.progress.json'):
                    try:
                        item = json.loads((output/(node+suffix)).read_text())
                        done += int(item['done_bytes'])
                        finished += suffix == '.result.json'
                        break
                    except FileNotFoundError:
                        continue
            result.update(done_bytes=done, total_bytes=m['total_bytes'], nodes_finished=finished)
        except (OSError, ValueError, KeyError, TypeError):
            pass
        parallel.save(status_path, result)

    class Sink:
        def execute(self, query, values):
            return None

    def stop(signum, frame):
        raise InterruptedError('prefetch interrupted')

    signal.signal(signal.SIGTERM, stop)
    try:
        baseline = parallel.rebuild_receipts(run, Sink(), candidate, original, report, copy.ssh)
        pointer = {'state': 'ready', 'parent_attempt': args.parent_attempt,
                   'prefetch_attempt': attempt, 'generation': os.environ.get('PROJSET_RECOVERY_GENERATION'),
                   'candidate_sha256': hashlib.sha256((output/'pending.json').read_bytes()).hexdigest(),
                   'original_sha256': hashlib.sha256(original_bytes).hexdigest(),
                   'snapshot_sha256': snapshot_sha, 'updated': time.time()}
        os.environ['PROJSET_ATTEMPT'] = args.parent_attempt
        checked = parallel.reuse_prefetch(run, Sink(), candidate, original, pointer)
        if checked != baseline:
            raise ValueError('prefetch baseline verification differs')
        report('verified', seen=sum(json.loads((output/(n+'.result.json')).read_text())['records'] for n in original), force=True)
        parallel.save(root/'prefetched-receipts.json', pointer)
        parallel.save(status_path, dict(base, state='ready', updated=time.time(),
                                       total_bytes=json.loads((output/'manifest.json').read_text())['total_bytes']))
    except Exception as error:
        parallel.save(status_path, dict(base, state='error', updated=time.time(), error=repr(error)))
        raise


if __name__ == '__main__':
    main()
