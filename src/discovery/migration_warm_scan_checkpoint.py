"""Read-only preparation of the current cold scan's append-only inventory.

PREPARATION ONLY: a prefix is not a traversal-completion proof.  Deriving a
pruning candidate requires the controller to first quiesce every source reader,
drain all accepted probes/publications, and verify a graceful foreground exit
has flushed the Python manifest buffer.  This module never signals processes,
starts a coordinator, edits the live bundle, or publishes migration tasks.
"""
import argparse
from collections import deque
import gc
import hashlib
import json
import os
from pathlib import Path
import pickle
import shutil
import socket
import stat
import time
import uuid

from migration_io import read_json, write_json
from migration_parallel_scan import _seed
from migration_scan import _Scanner
from migration_worker import _components


IDENTITY_FIELDS = ('dev', 'ino', 'mtime_ns', 'ctime_ns')
MAX_RECORD = 1024 * 1024
MAX_UNOFFERED_FILES = 512  # All allowed cold-frontier batch sizes are <= 512.
SCOPE = 'cold-current-source-scan-prefix-v1'


def directory_identity(entry):
    values = tuple(entry.get(k) for k in IDENTITY_FIELDS)
    if (entry.get('kind') != 'directory' or
            any(type(v) is not int for v in values) or
            type(entry.get('mode')) is not int or not stat.S_ISDIR(entry['mode'])):
        raise ValueError('complete directory identity required')
    return values


class Prefix:
    def __init__(self):
        self.paths = set()
        self.directories = {}
        self.parents = set()
        self.error_parents = set()
        self.tail = deque(maxlen=MAX_UNOFFERED_FILES)
        self.scanner = _Scanner(None, None, 1024**3, 256, {}, None, 3)
        self.root_identity = None
        self.records = 0

    def add(self, entry):
        if not isinstance(entry, dict):
            raise ValueError('inventory record must be an object')
        rel, kind = entry.get('rel'), entry.get('kind')
        if rel != '':
            _components(rel)
        elif kind not in ('directory', 'error'):
            raise ValueError('only a directory/error may have an empty path')
        if kind == 'error':
            self.error_parents.add(rel.rpartition('/')[0])
            # An open/scandir failure can name the directory itself.
            self.error_parents.add(rel)
        else:
            if rel in self.paths:
                raise ValueError('current cold inventory repeats a successful path')
            if kind not in ('directory', 'file', 'symlink') and not entry.get('unsupported'):
                raise ValueError('unrecognized current inventory kind')
            if kind == 'directory':
                identity = directory_identity(entry)
                self.directories[rel] = identity
                if rel == '':
                    self.root_identity = identity[:2]
            if rel:
                self.parents.add(rel.rpartition('/')[0])
            if kind in ('file', 'symlink') and not entry.get('hardlink_to'):
                self.tail.append(entry)
            self.paths.add(rel)
        _seed(self.scanner, entry)
        self.records += 1

    def pruning_candidates(self, active_directories, *, quiescent_and_flushed=False):
        """Derive candidates ONLY AFTER an independently verified cutover.

        A discovered directory with no child record might never have been
        entered. A nonempty one was completely enumerated only if it is not
        an active reader frame or an error directory. Retain every ancestor
        of uncertain work, not just the uncertain directories themselves.

        The boolean is deliberately not a production verifier. Callers must
        obtain the actual reader/PID, drain and flush evidence described above.
        A prepared prefix alone must never authorize pruning.
        """
        if quiescent_and_flushed is not True:
            raise ValueError('prefix alone is not a quiescent traversal proof')
        active = set(active_directories)
        for rel in active:
            if rel:
                _components(rel)
        uncertain = (self.directories.keys() - self.parents) | self.error_parents | active
        retained = {''}
        for rel in uncertain:
            while rel not in retained:
                retained.add(rel)
                rel = rel.rpartition('/')[0]
        return {rel: identity for rel, identity in self.directories.items()
                if rel not in retained}

    def containers(self):
        return {'paths': self.paths, 'directories': self.directories,
                'parents': self.parents, 'error_parents': self.error_parents,
                'tail': list(self.tail), 'root_identity': self.root_identity,
                'hardlinks': self.scanner.hardlinks, 'stats': self.scanner.stats,
                'records': self.records}


def read_prefix(stream, limit, prefix, digest, *, report=None):
    """Consume complete records wholly within an observed append-only prefix."""
    offset = stream.tell()
    if type(limit) is not int or limit < offset:
        raise ValueError('invalid inventory prefix boundary')
    while stream.tell() < limit:
        line = stream.readline(MAX_RECORD + 1)
        if len(line) > MAX_RECORD:
            raise ValueError('inventory record exceeds bounded limit')
        if not line.endswith(b'\n') or stream.tell() > limit:
            stream.seek(offset)
            break
        prefix.add(json.loads(line))
        digest.update(line)
        offset = stream.tell()
        if report and prefix.records % 32768 == 0:
            report(offset=offset, records=prefix.records, paths=len(prefix.paths),
                   directories=len(prefix.directories))
    return offset


def process_identity(pid):
    p = Path('/proc') / str(pid)
    fields = (p / 'stat').read_text().rsplit(') ', 1)[1].split()
    return {'pid': pid, 'start_ticks': fields[19], 'state': fields[0],
            'argv': (p / 'cmdline').read_bytes().decode().rstrip('\0').split('\0')}


def validate_runtime(control, expected):
    cfg = read_json(control / 'config.json')
    if (cfg.get('control') != str(control) or control.name != '.' + cfg.get('run_id', '')
            or expected.get('scope') != SCOPE or expected.get('control') != str(control)
            or expected.get('run_id') != cfg.get('run_id')
            or expected.get('job') != cfg.get('job')
            or cfg.get('cold_frontier_v1') is not True
            or cfg.get('master_host') != socket.gethostname()
            or cfg.get('master_host') != 'job-' + str(cfg.get('job')) + '-master-0'
            or (cfg.get('source'), cfg.get('destination')) !=
               ('/share/project/eai_pwm', '/share-mixed/project/eai_pwm')):
        raise ValueError('preparation requires the actual current cold-recovery master')
    if (control / 'STOP').exists():
        raise RuntimeError('STOP is present')
    state = read_json(control / 'state.json')
    fast = expected.get('entry_kind') == 'fast-p3-scan'
    if (state.get('status') != 'scanning' or state.get('stage') != 'p3-r3'
            or state.get('recovery_warm') is not True or state.get('scan_complete')
            or (not fast and state.get('recovery_label') != '旧清单供给结束；补充当前源目录变化')
            or read_json(control / 'pipeline.json').get('stages')):
        raise RuntimeError('current producer is no longer in the same cold full scan')
    role = read_json(control / 'recovery-roles' / (cfg['master_host'] + '-producer.json'))
    actual = process_identity(role['pid'])
    expected_argv = [role['argv'][0], '-u', str(control / 'migration_cold_frontier.py'),
                'run', '--control', str(control), '--warm-tasks', str(control / 'warm-tasks.jsonl')]
    if fast:
        bundle = Path(expected['source_bundle'])
        if (bundle.parent != control or bundle.is_symlink() or not bundle.name.startswith('fast-p3-scan-')
                or state.get('foreground_handover') != str(bundle/'handover.json')
                or state.get('source_scan_acceleration',{}).get('processes') != expected.get('source_reader_count')):
            raise ValueError('fast scan lineage/runtime budget differs')
        expected_argv = expected['producer']['argv']
        if (len(expected_argv)<3 or expected_argv[:3] !=
                ['/usr/bin/python3','-u',str(bundle/'migration_fast_cold_resume.py')]
                or '--control' not in expected_argv or
                expected_argv[expected_argv.index('--control')+1] != str(control)):
            raise ValueError('fast scan exact entry/control differs')
        if (bundle/'source-sha256.json').is_symlink():
            raise ValueError('fast scan source manifest must not be a symlink')
        raw_active=(bundle/'source-sha256.json').read_bytes()
        if hashlib.sha256(raw_active).hexdigest()!=expected['frozen_active_manifest_sha256']:
            raise ValueError('fast scan source manifest changed')
        for name,checksum in json.loads(raw_active).items():
            path=bundle/name
            if Path(name).name!=name or path.is_symlink() or hashlib.sha256(path.read_bytes()).hexdigest()!=checksum:
                raise ValueError('fast scan frozen source changed')
    if (actual['state'] not in ('R', 'S', 'D', 'I') or actual['argv'] != expected_argv
            or role['argv'] != expected_argv or
            str(role.get('start_ticks')) != actual['start_ticks'] or
            {k: actual[k] for k in ('pid', 'start_ticks', 'argv')} != expected.get('producer')):
        raise RuntimeError('live cold producer identity differs')
    if (control / 'source-sha256.json').is_symlink():
        raise ValueError('frozen runtime manifest must not be a symlink')
    raw = (control / 'source-sha256.json').read_bytes()
    if hashlib.sha256(raw).hexdigest() != expected.get('frozen_runtime_manifest_sha256'):
        raise ValueError('frozen runtime manifest no longer matches preparation intent')
    manifest = json.loads(raw)
    if not isinstance(manifest, dict) or not {
            'migration_cold_frontier.py', 'migration_process_scan.py',
            'migration_parallel_scan.py', 'migration_scan.py'} <= manifest.keys():
        raise ValueError('frozen runtime manifest is incomplete')
    for name, checksum in manifest.items():
        if (Path(name).name != name or (control / name).is_symlink()
                or hashlib.sha256((control / name).read_bytes()).hexdigest() != checksum):
            raise ValueError('frozen runtime source changed')
    return cfg, state, actual


def resource_budget(source_bytes):
    """Check reclaimable headroom, not misleading host-level free RAM."""
    estimate = 2 * source_bytes + 2 * 1024**3
    root = Path('/sys/fs/cgroup')
    if (root / 'memory.max').exists() and (root / 'memory.stat').exists():
        maximum = (root / 'memory.max').read_text().strip()
        if maximum != 'max':
            values = dict(line.split() for line in (root / 'memory.stat').read_text().splitlines())
            noncache = sum(int(values.get(k, 0)) for k in (
                'anon', 'shmem', 'pagetables', 'kernel_stack', 'slab_unreclaimable', 'sock'))
            if noncache + estimate > int(maximum) * .9:
                raise RuntimeError('insufficient cgroup anonymous-memory headroom for preparation')
    if shutil.disk_usage('/tmp').free < estimate:
        raise RuntimeError('insufficient private scratch capacity for preparation')
    return estimate


def prepare(control, directory, expected):
    control, directory = Path(control).resolve(), Path(directory).resolve()
    if directory.parent != control or not directory.name.startswith('warm-scan-checkpoint-'):
        raise ValueError('checkpoint must use its own direct child of this control')
    cfg, state, producer = validate_runtime(control, expected)
    source = control / 'manifests/recovery-missing.jsonl'
    if source.is_symlink():
        raise ValueError('current inventory must not be a symlink')
    estimate = resource_budget(source.stat().st_size)
    directory.mkdir(mode=0o700)  # One shot; never overwrite another preparation.
    scratch = Path('/tmp') / (cfg['run_id'] + '-scan-prefix-' + uuid.uuid4().hex)
    scratch.mkdir(mode=0o700)
    cache = scratch / 'prefix.pickle'
    began, last = time.monotonic(), [0.0]
    record = {'scope': SCOPE, 'control': str(control), 'run_id': cfg['run_id'],
              'producer': producer, 'source_manifest': str(source),
              'status': 'reading_prefix', 'safe_to_prune': False,
              'quiescence_verified': False, 'scratch': str(scratch),
              'conservative_memory_estimate': estimate}

    def report(**values):
        record.update(values)
        if time.monotonic() - last[0] >= 5 or values.get('status'):
            record.update(updated=time.time(), elapsed_seconds=time.monotonic() - began)
            write_json(directory / 'preparation.json', record)
            print(json.dumps(record), flush=True)
            last[0] = time.monotonic()

    was_enabled = gc.isenabled()
    gc.disable()  # Private acyclic containers, never the live producer's GC.
    try:
        prefix, checksum = Prefix(), hashlib.sha256()
        with source.open('rb') as stream:
            info = os.fstat(stream.fileno())
            report(source_manifest_dev=info.st_dev, source_manifest_ino=info.st_ino,
                   prefix_limit=info.st_size)
            offset = read_prefix(stream, info.st_size, prefix, checksum, report=report)
        root = os.stat(cfg['source'], follow_symlinks=False)
        if prefix.root_identity != (root.st_dev, root.st_ino):
            raise RuntimeError('source root identity changed')
        current = source.stat()
        if ((current.st_dev, current.st_ino) != (info.st_dev, info.st_ino)
                or current.st_size < offset):
            raise RuntimeError('source inventory was replaced or truncated')
        report(status='saving_private_cache', offset=offset,
               prefix_sha256=checksum.hexdigest(), records=prefix.records,
               paths=len(prefix.paths), directories=len(prefix.directories))
        with cache.open('xb') as stream:
            os.chmod(cache, 0o600)
            pickle.dump(prefix.containers(), stream, protocol=5)
            stream.flush()
            os.fsync(stream.fileno())
        digest = hashlib.sha256()
        with cache.open('rb') as stream:
            for block in iter(lambda: stream.read(8 * 1024 * 1024), b''):
                digest.update(block)
        cfg_after, _, actual = validate_runtime(control, expected)
        if (cfg_after['run_id'] != cfg['run_id'] or
                {k: actual[k] for k in ('pid', 'start_ticks', 'argv')} !=
                {k: producer[k] for k in ('pid', 'start_ticks', 'argv')}):
            raise RuntimeError('foreground identity changed during preparation')
        report(status='prepared_prefix_only', cache=str(cache), cache_sha256=digest.hexdigest(),
               root_identity=list(prefix.root_identity), stats=prefix.scanner.stats,
               source_code_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
               frozen_runtime_manifest_sha256=hashlib.sha256((control / 'source-sha256.json').read_bytes()).hexdigest())
        return record
    except BaseException as exc:
        report(status='failed_preparation_only', error=type(exc).__name__ + ': ' + str(exc))
        raise
    finally:
        if was_enabled:
            gc.enable()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--control', required=True)
    parser.add_argument('--directory', required=True)
    parser.add_argument('--expected-runtime', required=True)
    args = parser.parse_args()
    os.umask(0o077)
    prepare(args.control, args.directory, read_json(Path(args.expected_runtime)))
