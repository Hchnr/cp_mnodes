"""UNDEPLOYED P4 receipt-index component; not a bootstrap or coordinator.

Reuse the existing bounded, ordered 64-reader index builder in a private local
candidate. Validate exact P4 ancestry before reading receipts and again before
publication. Only same-root P4 proof may reach the requested new local index.
No existing index, source data, destination data or ancestor proof is changed.
The eventual launcher must check the memory TEMP budget before using this
builder. Candidates now carry an exact P4 scope marker for the separate bounded
INSERT-IGNORE merge primitive; runtime connection identity/locks and integration
with the coordinator remain unimplemented.
"""
import copy
from contextlib import closing
import os
from pathlib import Path
import re
import stat
import uuid

from migration_io import read_json
from migration_priority4_worker import ScopeGuard
from migration_priority4_index_merge import seal_receipt_candidate
from migration_resume_index_parallel_memory import build_resume_index as parallel_build


_FINGERPRINT_KEYS = ('source', 'destination', 'migration_scope', 'exclude', 'run_id',
                     'resume_from', 'covered_by_controls', 'protected_destination_relatives')


def _stages(control):
    root = control / 'stages'
    try:
        current = root.lstat()
    except FileNotFoundError:
        return
    if not stat.S_ISDIR(current.st_mode):
        raise ValueError('P4 stages must be a real directory')
    for path in root.iterdir():
        if path.name == '.snapshots':
            continue
        if not stat.S_ISDIR(path.lstat().st_mode):
            # The base reader also refuses to traverse symlink stage trees.
            continue
        if not re.fullmatch(r'p4-r[123]', path.name):
            raise ValueError('non-P4 stage in P4 resume control: ' + path.name)


def validate_chain(control):
    """Return newest-first controls/guards/fingerprints; no SQLite opened."""
    current = Path(os.path.abspath(control))
    controls, guards, fingerprints = [], {}, {}
    while True:
        if current in guards:
            raise ValueError('P4 resume_from cycle')
        if len(controls) >= 16:
            raise ValueError('P4 resume chain exceeds16 controls')
        if not stat.S_ISDIR(current.lstat().st_mode):
            raise ValueError('P4 resume control must be a real directory')
        cfg = read_json(current / 'config.json')
        guard = ScopeGuard(str(current), cfg)
        _stages(current)
        controls.append(current)
        guards[current] = guard
        fingerprints[current] = copy.deepcopy({key: cfg.get(key) for key in _FINGERPRINT_KEYS})
        previous = cfg.get('resume_from')
        if previous is None or previous == '':
            break
        if not isinstance(previous, str) or not os.path.isabs(previous):
            raise ValueError('P4 resume_from requires an absolute same-scope control path')
        current = Path(os.path.abspath(previous))
    return controls, guards, fingerprints


def local_target(db_path):
    target = Path(os.path.abspath(db_path))
    if target.exists() or target.is_symlink():
        raise FileExistsError(str(target))
    local = Path(os.path.realpath('/tmp'))
    parent = Path(os.path.realpath(target.parent))
    if parent != local and not parent.is_relative_to(local):
        raise ValueError('P4 receipt index must be on local /tmp, outside shared storage')
    return target


def build_resume_index(control, db_path, progress_callback=None,
                       progress_interval=5.0, read_workers=64):
    controls, guards, fingerprints = validate_chain(control)
    target = local_target(db_path)
    candidate = target.with_name('.' + target.name + '-p4-scope-' + uuid.uuid4().hex)
    allowed = set(controls)
    protected = frozenset().union(*(guard.protected for guard in guards.values()))
    source_reader = guards[controls[0]]

    def notify(value):
        current = value.get('current_control')
        if current is not None and Path(os.path.abspath(current)) not in allowed:
            raise ValueError('P4 base reader entered an unvalidated control')
        value = copy.deepcopy(value)
        value['scope'] = 'project_priority4'
        if value.get('status') == 'publishing':
            value['status'] = 'validating_scope'
        if progress_callback is not None:
            progress_callback(value)

    try:
        summary = parallel_build(controls[0], candidate, progress_callback=notify,
                                 progress_interval=progress_interval, read_workers=read_workers)
        observed = [Path(os.path.abspath(path)) for path in summary['controls']]
        if observed != list(reversed(controls)):
            raise ValueError('P4 resume chain changed during receipt reads')
        import sqlite3
        # The candidate is private and read-only here. Do not expose a partial
        # or unvalidated index to a live coordinator or mutate the base proof.
        with closing(sqlite3.connect(candidate.as_uri() + '?mode=ro', uri=True)) as db:
            for (rel,) in db.execute('SELECT rel FROM files'):
                source_reader.assert_path(rel)
                if rel.split('/', 1)[0] in protected:
                    raise ValueError('P4 receipt addresses a reserved ancestor/control tree')
        again, _, current_fingerprints = validate_chain(control)
        if again != controls or current_fingerprints != fingerprints:
            raise ValueError('P4 scope or resume configuration changed during recovery')
        with closing(sqlite3.connect(candidate)) as db:
            seal_receipt_candidate(db, source_reader, issues=summary['issues'],
                                   metadata_warnings=summary['metadata_warnings'])
        if progress_callback is not None:
            progress_callback({'status': 'publishing', 'scope': 'project_priority4',
                               'controls': summary['controls'],
                               'verified_files': summary['verified_files'],
                               'verified_symlinks': summary['verified_symlinks']})
        # Reporting may block while a scope/configuration publisher advances.
        # Revalidate after the callback and before exposing the new candidate.
        again, _, current_fingerprints = validate_chain(control)
        if again != controls or current_fingerprints != fingerprints:
            raise ValueError('P4 scope or resume configuration changed before publication')
        # Local hard-link publication is exclusive; an existing live index is
        # never replaced or removed even if it appeared during this build.
        os.link(candidate, target)
        summary.update(db_path=str(target), scope='project_priority4',
                       scope_validated_controls=len(controls))
        return summary
    finally:
        try:
            candidate.unlink()
        except FileNotFoundError:
            pass
