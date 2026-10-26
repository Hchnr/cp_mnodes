"""Read-only SAME-PASS P4 frontier preparation, never handover authority.

The frozen scanner has one FIFO event consumer and at most 8192 queued events.
After a readonly FD observation ends, force evidence of a later manifest flush
using the frozen producer's task-backpressure bound, then consume more than a
queue capacity of NEW manifest records. Previously closed readers' queued
records are now represented. Retain observed active frames, initially unentered
directories, errors, all subsequently discovered directories and ancestors.

This is NOT a claim that the currently live scanner's queues are empty. A
successor still requires independently authorized graceful exit/publication
drain, final tail merge, archive/lease verification and coordinator exclusion.
"""
import errno
import os
from pathlib import Path
import stat
import time

from migration_scan_boundary import identity, require_identity
from migration_warm_scan_checkpoint import Prefix
from migration_worker import _components


def ancestors(rel):
    while True:
        yield rel
        if not rel:
            break
        rel = rel.rpartition('/')[0]


def relative_source_directory(link, source_root):
    source_root = os.path.abspath(os.fspath(source_root))
    if link != source_root and not link.startswith(source_root + '/'):
        return None
    if link.endswith(' (deleted)'):
        raise ValueError('renamed/deleted source frame cannot authorize pruning')
    rel = link[len(source_root):].lstrip('/')
    if rel:
        _components(rel)
        if rel == 'eai_pwm' or rel.startswith('eai_pwm/'):
            raise ValueError('P4 frame entered the covered eai_pwm namespace')
    return rel


def observe_source_frames(expected, source_root):
    """No stop/ptrace. A descriptor that disappears closed before capture_end.

    A frame that remains open throughout the observation has a stable proc FD
    entry and is retained. Frames completed meanwhile are covered by the later
    FIFO drain. Frames opened later were either initially unentered or newly
    discovered, both retained by the prefix protocol.
    """
    require_identity(expected)
    started = time.time()
    paths, closed, changed = set(), 0, 0
    directory = Path('/proc') / str(expected['pid']) / 'fd'
    for entry in list(directory.iterdir()):
        try:
            before = os.readlink(entry)
            rel = relative_source_directory(before, source_root)
            info = None
            if rel is not None:
                info = entry.stat()
                paths.add(rel)
            after = os.readlink(entry)
            if before == after and info is not None and not stat.S_ISDIR(info.st_mode):
                raise ValueError('P4 scanner holds an unexpected source payload FD')
            if after != before:
                changed += 1
                other = relative_source_directory(after, source_root)
                if other is not None:
                    paths.add(other)
        except OSError as exc:
            if exc.errno not in (errno.ENOENT, errno.ESRCH):
                raise
            closed += 1
    require_identity(expected)
    if '' not in paths:
        raise ValueError('P4 scanner source-root anchor was not observed')
    return dict(started=started, finished=time.time(), directories=sorted(paths),
                closed_during_observation=closed, reused_during_observation=changed,
                source_root=os.path.abspath(os.fspath(source_root)), process=dict(expected),
                process_signals_sent=0)


class Priority4Prefix(Prefix):
    def __init__(self):
        super().__init__()
        self.scanner.stats['phase'] = 4

    def add(self, entry):
        rel = entry.get('rel') if isinstance(entry, dict) else None
        if rel == 'eai_pwm' or isinstance(rel, str) and rel.startswith('eai_pwm/'):
            raise ValueError('P4 prefix includes the covered eai_pwm subtree')
        super().add(entry)


class FifoFrontier:
    def __init__(self, prefix, frames, *, completed_tasks, capture_offset,
                 queue_capacity=8192, backpressure=4096, poll_every=256):
        if (not isinstance(prefix, Priority4Prefix) or type(completed_tasks) is not int
                or completed_tasks < 0 or type(capture_offset) is not int or capture_offset < 0
                or any(type(v) is not int or v <= 0 for v in (queue_capacity, backpressure, poll_every))):
            raise ValueError('bound P4 prefix and positive frozen queue/publication limits required')
        if frames.get('source_root') != '/share/project' or frames.get('process_signals_sent') != 0:
            raise ValueError('readonly P4 frame observation required')
        retained = {''}
        uncertain = ((prefix.directories.keys() - prefix.parents) | prefix.error_parents |
                     set(frames['directories']))
        for rel in uncertain:
            retained.update(ancestors(rel))
        self.candidates = {rel: value for rel, value in prefix.directories.items() if rel not in retained}
        self.capture_offset = capture_offset
        self.capture_records = prefix.records
        self.frames = frames
        self.queue_capacity = queue_capacity
        # Between checks the producer can accept < poll_every extra tasks.
        # Extra poll_every margin is conservative. This bound includes accepted
        # but unfinished publisher futures, not merely visible pending files.
        self.pre_capture_next_id_upper_bound = completed_tasks + backpressure + 2 * poll_every
        self.flush_observed_size = None
        self.flush_observed_task_id = None
        self.flush_marker_records = None
        self.latest_records = prefix.records

    def entry(self, value):
        """Call for every complete prefix record after capture_offset."""
        if value.get('kind') in ('directory', 'error'):
            rel = value['rel']
            if rel:
                _components(rel)
            for parent in ancestors(rel):
                self.candidates.pop(parent, None)
        self.latest_records += 1

    def observe_flush(self, saved_next_task_id, observed_manifest_size):
        """Sizes are observed AFTER the saved next ID, and AFTER capture_end."""
        if type(saved_next_task_id) is not int or type(observed_manifest_size) is not int:
            raise ValueError('integer task and manifest observations required')
        if self.flush_observed_size is None and saved_next_task_id > self.pre_capture_next_id_upper_bound:
            if observed_manifest_size < self.capture_offset:
                raise ValueError('manifest prefix moved backward')
            self.flush_observed_size = observed_manifest_size
            self.flush_observed_task_id = saved_next_task_id

    def progress(self, parsed_offset, records):
        if (type(parsed_offset) is not int or parsed_offset < self.capture_offset
                or records != self.latest_records):
            raise ValueError('all new complete records must be accounted for in order')
        if (self.flush_observed_size is not None and self.flush_marker_records is None
                and parsed_offset >= self.flush_observed_size):
            self.flush_marker_records = records
        new_records = 0 if self.flush_marker_records is None else records - self.flush_marker_records
        return dict(status=('prepared_fifo_frontier_only' if new_records > self.queue_capacity else
                            'awaiting_post_flush_fifo_drain' if self.flush_marker_records is not None else
                            'awaiting_later_flush'), queue_capacity=self.queue_capacity,
                    new_records_after_flush=new_records,
                    pre_capture_next_id_upper_bound=self.pre_capture_next_id_upper_bound,
                    flush_observed_task_id=self.flush_observed_task_id,
                    flush_observed_size=self.flush_observed_size,
                    capture_records=self.capture_records, parsed_records=records,
                    candidate_directories=len(self.candidates),
                    adoption_authorized=False, graceful_exit_and_final_tail_still_required=True)
