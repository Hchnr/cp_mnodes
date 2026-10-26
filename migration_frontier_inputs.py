"""Bounded read/probe/publish/ack pipeline; task ownership stays on the caller.

Background threads only read immutable hints and acknowledge proven published
groups. They never allocate IDs, query SQLite, submit probes or publish tasks.
"""
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict, deque
import os
from pathlib import Path
import stat
import threading
import time

from migration_io import read_json
from migration_input_receipts import InputReceipts


class InputPipeline:
    def __init__(self, owner, validator, *, group_receipts=True):
        self.owner, self.validator = owner, validator
        self.thread = threading.get_ident()
        self.receipts = InputReceipts(owner) if group_receipts else None
        self.completed = self.receipts.known if self.receipts else {}
        self.streams, self.buffered = {}, {}
        self.pass_versions, self.ended_versions = {}, {}
        self.next_scan, self.last_open = {}, {}
        self.io = ThreadPoolExecutor(max_workers=16, thread_name_prefix='candidate-io')
        self.scanner = ThreadPoolExecutor(max_workers=8, thread_name_prefix='candidate-directory')
        self.loader = ThreadPoolExecutor(max_workers=1, thread_name_prefix='candidate-prefetch')
        self.acker = ThreadPoolExecutor(max_workers=2, thread_name_prefix='candidate-ack')
        # Renames take the shared ready/done directory locks. Never queue them
        # on the read executor: a slow host could otherwise occupy all sixteen
        # readers. One lane per host also prevents same-directory lock herds.
        self.ack_io = {host: ThreadPoolExecutor(max_workers=1,
            thread_name_prefix='candidate-rename-' + str(i))
            for i, host in enumerate(owner.cfg.get('copy_hosts', []))} if not group_receipts else {}
        self.ack_stats = {host: {'files': 0, 'seconds': 0, 'groups': 0}
                          for host in self.ack_io}
        self.prefetch_io = {}
        self.prefetch = None
        self.groups = {}
        self.probe_groups = {}
        self.offering = None
        self.next_group = self.cursor = 0
        self.poll_at = self.tuning_at = 0
        self.limits = {'max_files': 256, 'files_per_host': 32}
        previous = owner.state.get('priority_inputs', {})
        self.files = self.receipts.files if self.receipts else previous.get('files_consumed', 0)
        self.entries = self.receipts.total_entries if self.receipts else previous.get('entries_consumed', 0)
        self.ack_records = self.ack_seconds = 0
        self.closed = False
        self.prefetch_started = 0
        self.last_prefetch_seconds = 0

    def _main(self):
        if threading.get_ident() != self.thread:
            raise RuntimeError('input ownership belongs to producer main thread')

    def _read(self, item):
        host, path = item
        payload = read_json(path)
        if (payload.get('control') != str(self.owner.control)
                or payload.get('run_id') != self.owner.cfg['run_id']
                or payload.get('host') != host
                or not isinstance(payload.get('entries'), list)
                or len(payload['entries']) > 512):
            raise ValueError('invalid distributed discovery payload')
        entries = [self.validator(e) for e in payload['entries']]
        done = path.parent.parent / 'done' / path.name
        if done.exists():
            raise RuntimeError('discovery acknowledgement collision')
        return path, done, [e for e in entries if e is not None], len(entries)

    def _scan_ready(self, host, limit, excluded):
        paths = []
        chosen = set()
        ready = self.owner.control / 'frontier-input' / host / 'ready'
        buffered = self.buffered.setdefault(host, deque())
        while buffered and len(paths) < limit:
            item = buffered.popleft()
            if item[1] not in excluded and item[1] not in chosen:
                paths.append(item)
                chosen.add(item[1])
        if len(paths) == limit:
            return paths
        stream = self.streams.get(host)
        if stream is None:
            now = time.monotonic()
            if now < self.next_scan.get(host, 0):
                return paths
            try:
                info = ready.stat(follow_symlinks=False)
                if not stat.S_ISDIR(info.st_mode):
                    raise ValueError('discovery ready path is not a directory')
                version = (info.st_dev, info.st_ino, info.st_mtime_ns, info.st_ctime_ns)
                # Original hints remain in ready/. Do not rescan all retired
                # names for every 64-record historical fallback batch. A changed
                # directory opens immediately; force a pass every 30 s as well,
                # so coarse timestamps cannot permanently hide an addition.
                if (self.ended_versions.get(host) == version
                        and now - self.last_open.get(host, 0) < 30):
                    self.next_scan[host] = now + 1
                    return paths
                stream = os.scandir(ready)
            except FileNotFoundError:
                self.next_scan[host] = now + 1
                return paths
            self.streams[host] = stream
            self.pass_versions[host] = version
            self.last_open[host] = now
        while len(paths) < limit:
            try:
                item = next(stream)
            except StopIteration:
                stream.close()
                self.streams[host] = None
                # Compare with the version captured BEFORE enumeration, not
                # after it: additions behind the cursor must trigger a new pass.
                if host in self.pass_versions:
                    self.ended_versions[host] = self.pass_versions[host]
                break
            path = Path(item.path)
            if item.name.endswith('.json') and path not in excluded and path not in chosen:
                if not item.is_file(follow_symlinks=False):
                    raise ValueError('discovery input is not a regular file')
                paths.append((host, path))
                chosen.add(path)
        return paths

    def _load(self, hosts, limits, excluded):
        # Listing a busy shared directory itself took ~1.93 s for 64 names.
        # Keep independent host queues parallel, not eleven serial READDIRs.
        started = time.monotonic()
        self.prefetch_io = {'phase': 'enumerating', 'started_monotonic': started}
        limit = min(limits['max_files'], limits['files_per_host'])
        groups = list(self.scanner.map(lambda host:self._scan_ready(host, limit, excluded), hosts))
        paths = []
        for host, group in zip(hosts, groups):
            take = min(len(group), max(0, limits['max_files']-len(paths)))
            paths.extend(group[:take])
            # A persistent iterator has advanced even for hosts beyond the
            # global cap. Retain excess references rather than dropping work.
            self.buffered.setdefault(host, deque()).extendleft(reversed(group[take:]))
        enumerated = time.monotonic()
        self.prefetch_io = {'phase': 'reading', 'started_monotonic': started,
                           'enumeration_seconds': enumerated - started,
                           'files': len(paths)}
        rows = list(self.io.map(self._read, paths))
        finished = time.monotonic()
        self.prefetch_io = {'phase': 'ready', 'started_monotonic': started,
            'enumeration_seconds': enumerated - started,
            'read_seconds': finished - enumerated, 'seconds': finished - started,
            'files': len(rows), 'entries': sum(row[3] for row in rows)}
        return rows

    def _prefetch(self):
        if self.prefetch is not None or self.closed:
            return
        hosts = list(self.owner.cfg.get('copy_hosts', []))
        if hosts:
            hosts = hosts[self.cursor:] + hosts[:self.cursor]
            self.cursor = (self.cursor + 1) % len(hosts)
        excluded = frozenset(self.completed).union(
            row[0] for group in self.groups.values() for row in group['items'])
        self.prefetch_started = time.monotonic()
        self.prefetch = self.loader.submit(self._load, hosts, dict(self.limits), excluded)

    def _tune(self):
        if time.monotonic() < self.tuning_at:
            return
        self.tuning_at = time.monotonic() + 5
        try:
            value = read_json(self.owner.control / 'frontier-input-tuning.json')
            if (value.get('control') != str(self.owner.control)
                    or value.get('run_id') != self.owner.cfg['run_id']):
                raise ValueError('input tuning identity differs')
            limits = {k: value.get(k, v) for k, v in self.limits.items()}
            if any(type(v) is not int or not 1 <= v <= 4096 for v in limits.values()):
                raise ValueError('input tuning bounds exceeded')
            copy_batch = value.get('copy_batch_files')
            if copy_batch is not None and (type(copy_batch) is not int
                    or copy_batch not in (64, 128, 256, 512)):
                raise ValueError('copy_batch_files must be 64, 128, 256 or 512')
            if copy_batch is not None:
                self.owner.set_copy_batch(copy_batch)
            self.limits = limits
            self.owner.state.pop('frontier_input_tuning_error', None)
        except FileNotFoundError:
            pass
        except (OSError, ValueError) as exc:
            self.owner.state['frontier_input_tuning_error'] = str(exc)

    @staticmethod
    def _ack_one(row):
        source, target = row[:2]
        if target.exists():
            raise RuntimeError('discovery acknowledgement collision')
        os.rename(source, target)

    def _ack_host(self, host, rows):
        started = time.monotonic()
        stats = self.ack_stats[host]
        try:
            for row in rows:
                self._ack_one(row)
                stats['files'] += 1
            stats['groups'] += 1
        finally:
            stats['seconds'] += time.monotonic() - started

    def _ack(self, rows, barrier=None):
        if self.receipts:
            started = time.monotonic()
            path = self.receipts.commit(rows, barrier)
            return {'path':str(path), 'seconds':time.monotonic()-started}
        by_host = defaultdict(list)
        for row in rows:
            host = row[0].parent.parent.name
            if host not in self.ack_io:
                raise ValueError('acknowledgement host is not allowed')
            by_host[host].append(row)
        futures = [self.ack_io[host].submit(self._ack_host, host, values)
                   for host, values in by_host.items()]
        for future in futures:
            future.result()

    def submitted(self, futures):
        self._main()
        if self.offering is not None:
            group = self.groups[self.offering]
            for future in futures:
                self.probe_groups[future] = self.offering
                group['probes'].add(future)

    def harvested(self, futures):
        self._main()
        for future in futures:
            group_id = self.probe_groups.pop(future, None)
            if group_id is not None:
                self.groups[group_id]['probes'].remove(future)
        self.reap()

    def reap(self):
        self._main()
        self.owner._collect_publications()
        for group_id, group in list(self.groups.items()):
            if group['ack'] is not None:
                if group['ack'].done():
                    result = group['ack'].result()
                    if self.receipts:
                        self.receipts.accept((row[0], row[3]) for row in group['items'])
                        self.files, self.entries = self.receipts.files, self.receipts.total_entries
                        self.ack_records += 1
                        self.ack_seconds += result['seconds']
                    else:
                        self.completed.update((row[0], row[3]) for row in group['items'])
                        self.files += len(group['items'])
                        self.entries += sum(row[3] for row in group['items'])
                    del self.groups[group_id]
                continue
            if group['offering'] or group['probes']:
                continue
            if group['barrier'] is None:
                # A task may contain entries from multiple input groups. A
                # conservative publication-ID barrier covers all of them.
                self.owner._flush_selected()
                group['barrier'] = self.owner.next_task_id - 1
            if not any(task_id <= group['barrier'] for task_id in self.owner._publication_tasks):
                group['ack'] = self.acker.submit(self._ack, group['items'], group['barrier'])
        self.owner.state['priority_inputs'] = {
            'closed': self.closed,
            'files_consumed': self.files, 'entries_consumed': self.entries,
            'root': str(self.owner.control / 'frontier-input'),
            'open_groups': len(self.groups),
            'probing_groups': sum(g['ack'] is None for g in self.groups.values()),
            'ack_groups': sum(g['ack'] is not None for g in self.groups.values()),
            'prefetch_ready': self.prefetch is not None and self.prefetch.done(),
            'prefetch_age_seconds':time.monotonic()-self.prefetch_started if self.prefetch else 0,
            'last_prefetch_seconds':self.last_prefetch_seconds,
            # Actual background I/O duration, unlike prefetch age which also
            # includes time waiting for the main thread to accept a ready group.
            'prefetch_io': dict(self.prefetch_io),
            'ack_lanes': {host: dict(stats) for host, stats in self.ack_stats.items()},
            'readers': 16, 'rename_lanes': len(self.ack_io),
            'acknowledgement': 'group-receipts-v1' if self.receipts else 'rename-v2',
            'acknowledgement_records': self.ack_records,
            'acknowledgement_io_seconds': self.ack_seconds,
            'restored_acknowledgements': self.receipts.loaded_groups if self.receipts else 0,
            'replayed_acknowledgements': self.receipts.replay_groups if self.receipts else 0,
            'directory_readers':8,
            'pipeline': 'group-receipts-cursor-publication-barriers-v3' if self.receipts
                        else 'isolated-read-ack-publication-barriers-v2'}

    def pump(self, force=False):
        self._main()
        self.reap()
        if self.closed or (not force and time.monotonic() < self.poll_at):
            return
        self.poll_at = time.monotonic() + .05
        self._tune()
        self._prefetch()
        for _ in range(2):
            if (len(self.groups) >= 4
                    or sum(g['ack'] is None for g in self.groups.values()) >= 2
                    or not self.prefetch.done()):
                break
            rows = self.prefetch.result()
            self.last_prefetch_seconds = time.monotonic()-self.prefetch_started
            self.prefetch = None
            if not rows:
                self.poll_at = time.monotonic() + .25
                break
            group_id = self.next_group
            self.next_group += 1
            group = {'items': rows, 'probes': set(), 'offering': True,
                     'barrier': None, 'ack': None}
            self.groups[group_id] = group
            # Reserve this batch before prefetching, so no ready file can be
            # selected by two groups while acknowledgement is still pending.
            self._prefetch()
            self.offering = group_id
            try:
                batch = []
                for _, _, entries, _ in rows:
                    # _offer flushes its tail on every call. Discovery files
                    # often contain only a handful of entries; flushing per
                    # file made live HTTP batches average just 23--30 entries.
                    # Coalesce hints within this single ownership/barrier group.
                    for entry in entries:
                        batch.append(entry)
                        if len(batch) == 64:
                            self.owner._offer(batch)
                            batch = []
                if batch:
                    self.owner._offer(batch)
            finally:
                self.offering = None
            group['offering'] = False
            self.reap()

    def busy(self):
        self._main()
        return bool(self.groups) or self.prefetch is not None

    def finish(self):
        """Finish accepted groups only; a read-only prefetched batch stays ready."""
        self._main()
        while self.groups:
            self.owner._check_stop()
            self.owner._harvest_probes(True)
            self.reap()
            if self.groups:
                time.sleep(.01)

    def close(self):
        self._main()
        self.closed = True
        self.loader.shutdown(wait=True)
        self.acker.shutdown(wait=True)
        for pool in self.ack_io.values():
            pool.shutdown(wait=True)
        self.scanner.shutdown(wait=True)
        for stream in self.streams.values():
            if stream is not None:
                stream.close()
        self.io.shutdown(wait=True)
