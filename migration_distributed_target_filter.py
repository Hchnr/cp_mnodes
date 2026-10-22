#!/usr/bin/env python3
"""Adopt the existing producer and spread read-only target probes over nodes.

The inherited assignment ledger, manifest/ID adoption, seed observation and
copy/SHA pipeline are unchanged. Only existence/source-metadata probes use
authenticated HTTP. Transport/protocol failures always fall back to the
original local probe; they can never stand for an existing or missing file.
"""

import argparse
import collections
import copy
import http.client
import json
import os
from pathlib import Path
import stat
import threading
import time
import urllib.parse
import urllib.request

from migration_io import read_json
from migration_overlap_parallel_filter import ParallelFilterResume


MAX_RESPONSE_BYTES = 1024 * 1024
PROBE_DEADLINE_SECONDS = 10.0
MAX_REMOTE_ATTEMPTS = 3  # Initial endpoint plus two different-node retries.


class ProbeProtocolError(ValueError):
    pass


def normalize_endpoints(value):
    if isinstance(value, dict):
        values = [value[key] for key in sorted(value)]
    elif isinstance(value, list):
        values = value
    else:
        raise ValueError('target_probe_endpoints must be a list or host-to-URL object')
    endpoints = []
    for item in values:
        if isinstance(item, dict):
            item = item.get('url')
        if not isinstance(item, str):
            raise ValueError('target probe URL must be a string')
        parsed = urllib.parse.urlsplit(item)
        if (parsed.scheme not in ('http', 'https') or not parsed.hostname
                or parsed.username is not None or parsed.password is not None
                or parsed.query or parsed.fragment or parsed.path not in ('', '/')):
            raise ValueError('target probe endpoint must be an HTTP(S) base URL')
        parsed.port  # Validate malformed/out-of-range ports before handover.
        endpoint = item.rstrip('/')
        if endpoint in endpoints:
            raise ValueError('target probe endpoints must be unique')
        endpoints.append(endpoint)
    if not endpoints:
        raise ValueError('at least one target probe endpoint is required')
    return tuple(endpoints)


class DistributedTargetFilterResume(ParallelFilterResume):
    def __init__(self, control, handover):
        # Validate client-only configuration before consuming the one-shot
        # inherited handover. Do not construct/delete/reseed any SQLite index.
        cfg = read_json(Path(control) / 'config.json')
        self._init_probe_client(cfg)
        super().__init__(control, handover)
        self.filter_workers = self._distributed_workers
        self.filter_limit = self._distributed_limit
        self.save(parallel_target_threads=self.filter_workers,
                  distributed_target_threads=self.filter_workers,
                  distributed_target_nodes=len(self._probe_endpoints),
                  target_filter_inflight=self.filter_limit,
                  recovery_label=(str(self.filter_workers) + '路跨节点只读检查目标，复制与索引恢复同时进行'))

    def _init_probe_client(self, cfg):
        self._probe_endpoints = normalize_endpoints(cfg.get('target_probe_endpoints'))
        token = cfg.get('broker_token')
        if not isinstance(token, str) or not token or '\r' in token or '\n' in token:
            raise ValueError('valid broker authentication required for target probes')
        self._probe_token = token
        workers = cfg.get('distributed_target_threads', 256)
        limit = cfg.get('target_filter_inflight', 4096)
        if type(workers) is not int or not 1 <= workers <= 4096:
            raise ValueError('distributed_target_threads must be between 1 and 4096')
        if type(limit) is not int or not 1 <= limit <= 4096:
            raise ValueError('target_filter_inflight must be between 1 and 4096')
        self._distributed_workers = workers
        self._distributed_limit = max(workers, limit)
        self._probe_tls = threading.local()
        self._probe_lock = threading.Lock()
        self._probe_cursor = 0
        self._probe_counts = collections.Counter()
        self._endpoint_counts = {endpoint: collections.Counter() for endpoint in self._probe_endpoints}

    def _probe_snapshot(self):
        with self._probe_lock:
            return {'endpoint_count': len(self._probe_endpoints),
                    'deadline_seconds': PROBE_DEADLINE_SECONDS,
                    'attempts_per_probe': min(MAX_REMOTE_ATTEMPTS, len(self._probe_endpoints)),
                    'threads': self._distributed_workers, 'inflight_limit': self._distributed_limit,
                    'counts': dict(self._probe_counts),
                    'endpoints': {endpoint: dict(counts) for endpoint, counts in self._endpoint_counts.items()}}

    def save(self, **updates):
        # Only the inherited producer/main thread publishes state. Probe
        # threads update these small process-local counters under a lock.
        if hasattr(self, '_probe_lock'):
            updates['distributed_target_probe'] = self._probe_snapshot()
            updates['distributed_target_nodes'] = len(self._probe_endpoints)
        return super().save(**updates)

    def _count(self, name, endpoint=None):
        with self._probe_lock:
            self._probe_counts[name] += 1
            if endpoint is not None:
                self._endpoint_counts[endpoint][name] += 1

    def _attempt_endpoints(self):
        with self._probe_lock:
            start = self._probe_cursor
            self._probe_cursor = (start + 1) % len(self._probe_endpoints)
        count = min(MAX_REMOTE_ATTEMPTS, len(self._probe_endpoints))
        return tuple(self._probe_endpoints[(start + index) % len(self._probe_endpoints)]
                     for index in range(count))

    def _opener(self):
        opener = getattr(self._probe_tls, 'opener', None)
        if opener is None:
            # Cluster-local endpoints must never inherit an external proxy.
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            self._probe_tls.opener = opener
        return opener

    @staticmethod
    def _remaining(deadline):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError('target probe deadline exceeded')
        return remaining

    @staticmethod
    def _response_payload(response, deadline):
        # read1 limits each socket operation; checking the remaining deadline
        # between chunks prevents a trickling response from renewing ten
        # seconds indefinitely. No source data is read by this client.
        chunks, length = [], 0
        while True:
            remaining = DistributedTargetFilterResume._remaining(deadline)
            raw = getattr(getattr(response, 'fp', None), 'raw', None)
            sock = getattr(raw, '_sock', None)
            if sock is not None:
                sock.settimeout(remaining)
            read = getattr(response, 'read1', response.read)
            piece = read(min(65536, MAX_RESPONSE_BYTES + 1 - length))
            if not piece:
                break
            chunks.append(piece)
            length += len(piece)
            if length > MAX_RESPONSE_BYTES:
                raise ProbeProtocolError('target probe response too large')
        return json.loads(b''.join(chunks).decode('utf-8'))

    def _http_probe(self, endpoint, entry, deadline):
        payload = json.dumps({'entry': entry}, ensure_ascii=True, allow_nan=False).encode('utf-8')
        request = urllib.request.Request(endpoint + '/probe', data=payload, method='POST',
                  headers={'Authorization': 'Bearer ' + self._probe_token,
                           'Content-Type': 'application/json'})
        with self._opener().open(request, timeout=self._remaining(deadline)) as response:
            if response.status != 200:
                raise ProbeProtocolError('target probe returned non-success status')
            return self._response_payload(response, deadline)

    @staticmethod
    def _checked_entry(payload, original):
        if not isinstance(payload, dict) or payload.get('ok') is not True or type(payload.get('missing')) is not bool:
            raise ProbeProtocolError('invalid target probe reply')
        if payload['missing'] is False:
            if payload.get('reason') != 'existing' or payload.get('entry') is not None:
                raise ProbeProtocolError('invalid existing-target reply')
            return None
        fresh = payload.get('entry')
        if (payload.get('reason') != 'missing' or not isinstance(fresh, dict)
                or fresh.get('rel') != original.get('rel')
                or fresh.get('kind') not in ('file', 'symlink')
                or fresh.get('known') != original.get('known')
                or '_refresh_source' in fresh):
            raise ProbeProtocolError('invalid missing-target entry')
        if not original.get('_refresh_source'):
            expected = {k: v for k, v in original.items() if k != '_refresh_source'}
            if fresh != expected:
                raise ProbeProtocolError('unchanged target probe altered source metadata')
        else:
            for field in ('size', 'mode', 'ino', 'dev', 'mtime_ns', 'ctime_ns', 'uid', 'gid'):
                if type(fresh.get(field)) is not int:
                    raise ProbeProtocolError('refreshed entry lacks source identity')
            if ((fresh['kind'] == 'file' and not stat.S_ISREG(fresh['mode']))
                    or (fresh['kind'] == 'symlink' and not stat.S_ISLNK(fresh['mode']))):
                raise ProbeProtocolError('refreshed entry kind/type mismatch')
        return copy.deepcopy(fresh)

    def _local_probe_missing(self, entry):
        return ParallelFilterResume._probe_missing(self, entry)

    def _probe_missing(self, entry):
        self._check_stop()
        self._count('probes')
        deadline = time.monotonic() + PROBE_DEADLINE_SECONDS
        for endpoint in self._attempt_endpoints():
            self._check_stop()
            try:
                self._remaining(deadline)
            except TimeoutError:
                self._count('deadline_exhausted')
                break
            self._count('requests', endpoint)
            try:
                payload = self._http_probe(endpoint, entry, deadline)
                self._remaining(deadline)
                result = self._checked_entry(payload, entry)
            except (OSError, ValueError, TypeError, KeyError, http.client.HTTPException):
                self._count('remote_errors', endpoint)
                continue
            self._check_stop()
            self._count('remote_success', endpoint)
            self._count('remote_existing' if result is None else 'remote_missing', endpoint)
            return result
        self._check_stop()
        self._count('local_fallbacks')
        return self._local_probe_missing(entry)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('run',))
    parser.add_argument('--control', required=True)
    parser.add_argument('--handover', required=True)
    args = parser.parse_args()
    os.umask(0o077)
    DistributedTargetFilterResume(args.control, args.handover).run()


if __name__ == '__main__':
    main()
