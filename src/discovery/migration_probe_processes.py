"""Spawned metadata clients with bounded batch IPC and one parent task owner.

Children perform only authenticated read-only probes and safe local fallback.
They never open the assignment ledger, migration index or task queue. Secrets
are sent through multiprocessing IPC, never command lines or diagnostic output.
"""
import concurrent.futures
import http.client
import json
import multiprocessing
import os
from pathlib import Path
import threading
import time
import urllib.request

from migration_distributed_target_filter import DistributedTargetFilterResume


_CLIENT = None
_THREADS = None


class _Client(DistributedTargetFilterResume):
    def __init__(self, control, cfg):
        self.control = Path(control)
        self.cfg = cfg
        self._stop_lock = threading.Lock()
        self._stop_until = 0
        self._stop_seen = False
        self._init_probe_client(cfg)

    def _check_stop(self):
        if self._stop_seen:
            raise RuntimeError('STOP requested')
        if time.monotonic() >= self._stop_until:
            with self._stop_lock:
                if time.monotonic() >= self._stop_until:
                    self._stop_seen = (self.control/'STOP').exists()
                    self._stop_until = time.monotonic()+.1
        if self._stop_seen:
            raise RuntimeError('STOP requested')

    def probe_many(self, entries):
        """One checked HTTP batch, individual existing fallback on uncertainty."""
        self._check_stop()
        endpoint = self._attempt_endpoints()[0]
        began = time.monotonic()
        payload = json.dumps({'entries': entries}, ensure_ascii=True, allow_nan=False).encode()
        request = urllib.request.Request(endpoint+'/probe-batch', data=payload, method='POST',
                    headers={'Authorization':'Bearer '+self._probe_token,'Content-Type':'application/json'})
        response = None
        self._count('http_batches', endpoint)
        try:
            with self._opener().open(request, timeout=10) as stream:
                response = self._response_payload(stream, began+10)
            if (not isinstance(response, dict) or response.get('ok') is not True
                    or not isinstance(response.get('results'), list)
                    or len(response['results']) != len(entries)):
                raise ValueError('invalid metadata batch response')
        except (OSError, ValueError, TypeError, KeyError, http.client.HTTPException):
            self._count('http_batch_errors', endpoint)
            return list(_THREADS.map(self._probe_missing, entries))
        result = [None]*len(entries)
        retry = []
        for index, (item, original) in enumerate(zip(response['results'], entries)):
            try:
                if not isinstance(item, dict) or item.get('status') != 200:
                    raise ValueError('metadata item unsuccessful')
                result[index] = self._checked_entry(item['result'], original)
            except (OSError, ValueError, TypeError, KeyError):
                retry.append(index)
                continue
            self._count('probes')
            self._count('requests', endpoint)
            self._count('remote_success', endpoint)
            self._count('remote_existing' if result[index] is None else 'remote_missing', endpoint)
        for index, value in zip(retry, _THREADS.map(self._probe_missing, [entries[i] for i in retry])):
            result[index] = value
        self._check_stop()
        return result


def _initialize(control, cfg, threads):
    global _CLIENT, _THREADS
    _CLIENT = _Client(control, cfg)
    # Stagger first endpoint across processes instead of starting all clients
    # at endpoint zero; subsequent requests retain the existing node rotation.
    _CLIENT._probe_cursor = os.getpid() % len(_CLIENT._probe_endpoints)
    _THREADS = concurrent.futures.ThreadPoolExecutor(max_workers=threads,
                                                    thread_name_prefix='metadata-client')


def _difference(after, before):
    return {key: value-before.get(key, 0) for key, value in after.items()
            if value != before.get(key, 0)}


def _probe_batch(entries):
    before = _CLIENT._probe_snapshot()
    began = time.monotonic()
    cpu = time.process_time()
    results = _CLIENT.probe_many(entries) if _CLIENT.cfg.get('metadata_batch_http', True) else list(_THREADS.map(_CLIENT._probe_missing, entries))
    after = _CLIENT._probe_snapshot()
    delta = {'counts': _difference(after['counts'], before['counts']),
             'endpoints': {key: _difference(value, before['endpoints'][key])
                           for key, value in after['endpoints'].items()}}
    return {'entries': results, 'probe_delta': delta, 'pid': os.getpid(),
            'seconds': time.monotonic()-began, 'cpu_seconds': time.process_time()-cpu}


class ProcessProbePool:
    def __init__(self, control, cfg, processes=8, threads=64):
        if type(processes) is not int or not 1 <= processes <= 32:
            raise ValueError('metadata processes must be within 1..32')
        if type(threads) is not int or not 1 <= threads <= 128:
            raise ValueError('metadata threads must be within 1..128')
        self.processes, self.threads = processes, threads
        self.pool = concurrent.futures.ProcessPoolExecutor(
            max_workers=processes, mp_context=multiprocessing.get_context('spawn'),
            initializer=_initialize, initargs=(str(control), cfg, threads))

    def submit(self, entries):
        if not 0 < len(entries) <= 64:
            raise ValueError('metadata batch must contain 1..64 entries')
        return self.pool.submit(_probe_batch, entries)

    def shutdown(self, **kwargs):
        return self.pool.shutdown(**kwargs)
