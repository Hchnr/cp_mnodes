#!/usr/bin/env python3
"""Bounded private old/new request CPU comparison, without network traffic.

Both paths construct real urllib handlers and serialize/parse the same JSON.
Only OpenerDirector.open is replaced with a private empty-claim response. The
result isolates handler/TLS construction, not broker or end-to-end throughput.
It never imports frozen production bundles, signals actors or touches queues.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import io
import json
from pathlib import Path
import sys
import threading
import time
from unittest import mock
import urllib.request

from migration_io import write_json
from migration_run import BrokerClient


class LegacyClient:
    def request(self, method, route, payload=None):
        body = None if payload is None else json.dumps(payload, ensure_ascii=True).encode('utf-8')
        request = urllib.request.Request('http://private.invalid' + route, data=body,
            method=method, headers={'Authorization': 'Bearer fixture', 'Content-Type': 'application/json'})
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open(request, timeout=30) as response:
            result = json.load(response)
        if not isinstance(result, dict) or result.get('ok') is False:
            raise ValueError('broker returned an invalid or rejected response')
        return result


def sample(mode, threads, requests_per_thread, idle_wait=0):
    client = (LegacyClient() if mode == 'legacy' else
              BrokerClient('http://private.invalid', 'fixture'))
    barrier = threading.Barrier(threads)
    original = urllib.request.build_opener
    constructions = 0
    lock = threading.Lock()

    def build(*args, **kwargs):
        nonlocal constructions
        with lock:
            constructions += 1
        return original(*args, **kwargs)

    def open_private(self, request, timeout):
        return io.BytesIO(b'{"ok":true,"task":null}')

    def run(index):
        barrier.wait(timeout=30)
        durations = []
        for iteration in range(requests_per_thread):
            started = time.monotonic()
            result = client.request('GET', '/claim?stage=p3-r3&owner=private-' + str(index))
            if result != {'ok': True, 'task': None}:
                raise AssertionError('private reply changed')
            durations.append(time.monotonic() - started)
            if idle_wait and iteration + 1 < requests_per_thread:
                time.sleep(idle_wait)
        return durations

    started, cpu = time.monotonic(), time.process_time()
    with mock.patch.object(urllib.request, 'build_opener', side_effect=build), \
            mock.patch.object(urllib.request.OpenerDirector, 'open', open_private):
        with ThreadPoolExecutor(max_workers=threads) as pool:
            times = list(pool.map(run, range(threads)))
    wall = time.monotonic() - started
    cpu_seconds = time.process_time() - cpu
    # Cold first requests are included in total CPU/wall, separated for latency.
    steady = sorted(duration for row in times for duration in row[1:])
    return dict(mode=mode, threads=threads, requests=threads * requests_per_thread,
        opener_constructions=constructions, elapsed_seconds=wall, cpu_seconds=cpu_seconds,
        average_cpu_cores=cpu_seconds / wall, idle_wait_seconds=idle_wait,
        steady_request_p50_seconds=steady[len(steady) // 2] if steady else None,
        steady_request_max_seconds=max(steady) if steady else None)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--threads', type=int, choices=range(1, 9), default=4)
    parser.add_argument('--requests-per-thread', type=int, choices=range(2, 33), default=8)
    parser.add_argument('--idle-wait', type=float, choices=(0, 1.5), default=0)
    args = parser.parse_args(argv)
    samples = []
    for mode in ('legacy', 'cached', 'cached', 'legacy'):
        value = sample(mode, args.threads, args.requests_per_thread, args.idle_wait)
        samples.append(value)
        print(json.dumps(value), flush=True)
    result = dict(python=sys.version, samples=samples, network_requests=0,
        production_requests=0, runtime_changes=0, signals_sent=0,
        scope='private-handler-json-cpu-only; not an end-to-end throughput claim')
    write_json(args.output, result)
    return result


if __name__ == '__main__':
    main()
