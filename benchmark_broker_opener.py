#!/usr/bin/env python3
"""Bounded CPU-only reproduction; no job requests and no runtime changes."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import sys
import time
import urllib.request

from migration_io import write_json


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = dict(python=sys.version, runtime_changes=0, network_requests=0, samples=[])
    for threads in (1, 4, 8):
        started, cpu = time.monotonic(), time.process_time()
        with ThreadPoolExecutor(max_workers=threads) as pool:
            list(pool.map(lambda _: urllib.request.build_opener(urllib.request.ProxyHandler({})), range(32)))
        sample = dict(threads=threads, constructs=32, elapsed=time.monotonic() - started,
                      cpu_seconds=time.process_time() - cpu)
        result['samples'].append(sample)
        print(json.dumps(sample), flush=True)
    write_json(args.output, result)


if __name__ == '__main__':
    main()
