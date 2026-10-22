#!/usr/bin/env python3
"""64-reader continuation using the existing explicit handover protocol.

The base coordinator already reads parallel_scan_threads from config. This
entrypoint only changes the default for a new immutable code bundle; queue,
lease, manifest recovery and background helper behavior remain in the base.
The handover must identify the frozen producer's current active_manifest.
"""
import argparse
import os

from migration_parallel_pipeline import ParallelPipeline as ExistingPipeline


class ParallelPipeline(ExistingPipeline):
    def __init__(self, control, handover):
        super().__init__(control, handover)
        self.scan_workers = max(1, int(self.cfg.get('parallel_scan_threads', 64)))
        self.state['threads_per_node'] = int(self.cfg.get('copy_threads_per_node', 128))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=('run',))
    parser.add_argument('--control', required=True)
    parser.add_argument('--handover', required=True)
    args = parser.parse_args()
    os.umask(0o077)
    ParallelPipeline(args.control, args.handover).run()


if __name__ == '__main__':
    main()
