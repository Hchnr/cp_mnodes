#!/usr/bin/env python3
"""Read-only scanner entry using a frozen, previously tested metadata core.

The core is supplied in the run bundle as readonly_scan_core.py. A fresh
partition, not a historical manifest, owns the full namespace. Newly created
directories under shallow leaves-only partitions must not fall into a gap.
"""
import json
import os
from pathlib import Path
import sys

import readonly_scan_core as core

core.PERSISTENT_ROOT = Path(__file__).resolve().parents[1]
COVERED_ROOTS = set()


class Audit(core.Audit):
    def scan_directory(self, task):
        children = super().scan_directory(task)
        rel, leaves_only = task
        if not leaves_only or core.excluded(rel, self.exclusions):
            return children
        fd = None
        try:
            fd = core.directory(self.source, rel)
            with os.scandir(fd) as entries:
                for entry in entries:
                    child = rel + '/' + entry.name if rel else entry.name
                    if core.excluded(child, self.exclusions) or child in COVERED_ROOTS:
                        continue
                    if entry.is_dir(follow_symlinks=False):
                        children.append((child, False))
                        self.record(rel, {'new_directory_since_partition': 1})
        except OSError as exc:
            self.error(rel, 'partition_child_recheck', exc)
        finally:
            if fd is not None:
                os.close(fd)
        return children


core.Audit = Audit
# Compatibility with tested frontier-transfer/resume helpers.
for _name in dir(core):
    if not _name.startswith('__') and _name not in ('Audit',):
        globals()[_name] = getattr(core, _name)


def main():
    path = Path(sys.argv[sys.argv.index('--plan') + 1])
    plan = json.loads(path.read_text())
    roots = plan.get('coverage_roots')
    if roots is None:
        roots = json.loads((core.PERSISTENT_ROOT / 'plan.json').read_text())['coverage_roots']
    COVERED_ROOTS.update(roots)
    core.main()


if __name__ == '__main__':
    main()
