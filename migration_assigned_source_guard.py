"""Optional warm-only metadata shortcut, never inventory or content proof.

An immutable, completed assignment prefix can identify the SAME regular inode
that the warm task filter already excludes. Only historically single-link
files qualify; unknown metadata, symlinks and inode collisions fail closed.
Every formal inventory/content verification pass must keep its normal stats.
"""

import stat

from migration_target_probe import phase3_relative


class AssignedSourceGuard:
    def __init__(self, inode_paths=None, excluded=None, assigned_paths=None):
        self.inode_paths = {} if inode_paths is None else inode_paths
        self.excluded = set() if excluded is None else excluded
        self.assigned_paths = assigned_paths

    def add(self, entry):
        rel = phase3_relative(entry)
        ino = entry.get('ino')
        if type(ino) is not int or ino <= 0:
            return
        if (entry.get('kind') != 'file' or type(entry.get('nlink')) is not int
                or entry['nlink'] != 1 or type(entry.get('mode')) is not int
                or not stat.S_ISREG(entry['mode'])):
            self.inode_paths.pop(ino, None)
            self.excluded.add(ino)
            return
        if ino in self.excluded:
            return
        previous = self.inode_paths.get(ino)
        if previous is not None and previous != rel:
            # This also rejects aliases across mounts and historical inode
            # reuse. Numeric inode equality alone is not a path proof.
            self.inode_paths.pop(ino, None)
            self.excluded.add(ino)
            return
        self.inode_paths[ino] = rel

    def matches(self, rel, ino, regular):
        return (regular is True and type(ino) is int and ino > 0
                and self.inode_paths.get(ino) == rel
                and (self.assigned_paths is None or rel in self.assigned_paths))


def sample_manifest(path, guard, window=4 * 1024**2):
    """Bounded, complete-line tail observations; not a completeness claim."""
    import json
    from pathlib import Path
    path = Path(path)
    with path.open('rb') as stream:
        size = stream.seek(0, 2)
        offset = max(0, size - window)
        stream.seek(offset)
        if offset:
            stream.readline()
        raw = stream.read(size - stream.tell())
    counts = dict(records=0, files=0, assigned_single_inode_matches=0, multi_link_files=0)
    for line in raw.splitlines(keepends=True):
        if not line.endswith(b'\n'):
            continue  # A live append tail is not yet a published record.
        entry = json.loads(line)
        counts['records'] += 1
        if entry.get('kind') != 'file':
            continue
        counts['files'] += 1
        counts['multi_link_files'] += int(entry.get('nlink', 0) > 1)
        counts['assigned_single_inode_matches'] += int(guard.matches(
            entry['rel'], entry.get('ino'), stat.S_ISREG(entry.get('mode', 0))))
    return dict(counts, source_size=size, metadata_inventory_proof=False)
