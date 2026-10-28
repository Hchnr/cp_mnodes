#!/usr/bin/env python3
"""Read-only scanner with durable single-owner frontier and link inventory."""
import os
import sqlite3
import stat
import migration_reaudit_scan as scanner


class DurableConnection(sqlite3.Connection):
    def execute(self, sql, *args, **kwargs):
        # No shared-memory WAL on NFS; every scanner exclusively owns its DB.
        sql = sql.replace('PRAGMA journal_mode=WAL', 'PRAGMA journal_mode=DELETE')
        sql = sql.replace('PRAGMA synchronous=NORMAL', 'PRAGMA synchronous=FULL')
        return super().execute(sql, *args, **kwargs)


_connect = sqlite3.connect


def connect(*args, **kwargs):
    kwargs['factory'] = DurableConnection
    return _connect(*args, **kwargs)


class Audit(scanner.Audit):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.files['source_links'] = open(self.output / 'source_links.jsonl', 'x', encoding='utf-8')

    def compare(self, rel, src, srcfd, name, targetfd, target_state):
        counts, events = super().compare(rel, src, srcfd, name, targetfd, target_state)
        if stat.S_ISLNK(src.st_mode) and target_state == 'present':
            try:
                link = os.readlink(name, dir_fd=targetfd)
                if link == '/share/project' or link.startswith('/share/project/'):
                    counts['target_links_to_source'] += 1
                    events.append(('source_links', dict(rel=rel, destination_link=link,
                        reason='absolute_target_link_depends_on_source', rewritten=False)))
            except OSError:
                pass  # The core separately records absent/conflicting targets.
        return counts, events


scanner.core.sqlite3.connect = connect
scanner.core.Audit = Audit
if __name__ == '__main__':
    scanner.main()
