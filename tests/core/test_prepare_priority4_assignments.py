import json
import sqlite3
import unittest

from prepare_priority4_assignments import consume_prefix, consume_sqlite_prefix, task_digest


class Guard:
    def assert_path(self, rel):
        if not isinstance(rel, str) or not rel or rel.startswith(('eai_pwm/', '../', '/')):
            raise ValueError('unsafe scope')
    def assert_task(self, task, entry):
        if entry['kind'] not in ('file', 'symlink') or entry['rel'].startswith(('eai_pwm/', '../', '/')):
            raise ValueError('unsafe scope')


def task(number, rel=None):
    return dict(id=number, phase=4, round=1, op='copy', entries=[dict(kind='file', rel=rel or str(number))])


class AssignmentPreparationTests(unittest.TestCase):
    def test_hardlinked_symlinks_are_not_regular_file_canonicals(self):
        first, second = task(0), task(1)
        for value in (first, second):
            value['entries'][0].update(kind='symlink', nlink=2, dev=1, ino=2)
        rows = [(i, json.dumps(v)) for i, v in enumerate((first, second))]
        parsed = consume_prefix(rows, Guard(), limit=2)
        self.assertEqual(parsed['paths'], {'0', '1'})
        self.assertEqual(parsed['hardlinks'], {})
        with sqlite3.connect(':memory:') as db:
            db.execute('CREATE TABLE claims(stage TEXT,id INTEGER,payload TEXT,PRIMARY KEY(stage,id))')
            db.executemany('INSERT INTO claims VALUES (?,?,?)', [('p4-r1', *r) for r in rows])
            sql = consume_sqlite_prefix(db, Guard(), limit=2)
        self.assertEqual(sql['paths'], parsed['paths'])
        self.assertEqual(sql['hardlinks'], {})

    def test_sql_extraction_rejects_duplicate_paths_and_preserves_gap_boundary(self):
        with sqlite3.connect(':memory:') as db:
            db.execute('CREATE TABLE claims(stage TEXT,id INTEGER,payload TEXT,PRIMARY KEY(stage,id))')
            db.executemany('INSERT INTO claims VALUES (?,?,?)',
                [('p4-r1', i, json.dumps(task(i))) for i in (0, 1, 3)])
            value = consume_sqlite_prefix(db, Guard(), limit=4)
            self.assertEqual(value['paths'], {'0', '1'})
            self.assertEqual(len(value['hashes']), 2)
            db.execute('UPDATE claims SET payload=? WHERE id=1', (json.dumps(task(1, '0')),))
            with self.assertRaisesRegex(ValueError, 'duplicate'):
                consume_sqlite_prefix(db, Guard(), limit=4)
    def test_stops_before_gap_and_limit_without_inventing_ids(self):
        rows = [(i, json.dumps(task(i))) for i in (0, 1, 3, 4)]
        result = consume_prefix(rows, Guard(), limit=10)
        self.assertEqual(len(result['hashes']), 2)
        self.assertEqual(result['paths'], {'0', '1'})
        self.assertEqual(len(consume_prefix(rows, Guard(), limit=1)['hashes']), 1)

    def test_duplicate_scope_payload_and_alias_conflicts_fail_closed(self):
        for bad in (task(1, '0'), task(1, 'eai_pwm/private'), dict(task(1), id=9)):
            with self.assertRaises(ValueError):
                consume_prefix([(0, json.dumps(task(0))), (1, json.dumps(bad))], Guard(), limit=10)
        alias = task(0)
        alias['entries'][0]['hardlink_to'] = 'other'
        with self.assertRaises(ValueError):
            task_digest(alias, Guard(), 0)

    def test_digest_is_independent_of_json_key_order(self):
        value = task(0)
        self.assertEqual(task_digest(value, Guard(), 0),
                         task_digest(dict(reversed(list(value.items()))), Guard(), 0))


if __name__ == '__main__':
    unittest.main()
