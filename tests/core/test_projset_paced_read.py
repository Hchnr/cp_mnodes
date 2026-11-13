import importlib.util
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch


MODULE_PATH = Path(__file__).resolve().parents[2] / 'ops/diagnostics/projset_paced_read.py'
spec = importlib.util.spec_from_file_location('projset_paced_read', MODULE_PATH)
pacing = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pacing)


class ReadPacerTests(unittest.TestCase):
    def setUp(self):
        self.now = 0.0
        self.values = [0, 0]
        self.sleeps = []
        self.pacer = pacing.ReadPacer(100, counter=lambda: tuple(self.values),
                                      clock=lambda: self.now, sleep=self.sleep,
                                      max_sleep=2, max_batch=500)

    def sleep(self, duration):
        self.sleeps.append(duration)
        self.now += duration

    def test_charges_both_counters_even_when_the_larger_counter_switches(self):
        self.values[:] = [300, 0]
        self.assertEqual(self.pacer.throttle()['charged_bytes'], 300)
        self.values[:] = [300, 200]
        self.assertEqual(self.pacer.throttle()['charged_bytes'], 200)
        self.assertEqual(sum(self.sleeps), 5)

    def test_pause_does_not_accumulate_a_burst_allowance(self):
        self.values[:] = [100, 0]
        self.pacer.throttle()
        self.now += 3600
        self.values[:] = [200, 0]
        self.pacer.throttle()
        self.assertEqual(self.sleeps, [1, 1])

    def test_stops_on_physical_overshoot_or_counter_regression(self):
        self.values[:] = [1, 501]
        with self.assertRaises(ValueError):
            self.pacer.throttle()
        self.values[:] = [10, 10]
        self.pacer.throttle()
        self.values[:] = [11, 9]
        with self.assertRaises(ValueError):
            self.pacer.throttle()

    def test_interruption_does_not_cancel_unpaid_debt(self):
        self.values[:] = [100, 0]
        self.pacer.health = lambda: (_ for _ in ()).throw(RuntimeError('stop'))
        with self.assertRaises(RuntimeError):
            self.pacer.throttle()
        self.pacer.health = lambda: None
        self.pacer.throttle()
        self.assertEqual(self.sleeps, [1])


class SQLiteDescriptorTests(unittest.TestCase):
    def test_only_open_readonly_sqlite_source_is_advised(self):
        with tempfile.TemporaryDirectory() as directory:
            source, other = (Path(directory) / name for name in ('source.sqlite', 'other.sqlite'))
            for path in (source, other):
                db = sqlite3.connect(path)
                db.execute('create table t(n integer)')
                db.commit(); db.close()
            source_db = sqlite3.connect('file:' + str(source) + '?mode=ro', uri=True)
            other_db = sqlite3.connect('file:' + str(other) + '?mode=ro', uri=True)
            try:
                advised = []
                def capture(fd, offset, length, advice):
                    info = os.fstat(fd)
                    advised.append((info.st_dev, info.st_ino, offset, length, advice))
                with patch.object(pacing.os, 'posix_fadvise', side_effect=capture):
                    self.assertGreater(pacing.disable_sqlite_readahead(source), 0)
                expected = source.stat()
                self.assertTrue(advised)
                self.assertTrue(all(x == (expected.st_dev, expected.st_ino, 0, 0, os.POSIX_FADV_RANDOM)
                                    for x in advised))
            finally:
                source_db.close(); other_db.close()

    def test_refuses_writable_missing_and_symlink_sources(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / 'source.sqlite'
            db = sqlite3.connect(source)
            db.execute('create table t(n integer)'); db.commit()
            try:
                with self.assertRaises(ValueError):
                    pacing.disable_sqlite_readahead(source)
            finally:
                db.close()
            with self.assertRaises(ValueError):
                pacing.disable_sqlite_readahead(source)
            link = Path(directory) / 'link.sqlite'; link.symlink_to(source)
            with self.assertRaises(ValueError):
                pacing.disable_sqlite_readahead(link)


if __name__ == '__main__':
    unittest.main()
