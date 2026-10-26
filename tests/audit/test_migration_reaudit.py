import importlib.util
import os
from pathlib import Path
import shutil
import sys
import tempfile
import unittest

import migration_reaudit as launch


class ReauditTests(unittest.TestCase):
    def test_partition_covers_root_without_overlap(self):
        self.assertTrue(launch.validate_partition([
            dict(rel='',leaves_only=True),dict(rel='a',leaves_only=True),
            dict(rel='a/b',leaves_only=False),dict(rel='c',leaves_only=False)]))

    def test_partition_rejects_holes_overlap_raw_duplicate(self):
        for units in ([dict(rel='a',leaves_only=False)],
                      [dict(rel='',leaves_only=False),dict(rel='a',leaves_only=False)],
                      [dict(rel='',leaves_only=True),dict(rel='a/b',leaves_only=False)],
                      [dict(rel='',leaves_only=True),dict(rel='',leaves_only=True)],
                      [dict(rel='',leaves_only=True),dict(rel='eai_pwm',leaves_only=True),
                       dict(rel='eai_pwm/datasets',leaves_only=True),
                       dict(rel='eai_pwm/datasets/raw',leaves_only=False)]):
            with self.subTest(units=units), self.assertRaises(ValueError):
                launch.validate_partition(units)

    def load_scan(self, tools):
        sys.path.insert(0,str(tools))
        spec=importlib.util.spec_from_file_location('isolated_reaudit_scan',tools/'scan.py')
        module=importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self.addCleanup(lambda:sys.path.remove(str(tools)))
        self.addCleanup(lambda:sys.modules.pop('readonly_scan_core',None))
        return module

    def test_new_shallow_child_discovered_without_stealing_known_owner(self):
        with tempfile.TemporaryDirectory(prefix='reaudit-test-') as directory:
            root=Path(directory);tools=root/'tools';tools.mkdir()
            shutil.copyfile(launch.ORIGINAL/'tools/cp_mnodes_readonly_scan_20261024.py',tools/'readonly_scan_core.py')
            shutil.copyfile(Path(launch.__file__).with_name('migration_reaudit_scan.py'),tools/'scan.py')
            scan=self.load_scan(tools)
            source,target=root/'source',root/'target';source.mkdir();target.mkdir()
            (source/'known').mkdir();(source/'new').mkdir()
            (source/'leaf').write_text('source only')
            scan.COVERED_ROOTS.update(('', 'known'))
            audit=scan.Audit(str(source),str(target),root/'report',threads=1,stat_threads=1)
            try:
                children=audit.scan_directory(('',True))
                self.assertEqual(children,[('new',False)])
                self.assertEqual(audit.counts['missing_files'],1)
                self.assertEqual(audit.counts['new_directory_since_partition'],1)
                self.assertEqual(audit.counts['source_logical_bytes'],11)
                self.assertFalse(list(target.iterdir()))
                self.assertEqual((source/'leaf').read_text(),'source only')
            finally:
                audit.attributes.shutdown()
                for stream in audit.files.values():stream.close()


if __name__=='__main__':unittest.main()
