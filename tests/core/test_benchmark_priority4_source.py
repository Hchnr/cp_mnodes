import os
from pathlib import Path
import tempfile
import unittest

import benchmark_priority4_source as bench


class ReadonlySourceBenchmarkTests(unittest.TestCase):
    def test_source_payloads_are_only_statted_and_symlinks_are_not_followed(self):
        with tempfile.TemporaryDirectory(prefix='p4-source-benchmark-fixture-') as name:
            root = Path(name)
            (root / 'd').mkdir()
            (root / 'd/f').write_bytes(b'unchanged')
            os.symlink('/unavailable/outside', root / 'd/link')
            info = root.stat()
            bench.initialize(root, (info.st_dev, info.st_ino))
            try:
                value = bench.visit('d')
                self.assertEqual((value['files'], value['symlinks'], value['errors']), (1, 1, 0))
                self.assertEqual((root / 'd/f').read_bytes(), b'unchanged')
                with self.assertRaises(ValueError):
                    bench.visit('eai_pwm/private')
            finally:
                bench.STAT_POOL.shutdown()
                os.close(bench.ROOT_FD)


if __name__ == '__main__':
    unittest.main()
