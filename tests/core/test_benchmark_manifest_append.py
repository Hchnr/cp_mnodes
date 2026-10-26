from pathlib import Path
import tempfile
import unittest

from benchmark_manifest_append import run_case


class SyntheticManifestTests(unittest.TestCase):
    def test_both_modes_preserve_the_old_prefix_and_append_every_line(self):
        with tempfile.TemporaryDirectory(prefix='synthetic-manifest-fixture-') as name:
            for index, append in enumerate((True, False)):
                result = run_case(Path(name), index, append, 8192, records=300)
                self.assertEqual(len(Path(result['path']).read_text().splitlines()), 301)


if __name__ == '__main__':
    unittest.main()
