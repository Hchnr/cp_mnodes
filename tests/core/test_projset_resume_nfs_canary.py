import json
import multiprocessing
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch


OPS = Path(__file__).resolve().parents[2] / 'ops/diagnostics'
sys.path.insert(0, str(OPS))
import projset_resume_nfs_canary as canary
import projset_snapshot_capture_canary as capture


class ResumeCanaryRolesTests(unittest.TestCase):
    def test_interrupted_snapshot_includes_concurrent_writer_and_stops_it(self):
        with tempfile.TemporaryDirectory(prefix=capture.MARKER + '-roles-') as directory:
            proto = Path(directory)
            work = proto / canary.WORK.name
            work.mkdir()
            source = work / capture.LEGACY
            with patch.multiple(canary, PROTO=proto, WORK=work, SOURCE=source, ROWS=1024), \
                    patch.object(canary.safety, 'live_states', return_value=([{}], [])):
                prepared = canary.prepare()
                self.assertEqual(prepared['initial_rows'], 1024)
                writer = multiprocessing.get_context('fork').Process(target=canary.writer)
                writer.start()
                try:
                    deadline = time.monotonic() + 10
                    while not (work / 'writer-ready.json').exists() and time.monotonic() < deadline:
                        if not writer.is_alive():
                            self.fail('synthetic writer exited before becoming ready')
                        time.sleep(.05)
                    self.assertTrue((work / 'writer-ready.json').exists())
                    result = canary.scan()
                finally:
                    (work / 'WRITER_STOP').touch(exist_ok=True)
                    writer.join(timeout=10)
                    if writer.is_alive():
                        writer.terminate()
                        writer.join(timeout=5)
                self.assertFalse(writer.is_alive())
                self.assertEqual(writer.exitcode, 0)
                self.assertEqual(result['state'], 'passed')
                self.assertTrue(result['overlap']['resumed'])
                self.assertGreater(result['overlap']['pages_while_writer_running'], 0)
                writer_result = json.loads((work / 'writer-done.json').read_text())
                self.assertGreaterEqual(writer_result['transactions'], 2)
                self.assertEqual(result['snapshot']['seq'], writer_result['last_seq'])
                self.assertEqual(result['writer'], writer_result)


if __name__ == '__main__':
    unittest.main()
