import io
import json
import os
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest import mock
import uuid

from migration_io import read_json, write_json
import migration_prepared_resume as previous
import migration_prepare_next_resume as next_resume
from migration_scan import _stat_fields


class NextCheckpointTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir='/tmp')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.control = self.root/'control'; self.control.mkdir()
        self.source = self.root/'source'; self.source.mkdir()
        self.run_id = 'next-fixture-'+uuid.uuid4().hex
        self.old = self.control/'first'; self.old.mkdir()
        self.new = self.control/'second'
        self.gc_patch = mock.patch.object(previous.gc, 'freeze')
        self.gc_patch.start(); self.addCleanup(self.gc_patch.stop)
        self.umask = os.umask(0o077); self.addCleanup(os.umask, self.umask)
        self.addCleanup(previous.gc.enable)
        for name in ('first', 'second'):
            scratch = Path('/tmp')/(self.run_id+'-'+name)
            self.addCleanup(shutil.rmtree, scratch, True)
        self.entries = []
        for name in ('assigned', 'old-orphan', 'new-orphan'):
            path = self.source/name; path.write_bytes(name.encode())
            self.entries.append(dict(rel=name, kind='file', **_stat_fields(path.stat())))
        self.ledger = self.control/'ledger.jsonl'
        self.ledger.write_text(json.dumps(dict(id=0, entries=[self.entries[0]]))+'\n')
        original_manifest = self.control/'original.jsonl'
        original_manifest.write_text(''.join(json.dumps(e)+'\n' for e in self.entries[:2]))
        self.old_handover = self.control/'old-handover.json'
        initial = dict(assigned_tasks_path=str(self.ledger),
                       ledger_sha256=previous.digest_file(self.ledger), next_task_id=1)
        write_json(self.old_handover, initial)
        write_json(self.control/'config.json', dict(run_id=self.run_id, source=str(self.source)))
        state = dict(recovery_warm=True, stage='p3-r3', scan_complete=False,
                     recovery_replay={'phase':'fresh_scan'}, foreground_handover=str(self.old_handover),
                     active_manifest=str(original_manifest))
        write_json(self.control/'state.json', state)
        with mock.patch('builtins.print'):
            self.meta = previous.prepare(self.control, self.old)
        self.current = self.control/'current-handover.json'
        write_json(self.current, dict(initial, prepared_resume=str(self.old/'prepared.json'),
                                     predecessor_handover=str(self.old_handover)))
        self.active = Path(self.meta['manifest'])
        with open(self.active, 'ab') as handle:
            handle.write((json.dumps(self.entries[-1])+'\n').encode())
        self.prior_tail = Path(self.meta['cache']).parent/'tail-orphans.jsonl'
        self.prior_tail.write_text('')
        state.update(foreground_handover=str(self.current), active_manifest=str(self.active))
        write_json(self.control/'state.json', state)

    def prepare(self):
        with mock.patch('builtins.print'):
            return next_resume.prepare_next(self.control, self.new)

    def test_reuses_cache_without_opening_base_ledger_and_only_parses_tail(self):
        self.ledger.unlink()  # No full historical ledger read is permitted.
        original_load = next_resume.json.loads
        parsed = []
        def load(value, *args, **kwargs):
            if isinstance(value, bytes):
                parsed.append(value)
            return original_load(value, *args, **kwargs)
        with mock.patch.object(next_resume.json, 'loads', side_effect=load):
            result = self.prepare()
        self.assertEqual(len(parsed), 1)
        self.assertEqual(result['parsed_tail_records'], 1)
        self.assertEqual(result['base_next_task_id'], 1)
        self.assertEqual(result['seeded_paths'], 3)
        self.assertEqual(result['assigned_paths'], 1)
        self.assertEqual(result['orphans'], 2)
        self.assertEqual(result['source_handover'], str(self.current))
        self.assertEqual(Path(result['manifest']).read_bytes(), self.active.read_bytes())
        handover = dict(read_json(self.current), prepared_resume=str(self.new/'prepared.json'),
                        predecessor_handover=str(self.current))
        _, values = previous.load_checkpoint(handover, self.control)
        self.assertEqual(values['assigned'], {'assigned'})
        self.assertEqual(values['seeded_paths'], {'assigned','old-orphan','new-orphan'})
        self.assertEqual(result['spools'][:-2], self.meta['spools'])
        self.assertEqual(result['spools'][-2], str(self.prior_tail))
        self.assertNotEqual(Path(result['cache']).parent, Path(self.meta['cache']).parent)

    def test_snapshot_omits_incomplete_live_tail(self):
        with open(self.active, 'ab') as handle:
            handle.write(b'{"rel":"not-complete')
        result = self.prepare()
        self.assertTrue(Path(result['manifest']).read_bytes().endswith(b'\n'))
        self.assertEqual(result['manifest_offset'], self.active.stat().st_size-len(b'{"rel":"not-complete'))
        self.assertEqual(result['parsed_tail_records'], 1)

    def test_refuses_cached_prefix_corruption(self):
        with open(self.active, 'r+b') as handle:
            handle.write(b'X')
        with self.assertRaisesRegex(RuntimeError, 'cached prefix'):
            self.prepare()
        self.assertFalse((self.new/'prepared.json').exists())

    def test_missing_predecessor_spool_does_not_publish_ready_descriptor(self):
        self.prior_tail.unlink()
        with self.assertRaisesRegex(RuntimeError, 'spool is missing'):
            self.prepare()
        self.assertFalse((self.new/'prepared.json').exists())

    def test_uses_explicit_runtime_tail_spool_for_chained_handovers(self):
        selected=self.prior_tail.with_name('tail-orphans-adoption-specific.jsonl')
        self.prior_tail.rename(selected)
        state=read_json(self.control/'state.json')
        state['recovery_replay']['tail_spool']=str(selected)
        write_json(self.control/'state.json',state)
        result=self.prepare()
        self.assertIn(str(selected),result['spools'])
        self.assertNotIn(str(self.prior_tail),result['spools'])

    def test_complete_prefix_handles_lines_larger_than_one_block(self):
        with mock.patch.object(next_resume, 'BLOCK', 4):
            self.assertEqual(next_resume.complete_prefix(io.BytesIO(b'a\n123456789'), 11), 2)
            self.assertEqual(next_resume.complete_prefix(io.BytesIO(b'123456789'), 9), 0)


if __name__ == '__main__':
    unittest.main()
