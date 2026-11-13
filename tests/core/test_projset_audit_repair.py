import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sqlite3
import stat
import subprocess
import sys
import tempfile
import textwrap
import time
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'src/core'))


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


audit = load('repair_test_audit', ROOT / 'ops/diagnostics/projset_full_audit.py')
repair = load('repair_test', ROOT / 'ops/diagnostics/projset_audit_repair.py')
recovery = load('repair_test_recovery', ROOT / 'ops/copy/projset_recovery_copy.py')


class AuditRepairTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.source = self.root / 'source'
        self.target = self.root / 'target'
        self.source.mkdir(); self.target.mkdir()
        self.cfg = {'source': str(self.source), 'target': str(self.target), 'run_id': 'audit-test'}
        recovery.engine.configure_directory_opener(recovery.native.open_parent_readonly)
        repair.configure_no_replace(recovery.engine)

    def tearDown(self):
        self.temp.cleanup()

    def test_copy_missing_file_hash_and_no_replace(self):
        (self.source / 'dir').mkdir()
        (self.source / 'dir/file').write_text('important data')
        result = repair.repair_one(recovery, self.cfg, 'dir/file')
        self.assertEqual(result['status'], 'copied')
        self.assertEqual((self.target / 'dir/file').read_text(), 'important data')
        self.assertEqual(result['receipt']['sha256'], hashlib.sha256(b'important data').hexdigest())
        (self.target / 'dir/file').write_text('other contents')
        again = repair.repair_one(recovery, self.cfg, 'dir/file')
        self.assertEqual(again['status'], 'manual_conflict')
        self.assertEqual((self.target / 'dir/file').read_text(), 'other contents')

    def test_existing_equal_file_does_not_change_target_metadata(self):
        (self.source / 'file').write_text('same')
        (self.target / 'file').write_text('same')
        os.chmod(self.source / 'file', 0o600)
        os.chmod(self.target / 'file', 0o644)
        os.utime(self.target / 'file', ns=(1600000000000000000, 1600000000000000000))
        before = (self.target / 'file').stat()
        result = repair.repair_one(recovery, self.cfg, 'file')
        after = (self.target / 'file').stat()
        self.assertEqual(result['status'], 'already_verified')
        self.assertEqual(stat.S_IMODE(before.st_mode), stat.S_IMODE(after.st_mode))
        self.assertEqual(before.st_mtime_ns, after.st_mtime_ns)

    def test_symlinks_preserved_without_touching_existing_target(self):
        (self.source / 'link').symlink_to('/source/reference')
        self.assertEqual(repair.repair_one(recovery, self.cfg, 'link')['status'], 'copied')
        self.assertEqual(os.readlink(self.target / 'link'), '/source/reference')
        before = (self.target / 'link').lstat()
        self.assertEqual(repair.repair_one(recovery, self.cfg, 'link')['status'], 'already_verified')
        after = (self.target / 'link').lstat()
        self.assertEqual((before.st_mtime_ns, before.st_ctime_ns), (after.st_mtime_ns, after.st_ctime_ns))
        (self.target / 'link').unlink(); (self.target / 'link').symlink_to('/different')
        self.assertEqual(repair.repair_one(recovery, self.cfg, 'link')['status'], 'manual_conflict')
        self.assertEqual(os.readlink(self.target / 'link'), '/different')

    def test_missing_directory_conflict_and_exclusion(self):
        (self.source / 'dir').mkdir()
        self.assertEqual(repair.repair_one(recovery, self.cfg, 'dir')['status'], 'directory_ready')
        (self.source / 'bad').mkdir(); (self.target / 'bad').write_text('keep')
        self.assertEqual(repair.repair_one(recovery, self.cfg, 'bad')['status'], 'manual_conflict')
        self.assertEqual((self.target / 'bad').read_text(), 'keep')
        self.assertEqual(repair.repair_one(recovery, self.cfg, 'eai_pwm/datasets/Sekai/item')['status'], 'excluded')
        self.assertFalse(repair.excluded('eai_pwm/datasets/Sekai-other'))

    def test_target_symlink_ancestor_is_never_followed(self):
        (self.source / 'dir').mkdir(); (self.source / 'dir/file').write_text('data')
        other = self.root / 'other'; other.mkdir()
        (self.target / 'dir').symlink_to(other)
        result = repair.repair_one(recovery, self.cfg, 'dir/file')
        self.assertEqual(result['status'], 'manual_conflict')
        self.assertFalse((other / 'file').exists())

    def test_outbox_captures_existing_and_future_issues_exactly_once_at_install(self):
        path = self.root / 'audit.sqlite'
        db = audit.database(path)
        db.execute('insert into issues values(?,?,?,?,?)', (1, audit.encoded('old'), 'missing_target', 1,
                   audit.encoded({'rel': 'old', 'issue': 'missing_target'})))
        db.execute('insert into issues values(?,?,?,?,?)', (1, audit.encoded('conflict'), 'different_size', 1,
                   audit.encoded({'rel': 'conflict', 'issue': 'different_size'})))
        db.commit(); db.close()
        self.assertEqual(repair.install_outbox(path), 1)
        self.assertEqual(repair.install_outbox(path), 1)
        db = sqlite3.connect(path)
        db.execute('insert into issues values(?,?,?,?,?)', (1, audit.encoded('new'), 'missing_target', 1,
                   audit.encoded({'rel': 'new', 'issue': 'missing_target'})))
        db.commit()
        self.assertEqual([json.loads(x[0]) for x in db.execute('select rel from repair_outbox order by id')], ['old', 'new'])
        db.close()

    def test_locked_outbox_retries_and_does_not_hide_other_database_errors(self):
        path = self.root / 'audit-lock.sqlite'
        db = audit.database(path); db.close()
        repair.install_outbox(path)
        writer = sqlite3.connect(path, timeout=.1)
        reader = sqlite3.connect('file:' + str(path) + '?mode=ro', uri=True, timeout=.05)
        try:
            writer.execute('begin exclusive')
            writer.execute('insert into issues values(?,?,?,?,?)', (1, audit.encoded('later'), 'missing_target', 1,
                           audit.encoded({'rel': 'later', 'issue': 'missing_target'})))
            self.assertIsNone(repair.read_outbox(reader, 0))
            writer.commit()
            rows = repair.read_outbox(reader, 0)
            self.assertEqual(len(rows), 1)
            self.assertEqual(json.loads(rows[0][1]), 'later')
            with self.assertRaises(sqlite3.OperationalError):
                repair.read_outbox(sqlite3.connect(':memory:'), 0)
        finally:
            writer.close(); reader.close()

    def test_final_audit_chunk_flows_to_verified_repair(self):
        payload = b'final chunk payload'
        (self.source / 'file').write_bytes(payload)
        path = self.root / 'final-audit.sqlite'
        audit.database(path).close()
        repair.install_outbox(path)
        db = sqlite3.connect(path)
        stats = json.loads(db.execute('select value from meta where key="stats"').fetchone()[0])
        db.execute('update dirs set state=1,node=?,attempt=1 where id=1', ('node',))
        message = {'event': 'done', 'id': 1, 'attempt': 1,
                   'counts': {'files': 1, 'file_bytes': len(payload), 'missing_target': 1},
                   'final_children': [], 'final_issues': [{'rel': 'file', 'issue': 'missing_target',
                       'source_type': 'file', 'target_type': 'missing', 'source_bytes': len(payload)}]}
        self.assertTrue(audit.consume(db, stats, 'node', message))
        db.execute('update meta set value=? where key="stats"', (audit.encoded(stats),))
        db.commit()
        rows = db.execute('select id,rel,data from repair_outbox order by id').fetchall()
        self.assertEqual(len(rows), 1)
        db.close()

        queue = repair.database(self.root / 'final-repair.sqlite')
        repair_stats = json.loads(queue.execute('select value from meta where key="stats"').fetchone()[0])
        repair.ingest(queue, repair_stats, rows)
        queue.execute('update tasks set state=1,node=?,attempt=1,leased_seq=1 where id=1', ('node',))
        result = repair.repair_one(recovery, self.cfg, 'file')
        self.assertTrue(repair.consume(queue, repair_stats, 'node',
            {'event': 'done', 'id': 1, 'attempt': 1, 'result': result}))
        self.assertEqual((repair_stats['total'], repair_stats['done'], repair_stats['copied_bytes']),
                         (1, 1, len(payload)))
        self.assertEqual((self.target / 'file').read_bytes(), payload)
        self.assertEqual(result['receipt']['sha256'], hashlib.sha256(payload).hexdigest())
        queue.close()

    def test_resume_duplicates_and_new_observation_during_copy(self):
        db = repair.database(self.root / 'repair.sqlite')
        stats = json.loads(db.execute('select value from meta where key="stats"').fetchone()[0])
        repair.ingest(db, stats, [(1, repair.encoded('file'), '{}')])
        db.execute('update tasks set state=1,node=?,attempt=1,leased_seq=1 where id=1', ('node',))
        repair.ingest(db, stats, [(2, repair.encoded('file'), '{}')])
        self.assertTrue(repair.consume(db, stats, 'node', {'event': 'done', 'id': 1, 'attempt': 1,
            'result': {'status': 'copied', 'receipt': {'status': 'copied', 'bytes': 5}}}))
        self.assertEqual(db.execute('select state from tasks where id=1').fetchone()[0], 0)
        self.assertEqual(stats['done'], 0)
        db.execute('update tasks set state=1,node=?,attempt=2,leased_seq=2 where id=1', ('node',))
        self.assertFalse(repair.consume(db, stats, 'node', {'event': 'done', 'id': 1, 'attempt': 1, 'result': {}}))
        self.assertTrue(repair.consume(db, stats, 'node', {'event': 'done', 'id': 1, 'attempt': 2,
            'result': {'status': 'already_verified', 'receipt': {'status': 'skipped'}}}))
        self.assertEqual((stats['total'], stats['done'], stats['copied_bytes']), (1, 1, 5))
        repair.ingest(db, stats, [(3, repair.encoded('file'), '{}')])
        self.assertEqual((stats['total'], stats['done']), (1, 0))
        self.assertEqual(db.execute('select state from tasks where id=1').fetchone()[0], 0)
        db.execute('update meta set value=? where key="stats"', (repair.encoded(stats),))
        db.commit(); db.close()
        db = repair.database(self.root / 'repair.sqlite')
        self.assertEqual(json.loads(db.execute('select value from meta').fetchone()[0])['cursor'], 3)
        db.close()

    def test_retries_remain_pending_until_success(self):
        db = repair.database(self.root / 'repair.sqlite')
        stats = json.loads(db.execute('select value from meta').fetchone()[0])
        repair.ingest(db, stats, [(1, repair.encoded('file'), '{}')])
        db.execute('update tasks set state=1,node=?,attempt=1,leased_seq=1 where id=1', ('node',))
        repair.consume(db, stats, 'node', {'event': 'retry', 'id': 1, 'attempt': 1, 'detail': 'ESTALE'})
        self.assertEqual(db.execute('select state from tasks').fetchone()[0], 3)
        self.assertEqual(stats['done'], 0)
        db.execute('update tasks set state=1,node=?,attempt=2,leased_seq=1 where id=1', ('node',))
        repair.consume(db, stats, 'node', {'event': 'done', 'id': 1, 'attempt': 2,
            'result': {'status': 'already_verified', 'receipt': {'status': 'skipped'}}})
        self.assertEqual((stats['done'], stats['retries']), (1, 1))
        db.close()

    def test_controller_restart_releases_inflight_lease_without_losing_cursor(self):
        path = self.root / 'repair.sqlite'
        db = repair.database(path)
        stats = json.loads(db.execute('select value from meta').fetchone()[0])
        repair.ingest(db, stats, [(5, repair.encoded('file'), '{}')])
        db.execute('update tasks set state=1,node=?,attempt=1,leased_seq=5 where id=1', ('node',))
        db.execute('update meta set value=? where key="stats"', (repair.encoded(stats),))
        db.commit(); db.close()
        db = repair.database(path)
        self.assertEqual(db.execute('select state,node,latest_seq from tasks').fetchone(), (0, None, 5))
        restored = json.loads(db.execute('select value from meta').fetchone()[0])
        self.assertEqual((restored['cursor'], restored['total'], restored['done']), (5, 1, 0))
        db.close()

    def test_live_outbox_worker_failure_and_final_drain(self):
        run, work, audit_work = self.root / 'run', self.root / 'work', self.root / 'audit'
        (run / 'recovery/code').mkdir(parents=True)
        (work / 'code').mkdir(parents=True); (work / 'logs').mkdir()
        audit_work.mkdir()
        code = work / 'code/projset_audit_repair.py'
        code.write_bytes((ROOT / 'ops/diagnostics/projset_audit_repair.py').read_bytes())
        (work / 'manifest.json').write_text(json.dumps({'code_sha256': hashlib.sha256(code.read_bytes()).hexdigest()}))
        wrapper = self.root / 'fake_repair_worker.py'
        wrapper.write_text(textwrap.dedent('''
            import importlib.util,json,os,pathlib,sys
            args=dict(zip(sys.argv[1::2],sys.argv[2::2]));work=pathlib.Path(args['--work'])
            marker=work/'disconnected'
            if args['--node']=='one' and not marker.exists():
                marker.write_text('yes');print(json.dumps({'event':'ready','host':'fake','pid':os.getpid()}),flush=True)
                for line in sys.stdin:
                    if json.loads(line)['event']=='task':os._exit(7)
            spec=importlib.util.spec_from_file_location('r',work/'code/projset_audit_repair.py')
            r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r);original=r.repair_one
            def fixture(recovery,cfg,rel):
                return original(recovery,dict(cfg,source=os.environ['TEST_SOURCE'],target=os.environ['TEST_TARGET']),rel)
            r.repair_one=fixture;r.worker(pathlib.Path(args['--run']),work,args['--node'])
        '''))
        (run / 'recovery/code/projset_recovery_copy.py').write_text(textwrap.dedent('''
            import importlib.util,shlex,sys
            sys.path.insert(0,%r)
            spec=importlib.util.spec_from_file_location('real_recovery',%r)
            real=importlib.util.module_from_spec(spec);spec.loader.exec_module(real)
            engine=real.engine;native=real.native;copy_entry_with_estale_retry=real.copy_entry_with_estale_retry
            def validate(run,cfg,node): pass
            def ssh(node,command): return [sys.executable,'-S','-B',%r]+shlex.split(command)[5:]
        ''' % (str(ROOT / 'src/core'), str(ROOT / 'ops/copy/projset_recovery_copy.py'), str(wrapper))))
        (run / 'config.json').write_text(json.dumps({'source': str(self.source), 'target': str(self.target), 'run_id': 'test'}))
        (run / 'recovery/ssh.json').write_text(json.dumps({'hosts': {'one': 'fake1', 'two': 'fake2'}}))
        (audit_work / 'status.json').write_text('{"state":"running"}')
        db = audit.database(audit_work / 'audit.sqlite3')
        db.execute('insert into issues values(?,?,?,?,?)', (1, audit.encoded('first'), 'missing_target', 1,
                   audit.encoded({'rel': 'first', 'issue': 'missing_target'})))
        db.commit(); db.close()
        repair.install_outbox(audit_work / 'audit.sqlite3')
        (self.source / 'first').write_text('one')
        (self.source / 'second').write_text('two')
        env = dict(os.environ, TEST_SOURCE=str(self.source), TEST_TARGET=str(self.target))
        writer = sqlite3.connect(audit_work / 'audit.sqlite3', timeout=5)
        writer.execute('begin exclusive')
        proc = subprocess.Popen([sys.executable, '-S', '-B', str(code), 'controller', '--run', str(run),
                                 '--work', str(work), '--audit', str(audit_work)], env=env,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            # Hold an actual exclusive SQLite lock through the first read
            # timeout; the controller must keep running without declaring the
            # pending outbox empty and retry once the lock is released.
            deadline = time.monotonic() + 10
            observed_busy = False
            while time.monotonic() < deadline:
                self.assertIsNone(proc.poll())
                try:
                    observed_busy = json.loads((work / 'status.json').read_text()).get('outbox_busy_retries', 0) > 0
                except (OSError, ValueError):
                    pass
                if observed_busy:
                    break
                time.sleep(.05)
            self.assertTrue(observed_busy)
            self.assertFalse((self.target / 'first').exists())
            writer.commit(); writer.close(); writer = None
            deadline = time.monotonic() + 20
            while not (self.target / 'first').exists() and time.monotonic() < deadline:
                time.sleep(.05)
            self.assertTrue((self.target / 'first').exists())
            db = sqlite3.connect(audit_work / 'audit.sqlite3')
            db.execute('begin exclusive')
            db.execute('insert into issues values(?,?,?,?,?)', (1, audit.encoded('second'), 'missing_target', 1,
                       audit.encoded({'rel': 'second', 'issue': 'missing_target'})))
            (audit_work / 'status.json').write_text('{"state":"finished"}')
            # A locked final poll cannot be interpreted as an empty immutable
            # outbox; the newly committed entry must still be copied.
            time.sleep(3.5)
            self.assertIsNone(proc.poll())
            self.assertFalse((self.target / 'second').exists())
            db.commit(); db.close()
            out, err = proc.communicate(timeout=20)
            self.assertEqual(proc.returncode, 0, (out, err))
            status = json.loads((work / 'status.json').read_text())
            self.assertEqual(status['state'], 'finished')
            self.assertEqual((status['total'], status['done']), (2, 2))
            self.assertEqual((self.target / 'second').read_text(), 'two')
            self.assertTrue((work / 'disconnected').exists())
            self.assertIn('second', (work / 'repairs.csv').read_text())
        finally:
            if writer is not None:
                writer.close()
            if proc.poll() is None:
                proc.kill(); proc.communicate()


if __name__ == '__main__':
    unittest.main()
