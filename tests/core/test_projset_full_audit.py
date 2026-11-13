import importlib.util
import json
import hashlib
import os
from pathlib import Path
import select
import sqlite3
import subprocess
import sys
import tempfile
import textwrap
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'src/core'))
import migration_linux_open as native

spec = importlib.util.spec_from_file_location('full_audit', ROOT / 'ops/diagnostics/projset_full_audit.py')
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)


class FullAuditTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.source = self.root / 'source'
        self.target = self.root / 'target'
        self.source.mkdir()
        self.target.mkdir()
        self.srcfd = native._open_absolute_directory(str(self.source))
        self.dstfd = native._open_absolute_directory(str(self.target))

    def tearDown(self):
        os.close(self.srcfd)
        os.close(self.dstfd)
        self.temp.cleanup()

    def scan(self, rel=''):
        chunks = []
        counts = audit.scan_directory(native, self.srcfd, self.dstfd, rel,
                                      lambda children, issues: chunks.append((children, issues)), 2)
        children = [item for c, _ in chunks for item in c]
        issues = [item for _, i in chunks for item in i]
        return counts, children, issues

    def test_detects_missing_size_type_link_and_does_not_mutate_target(self):
        (self.source / 'missing').write_text('abc')
        (self.source / 'different_size').write_text('abc')
        (self.target / 'different_size').write_text('x')
        (self.source / 'directory').mkdir()
        (self.target / 'directory').write_text('not a directory')
        (self.source / 'link').symlink_to('/one')
        (self.target / 'link').symlink_to('/two')
        (self.source / '.hidden').write_text('x')
        counts, children, issues = self.scan()
        pairs = {(i['rel'], i['issue']) for i in issues}
        self.assertEqual(counts['entries'], 5)
        self.assertEqual(set(children), {'directory'})
        self.assertIn(('missing', 'missing_target'), pairs)
        self.assertIn(('.hidden', 'missing_target'), pairs)
        self.assertIn(('different_size', 'different_size'), pairs)
        self.assertIn(('directory', 'different_type'), pairs)
        self.assertIn(('link', 'different_link'), pairs)
        self.assertFalse((self.target / 'missing').exists())
        self.assertEqual((self.target / 'directory').read_text(), 'not a directory')

    def test_same_size_is_not_content_verified(self):
        (self.source / 'file').write_text('one')
        (self.target / 'file').write_text('two')
        when = 1700000000000000000
        os.utime(self.source / 'file', ns=(when, when))
        os.utime(self.target / 'file', ns=(when, when))
        counts, _, issues = self.scan()
        self.assertEqual(issues, [])
        self.assertEqual(counts['same_type_and_metadata'], 1)
        self.assertNotEqual((self.source / 'file').read_text(), (self.target / 'file').read_text())

    def test_exclusion_and_target_symlink_are_not_followed(self):
        (self.source / 'eai_pwm/datasets/Sekai').mkdir(parents=True)
        (self.source / 'eai_pwm/datasets/Sekai/ignored').write_text('x')
        (self.target / 'eai_pwm/datasets').mkdir(parents=True)
        counts, children, issues = self.scan('eai_pwm/datasets')
        self.assertEqual((counts['excluded_entries'], children, issues), (1, [], []))
        self.assertFalse(audit.excluded('eai_pwm/datasets/Sekai-other'))
        (self.source / 'blocked').mkdir()
        (self.source / 'blocked/file').write_text('x')
        (self.target / 'blocked').symlink_to(self.source / 'blocked')
        counts, children, issues = self.scan('blocked')
        self.assertEqual(counts['target_ancestor_not_directory'], 1)
        self.assertEqual(issues[0]['rel'], 'blocked/file')

    def test_missing_directory_is_still_enumerated(self):
        (self.source / 'new').mkdir()
        (self.source / 'new/child').write_text('payload')
        counts, _, issues = self.scan('new')
        self.assertEqual(counts['missing_target'], 1)
        self.assertEqual(counts['missing_file_bytes'], 7)
        self.assertEqual(issues[0]['rel'], 'new/child')

    def test_full_directory_chunks_flush_before_final_chunk(self):
        for name in ('a', 'b', 'c', 'd', 'e'):
            (self.source / name).mkdir()
        events = []
        counts = audit.scan_directory(native, self.srcfd, self.dstfd, '',
            lambda children, issues: events.append(('chunk', children, issues)), batch_size=3,
            final_emit=lambda children, issues: events.append(('final', children, issues)))
        self.assertEqual([event[0] for event in events], ['chunk', 'chunk', 'final'])
        self.assertEqual([len(event[1]) for event in events], [2, 2, 1])
        self.assertEqual(counts['missing_target'], 5)

    def test_final_chunk_waits_for_successful_directory_validation(self):
        (self.source / 'child').mkdir()
        events = []
        with mock.patch.object(audit, 'unchanged', return_value=False):
            with self.assertRaises(RuntimeError):
                audit.scan_directory(native, self.srcfd, self.dstfd, '',
                    lambda children, issues: events.append('chunk'),
                    final_emit=lambda children, issues: events.append('final'))
        self.assertEqual(events, [])
        result = audit.scan_directory(native, self.srcfd, self.dstfd, 'gone',
            lambda children, issues: events.append('chunk'),
            final_emit=lambda children, issues: events.append(('final', issues[0]['issue'])))
        self.assertEqual(result, {'source_disappeared': 1})
        self.assertEqual(events, [('final', 'source_disappeared')])

    def test_committed_chunks_survive_restart_and_completion_is_exactly_once(self):
        path = self.root / 'audit.sqlite'
        db = audit.database(path)
        stats = json.loads(db.execute('select value from meta where key="stats"').fetchone()[0])
        db.execute('update dirs set state=1,node=?,attempt=1 where id=1', ('node',))
        self.assertTrue(audit.consume(db, stats, 'node', {'event': 'chunk', 'id': 1, 'attempt': 1,
            'children': ['child', 'child'], 'issues': [{'rel': 'old', 'issue': 'missing_target'}]}))
        db.execute('update meta set value=? where key="stats"', (audit.encoded(stats),))
        db.commit()
        db.close()
        db = audit.database(path)
        self.assertEqual(db.execute('select state from dirs where id=1').fetchone()[0], 0)
        stats = json.loads(db.execute('select value from meta where key="stats"').fetchone()[0])
        self.assertEqual(stats['discovered_dirs'], 2)
        db.execute('update dirs set state=1,node=?,attempt=2 where id=1', ('other',))
        self.assertFalse(audit.consume(db, stats, 'node', {'event': 'done', 'id': 1, 'attempt': 1, 'counts': {}}))
        self.assertTrue(audit.consume(db, stats, 'other', {'event': 'done', 'id': 1, 'attempt': 2, 'counts': {'entries': 3}}))
        self.assertFalse(audit.consume(db, stats, 'other', {'event': 'done', 'id': 1, 'attempt': 2, 'counts': {'entries': 3}}))
        self.assertEqual(stats['done_dirs'], 1)
        self.assertEqual(stats['counts']['entries'], 3)
        self.assertEqual(db.execute('select count(*) from issues').fetchone()[0], 0)
        db.close()

    def test_transient_failure_remains_retryable_until_success(self):
        db = audit.database(self.root / 'audit.sqlite')
        stats = json.loads(db.execute('select value from meta where key="stats"').fetchone()[0])
        db.execute('update dirs set state=1,node=?,attempt=1 where id=1', ('node',))
        audit.consume(db, stats, 'node', {'event': 'retry', 'id': 1, 'attempt': 1,
                                        'detail': 'OSError: stale file handle'})
        self.assertEqual(db.execute('select state from dirs where id=1').fetchone()[0], 3)
        self.assertEqual(db.execute('select resolved from failures').fetchone()[0], 0)
        db.execute('update dirs set state=1,node=?,attempt=2 where id=1', ('node',))
        audit.consume(db, stats, 'node', {'event': 'done', 'id': 1, 'attempt': 2, 'counts': {}})
        self.assertEqual(db.execute('select resolved from failures').fetchone()[0], 1)
        db.close()

    def test_worker_uses_final_chunk_only_when_controller_advertises_support(self):
        run, work = self.root / 'protocol-run', self.root / 'protocol-work'
        (run / 'recovery/code').mkdir(parents=True)
        (work / 'code').mkdir(parents=True)
        (self.source / 'child').mkdir()
        (self.source / 'missing').write_text('payload')
        deployed = work / 'code/projset_full_audit.py'
        deployed.write_bytes((ROOT / 'ops/diagnostics/projset_full_audit.py').read_bytes())
        (work / 'manifest.json').write_text(json.dumps({'code_sha256': hashlib.sha256(deployed.read_bytes()).hexdigest()}))
        (run / 'config.json').write_text('{}')
        (run / 'recovery/code/projset_recovery_copy.py').write_text(textwrap.dedent('''
            import sys
            sys.path.insert(0, %r)
            import migration_linux_open as native
            def validate(run, cfg, node): pass
        ''' % str(ROOT / 'src/core')))
        wrapper = self.root / 'protocol_worker.py'
        wrapper.write_text(textwrap.dedent('''
            import importlib.util, os, pathlib
            spec = importlib.util.spec_from_file_location('auditor', %r)
            module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
            module.SOURCE = os.environ['TEST_SOURCE']; module.TARGET = os.environ['TEST_TARGET']
            module.worker(pathlib.Path(%r), pathlib.Path(%r), 'one')
        ''' % (str(deployed), str(run), str(work))))
        env = dict(os.environ, TEST_SOURCE=str(self.source), TEST_TARGET=str(self.target))

        for capability in (False, True):
            with self.subTest(capability=capability):
                proc = subprocess.Popen([sys.executable, '-S', '-B', str(wrapper)], env=env,
                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)

                def receive():
                    self.assertTrue(select.select([proc.stdout], [], [], 10)[0], 'worker response timed out')
                    return json.loads(proc.stdout.readline())

                def send(message):
                    proc.stdin.write(json.dumps(message) + '\n'); proc.stdin.flush()

                try:
                    self.assertEqual(receive()['event'], 'ready')
                    task = {'id': 1, 'attempt': 1, 'rel': ''}
                    if capability:
                        task['commit_final_chunk_v1'] = True
                    send({'event': 'task', 'task': task})
                    first = receive()
                    if capability:
                        self.assertEqual(first['event'], 'done')
                        self.assertEqual(first['final_children'], ['child'])
                        self.assertIn('missing', {item['rel'] for item in first['final_issues']})
                    else:
                        self.assertEqual(first['event'], 'chunk')
                        self.assertEqual(first['children'], ['child'])
                        send({'event': 'ack', 'id': 1, 'attempt': 1, 'sequence': first['sequence']})
                        done = receive()
                        self.assertEqual(done['event'], 'done')
                        self.assertNotIn('final_children', done)
                    send({'event': 'stop'})
                    proc.wait(timeout=10)
                    self.assertEqual(proc.returncode, 0)
                finally:
                    if proc.poll() is None:
                        proc.kill(); proc.wait(timeout=10)
                    for stream in (proc.stdin, proc.stdout, proc.stderr):
                        stream.close()

    def test_new_controller_accepts_legacy_worker_chunks(self):
        run, work = self.root / 'legacy-run', self.root / 'legacy-work'
        (run / 'recovery/code').mkdir(parents=True)
        (work / 'code').mkdir(parents=True)
        (work / 'logs').mkdir()
        deployed = work / 'code/projset_full_audit.py'
        deployed.write_bytes((ROOT / 'ops/diagnostics/projset_full_audit.py').read_bytes())
        (work / 'manifest.json').write_text(json.dumps({'reporter': False,
            'code_sha256': hashlib.sha256(deployed.read_bytes()).hexdigest()}))
        wrapper = self.root / 'legacy_worker.py'
        wrapper.write_text(textwrap.dedent('''
            import json, os, pathlib, sys
            args = dict(zip(sys.argv[1::2], sys.argv[2::2]))
            work = pathlib.Path(args['--work'])
            def send(value): print(json.dumps(value), flush=True)
            send({'event': 'ready', 'node': 'one', 'host': 'fake', 'pid': os.getpid()})
            for line in sys.stdin:
                message = json.loads(line)
                if message['event'] == 'stop': break
                if message['event'] != 'task': continue
                task = message['task']; ident = task['id']; attempt = task['attempt']
                if not task['rel']:
                    (work / 'capability-observed').write_text(str(task.get('commit_final_chunk_v1')))
                    send({'event': 'chunk', 'id': ident, 'attempt': attempt, 'sequence': 1,
                          'children': ['child'], 'issues': [{'rel': 'missing', 'issue': 'missing_target',
                              'source_type': 'file', 'target_type': 'missing', 'source_bytes': 7}]})
                    ack = json.loads(sys.stdin.readline())
                    assert ack['event'] == 'ack' and ack['id'] == ident and ack['sequence'] == 1
                    counts = {'entries': 2, 'directorys': 1, 'files': 1, 'file_bytes': 7, 'missing_target': 1}
                else:
                    counts = {}
                send({'event': 'done', 'id': ident, 'attempt': attempt, 'counts': counts})
        '''))
        (run / 'recovery/code/projset_recovery_copy.py').write_text(textwrap.dedent('''
            import shlex, sys
            def ssh(node, command): return [sys.executable, '-S', '-B', %r] + shlex.split(command)[5:]
        ''' % str(wrapper)))
        (run / 'config.json').write_text('{}')
        (run / 'recovery/ssh.json').write_text(json.dumps({'hosts': {'one': 'fake'}, 'reporter_python': '/bin/true'}))
        starter = self.root / 'legacy_controller.py'
        starter.write_text(textwrap.dedent('''
            import importlib.util, pathlib
            spec = importlib.util.spec_from_file_location('auditor', %r)
            module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
            module.THREADS = 1
            module.controller(pathlib.Path(%r), pathlib.Path(%r))
        ''' % (str(deployed), str(run), str(work))))
        subprocess.run([sys.executable, '-S', '-B', str(starter)], check=True, timeout=30,
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        status = json.loads((work / 'status.json').read_text())
        self.assertEqual((work / 'capability-observed').read_text(), 'True')
        self.assertEqual(status['state'], 'finished')
        self.assertEqual(status['done_dirs'], 2)
        self.assertEqual(status['counts']['missing_target'], 1)
        self.assertIn('missing', (work / 'issues.csv').read_text())

    def test_repair_metrics_include_pending_conflicts_and_tb(self):
        values = audit.repair_values({'total': 12, 'done': 7, 'pending': 2, 'active': 2,
            'retry': 1, 'retries': 4, 'copied_operations': 3, 'copied_bytes': 2500000000000,
            'nodes_ready': 16, 'outcomes': {'already_verified': 1, 'directory_ready': 2,
                                            'manual_conflict': 1, 'manual_metadata_warning': 1}})
        self.assertEqual(values['repair/待处理路径'], 5)
        self.assertEqual(values['repair/补传_TB'], 2.5)
        self.assertEqual(values['repair/人工关注'], 2)
        self.assertEqual(values['repair/在线节点'], 16)

    def test_file_and_capacity_rates_use_committed_counters_and_timestamps(self):
        scan = {'updated': 220, 'counts': {'files': 120, 'symlinks': 10, 'file_bytes': 3_000_000_000_000}}
        repair_state = {'updated': 220, 'copied_operations': 6, 'copied_bytes': 600_000_000_000}
        previous = {'scan_time': 100, 'files_links': 70, 'file_bytes': 1_000_000_000_000,
                    'repair_time': 100, 'copied_operations': 2, 'copied_bytes': 200_000_000_000}
        values, sample = audit.speed_values(scan, repair_state, previous)
        self.assertEqual(values['audit/已核对文件及链接'], 130)
        self.assertEqual(values['speed/复核文件及链接_项每小时'], 1800)
        self.assertEqual(values['speed/复核逻辑容量_TB每小时'], 60)
        self.assertEqual(values['speed/补传文件及链接_项每小时'], 120)
        self.assertAlmostEqual(values['speed/补传数据_TB每小时'], 12)
        self.assertEqual(sample['scan_time'], 220)
        reset, _ = audit.speed_values({'updated': 230, 'counts': {'files': 1}}, {}, sample)
        self.assertNotIn('speed/复核文件及链接_项每小时', reset)

    def test_fresh_monitor_metrics_use_current_progress_and_historical_baselines(self):
        scan = {'state': 'running', 'updated': 215, 'started': 70, 'pending_dirs': 20,
                'active_dirs': 4, 'retry_dirs': 2,
                'counts': {'files': 120, 'symlinks': 10, 'file_bytes': 3_000_000_000_000}}
        repair_state = {'state': 'running', 'updated': 208}
        monitor = {'updated': 200, 'audit_state': 'running', 'sample': {'audit_updated': 200},
                   'rates': {'audit_files_links_per_hour': 1800, 'audit_logical_TB_per_hour': 60,
                             'audit_directories_per_hour': 100, 'audit_updated_seconds': 120},
                   'estimates': {'files_baseline': 1030, 'logical_TB_baseline': 6}}
        values = audit.monitor_values(scan, monitor, 220, repair_state)
        self.assertEqual(values['speed20/复核文件及链接_项每小时'], 1800)
        self.assertEqual(values['speed20/复核逻辑容量_TB每小时'], 60)
        self.assertEqual(values['estimate20/按首轮累计项外推_剩余小时'], .5)
        self.assertAlmostEqual(values['estimate20/按首轮累计TB外推_剩余小时'], .05)
        self.assertEqual(values['estimate20/文件外推数据有效'], 1)
        self.assertEqual(values['estimate20/容量外推数据有效'], 1)
        self.assertEqual(values['speed20/完成目录_项每小时'], 100)
        self.assertEqual(values['estimate20/仅已发现目录外推_剩余小时'], .26)
        self.assertEqual(values['estimate20/已发现目录外推数据有效'], 1)
        self.assertAlmostEqual(values['monitor/复核心跳距今_分钟'], 5 / 60)
        self.assertAlmostEqual(values['monitor/补传心跳距今_分钟'], 12 / 60)
        self.assertAlmostEqual(values['monitor/二十分钟采样距今_分钟'], 1 / 3)

    def test_stale_or_exhausted_estimates_never_claim_completion(self):
        scan = {'state': 'running', 'updated': 220,
                'counts': {'files': 100, 'file_bytes': 3_000_000_000_000}}
        monitor = {'updated': 200, 'audit_state': 'running',
                   'rates': {'audit_files_links_per_hour': 10, 'audit_logical_TB_per_hour': 0},
                   'estimates': {'files_baseline': 100, 'logical_TB_baseline': 6}}
        values = audit.monitor_values(scan, monitor, 220)
        self.assertEqual(values['speed20/复核逻辑容量_TB每小时'], 0)
        self.assertNotIn('estimate20/按首轮累计项外推_剩余小时', values)
        self.assertNotIn('estimate20/按首轮累计TB外推_剩余小时', values)
        self.assertEqual(values['estimate20/文件外推数据有效'], 0)
        self.assertEqual(values['estimate20/容量外推数据有效'], 0)
        stale = audit.monitor_values(scan, monitor, 3000)
        self.assertNotIn('speed20/复核逻辑容量_TB每小时', stale)
        self.assertEqual(stale['estimate20/文件外推数据有效'], 0)
        self.assertEqual(stale['estimate20/已发现目录外推数据有效'], 0)
        missing = audit.monitor_values(scan, None, 220)
        self.assertNotIn('estimate20/按首轮累计项外推_剩余小时', missing)
        self.assertEqual(missing['estimate20/文件外推数据有效'], 0)

    def test_stale_or_finished_scan_never_keeps_a_published_eta_valid(self):
        monitor = {'updated': 1000, 'audit_state': 'running',
                   'rates': {'audit_files_links_per_hour': 10, 'audit_logical_TB_per_hour': 2},
                   'estimates': {'files_baseline': 100, 'logical_TB_baseline': 5}}
        scan = {'state': 'running', 'updated': 600,
                'counts': {'files': 1, 'file_bytes': 1_000_000_000_000}}
        stale = audit.monitor_values(scan, monitor, 1000)
        self.assertEqual(stale['estimate20/文件外推数据有效'], 0)
        self.assertEqual(stale['estimate20/容量外推数据有效'], 0)
        self.assertNotIn('speed20/复核文件及链接_项每小时', stale)
        scan.update(state='finished', updated=1000)
        finished = audit.monitor_values(scan, monitor, 1000)
        self.assertEqual(finished['estimate20/文件外推数据有效'], 0)
        self.assertNotIn('estimate20/按首轮累计项外推_剩余小时', finished)

    def test_directory_projection_never_claims_completion_or_divides_by_zero(self):
        scan = {'state': 'running', 'updated': 1000, 'pending_dirs': 0,
                'active_dirs': 0, 'retry_dirs': 0, 'counts': {}}
        monitor = {'updated': 1000, 'audit_state': 'running',
                   'rates': {'audit_directories_per_hour': 10}, 'estimates': {}}
        values = audit.monitor_values(scan, monitor, 1000)
        self.assertEqual(values['estimate20/已发现目录外推数据有效'], 0)
        self.assertNotIn('estimate20/仅已发现目录外推_剩余小时', values)
        scan['pending_dirs'] = 10
        monitor['rates']['audit_directories_per_hour'] = 0
        values = audit.monitor_values(scan, monitor, 1000)
        self.assertEqual(values['speed20/完成目录_项每小时'], 0)
        self.assertEqual(values['estimate20/已发现目录外推数据有效'], 0)
        self.assertNotIn('estimate20/仅已发现目录外推_剩余小时', values)

    def test_cadence_wait_can_make_an_otherwise_fresh_eta_invalid(self):
        scan = {'state': 'running', 'updated': 1000, 'started': 800,
                'counts': {'files': 10, 'file_bytes': 1_000_000_000_000}}
        monitor = {'updated': 1000, 'audit_state': 'running', 'sample': {'audit_updated': 1000},
                   'rates': {'audit_files_links_per_hour': 100, 'audit_logical_TB_per_hour': 1,
                             'audit_updated_seconds': 120},
                   'estimates': {'files_baseline': 100, 'logical_TB_baseline': 5}}
        self.assertEqual(audit.monitor_values(scan, monitor, 1299)['estimate20/文件外推数据有效'], 1)
        after_wait = audit.monitor_values(scan, monitor, 1301)
        self.assertEqual(after_wait['estimate20/文件外推数据有效'], 0)
        self.assertNotIn('estimate20/按首轮累计项外推_剩余小时', after_wait)

    def test_handover_keeps_historical_rates_but_invalidates_old_eta(self):
        scan = {'state':'running', 'updated':1000, 'started':950, 'pending_dirs':100,
                'counts':{'files':10, 'file_bytes':1_000_000_000_000}}
        monitor = {'updated':1000, 'audit_state':'running', 'sample':{'audit_updated':1000},
                   'rates':{'audit_updated_seconds':120, 'audit_files_links_per_hour':100,
                            'audit_logical_TB_per_hour':1, 'audit_directories_per_hour':50},
                   'estimates':{'files_baseline':100, 'logical_TB_baseline':5}}
        values = audit.monitor_values(scan, monitor, 1020)
        self.assertEqual(values['speed20/复核文件及链接_项每小时'], 100)
        self.assertEqual(values['speed20/复核逻辑容量_TB每小时'], 1)
        self.assertEqual(values['estimate20/文件外推数据有效'], 0)
        self.assertEqual(values['estimate20/容量外推数据有效'], 0)
        self.assertEqual(values['estimate20/已发现目录外推数据有效'], 0)
        self.assertNotIn('estimate20/仅已发现目录外推_剩余小时', values)

    def test_nfs_chart_uses_fresh_verified_controller_mount_sample(self):
        scan = {'state': 'running', 'updated': 1000, 'started': 800, 'controller_host': 'host-a',
                'controller_pid': 123, 'counts': {}}
        monitor = {'audit_state': 'running', 'updated': 1000, 'sample': {'audit_updated': 1000},
                   'rates': {'audit_updated_seconds': 120}, 'estimates': {},
                   'probes': [{'node': 'n1', 'host': 'host-a', 'identity_ok': True,
                               'roles': {'audit': {'controllers': [{'pid': 123}]}}}],
                   'nfs_rates': {'n1': {'target': {'WRITE': {'requests': 50,
                        'mean_rtt_ms': 5600, 'requests_per_second': 2.5}}}}}
        values = audit.monitor_values(scan, monitor, 1020)
        self.assertEqual(values['nfs20/主控目标挂载WRITE平均RTT_秒'], 5.6)
        self.assertEqual(values['nfs20/主控目标挂载WRITE_次每秒'], 2.5)
        self.assertEqual(values['nfs20/主控目标挂载数据有效'], 1)
        scan['controller_pid'] = 124
        self.assertEqual(audit.monitor_values(scan, monitor, 1020)['nfs20/主控目标挂载数据有效'], 0)
        scan['controller_pid'] = 123
        scan['started'] = 900
        self.assertEqual(audit.monitor_values(scan, monitor, 1020)['nfs20/主控目标挂载数据有效'], 0)

    def test_nfs_chart_rejects_missing_requests_and_stale_samples(self):
        scan = {'state': 'running', 'updated': 1000, 'started': 800, 'controller_host': 'host-a',
                'controller_pid': 123, 'counts': {}}
        monitor = {'audit_state': 'running', 'updated': 1000, 'sample': {'audit_updated': 1000},
                   'rates': {'audit_updated_seconds': 120}, 'estimates': {},
                   'probes': [{'node': 'n1', 'host': 'host-a', 'identity_ok': True,
                               'roles': {'audit': {'controllers': [{'pid': 123}]}}}],
                   'nfs_rates': {'n1': {'target': {'WRITE': {'requests': 0,
                        'mean_rtt_ms': 5600, 'requests_per_second': 0}}}}}
        values = audit.monitor_values(scan, monitor, 1020)
        self.assertEqual(values['nfs20/主控目标挂载数据有效'], 0)
        self.assertNotIn('nfs20/主控目标挂载WRITE平均RTT_秒', values)
        monitor['nfs_rates']['n1']['target']['WRITE']['requests'] = 50
        stale = audit.monitor_values(scan, monitor, 1400)
        self.assertEqual(stale['nfs20/主控目标挂载数据有效'], 0)
        self.assertNotIn('nfs20/主控目标挂载WRITE平均RTT_秒', stale)

    def test_controller_reassigns_disconnected_worker_and_exports_results(self):
        run = self.root / 'run'
        work = self.root / 'work'
        code = work / 'code'
        (run / 'recovery/code').mkdir(parents=True)
        (work / 'logs').mkdir(parents=True)
        code.mkdir()
        (self.source / 'nested').mkdir()
        (self.source / 'nested/missing').write_text('data')
        (self.target / 'nested').mkdir()
        script = ROOT / 'ops/diagnostics/projset_full_audit.py'
        deployed = code / script.name
        deployed.write_bytes(script.read_bytes())
        (work / 'manifest.json').write_text(json.dumps({'code_sha256': hashlib.sha256(deployed.read_bytes()).hexdigest()}))
        wrapper = self.root / 'fake_worker.py'
        wrapper.write_text(textwrap.dedent('''
            import importlib.util, json, os, pathlib, sys
            args = dict(zip(sys.argv[1::2], sys.argv[2::2]))
            work = pathlib.Path(args['--work'])
            marker = work / 'worker-disconnected'
            if args['--node'] == 'one' and not marker.exists():
                marker.write_text('yes')
                print(json.dumps({'event': 'ready', 'host': 'fake', 'pid': os.getpid()}), flush=True)
                for line in sys.stdin:
                    if json.loads(line)['event'] == 'task':
                        os._exit(7)
            spec = importlib.util.spec_from_file_location('auditor', work / 'code/projset_full_audit.py')
            audit = importlib.util.module_from_spec(spec); spec.loader.exec_module(audit)
            audit.SOURCE = os.environ['TEST_SOURCE']; audit.TARGET = os.environ['TEST_TARGET']
            audit.worker(pathlib.Path(args['--run']), work, args['--node'])
        '''))
        fake = run / 'recovery/code/projset_recovery_copy.py'
        fake.write_text(textwrap.dedent('''
            import shlex, sys
            sys.path.insert(0, %r)
            import migration_linux_open as native
            def validate(run, cfg, node): pass
            def ssh(node, command):
                return [sys.executable, '-S', '-B', %r] + shlex.split(command)[5:]
        ''' % (str(ROOT / 'src/core'), str(wrapper))))
        (run / 'config.json').write_text('{}')
        (run / 'recovery/ssh.json').write_text(json.dumps({'hosts': {'one': 'fake1', 'two': 'fake2'}, 'reporter_python': '/bin/true'}))
        starter = self.root / 'starter.py'
        starter.write_text(textwrap.dedent('''
            import importlib.util, os, pathlib
            spec = importlib.util.spec_from_file_location('auditor', %r)
            audit = importlib.util.module_from_spec(spec); spec.loader.exec_module(audit)
            audit.SOURCE = os.environ['TEST_SOURCE']; audit.TARGET = os.environ['TEST_TARGET']
            audit.controller(pathlib.Path(%r), pathlib.Path(%r))
        ''' % (str(deployed), str(run), str(work))))
        env = dict(os.environ, TEST_SOURCE=str(self.source), TEST_TARGET=str(self.target))
        subprocess.run([sys.executable, '-S', '-B', str(starter)], env=env, check=True, timeout=30,
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        status = json.loads((work / 'status.json').read_text())
        self.assertTrue((work / 'worker-disconnected').exists())
        self.assertEqual(status['state'], 'finished')
        self.assertEqual(status['done_dirs'], status['discovered_dirs'])
        self.assertEqual(status['counts']['missing_target'], 1)
        self.assertIn('nested/missing', (work / 'issues.csv').read_text())
        self.assertFalse((self.target / 'nested/missing').exists())

    def test_transaction_failure_rolls_back_completion_and_resume_keeps_outbox(self):
        run, work = self.root / 'transaction-run', self.root / 'transaction-work'
        (run / 'recovery/code').mkdir(parents=True)
        (work / 'code').mkdir(parents=True)
        (work / 'logs').mkdir()
        (self.source / 'nested').mkdir()
        (self.source / 'nested/missing').write_text('payload')
        for name in ('second', 'third', 'fourth', 'fifth'):
            (self.source / name).mkdir()
        script = ROOT / 'ops/diagnostics/projset_full_audit.py'
        deployed = work / 'code/projset_full_audit.py'
        deployed.write_bytes(script.read_bytes())
        (work / 'manifest.json').write_text(json.dumps({'reporter': False,
            'code_sha256': hashlib.sha256(deployed.read_bytes()).hexdigest()}))
        db = audit.database(work / 'audit.sqlite3')
        db.executescript('''
            create table repair_outbox(id integer primary key, rel text, data text);
            create trigger save_missing after insert on issues when new.issue='missing_target'
            begin insert into repair_outbox(rel,data) values(new.rel,new.data); end;
            create trigger fail_child_assignment before update on dirs
            when new.id <> 1 and new.state=1
            begin select raise(abort,'synthetic lease failure'); end;
        ''')
        db.close()
        wrapper = self.root / 'transaction_worker.py'
        wrapper.write_text(textwrap.dedent('''
            import importlib.util, os, pathlib, sys
            args = dict(zip(sys.argv[1::2], sys.argv[2::2]))
            work = pathlib.Path(args['--work'])
            spec = importlib.util.spec_from_file_location('auditor', work / 'code/projset_full_audit.py')
            module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
            module.SOURCE = os.environ['TEST_SOURCE']; module.TARGET = os.environ['TEST_TARGET']
            scan = module.scan_directory
            def small_batches(*args, **kwargs): return scan(*args, batch_size=3, **kwargs)
            module.scan_directory = small_batches
            module.worker(pathlib.Path(args['--run']), work, args['--node'])
        '''))
        (run / 'recovery/code/projset_recovery_copy.py').write_text(textwrap.dedent('''
            import shlex, sys
            sys.path.insert(0, %r)
            import migration_linux_open as native
            def validate(run, cfg, node): pass
            def ssh(node, command): return [sys.executable, '-S', '-B', %r] + shlex.split(command)[5:]
        ''' % (str(ROOT / 'src/core'), str(wrapper))))
        (run / 'config.json').write_text('{}')
        (run / 'recovery/ssh.json').write_text(json.dumps({'hosts': {'one': 'fake'}, 'reporter_python': '/bin/true'}))
        starter = self.root / 'transaction_controller.py'
        starter.write_text(textwrap.dedent('''
            import importlib.util, os, pathlib
            spec = importlib.util.spec_from_file_location('auditor', %r)
            module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
            module.SOURCE = os.environ['TEST_SOURCE']; module.TARGET = os.environ['TEST_TARGET']
            module.THREADS = 1
            module.controller(pathlib.Path(%r), pathlib.Path(%r))
        ''' % (str(deployed), str(run), str(work))))
        env = dict(os.environ, TEST_SOURCE=str(self.source), TEST_TARGET=str(self.target))
        command = [sys.executable, '-S', '-B', str(starter)]
        failed = subprocess.run(command, env=env, timeout=30, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.assertNotEqual(failed.returncode, 0)
        self.assertIn(b'synthetic lease failure', failed.stderr)
        with sqlite3.connect(work / 'audit.sqlite3') as check:
            stats = json.loads(check.execute('select value from meta where key="stats"').fetchone()[0])
            self.assertEqual(stats['done_dirs'], 0)
            self.assertEqual(check.execute('select state from dirs where id=1').fetchone()[0], 1)
            # Full chunks were ACKed and persisted; the final chunk and
            # completion rolled back together with the failed assignment.
            self.assertEqual(check.execute('select count(*) from dirs').fetchone()[0], 5)
            self.assertGreaterEqual(check.execute('select count(*) from repair_outbox').fetchone()[0], 4)
            check.execute('drop trigger fail_child_assignment')
        subprocess.run(command, env=env, check=True, timeout=30, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        status = json.loads((work / 'status.json').read_text())
        self.assertEqual(status['state'], 'finished')
        self.assertEqual(status['done_dirs'], 6)
        self.assertEqual(status['discovered_dirs'], 6)
        with sqlite3.connect(work / 'audit.sqlite3') as check:
            self.assertGreater(check.execute('select count(*) from repair_outbox').fetchone()[0], 0)
            self.assertEqual(check.execute('select count(*) from dirs where state=2').fetchone()[0], 6)


if __name__ == '__main__':
    unittest.main()
