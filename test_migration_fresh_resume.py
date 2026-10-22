"""Local-only fixture for bootstrap, eight stages, seed telemetry and STOP."""
import hashlib
import json
import os
import pathlib
import shutil
import sqlite3
import tempfile
import uuid

import migration_fresh_resume as fresh
import migration_pipeline as first
from migration_io import read_json, write_json
from migration_pipeline import ORDER
from migration_run import process_entry, now
from migration_scan import _stat_fields


def main():
    fresh.validate_mounts = lambda _cfg: None
    first.validate_mounts = lambda _cfg: None
    root = pathlib.Path(tempfile.mkdtemp(prefix='fresh-resume-eight-stage-')).resolve()
    src, dst, old0, old1, control = (root / name for name in ('source', 'target', 'old0', 'old1', 'new'))
    for path in (src, dst, old0, old1, control):
        path.mkdir()
    bodies = {'home/hcr/old.txt': b'old verified payload',
              'repos/cosmos-framework-pwm/repo.txt': b'repo payload', 'other/new.txt': b'new payload'}
    for rel, body in bodies.items():
        path = src / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
    canonical = 'home/hcr/old.txt'
    os.link(src / canonical, src / 'other' / 'alias.txt')
    bodies['other/alias.txt'] = bodies[canonical]
    (dst / canonical).parent.mkdir(parents=True)
    shutil.copy2(src / canonical, dst / canonical)
    authorized = {'source': '/share/project/eai_pwm', 'destination': '/share-mixed/project/eai_pwm'}
    write_json(old0 / 'config.json', dict(authorized, run_id='fixture-old0'))
    write_json(old1 / 'config.json', dict(authorized, run_id='fixture-old1', resume_from=str(old0)))
    (old1 / 'STOP').write_text('fixture predecessor stopped\n')
    proof = {'rel': canonical, 'kind': 'file', 'bytes': len(bodies[canonical]),
             'source_identity': _stat_fields((src / canonical).stat()),
             'dst_stat': _stat_fields((dst / canonical).stat()),
             'sha256': hashlib.sha256(bodies[canonical]).hexdigest()}
    for previous, status in ((old0, 'copied'), (old1, 'verified')):
        write_json(previous / 'stages' / 'p1-r1' / 'done' / '0' / '0.json',
                   {'id': 0, 'finished': now(), 'results': [dict(proof, status=status)]})
    plan = {'id': 'fixed-p4-plan', 'priority': 4, 'source': '/share/project',
            'destination': '/share-mixed/project', 'exclude': ['eai_pwm'], 'status': 'queued',
            'depends_on': {'control': str(old1)}, 'successor': {'run_id': 'fixed-p4-run'}}
    write_json(old1 / 'extension-plan.json', plan)
    old_plan_hash = hashlib.sha256((old1 / 'extension-plan.json').read_bytes()).hexdigest()

    def config(path):
        value = dict(authorized, run_id='fresh-fixture-' + uuid.uuid4().hex, resume_from=str(old1),
                     threads=64, copy_threads_per_node=128, parallel_scan_threads=4, scan_batch_files=2,
                     predecessor_isolation={'confirmed': True, 'old_control': str(old1),
                                            'method': 'fixture_all_predecessor_writers_stopped'})
        write_json(path / 'config.json', value)
        return value

    class Fixture(fresh.FreshResumePipeline):
        def drain(self, stage):
            for path in sorted((self.control / 'stages' / stage / 'pending').glob('*/*.json')):
                task = read_json(path)
                results = [process_entry(self.cfg, task, entry) for entry in task['entries']]
                write_json(self.control / 'stages' / stage / 'done' / str(task['id'] % 64) / path.name,
                           {'id': task['id'], 'finished': now(), 'results': results})
                path.unlink()

    cfg = config(control)
    coordinator = Fixture(control)
    assert coordinator.state['resume_index']['verified_files'] == 1
    assert coordinator.state['resume_index']['owned_files'] == 1
    assert coordinator.db.execute('SELECT owned FROM files WHERE rel=?', (canonical,)).fetchone() == (1,)
    assert read_json(control / 'seed-progress.json')['status'] == 'complete'
    assert read_json(control / 'resume-index.json')['receipts'] == 2
    # Bootstrap checks the real authorized mapping. Only this local fixture
    # substitutes payload paths afterwards; no real mount is accessed.
    coordinator.cfg.update(source=str(src), destination=str(dst))
    write_json(control / 'config.json', coordinator.cfg)
    coordinator.run()
    assert coordinator.state['status'] == 'complete', coordinator.state
    assert set(coordinator.pipeline['stages']) == {'p%d-r%d' % pair for pair in ORDER}
    assert coordinator.pipeline['stages']['p1-r1']['summary'].get('verified') == 1
    assert all((dst / rel).read_bytes() == body and (src / rel).read_bytes() == body
               for rel, body in bodies.items())
    assert (dst / canonical).stat().st_ino == (dst / 'other/alias.txt').stat().st_ino
    assert all(record.get('finalize', {}).get('errors', 0) == 0
               for record in coordinator.pipeline['stages'].values())
    assert set(coordinator.pipeline['directory_jobs']) == {'p1-r3', 'p2-r3', 'p3-r2'}
    carried = read_json(control / 'extension-plan.json')
    assert carried['id'] == plan['id'] and carried['successor'] == plan['successor']
    assert carried['depends_on']['control'] == str(control)
    assert carried['covered_by']['controls'] == [str(old0), str(old1), str(control)]
    assert hashlib.sha256((old1 / 'extension-plan.json').read_bytes()).hexdigest() == old_plan_hash
    coordinator._lock.close()
    write_json(control / 'config.json', cfg)
    try:
        fresh.FreshResumePipeline(control)
    except (FileExistsError, RuntimeError):
        pass
    else:
        raise AssertionError('reentry was accepted')

    stopped = root / 'stopped-bootstrap'
    stopped.mkdir()
    stopped_cfg = config(stopped)
    builder = fresh.build_resume_index
    def interrupted_builder(previous, index, progress_callback=None):
        def interrupt(progress):
            (stopped / 'STOP').write_text('fixture cooperative stop\n')
            progress_callback(progress)
        return builder(previous, index, progress_callback=interrupt)
    fresh.build_resume_index = interrupted_builder
    try:
        try:
            fresh.FreshResumePipeline(stopped)
        except RuntimeError:
            pass
        else:
            raise AssertionError('STOP did not stop bootstrap')
        assert read_json(stopped / 'state.json')['status'] == 'stopped'
        stopped_index = pathlib.Path('/tmp') / (stopped_cfg['run_id'] + '-index.sqlite')
        assert not stopped_index.exists()
        assert not list(stopped_index.parent.glob('.' + stopped_index.name + '.*.tmp'))
    finally:
        fresh.build_resume_index = builder
    for value in (cfg, stopped_cfg):
        for suffix in ('', '-wal', '-shm'):
            pathlib.Path('/tmp/' + value['run_id'] + '-index.sqlite' + suffix).unlink(missing_ok=True)
        pathlib.Path('/tmp/' + value['run_id'] + '-coordinator.lock').unlink(missing_ok=True)
    print(json.dumps({'PASS': 'eight-stage ancestor resume, telemetry, cross-phase hardlinks, metadata, '
                             'priority4 carry, reentry refusal, STOP temporary-index cleanup',
                      'stages': list(coordinator.pipeline['stages']), 'status': coordinator.state['status']}))


if __name__ == '__main__':
    main()
