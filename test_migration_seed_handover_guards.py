"""Local-only rejection tests; no mount, SSH, payload read or write."""
import os
import pathlib
import sqlite3
import tempfile
import uuid
import sys
import types
try:
    import migration_resume_index_parallel
except ModuleNotFoundError:
    import migration_resume_index
    shim = types.ModuleType('migration_resume_index_parallel')
    shim.build_resume_index = migration_resume_index.build_resume_index
    sys.modules['migration_resume_index_parallel'] = shim
import migration_seed_handover as module
from migration_io import read_json, write_json
from migration_run import now


def main():
    module.validate_mounts = lambda _cfg: None
    root = pathlib.Path(tempfile.mkdtemp(prefix='seed-guards-'))
    previous = root / 'old'
    previous.mkdir()
    (previous / 'STOP').write_text('isolated')
    write_json(previous / 'config.json', {'run_id': 'old', 'source': '/share/project/eai_pwm',
                                         'destination': '/share-mixed/project/eai_pwm'})
    visited = []
    def builder(*_args, **_kwargs):
        visited.append('builder')
        raise AssertionError('rejected seed handover reached index builder')
    module.build_resume_index = builder
    names = []
    for name in ('live_old', 'changed_state', 'claim', 'stage_file', 'manifest_file',
                 'stage_symlink', 'published_index', 'pipeline', 'bad_bootstrap', 'consumed'):
        control = root / name
        control.mkdir()
        run_id = 'seed-guard-' + uuid.uuid4().hex
        cfg = {'run_id': run_id, 'source': '/share/project/eai_pwm',
               'destination': '/share-mixed/project/eai_pwm', 'resume_from': str(previous),
               'predecessor_isolation': {'confirmed': True, 'old_control': str(previous), 'method': 'fixture'}}
        write_json(control / 'config.json', cfg)
        state = {'status': 'preparing_resume', 'stage': None, 'run_id': run_id}
        bootstrap = {'status': 'seeding', 'started': now(), 'run_id': run_id, 'resume_from': str(previous)}
        write_json(control / 'state.json', state)
        write_json(control / 'fresh-bootstrap.json', bootstrap)
        proof = {'version': 1, 'purpose': 'resume_index_seed_only', 'id': str(uuid.uuid4()),
                 'control': str(control), 'run_id': run_id,
                 'old_producer': {'pid': 99999999, 'starttime_ticks': '1', 'argv': ['fixture-dead']},
                 'state_snapshot': state, 'fresh_bootstrap_snapshot': bootstrap}
        handover = control / 'bundle' / 'seed-handover.json'
        write_json(handover, proof)
        broker_path = pathlib.Path('/tmp/' + run_id + '-broker.sqlite')
        db = sqlite3.connect(broker_path)
        db.execute('CREATE TABLE claims (id INTEGER)')
        if name == 'claim':
            db.execute('INSERT INTO claims VALUES (0)')
        db.commit()
        db.close()
        if name == 'changed_state':
            write_json(control / 'state.json', dict(state, updated='different'))
        elif name in ('stage_file', 'manifest_file'):
            part = control / ('stages' if name == 'stage_file' else 'manifests') / 'any'
            part.parent.mkdir()
            part.write_text('do not touch')
        elif name == 'stage_symlink':
            (control / 'stages').mkdir()
            (control / 'stages' / 'link').symlink_to(previous, target_is_directory=True)
        elif name == 'published_index':
            pathlib.Path('/tmp/' + run_id + '-index.sqlite').write_text('preserve')
        elif name == 'pipeline':
            write_json(control / 'pipeline.json', {'started': 'yes'})
        elif name == 'bad_bootstrap':
            write_json(control / 'fresh-bootstrap.json', dict(bootstrap, status='seeded'))
        elif name == 'consumed':
            write_json(control / 'seed-handovers' / (proof['id'] + '.json'), {'status': 'seeding'})
        original_matches = module.process_matches
        if name == 'live_old':
            module.process_matches = lambda identity: True
        try:
            try:
                module.SeedHandoverPipeline(control, handover)
            except (RuntimeError, FileExistsError):
                names.append(name)
            else:
                raise AssertionError('unsafe handover accepted: ' + name)
        finally:
            module.process_matches = original_matches
            broker_path.unlink()
            pathlib.Path('/tmp/' + run_id + '-index.sqlite').unlink(missing_ok=True)
            pathlib.Path('/tmp/' + run_id + '-coordinator.lock').unlink(missing_ok=True)
    assert not visited, visited
    print({'PASS': names})


if __name__ == '__main__':
    main()
