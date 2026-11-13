"""Generation fencing and pinned connectivity for cluster handover."""
import importlib.util
import json
import os
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT/'src/core'))


def load(name):
    spec = importlib.util.spec_from_file_location(name, ROOT/'ops/copy'/(name+'.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


copy = load('projset_recovery_copy')
supervisor = load('projset_recovery_supervisor')


@pytest.mark.parametrize('module', [copy, supervisor])
def test_generation_fence_is_fail_closed_and_supports_stop(tmp_path, monkeypatch, module):
    (tmp_path/'recovery').mkdir()
    marker = tmp_path/'STOP'
    marker.write_text('legacy-cluster-fence generation-a\n')
    st = marker.stat()
    data = {'generation': 'generation-a', 'marker_ino': st.st_ino,
            'marker_size': st.st_size, 'marker_mtime_ns': st.st_mtime_ns,
            'marker_content': marker.read_text()}
    fence = tmp_path/'recovery'/'legacy-cluster-fence.json'
    fence.write_text(json.dumps(data))
    monkeypatch.delenv('PROJSET_RECOVERY_GENERATION', raising=False)
    assert module.stop_requested(tmp_path)  # Old process is fenced.
    monkeypatch.setenv('PROJSET_RECOVERY_GENERATION', 'generation-a')
    assert not module.stop_requested(tmp_path)
    (tmp_path/'STOP.requested').touch()
    assert module.stop_requested(tmp_path)
    (tmp_path/'STOP.requested').unlink()
    marker.write_text('altered')
    assert module.stop_requested(tmp_path)
    marker.unlink()
    assert module.stop_requested(tmp_path)
    fence.unlink()
    assert module.stop_requested(tmp_path)


def test_pinned_address_preserves_host_key_verification(tmp_path, monkeypatch):
    cfg = {'hosts': {'node': 'node.example'}, 'addresses': {'node': '192.0.2.1'},
           'known_hosts': '/safe/known_hosts', 'key': '/safe/key'}
    path = tmp_path/'ssh.json'
    path.write_text(json.dumps(cfg))
    monkeypatch.setenv('PROJSET_SSH_TOPOLOGY', str(path))
    for args in (copy.ssh('node', 'true'), supervisor.ssh(cfg, 'node', 'true')):
        assert 'HostKeyAlias=node.example' in args
        assert 'StrictHostKeyChecking=yes' in args
        assert args[-2:] == ['192.0.2.1', 'true']


def test_detach_propagates_generation(monkeypatch, tmp_path):
    monkeypatch.setenv('PROJSET_RECOVERY_GENERATION', 'generation-a')
    generated = supervisor.detach(tmp_path/'supervisor.py', tmp_path, 'standby')
    assert 'PROJSET_RECOVERY_GENERATION' in generated
    assert 'generation-a' in generated
    compile(generated, '<detach>', 'exec')
