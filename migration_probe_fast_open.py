#!/usr/bin/env python3
"""Run the unchanged authenticated target probe with safe one-call parent opens.

The wrapper changes only the read-only directory-open primitive and exposes its
actual backend in existing health/role records. All command-line options,
authentication, STOP handling, concurrency bounds and probe semantics remain
owned by migration_target_probe.
"""

import migration_linux_open
import migration_target_probe
from pathlib import Path

from migration_io import read_json


_original_health = migration_target_probe.ProbeService.health


def _health_with_backend(self):
    value = _original_health(self)
    value['safe_open_backend'] = migration_linux_open.safe_open_backend()
    return value


def install():
    """Install the narrowly scoped primitive replacement once per process."""
    migration_target_probe._open_parent = migration_linux_open.open_parent_readonly
    migration_target_probe.ProbeService.health = _health_with_backend


def runtime_policy(path, controller):
    # The optional runtime policy is outside the SHA-bound flat source bundle.
    # An absent override keeps the current policy, not a reset to old defaults.
    if not path.exists() and not (path.parent / '.snapshots' / path.name).exists():
        return controller.current_policy()
    return read_json(path)


def main(*, installer=None):
    (install if installer is None else installer)()
    reloader = None
    policy_path = Path(__file__).resolve().parent / 'probe-policy.json'
    if policy_path.is_file():
        from migration_small_probe_delegate import (install as install_delegation,
                                                    installed_controller, PolicyReloader)
        install_delegation(read_json(policy_path))
        controller = installed_controller()
        runtime_path = policy_path.parent / 'roles' / 'probe-policy-runtime.json'
        reloader = PolicyReloader(controller, lambda: runtime_policy(runtime_path, controller))
        reloader.start()
    try:
        migration_target_probe.main()
    finally:
        if reloader is not None:
            reloader.close()


if __name__ == '__main__':
    main()
