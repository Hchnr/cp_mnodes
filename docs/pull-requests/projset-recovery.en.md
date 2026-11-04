# Add automatic recovery for distributed projset copying

When the development container or controller exits, copying can stop even though shared journals and receipts remain available. Add CPU-node supervision and queue reconstruction so unacknowledged work resumes while existing conflicting targets remain intact.

## Changes

- Add a fixed controller anchor on `cpu-master-0` and a standby on `cpu-worker-0`, with mutual supervisor restart, local exclusive locks, child-process monitoring, and bounded retries.
- Fence previous controllers and workers using host, boot ID, PID start time, and command identity before replaying leases; recovery waits when a worker cannot be fenced.
- Rebuild the disposable SQLite queue from durable task/completion journals and all receipt attempts. Preserve out-of-order completion, deduplicate discovered paths, and reuse valid receipts without double-counting.
- Extend outcome exports across recovery attempts using receipt path plus byte offset, preserving legacy checkpoints. Inspect the latest worker attempt for node health and OOM diagnostics.
- Supervise SwanLab and outcome reporting, retain the existing experiment/checkpoints and 120-second reporting cadence, and document recovery operations and manual startup.
- Remove two obsolete top-level PR descriptions whose contents are already archived under `docs/pull-requests/archive/`.

## Validation

- `python3 -S -B tools/repo.py test test_projset_copy test_projset_overview test_projset_usage test_projset_outcomes test_projset_recovery test_dataset_archives`: 22 tests passed.
- Source-layout checks: 3 recovery tests and 2 outcome-export tests passed, covering journal replay, changed-target rejection, process-identity fencing, and idempotent multi-attempt exports.
- All 412 bundled Python sources parsed successfully; bundle filenames are unique. `git diff --check` passed.
- Previously deployed runtime was verified by terminating the primary supervisor: standby restarted it, fenced the old controller, and resumed all 16 CPU workers. Evidence locations are documented in `docs/operations/projset-recovery.md`.

## Operational limits

Confirmed conflicts, changed sources, and file-level errors remain recorded for manual handling; recovery does not overwrite targets or start a second copy pass. If both supervisor containers disappear, one supervisor requires manual startup because no system boot service is installed. An unreachable anchor does not permit cross-host controller takeover. Live-source copying is not a point-in-time snapshot; directory metadata and hardlink topology still require finalization.
