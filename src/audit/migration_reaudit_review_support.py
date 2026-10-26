"""Run-local adapter for the immutable metadata review template, not old state."""
from pathlib import Path

from migration_reaudit import atomic

ROOT = Path(__file__).resolve().parents[2]


def paused(record):
    # The old template's CLI is intentionally not an entry for this new run.
    # The new entry validates its plan then calls run_review directly.
    raise RuntimeError('historical review CLI is not authorized for the new audit')
