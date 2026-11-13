#!/bin/sh
# Keep one historical reporter active and another waiting on reporter.lock.
work_path="$1"
python_path="$2"
while [ ! -f "$work_path/historical-reporter/STOP" ]; do
  "$python_path" -B "$work_path/historical-reporter/code/projset_swanlab_historical.py" --work "$work_path"
  # Keep retrying until the combined run has actually published its final
  # state; otherwise a reporter failure at task completion is never retried.
  python3 -S -B -c 'import json,sys; from pathlib import Path; w=Path(sys.argv[1]); a=json.loads((w/"status.json").read_text()); r=json.loads((w/"repair/status.json").read_text()); s=json.loads((w/"swanlab.json").read_text()); sys.exit(0 if a.get("state")==r.get("state")==s.get("state")=="finished" else 1)' "$work_path" && exit 0
  sleep 15
done
