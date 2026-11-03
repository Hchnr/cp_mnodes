# Add multi-node projset copying and SwanLab overview; organize analysis archives

The `projset → project` migration needs an independent copier, progress reporting, and per-path exception records. Analysis scripts, historical reports, and large manifests also need a consistent home. This change adds those tools and organizes sources, tests, static evidence, and runtime artifacts.

## Changes

- Add bounded distributed scanning and copying with relative-path preservation, SHA-256 verification, destination readback, and no-replace publication. Preserve conflicting existing targets and record the conflict.
- Add a separate SwanLab reporter with one batch every 120 seconds, TB capacity metrics, and TB/s rates. An overview shows confirmed capacity, completed TB/h, live nodes, and exceptions.
- Add process/cgroup/OOM diagnostics and incremental outcome exports every 120 seconds, including reused paths, conflicts, source changes, errors, sizes, reasons, current attributes, and receipt locations.
- Move analysis tools, tests, and historical PR descriptions into dedicated directories; organize storage reports and JSON/CSV evidence and update navigation and chart documentation.
- Losslessly compress approximately 287 MB of static manifests to 29 MB, recording hashes before and after compression. Analysis reads compressed CSV files directly.
- Exclude live `runs/`, logs, caches, and personal editor preferences. Make the new tests work from both source directories and flat bundles at arbitrary locations.

## Validation

- Syntax checks passed for all 409 bundled Python files, with no duplicate bundle filenames.
- All 18 new functional tests passed in both source and flat-bundle layouts; nine repository-tool tests also passed, covering 27 distinct tests.
- All three gzip archives were read back and matched their original SHA-256 hashes. New relative documentation links and `git diff --check` passed.

## Operational boundaries

Existing jobs keep their original paths and frozen code. The overview omits percentage and ETA while the total scope is unknown. Refreshing reports does not retry exceptions or overwrite targets. Directory metadata and hardlink topology still require finalization; copying a live source does not produce a point-in-time snapshot.
