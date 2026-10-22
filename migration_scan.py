"""Read-only, streaming inventory for the eai_pwm migration.

``scan_phase`` writes a local JSONL manifest and calls ``emit_batch(entries)``
with regular files and symlinks. Directories, secondary hardlinks, unsupported
objects, and scan errors are manifest-only. No destination path is accessed.

Pass the same ``hardlink_index`` dictionary to all three phases to recognize
hardlinks across phase boundaries. JSON escaping preserves unusual filenames,
including filenames represented with Python's surrogateescape convention.
"""

from __future__ import annotations

import errno
import json
import os
import stat
import time
from typing import Callable, MutableMapping


PHASE_PATHS = {1: "home/hcr", 2: "repos/cosmos-framework-pwm", 3: ""}
PRIORITY_SUBTREES = frozenset((PHASE_PATHS[1], PHASE_PATHS[2]))


def _stat_fields(st: os.stat_result) -> dict:
    return {
        "size": st.st_size,
        "mtime_ns": st.st_mtime_ns,
        "ctime_ns": st.st_ctime_ns,
        "atime_ns": st.st_atime_ns,
        "ino": st.st_ino,
        "dev": st.st_dev,
        "mode": st.st_mode,
        "uid": st.st_uid,
        "gid": st.st_gid,
        "nlink": st.st_nlink,
        "blocks": getattr(st, "st_blocks", 0),
    }


def _open_directory(path: str, *, dir_fd: int | None = None) -> int:
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    flags |= getattr(os, "O_CLOEXEC", 0)
    noatime = getattr(os, "O_NOATIME", 0)
    try:
        return os.open(path, flags | noatime, dir_fd=dir_fd)
    except OSError as exc:
        if not noatime or exc.errno not in (errno.EPERM, errno.EACCES, errno.EINVAL):
            raise
        return os.open(path, flags, dir_fd=dir_fd)


class _Scanner:
    def __init__(self, manifest, emit_batch, batch_bytes, batch_files,
                 hardlink_index, on_error, phase, entries_per_second=0):
        self.manifest = manifest
        self.emit_batch = emit_batch
        self.batch_bytes = batch_bytes
        self.batch_files = batch_files
        self.hardlinks = hardlink_index
        self.on_error = on_error
        self.batch = []
        self.pending_bytes = 0
        self.entries_per_second = entries_per_second
        self.rate_started = time.monotonic()
        self.rate_entries = 0
        self.stats = {
            "phase": phase, "files": 0, "bytes": 0, "copy_bytes": 0,
            "allocated_bytes": 0, "symlinks": 0, "dirs": 0,
            "hardlinks": 0, "unsupported": 0, "errors": 0,
            "copy_entries": 0, "batches": 0,
        }

    def write(self, entry):
        self.rate_entries += 1
        if self.entries_per_second and self.rate_entries % 32 == 0:
            delay = self.rate_entries / self.entries_per_second - (time.monotonic() - self.rate_started)
            if delay > 0:
                time.sleep(delay)
        self.manifest.write(json.dumps(entry, ensure_ascii=True,
                                       separators=(",", ":")) + "\n")

    def error(self, rel, operation, exc):
        entry = {"rel": rel, "kind": "error", "operation": operation,
                 "error": str(exc), "errno": getattr(exc, "errno", None)}
        self.stats["errors"] += 1
        self.write(entry)
        if self.on_error is not None:
            self.on_error(entry)

    def directory(self, rel, st):
        self.stats["dirs"] += 1
        self.write({"rel": rel, "kind": "directory", **_stat_fields(st)})

    def flush_batch(self):
        if not self.batch:
            return
        batch = self.batch
        self.batch = []
        self.pending_bytes = 0
        # Inventory must be visible before workers receive a corresponding task.
        self.manifest.flush()
        self.emit_batch(batch)
        self.stats["batches"] += 1

    def enqueue(self, entry):
        size = entry["size"] if entry["kind"] == "file" else 0
        if self.batch and (len(self.batch) >= self.batch_files or
                           self.pending_bytes + size > self.batch_bytes):
            self.flush_batch()
        self.batch.append(entry)
        self.pending_bytes += size
        self.stats["copy_entries"] += 1
        if len(self.batch) >= self.batch_files or self.pending_bytes >= self.batch_bytes:
            self.flush_batch()

    def nondirectory(self, rel, name, parent_fd, st):
        fields = _stat_fields(st)
        if stat.S_ISREG(st.st_mode):
            entry = {"rel": rel, "kind": "file", **fields}
            self.stats["files"] += 1
            self.stats["bytes"] += st.st_size
            if st.st_nlink > 1:
                key = (st.st_dev, st.st_ino)
                canonical = self.hardlinks.setdefault(key, rel)
                if canonical != rel:
                    entry["hardlink_to"] = canonical
                    self.stats["hardlinks"] += 1
                    self.write(entry)
                    return
            self.stats["copy_bytes"] += st.st_size
            self.stats["allocated_bytes"] += fields["blocks"] * 512
        elif stat.S_ISLNK(st.st_mode):
            try:
                target = os.readlink(name, dir_fd=parent_fd)
                after = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
                if (after.st_dev, after.st_ino, after.st_ctime_ns) != (
                        st.st_dev, st.st_ino, st.st_ctime_ns):
                    raise OSError(errno.ESTALE, "symlink changed during inventory")
            except OSError as exc:
                self.error(rel, "readlink", exc)
                return
            entry = {"rel": rel, "kind": "symlink", "target": target,
                     "link_target": target, **fields}
            self.stats["symlinks"] += 1
        else:
            if stat.S_ISFIFO(st.st_mode):
                kind = "fifo"
            elif stat.S_ISSOCK(st.st_mode):
                kind = "socket"
            elif stat.S_ISCHR(st.st_mode):
                kind = "char"
            elif stat.S_ISBLK(st.st_mode):
                kind = "block"
            else:
                kind = "unknown"
            entry = {"rel": rel, "kind": kind, "unsupported": True, **fields}
            self.stats["unsupported"] += 1
            self.write(entry)
            if self.on_error is not None:
                self.on_error(entry)
            return
        self.write(entry)
        self.enqueue(entry)

    def walk(self, root_fd, root_rel, exclude):
        """Consume root_fd; traverse by descriptors so symlink races cannot escape."""
        frames = []

        def push(fd, rel):
            try:
                iterator = os.scandir(fd)
            except OSError as exc:
                os.close(fd)
                self.error(rel, "scandir", exc)
                return
            frames.append((fd, iterator, rel))

        push(root_fd, root_rel)
        try:
            while frames:
                fd, iterator, parent_rel = frames[-1]
                try:
                    child = next(iterator)
                except StopIteration:
                    iterator.close()
                    os.close(fd)
                    frames.pop()
                    continue
                except OSError as exc:
                    self.error(parent_rel, "scandir", exc)
                    iterator.close()
                    os.close(fd)
                    frames.pop()
                    continue
                rel = parent_rel + "/" + child.name if parent_rel else child.name
                if rel in exclude:
                    continue
                try:
                    st = child.stat(follow_symlinks=False)
                except OSError as exc:
                    self.error(rel, "stat", exc)
                    continue
                if not stat.S_ISDIR(st.st_mode):
                    self.nondirectory(rel, child.name, fd, st)
                    continue
                try:
                    child_fd = _open_directory(child.name, dir_fd=fd)
                    actual = os.fstat(child_fd)
                    if (actual.st_dev, actual.st_ino) != (st.st_dev, st.st_ino):
                        os.close(child_fd)
                        raise OSError(errno.ESTALE, "directory changed during inventory")
                except OSError as exc:
                    self.error(rel, "open_directory", exc)
                    continue
                self.directory(rel, actual)
                push(child_fd, rel)
        finally:
            for fd, iterator, _rel in reversed(frames):
                iterator.close()
                os.close(fd)


def scan_phase(
    source_root: str,
    phase: int,
    emit_batch: Callable[[list[dict]], None],
    manifest_path: str,
    *,
    batch_bytes: int = 2 * 1024 ** 3,
    batch_files: int = 512,
    hardlink_index: MutableMapping[tuple[int, int], str] | None = None,
    on_error: Callable[[dict], None] | None = None,
    overwrite_manifest: bool = False,
    entries_per_second: float = 0,
) -> dict:
    """Inventory one priority phase without writing source or destination data.

    ``phase`` is 1 (home/hcr), 2 (cosmos repo), or 3 (all remaining entries).
    Ancestor directory metadata is included in phases 1 and 2. ``bytes`` counts
    every regular path's apparent size; ``copy_bytes`` and ``allocated_bytes``
    count canonical regular files only. ``hardlinks`` counts secondary paths.
    Unsupported objects are recorded and counted but never sent to workers.
    Scan failures are error records and increment ``errors``; callback failures
    propagate. Existing manifests are refused unless explicitly allowed.
    """
    if phase not in PHASE_PATHS:
        raise ValueError("phase must be 1, 2, or 3")
    if batch_bytes < 1 or batch_files < 1:
        raise ValueError("batch limits must be positive")
    source_root = os.path.abspath(os.fspath(source_root))
    manifest_path = os.path.abspath(os.fspath(manifest_path))
    if os.path.commonpath((os.path.realpath(source_root),
                           os.path.realpath(manifest_path))) == os.path.realpath(source_root):
        raise ValueError("manifest must be outside source_root")
    os.makedirs(os.path.dirname(manifest_path), exist_ok=True)
    started = time.time()
    with open(manifest_path, "w" if overwrite_manifest else "x", encoding="utf-8") as manifest:
        scanner = _Scanner(manifest, emit_batch, batch_bytes, batch_files,
                           {} if hardlink_index is None else hardlink_index,
                           on_error, phase, entries_per_second)
        source_fd = None
        try:
            try:
                source_fd = _open_directory(source_root)
                scanner.directory("", os.fstat(source_fd))
            except OSError as exc:
                scanner.error("", "open_source_root", exc)
            if source_fd is not None:
                rel = ""
                for part in PHASE_PATHS[phase].split("/") if PHASE_PATHS[phase] else ():
                    rel = rel + "/" + part if rel else part
                    try:
                        child_fd = _open_directory(part, dir_fd=source_fd)
                    except OSError as exc:
                        scanner.error(rel, "open_phase_root", exc)
                        break
                    os.close(source_fd)
                    source_fd = child_fd
                    scanner.directory(rel, os.fstat(source_fd))
                else:
                    walk_fd = source_fd
                    source_fd = None
                    scanner.walk(walk_fd, rel, PRIORITY_SUBTREES if phase == 3 else ())
        finally:
            if source_fd is not None:
                os.close(source_fd)
        scanner.flush_batch()
        manifest.flush()
        os.fsync(manifest.fileno())
        return {
            **scanner.stats,
            "source_root": source_root,
            "manifest_path": manifest_path,
            "elapsed_seconds": round(time.time() - started, 3),
            "scan_complete": scanner.stats["errors"] == 0,
            "fully_copyable": scanner.stats["unsupported"] == 0,
        }
