#!/usr/bin/env python3
"""The existing read-only probe protocol using safe directory-only O_PATH FDs.

No payload is opened or written. stat/readlink retain their original
follow_symlinks=False checks; the parent walk still rejects every symlink.
The ordinary fast-open entry remains unchanged unless this entry is launched.
"""
import migration_linux_open as opening
import migration_probe_fast_open as fast
import migration_target_probe as probe


def health(self):
    return dict(fast._health_with_backend(self), parent_open_mode='O_PATH')


def install():
    fast.install()
    probe._open_parent = opening.open_parent_path
    probe.ProbeService.health = health


def main():
    fast.main(installer=install)


if __name__ == '__main__':
    main()
