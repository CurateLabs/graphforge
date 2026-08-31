#!/usr/local/bin/python3
"""Fail-closed bootstrap for the progressive provider qualification runner."""

from __future__ import annotations

import ctypes
import os
import stat
import sys
from typing import NoReturn

WORK_ROOT = "/work"
RUN_UID = 10001
RUN_GID = 10001
PYTHON = "/opt/graphforge/benchmarks/.venv/bin/python"
MODULE = "graphforge_bench.progressive_provider_run"
PR_SET_NO_NEW_PRIVS = 38

# Do not forward the container's ambient environment. In particular, provider
# credentials and control-plane configuration must never reach the offline runner.
EXEC_ENV = {
    "HOME": "/work",
    "LANG": "C.UTF-8",
    "PATH": "/opt/graphforge/benchmarks/.venv/bin:/usr/local/bin:/usr/bin:/bin",
    "PYTHONDONTWRITEBYTECODE": "1",
    "PYTHONPATH": "/opt/graphforge/benchmarks/harness",
    "PYTHONUNBUFFERED": "1",
}


def refuse(message: str) -> NoReturn:
    print(f"qualification bootstrap refused: {message}", file=sys.stderr)
    raise SystemExit(64)


def validate_work_root() -> None:
    try:
        metadata = os.lstat(WORK_ROOT)
    except OSError as error:
        refuse(f"work root is unavailable: {error}")
    if not stat.S_ISDIR(metadata.st_mode):
        refuse("work root is not a directory")
    if os.path.realpath(WORK_ROOT) != WORK_ROOT:
        refuse("work root does not resolve to the exact mount path")
    if not os.path.ismount(WORK_ROOT):
        refuse("work root is not a mount point")


def enable_no_new_privileges() -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    libc.prctl.argtypes = [
        ctypes.c_int,
        ctypes.c_ulong,
        ctypes.c_ulong,
        ctypes.c_ulong,
        ctypes.c_ulong,
    ]
    libc.prctl.restype = ctypes.c_int
    if libc.prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0:
        error_number = ctypes.get_errno()
        refuse(f"PR_SET_NO_NEW_PRIVS failed: {os.strerror(error_number)}")


def main() -> None:
    if os.geteuid() != 0:
        refuse("bootstrap must start as root")
    validate_work_root()

    # Ownership changes only for the mount root itself, never its contents.
    try:
        os.chown(WORK_ROOT, RUN_UID, RUN_GID, follow_symlinks=False)
        enable_no_new_privileges()
        os.setgroups([])
        os.setgid(RUN_GID)
        os.setuid(RUN_UID)
        os.chdir(WORK_ROOT)
        os.execve(PYTHON, [PYTHON, "-P", "-m", MODULE, *sys.argv[1:]], EXEC_ENV)
    except OSError as error:
        refuse(str(error))


if __name__ == "__main__":
    main()
