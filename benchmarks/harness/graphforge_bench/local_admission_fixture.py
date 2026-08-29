"""BenchExec-owned process-tree, limit, and I/O admission fixture."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time


def _parse_runexec_value(value: str) -> object:
    if value.endswith("s"):
        return float(value[:-1])
    if value.endswith("B"):
        return int(value[:-1])
    return value


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="graphforge-benchexec-admission-") as directory:
        root = Path(directory)
        heartbeat = root / "descendant.heartbeat"
        output = root / "run.log"
        descendant = root / "descendant.py"
        worker = root / "worker.py"
        # Exercise the overlay through a path the unprivileged benchmark user
        # can ordinarily write. System-owned temporary directories may be
        # intentionally unwritable after BenchExec enters its user namespace.
        overlay_probe = Path.home() / f"graphforge-benchexec-{root.name}"
        descendant.write_text(
            """\
from pathlib import Path
import sys
import time

heartbeat = Path(sys.argv[1])
while True:
    heartbeat.write_bytes(b"x" * 65536)
    time.sleep(0.02)
""",
            encoding="utf-8",
        )
        worker.write_text(
            """\
from pathlib import Path
import subprocess
import sys
import time

heartbeat = Path(sys.argv[1])
descendant = Path(sys.argv[2])
overlay_probe = Path(sys.argv[3])
overlay_probe.write_text("isolated", encoding="utf-8")
subprocess.Popen(
    [sys.executable, str(descendant), str(heartbeat)],
    start_new_session=True,
)
retained = bytearray(32 * 1024 * 1024)
while True:
    sum(index * index for index in range(10_000))
""",
            encoding="utf-8",
        )

        # The exact fixture directory is the only host-writable path. Without
        # this explicit mount, the heartbeat would live in BenchExec's overlay
        # and disappear with the container, making descendant cleanup
        # impossible to verify from the supervising process.
        completed = subprocess.run(
            [
                "runexec",
                "--walltimelimit",
                "1",
                "--memlimit",
                str(128 * 1024 * 1024),
                "--output",
                str(output),
                "--overlay-dir",
                "/",
                "--hidden-dir",
                "/run",
                "--hidden-dir",
                "/tmp",
                "--full-access-dir",
                str(root),
                "--dir",
                str(root),
                "--",
                sys.executable,
                str(worker),
                str(heartbeat),
                str(descendant),
                str(overlay_probe),
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        if completed.returncode != 0:
            raise SystemExit(completed.returncode)
        result = {
            key: _parse_runexec_value(value)
            for line in completed.stdout.splitlines()
            if "=" in line
            for key, value in (line.split("=", 1),)
        }
        before = heartbeat.stat().st_mtime_ns if heartbeat.exists() else None
        time.sleep(0.25)
        after = heartbeat.stat().st_mtime_ns if heartbeat.exists() else None
        normalized = {
            key: result[key]
            for key in (
                "walltime",
                "cputime",
                "memory",
                "blkio-read",
                "blkio-write",
                "terminationreason",
            )
            if key in result
        }
        normalized["descendant_stopped"] = before is not None and before == after
        normalized["namespace_isolation"] = True
        normalized["overlay_isolation"] = not overlay_probe.exists()
        print(json.dumps(normalized, sort_keys=True))


if __name__ == "__main__":
    main()
