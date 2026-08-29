"""BenchExec-owned process-tree, limit, and I/O admission fixture."""

from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import time


def _json_value(value: object) -> object:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    raw = getattr(value, "raw", None)
    if raw is not None:
        return raw
    return str(value)


def main() -> None:
    # Import only after the outer admission has proved a Linux host.
    from benchexec.containerexecutor import DIR_FULL_ACCESS, DIR_HIDDEN, DIR_OVERLAY
    from benchexec.runexecutor import RunExecutor

    with tempfile.TemporaryDirectory(prefix="graphforge-benchexec-admission-") as directory:
        root = Path(directory)
        heartbeat = root / "descendant.heartbeat"
        output = root / "run.log"
        descendant = root / "descendant.py"
        worker = root / "worker.py"
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
subprocess.Popen(
    [sys.executable, str(descendant), str(heartbeat)],
    start_new_session=True,
)
while True:
    bytearray(1024 * 1024)
    time.sleep(0.02)
""",
            encoding="utf-8",
        )

        # The exact fixture directory is the only host-writable path. Without
        # this explicit mount, the heartbeat would live in BenchExec's overlay
        # and disappear with the container, making descendant cleanup
        # impossible to verify from the supervising process.
        executor = RunExecutor(
            use_namespaces=True,
            dir_modes={
                "/": DIR_OVERLAY,
                "/run": DIR_HIDDEN,
                "/tmp": DIR_HIDDEN,
                str(root): DIR_FULL_ACCESS,
            },
        )
        result = executor.execute_run(
            [sys.executable, str(worker), str(heartbeat), str(descendant)],
            str(output),
            walltimelimit=1,
            memlimit=128 * 1024 * 1024,
            workingDir=str(root),
        )
        before = heartbeat.stat().st_mtime_ns if heartbeat.exists() else None
        time.sleep(0.25)
        after = heartbeat.stat().st_mtime_ns if heartbeat.exists() else None
        normalized = {key: _json_value(value) for key, value in result.items()}
        normalized["descendant_stopped"] = before is not None and before == after
        print(json.dumps(normalized, sort_keys=True))


if __name__ == "__main__":
    main()
