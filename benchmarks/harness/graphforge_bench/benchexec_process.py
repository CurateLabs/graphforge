"""Run the BenchExec CLI as a process group with a bounded wait (#1914).

BenchExec can finish a run set, write its final results and then never exit: a
process it left behind holds a pipe that BenchExec's own exit handler waits on
(see `benchexec_launcher`). A plain `subprocess.run` then blocks forever, and the
phase stays unfinished for as long as the host stays up.

`run_bounded` starts BenchExec in its own process group, waits a bounded time and
kills the group when BenchExec does not exit. It always kills what the run left
behind, so no orphan outlives its phase.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
import contextlib
import json
import os
from pathlib import Path
import signal
import subprocess
import threading
import time
import xml.etree.ElementTree as ET

# The exit status of a BenchExec run that was killed for not exiting, in the
# convention of timeout(1). BenchExec itself exits with 0 or 1.
HUNG_STATUS = 124
# How long BenchExec may take to exit once its final results are written. The
# real exit takes under a second; the bound only has to outlast slow teardown.
EXIT_GRACE_SECONDS = 120.0
# How far past its own wall limit BenchExec may run before the run counts as hung:
# start-up (tool-info container, cgroup set-up) and teardown sit outside that limit.
WALL_GRACE_SECONDS = 900.0
# One poll per tick. The tick is short because the observer reads cgroup counters that
# vanish with the run; the exit and wall checks run once per `CHECK_SECONDS`.
POLL_SECONDS = 0.05
CHECK_SECONDS = 1.0
# How long an interrupted run gets to stop on SIGTERM before the group is killed.
INTERRUPT_GRACE_SECONDS = 10.0
HUNG_EVIDENCE = "benchexec-hung.json"


def results_final(raw_output: Path) -> bool:
    """Whether BenchExec has written the closing record of its run set.

    BenchExec rewrites the result XML as runs finish and stamps `endtime` on the
    root only once the run set is complete.
    """
    for document in sorted(raw_output.glob("*.xml")):
        try:
            root = ET.parse(document).getroot()
        except (ET.ParseError, OSError):
            continue  # absent or caught mid-write; the next poll sees the whole file
        if root.get("endtime"):
            return True
    return False


def _kill_group(group: int) -> None:
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.killpg(group, signal.SIGKILL)


@contextlib.contextmanager
def _unwind_on_termination() -> Iterator[None]:
    """Make SIGTERM and SIGHUP unwind the supervisor instead of killing it outright.

    BenchExec runs in its own process group, so a signal sent to the harness's group
    no longer reaches it. Unwinding runs the `finally` in `run_bounded`, which stops
    BenchExec's group, so stopping the harness stops the run as it always did.
    """
    if threading.current_thread() is not threading.main_thread():
        yield
        return

    def unwind(signum: int, _frame: object) -> None:
        raise SystemExit(128 + signum)

    previous = {number: signal.signal(number, unwind) for number in (signal.SIGTERM, signal.SIGHUP)}
    try:
        yield
    finally:
        for number, handler in previous.items():
            signal.signal(number, handler)


def run_bounded(
    command: Sequence[str],
    *,
    env: Mapping[str, str],
    raw_output: Path,
    wall_seconds: float | None,
    exit_grace_seconds: float = EXIT_GRACE_SECONDS,
    wall_grace_seconds: float = WALL_GRACE_SECONDS,
    poll_seconds: float = POLL_SECONDS,
    check_seconds: float = CHECK_SECONDS,
    on_poll: Callable[[int], None] | None = None,
) -> int:
    """Run `command` to exit; kill its process group and return `HUNG_STATUS` if it hangs.

    The run is hung when it has not exited `exit_grace_seconds` after BenchExec
    wrote its final results, or when it outlives `wall_seconds` (BenchExec's own
    wall limit, when there is one) by `wall_grace_seconds`. The group is killed on
    every exit, normal or not, so a descendant that outlives BenchExec is reaped.
    `on_poll(pid)` runs every `poll_seconds` while BenchExec is alive.
    """
    with _unwind_on_termination():
        started = time.monotonic()
        finished_at: float | None = None
        next_check = started
        process = subprocess.Popen(command, env=dict(env), process_group=0)
        try:
            while True:
                try:
                    return process.wait(timeout=poll_seconds)
                except subprocess.TimeoutExpired:
                    pass
                if on_poll is not None:
                    on_poll(process.pid)
                now = time.monotonic()
                if now < next_check:
                    continue
                next_check = now + check_seconds
                if finished_at is None and results_final(raw_output):
                    finished_at = now
                reason: str | None = None
                if finished_at is not None and now - finished_at > exit_grace_seconds:
                    reason = (
                        f"BenchExec wrote its final results and did not exit within "
                        f"{exit_grace_seconds:g} s"
                    )
                elif wall_seconds is not None and now - started > wall_seconds + wall_grace_seconds:
                    reason = (
                        f"BenchExec outlived its {wall_seconds:g} s wall limit by more than "
                        f"{wall_grace_seconds:g} s"
                    )
                if reason is not None:
                    _kill_group(process.pid)
                    process.wait()
                    _record_hang(raw_output, reason, now - started, finished_at is not None)
                    return HUNG_STATUS
        finally:
            if process.poll() is None:
                # Interrupted: let BenchExec stop its run and clean up before the group dies.
                with contextlib.suppress(ProcessLookupError, PermissionError):
                    os.killpg(process.pid, signal.SIGTERM)
                with contextlib.suppress(subprocess.TimeoutExpired):
                    process.wait(timeout=INTERRUPT_GRACE_SECONDS)
            _kill_group(process.pid)


def _record_hang(raw_output: Path, reason: str, elapsed: float, results_written: bool) -> None:
    """Keep the cause beside the run's raw output, which a failed phase retains."""
    with contextlib.suppress(OSError):
        (raw_output / HUNG_EVIDENCE).write_text(
            json.dumps(
                {
                    "reason": reason,
                    "elapsed_seconds": round(elapsed, 1),
                    "results_written": results_written,
                },
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
