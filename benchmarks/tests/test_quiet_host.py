from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
import tempfile
import unittest

from graphforge_bench.quiet_host import (
    SAMPLE_SECONDS,
    WINDOW_SECONDS,
    HostNotQuietError,
    busy_processes,
    cpu_jiffies,
    wait_for_quiet_host,
)

CORES = 16
JIFFIES_PER_SAMPLE = CORES * SAMPLE_SECONDS * 100


class FakeHost:
    """A /proc tree whose CPU counters advance by a scripted busy-core load per sample."""

    def __init__(self, root: Path, load: Callable[[float], float]):
        self.root = root
        self.load = load
        self.now = 0.0
        self.total = 0
        self.idle = 0
        self._write_stat()

    def _write_stat(self) -> None:
        busy = self.total - self.idle
        (self.root / "stat").write_text(
            f"cpu  {busy} 0 0 {self.idle} 0 0 0 0 0 0\ncpu0 0 0 0 0 0 0 0 0 0 0\n",
            encoding="utf-8",
        )

    def process(self, pid: int, name: str) -> None:
        (self.root / str(pid)).mkdir()
        (self.root / str(pid) / "comm").write_text(f"{name}\n", encoding="utf-8")

    def exit(self, pid: int) -> None:
        (self.root / str(pid) / "comm").unlink()
        (self.root / str(pid)).rmdir()

    def clock(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        busy_cores = self.load(self.now)
        self.now += seconds
        busy = round(JIFFIES_PER_SAMPLE * busy_cores / CORES)
        self.total += JIFFIES_PER_SAMPLE
        self.idle += JIFFIES_PER_SAMPLE - busy
        self._write_stat()

    def wait(self, wait_seconds: int) -> dict[str, object]:
        return wait_for_quiet_host(
            wait_seconds, proc=self.root, cores=CORES, clock=self.clock, sleep=self.sleep
        )


class QuietHostTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    def test_quiet_host_returns_one_full_window_without_waiting(self) -> None:
        host = FakeHost(self.root, lambda _now: 0.25)
        host.process(10, "node")
        window = host.wait(0)
        self.assertEqual(
            window,
            {
                "window_seconds": WINDOW_SECONDS,
                "waited_seconds": 0,
                "mean_busy_cores": 0.25,
                "peak_busy_cores": 0.25,
            },
        )
        self.assertEqual(host.now, WINDOW_SECONDS)

    def test_named_competing_process_refuses_when_wait_expires(self) -> None:
        host = FakeHost(self.root, lambda _now: 0.0)
        host.process(10, "cargo")
        host.process(11, "graphforge-benc")
        with self.assertRaises(HostNotQuietError) as raised:
            host.wait(30)
        self.assertEqual(raised.exception.busy_processes, ["cargo", "graphforge-benc"])
        self.assertGreaterEqual(host.now, 30)

    def test_launch_waits_until_competing_build_exits(self) -> None:
        host = FakeHost(self.root, lambda _now: 0.0)
        host.process(10, "rustc")
        original_sleep = host.sleep

        def sleep(seconds: float) -> None:
            original_sleep(seconds)
            if host.now == 20:
                host.exit(10)

        host.sleep = sleep  # type: ignore[method-assign]
        window = host.wait(3_600)
        self.assertEqual(window["waited_seconds"], 20)
        self.assertEqual(host.now, 20 + WINDOW_SECONDS)

    def test_cpu_load_without_named_process_is_not_quiet(self) -> None:
        sustained = FakeHost(self.root, lambda _now: 0.8)
        with self.assertRaises(HostNotQuietError):
            sustained.wait(0)

    def test_one_busy_sample_restarts_the_window(self) -> None:
        host = FakeHost(self.root, lambda now: 3.0 if now == 10 else 0.1)
        window = host.wait(3_600)
        # The spike ends the first window at 15 s; a full window follows it.
        self.assertEqual(window["waited_seconds"], 15)
        self.assertEqual(window["peak_busy_cores"], 0.1)

    def test_busy_processes_reads_comm_and_ignores_command_lines(self) -> None:
        host = FakeHost(self.root, lambda _now: 0.0)
        host.process(10, "bash")
        (self.root / "10" / "cmdline").write_text("pgrep -x cargo", encoding="utf-8")
        host.process(11, "gf.real")
        (self.root / "self").mkdir()
        self.assertEqual(busy_processes(self.root), ["gf.real"])

    def test_cpu_jiffies_counts_iowait_as_idle(self) -> None:
        (self.root / "stat").write_text("cpu  10 1 2 30 4 5 6 7 0 0\n", encoding="utf-8")
        self.assertEqual(cpu_jiffies(self.root), (65, 34))


if __name__ == "__main__":
    unittest.main()
