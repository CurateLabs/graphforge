"""BenchExec must not be able to finish its work and never exit (#1914).

BenchExec loads its tool-info module in a `multiprocessing.Pool` worker that it moves
into a container. The worker unshares a PID namespace and forks, so the process the
pool tracks is a waiting parent; BenchExec's exit SIGTERMs it and the real worker is
orphaned. With a forkserver or spawn start method that orphan holds the resource
tracker's pipe and BenchExec's exit handler waits for the tracker for ever.

Whether the interpreter's own exit handler does that wait depends on its version
(3.12.3 does not, 3.12.13 and 3.14 do), so the model below waits for the tracker
explicitly, the way that handler does. The hang then follows from the start method
alone, on every interpreter the harness supports, and the tests need no BenchExec.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import time
import unittest

from graphforge_bench import benchexec_process
from graphforge_bench.benchexec_process import HUNG_EVIDENCE, HUNG_STATUS, run_bounded
from graphforge_bench.gdc_contracts import workspace_root

HARNESS = workspace_root() / "harness"
FINAL_RESULTS = '<result endtime="2026-10-09T00:00:00+00:00"><run/></result>'
OPEN_RESULTS = '<result starttime="2026-10-09T00:00:00+00:00"><run/></result>'

# A pool whose worker forks like BenchExec's containerized tool: the process the pool
# tracks waits for a child, and the child carries on as the worker and outlives the pool.
POOL_MODEL = """
    import multiprocessing, multiprocessing.resource_tracker, os, signal, sys, threading, time
    from pathlib import Path

    def init():
        signal.signal(signal.SIGTERM, signal.SIG_DFL)

    def enter_container():
        pid = os.fork()
        if pid:
            os.waitpid(pid, 0)
            os._exit(0)
        threading.Thread(target=time.sleep, args=(60,)).start()
        return os.getpid()

    def run(context, raw):
        pool = (multiprocessing.get_context(context) if context else multiprocessing).Pool(1, init)
        Path(raw, "pids").write_text(str(pool.apply(enter_container)))
        Path(raw, "benchmark.results.xml").write_text(%r)
        # BenchExec's exit: the interpreter's exit handler stops the resource tracker, which
        # closes its end of the tracker's pipe and waits for the tracker to exit. The tracker
        # exits only when every holder of the pipe has closed it; a forkserver or spawn worker
        # holds it, and the orphaned worker never closes it. A fork pool starts no tracker.
        multiprocessing.resource_tracker._resource_tracker._stop()
"""


def alive(pid: int) -> bool:
    """Whether a process still runs; a zombie awaiting its reaper has already died."""
    try:
        state = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[0]
    except (OSError, IndexError):
        return False
    return state != "Z"


def wait_dead(pid: int, seconds: float = 5.0) -> bool:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if not alive(pid):
            return True
        time.sleep(0.05)
    return not alive(pid)


class ScratchTest(unittest.TestCase):
    def setUp(self) -> None:
        scratch = tempfile.TemporaryDirectory(prefix="benchexec-process-")
        self.addCleanup(scratch.cleanup)
        self.scratch = Path(scratch.name)
        self.raw = self.scratch / "raw"
        self.raw.mkdir()

    def script(self, name: str, body: str) -> list[str]:
        path = self.scratch / name
        path.write_text(textwrap.dedent(body), encoding="utf-8")
        return [sys.executable, str(path)]

    def run_bounded(self, command: list[str], **kwargs: object) -> int:
        options: dict[str, object] = {
            "env": {**os.environ, "PYTHONPATH": str(HARNESS)},
            "raw_output": self.raw,
            "wall_seconds": None,
            "exit_grace_seconds": 1.0,
            "poll_seconds": 0.02,
            "check_seconds": 0.1,
        }
        options.update(kwargs)
        return run_bounded(command, **options)  # type: ignore[arg-type]

    def evidence(self) -> dict[str, object]:
        return json.loads((self.raw / HUNG_EVIDENCE).read_text(encoding="utf-8"))


class ResultsFinalTests(ScratchTest):
    def test_only_the_closing_record_of_the_run_set_counts(self) -> None:
        self.assertFalse(benchexec_process.results_final(self.raw))
        document = self.raw / "benchmark.results.xml"
        document.write_text(OPEN_RESULTS)
        self.assertFalse(benchexec_process.results_final(self.raw))
        document.write_text("<result endtime=")  # caught mid-write
        self.assertFalse(benchexec_process.results_final(self.raw))
        document.write_text(FINAL_RESULTS)
        self.assertTrue(benchexec_process.results_final(self.raw))


class RunBoundedTests(ScratchTest):
    def test_a_run_that_exits_returns_its_own_status(self) -> None:
        self.assertEqual(self.run_bounded(self.script("exit.py", "raise SystemExit(3)")), 3)
        self.assertFalse((self.raw / HUNG_EVIDENCE).exists())

    def test_the_poll_hook_sees_the_process_while_it_runs(self) -> None:
        seen: list[int] = []
        command = self.script("slow.py", "import time; time.sleep(0.3)")
        self.assertEqual(self.run_bounded(command, on_poll=seen.append), 0)
        self.assertTrue(seen)
        self.assertEqual(len(set(seen)), 1)

    def test_a_process_left_behind_by_a_finished_run_is_killed(self) -> None:
        command = self.script(
            "leaves.py",
            f"""
            import subprocess, sys
            child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
            open({str(self.scratch / "pid")!r}, "w").write(str(child.pid))
            """,
        )
        self.assertEqual(self.run_bounded(command), 0)
        self.assertTrue(wait_dead(int((self.scratch / "pid").read_text())))

    def test_a_run_held_by_an_orphan_after_its_results_are_written_is_killed_typed(self) -> None:
        # The tracker shape: the run waits until every writer of a pipe has closed, and
        # an orphan it cannot see keeps the write end open.
        command = self.script(
            "held.py",
            f"""
            import os, subprocess, sys
            from pathlib import Path
            read_end, write_end = os.pipe()
            os.set_inheritable(write_end, True)
            orphan = subprocess.Popen(
                [sys.executable, "-c", "import time; time.sleep(60)"],
                pass_fds=[write_end],
            )
            os.close(write_end)
            Path({str(self.scratch / "pid")!r}).write_text(str(orphan.pid))
            Path({str(self.raw / "benchmark.results.xml")!r}).write_text({FINAL_RESULTS!r})
            os.read(read_end, 1)
            """,
        )
        started = time.monotonic()
        self.assertEqual(self.run_bounded(command), HUNG_STATUS)
        self.assertLess(time.monotonic() - started, 30)
        evidence = self.evidence()
        self.assertTrue(evidence["results_written"])
        self.assertIn("did not exit within 1 s", str(evidence["reason"]))
        self.assertTrue(wait_dead(int((self.scratch / "pid").read_text())))

    def test_stopping_the_supervisor_stops_benchexec_in_its_own_group(self) -> None:
        # BenchExec has its own process group, so a signal to the harness's group no
        # longer reaches it; SIGTERM to the harness must still end the run.
        pid_file = self.scratch / "child.pid"
        supervisor = subprocess.Popen(
            [
                sys.executable,
                "-c",
                textwrap.dedent(
                    f"""
                    import sys
                    from pathlib import Path
                    sys.path.insert(0, {str(HARNESS)!r})
                    from graphforge_bench.benchexec_process import run_bounded
                    child = (
                        "import os, time; "
                        "open({str(pid_file)!r}, 'w').write(str(os.getpid())); time.sleep(60)"
                    )
                    run_bounded(
                        [sys.executable, "-c", child],
                        env={{}},
                        raw_output=Path({str(self.raw)!r}),
                        wall_seconds=None,
                    )
                    """
                ),
            ]
        )
        self.addCleanup(supervisor.kill)
        deadline = time.monotonic() + 20
        while not pid_file.exists() and time.monotonic() < deadline:
            time.sleep(0.05)
        child = int(pid_file.read_text())
        supervisor.terminate()
        self.assertEqual(supervisor.wait(timeout=30), 128 + 15)
        self.assertTrue(wait_dead(child))

    def test_a_run_that_outlives_its_wall_limit_is_killed_typed(self) -> None:
        command = self.script("forever.py", "import time; time.sleep(60)")
        self.assertEqual(
            self.run_bounded(command, wall_seconds=0.2, wall_grace_seconds=0.3), HUNG_STATUS
        )
        evidence = self.evidence()
        self.assertFalse(evidence["results_written"])
        self.assertIn("outlived its 0.2 s wall limit", str(evidence["reason"]))

    def test_no_wall_limit_leaves_a_slow_run_alone_until_its_results_are_written(self) -> None:
        command = self.script("slow.py", "import time; time.sleep(1.5)")
        self.assertEqual(self.run_bounded(command, wall_seconds=None), 0)


class ForkserverExitHangTests(ScratchTest):
    """The mechanism itself: an orphaned pool worker and a forkserver or spawn tracker."""

    def model(self, context: str | None) -> list[str]:
        body = textwrap.dedent(POOL_MODEL % FINAL_RESULTS)
        body += textwrap.dedent(
            f"""
            if __name__ == "__main__":
                run({context!r}, {str(self.raw)!r})
            """
        )
        return self.script("model.py", body)

    def test_a_forkserver_pool_never_exits_and_is_killed_typed(self) -> None:
        self.assertEqual(self.run_bounded(self.model("forkserver")), HUNG_STATUS)
        self.assertTrue(self.evidence()["results_written"])
        self.assertTrue(wait_dead(int((self.raw / "pids").read_text())))

    def test_a_fork_pool_exits_and_its_orphan_is_reaped(self) -> None:
        self.assertEqual(self.run_bounded(self.model("fork"), exit_grace_seconds=20.0), 0)
        self.assertFalse((self.raw / HUNG_EVIDENCE).exists())
        self.assertTrue(wait_dead(int((self.raw / "pids").read_text())))


class LauncherTests(ScratchTest):
    """`benchexec_launcher` runs BenchExec's entry point under the fork start method."""

    def stub_benchexec(self, main: str) -> dict[str, str]:
        package = self.scratch / "stub" / "benchexec"
        package.mkdir(parents=True, exist_ok=True)
        (package / "__init__.py").write_text("")
        (package / "benchexec.py").write_text(textwrap.dedent(main), encoding="utf-8")
        return {
            **os.environ,
            "PYTHONPATH": os.pathsep.join([str(self.scratch / "stub"), str(HARNESS)]),
        }

    def test_the_entry_point_runs_with_its_own_arguments_under_fork(self) -> None:
        environment = self.stub_benchexec(
            """
            import multiprocessing, sys

            def main():
                print(multiprocessing.get_start_method(allow_none=True), *sys.argv)
            """
        )
        completed = subprocess.run(
            [sys.executable, "-m", "graphforge_bench.benchexec_launcher", "--tool-directory", "x"],
            env=environment,
            capture_output=True,
            text=True,
            check=True,
        )
        self.assertEqual(completed.stdout.split(), ["fork", "benchexec", "--tool-directory", "x"])

    def stub_pool_benchexec(self) -> dict[str, str]:
        # What BenchExec does: Pool(1) on the default context, whose worker is orphaned.
        main = textwrap.dedent(POOL_MODEL % FINAL_RESULTS) + textwrap.dedent(
            """
            def main():
                raw = sys.argv[sys.argv.index("--outputpath") + 1]
                run(None, raw)
            """
        )
        return self.stub_benchexec(main)

    def launch(self, *, interpreter_default: str | None, through_launcher: bool) -> int:
        """Run the stub BenchExec under `run_bounded`, with the interpreter's default forced."""
        force = (
            f"import multiprocessing; multiprocessing.set_start_method({interpreter_default!r}); "
            if interpreter_default
            else ""
        )
        entry = (
            "import runpy; runpy.run_module('graphforge_bench.benchexec_launcher', "
            "run_name='__main__')"
            if through_launcher
            else "from benchexec.benchexec import main; main()"
        )
        return run_bounded(
            [sys.executable, "-c", force + entry, "--outputpath", str(self.raw)],
            env=self.stub_pool_benchexec(),
            raw_output=self.raw,
            wall_seconds=None,
            exit_grace_seconds=3.0,
            poll_seconds=0.02,
            check_seconds=0.1,
        )

    def test_the_launcher_makes_the_pool_exit_whatever_the_interpreters_default_is(self) -> None:
        for default in (None, "fork", "forkserver", "spawn"):
            with self.subTest(interpreter_default=default):
                (self.raw / "pids").unlink(missing_ok=True)
                self.assertEqual(self.launch(interpreter_default=default, through_launcher=True), 0)
                self.assertTrue(wait_dead(int((self.raw / "pids").read_text())))

    def test_without_the_launcher_a_forkserver_or_spawn_default_hangs_and_is_killed_typed(
        self,
    ) -> None:
        # The control: the same BenchExec stand-in, the same supervisor, and a default start
        # method that starts a resource tracker. It never exits and is killed with its group.
        for default in ("forkserver", "spawn"):
            with self.subTest(interpreter_default=default):
                (self.raw / HUNG_EVIDENCE).unlink(missing_ok=True)
                self.assertEqual(
                    self.launch(interpreter_default=default, through_launcher=False), HUNG_STATUS
                )
                self.assertTrue(self.evidence()["results_written"])
                self.assertTrue(wait_dead(int((self.raw / "pids").read_text())))


if __name__ == "__main__":
    unittest.main()
