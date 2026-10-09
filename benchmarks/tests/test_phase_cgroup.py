"""A phase fails for its own swap use, not for another process's (#1914).

The tests build the cgroup and /proc files BenchExec's layout produces (a `.scope`
holding `benchexec_process_*` for BenchExec and `benchmark_*` for the run), so the
two cases a shared host separates are exercised without delegated cgroups: the
phase's pages swapped, and some other process's pages swapped.
"""

from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import unittest

from graphforge_bench import phase_cgroup
from graphforge_bench.benchexec_process import run_bounded
from graphforge_bench.phase_cgroup import PhaseSwapSampler

SCOPE = "user.slice/user-1000.slice/user@1000.service/benchexec.slice/benchexec_AbC.scope"
LAUNCHER = "user.slice/user-1000.slice/user@1000.service/app.slice/t3code.service"
PID = 4242


class FakeHost:
    """A `/sys/fs/cgroup` and a `/proc` in a temporary directory."""

    def __init__(self, base: Path) -> None:
        self.cgroups = base / "cgroup"
        self.proc = base / "proc"

    def cgroup(
        self, path: str, *, pswpout: int | None, peak: int | None = 0, swap_max: str = "max"
    ) -> Path:
        directory = self.cgroups / path
        directory.mkdir(parents=True, exist_ok=True)
        lines = ["anon 1048576", "file 4096"]
        if pswpout is not None:
            lines += [f"pswpin {pswpout}", f"pswpout {pswpout}"]
        (directory / "memory.stat").write_text("\n".join(lines) + "\n")
        if peak is not None:
            (directory / "memory.swap.peak").write_text(f"{peak}\n")
        (directory / "memory.swap.max").write_text(f"{swap_max}\n")
        return directory

    def place(self, pid: int, path: str) -> None:
        (self.proc / str(pid)).mkdir(parents=True, exist_ok=True)
        (self.proc / str(pid) / "cgroup").write_text(f"0::/{path}\n")

    def sampler(self) -> PhaseSwapSampler:
        return PhaseSwapSampler(proc_root=self.proc, cgroup_root=self.cgroups)

    def benchexec_run(self, *, pswpout: int, peak: int = 0) -> Path:
        """BenchExec in its process cgroup, the run in `benchmark_*` beside it."""
        self.cgroup(f"{SCOPE}/benchexec_process_x1", pswpout=0)
        self.place(PID, f"{SCOPE}/benchexec_process_x1")
        return self.cgroup(f"{SCOPE}/benchmark_r1", pswpout=pswpout, peak=peak, swap_max="0")


class ScratchTest(unittest.TestCase):
    def setUp(self) -> None:
        scratch = tempfile.TemporaryDirectory(prefix="phase-cgroup-")
        self.addCleanup(scratch.cleanup)
        self.scratch = Path(scratch.name)
        self.host = FakeHost(self.scratch)


class PhaseSwapTests(ScratchTest):
    def test_pages_of_the_phase_that_went_to_swap_fail_it(self) -> None:
        self.host.benchexec_run(pswpout=5, peak=20480)
        sampler = self.host.sampler()
        sampler.sample(PID)
        self.assertTrue(sampler.observed)
        self.assertTrue(sampler.swapped)
        evidence = sampler.evidence()
        self.assertEqual(phase_cgroup.verdict(evidence), True)
        self.assertEqual(
            phase_cgroup.detail(evidence),
            "the phase's own cgroup paged out (benchmark_r1: pswpout 5 pages, swap peak "
            "20480 bytes); see phase-cgroup-swap.json",
        )

    def test_swap_held_at_any_time_fails_the_phase_even_when_none_was_paged_out_since(self) -> None:
        self.host.benchexec_run(pswpout=0, peak=4096)
        sampler = self.host.sampler()
        sampler.sample(PID)
        self.assertTrue(sampler.swapped)

    def test_a_phase_that_never_swapped_passes(self) -> None:
        self.host.benchexec_run(pswpout=0, peak=0)
        sampler = self.host.sampler()
        sampler.sample(PID)
        self.assertTrue(sampler.observed)
        self.assertFalse(sampler.swapped)
        self.assertEqual(phase_cgroup.verdict(sampler.evidence()), False)

    def test_other_processes_swapping_on_a_shared_host_do_not_fail_it(self) -> None:
        # /proc/vmstat would count all of this. The launcher's cgroup (everything else the
        # user runs), BenchExec's slice, BenchExec's own process and an unrelated run
        # cgroup of another BenchExec all page out; this phase's run cgroup does not.
        self.host.benchexec_run(pswpout=0)
        self.host.cgroup(LAUNCHER, pswpout=760_340, peak=1_038_213_120)
        self.host.cgroup(
            "user.slice/user-1000.slice/user@1000.service/benchexec.slice", pswpout=6, peak=24576
        )
        self.host.cgroup(f"{SCOPE}/benchexec_process_x1", pswpout=9, peak=8192)
        self.host.cgroup(
            "user.slice/user-1000.slice/user@1000.service/benchexec.slice/"
            "benchexec_Other.scope/benchmark_zz",
            pswpout=8466,
            peak=34_652_160,
        )
        sampler = self.host.sampler()
        sampler.sample(PID)
        self.assertTrue(sampler.observed)
        self.assertFalse(sampler.swapped)
        self.assertEqual(list(sampler.evidence()["runs"]), ["benchmark_r1"])

    def test_a_process_not_yet_moved_into_its_scope_is_not_read(self) -> None:
        # BenchExec starts in the launcher's cgroup, which holds unrelated processes.
        self.host.cgroup(LAUNCHER, pswpout=760_340, peak=1_038_213_120)
        self.host.cgroup(
            "user.slice/user-1000.slice/user@1000.service/app.slice/benchmark_stray", pswpout=99
        )
        self.host.place(PID, LAUNCHER)
        sampler = self.host.sampler()
        sampler.sample(PID)
        self.assertFalse(sampler.observed)
        self.assertIsNone(phase_cgroup.verdict(sampler.evidence()))

    def test_counters_seen_before_the_run_cgroup_was_removed_are_kept(self) -> None:
        run = self.host.benchexec_run(pswpout=3, peak=12288)
        sampler = self.host.sampler()
        sampler.sample(PID)
        for name in ("memory.stat", "memory.swap.peak", "memory.swap.max"):
            (run / name).unlink()  # BenchExec removed the run cgroup
        run.rmdir()
        sampler.sample(PID)
        self.assertTrue(sampler.swapped)
        self.assertEqual(sampler.evidence()["runs"]["benchmark_r1"]["pswpout_pages"], 3)

    def test_the_largest_reading_of_each_counter_is_the_evidence(self) -> None:
        run = self.host.benchexec_run(pswpout=0, peak=0)
        sampler = self.host.sampler()
        sampler.sample(PID)
        self.assertFalse(sampler.swapped)
        self.host.cgroup(f"{SCOPE}/benchmark_r1", pswpout=11, peak=45056, swap_max="0")
        sampler.sample(PID)
        self.host.cgroup(f"{SCOPE}/benchmark_r1", pswpout=2, peak=8192, swap_max="0")
        sampler.sample(PID)
        self.assertTrue(run.is_dir())
        self.assertEqual(
            sampler.evidence()["runs"]["benchmark_r1"],
            {"pswpout_pages": 11, "swap_peak_bytes": 45056, "swap_max": "0"},
        )

    def test_a_cgroup_that_does_not_report_page_outs_is_unobserved_not_clean(self) -> None:
        self.host.cgroup(f"{SCOPE}/benchexec_process_x1", pswpout=0)
        self.host.place(PID, f"{SCOPE}/benchexec_process_x1")
        self.host.cgroup(f"{SCOPE}/benchmark_r1", pswpout=None, peak=0)
        sampler = self.host.sampler()
        sampler.sample(PID)
        self.assertFalse(sampler.observed)
        self.assertIsNone(phase_cgroup.verdict(sampler.evidence()))

    def test_a_missing_peak_file_does_not_hide_a_page_out(self) -> None:
        self.host.benchexec_run(pswpout=0, peak=0)
        self.host.cgroup(f"{SCOPE}/benchmark_r1", pswpout=4, peak=None, swap_max="0")
        sampler = self.host.sampler()
        sampler.sample(PID)
        self.assertTrue(sampler.swapped)


class EvidenceTests(ScratchTest):
    def test_evidence_survives_a_round_trip_and_rejects_anything_else(self) -> None:
        self.host.benchexec_run(pswpout=1, peak=4096)
        sampler = self.host.sampler()
        sampler.sample(PID)
        path = self.scratch / phase_cgroup.EVIDENCE_NAME
        sampler.write(path)
        self.assertEqual(phase_cgroup.load_evidence(path), sampler.evidence())
        self.assertIsNone(phase_cgroup.load_evidence(self.scratch / "absent.json"))
        for bad in (
            "not json",
            json.dumps({"schema": "other/1", "runs": {}}),
            json.dumps({"schema": phase_cgroup.SCHEMA, "runs": {"r": {"pswpout_pages": "1"}}}),
            json.dumps([]),
        ):
            path.write_text(bad)
            self.assertIsNone(phase_cgroup.load_evidence(path), bad)

    def test_the_supervisor_feeds_the_sampler_while_benchexec_runs(self) -> None:
        # The wiring: run_bounded polls the sampler with BenchExec's pid, and the evidence
        # the sampler gathered is what a phase is judged on.
        sampler = self.host.sampler()
        placed: list[int] = []

        def poll(pid: int) -> None:
            if not placed:
                placed.append(pid)
                self.host.place(pid, f"{SCOPE}/benchexec_process_x1")
                self.host.cgroup(f"{SCOPE}/benchexec_process_x1", pswpout=0)
                self.host.cgroup(f"{SCOPE}/benchmark_r1", pswpout=6, peak=24576, swap_max="0")
            sampler.sample(pid)

        status = run_bounded(
            [sys.executable, "-c", "import time; time.sleep(0.4)"],
            env={},
            raw_output=self.scratch,
            wall_seconds=None,
            poll_seconds=0.02,
            on_poll=poll,
        )
        self.assertEqual(status, 0)
        self.assertEqual(phase_cgroup.verdict(sampler.evidence()), True)


if __name__ == "__main__":
    unittest.main()
