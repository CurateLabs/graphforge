from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from graphforge_bench.hybrid_cgroup_v2 import (
    benchexec_cgroup_version,
    cgroup_v1_mountpoints,
    has_cgroup_v2_mount,
    hybrid_pressure_deltas,
    is_hybrid_cgroup_layout,
    read_psi_total_seconds,
    read_unified_pressure_totals,
)


class HybridCgroupV2Tests(unittest.TestCase):
    def test_detects_hybrid_layout(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cgroup = root / "cgroup"
            unified = cgroup / "unified"
            unified.mkdir(parents=True)
            (unified / "cgroup.controllers").write_text("", encoding="utf-8")
            mounts = root / "mounts"
            mounts.write_text(
                "\n".join(
                    (
                        "tmpfs /sys/fs/cgroup tmpfs rw 0 0",
                        "cgroup2 /sys/fs/cgroup/unified cgroup2 rw 0 0",
                        "cgroup /sys/fs/cgroup/memory cgroup rw 0 0",
                    )
                ),
                encoding="utf-8",
            )
            self.assertTrue(has_cgroup_v2_mount(mounts_path=mounts))
            self.assertEqual(
                cgroup_v1_mountpoints(mounts_path=mounts),
                (Path("/sys/fs/cgroup/memory"),),
            )
            self.assertEqual(benchexec_cgroup_version(mounts_path=mounts), 1)
            self.assertTrue(is_hybrid_cgroup_layout(cgroup_root=cgroup, mounts_path=mounts))

    def test_pure_v2_is_not_hybrid(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cgroup = root / "cgroup"
            unified = cgroup / "unified"
            unified.mkdir(parents=True)
            (unified / "cgroup.controllers").write_text("cpu io memory", encoding="utf-8")
            mounts = root / "mounts"
            mounts.write_text(
                "cgroup2 /sys/fs/cgroup/unified cgroup2 rw 0 0\n",
                encoding="utf-8",
            )
            self.assertFalse(is_hybrid_cgroup_layout(cgroup_root=cgroup, mounts_path=mounts))
            self.assertEqual(benchexec_cgroup_version(mounts_path=mounts), 2)

    def test_read_psi_total_seconds(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            pressure = Path(directory) / "cpu.pressure"
            pressure.write_text(
                "some avg10=0.00 avg60=0.00 avg300=0.00 total=2500000000\n",
                encoding="utf-8",
            )
            self.assertEqual(read_psi_total_seconds(pressure), 2.5)

    def test_hybrid_pressure_deltas_are_non_negative(self) -> None:
        before = {
            "pressure-cpu-some": 1.0,
            "pressure-io-some": 2.0,
            "pressure-memory-some": 3.0,
        }
        after = {
            "pressure-cpu-some": 1.5,
            "pressure-io-some": 1.0,
            "pressure-memory-some": 4.0,
        }
        self.assertEqual(
            hybrid_pressure_deltas(before, after),
            {
                "pressure-cpu-some": 0.5,
                "pressure-io-some": 0.0,
                "pressure-memory-some": 1.0,
            },
        )

    def test_read_unified_pressure_totals(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            unified = Path(directory)
            (unified / "cpu.pressure").write_text("some total=1000\n", encoding="utf-8")
            (unified / "io.pressure").write_text("some total=2000\n", encoding="utf-8")
            (unified / "memory.pressure").write_text("some total=3000\n", encoding="utf-8")
            totals = read_unified_pressure_totals(unified_root=unified)
            self.assertEqual(totals["pressure-cpu-some"], 0.000001)
            self.assertEqual(totals["pressure-io-some"], 0.000002)
            self.assertEqual(totals["pressure-memory-some"], 0.000003)


if __name__ == "__main__":
    unittest.main()
