#!/usr/bin/env python3
"""Integration tests for native executor invocation and provenance binding."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import unittest

ROOT = Path(__file__).resolve().parents[2]
EXECUTOR = ROOT / "scripts/ci/release-load-executor.py"
SPEC = importlib.util.spec_from_file_location("release_load_executor", EXECUTOR)
assert SPEC and SPEC.loader
EXECUTOR_MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = EXECUTOR_MODULE
SPEC.loader.exec_module(EXECUTOR_MODULE)


class LoadExecutorTests(unittest.TestCase):
    def test_timeout_kills_descendants_after_process_group_leader_exits(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            child_pid = Path(raw) / "child.pid"
            parent = Path(raw) / "parent.py"
            parent.write_text(
                textwrap.dedent(
                    f"""
                    import subprocess, sys, time
                    child = subprocess.Popen([
                        sys.executable,
                        "-c",
                        "import signal,time; "
                        "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
                        "time.sleep(60)",
                    ])
                    open({str(child_pid)!r}, "w").write(str(child.pid))
                    time.sleep(60)
                    """
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "timed out"):
                EXECUTOR_MODULE.run_command([sys.executable, str(parent)], timeout=1)
            pid = int(child_pid.read_text(encoding="utf-8"))
            state = subprocess.run(
                ["ps", "-o", "stat=", "-p", str(pid)],
                check=False,
                capture_output=True,
                text=True,
            ).stdout.strip()
            self.assertTrue(not state or state.startswith("Z"), state)

    def test_real_invocation_cache_and_result_provenance_fail_closed(self) -> None:
        sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
        with tempfile.TemporaryDirectory() as raw:
            work = Path(raw)
            requests = work / "requests"
            requests.mkdir()
            fixture = work / "fixture.json"
            fixture.write_text('{"nodes":[],"edges":[]}\n', encoding="utf-8")
            fixture_sha = hashlib.sha256(fixture.read_bytes()).hexdigest()
            artifact = work / "native.bin"
            artifact.write_bytes(b"native-artifact")
            marker = work / "preflight-count"
            preflight = work / "preflight.py"
            preflight.write_text(
                textwrap.dedent(
                    f"""
                    from pathlib import Path
                    path = Path({str(marker)!r})
                    path.write_text((path.read_text() + "x") if path.exists() else "x")
                    print("exhaustive native suite passed")
                    """
                ),
                encoding="utf-8",
            )
            probe = work / "probe.py"
            probe.write_text(
                textwrap.dedent(
                    """
                    import argparse, json
                    from pathlib import Path
                    parser = argparse.ArgumentParser()
                    parser.add_argument("--request", type=Path, required=True)
                    parser.add_argument("--output", type=Path, required=True)
                    args = parser.parse_args()
                    request = json.loads(args.request.read_text())
                    report = {
                        "schema": "graphforge-load-native-probe/1",
                        "language": "python",
                        "dataset_sha256": request["manifest"]["content_sha256"],
                        "workload": request["workload"]["id"],
                        "observed": {"node_rows": 2, "edge_rows": 1,
                                     "rank_rows": 0, "find_rows": 0,
                                     "reopen_node_rows": 2,
                                     "schema_sha256": "e" * 64,
                                     "ordering_sha256": "f" * 64,
                                     "node_result_sha256": (
                                         "a" if request["identity"].endswith("/first") else "d"
                                     ) * 64,
                                     "rank_result_sha256": "b" * 64,
                                     "find_result_sha256": "c" * 64},
                        "persisted_bytes": 10, "temporary_bytes": 0,
                        "cleanup": "complete", "reopen_equivalent": True,
                    }
                    args.output.write_text(json.dumps(report))
                    """
                ),
                encoding="utf-8",
            )
            surface = json.loads(
                (ROOT / "tests/contracts/non-cypher-rust-surface.json").read_text()
            )
            inventory = surface["method_evidence_groups"]["lifecycle-construction"]["ids"]

            def invoke(name: str, *, nodes: int = 2) -> subprocess.CompletedProcess[str]:
                request = requests / f"{name}.json"
                request.write_text(
                    json.dumps(
                        {
                            "schema": "graphforge-load-request/1",
                            "identity": f"python/public-project-and-knowledge-surfaces/{name}",
                            "source_sha": sha,
                            "fixture": str(fixture),
                            "manifest": {
                                "content_sha256": fixture_sha,
                                "live_nodes": nodes,
                                "live_edges": 1,
                            },
                            "workload": {"id": "public-project-and-knowledge-surfaces"},
                            "required_inventory": inventory,
                            "case_timeout_seconds": 30,
                            "preflight_timeout_seconds": 30,
                        }
                    ),
                    encoding="utf-8",
                )
                env = dict(os.environ)
                env["GF_LOAD_EXECUTOR_TESTING"] = "1"
                return subprocess.run(
                    [
                        sys.executable,
                        str(EXECUTOR),
                        "--language",
                        "python",
                        "--request",
                        str(request),
                        "--output",
                        str(work / f"{name}.report.json"),
                        "--preflight-command",
                        sys.executable,
                        str(preflight),
                        "--probe-command",
                        sys.executable,
                        str(probe),
                        "--artifact",
                        str(artifact),
                    ],
                    cwd=ROOT,
                    text=True,
                    capture_output=True,
                    check=False,
                    env=env,
                )

            first = invoke("first")
            self.assertEqual(first.returncode, 0, first.stdout + first.stderr)
            second = invoke("second")
            self.assertEqual(second.returncode, 0, second.stdout + second.stderr)
            self.assertEqual(marker.read_text(), "x", "preflight must be reused only after proof")
            first_report = json.loads((work / "first.report.json").read_text())
            report = json.loads((work / "second.report.json").read_text())
            self.assertEqual(report["provenance"]["outcome"], "passed")
            self.assertEqual(
                report["package"]["artifact_sha256"],
                hashlib.sha256(artifact.read_bytes()).hexdigest(),
            )
            self.assertNotEqual(report["result"]["rows_sha256"], fixture_sha)
            self.assertNotEqual(
                first_report["result"]["rows_sha256"],
                report["result"]["rows_sha256"],
                "native result changes must alter parity evidence",
            )

            mismatch = invoke("mismatch", nodes=3)
            self.assertNotEqual(mismatch.returncode, 0)
            self.assertIn("complete fixture", mismatch.stderr)

            artifact.write_bytes(b"changed-artifact")
            stale = invoke("stale")
            self.assertNotEqual(stale.returncode, 0)
            self.assertIn("preflight cache", stale.stderr)


if __name__ == "__main__":
    unittest.main()
