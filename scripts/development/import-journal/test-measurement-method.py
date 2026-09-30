#!/usr/bin/env python3
"""Synthetic qualification/parity regressions; no builds, cache reset or ingest."""

import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from unittest.mock import patch

from measurement_contract import (
    BUILD_ENV,
    RECEIPTS,
    digest,
    read_json,
    require_external_output,
    runexec_result,
    successful_workload,
    write_json,
)

ROOT = Path(__file__).resolve().parent
MEASURE = ROOT / "measure-lane.py"
COMPARE = ROOT / "compare-pair.py"
VALUES = {
    "wall_ns": 1,
    "process_cpu_ns": 1,
    "fsync_calls": 1,
    "fsync_elapsed_ns": 1,
    "written_bytes": 1,
    "hashed_bytes": 1,
    "hash_elapsed_ns": 1,
}


def regions(lane, index):
    rows = {"root/manifest_persistence": {"calls": 1, "inclusive": VALUES.copy()}}
    if lane == "candidate":
        rows["root/journal_sync"] = {"calls": 1, "inclusive": VALUES.copy()}
        if index == 0:
            rows["root/journal_namespace_publication"] = {"calls": 1, "inclusive": VALUES.copy()}
        if index == 3:
            rows["root/journal_append"] = {"calls": 1, "inclusive": VALUES.copy()}
    return {
        "region_diagnostics": {
            "complete": True,
            "contract": "graphforge-region-diagnostics/2",
            "regions": rows,
        }
    }


with tempfile.TemporaryDirectory(prefix="gf1624-method-fixtures-") as tmp:
    container = Path(tmp)
    repo = container / "repo"
    repo.mkdir()
    binary = container / "synthetic-binary"
    binary.write_bytes(b"synthetic input identity; never executed")
    nodes = container / "nodes.parquet"
    edges = container / "edges.parquet"
    manifest = container / "inputs.sha256"
    manifest.write_text(
        f"bcbcbea526e61ceb63f6006ee5f56de6bb4f74cffdd68dc6eff6d230d3897f06  {nodes}\n"
        f"1c0ff75485f75e904cbd59b6f5d42da1d8b1af6ddac59ee6c495a4948a428d13  {edges}\n"
    )
    source = container / "source.sha"
    source.write_text("a" * 40)
    settings = {
        "source_sha": "a" * 40,
        "binary_sha256": digest(binary),
        "profile": "release",
        "features": [],
        "default_features": True,
        "target": "fixture-linux",
        "rustc_version": "fixture-rustc",
        "cargo_version": "fixture-cargo",
        "cargo_args": ["build", "--locked", "--release", "-p", "graphforge-cli", "--bin", "gf"],
        "build_environment": dict.fromkeys(BUILD_ENV),
    }

    def lane(pair, name, case="valid", profile="release"):
        evidence = container / "evidence"
        build = container / f"build-{pair}-{name}.json"
        metadata = {**settings, "profile": profile}
        if profile == "dev":
            metadata["cargo_args"] = [arg for arg in settings["cargo_args"] if arg != "--release"]
        write_json(build, metadata)
        count = 0

        def sample():
            nonlocal count
            index = count
            count += 1
            processes = []
            if case == "pre_overlap" or (case == "compiler" and index >= 13):
                processes.append({"name": "cargo"})
            return {
                "mono": index * 5.0,
                "system_busy_ticks": index * (500 if case == "busy" else 0),
                "processes": processes,
            }

        class Process:
            pid = 123

            def __init__(self, arguments, **kwargs):
                self.polls = 0
                assert arguments[arguments.index("--output") + 1] == str(
                    Path(kwargs["cwd"]) / "workload.log"
                )
                result = (
                    "returnvalue=7\n"
                    if case == "workload_exit"
                    else "exitsignal=9\n"
                    if case == "signal"
                    else "returnvalue=0\nterminationreason=walltime\n"
                    if case == "termination"
                    else "walltime=1s\n"
                    if case == "missing_return"
                    else "returnvalue=0\n"
                )
                kwargs["stdout"].write(result)
                (Path(kwargs["cwd"]) / "workload.log").write_text("synthetic workload log\n")
                env = kwargs["env"]
                output = Path(env["GF1624_OUT"])
                for index, filename in enumerate(RECEIPTS):
                    write_json(output / filename, regions(name, index))

            def poll(self):
                self.polls += 1
                return None if self.polls == 1 else (1 if case == "exit" else 0)

        arguments = [
            str(MEASURE),
            "--lane",
            name,
            "--pair",
            str(pair),
            "--binary",
            str(binary),
            "--worktree",
            str(repo),
            "--expected-source-sha",
            "a" * 40,
            "--build-source-file",
            str(source),
            "--build-provenance-file",
            str(build),
            "--evidence-root",
            str(evidence),
            "--inputs-sha-file",
            str(manifest),
            "--nodes",
            str(nodes if case != "inputs" else container / "wrong-nodes.parquet"),
            "--edges",
            str(edges),
        ]
        # Inject only the observation provider. Production qualification and
        # final-receipt persistence paths run unchanged; subprocesses are mocked.
        code = MEASURE.read_text().replace(
            "try:\n    run()", "sample = _fixture_sample\ntry:\n    run()"
        )
        with (
            patch.object(sys, "argv", arguments),
            patch("time.sleep"),
            patch("subprocess.check_output", return_value="a" * 40),
            patch("subprocess.run", return_value=subprocess.CompletedProcess([], 0)),
            patch("subprocess.Popen", Process),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            try:
                exec(
                    compile(code, str(MEASURE), "exec"),
                    {"__name__": "__main__", "__file__": str(MEASURE), "_fixture_sample": sample},
                )
            except (AssertionError, RuntimeError):
                if case == "valid":
                    raise
        return evidence / f"pair-{pair}" / name

    def compare(pair):
        arguments = [
            str(COMPARE),
            "--pair",
            str(pair),
            "--repository",
            str(repo),
            "--evidence-root",
            str(container / "evidence"),
        ]
        with (
            patch.object(sys, "argv", arguments),
            patch(
                "subprocess.check_output",
                return_value="crates/graphforge-api/src/import_session/journal.rs\n",
            ),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            exec(
                compile(COMPARE.read_text(), str(COMPARE), "exec"),
                {"__name__": "__main__", "__file__": str(COMPARE)},
            )

    def refused(pair):
        try:
            compare(pair)
        except (AssertionError, FileNotFoundError, ValueError):
            return
        raise AssertionError(f"Unqualified/mismatched pair {pair} was accepted")

    lane(1, "baseline")
    candidate = lane(1, "candidate")
    compare(1)
    output = read_json(container / "evidence/pair-1/comparison.json")
    before, after = output["lanes"]
    assert before["persistence_totals"]["fsync_calls"] == 5
    assert after["persistence_totals"]["fsync_calls"] == 12
    assert after["by_category"]["manifest_checkpoint"]["fsync_calls"] == 5
    assert after["by_category"]["journal_sync"]["fsync_calls"] == 5
    assert after["by_category"]["journal_namespace"]["fsync_calls"] == 1
    assert output["persistence_delta_candidate_minus_baseline"]["fsync_calls"] == 7

    for pair, case in enumerate(
        (
            "exit",
            "compiler",
            "busy",
            "pre_overlap",
            "workload_exit",
            "signal",
            "termination",
            "missing_return",
            "inputs",
        ),
        20,
    ):
        lane(pair, "baseline")
        failed = lane(pair, "candidate", case)
        status = read_json(failed / "qualification.json")
        assert status["qualified"] is False
        if case == "exit":
            assert status["completed"] is True and status["runexec_exit_code"] == 1
        if case == "workload_exit":
            assert status["completed"] is True and status["runexec_exit_code"] == 0
            assert status["workload_result"]["returnvalue"] == 7
        if case == "compiler":
            assert status["completed"] is True and status["compiler_overlap_samples"] > 0
        if case == "inputs":
            assert status["completed"] is False and status["inputs_verified"] is False
        if case in ("busy", "pre_overlap"):
            assert status["completed"] is False and status["quiet_window_passed"] is False
        refused(pair)

    # Qualification receipts are mandatory, and completed=false stays refused.
    qpath = candidate / "qualification.json"
    original_q = qpath.read_text()
    qpath.unlink()
    refused(1)
    status = json.loads(original_q)
    status["completed"] = False
    write_json(qpath, status)
    refused(1)
    qpath.write_text(original_q)
    changed = candidate / f"run/{RECEIPTS[3]}"
    original = changed.read_text()
    changed.write_text(original + "\n")
    refused(1)
    changed.write_text(original)

    # Rebind hashes and success flags to force semantic revalidation of raw
    # workload and compiler/quiet observations, beyond tamper detection alone.
    outcome_path = candidate / "runexec.txt"
    original_outcome = outcome_path.read_text()
    for raw, claimed in (
        ("returnvalue=7\n", {"returnvalue": 7, "exitsignal": None, "terminationreason": None}),
        (
            "returnvalue=0\nexitsignal=9\n",
            {"returnvalue": 0, "exitsignal": 9, "terminationreason": None},
        ),
        (
            "returnvalue=0\nterminationreason=walltime\n",
            {"returnvalue": 0, "exitsignal": None, "terminationreason": "walltime"},
        ),
        ("returnvalue=7\n", {"returnvalue": 0, "exitsignal": None, "terminationreason": None}),
        (
            "returnvalue=0\nreturnvalue=0\n",
            {"returnvalue": 0, "exitsignal": None, "terminationreason": None},
        ),
    ):
        outcome_path.write_text(raw)
        status = json.loads(original_q)
        status["workload_result"] = claimed
        status["artifact_sha256"]["runexec.txt"] = digest(outcome_path)
        write_json(qpath, status)
        refused(1)
    outcome_path.write_text(original_outcome)
    for filename in ("quiet-before.json", "host-during.json"):
        artifact = candidate / filename
        original_observations = artifact.read_text()
        samples = json.loads(original_observations)
        samples[0]["processes"] = [{"name": "cargo"}]
        write_json(artifact, samples)
        status = json.loads(original_q)
        status["artifact_sha256"][filename] = digest(artifact)
        write_json(qpath, status)
        refused(1)
        artifact.write_text(original_observations)
    qpath.write_text(original_q)
    # Both a source worktree and a separate stored-method checkout must refuse
    # raw output paths, even when the caller supplied a different repository.
    method_repo = container / "method-repo"
    method_repo.mkdir()
    (method_repo / ".git").mkdir()

    def refuse_repo_output(output_path):
        try:
            require_external_output(output_path, repo, method_repo / "scripts")
        except AssertionError:
            return
        raise AssertionError("raw output inside repository admitted")

    refuse_repo_output(repo / "results")
    refuse_repo_output(method_repo / "results")

    lane(6, "baseline")
    lane(6, "candidate", profile="dev")
    refused(6)
    lane(7, "baseline")
    with patch.dict(os.environ, {"RAYON_NUM_THREADS": "123"}):
        lane(7, "candidate")
    refused(7)
    # Even rebinding the receipt hash cannot admit nested or null measurements.
    for case in ("nested", "unavailable", "missing_manifest"):
        receipt = json.loads(original)
        rows = receipt["region_diagnostics"]["regions"]
        if case == "nested":
            rows["root/journal_append/journal_sync"] = {"calls": 1, "inclusive": VALUES.copy()}
        elif case == "unavailable":
            rows["root/journal_sync"]["inclusive"]["fsync_calls"] = None
        else:
            del rows["root/manifest_persistence"]
        write_json(changed, receipt)
        status = json.loads(original_q)
        status["artifact_sha256"][f"run/{RECEIPTS[3]}"] = digest(changed)
        write_json(qpath, status)
        refused(1)
    print(
        "PASS: saved positive completion; runexec/workload/termination/compiler/busy refusal; "
        "incomplete/missing qualification refusal; "
        "artifact tampering, build/resource mismatch, nested/null/missing regions; disjoint sums"
    )

# Real, tiny BenchExec regression: its successful process exit cannot qualify an
# unsuccessful workload. This runs only /bin/sh, with all output outside repo.
with tempfile.TemporaryDirectory(prefix="gf1624-runexec-probe-") as probe:
    for exit_value in (0, 7):
        result = subprocess.run(
            [
                "runexec",
                "--no-container",
                "--output",
                str(Path(probe) / f"exit{exit_value}.log"),
                "--cores",
                "0-15",
                "--",
                "/bin/sh",
                "-c",
                f"exit {exit_value}",
            ],
            cwd=probe,
            text=True,
            capture_output=True,
            check=True,
        )
        summary = Path(probe) / f"exit{exit_value}.txt"
        summary.write_text(result.stdout)
        outcome = runexec_result(summary)
        assert result.returncode == 0 and outcome["returnvalue"] == exit_value
        if exit_value == 0:
            successful_workload(outcome)
        else:
            try:
                successful_workload(outcome)
            except AssertionError:
                pass
            else:
                raise AssertionError("failed real workload admitted")
print("PASS: real runexec process exit 0 with workload exit 7 refused; workload exit 0 admitted")
