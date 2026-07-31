#!/usr/bin/env python3
"""Mutation tests for the deterministic concurrency/recovery ledger gate."""

from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts/ci/concurrency-recovery-gate.py"
SPEC = importlib.util.spec_from_file_location("concurrency_recovery_gate", SCRIPT)
if SPEC is None or SPEC.loader is None:
    raise SystemExit("cannot load concurrency recovery gate")
GATE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GATE)


def run_matrix(matrix: dict[str, object]) -> dict[str, object]:
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory, "matrix.json")
        path.write_text(json.dumps(matrix), encoding="utf-8")
        original = GATE.MATRIX_PATH
        GATE.MATRIX_PATH = path
        try:
            return GATE.validate_matrix()
        finally:
            GATE.MATRIX_PATH = original


def reject(matrix: dict[str, object]) -> None:
    try:
        run_matrix(matrix)
    except GATE.GateError:
        return
    raise AssertionError("invalid concurrency matrix was accepted")


def reject_scope(scope: str, strings: object = None) -> None:
    try:
        GATE.validate_scope(scope, strings or ["phase=synthetic"], "synthetic", "channel")
    except GATE.GateError:
        return
    raise AssertionError("invalid scoped evidence was accepted")


def reject_subprocess_scope(scope: str) -> None:
    try:
        GATE.validate_scope(scope, ["phase=child"], "synthetic", "subprocess-ipc")
    except GATE.GateError:
        return
    raise AssertionError("invalid subprocess evidence was accepted")


def main() -> None:
    matrix = GATE.validate_matrix()
    assert len(matrix["criteria"]) == 10
    assert len(matrix["evidence"]) == 10

    omitted = copy.deepcopy(matrix)
    omitted["criteria"].pop()
    reject(omitted)

    duplicate_criterion = copy.deepcopy(matrix)
    duplicate_criterion["criteria"][1]["id"] = duplicate_criterion["criteria"][0]["id"]
    reject(duplicate_criterion)

    stale_symbol = copy.deepcopy(matrix)
    stale_symbol["evidence"][0]["symbol"] = "absent_concurrency_test"
    reject(stale_symbol)

    ignored = copy.deepcopy(matrix)
    ignored["evidence"][0]["symbol"] = "ignored_fixture"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".rs", dir=ROOT, delete=False) as file:
        file.write(
            "#[test]\n#[ignore]\nfn ignored_fixture() "
            '{ panic!("phase=ignored"); }\nconst DEADLINE: u8 = 1;\n'
        )
        fixture = Path(file.name)
    try:
        ignored["evidence"][0]["path"] = str(fixture.relative_to(ROOT))
        reject(ignored)
    finally:
        fixture.unlink()

    sleeping = copy.deepcopy(matrix)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".rs", dir=ROOT, delete=False) as file:
        file.write(
            "const DEADLINE: u8 = 1;\n#[test]\nfn timed() "
            '{ std::thread::sleep(x); panic!("phase=timed"); }\n'
        )
        fixture = Path(file.name)
    try:
        sleeping["evidence"][0]["path"] = str(fixture.relative_to(ROOT))
        sleeping["evidence"][0]["symbol"] = "timed"
        reject(sleeping)
    finally:
        fixture.unlink()

    unbounded = copy.deepcopy(matrix)
    unbounded["evidence"][0]["deadline"] = "unbounded"
    reject(unbounded)

    incomplete_validation = copy.deepcopy(matrix)
    incomplete_validation["validation"]["commands"].pop()
    reject(incomplete_validation)

    duplicate = copy.deepcopy(matrix)
    duplicate["criteria"][0]["evidence"].append(duplicate["criteria"][0]["evidence"][0])
    reject(duplicate)

    swapped = copy.deepcopy(matrix)
    swapped["criteria"][1]["evidence"] = ["composite-kill-reopen"]
    reject(swapped)

    fake_coordination = copy.deepcopy(matrix)
    fake_coordination["evidence"][2]["coordination"] = "failpoint-subprocess"
    reject(fake_coordination)

    duplicate_executable = copy.deepcopy(matrix)
    clone = copy.deepcopy(duplicate_executable["evidence"][0])
    clone["id"] = "duplicate-source"
    duplicate_executable["evidence"].append(clone)
    reject(duplicate_executable)

    unsafe_path = copy.deepcopy(matrix)
    unsafe_path["evidence"][0]["path"] = "../outside.rs"
    reject(unsafe_path)

    missing_slice = copy.deepcopy(matrix)
    missing_slice["evidence"][0]["slice"] = "cancellation"
    reject(missing_slice)

    valid_scope = "fn cited() { let _ = DEADLINE; sync_channel(1); recv_timeout(); }"
    GATE.validate_scope(valid_scope, ["phase=cited"], "synthetic", "channel")
    reject_scope("fn cited() { sync_channel(1); recv_timeout(); }")
    reject_scope(valid_scope, ["not a diagnostic"])
    reject_scope("fn cited() { let _ = DEADLINE; }")
    reject_scope(valid_scope.replace("recv_timeout();", "recv_timeout(); retry();"))
    reject_scope(valid_scope.replace("recv_timeout();", "recv_timeout(); std::thread::sleep(x);"))
    reject_scope(valid_scope.replace("recv_timeout();", "recv();"))
    reject_scope(valid_scope + " loop { execute_again(); }")

    spoofed = r"""// DEADLINE recv_timeout phase=comment
#[test]
fn cited() {
    let fake = "{ DEADLINE recv_timeout phase=string }";
    let raw = r###"} retry recv_timeout phase=raw"###;
    /* { nested /* } */ retry } */
    sync_channel(1);
}
const DEADLINE: u8 = 1;
"""
    scope, strings = GATE.function_scope(spoofed, "cited", require_test=True)
    assert scope.count("{") == scope.count("}")
    reject_scope(scope, strings)

    lifetime_and_char = r"""
#[test]
fn cited<'a, 'b>() {
    let _borrow: &'a str = "phase=lifetime";
    let _brace = '}';
    let _escaped = '\\'';
    let _ = DEADLINE;
    sync_channel(1);
    recv_timeout();
}
"""
    cleaned, _ = GATE.lex_rust(lifetime_and_char)
    assert "<'a, 'b>" in cleaned
    assert "'}'" not in cleaned
    scope, strings = GATE.function_scope(lifetime_and_char, "cited", require_test=True)
    assert scope.count("{") == scope.count("}")
    GATE.validate_scope(scope, strings, "synthetic", "channel")

    bounded_child = """
        let _ = DEADLINE; Command::new(x); marker(); kill(); reaped = true;
        loop { try_wait(); assert!(Instant::now() < deadline); }
    """
    GATE.validate_scope(bounded_child, ["phase=child"], "synthetic", "subprocess-ipc")
    reject_subprocess_scope(
        "let _ = DEADLINE; Command::new(x); marker(); child.wait(); reaped = true; kill();"
    )
    reject_subprocess_scope(
        "let _ = DEADLINE; Command::new(x); marker(); loop { try_wait(); } reaped = true; kill();"
    )

    with tempfile.TemporaryDirectory() as directory:
        original_popen = GATE.subprocess.Popen
        original_killpg = GATE.os.killpg
        original_checkout_sha = GATE.checkout_sha
        killed: list[tuple[int, int]] = []

        class TimedOutProcess:
            pid = 4242

            def __init__(self, argv: list[str], **kwargs: object) -> None:
                assert kwargs["start_new_session"] is True
                self.argv = argv
                self.waits = 0

            def wait(self, timeout: int | None = None) -> int:
                self.waits += 1
                if self.waits == 1:
                    assert timeout == GATE.COMMAND_TIMEOUT_SECONDS
                    raise subprocess.TimeoutExpired(self.argv, timeout)
                assert timeout is None
                return -9

        GATE.subprocess.Popen = TimedOutProcess
        GATE.os.killpg = lambda pid, sig: killed.append((pid, sig))
        GATE.checkout_sha = lambda: "0" * 40
        try:
            timeout_output = Path(directory)
            assert GATE.run_commands(timeout_output) == 1
        finally:
            GATE.subprocess.Popen = original_popen
            GATE.os.killpg = original_killpg
            GATE.checkout_sha = original_checkout_sha
        timeout_execution = json.loads((timeout_output / "execution.json").read_text())
        assert len(timeout_execution["commands"]) == 1
        assert timeout_execution["commands"][0]["exit_code"] == GATE.TIMEOUT_EXIT_CODE
        assert killed == [(4242, GATE.signal.SIGKILL)]
        assert "command timed out after 900s" in (timeout_output / "format.log").read_text()

    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        fragments = root / "fragments"
        fragments.mkdir()
        results = []
        for command in GATE.REQUIRED_COMMANDS:
            log = fragments / f"{command['id']}.log"
            log.write_text("verified\n", encoding="utf-8")
            result = {
                "id": command["id"],
                "argv": command["argv"],
                "exit_code": 0,
                "duration_ms": 1,
                "log": log.name,
                "log_sha256": GATE.sha256(log),
            }
            if command["id"] in GATE.REQUIRED_EVIDENCE:
                result["test_result"] = {
                    "evidence_id": command["id"],
                    "name": command["argv"][5],
                    "outcome": "ok",
                    "duration_ms": 1,
                }
            results.append(result)
        sha = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True, capture_output=True, check=True
        ).stdout.strip()
        execution = {
            "schema_version": 1,
            "commit_sha": sha,
            "matrix_sha256": GATE.sha256(GATE.MATRIX_PATH),
            "commands": results,
            "test_evidence": [f"{item['path']}::{item['symbol']}" for item in matrix["evidence"]],
        }
        execution_path = fragments / "execution.json"
        execution_path.write_text(json.dumps(execution), encoding="utf-8")
        ci_path = root / "ci.json"
        ci_path.write_text(
            json.dumps(
                {
                    "repository": "CurateLabs/graphforge",
                    "run_id": 101,
                    "job_id": 202,
                }
            ),
            encoding="utf-8",
        )
        run_payload = {
            "id": 101,
            "head_sha": sha,
            "name": "Test Suite",
            "conclusion": "success",
            "html_url": "https://github.com/CurateLabs/graphforge/actions/runs/101",
            "repository": {"full_name": "CurateLabs/graphforge"},
        }
        jobs_payload = {
            "jobs": [
                {
                    "id": 202,
                    "name": "CI Gate",
                    "conclusion": "success",
                    "html_url": (
                        "https://github.com/CurateLabs/graphforge/actions/runs/101/job/202"
                    ),
                }
            ]
        }

        api_paths: list[str] = []

        def fake_api(path: str) -> dict[str, object]:
            api_paths.append(path)
            return jobs_payload if "/jobs?" in path else run_payload

        output = root / "report"
        GATE.build_report(sha, fragments, ci_path, output, fake_api)
        report = json.loads((output / "concurrency-recovery-report.json").read_text())
        assert report["commit_sha"] == sha
        assert report["summary"] == {"criteria": 10, "tests": 10, "passed": True}
        assert any(path.endswith("/jobs?per_page=100") for path in api_paths)
        GATE.verify_report(output, fragments, ci_path, fake_api)

        report_path = output / "concurrency-recovery-report.json"
        original_report = report_path.read_text(encoding="utf-8")
        report_path.write_text(original_report + " ", encoding="utf-8")
        try:
            GATE.verify_report(output, fragments, ci_path, fake_api)
        except GATE.GateError:
            pass
        else:
            raise AssertionError("report verification accepted report tampering")
        report_path.write_text(original_report, encoding="utf-8")

        coordinated = json.loads(original_report)
        coordinated["commands"][0]["argv"] = ["cargo", "test"]
        report_path.write_text(json.dumps(coordinated), encoding="utf-8")
        (output / "concurrency-recovery-report.sha256").write_text(
            f"{GATE.sha256(report_path)}  {report_path.name}\n", encoding="utf-8"
        )
        try:
            GATE.verify_report(output, fragments, ci_path, fake_api)
        except GATE.GateError:
            pass
        else:
            raise AssertionError("report verification accepted report and sidecar tampering")
        GATE.build_report(sha, fragments, ci_path, output, fake_api)

        dynamic = json.loads(report_path.read_text(encoding="utf-8"))
        dynamic["commands"][0]["log_sha256"] = "0" * 64
        dynamic["commands"][0]["duration_ms"] = 99
        dynamic["execution_sha256"] = "1" * 64
        dynamic["ci_evidence_sha256"] = "2" * 64
        report_path.write_text(json.dumps(dynamic), encoding="utf-8")
        (output / "concurrency-recovery-report.sha256").write_text(
            f"{GATE.sha256(report_path)}  {report_path.name}\n", encoding="utf-8"
        )
        try:
            GATE.verify_report(output, fragments, ci_path, fake_api)
        except GATE.GateError:
            pass
        else:
            raise AssertionError("report verification accepted coordinated dynamic tampering")
        GATE.build_report(sha, fragments, ci_path, output, fake_api)

        for field, value in (("commit_sha", "0" * 40), ("matrix_sha256", "0" * 64)):
            stale_execution = copy.deepcopy(execution)
            stale_execution[field] = value
            execution_path.write_text(json.dumps(stale_execution), encoding="utf-8")
            stale_report = json.loads(report_path.read_text(encoding="utf-8"))
            stale_report["execution_sha256"] = GATE.sha256(execution_path)
            report_path.write_text(json.dumps(stale_report), encoding="utf-8")
            (output / "concurrency-recovery-report.sha256").write_text(
                f"{GATE.sha256(report_path)}  {report_path.name}\n", encoding="utf-8"
            )
            try:
                GATE.verify_report(output, fragments, ci_path, fake_api)
            except GATE.GateError:
                pass
            else:
                raise AssertionError(f"report accepted stale execution {field}")
            execution_path.write_text(json.dumps(execution), encoding="utf-8")
            GATE.build_report(sha, fragments, ci_path, output, fake_api)

        def reject_execution(mutated: dict[str, object]) -> None:
            execution_path.write_text(json.dumps(mutated), encoding="utf-8")
            try:
                GATE.build_report(sha, fragments, ci_path, output, fake_api)
            except GATE.GateError:
                return
            raise AssertionError("report accepted tampered individual test results")

        missing_result = copy.deepcopy(execution)
        del missing_result["commands"][2]["test_result"]
        reject_execution(missing_result)
        duplicate_result = copy.deepcopy(execution)
        duplicate_result["commands"][3]["test_result"]["evidence_id"] = "same-instance"
        reject_execution(duplicate_result)
        unknown_result = copy.deepcopy(execution)
        unknown_result["commands"][2]["test_result"]["evidence_id"] = "unknown"
        reject_execution(unknown_result)
        failed_result = copy.deepcopy(execution)
        failed_result["commands"][2]["test_result"]["outcome"] = "FAILED"
        reject_execution(failed_result)
        duration_tamper = copy.deepcopy(execution)
        duration_tamper["commands"][2]["test_result"]["duration_ms"] = 2
        reject_execution(duration_tamper)
        extra_result = copy.deepcopy(execution)
        extra_result["commands"].append(copy.deepcopy(extra_result["commands"][2]))
        reject_execution(extra_result)
        traversal_log = copy.deepcopy(execution)
        traversal_log["commands"][0]["log"] = "../format.log"
        reject_execution(traversal_log)
        absolute_log = copy.deepcopy(execution)
        absolute_log["commands"][0]["log"] = str((fragments / "format.log").resolve())
        reject_execution(absolute_log)
        stale = copy.deepcopy(execution)
        stale["commands"][0]["argv"] = ["cargo", "test"]
        execution_path.write_text(json.dumps(stale), encoding="utf-8")
        try:
            GATE.build_report(sha, fragments, ci_path, output, fake_api)
        except GATE.GateError:
            pass
        else:
            raise AssertionError("report accepted command argv tampering")
        execution_path.write_text(json.dumps(execution), encoding="utf-8")
        (fragments / "format.log").write_text("tampered\n", encoding="utf-8")
        try:
            GATE.build_report(sha, fragments, ci_path, output, fake_api)
        except GATE.GateError:
            pass
        else:
            raise AssertionError("report accepted command log tampering")
        (fragments / "format.log").write_text("verified\n", encoding="utf-8")
        tampered_ci = json.loads(ci_path.read_text(encoding="utf-8"))
        tampered_ci["repository"] = "Other/repository"
        ci_path.write_text(json.dumps(tampered_ci), encoding="utf-8")
        try:
            GATE.build_report(sha, fragments, ci_path, output, fake_api)
        except GATE.GateError:
            pass
        else:
            raise AssertionError("report accepted CI repository tampering")
        ci_path.write_text(
            json.dumps(
                {
                    "repository": "CurateLabs/graphforge",
                    "run_id": 101,
                    "job_id": 202,
                }
            ),
            encoding="utf-8",
        )
        wrong_run = {"repository": "CurateLabs/graphforge", "run_id": 999, "job_id": 202}
        ci_path.write_text(json.dumps(wrong_run), encoding="utf-8")
        try:
            GATE.build_report(sha, fragments, ci_path, output, fake_api)
        except GATE.GateError:
            pass
        else:
            raise AssertionError("report accepted a mismatched immutable run ID")
        wrong_job = {"repository": "CurateLabs/graphforge", "run_id": 101, "job_id": 999}
        ci_path.write_text(json.dumps(wrong_job), encoding="utf-8")
        try:
            GATE.build_report(sha, fragments, ci_path, output, fake_api)
        except GATE.GateError:
            pass
        else:
            raise AssertionError("report accepted a mismatched immutable job ID")
        ci_path.write_text(
            json.dumps(
                {
                    "repository": "CurateLabs/graphforge",
                    "run_id": 101,
                    "job_id": 202,
                }
            ),
            encoding="utf-8",
        )
        original_head = run_payload["head_sha"]
        run_payload["head_sha"] = "0" * 40
        try:
            GATE.build_report(sha, fragments, ci_path, output, fake_api)
        except GATE.GateError:
            pass
        else:
            raise AssertionError("report accepted live CI head SHA tampering")
        run_payload["head_sha"] = original_head
        try:
            GATE.build_report("0" * 40, fragments, ci_path, output, fake_api)
        except GATE.GateError:
            pass
        else:
            raise AssertionError("report accepted a SHA different from checkout HEAD")

    print("concurrency recovery gate mutation tests passed")


if __name__ == "__main__":
    main()
