#!/usr/bin/env python3
"""Validate the finite deterministic concurrency and recovery evidence ledger."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
REPOSITORY = "CurateLabs/graphforge"
MATRIX_PATH = ROOT / "tests/contracts/concurrency-recovery-matrix.json"
REQUIRED_SLICES = {
    "same-process": 2541,
    "cancellation": 2542,
    "shared-directory": 2543,
    "multi-process": 2544,
    "composite-recovery": 2545,
}
REQUIRED_CRITERIA = {
    "deterministic-coordination",
    "canonical-concurrent-reads",
    "cancellation-isolation",
    "writer-busy-before-publication",
    "complete-supported-generation",
    "previous-or-new-after-kill",
    "long-lived-reader-visibility",
    "bounded-phase-diagnostics",
    "no-ignore-or-retry",
    "required-validation",
}
REQUIRED_MAPPING = {
    "deterministic-coordination": {
        "same-instance",
        "stream-drop",
        "shared-read-write",
        "killed-writer",
        "composite-kill-reopen",
    },
    "canonical-concurrent-reads": {"same-instance", "cross-session"},
    "cancellation-isolation": {"stream-drop", "token-cancellation"},
    "writer-busy-before-publication": {"competing-writer", "killed-writer"},
    "complete-supported-generation": {"shared-read-write", "published-child"},
    "previous-or-new-after-kill": {"killed-writer", "composite-kill-reopen"},
    "long-lived-reader-visibility": {"shared-read-write", "published-child"},
    "bounded-phase-diagnostics": {
        "nested-runtime",
        "token-cancellation",
        "published-child",
        "composite-kill-reopen",
    },
    "no-ignore-or-retry": {
        "cross-session",
        "stream-drop",
        "competing-writer",
        "composite-kill-reopen",
    },
}
COORDINATION = {"barrier-channel", "channel", "subprocess-ipc", "failpoint-subprocess"}
REQUIRED_EVIDENCE = {
    "same-instance": (
        "same-process",
        "crates/gf-api/src/same_process_concurrency_tests.rs",
        "independent_instances_and_one_instance_reads_are_deterministic",
        ["run_workers"],
        "barrier-channel",
    ),
    "cross-session": (
        "same-process",
        "crates/gf-api/src/same_process_concurrency_tests.rs",
        "cross_session_reads_are_canonically_equal",
        ["run_workers"],
        "barrier-channel",
    ),
    "nested-runtime": (
        "same-process",
        "crates/gf-api/src/same_process_concurrency_tests.rs",
        "synchronous_calls_complete_inside_existing_tokio_runtime",
        [],
        "channel",
    ),
    "stream-drop": (
        "cancellation",
        "crates/gf-api/src/stream_cancellation_isolation_tests.rs",
        "early_stream_drop_does_not_truncate_concurrent_peer",
        ["recv"],
        "channel",
    ),
    "token-cancellation": (
        "cancellation",
        "crates/gf-api/src/stream_cancellation_isolation_tests.rs",
        "cooperative_token_cancellation_does_not_cancel_concurrent_peer",
        ["recv"],
        "channel",
    ),
    "shared-read-write": (
        "shared-directory",
        "crates/gf-api/src/shared_directory_semantics_tests.rs",
        "shared_directory_reads_pin_complete_generations_and_reopen_sees_commit",
        ["recv"],
        "barrier-channel",
    ),
    "competing-writer": (
        "shared-directory",
        "crates/gf-api/src/shared_directory_semantics_tests.rs",
        "competing_writer_fails_before_its_staging_or_publication",
        ["recv"],
        "barrier-channel",
    ),
    "killed-writer": (
        "multi-process",
        "crates/gf-api/src/multi_process_publication_tests.rs",
        "killed_staged_child_releases_lock_and_recovers_without_partial_generation",
        ["spawn", "marker_before", "kill", "wait", "drop"],
        "subprocess-ipc",
    ),
    "published-child": (
        "multi-process",
        "crates/gf-api/src/multi_process_publication_tests.rs",
        "published_child_is_visible_only_to_fresh_current_reader",
        ["spawn", "marker", "marker_before", "wait", "drop"],
        "subprocess-ipc",
    ),
    "composite-kill-reopen": (
        "composite-recovery",
        "crates/gf-api/src/composite_recovery_tests.rs",
        "composite_kill_reopen_matrix_never_exposes_mixed_state",
        ["spawn", "wait", "drop", "verify_case"],
        "failpoint-subprocess",
    ),
}
QUALITY_COMMANDS = [
    {"id": "format", "argv": ["cargo", "fmt", "--all", "--", "--check"]},
    {"id": "clippy", "argv": ["cargo", "clippy", "--workspace", "--", "-D", "warnings"]},
]
MODULES = {
    "same-instance": "same_process_concurrency_tests",
    "cross-session": "same_process_concurrency_tests",
    "nested-runtime": "same_process_concurrency_tests",
    "stream-drop": "stream_cancellation_isolation_tests",
    "token-cancellation": "stream_cancellation_isolation_tests",
    "shared-read-write": "shared_directory_semantics_tests",
    "competing-writer": "shared_directory_semantics_tests",
    "killed-writer": "multi_process_publication_tests",
    "published-child": "multi_process_publication_tests",
    "composite-kill-reopen": "composite_recovery_tests",
}
TEST_COMMANDS = [
    {
        "id": evidence_id,
        "argv": [
            "cargo",
            "test",
            "-p",
            "gf-api",
            "--lib",
            f"{MODULES[evidence_id]}::{REQUIRED_EVIDENCE[evidence_id][2]}",
            "--",
            "--exact",
        ],
    }
    for evidence_id in REQUIRED_EVIDENCE
]
REQUIRED_COMMANDS = QUALITY_COMMANDS + TEST_COMMANDS
COMMAND_TIMEOUT_SECONDS = 900
TIMEOUT_EXIT_CODE = 124
REQUIRED_VALIDATION = {
    "commands": REQUIRED_COMMANDS,
    "required_ci_job": "Test Suite / CI Gate",
}


class GateError(RuntimeError):
    """A deterministic concurrency-ledger validation failure."""


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def checkout_sha() -> str:
    sha = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True, capture_output=True, check=True
    ).stdout.strip()
    if not re.fullmatch(r"[0-9a-f]{40}", sha):
        raise GateError("checkout HEAD must be a full lowercase commit SHA")
    return sha


def load_matrix() -> dict[str, Any]:
    try:
        value = json.loads(MATRIX_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise GateError(f"cannot read concurrency matrix: {error}") from error
    if not isinstance(value, dict):
        raise GateError("matrix root must be an object")
    return value


def source_for(value: object) -> tuple[Path, str]:
    if not isinstance(value, str) or not value or Path(value).is_absolute():
        raise GateError(f"unsafe evidence path: {value!r}")
    relative = Path(value)
    if ".." in relative.parts:
        raise GateError(f"unsafe evidence path: {value!r}")
    path = ROOT / relative
    if not path.is_file():
        raise GateError(f"evidence source does not exist: {value}")
    return path, path.read_text(encoding="utf-8")


def lex_rust(source: str) -> tuple[str, list[str]]:
    """Blank Rust comments/literals while preserving offsets; return string contents."""
    code = list(source)
    strings: list[str] = []
    index = 0
    length = len(source)

    def blank(start: int, end: int) -> None:
        for position in range(start, end):
            if code[position] != "\n":
                code[position] = " "

    while index < length:
        if source.startswith("//", index):
            end = source.find("\n", index + 2)
            end = length if end < 0 else end
            blank(index, end)
            index = end
            continue
        if source.startswith("/*", index):
            depth = 1
            end = index + 2
            while end < length and depth:
                if source.startswith("/*", end):
                    depth += 1
                    end += 2
                elif source.startswith("*/", end):
                    depth -= 1
                    end += 2
                else:
                    end += 1
            blank(index, end)
            index = end
            continue
        raw = re.match(r"(?:br|r)(?P<hashes>#{0,16})\"", source[index:])
        if raw:
            hashes = raw.group("hashes")
            content_start = index + raw.end()
            terminator = '"' + hashes
            end_content = source.find(terminator, content_start)
            end_content = length if end_content < 0 else end_content
            strings.append(source[content_start:end_content])
            end = length if end_content == length else end_content + len(terminator)
            blank(index, end)
            index = end
            continue
        prefix = 1 if source.startswith('b"', index) else 0
        if source[index + prefix : index + prefix + 1] == '"':
            content_start = index + prefix + 1
            end = content_start
            escaped = False
            while end < length:
                character = source[end]
                if escaped:
                    escaped = False
                elif character == "\\":
                    escaped = True
                elif character == '"':
                    break
                end += 1
            strings.append(source[content_start:end])
            end = min(length, end + 1)
            blank(index, end)
            index = end
            continue
        if source[index] == "'":
            character = re.match(
                r"'(?:\\(?:x[0-9A-Fa-f]{2}|u\{[0-9A-Fa-f_]{1,6}\}|.)|[^\\'\n])'",
                source[index:],
            )
            if character is not None:
                end = index + character.end()
                blank(index, end)
                index = end
            else:
                index += 1
            continue
        index += 1
    return "".join(code), strings


def function_scope(source: str, symbol: str, *, require_test: bool) -> tuple[str, list[str]]:
    cleaned, _ = lex_rust(source)
    match = re.search(
        rf"(?P<attrs>(?:#\[[^\]]+\]\s*)*)(?:pub\s+)?(?:async\s+)?fn\s+{re.escape(symbol)}(?:<[^>]+>)?\s*\(",
        cleaned,
    )
    if match is None:
        raise GateError(f"executable function is absent: {symbol}")
    attributes = source[match.start("attrs") : match.end("attrs")]
    if require_test and "#[test]" not in attributes:
        raise GateError(f"executable Rust test is absent: {symbol}")
    if "#[ignore" in attributes:
        raise GateError(f"ignored function cannot prove closure: {symbol}")
    opening = cleaned.find("{", match.end())
    if opening < 0:
        raise GateError(f"function has no body: {symbol}")
    depth = 0
    for index in range(opening, len(cleaned)):
        if cleaned[index] == "{":
            depth += 1
        elif cleaned[index] == "}":
            depth -= 1
            if depth == 0:
                original_scope = attributes + source[opening : index + 1]
                scoped_code, strings = lex_rust(original_scope)
                return scoped_code, strings
    raise GateError(f"function has no closing brace: {symbol}")


def validate_scope(scope: str, strings: list[str], evidence_id: str, coordination: str) -> None:
    if re.search(r"\b(?:thread::sleep|tokio::time::sleep)\s*\(", scope):
        raise GateError(f"{evidence_id}: timing sleep found in evidence scope")
    if re.search(r"\bretr(?:y|ies|ied|ying)\b", scope, re.IGNORECASE):
        raise GateError(f"{evidence_id}: retry-to-green cannot prove closure")
    if re.search(r"\bwhile\s+true\b", scope) or (
        re.search(r"\bloop\s*\{", scope) and "deadline" not in scope
    ):
        raise GateError(f"{evidence_id}: unbounded loop or re-execution is forbidden")
    if re.search(r"\.recv\s*\(", scope):
        raise GateError(f"{evidence_id}: blocking channel receive is forbidden")
    if "DEADLINE" not in scope:
        raise GateError(f"{evidence_id}: bounded deadline evidence is required")
    if not any("phase=" in value for value in strings):
        raise GateError(f"{evidence_id}: phase diagnostics are required")
    markers = {
        "barrier-channel": ("Barrier", "sync_channel"),
        "channel": ("sync_channel", "recv_timeout"),
        "subprocess-ipc": ("Command", "marker", "try_wait", "Instant::now", "reaped", "kill"),
        "failpoint-subprocess": (
            "Command",
            "failpoint",
            "try_wait",
            "Instant::now",
            "reaped",
            "kill",
        ),
    }[coordination]
    if any(marker not in scope for marker in markers):
        raise GateError(f"{evidence_id}: scoped coordination markers are absent")
    if coordination in {"subprocess-ipc", "failpoint-subprocess"}:
        if re.search(r"\.wait\s*\(", scope) and not re.search(r"\.kill\s*\(", scope):
            raise GateError(f"{evidence_id}: blocking child wait lacks kill cleanup")
        polls = len(re.findall(r"\btry_wait\s*\(", scope))
        deadlines = len(re.findall(r"Instant::now\s*\(\)\s*<\s*deadline", scope))
        if polls == 0 or deadlines == 0:
            raise GateError(f"{evidence_id}: child polling is not explicitly deadline-bounded")


def validate_matrix() -> dict[str, Any]:
    matrix = load_matrix()
    if (
        matrix.get("schema_version") != 1
        or matrix.get("contract") != "graphforge-concurrency-recovery/1"
        or matrix.get("issue") != 2546
        or matrix.get("parent_issue") != 2415
    ):
        raise GateError("matrix must declare concurrency recovery schema 1")
    if matrix.get("slices") != REQUIRED_SLICES:
        raise GateError("matrix must map exactly the five completed delivery slices")
    if matrix.get("validation") != REQUIRED_VALIDATION:
        raise GateError("matrix must declare the exact formatting, Clippy, targeted, and CI gates")

    evidence = matrix.get("evidence")
    if not isinstance(evidence, list) or not evidence:
        raise GateError("evidence must be a non-empty array")
    evidence_by_id: dict[str, dict[str, Any]] = {}
    source_symbols: set[tuple[str, str]] = set()
    represented_slices: set[str] = set()
    for item in evidence:
        if not isinstance(item, dict) or not isinstance(item.get("id"), str):
            raise GateError("every evidence item needs an ID")
        evidence_id = item["id"]
        if evidence_id in evidence_by_id:
            raise GateError(f"duplicate evidence ID: {evidence_id}")
        expected = REQUIRED_EVIDENCE.get(evidence_id)
        if expected is None:
            raise GateError(f"evidence is outside the frozen catalog: {evidence_id}")
        actual_contract = (
            item.get("slice"),
            item.get("path"),
            item.get("symbol"),
            item.get("helpers"),
            item.get("coordination"),
        )
        if actual_contract != expected or item.get("deadline") != "bounded":
            raise GateError(f"{evidence_id}: exact executable evidence contract changed")
        symbol = item.get("symbol")
        if not isinstance(symbol, str) or not symbol:
            raise GateError(f"{evidence_id}: exact test symbol is required")
        path, source = source_for(item.get("path"))
        source_symbol = (str(path.relative_to(ROOT)), symbol)
        if source_symbol in source_symbols:
            raise GateError(f"duplicate executable evidence: {source_symbol}")
        source_symbols.add(source_symbol)
        scope, strings = function_scope(source, symbol, require_test=True)
        helpers = item.get("helpers")
        if not isinstance(helpers, list) or any(not isinstance(helper, str) for helper in helpers):
            raise GateError(f"{evidence_id}: helper scope list is required")
        if len(helpers) != len(set(helpers)):
            raise GateError(f"{evidence_id}: duplicate helper scope")
        for helper in helpers:
            helper_scope, helper_strings = function_scope(source, helper, require_test=False)
            scope += "\n" + helper_scope
            strings.extend(helper_strings)
        if item.get("coordination") not in COORDINATION:
            raise GateError(f"{evidence_id}: explicit coordination is required")
        validate_scope(scope, strings, evidence_id, item["coordination"])
        slice_id = item.get("slice")
        if slice_id not in REQUIRED_SLICES:
            raise GateError(f"{evidence_id}: unknown delivery slice")
        represented_slices.add(slice_id)
        evidence_by_id[evidence_id] = item
    if represented_slices != set(REQUIRED_SLICES):
        raise GateError("evidence omits a required delivery slice")

    criteria = matrix.get("criteria")
    if not isinstance(criteria, list):
        raise GateError("criteria must be an array")
    criterion_ids = [item.get("id") for item in criteria if isinstance(item, dict)]
    if len(criterion_ids) != len(criteria) or set(criterion_ids) != REQUIRED_CRITERIA:
        raise GateError("parent criteria are missing, duplicated, or outside the frozen set")
    for criterion in criteria:
        criterion_id = criterion["id"]
        if criterion_id == "required-validation":
            if criterion.get("validation") != ["format", "clippy", "targeted-tests", "required-ci"]:
                raise GateError("required-validation: exact executable gates are required")
            if "evidence" in criterion:
                raise GateError("required-validation: test evidence cannot replace release gates")
            continue
        refs = criterion.get("evidence")
        if not isinstance(refs, list) or not refs or any(not isinstance(ref, str) for ref in refs):
            raise GateError(f"{criterion_id}: executable evidence is required")
        if len(refs) != len(set(refs)):
            raise GateError(f"{criterion_id}: duplicate evidence reference")
        unknown = set(refs) - set(evidence_by_id)
        if unknown:
            raise GateError(f"{criterion_id}: unknown evidence {sorted(unknown)}")
        if set(refs) != REQUIRED_MAPPING[criterion_id]:
            raise GateError(f"{criterion_id}: exact evidence mapping changed")
    return matrix


def run_commands(output: Path) -> int:
    matrix = validate_matrix()
    output.mkdir(parents=True, exist_ok=True)
    sha = checkout_sha()
    results: list[dict[str, Any]] = []
    for command in matrix["validation"]["commands"]:
        log_path = output / f"{command['id']}.log"
        started = time.monotonic_ns()
        with log_path.open("wb") as log:
            process = subprocess.Popen(
                command["argv"],
                cwd=ROOT,
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            try:
                exit_code = process.wait(timeout=COMMAND_TIMEOUT_SECONDS)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
                log.write(f"\ncommand timed out after {COMMAND_TIMEOUT_SECONDS}s\n".encode())
                exit_code = TIMEOUT_EXIT_CODE
        duration_ms = (time.monotonic_ns() - started) // 1_000_000
        result = {
            "id": command["id"],
            "argv": command["argv"],
            "exit_code": exit_code,
            "duration_ms": duration_ms,
            "log": log_path.name,
            "log_sha256": sha256(log_path),
        }
        if command["id"] in REQUIRED_EVIDENCE:
            test_name = command["argv"][5]
            lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
            outcomes = [
                match.group(1)
                for line in lines
                if (match := re.fullmatch(rf"test {re.escape(test_name)} \.\.\. (ok|FAILED)", line))
            ]
            result["test_result"] = {
                "evidence_id": command["id"],
                "name": test_name,
                "outcome": outcomes[0] if len(outcomes) == 1 else "invalid",
                "duration_ms": duration_ms,
            }
            if outcomes != ["ok"]:
                result["exit_code"] = 1
        results.append(result)
        if result["exit_code"] != 0:
            break
    evidence = [f"{item['path']}::{item['symbol']}" for item in matrix["evidence"]]
    fragment = {
        "schema_version": 1,
        "commit_sha": sha,
        "matrix_sha256": sha256(MATRIX_PATH),
        "commands": results,
        "test_evidence": evidence,
    }
    (output / "execution.json").write_text(
        json.dumps(fragment, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return (
        0
        if len(results) == len(REQUIRED_COMMANDS) and all(r["exit_code"] == 0 for r in results)
        else 1
    )


def github_json(path: str) -> dict[str, Any]:
    completed = subprocess.run(
        ["gh", "api", path], cwd=ROOT, text=True, capture_output=True, check=False
    )
    if completed.returncode != 0:
        raise GateError(f"cannot resolve GitHub CI evidence: {completed.stderr.strip()}")
    try:
        value = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise GateError(f"GitHub CI evidence is not JSON: {error}") from error
    if not isinstance(value, dict):
        raise GateError("GitHub CI evidence root must be an object")
    return value


def resolve_ci_evidence(value: object, sha: str, api_get: Any = github_json) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != {"repository", "run_id", "job_id"}:
        raise GateError("CI evidence must contain only repository, run_id, and job_id")
    repository = value["repository"]
    run_id = value["run_id"]
    job_id = value["job_id"]
    if repository != REPOSITORY or not isinstance(run_id, int) or not isinstance(job_id, int):
        raise GateError("CI evidence repository or immutable IDs are invalid")
    run = api_get(f"repos/{repository}/actions/runs/{run_id}")
    if (
        run.get("id") != run_id
        or run.get("head_sha") != sha
        or run.get("name") != "Test Suite"
        or run.get("conclusion") != "success"
        or run.get("repository", {}).get("full_name") != repository
    ):
        raise GateError("GitHub Test Suite run does not match repository, SHA, or success")
    jobs = api_get(f"repos/{repository}/actions/runs/{run_id}/jobs?per_page=100")
    matching = [job for job in jobs.get("jobs", []) if job.get("id") == job_id]
    if (
        len(matching) != 1
        or matching[0].get("name") != "CI Gate"
        or matching[0].get("conclusion") != "success"
    ):
        raise GateError("GitHub CI Gate job is absent, duplicated, or unsuccessful")
    url_prefix = f"https://github.com/{repository}/actions/runs/"
    if not str(run.get("html_url", "")).startswith(url_prefix) or not str(
        matching[0].get("html_url", "")
    ).startswith(url_prefix):
        raise GateError("GitHub run or job URL does not belong to the required repository")
    return {
        "repository": repository,
        "commit_sha": sha,
        "workflow": "Test Suite",
        "run_id": run_id,
        "run_url": run.get("html_url"),
        "job": "CI Gate",
        "job_id": job_id,
        "job_url": matching[0].get("html_url"),
        "conclusion": "success",
    }


def build_report(
    sha: str,
    fragments: Path,
    ci_evidence: Path,
    output: Path,
    api_get: Any = github_json,
) -> None:
    matrix = validate_matrix()
    if not re.fullmatch(r"[0-9a-f]{40}", sha) or checkout_sha() != sha:
        raise GateError("report SHA must equal checkout HEAD as a full lowercase commit")
    execution_path = fragments / "execution.json"
    if not execution_path.is_file():
        raise GateError("execution fragment is absent")
    execution = json.loads(execution_path.read_text(encoding="utf-8"))
    if (
        execution.get("schema_version") != 1
        or execution.get("commit_sha") != sha
        or execution.get("matrix_sha256") != sha256(MATRIX_PATH)
    ):
        raise GateError("execution fragment schema, commit SHA, or matrix digest changed")
    expected_evidence = [f"{item['path']}::{item['symbol']}" for item in matrix["evidence"]]
    if execution.get("test_evidence") != expected_evidence:
        raise GateError("execution test evidence changed")
    results = execution.get("commands")
    if not isinstance(results, list) or len(results) != len(REQUIRED_COMMANDS):
        raise GateError("execution command results are incomplete")
    for expected, result in zip(REQUIRED_COMMANDS, results, strict=True):
        if result.get("id") != expected["id"] or result.get("argv") != expected["argv"]:
            raise GateError("executed command argv differs from the frozen ledger")
        if (
            result.get("exit_code") != 0
            or not isinstance(result.get("duration_ms"), int)
            or result["duration_ms"] < 0
        ):
            raise GateError(f"{expected['id']}: command did not pass with a measured duration")
        expected_log = f"{expected['id']}.log"
        if result.get("log") != expected_log:
            raise GateError(f"{expected['id']}: command log identity changed")
        log_path = fragments / expected_log
        if not log_path.is_file() or result.get("log_sha256") != sha256(log_path):
            raise GateError(f"{expected['id']}: command log digest does not match")
    test_results = [
        result.get("test_result") for result in results if result["id"] in REQUIRED_EVIDENCE
    ]
    if any(not isinstance(result, dict) for result in test_results):
        raise GateError("individual executable test results are absent")
    observed_ids = [result["evidence_id"] for result in test_results]
    if observed_ids != list(REQUIRED_EVIDENCE) or len(set(observed_ids)) != len(REQUIRED_EVIDENCE):
        raise GateError("individual test results are missing, duplicated, extra, or reordered")
    command_by_id = {result["id"]: result for result in results}
    for expected_command, result in zip(TEST_COMMANDS, test_results, strict=True):
        command_result = command_by_id.get(result.get("evidence_id"))
        if (
            command_result is None
            or result.get("name") != expected_command["argv"][5]
            or result.get("outcome") != "ok"
            or not isinstance(result.get("duration_ms"), int)
            or result["duration_ms"] < 0
            or result["duration_ms"] != command_result["duration_ms"]
        ):
            raise GateError("individual test outcome or duration is invalid")
    try:
        ci = json.loads(ci_evidence.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise GateError(f"cannot read CI evidence: {error}") from error
    resolved_ci = resolve_ci_evidence(ci, sha, api_get)
    report = {
        "gate": "Concurrency and Recovery",
        "schema_version": 1,
        "commit_sha": sha,
        "matrix_sha256": sha256(MATRIX_PATH),
        "execution_sha256": sha256(execution_path),
        "ci_evidence_sha256": sha256(ci_evidence),
        "commands": results,
        "test_results": test_results,
        "test_evidence": expected_evidence,
        "criteria": matrix["criteria"],
        "required_ci": resolved_ci,
        "summary": {
            "criteria": len(matrix["criteria"]),
            "tests": len(expected_evidence),
            "passed": True,
        },
    }
    output.mkdir(parents=True, exist_ok=True)
    report_path = output / "concurrency-recovery-report.json"
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (output / "concurrency-recovery-report.sha256").write_text(
        f"{sha256(report_path)}  {report_path.name}\n", encoding="utf-8"
    )


def verify_report(
    output: Path, fragments: Path, ci_evidence: Path, api_get: Any = github_json
) -> None:
    report_path = output / "concurrency-recovery-report.json"
    digest_path = output / "concurrency-recovery-report.sha256"
    if not report_path.is_file() or not digest_path.is_file():
        raise GateError("report or SHA-256 sidecar is absent")
    parts = digest_path.read_text(encoding="utf-8").strip().split()
    if parts != [sha256(report_path), report_path.name]:
        raise GateError("report SHA-256 sidecar does not match")
    try:
        report = json.loads(report_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise GateError(f"report is not JSON: {error}") from error
    matrix = validate_matrix()
    sha = checkout_sha()
    expected_evidence = [f"{item['path']}::{item['symbol']}" for item in matrix["evidence"]]
    if (
        report.get("gate") != "Concurrency and Recovery"
        or report.get("schema_version") != 1
        or report.get("commit_sha") != sha
        or report.get("matrix_sha256") != sha256(MATRIX_PATH)
        or report.get("criteria") != matrix["criteria"]
        or report.get("test_evidence") != expected_evidence
        or report.get("summary")
        != {"criteria": len(matrix["criteria"]), "tests": len(expected_evidence), "passed": True}
    ):
        raise GateError("report immutable schema, SHA, matrix, criteria, or summary changed")
    for field in ("execution_sha256", "ci_evidence_sha256"):
        if not re.fullmatch(r"[0-9a-f]{64}", str(report.get(field, ""))):
            raise GateError(f"report {field} is not a SHA-256 digest")
    execution_path = fragments / "execution.json"
    if (
        not execution_path.is_file()
        or report["execution_sha256"] != sha256(execution_path)
        or not ci_evidence.is_file()
        or report["ci_evidence_sha256"] != sha256(ci_evidence)
    ):
        raise GateError("report execution or CI fragment digest differs from source evidence")
    execution = json.loads(execution_path.read_text(encoding="utf-8"))
    if (
        execution.get("schema_version") != 1
        or execution.get("commit_sha") != sha
        or execution.get("matrix_sha256") != sha256(MATRIX_PATH)
    ):
        raise GateError("execution fragment schema, commit SHA, or matrix digest changed")
    results = report.get("commands")
    if not isinstance(results, list) or len(results) != len(REQUIRED_COMMANDS):
        raise GateError("report command results are incomplete")
    if execution.get("commands") != results or execution.get("test_evidence") != expected_evidence:
        raise GateError("report command or test evidence differs from execution fragment")
    for expected, result in zip(REQUIRED_COMMANDS, results, strict=True):
        if (
            result.get("id") != expected["id"]
            or result.get("argv") != expected["argv"]
            or result.get("exit_code") != 0
            or result.get("log") != f"{expected['id']}.log"
            or not re.fullmatch(r"[0-9a-f]{64}", str(result.get("log_sha256", "")))
            or not isinstance(result.get("duration_ms"), int)
            or result["duration_ms"] < 0
        ):
            raise GateError("report command identity, outcome, duration, or log digest changed")
        log_path = fragments / f"{expected['id']}.log"
        if not log_path.is_file() or result["log_sha256"] != sha256(log_path):
            raise GateError("report command log digest differs from source log")
    expected_test_results = [result["test_result"] for result in results[len(QUALITY_COMMANDS) :]]
    if report.get("test_results") != expected_test_results:
        raise GateError("report individual test results differ from command evidence")
    command_by_id = {result["id"]: result for result in results}
    for expected, result in zip(TEST_COMMANDS, expected_test_results, strict=True):
        command_result = command_by_id.get(result.get("evidence_id"))
        if (
            command_result is None
            or result.get("evidence_id") != expected["id"]
            or result.get("name") != expected["argv"][5]
            or result.get("outcome") != "ok"
            or result.get("duration_ms") != command_result["duration_ms"]
        ):
            raise GateError("report individual test identity, outcome, or duration changed")
    ci = report.get("required_ci")
    if not isinstance(ci, dict):
        raise GateError("report required CI evidence is absent")
    input_ci = json.loads(ci_evidence.read_text(encoding="utf-8"))
    resolved = resolve_ci_evidence(input_ci, sha, api_get)
    if ci != resolved:
        raise GateError("report required CI evidence differs from live GitHub state")


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("validate")
    run = subparsers.add_parser("run")
    run.add_argument("--output", type=Path, required=True)
    report = subparsers.add_parser("report")
    report.add_argument("--sha", required=True)
    report.add_argument("--fragments", type=Path, required=True)
    report.add_argument("--ci-evidence", type=Path, required=True)
    report.add_argument("--output", type=Path, required=True)
    verify = subparsers.add_parser("verify-report")
    verify.add_argument("--output", type=Path, required=True)
    verify.add_argument("--fragments", type=Path, required=True)
    verify.add_argument("--ci-evidence", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == "run":
            return run_commands(args.output)
        if args.command == "report":
            build_report(args.sha, args.fragments, args.ci_evidence, args.output)
            return 0
        if args.command == "verify-report":
            verify_report(args.output, args.fragments, args.ci_evidence)
            return 0
        matrix = validate_matrix()
    except GateError as error:
        print(f"concurrency recovery gate failed: {error}", file=sys.stderr)
        return 1
    print(
        "concurrency recovery matrix valid: "
        f"{len(matrix['criteria'])} criteria, {len(matrix['evidence'])} tests"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
