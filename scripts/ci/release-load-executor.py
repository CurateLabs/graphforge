#!/usr/bin/env python3
"""Repository-owned native executor for one standardized load-matrix case."""

from __future__ import annotations

import argparse
from contextlib import suppress
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import resource
import signal
import subprocess
import sys
import tempfile
import time
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
SURFACE = ROOT / "tests/contracts/non-cypher-rust-surface.json"
ADAPTER = Path(__file__).resolve()


def load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path}: expected JSON object")
    return value


def canonical(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def digest_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def digest_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True)
class CommandResult:
    output: bytes
    peak_rss_bytes: int


def run_command(
    command: list[str], *, timeout: int, env: dict[str, str] | None = None
) -> CommandResult:
    """Run one process group and return resource usage for exactly that child."""
    with tempfile.TemporaryFile() as captured:
        process = subprocess.Popen(
            command,
            cwd=ROOT,
            env=env,
            stdout=captured,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        deadline = time.monotonic() + timeout
        usage: resource.struct_rusage | None = None
        status: int | None = None
        while status is None:
            child, child_status, child_usage = os.wait4(process.pid, os.WNOHANG)
            if child:
                status, usage = child_status, child_usage
                break
            if time.monotonic() >= deadline:
                os.killpg(process.pid, signal.SIGTERM)
                grace_deadline = time.monotonic() + 2
                child = 0
                while time.monotonic() < grace_deadline:
                    if not child:
                        child, child_status, child_usage = os.wait4(process.pid, os.WNOHANG)
                    time.sleep(0.01)
                with suppress(ProcessLookupError):
                    os.killpg(process.pid, signal.SIGKILL)
                if not child:
                    child, child_status, child_usage = os.wait4(process.pid, 0)
                process.returncode = os.waitstatus_to_exitcode(child_status)
                raise ValueError(f"native command timed out after {timeout}s")
            time.sleep(0.01)
        process.returncode = os.waitstatus_to_exitcode(status)
        captured.seek(0)
        output = captured.read()
    if process.returncode != 0:
        tail = output.decode("utf-8", errors="replace")[-2000:]
        raise ValueError(f"native command failed ({process.returncode}): {tail}")
    assert usage is not None
    rss_scale = 1 if sys.platform == "darwin" else 1024
    return CommandResult(output=output, peak_rss_bytes=int(usage.ru_maxrss) * rss_scale)


def rust_target_dir() -> Path:
    configured = os.environ.get("CARGO_TARGET_DIR")
    return Path(configured).resolve() if configured else ROOT / "target"


def default_preflight(language: str) -> tuple[list[str], dict[str, str] | None]:
    if language == "rust":
        return (["cargo", "test", "-p", "graphforge-api"], None)
    if language == "python":
        return (
            [sys.executable, str(ROOT / "scripts/ci/run-python-binding-contract.py")],
            None,
        )
    return (["pnpm", "--filter", "@curatelabs/graphforge", "test"], None)


def complete_inventory() -> list[str]:
    surface = load(SURFACE)
    values = {
        identity
        for group in surface["method_evidence_groups"].values()
        for identity in group["ids"]
    }
    values.update(surface["m18_registry"]["release-tested"]["ids"])
    values.update(surface["m19_contracts"]["release-tested"]["ids"])
    return sorted(values)


def default_probe(language: str, *, preflight_timeout: int) -> tuple[list[str], Path]:
    if language == "rust":
        executable = rust_target_dir() / "release/examples/release_load_probe"
        if sys.platform == "win32":
            executable = executable.with_suffix(".exe")
        run_command(
            [
                "cargo",
                "build",
                "--release",
                "-p",
                "graphforge-api",
                "--example",
                "release_load_probe",
            ],
            timeout=preflight_timeout,
        )
        return ([str(executable)], executable)
    if language == "python":
        script = ROOT / "scripts/ci/release-load-python-probe.py"
        extension = subprocess.check_output(
            [
                sys.executable,
                "-c",
                "import graphforge._graphforge_rs as m; print(m.__file__)",
            ],
            cwd=ROOT,
            text=True,
        ).strip()
        return ([sys.executable, str(script)], Path(extension))
    script = ROOT / "crates/graphforge-bindings-node/tests/release-load-probe.mjs"
    addons = sorted((ROOT / "crates/graphforge-bindings-node").glob("*.node"))
    if len(addons) != 1:
        raise ValueError(f"Node executor requires exactly one freshly built addon, found {addons}")
    return (["node", str(script)], addons[0])


def preflight(
    language: str,
    source_sha: str,
    artifact: Path,
    inventory_sha: str,
    cache: Path,
    command: list[str],
    probe_path: Path,
    timeout: int,
) -> dict[str, Any]:
    expected = {
        "schema": "graphforge-load-preflight/1",
        "language": language,
        "source_sha": source_sha,
        "artifact_sha256": digest_file(artifact),
        "inventory_sha256": inventory_sha,
        "surface_manifest_sha256": digest_file(SURFACE),
        "adapter_sha256": digest_file(ADAPTER),
        "probe_sha256": digest_file(probe_path),
        "command_sha256": digest_bytes(canonical(command)),
    }
    if cache.is_file():
        cached = load(cache)
        if (
            all(cached.get(key) == value for key, value in expected.items())
            and cached.get("outcome") == "passed"
        ):
            return cached
        raise ValueError("stale or mismatched native preflight cache")
    started = time.monotonic_ns()
    output = run_command(command, timeout=timeout).output
    report = {
        **expected,
        "outcome": "passed",
        "elapsed_ns": time.monotonic_ns() - started,
        "output_sha256": digest_bytes(output),
    }
    cache.parent.mkdir(parents=True, exist_ok=True)
    cache.write_bytes(canonical(report))
    return report


def package_version(language: str) -> str:
    if language == "node":
        return str(load(ROOT / "crates/graphforge-bindings-node/package.json")["version"])
    cargo = (ROOT / "Cargo.toml").read_text(encoding="utf-8")
    for line in cargo.splitlines():
        if line.strip().startswith("version ="):
            return line.split("=", 1)[1].strip().strip('"')
    raise ValueError("workspace version is missing")


def toolchain(language: str) -> dict[str, str]:
    command = {
        "rust": ["rustc", "--version"],
        "python": [sys.executable, "--version"],
        "node": ["node", "--version"],
    }[language]
    version = subprocess.check_output(
        command, cwd=ROOT, text=True, stderr=subprocess.STDOUT
    ).strip()
    normalized = re.sub(r"[^A-Za-z0-9_.+/-]", "_", version.replace(" ", "+"))[:128]
    return {"name": language, "version": normalized}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--language", choices=("rust", "python", "node"), required=True)
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--preflight-command", nargs="+")
    parser.add_argument("--probe-command", nargs="+")
    parser.add_argument("--artifact", type=Path)
    args = parser.parse_args()
    try:
        injected = args.preflight_command or args.probe_command or args.artifact
        if injected and os.environ.get("GF_LOAD_EXECUTOR_TESTING") != "1":
            raise ValueError("executor command injection is restricted to integration tests")
        request = load(args.request)
        if request.get("schema") != "graphforge-load-request/1":
            raise ValueError("unsupported load request schema")
        case_timeout = request.get("case_timeout_seconds")
        preflight_timeout = request.get("preflight_timeout_seconds")
        if not all(
            isinstance(value, int) and value > 0 for value in (case_timeout, preflight_timeout)
        ):
            raise ValueError("load request requires positive case and preflight timeouts")
        source_sha = request["source_sha"]
        checkout = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip()
        if source_sha != checkout:
            raise ValueError("load request SHA does not match checkout")
        required_inventory = request.get("required_inventory")
        if not isinstance(required_inventory, list) or not required_inventory:
            raise ValueError("load request has no required public inventory")
        complete = complete_inventory()
        if not set(required_inventory).issubset(complete):
            raise ValueError("load request inventory is not a subset of the release surface")
        inventory_sha = digest_bytes(canonical(complete))
        if digest_file(Path(request["fixture"])) != request["manifest"]["content_sha256"]:
            raise ValueError("load fixture content does not match its manifest")

        if args.probe_command:
            if not args.artifact:
                raise ValueError("injected probe command requires --artifact")
            probe_command, artifact = args.probe_command, args.artifact
            probe_path = Path(probe_command[-1]) if Path(probe_command[-1]).is_file() else artifact
        else:
            probe_command, artifact = default_probe(
                args.language, preflight_timeout=preflight_timeout
            )
            probe_path = {
                "rust": ROOT / "crates/graphforge-api/examples/release_load_probe.rs",
                "python": ROOT / "scripts/ci/release-load-python-probe.py",
                "node": ROOT / "crates/graphforge-bindings-node/tests/release-load-probe.mjs",
            }[args.language]
        if not artifact.is_file():
            raise ValueError(f"native artifact is missing: {artifact}")
        preflight_command = args.preflight_command or default_preflight(args.language)[0]
        cache = args.request.parents[1] / "preflight" / f"{args.language}.json"
        proof = preflight(
            args.language,
            source_sha,
            artifact,
            inventory_sha,
            cache,
            preflight_command,
            probe_path,
            preflight_timeout,
        )

        probe_output = args.output.with_suffix(".probe.json")
        started = time.monotonic_ns()
        command_result = run_command(
            [*probe_command, "--request", str(args.request), "--output", str(probe_output)],
            timeout=case_timeout,
        )
        elapsed = time.monotonic_ns() - started
        probe = load(probe_output)
        probe_output.unlink()
        if (
            set(probe)
            != {
                "schema",
                "language",
                "dataset_sha256",
                "workload",
                "observed",
                "persisted_bytes",
                "temporary_bytes",
                "cleanup",
                "reopen_equivalent",
            }
            or probe.get("schema") != "graphforge-load-native-probe/1"
        ):
            raise ValueError("native probe returned an unsafe or incomplete report")
        if (
            probe["language"] != args.language
            or probe["dataset_sha256"] != request["manifest"]["content_sha256"]
            or probe["workload"] != request["workload"]["id"]
            or probe["cleanup"] != "complete"
            or probe["reopen_equivalent"] is not True
        ):
            raise ValueError("native probe provenance or lifecycle result drift")
        observed = probe["observed"]
        expected_nodes = request["manifest"]["live_nodes"]
        expected_edges = request["manifest"]["live_edges"]
        if (
            not isinstance(observed, dict)
            or observed.get("node_rows") != expected_nodes
            or observed.get("edge_rows") != expected_edges
            or observed.get("reopen_node_rows") != expected_nodes
        ):
            raise ValueError("native probe did not observe the complete fixture")
        for field in (
            "schema_sha256",
            "ordering_sha256",
            "node_result_sha256",
            "rank_result_sha256",
            "find_result_sha256",
        ):
            if not isinstance(observed.get(field), str) or not re.fullmatch(
                r"[0-9a-f]{64}", observed[field]
            ):
                raise ValueError(f"native probe is missing actual {field}")
        if request["workload"]["id"].startswith("m18-") and observed.get("rank_rows", 0) <= 0:
            raise ValueError("native M18 probe returned no rows")
        if request["workload"]["id"].startswith("m19-") and observed.get("find_rows", 0) <= 0:
            raise ValueError("native M19 probe returned no rows")

        result_payload = {
            "dataset_sha256": probe["dataset_sha256"],
            "workload": probe["workload"],
            "node_rows": observed["node_rows"],
            "edge_rows": observed["edge_rows"],
            "rank_rows": observed.get("rank_rows", 0),
            "find_rows": observed.get("find_rows", 0),
            "reopen_node_rows": observed["reopen_node_rows"],
            "node_result_sha256": observed["node_result_sha256"],
            "rank_result_sha256": observed["rank_result_sha256"],
            "find_result_sha256": observed["find_result_sha256"],
        }
        rows_hash = digest_bytes(canonical(result_payload))
        schema_hash = observed["schema_sha256"]
        ordering_hash = observed["ordering_sha256"]
        report = {
            "schema": "graphforge-load-case/1",
            "identity": request["identity"],
            "source_sha": source_sha,
            "dataset_sha256": probe["dataset_sha256"],
            "outcome": "passed",
            "attempt": 1,
            "sanitized_error": None,
            "parity_diff": [],
            "package": {
                "name": {
                    "rust": "graphforge-api",
                    "python": "graphforge",
                    "node": "@curatelabs/graphforge",
                }[args.language],
                "version": package_version(args.language),
                "artifact_sha256": digest_file(artifact),
            },
            "platform": {"os": platform.system(), "arch": platform.machine()},
            "toolchain": toolchain(args.language),
            "covered_inventory": required_inventory,
            "provenance": proof,
            "observations": {
                "elapsed_ns": elapsed,
                "peak_rss_bytes": command_result.peak_rss_bytes,
                "output_rows": observed["node_rows"],
                "output_bytes": len(canonical(result_payload)),
                "open_files": None,
                "threads": None,
                "tasks": None,
                "persisted_bytes": probe["persisted_bytes"],
                "temporary_bytes": probe["temporary_bytes"],
                "cleanup": probe["cleanup"],
                "reopen_equivalent": probe["reopen_equivalent"],
            },
            "runner_observations": {"elapsed_ns": 0},
            "result": {
                "schema_sha256": schema_hash,
                "rows_sha256": rows_hash,
                "ordering_sha256": ordering_hash,
                "fingerprint": digest_bytes(canonical([schema_hash, rows_hash, ordering_hash])),
            },
        }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_bytes(canonical(report))
    except (
        OSError,
        ValueError,
        KeyError,
        json.JSONDecodeError,
        subprocess.SubprocessError,
    ) as error:
        print(f"release load executor failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
