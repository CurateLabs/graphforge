#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[3]
BUNDLE = Path(__file__).resolve().parent
RUST_EXAMPLE = ROOT / "crates/graphforge-api/examples/derived_state_freshness_workflow.rs"
GENERATOR = BUNDLE / "generator.yaml"
TIMEOUT = 900
BARRIER = (
    "search_index::tests::adjacency_barrier_cancellation_and_follow_on_rebuild_are_deterministic"
)
TRANSITIONS = {
    "text": ["current", "stale", "current"],
    "adjacency": ["current", "stale", "current"],
    "embedding": ["fresh", "fresh"],
}
HEX64 = re.compile(r"[0-9a-f]{64}")
BINDING_KEYS = {
    "schema_version",
    "scenario_id",
    "binding",
    "commit_sha",
    "text_states",
    "adjacency_states",
    "compatibility_ids",
    "generation_ids",
    "embedding_states",
    "reopen_equal",
    "package_version",
    "native_version",
    "native_module_path",
    "native_module_sha256",
}
PACKAGE_VERSIONS = {"python": "0.5.0.dev0", "node": "0.5.0-dev.0"}
RUST_KEYS = {
    "scenario_id",
    "slice",
    "text",
    "text_results",
    "adjacency",
    "cancellation_code",
    "prior_authority_preserved",
    "analysis",
    "embeddings",
    "hypothesis",
    "transaction_time_view",
    "ontology_constant",
    "reopen_equal",
}
RUST_EMBEDDING_KEYS = {
    "compatibility_ids",
    "states",
    "exact_replay",
    "incompatible_code",
    "prior_authority_preserved",
}


def execute(command: list[str], env: dict[str, str]) -> str:
    try:
        return subprocess.run(
            command,
            cwd=ROOT,
            env=env,
            check=True,
            text=True,
            stdout=subprocess.PIPE,
            timeout=TIMEOUT,
        ).stdout
    except subprocess.TimeoutExpired as error:
        raise SystemExit(f"step timed out after {TIMEOUT}s: {' '.join(command)}") from error
    except subprocess.CalledProcessError as error:
        raise SystemExit(f"step failed ({error.returncode}): {' '.join(command)}") from error


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def parse_rust_evidence(stdout: str) -> dict[str, object]:
    lines = stdout.strip().splitlines()
    if not lines:
        raise SystemExit(f"{RUST_EXAMPLE.stem} produced no evidence on stdout")
    try:
        record = json.loads(lines[-1])
    except json.JSONDecodeError as error:
        raise SystemExit(
            f"{RUST_EXAMPLE.stem} emitted non-JSON evidence on its final stdout line"
        ) from error
    if not isinstance(record, dict):
        raise SystemExit(f"{RUST_EXAMPLE.stem} evidence must be a JSON object")
    return record


def id_pair(value: object, field: str) -> list[str]:
    if (
        not isinstance(value, list)
        or len(value) != 2
        or not all(isinstance(item, str) and HEX64.fullmatch(item) for item in value)
        or value[0] == value[1]
    ):
        raise ValueError(f"{field} must contain two distinct lowercase hash identities")
    return value


def validate_binding(
    record: dict[str, object], binding: str, sha: str, root: Path
) -> dict[str, object]:
    if set(record) != BINDING_KEYS:
        raise ValueError(f"{binding} evidence keys differ from the closed schema")
    fixed = {
        "schema_version": 1,
        "scenario_id": "derived-state-freshness",
        "binding": binding,
        "commit_sha": sha,
        "reopen_equal": True,
        "package_version": PACKAGE_VERSIONS[binding],
        "native_version": "0.5.0-dev",
    }
    if any(
        type(record.get(key)) is not type(expected) or record.get(key) != expected
        for key, expected in fixed.items()
    ):
        raise ValueError(f"{binding} evidence fixed fields or types differ")
    observed = {
        "text": record.get("text_states"),
        "adjacency": record.get("adjacency_states"),
        "embedding": record.get("embedding_states"),
    }
    if observed != TRANSITIONS:
        raise ValueError(f"{binding} freshness contract differs from Rust")
    id_pair(record.get("compatibility_ids"), f"{binding} compatibility_ids")
    id_pair(record.get("generation_ids"), f"{binding} generation_ids")
    native = Path(str(record.get("native_module_path", ""))).resolve()
    expected_name = (
        r"_graphforge_rs.*\.(?:so|pyd|dylib)"
        if binding == "python"
        else r"graphforge(?:\.[a-z0-9_-]+)*\.node"
    )
    native_hash = record.get("native_module_sha256")
    if (
        root.resolve() not in native.parents
        or not re.fullmatch(expected_name, native.name)
        or not isinstance(native_hash, str)
        or not HEX64.fullmatch(native_hash)
        or native_hash != digest(native)
    ):
        raise ValueError(f"{binding} did not execute its isolated native artifact")
    return {**fixed, "transitions": observed, "native_module": native.name}


def validate_rust(record: dict[str, object]) -> dict[str, object]:
    if set(record) != RUST_KEYS or not isinstance(record.get("embeddings"), dict):
        raise ValueError("Rust evidence keys differ from the closed contract")
    embeddings = record["embeddings"]
    if set(embeddings) != RUST_EMBEDDING_KEYS:
        raise ValueError("Rust embedding evidence keys differ from the closed contract")
    observed = {
        "text": record.get("text"),
        "adjacency": record.get("adjacency"),
        "embedding": embeddings.get("states"),
    }
    fixed = {
        "scenario_id": "derived-state-freshness",
        "slice": "authoritative-rust",
        "cancellation_code": "GF_CANCELLED",
        "prior_authority_preserved": True,
        "ontology_constant": True,
        "reopen_equal": True,
    }
    if observed != TRANSITIONS or any(record.get(key) != value for key, value in fixed.items()):
        raise ValueError("authoritative Rust evidence differs from the fixed contract")
    expected_embedding = {
        "states": TRANSITIONS["embedding"],
        "exact_replay": True,
        "incompatible_code": "GF_VALIDATION",
        "prior_authority_preserved": True,
    }
    if any(embeddings.get(key) != value for key, value in expected_embedding.items()):
        raise ValueError("authoritative Rust embedding evidence differs from the contract")
    id_pair(embeddings.get("compatibility_ids"), "Rust compatibility_ids")
    analysis = record.get("analysis")
    hypothesis = record.get("hypothesis")
    if (
        not isinstance(analysis, dict)
        or set(analysis) != {"authoritative_vector_uuids", "property_correction_reanalyzed"}
        or not isinstance(analysis.get("authoritative_vector_uuids"), list)
        or not analysis["authoritative_vector_uuids"]
        or any(
            not isinstance(value, str) or not value
            for value in analysis["authoritative_vector_uuids"]
        )
        or analysis["property_correction_reanalyzed"] is not True
        or hypothesis != {"exact_snapshot_equal": True}
    ):
        raise ValueError("authoritative analysis or hypothesis evidence differs from the contract")
    expected_arrow = json.loads((BUNDLE / "expected/arrow-fingerprints.json").read_text())
    text_results = record.get("text_results")
    if (
        not isinstance(text_results, dict)
        or set(text_results) != {"baseline", "refreshed"}
        or any(
            not isinstance(value, dict) or set(value) != {"schema", "rows"}
            for value in text_results.values()
        )
    ):
        raise ValueError("authoritative Rust text results are missing")
    observed_schemas = {name: value.get("schema") for name, value in text_results.items()}
    observed_rows = {name: value.get("rows") for name, value in text_results.items()}
    result_sha256 = hashlib.sha256(
        json.dumps(observed_rows, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    if (
        observed_schemas != expected_arrow.get("schemas")
        or observed_rows != expected_arrow.get("results")
        or result_sha256 != expected_arrow.get("result_sha256")
    ):
        raise ValueError("authoritative Rust text Arrow evidence differs from expected")
    transaction_time = record.get("transaction_time_view")
    if transaction_time != {"cutoff": 9223372036854775807, "exact_snapshot_equal": True}:
        raise ValueError("authoritative transaction-time view differs from expected")
    return {
        **fixed,
        "transitions": observed,
        "analysis": analysis,
        "hypothesis": hypothesis,
        "embeddings": expected_embedding,
        "text_results": {"schemas": observed_schemas, "result_sha256": result_sha256},
        "transaction_time_view": transaction_time,
    }


def write_evidence(
    output: Path,
    sha: str,
    rust: dict[str, object],
    bindings: list[dict[str, object]],
    observations: dict[str, object],
) -> None:
    canonical = {
        "schema_version": 1,
        "scenario_id": "derived-state-freshness",
        "commit_sha": sha,
        "barrier_test": BARRIER,
        "generator_sha256": digest(GENERATOR),
        "ontology_sha256": digest(BUNDLE / "ontologies/strict-v1.yaml"),
        "transitions": TRANSITIONS,
        "rust": rust,
        "bindings": {str(item["binding"]): item for item in bindings},
    }
    envelope = {
        "schema_version": 1,
        "scenario_id": "derived-state-freshness",
        "commit_sha": sha,
        **observations,
    }
    output.write_text(json.dumps(canonical, indent=2, sort_keys=True) + "\n")
    output.with_name(f"{output.stem}-observations.json").write_text(
        json.dumps(envelope, indent=2, sort_keys=True) + "\n"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--commit-sha", required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if not re.fullmatch(r"[0-9a-f]{40}", args.commit_sha):
        raise SystemExit("--commit-sha must be 40 lowercase hexadecimal characters")
    actual = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True, timeout=TIMEOUT
    ).strip()
    dirty = subprocess.check_output(
        ["git", "status", "--porcelain", "--untracked-files=all"],
        cwd=ROOT,
        text=True,
        timeout=TIMEOUT,
    ).strip()
    if actual != args.commit_sha or dirty:
        raise SystemExit("same-SHA evidence requires the requested clean checkout")

    output = (
        args.output or ROOT / "target/release-workflows/derived-state-freshness/evidence.json"
    ).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env.setdefault("CARGO_TARGET_DIR", str(ROOT / "target/release-workflows/cargo-derived-state"))
    execute([sys.executable, str(BUNDLE / "test_runner.py"), "--quiet"], env)
    execute(["cargo", "test", "-p", "graphforge-api", "--lib", BARRIER, "--", "--exact"], env)
    rust_text = execute(
        ["cargo", "run", "-p", "graphforge-api", "--example", RUST_EXAMPLE.stem], env
    )
    rust = parse_rust_evidence(rust_text)
    rust_summary = validate_rust(rust)

    with tempfile.TemporaryDirectory(prefix="gf-derived-state-") as temporary:
        temp = Path(temporary)
        wheel_dir, node_dir = temp / "wheel", temp / "node"
        wheel_dir.mkdir()
        node_dir.mkdir()
        execute(
            [
                "uv",
                "run",
                "maturin",
                "build",
                "--manifest-path",
                "crates/graphforge-bindings-py/Cargo.toml",
                "--profile",
                "dev",
                "--out",
                str(wheel_dir),
            ],
            env,
        )
        wheels = list(wheel_dir.glob("*.whl"))
        if len(wheels) != 1:
            raise ValueError(f"expected one Python wheel, found {len(wheels)}")
        venv = temp / "venv"
        execute(["uv", "venv", str(venv), "--python", "3.13"], env)
        python = venv / ("Scripts/python.exe" if sys.platform == "win32" else "bin/python")
        execute(["uv", "pip", "install", "--python", str(python), str(wheels[0])], env)
        (temp / "python-project").mkdir()
        execute(
            [
                str(python),
                str(BUNDLE / "binding_workflow.py"),
                "--project",
                str(temp / "python-project"),
                "--evidence",
                str(python_path := temp / "python.json"),
                "--commit-sha",
                actual,
            ],
            env,
        )
        python_record = json.loads(python_path.read_text())
        python_summary = validate_binding(python_record, "python", actual, venv)

        execute(["pnpm", "install", "--frozen-lockfile", "--filter", "@graphforge/node..."], env)
        execute(
            [
                "pnpm",
                "--filter",
                "@graphforge/node",
                "exec",
                "napi",
                "build",
                "--platform",
                "--release",
                "--output-dir",
                str(node_dir),
            ],
            env,
        )
        (node_dir / "package.json").write_text(
            (ROOT / "crates/graphforge-bindings-node/package.json").read_text()
        )
        modules = list(node_dir.glob("index.js"))
        if len(modules) != 1:
            raise ValueError(f"expected one Node module, found {modules}")
        node_path = temp / "node.json"
        (temp / "node-project").mkdir()
        execute(
            [
                "node",
                str(BUNDLE / "binding_workflow.mjs"),
                "--project",
                str(temp / "node-project"),
                "--evidence",
                str(node_path),
                "--commit-sha",
                actual,
                "--module",
                str(modules[0]),
            ],
            env,
        )
        node_record = json.loads(node_path.read_text())
        node_summary = validate_binding(node_record, "node", actual, node_dir)
        if python_record["compatibility_ids"] != node_record["compatibility_ids"]:
            raise ValueError("Python and Node compatibility identities differ")
        write_evidence(
            output,
            actual,
            rust_summary,
            [python_summary, node_summary],
            {
                "rust": rust,
                "python": {**python_record, "native_module_path": python_summary["native_module"]},
                "node": {**node_record, "native_module_path": node_summary["native_module"]},
                "wheel_sha256": digest(wheels[0]),
            },
        )


if __name__ == "__main__":
    main()
