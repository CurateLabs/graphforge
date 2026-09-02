"""GDC SNB BI suite adapter (workload semantics).

Shares identity/acquisition contracts from ``gdc_contracts`` without embedding
those contracts' workload-free rules into operation mapping. Rust owns mapping,
validation modes, phase separation, per-phase resource recording, and reference
validation via ``graphforge-benchmark-gdc-snb-bi``.

Resource evidence (load/query/spill/rss/io) is kept in a distinct ``resources``
section, separate from the per-operation correctness ``operations``. Results
here are engineering evidence only. They never masquerade as an audited GDC
certification (the runner stamps ``certification: false``).
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
from typing import Any

from graphforge_bench.gdc_contracts import (
    GdcContractError,
    load_pinned_identity,
    validate_acquisition,
    workspace_root,
)

ANALYTICAL_READS = tuple(f"BI{index}" for index in range(1, 21))
BATCH_INSERTS = tuple(f"INS{index}" for index in range(1, 9))
BATCH_DELETES = tuple(f"DEL{index}" for index in range(1, 9))
OPERATIONS = ANALYTICAL_READS + BATCH_INSERTS + BATCH_DELETES

JOB_SCHEMA = "graphforge-gdc-snb-bi-job/1"
EVIDENCE_SCHEMA = "graphforge-gdc-snb-bi-evidence/1"
RESOURCE_SCHEMA = "graphforge-gdc-snb-bi-resources/1"
LIVE_EVIDENCE_SCHEMA = "graphforge-gdc-snb-bi-live-evidence/1"
LIVE_RESULT_SCHEMA = "graphforge-gdc-snb-bi-live-result/1"
LIVE_OPERATION = "BI2"
LIVE_FIXTURE = "snb-bi-live"

BATCH_UPDATE_CAUSE = "bi_batch_update_stream_not_exposed"
WEIGHTED_PATH_CAUSE = "weighted_shortest_path_not_exposed"
WEIGHTED_PATH_READS = ("BI15", "BI19", "BI20")

BOUNDED_TINY_DATASET = "snb-bi-sf0.003"

_PINNED_SPEC = {
    "source": "https://github.com/ldbc/ldbc_snb_docs",
    "release": "v2.2.4",
    "commit": "5f7956e07a214373c363b371a3b88bc83ddcd118",
}
_PINNED_QUERY = {
    "source": "https://github.com/ldbc/ldbc_snb_bi",
    "release": "v1.0.0",
    "commit": "abf8cd4862f2b96ba9267e6298a1f7402439040b",
    "path": "cypher/queries/bi-2.cypher",
}

_LIVE_BI2 = """
MATCH (tag:Tag)-[:HAS_TYPE]->(:TagClass {name: $tagClass})
OPTIONAL MATCH (message1:Message)-[:HAS_TAG]->(tag)
  WHERE $window1Start <= message1.creationDate
    AND message1.creationDate < $window2Start
WITH tag, count(message1) AS countWindow1
OPTIONAL MATCH (message2:Message)-[:HAS_TAG]->(tag)
  WHERE $window2Start <= message2.creationDate
    AND message2.creationDate < $windowEnd
WITH tag, countWindow1, count(message2) AS countWindow2
RETURN
  tag.name AS tagName,
  countWindow1,
  countWindow2,
  abs(countWindow1 - countWindow2) AS diff
ORDER BY diff DESC, tagName ASC
LIMIT 100
""".strip()


class SnbBiSuiteError(ValueError):
    """SNB BI suite mapping or validation failed."""

    def __init__(self, cause: str, message: str) -> None:
        super().__init__(message)
        self.cause = cause


def identity_path(root: Path | None = None) -> Path:
    return (root or workspace_root()) / "profiles" / "gdc" / "snb-bi-identity.json"


def runner_binary(root: Path | None = None) -> Path:
    base = root or workspace_root()
    override = os.environ.get("GRAPHFORGE_GDC_SNB_BI_BIN")
    if override:
        return Path(override)
    target = base / "target"
    for profile in ("debug", "release"):
        candidate = target / profile / "graphforge-benchmark-gdc-snb-bi"
        if candidate.is_file():
            return candidate
    raise SnbBiSuiteError(
        "missing_runner",
        "graphforge-benchmark-gdc-snb-bi binary not built; "
        "run cargo build -p graphforge-benchmark-gdc-snb-bi",
    )


def _run_runner(args: list[str], root: Path | None = None) -> subprocess.CompletedProcess[str]:
    binary = runner_binary(root)
    return subprocess.run(
        [str(binary), *args],
        check=False,
        capture_output=True,
        text=True,
    )


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def validate_live_fixture(fixture: Path) -> dict[str, Any]:
    """Validate immutable live-lane identity and every byte-bearing input."""
    try:
        identity = json.loads((fixture / "identity.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise SnbBiSuiteError("invalid_document", f"invalid live identity: {error}") from error
    if identity.get("schema") != "graphforge-gdc-snb-bi-live-identity/1":
        raise SnbBiSuiteError("invalid_document", "unexpected live identity schema")
    if identity.get("certification") is not False:
        raise SnbBiSuiteError("invalid_document", "live lane must set certification=false")
    if identity.get("upstream_spec") != _PINNED_SPEC:
        raise SnbBiSuiteError("identity_drift", "pinned upstream SNB specification drifted")
    if identity.get("upstream_query") != _PINNED_QUERY:
        raise SnbBiSuiteError("identity_drift", "pinned upstream BI2 query drifted")
    if identity.get("fixture", {}).get("kind") != "synthetic_minimal_snb_shaped":
        raise SnbBiSuiteError("identity_drift", "fixture must disclose synthetic provenance")
    if identity.get("fixture", {}).get("scale_factor") is not None:
        raise SnbBiSuiteError(
            "identity_drift", "synthetic fixture must not claim an SNB scale factor"
        )
    if identity.get("reference", {}).get("captured_from_engine") is not False:
        raise SnbBiSuiteError("identity_drift", "reference must be independently derived")
    if identity.get("driver", {}).get("kind") != "internal_python_public_api_driver":
        raise SnbBiSuiteError("identity_drift", "live lane must disclose its internal driver")
    for key in ("fixture", "parameters", "reference"):
        item = identity.get(key, {})
        path = fixture / item.get("path", "")
        if not path.is_file():
            raise SnbBiSuiteError("missing_assets", f"missing live {key}: {path.name}")
        if _sha256(path) != item.get("sha256"):
            raise SnbBiSuiteError("checksum_mismatch", f"live {key} bytes drifted")
    return identity


def _load_live_inputs(
    fixture: Path,
    parameters_override: dict[str, Any] | None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], str]:
    identity = validate_live_fixture(fixture)
    seed = json.loads((fixture / identity["fixture"]["path"]).read_text(encoding="utf-8"))
    parameters = json.loads((fixture / identity["parameters"]["path"]).read_text(encoding="utf-8"))
    parameters_sha256 = identity["parameters"]["sha256"]
    if parameters_override:
        parameters.update(parameters_override)
        parameters_sha256 = hashlib.sha256(
            json.dumps(parameters, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
    if seed.get("schema") != "graphforge-gdc-snb-bi-synthetic-seed/1":
        raise SnbBiSuiteError("invalid_document", "unexpected live seed schema")
    if seed.get("seed") != 963:
        raise SnbBiSuiteError("identity_drift", "deterministic seed must remain 963")
    if parameters.get("schema") != "graphforge-gdc-snb-bi-live-parameters/1":
        raise SnbBiSuiteError("invalid_document", "unexpected live parameter schema")
    if parameters.get("operation") != LIVE_OPERATION:
        raise SnbBiSuiteError("invalid_document", "live parameters must select BI2")
    return identity, seed, parameters, parameters_sha256


def _load_seed_into_graphforge(forge: Any, seed: dict[str, Any]) -> int:
    for name in seed["tag_classes"]:
        forge.execute("CREATE (:TagClass {name: $name})", {"name": name})
    for tag in seed["tags"]:
        forge.execute("CREATE (:Tag {name: $name})", {"name": tag["name"]})
        forge.execute(
            "MATCH (tag:Tag {name: $tag}), (class:TagClass {name: $class}) "
            "CREATE (tag)-[:HAS_TYPE]->(class)",
            {"tag": tag["name"], "class": tag["tag_class"]},
        )
    tagged_edges = 0
    for message in seed["messages"]:
        forge.execute(
            "CREATE (:Message {id: $id, creationDate: $creationDate})",
            {"id": message["id"], "creationDate": message["creation_day"]},
        )
        for tag in message["tags"]:
            forge.execute(
                "MATCH (message:Message {id: $id}), (tag:Tag {name: $tag}) "
                "CREATE (message)-[:HAS_TAG]->(tag)",
                {"id": message["id"], "tag": tag},
            )
            tagged_edges += 1
    return (
        len(seed["tag_classes"])
        + len(seed["tags"])
        + len(seed["tags"])
        + len(seed["messages"])
        + tagged_edges
    )


def validate_live_result_document(
    result_path: Path,
    *,
    root: Path | None = None,
    fixture: Path | None = None,
) -> None:
    base = root or workspace_root()
    live_fixture = fixture or (base / "fixtures" / "gdc" / LIVE_FIXTURE)
    identity = validate_live_fixture(live_fixture)
    reference = live_fixture / identity["reference"]["path"]
    completed = _run_runner(
        [
            "validate-live",
            str(reference),
            str(result_path),
            identity["parameters"]["sha256"],
        ],
        base,
    )
    if completed.returncode != 0:
        message = completed.stderr.strip()
        if "reference_mismatch" in message:
            raise SnbBiSuiteError("reference_mismatch", message)
        if "parameter_identity_mismatch" in message:
            raise SnbBiSuiteError("parameter_identity_mismatch", message)
        if "static_output_rejected" in message or "invalid live result" in message:
            raise SnbBiSuiteError("static_output_rejected", message)
        raise SnbBiSuiteError("invalid_document", message)


def run_live_bi2(
    *,
    root: Path | None = None,
    parameters_override: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Execute normalized BI2 through the real in-memory public Python API."""
    from graphforge import GraphForge

    base = root or workspace_root()
    fixture = base / "fixtures" / "gdc" / LIVE_FIXTURE
    identity, seed, parameters, parameters_sha256 = _load_live_inputs(fixture, parameters_override)
    forge = GraphForge()
    load_started = time.perf_counter_ns()
    rows_loaded = _load_seed_into_graphforge(forge, seed)
    load_ms = (time.perf_counter_ns() - load_started) // 1_000_000

    query_parameters = {
        key: parameters[key] for key in ("tagClass", "window1Start", "window2Start", "windowEnd")
    }
    query_started = time.perf_counter_ns()
    table = forge.execute(_LIVE_BI2, query_parameters)
    query_ms = (time.perf_counter_ns() - query_started) // 1_000_000
    columns = ["tagName", "countWindow1", "countWindow2", "diff"]
    rows = [
        " ".join(str(row[column]) for column in columns)
        for row in table.select(columns).to_pylist()
    ]
    result = {
        "schema": LIVE_RESULT_SCHEMA,
        "operation": LIVE_OPERATION,
        "source": "graphforge_public_python_api",
        "parameters_sha256": parameters_sha256,
        "columns": columns,
        "rows": rows,
    }
    with tempfile.TemporaryDirectory(prefix="gdc-snb-bi-live-") as tmp:
        result_path = Path(tmp) / "live-result.json"
        result_path.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        validation_started = time.perf_counter_ns()
        validate_live_result_document(result_path, root=base, fixture=fixture)
        validation_ms = (time.perf_counter_ns() - validation_started) // 1_000_000

    return {
        "schema": LIVE_EVIDENCE_SCHEMA,
        "suite_id": "snb-bi",
        "lane": "live_in_memory",
        "operation": LIVE_OPERATION,
        "status": "passed",
        "certification": False,
        "phases": ["load", "query", "validation"],
        "identities": identity,
        "correctness": {
            "status": "passed",
            "validation_mode": "normalized",
            "validator": "graphforge-benchmark-gdc-snb-bi",
            "reference_authority": identity["reference"]["authority"],
            "rows": rows,
        },
        "resources": {
            "correctness_authority": False,
            "load": {"wall_ms": load_ms, "rows_loaded": rows_loaded},
            "query": {"wall_ms": query_ms, "rows_returned": len(rows)},
            "validation": {"wall_ms": validation_ms},
            "unobserved": ["spill_bytes", "peak_rss_bytes", "io_bytes"],
        },
    }


def list_operation_rules(root: Path | None = None) -> dict[str, dict[str, str]]:
    completed = _run_runner(["list-operations"], root)
    if completed.returncode != 0:
        raise SnbBiSuiteError("invalid_document", completed.stderr.strip())
    rules: dict[str, dict[str, str]] = {}
    for line in completed.stdout.splitlines():
        operation, _, rest = line.partition(" ")
        fields: dict[str, str] = {}
        for token in rest.split():
            key, _, value = token.partition("=")
            fields[key] = value
        if "category" not in fields or "validation" not in fields or "mapping" not in fields:
            raise SnbBiSuiteError("invalid_document", f"bad operation line: {line}")
        rules[operation] = fields
    if set(rules) != set(OPERATIONS):
        raise SnbBiSuiteError(
            "invalid_document",
            f"runner must declare all {len(OPERATIONS)} operations, got {sorted(rules)}",
        )
    return rules


def run_tiny_suite(
    *,
    fixture_name: str = "compatible",
    root: Path | None = None,
    evidence_path: Path | None = None,
) -> dict[str, Any]:
    """Replay the legacy synthetic static contract fixture through Rust."""
    base = root or workspace_root()
    fixture = base / "fixtures" / "gdc" / "snb-bi-tiny" / fixture_name
    pin = load_pinned_identity(identity_path(base))
    acquisition = json.loads((fixture / "acquisition.json").read_text(encoding="utf-8"))
    # Provenance evidence from shared contracts (checksummed assets only).
    contract_evidence = validate_acquisition(pin, acquisition, fixture)
    identities = contract_evidence["identities"]
    with tempfile.TemporaryDirectory(prefix="gdc-snb-bi-") as tmp:
        tmp_path = Path(tmp)
        identities_path = tmp_path / "identities.json"
        identities_path.write_text(json.dumps(identities, indent=2) + "\n", encoding="utf-8")
        out_evidence = evidence_path or (tmp_path / "evidence.json")
        completed = _run_runner(
            [
                "run-suite",
                str(fixture / "jobs"),
                str(fixture / "references"),
                str(fixture / "system-outputs"),
                str(fixture / "resources.json"),
                str(identities_path),
                str(out_evidence),
            ],
            base,
        )
        if not out_evidence.is_file():
            raise SnbBiSuiteError(
                "invalid_document",
                f"runner failed to emit evidence: {completed.stderr.strip()}",
            )
        evidence = json.loads(out_evidence.read_text(encoding="utf-8"))
        if evidence.get("schema") != EVIDENCE_SCHEMA:
            raise SnbBiSuiteError(
                "invalid_document",
                "unexpected snb-bi evidence schema",
            )
        if evidence.get("certification") is not False:
            raise SnbBiSuiteError(
                "invalid_document",
                "evidence must never claim GDC certification",
            )
        if "resources" not in evidence or "operations" not in evidence:
            raise SnbBiSuiteError(
                "invalid_document",
                "evidence must record resources separately from correctness",
            )
        if completed.returncode != 0 and fixture_name == "compatible":
            raise SnbBiSuiteError(
                "reference_mismatch",
                f"compatible fixture must pass: {completed.stderr.strip()}",
            )
        return evidence


def map_operation_file(path: Path, root: Path | None = None) -> dict[str, Any]:
    completed = _run_runner(["map-operation", str(path)], root)
    if completed.returncode == 3:
        raise SnbBiSuiteError("semantic_incompatibility", completed.stderr.strip())
    if completed.returncode != 0:
        raise SnbBiSuiteError("invalid_document", completed.stderr.strip())
    return json.loads(completed.stdout)


def assert_large_scale_factors_are_opt_in(root: Path | None = None) -> None:
    """Only the synthetic tiny fixture is replayed by default; scale runs are opt-in.

    ``snb-bi-sf0.003`` is a historical synthetic fixture identifier, not an
    official scale-factor claim. Real generated scale factors are external and
    opt-in.
    """
    base = root or workspace_root()
    suite = json.loads((base / "suites" / "gdc-snb-bi.json").read_text(encoding="utf-8"))
    datasets = suite.get("datasets", [])
    if datasets != [BOUNDED_TINY_DATASET]:
        raise SnbBiSuiteError(
            "invalid_document",
            "default SNB BI suite must run only the bounded tiny fixture; "
            "larger scale factors are opt-in / external",
        )
    pin = load_pinned_identity(identity_path(base))
    pinned_ids = [dataset["id"] for dataset in pin.get("datasets", [])]
    if pinned_ids != [BOUNDED_TINY_DATASET]:
        raise SnbBiSuiteError(
            "invalid_document",
            "pinned identity must bound the committed fixture to the tiny scale factor",
        )


def assert_separate_from_other_suites(root: Path | None = None) -> None:
    """SNB BI profiles/validation/evidence stay distinct from siblings."""
    base = root or workspace_root()
    suite = json.loads((base / "suites" / "gdc-snb-bi.json").read_text(encoding="utf-8"))
    if suite.get("family") != "gdc" or suite.get("suite_id") != "snb-bi":
        raise SnbBiSuiteError(
            "invalid_document",
            "suite must remain a GDC SNB BI suite",
        )
    rendered = json.dumps(suite)
    for foreign in ("graph500", "graphalytics", "snb-interactive", "finbench", "spb"):
        if foreign in rendered:
            raise SnbBiSuiteError(
                "invalid_document",
                f"SNB BI suite must not embed {foreign}",
            )
    if suite.get("runner") != "gdc-snb-bi":
        raise SnbBiSuiteError(
            "invalid_document",
            "SNB BI suite must use the gdc-snb-bi runner",
        )


__all__ = [
    "ANALYTICAL_READS",
    "BATCH_DELETES",
    "BATCH_INSERTS",
    "BATCH_UPDATE_CAUSE",
    "BOUNDED_TINY_DATASET",
    "EVIDENCE_SCHEMA",
    "JOB_SCHEMA",
    "LIVE_EVIDENCE_SCHEMA",
    "LIVE_FIXTURE",
    "LIVE_OPERATION",
    "LIVE_RESULT_SCHEMA",
    "OPERATIONS",
    "RESOURCE_SCHEMA",
    "WEIGHTED_PATH_CAUSE",
    "WEIGHTED_PATH_READS",
    "GdcContractError",
    "SnbBiSuiteError",
    "assert_large_scale_factors_are_opt_in",
    "assert_separate_from_other_suites",
    "identity_path",
    "list_operation_rules",
    "map_operation_file",
    "run_live_bi2",
    "run_tiny_suite",
    "validate_live_fixture",
    "validate_live_result_document",
]
