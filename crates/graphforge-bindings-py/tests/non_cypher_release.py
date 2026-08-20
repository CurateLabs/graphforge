"""Deterministic release acceptance for the native Python non-Cypher surface.

This file is run against the freshly built wheel by the native-artifact CI job.
It also emits a receiver-qualified projection of the authoritative Rust
inventory when ``GRAPHFORGE_PYTHON_PARITY_REPORT`` is set.
"""

from __future__ import annotations

import ast
import hashlib
import importlib.util
import json
import multiprocessing
import os
from pathlib import Path
import re
import socket
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[3]
RUST_MANIFEST = ROOT / "tests/contracts/non-cypher-rust-surface.json"
RUST_GATE = ROOT / "scripts/ci/non-cypher-surface-gate.py"
PYO3_SOURCE = ROOT / "crates/graphforge-bindings-py/src/lib.rs"
EXPECTED_RUST_DIGEST = "268d0832e1fa2bc823e1aa6a0f7a5129c29f4b0b23886a6fc2927616671a3b73"
EXPECTED_RELEASE_DIGEST = "9ca796af78a3ea51e4c0d404e9f74e3eb1cdd49a0b862dc15838cd9ada037877"

PYTHON_ONLY_METHODS = frozenset(
    {
        "CancellationToken.__init__",
        "EdgeHandle.__repr__",
        "EdgeHandle.__str__",
        "EdgeHandle.rel_type",
        "EdgeHandle.uuid",
        "GraphForge.__init__",
        "GraphForge.__repr__",
        "GraphForge.add_edges",
        "GraphForge.add_nodes",
        "GraphForge.close",
        "GraphForge.configure_openrouter",
        "GraphForge.execute_polars",
        "GraphForge.load_ontology",
        "GraphForge.ontology_mode",
        "GraphForge.path",
        "InvocationDescriptor.algorithm",
        "InvocationDescriptor.fingerprint",
        "InvocationDescriptor.projection_fingerprint",
        "InvocationDescriptor.verb",
        "NodeHandle.__repr__",
        "NodeHandle.__str__",
        "NodeHandle.label",
        "NodeHandle.uuid",
        "ResolvedBeliefProjection.graph_content_fingerprint",
        "ResolvedBeliefProjection.policy_fingerprint",
        "ResolvedBeliefProjection.snapshot_fingerprint",
        "ResolvedBeliefProjection.source_generation_uuid",
        "ResolvedBeliefProjection.transaction_cutoff",
        "ResolvedBeliefProjection.valid_time",
        "ResolvedBeliefProjection.valid_time_fingerprint",
    }
)

EVIDENCE = {
    "infra-validation": {
        "non_cypher_release.py": ["check_lifecycle_checkpoint_errors_and_reopen"],
    },
    "repository-sync": {
        "non_cypher_release.py": ["check_lifecycle_checkpoint_errors_and_reopen"],
    },
    "ontology-lifecycle": {
        "ontology_lifecycle.py": ["main"],
    },
    "lifecycle-construction": {
        "non_cypher_release.py": ["check_lifecycle_checkpoint_errors_and_reopen"],
    },
    "gsi-profiler": {
        "gsi_profiler.py": [
            "check_empty_and_configured_grades",
            "check_tiny_graph_and_reject_unknown",
        ],
    },
    "checkpoint-view": {"checkpoints.py": ["main"]},
    "algorithm": {
        "smoke.py": [
            "check_degree_rank",
            "check_components_cluster",
            "check_dijkstra_paths",
            "check_is_dag",
            "check_knn",
        ],
    },
    "search-provider-rerank": {
        "search_index.py": ["check_typed_search_indexing"],
        "provider_workflow.py": ["check_configured_provider_workflow"],
    },
    "knowledge": {
        "smoke.py": [
            "check_assertions",
            "check_confidence_assessments",
            "check_evidence_links",
        ],
        "composite_transaction.py": [
            "check_composite_transaction",
            "check_no_inference_helpers",
        ],
    },
    "epistemic": {
        "smoke.py": [
            "check_reasoning",
            "check_assertion_status",
            "check_assertion_supersessions",
            "check_hypothesis_selection",
            "check_assertion_validity",
            "check_resolved_belief_projection",
            "check_algorithm_runs",
        ],
    },
    "streaming-errors-maintenance": {
        "smoke.py": ["check_execute_stream", "check_lifecycle", "check_parse_error_span"],
        "result_sink_stream.py": ["check_result_sink_stream"],
    },
    "transaction-maintenance": {
        "transaction_parity.py": [
            "check_mixed_commit_and_rollback",
            "check_dropped_handle_never_commits",
            "check_maintenance_preview_execute_reconcile",
            "check_cli_parity",
        ],
    },
    "compatibility": {
        "non_cypher_release.py": ["check_native_artifact_and_no_fallback"],
    },
    "resumable-import": {
        "import_session_lifecycle.py": ["check_import_session_lifecycle"],
    },
    "semantic-generation-diff": {
        "generation_diff.py": ["check_generation_diff"],
    },
    "portable-v2-facade": {
        "portable_v2_parity.py": ["check_portable_v2_parity"],
    },
}


def _provider_error_server(ready: multiprocessing.Queue) -> None:
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    listener.settimeout(10)
    ready.put(f"http://127.0.0.1:{listener.getsockname()[1]}")
    try:
        for call in range(5):
            connection, _ = listener.accept()
            with connection:
                received = bytearray()
                while b"\r\n\r\n" not in received:
                    received.extend(connection.recv(4096))
                headers, body = bytes(received).split(b"\r\n\r\n", 1)
                length = next(
                    int(line.split(b":", 1)[1].strip())
                    for line in headers.split(b"\r\n")
                    if line.lower().startswith(b"content-length:")
                )
                while len(body) < length:
                    body += connection.recv(4096)
                if call == 0:
                    encoded = json.dumps(
                        {
                            "model": "vendor/model",
                            "data": [{"index": 0, "embedding": [1.0, 0.0]}],
                        }
                    ).encode()
                    connection.sendall(
                        b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                        + f"Content-Length: {len(encoded)}\r\nConnection: close\r\n\r\n".encode()
                        + encoded
                    )
                else:
                    connection.sendall(
                        b"HTTP/1.1 500 Internal Server Error\r\n"
                        b"Content-Length: 0\r\nConnection: close\r\n\r\n"
                    )
    except TimeoutError:
        pass
    finally:
        listener.close()


def _load_rust_gate():
    spec = importlib.util.spec_from_file_location("non_cypher_surface_gate", RUST_GATE)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _matching_brace(text: str, opening: int) -> int:
    depth = 0
    for index in range(opening, len(text)):
        if text[index] == "{":
            depth += 1
        elif text[index] == "}":
            depth -= 1
            if depth == 0:
                return index
    raise AssertionError("unbalanced PyO3 source braces")


def _python_methods() -> set[str]:
    """Extract receiver-qualified methods from the compiled PyO3 surface."""
    text = PYO3_SOURCE.read_text()
    receiver_names = {
        "GraphForge": "GraphForge",
        "PyCheckpointView": "CheckpointView",
        "PyCancellationToken": "CancellationToken",
        "PyInvocationDescriptor": "InvocationDescriptor",
        "PyResolvedBeliefProjection": "ResolvedBeliefProjection",
        "PyNodeHandle": "NodeHandle",
        "PyEdgeHandle": "EdgeHandle",
        "PyGraphImportSession": "GraphImportSession",
    }
    found: set[str] = set()
    for rust_receiver, public_receiver in receiver_names.items():
        pattern = re.compile(rf"#\[pymethods\]\s*impl\s+{rust_receiver}\s*\{{")
        for match in pattern.finditer(text):
            end = _matching_brace(text, match.end() - 1)
            body = text[match.end() : end]
            for method in re.findall(r"^\s*fn\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(", body, re.M):
                public_method = {"new": "__init__", "repr": "__repr__"}.get(method, method)
                found.add(f"{public_receiver}.{public_method}")
    return found


def _digest(values: set[str]) -> str:
    return hashlib.sha256(("\n".join(sorted(values)) + "\n").encode()).hexdigest()


def _classification_report() -> dict[str, object]:
    manifest = json.loads(RUST_MANIFEST.read_text())
    assert manifest["public_method_digest"] == EXPECTED_RUST_DIGEST
    gate = _load_rust_gate()
    rust_methods = gate.public_methods()
    assert gate.method_digest(rust_methods) == EXPECTED_RUST_DIGEST
    python_methods = _python_methods()
    release_methods = {
        method_id
        for group in manifest["method_evidence_groups"].values()
        for method_id in group["ids"]
    }
    assert len(release_methods) == 210
    assert _digest(release_methods) == EXPECTED_RELEASE_DIGEST
    assert set(EVIDENCE) == set(manifest["method_evidence_groups"])

    test_root = ROOT / "crates/graphforge-bindings-py/tests"
    for group, files in EVIDENCE.items():
        assert files, f"{group} has no exact Python evidence"
        for filename, symbols in files.items():
            tree = ast.parse((test_root / filename).read_text(), filename=filename)
            defined = {
                node.name
                for node in ast.walk(tree)
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            }
            missing = sorted(set(symbols) - defined)
            assert not missing, f"{group} has stale Python evidence in {filename}: {missing}"
            main = next(
                (
                    node
                    for node in tree.body
                    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                    and node.name == "main"
                ),
                None,
            )
            executable_nodes = (
                [main]
                if main is not None
                else [node for node in tree.body if isinstance(node, ast.If)]
            )
            assert executable_nodes, f"{filename} has no executable entry point"
            invoked = {
                node.func.id
                for executable in executable_nodes
                for node in ast.walk(executable)
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
            }
            not_invoked = sorted(set(symbols) - {"main"} - invoked)
            assert not not_invoked, f"{group} evidence is not run by {filename}/main: {not_invoked}"

    aliases = {
        "GraphForge.new": "GraphForge.__init__",
        "crate.version": "crate.version",
        "crate.verify_portable_v2": "GraphForge.verify_portable_v2",
        "crate.publish_portable_v2_oci": "GraphForge.publish_portable_v2_oci",
        "crate.pull_portable_v2_oci": "GraphForge.pull_portable_v2_oci",
        "GraphForge.execute_to_parquet_stream_with_params": "GraphForge.execute_to_parquet_stream",
        "GraphForge.execute_to_arrow_ipc_stream_with_params": "GraphForge.execute_to_arrow_ipc_stream",
    }
    classifications: dict[str, dict[str, str]] = {}
    for rust_id in sorted(rust_methods):
        python_id = aliases.get(rust_id, rust_id)
        if python_id in python_methods:
            classification = "equivalent"
            reason = "same receiver operation delegates through the compiled PyO3 extension"
        elif rust_id.startswith("OpenRouterProviderSession."):
            classification = "intentionally-language-specific"
            reason = (
                "Python exposes provider configuration and execution on GraphForge, "
                "not the Rust session type"
            )
        elif rust_id.startswith(("ConfiguredProvider", "Provider", "RuntimeGuard.")):
            classification = "not-exposed"
            reason = "Rust execution plumbing is intentionally not a Python product facade"
        elif rust_id.startswith("CheckpointView."):
            classification = "not-exposed"
            reason = (
                "the Python historical view exposes a deliberately smaller read-only projection"
            )
        else:
            classification = "not-exposed"
            reason = "no compiled Python method with the same public identity"
        classifications[rust_id] = {
            "classification": classification,
            "python_id": python_id if classification == "equivalent" else "",
            "reason": reason,
        }

    # A newly exposed Python product method must be deliberately projected or
    # explicitly listed as Python-only. This catches silent binding expansion.
    projected = {item["python_id"] for item in classifications.values() if item["python_id"]}
    python_only = set(PYTHON_ONLY_METHODS)
    unclassified_python = sorted(python_methods - projected - python_only)
    assert not unclassified_python, f"unclassified compiled Python methods: {unclassified_python}"
    return {
        "schema": "graphforge-python-non-cypher-parity/1",
        "rust_public_method_digest": EXPECTED_RUST_DIGEST,
        "rust_release_method_digest": EXPECTED_RELEASE_DIGEST,
        "python_pyo3_method_digest": _digest(python_methods),
        "classifications": classifications,
        "release_classifications": {
            method_id: classifications[method_id] for method_id in sorted(release_methods)
        },
        "evidence": EVIDENCE,
        "python_only": sorted(python_only),
    }


def check_surface_projection() -> None:
    report = _classification_report()
    output = os.environ.get("GRAPHFORGE_PYTHON_PARITY_REPORT")
    if output:
        Path(output).write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")


def _code(error: BaseException) -> str | None:
    return getattr(error, "code", None)


def _expect_code(exception, code: str, call) -> BaseException:
    try:
        call()
    except exception as error:
        assert _code(error) == code, (_code(error), error)
        return error
    raise AssertionError(f"expected {exception.__name__} with {code}")


def check_stable_error_code_matrix() -> None:
    """Exercise public errors through the freshly built extension, not message parsing."""
    import graphforge as g

    forge = g.GraphForge()
    parse = _expect_code(g.ParseError, "GF_PARSE", lambda: forge.execute("MATCH ("))
    assert parse.span == (7, 0)
    assert isinstance(parse, g.GraphForgeError)
    _expect_code(g.ParseError, "GF_PARSE", lambda: forge.execute("RETURN absent"))
    forge.execute("CREATE (:Vertex)-[:LINK]->(:Vertex)")
    _expect_code(
        g.ExecutionError,
        "GF_EXECUTION",
        lambda: forge.analyze(by="euler_circuit", directed=False),
    )
    _expect_code(g.ValidationError, "GF_VALIDATION", lambda: forge.list_checkpoints(limit=0))
    _expect_code(
        g.ValidationError,
        "GF_PAGE_INVALID",
        lambda: forge.list_checkpoints(after="future-version-token"),
    )
    _expect_code(
        g.StorageError,
        "GF_UNSUPPORTED_CAPABILITY_VERSION",
        lambda: forge.enable_capability(
            operation_uuid="018f0f4e-7b8c-7000-8000-000000002500",
            capability_id="knowledge",
            capability_version=2,
        ),
    )
    for method in ("begin", "commit", "rollback"):
        assert not hasattr(forge, method), method

    context = multiprocessing.get_context("spawn")
    ready: multiprocessing.Queue = context.Queue()
    server = context.Process(target=_provider_error_server, args=(ready,))
    server.start()
    origin = ready.get(timeout=10)
    try:
        provider_forge = g.GraphForge()
        provider_forge.add_node("Document", body="provider failure")
        provider_forge.configure_openrouter(
            "redacted-test-credential",
            origin=origin,
            model="vendor/model",
            transport_timeout_millis=5_000,
        )
        provider_forge.publish_provider_embeddings("failed", "Document", ["body"], dimensions=2)
        error = _expect_code(
            g.ExecutionError,
            "GF_EXECUTION",
            lambda: provider_forge.find(
                label="Document",
                semantic_query="query",
                space="failed",
                suppress_rerank_advisory=True,
            ),
        )
        assert error.provider == "openrouter"
        assert error.model == "vendor/model"
        assert error.provider_class
    finally:
        server.terminate()
        server.join(timeout=2)

    cancelled = g.CancellationToken()
    cancelled.cancel()
    _expect_code(
        g.ValidationError,
        "GF_CANCELLED",
        lambda: forge.list_checkpoints(cancellation=cancelled),
    )

    with tempfile.TemporaryDirectory() as directory:
        invalid = Path(directory) / "invalid.yaml"
        invalid.write_text("ontology_id: [")
        _expect_code(g.OntologyError, "GF_ONTOLOGY", lambda: forge.load_ontology(str(invalid)))

    forge.close()
    _expect_code(g.LifecycleError, "GF_LIFECYCLE", forge.project_capabilities)

    # Python/PyO3 rejects these before a GfError exists; no semantic code is invented.
    try:
        g.GraphForge(unknown=True)
    except TypeError as error:
        assert not hasattr(error, "code")
    else:
        raise AssertionError("unknown constructor keyword must fail in PyO3 conversion")
    try:
        g.GraphForge().list_checkpoints(limit=2**100)
    except OverflowError as error:
        assert not hasattr(error, "code")
    else:
        raise AssertionError("out-of-range integer must fail in PyO3 conversion")


def check_native_artifact_and_no_fallback() -> None:
    import graphforge as g
    from graphforge import _graphforge_rs as native

    assert Path(native.__file__).suffix in {".so", ".pyd", ".dylib"}, native.__file__
    assert g.GraphForge is native.GraphForge
    for compatibility_file in [
        ROOT / "crates/graphforge-bindings-py/python/graphforge/__init__.py",
        ROOT / "crates/graphforge-bindings-py/python/graphforge/api.py",
    ]:
        source = compatibility_file.read_text()
        assert "from graphforge._graphforge_rs import" in source
        assert not re.search(
            r"^\s*def\s+(rank|cluster|paths|analyze|similar|find)\s*\(", source, re.M
        )


def check_lifecycle_checkpoint_errors_and_reopen() -> None:
    import pyarrow as pa

    import graphforge as g

    with tempfile.TemporaryDirectory() as directory:
        forge = g.GraphForge(directory)
        alice = forge.add_node("Person", name="Alice")
        bob = forge.add_node("Person", name="Bob")
        forge.add_edge(alice, "KNOWS", bob, since=2026)
        assert forge.labels() == ["Person"]
        assert forge.relationship_types() == ["KNOWS"]
        assert forge.node_count() == 2
        assert forge.node_count("Person") == 2
        assert forge.node_count("Person') MATCH (n) RETURN n //") == 0
        inspection_schema = forge.schema()
        assert inspection_schema.to_pydict() == {
            "label": ["Person", None],
            "node_count": [2, None],
            "rel_type": [None, "KNOWS"],
            "rel_count": [None, 1],
        }
        checkpoint = forge.checkpoint(
            name="baseline",
            idempotency_key="018f0f4e-7b8c-7000-8000-000000002430",
        )
        assert isinstance(checkpoint, pa.Table)
        assert forge.list_checkpoints(limit=1).column("name").to_pylist() == ["baseline"]
        view = forge.open_checkpoint("baseline")
        assert view.execute("MATCH (n:Person) RETURN n.name AS name ORDER BY name").column(
            "name"
        ).to_pylist() == ["Alice", "Bob"]
        assert not hasattr(view, "add_node") and not hasattr(view, "checkpoint")

        cancelled = g.CancellationToken()
        cancelled.cancel()
        assert cancelled.is_cancelled
        try:
            forge.list_checkpoints(cancellation=cancelled)
        except g.ValidationError as error:
            assert _code(error) == "GF_CANCELLED", _code(error)
        else:
            raise AssertionError("cancelled checkpoint listing must fail")

        try:
            forge.list_checkpoints(limit=0)
        except g.ValidationError as error:
            assert _code(error) == "GF_VALIDATION", _code(error)
        else:
            raise AssertionError("zero limit must fail")

        try:
            forge.list_checkpoints(after="future-version-token")
        except g.ValidationError as error:
            assert _code(error), "page-token failures must retain a structured code"
        else:
            raise AssertionError("invalid/future page token must fail")

        forge.close()
        try:
            forge.project_capabilities()
        except g.LifecycleError as error:
            assert _code(error) == "GF_LIFECYCLE", _code(error)
        else:
            raise AssertionError("operation after close must fail")

        reopened = g.GraphForge(directory)
        rows = reopened.execute(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
            "RETURN a.name AS src, b.name AS dst, r.since AS since"
        )
        assert rows.to_pydict() == {"src": ["Alice"], "dst": ["Bob"], "since": [2026]}
        assert reopened.labels() == ["Person"]
        assert reopened.relationship_types() == ["KNOWS"]
        assert reopened.node_count() == 2
        assert reopened.schema().equals(inspection_schema)
        assert reopened.rank("Person", by="degree").column("score").to_pylist() == [1.0, 0.0]
        reopened.index("Person", properties=["name"], rebuild=True)
        assert reopened.find("alice", label="Person").column("name").to_pylist() == ["Alice"]
        reopened.close()


def check_knowledge_epistemic_native_projection() -> None:
    """Exercise the shipped provenance/knowledge path without a Python implementation."""
    import graphforge as g

    with tempfile.TemporaryDirectory() as directory:
        forge = g.GraphForge(directory)
        node = forge.add_node("Finding", title="Native evidence")
        forge.enable_capability(
            operation_uuid="018f0f4e-7b8c-7000-8000-000000002431",
            capability_id="provenance",
            capability_version=1,
        )
        forge.enable_capability(
            operation_uuid="018f0f4e-7b8c-7000-8000-000000002432",
            capability_id="knowledge",
            capability_version=1,
        )
        assertion_uuid = "018f0f4e-7b8c-7000-8000-000000002433"
        created = forge.create_assertion(
            operation_uuid="018f0f4e-7b8c-7000-8000-000000002434",
            assertion_uuid=assertion_uuid,
            claim="The binding delegates knowledge writes to Rust.",
            graph_refs=[
                {"graph_uuid": node.uuid, "graph_kind": "node", "role": "subject", "ordinal": 0}
            ],
        )
        assert created.num_rows == 1
        assert forge.assertion(assertion_uuid).num_rows == 1
        assert forge.assertion_graph_refs(assertion_uuid).num_rows == 1
        history = forge.list_provenance_history(limit=100)
        assert history.num_rows > 0
        forge.close()

        reopened = g.GraphForge(directory)
        assert reopened.assertion(assertion_uuid).num_rows == 1
        assert reopened.list_provenance_history(limit=100).num_rows == history.num_rows
        reopened.close()


def main() -> None:
    check_surface_projection()
    if "--classification-only" in sys.argv:
        return
    check_native_artifact_and_no_fallback()
    check_stable_error_code_matrix()
    check_lifecycle_checkpoint_errors_and_reopen()
    check_knowledge_epistemic_native_projection()


if __name__ == "__main__":
    main()
