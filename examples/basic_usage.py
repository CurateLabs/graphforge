"""Native M18/M19 composition example.

This example builds a local graph, executes a Rust-owned analyst verb, performs
text/vector/hybrid search, publishes complete embedding spaces, inspects
freshness, and demonstrates configured provider planning and explicit
reranking against a deterministic loopback mock. No external service or
credential is used.
"""

from __future__ import annotations

import argparse
import json
import multiprocessing
from pathlib import Path
import socket
import tempfile
from typing import Any
from uuid import UUID
import warnings

import pyarrow as pa

import graphforge as g

RESULT_PREFIX = "GRAPHFORGE_CONSUMER_RESULT="
SEARCH_FIELDS = {"node_uuid", "score", "matched_on"}


def _read_request(connection: socket.socket) -> tuple[str, dict[str, Any]]:
    received = bytearray()
    while b"\r\n\r\n" not in received:
        chunk = connection.recv(4096)
        if not chunk:
            raise ConnectionError("provider request ended before its headers")
        received.extend(chunk)
    headers, body = bytes(received).split(b"\r\n\r\n", 1)
    length = next(
        int(line.split(b":", 1)[1].strip())
        for line in headers.split(b"\r\n")
        if line.lower().startswith(b"content-length:")
    )
    while len(body) < length:
        chunk = connection.recv(4096)
        if not chunk:
            raise ConnectionError("provider request ended before its body")
        body += chunk
    return headers.decode(), json.loads(body[:length])


def _mock_provider(ready: Any, result: Any) -> None:
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    ready.put(f"http://127.0.0.1:{listener.getsockname()[1]}")
    try:
        for call in range(4):
            connection, _ = listener.accept()
            with connection:
                headers, payload = _read_request(connection)
                assert "authorization: bearer local-test-credential" in headers.lower()
                if call == 0:
                    assert headers.startswith("POST /api/v1/embeddings ")
                    assert len(payload["input"]) == 2
                    response = {
                        "model": "local/mock-model",
                        "data": [
                            {"index": 0, "embedding": [1.0, 0.0]},
                            {"index": 1, "embedding": [0.0, 1.0]},
                        ],
                    }
                elif call in (1, 2):
                    assert headers.startswith("POST /api/v1/embeddings ")
                    assert isinstance(payload["input"], str)
                    response = {
                        "model": "local/mock-model",
                        "data": [{"index": 0, "embedding": [1.0, 0.0]}],
                    }
                else:
                    assert headers.startswith("POST /api/v1/rerank ")
                    assert len(payload["documents"]) == 2
                    response = {
                        "model": "local/mock-model",
                        "results": [
                            {"index": 0, "relevance_score": 0.1},
                            {"index": 1, "relevance_score": 0.9},
                        ],
                    }
                encoded = json.dumps(response).encode()
                connection.sendall(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                    + f"Content-Length: {len(encoded)}\r\nConnection: close\r\n\r\n".encode()
                    + encoded
                )
        listener.settimeout(1.0)
        try:
            listener.accept()
        except TimeoutError:
            result.put(4)
        else:
            result.put("unexpected provider request")
    except Exception as error:  # pragma: no cover - relayed to the parent process
        result.put(repr(error))
    finally:
        listener.close()


def _assert_search_table(table: pa.Table, allowed_markers: set[str]) -> None:
    assert isinstance(table, pa.Table)
    assert SEARCH_FIELDS.issubset(table.column_names)
    assert table.schema.field("node_uuid").type == pa.binary(16)
    assert table.schema.field("score").type == pa.float64()
    assert set(table.column("matched_on").to_pylist()).issubset(allowed_markers)
    assert all(len(value) == 16 for value in table.column("node_uuid").to_pylist())


def _configured_provider_workflow(forge: g.GraphForge) -> dict[str, bool]:
    context = multiprocessing.get_context("spawn")
    ready = context.Queue()
    result = context.Queue()
    server = context.Process(target=_mock_provider, args=(ready, result))
    server.daemon = True
    server.start()
    origin = ready.get(timeout=10)
    assert origin.startswith("http://127.0.0.1:")

    forge.add_node("Document", title="Native graph search")
    forge.add_node("Document", title="Deterministic Arrow results")
    forge.configure_openrouter(
        "local-test-credential",
        origin=origin,
        model="local/mock-model",
        revision="fixture-v1",
        capabilities=[
            "document_embeddings",
            "query_embeddings",
            "candidate_reranking",
        ],
        max_input_tokens=10_000,
    )
    plan = forge.inspect_provider_embedding_plan("documents", "Document", ["title"], dimensions=2)
    assert plan["provider"] == "openrouter"
    assert plan["model"] == "local/mock-model"
    assert plan["tokenizer_identifier"] == "graphforge_utf8_byte_upper_bound"
    assert plan["tokenizer_version"] == "v1"
    assert plan["tokenizer_normalization"] == "utf8_bytes_v1"
    assert plan["token_count_class"] == "approximate"
    assert plan["model_input_tokens"] == 10_000
    assert plan["chunking"] is None
    assert plan["selected_nodes"] == 2
    assert "Native graph search" not in repr(plan)
    assert "local-test-credential" not in repr(plan)

    published = forge.publish_provider_embeddings("documents", "Document", ["title"], dimensions=2)
    assert published["producer"]["provider"] == "openrouter"

    with warnings.catch_warnings(record=True) as emitted:
        warnings.simplefilter("always")
        advisory = forge.find(vector=[1.0, 0.0], label="Document", space="documents", limit=2)
    assert len(emitted) == 1
    assert "reranker" in str(emitted[0].message)
    suppressed = forge.find(
        vector=[1.0, 0.0],
        label="Document",
        space="documents",
        limit=2,
        suppress_rerank_advisory=True,
    )
    assert advisory.equals(suppressed)

    semantic = forge.find(
        label="Document",
        semantic_query="graph",
        space="documents",
        limit=2,
        suppress_rerank_advisory=True,
    )
    reranked = forge.find(
        label="Document",
        semantic_query="graph",
        space="documents",
        limit=2,
        rerank={
            "query": "graph",
            "properties": ["title"],
            "candidate_depth": 2,
            "failure_policy": "error",
        },
    )
    _assert_search_table(semantic, {"vector"})
    _assert_search_table(reranked, {"vector"})
    assert semantic.schema == reranked.schema
    assert set(semantic.column("node_uuid").to_pylist()) == set(
        reranked.column("node_uuid").to_pylist()
    )
    assert semantic.column("node_uuid")[0].as_py() == reranked.column("node_uuid")[1].as_py()

    server.join(timeout=10)
    assert not server.is_alive()
    assert result.get(timeout=2) == 4
    return {
        "provider_plan": True,
        "semantic_query": True,
        "rerank": True,
        "rerank_advisory": True,
    }


def run() -> dict[str, Any]:
    """Execute the installed-wheel M18/M19 workflow and return evidence."""
    module_path = Path(g.__file__).resolve()
    repository = Path(__file__).resolve().parents[1]
    source_package = repository / "crates/gf-bindings-py/python/graphforge"
    assert not module_path.is_relative_to(source_package)

    with tempfile.TemporaryDirectory() as project:
        forge = g.GraphForge(project)
        people = {
            "Alice": forge.add_node(
                "Person", name="Alice", bio="Graph engineer and search specialist"
            ),
            "Bob": forge.add_node("Person", name="Bob", bio="Distributed systems engineer"),
            "Carol": forge.add_node("Person", name="Carol", bio="Knowledge graph researcher"),
        }
        for source, target in (
            ("Alice", "Bob"),
            ("Bob", "Carol"),
            ("Alice", "Carol"),
        ):
            forge.execute(
                "MATCH (source:Person {name: $source}), (target:Person {name: $target}) "
                "CREATE (source)-[:KNOWS]->(target)",
                {"source": source, "target": target},
            )

        rank = forge.rank("Person", by="degree", via="KNOWS", directed=False)
        assert isinstance(rank, pa.Table)
        assert rank.schema.field("node_uuid").type == pa.binary(16)
        assert rank.num_rows == 3

        forge.add_node("Tool", name="search", description="Search local graph properties")
        forge.add_node("Tool", name="rank", description="Rank graph nodes")
        lazy_text = forge.find("search", label="Tool", limit=2)
        _assert_search_table(lazy_text, {"text"})
        assert lazy_text.num_rows >= 1

        forge.index("Person", properties=["name", "bio"])
        text = forge.find("engineer", label="Person", limit=3)
        _assert_search_table(text, {"text"})
        assert text.num_rows >= 1

        vectors = {
            "Alice": [1.0, 0.0],
            "Bob": [0.8, 0.2],
            "Carol": [0.0, 1.0],
        }
        rows = [{"node": people[name], "vector": vector} for name, vector in vectors.items()]
        primary_id = forge.publish_caller_embeddings(
            "profiles",
            rows,
            dimensions=2,
            source_projection={"label": "Person", "recipe": "profiles-v1"},
        )
        alternate_id = forge.publish_caller_embeddings(
            "profiles-alt",
            [
                {"node": people[name], "vector": list(reversed(vector))}
                for name, vector in vectors.items()
            ],
            dimensions=2,
            source_projection={"label": "Person", "recipe": "profiles-alt-v1"},
        )
        assert primary_id != alternate_id
        spaces = forge.embedding_spaces()
        assert {primary_id, alternate_id}.issubset({space["compatibility_id"] for space in spaces})
        freshness = forge.inspect_embedding_space_freshness("profiles")
        assert freshness["state"] == "fresh"
        assert freshness["decision"] == {"kind": "serve_fresh"}

        vector = forge.find(vector=[1.0, 0.0], label="Person", space="profiles", limit=3)
        hybrid = forge.find(
            "engineer",
            vector=[1.0, 0.0],
            label="Person",
            space="profiles",
            limit=3,
        )
        _assert_search_table(vector, {"vector"})
        _assert_search_table(hybrid, {"text", "vector", "text+vector"})
        assert vector.column("node_uuid")[0].as_py() == UUID(people["Alice"].uuid).bytes
        assert hybrid.num_rows >= 1

        provider = _configured_provider_workflow(forge)
        forge.close()

    return {
        "consumer": "examples/basic_usage.py",
        "installed_wheel": True,
        "m18_verbs": ["rank"],
        "m19_modes": ["hybrid", "text", "vector"],
        "explicit_index": True,
        "lazy_text_index": True,
        "atomic_embedding_publication": True,
        "multiple_embedding_spaces": True,
        "freshness_inspection": True,
        "uuid_only": True,
        "arrow_results": True,
        **provider,
    }


def main() -> None:
    """Run the example and optionally emit machine-readable CI evidence."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", action="store_true", help="emit CI evidence")
    args = parser.parse_args()
    result = run()
    if args.json:
        print(f"{RESULT_PREFIX}{json.dumps(result, sort_keys=True)}")
        return
    print("Native M18/M19 workflow completed")
    print("  analyst verb: rank")
    print("  search modes: text, vector, hybrid")
    print("  provider plan, semantic query, advisory, and reranking: verified")


if __name__ == "__main__":
    main()
