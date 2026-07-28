"""Fresh-wheel acceptance for the configured provider workflow."""

import json
import multiprocessing
import socket
import tempfile
from uuid import UUID
import warnings

import graphforge as g


def _read_request(connection: socket.socket) -> tuple[str, dict]:
    received = bytearray()
    while b"\r\n\r\n" not in received:
        chunk = connection.recv(4096)
        if not chunk:
            raise ConnectionError("client closed before headers completed")
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
            raise ConnectionError("client closed before body completed")
        body += chunk
    return headers.decode(), json.loads(body[:length])


def _mock_provider(ready: multiprocessing.Queue, result: multiprocessing.Queue) -> None:
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    ready.put(f"http://127.0.0.1:{listener.getsockname()[1]}")
    try:
        for call in range(4):
            connection, _ = listener.accept()
            with connection:
                headers, payload = _read_request(connection)
                assert "authorization: bearer test-secret" in headers.lower()
                if call == 0:
                    assert headers.startswith("POST /api/v1/embeddings ")
                    assert len(payload["input"]) == 2
                    response = {
                        "model": "vendor/model",
                        "data": [
                            {"index": 0, "embedding": [1.0, 0.0]},
                            {"index": 1, "embedding": [0.0, 1.0]},
                        ],
                    }
                elif call in (1, 2):
                    assert headers.startswith("POST /api/v1/embeddings ")
                    assert isinstance(payload["input"], str)
                    response = {
                        "model": "vendor/model",
                        "data": [{"index": 0, "embedding": [1.0, 0.0]}],
                    }
                else:
                    assert headers.startswith("POST /api/v1/rerank ")
                    assert len(payload["documents"]) == 2
                    response = {
                        "model": "vendor/model",
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
            result.put("unexpected fourth provider request")
    except Exception as error:  # pragma: no cover - relayed to parent
        result.put(repr(error))
    finally:
        listener.close()


def _expect_error(fragment: str, call) -> str:
    try:
        call()
    except g.GraphForgeError as error:
        rendered = str(error)
        assert fragment in rendered, rendered
        assert "test-secret" not in rendered
        return rendered
    raise AssertionError(f"expected GraphForgeError containing {fragment!r}")


def check_configured_provider_workflow() -> None:
    context = multiprocessing.get_context("spawn")
    ready: multiprocessing.Queue = context.Queue()
    result: multiprocessing.Queue = context.Queue()
    server = context.Process(target=_mock_provider, args=(ready, result))
    server.daemon = True
    server.start()
    origin = ready.get(timeout=10)
    with tempfile.TemporaryDirectory() as project:
        forge = g.GraphForge(project)
        first = forge.add_node("Paper", title="First")
        second = forge.add_node("Paper", title="Second")
        _expect_error(
            "origin",
            lambda: forge.configure_openrouter(
                "test-secret", origin="https://example.com/path", model="vendor/model"
            ),
        )
        _expect_error(
            "authentication",
            lambda: forge.configure_openrouter("", origin=origin, model="vendor/model"),
        )
        _expect_error(
            "provider model",
            lambda: forge.configure_openrouter("test-secret", origin=origin, model=" "),
        )
        _expect_error(
            "non-zero",
            lambda: forge.configure_openrouter(
                "test-secret", origin=origin, model="vendor/model", max_input_tokens=0
            ),
        )
        forge.configure_openrouter(
            "test-secret",
            origin=origin,
            model="vendor/model",
            revision="revision",
            capabilities=[
                "document_embeddings",
                "query_embeddings",
                "candidate_reranking",
            ],
            max_input_tokens=10_000,
        )
        inspection = forge.inspect_provider_embedding_plan(
            "semantic", "Paper", ["title"], dimensions=2
        )
        assert inspection["provider"] == "openrouter"
        assert inspection["model"] == "vendor/model"
        assert inspection["token_count_class"] == "approximate"
        assert inspection["model_input_tokens"] == 10_000
        assert inspection["properties"] == ["title"]
        assert inspection["selected_nodes"] == 2
        assert inspection["input_tokens"] > 0
        assert inspection["batches"]
        assert inspection["request_limits"] == {
            "items": 1_024,
            "input_bytes": 8 * 1024 * 1024,
            "input_tokens": 1_000_000,
            "output_values": 16_777_216,
            "provider_calls": 128,
        }
        assert inspection["execution_limits"]["provider_calls"] == 128
        assert inspection["execution_limits"]["retries"] == 2
        assert inspection["execution_limits"]["timeout_millis"] == 30_000
        assert "First" not in repr(inspection)
        assert "Second" not in repr(inspection)
        assert "test-secret" not in repr(inspection)

        published = forge.publish_provider_embeddings("semantic", "Paper", ["title"], dimensions=2)
        assert published["producer"] == {
            "kind": "remote",
            "provider": "openrouter",
            "model": "vendor/model",
            "revision": "revision",
            "response_contract_version": "v1",
        }

        baseline = forge.find(
            vector=[1.0, 0.0],
            label="Paper",
            space="semantic",
            limit=2,
            suppress_rerank_advisory=True,
        )
        with warnings.catch_warnings(record=True) as emitted:
            warnings.simplefilter("always")
            advisory = forge.find(vector=[1.0, 0.0], label="Paper", space="semantic", limit=2)
        assert len(emitted) == 1
        assert "reranker" in str(emitted[0].message)
        suppressed = forge.find(
            vector=[1.0, 0.0],
            label="Paper",
            space="semantic",
            limit=2,
            suppress_rerank_advisory=True,
        )
        assert advisory.equals(suppressed)
        assert baseline.equals(suppressed)

        semantic = forge.find(
            label="Paper",
            semantic_query="meaning",
            space="semantic",
            limit=2,
            suppress_rerank_advisory=True,
        )
        assert semantic.schema == baseline.schema
        assert semantic["node_uuid"].to_pylist() == baseline["node_uuid"].to_pylist()

        reranked = forge.find(
            label="Paper",
            semantic_query="meaning",
            space="semantic",
            limit=2,
            rerank={
                "query": "meaning",
                "properties": ["title"],
                "candidate_depth": 2,
                "failure_policy": "error",
            },
        )
        assert reranked.schema == semantic.schema
        assert set(reranked["node_uuid"].to_pylist()) == {
            UUID(first.uuid).bytes,
            UUID(second.uuid).bytes,
        }
        assert reranked["node_uuid"][0].as_py() == semantic["node_uuid"][1].as_py()

        _expect_error(
            "failure policy",
            lambda: forge.find(
                label="Paper",
                semantic_query="meaning",
                space="semantic",
                limit=2,
                rerank={
                    "query": "meaning",
                    "properties": ["title"],
                    "candidate_depth": 2,
                    "failure_policy": "unknown",
                },
            ),
        )
        _expect_error(
            "candidate_depth",
            lambda: forge.find(
                label="Paper",
                semantic_query="meaning",
                space="semantic",
                limit=2,
                rerank={
                    "query": "meaning",
                    "properties": ["title"],
                    "candidate_depth": 1,
                },
            ),
        )

        forge.set_embedding_refresh_project_policy(
            proactive=False, debounce_millis=250, max_concurrent_jobs=2
        )
        forge.add_node("Paper", title="text that is too large")
        with warnings.catch_warnings(record=True) as stale_warnings:
            warnings.simplefilter("always")
            stale = forge.find(
                vector=[1.0, 0.0],
                label="Paper",
                space="semantic",
                limit=2,
                force_stale=True,
                suppress_rerank_advisory=True,
            )
        assert stale.num_rows == 2
        assert len(stale_warnings) == 1
        assert "stale" in str(stale_warnings[0].message).lower()
        forge.configure_openrouter(
            "test-secret",
            origin=origin,
            model="vendor/model",
            revision="revision",
            max_input_tokens=2,
        )
        _expect_error(
            "resource",
            lambda: forge.inspect_provider_embedding_plan(
                "too-small", "Paper", ["title"], dimensions=2
            ),
        )
        _expect_error(
            "unknown provider capability",
            lambda: forge.configure_openrouter(
                "test-secret",
                origin=origin,
                model="vendor/model",
                capabilities=["unknown"],
            ),
        )
        forge.close()
    server.join(timeout=10)
    assert not server.is_alive()
    assert result.get(timeout=2) == 4


if __name__ == "__main__":
    check_configured_provider_workflow()
