"""Deterministic Python native concurrency parity against the Rust contract (#2416)."""

from __future__ import annotations

from pathlib import Path
import tempfile
import threading
import time
from uuid import UUID

import gil_release

import graphforge as g
from graphforge import _graphforge_rs as native

DEADLINE_SECONDS = 10.0
QUERY = "MATCH (n:Person) RETURN n.name AS name ORDER BY name"


def _deadline() -> float:
    return time.monotonic() + DEADLINE_SECONDS


def _remaining(deadline: float) -> float:
    return max(0.0, deadline - time.monotonic())


def _seed(path: str) -> None:
    forge = g.GraphForge(path)
    forge.execute(
        "CREATE "
        "(alice:Person {name:'Alice'}), "
        "(bob:Person {name:'Bob'}), "
        "(carol:Person {name:'Carol'}), "
        "(alice)-[:KNOWS]->(bob), "
        "(bob)-[:KNOWS]->(carol)"
    )
    forge.close()


def _names(forge: g.GraphForge) -> list[str]:
    return forge.execute(QUERY).column("name").to_pylist()


def _join(phase: str, worker: threading.Thread, deadline: float) -> None:
    worker.join(timeout=_remaining(deadline))
    if worker.is_alive():
        raise AssertionError(f"phase={phase} timeout={DEADLINE_SECONDS}s worker hung")


def _expect_constructor_validation(kwargs: dict[str, object]) -> None:
    try:
        g.GraphForge(**kwargs)
    except g.GraphForgeError as error:
        assert error.code == "GF_VALIDATION", error.code
    else:
        raise AssertionError(f"constructor accepted invalid options: {kwargs}")


def check_gil_release_native_boundary() -> None:
    gil_release.check_native_call_inventory()
    gil_release.check_python_progresses_during_native_work()


def check_independent_and_same_instance_reads() -> None:
    deadline = _deadline()
    with tempfile.TemporaryDirectory() as first, tempfile.TemporaryDirectory() as second:
        _seed(first)
        _seed(second)
        left = g.GraphForge(first)
        right = g.GraphForge(second)
        barrier = threading.Barrier(2)
        results: dict[str, list[str] | BaseException] = {}

        def worker(label: str, forge: g.GraphForge) -> None:
            try:
                barrier.wait(timeout=_remaining(deadline))
                results[label] = _names(forge)
            except BaseException as error:
                results[label] = error

        threads = [
            threading.Thread(target=worker, args=("left", left), name="gf-py-left"),
            threading.Thread(target=worker, args=("right", right), name="gf-py-right"),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            _join("independent-instances", thread, deadline)
        for label, value in results.items():
            if isinstance(value, BaseException):
                raise AssertionError(f"phase=independent-instances worker={label}") from value
        assert results["left"] == results["right"] == ["Alice", "Bob", "Carol"]

        shared = g.GraphForge(first)
        barrier = threading.Barrier(4)
        outcomes: list[list[str] | BaseException | None] = [None] * 4

        def same_instance(index: int) -> None:
            try:
                barrier.wait(timeout=_remaining(deadline))
                outcomes[index] = _names(shared)
            except BaseException as error:
                outcomes[index] = error

        workers = [
            threading.Thread(target=same_instance, args=(index,), name=f"gf-py-same-{index}")
            for index in range(4)
        ]
        for thread in workers:
            thread.start()
        for thread in workers:
            _join("one-instance-reads", thread, deadline)
        for index, value in enumerate(outcomes):
            if isinstance(value, BaseException):
                raise AssertionError(f"phase=one-instance-reads worker={index}") from value
            assert value == ["Alice", "Bob", "Carol"], value


def check_cancellation_isolation() -> None:
    deadline = _deadline()
    forge = g.GraphForge()
    forge.checkpoint(name="one", idempotency_key="018f0f4e-7b8c-7000-8000-000000002416")
    barrier = threading.Barrier(2)
    outcomes: dict[str, object] = {}

    def cancelled_worker() -> None:
        try:
            token = g.CancellationToken()
            token.cancel()
            barrier.wait(timeout=_remaining(deadline))
            forge.list_checkpoints(cancellation=token)
            outcomes["cancelled"] = "completed"
        except g.GraphForgeError as error:
            outcomes["cancelled"] = error.code
        except BaseException as error:
            outcomes["cancelled"] = error

    def peer_worker() -> None:
        try:
            barrier.wait(timeout=_remaining(deadline))
            table = forge.list_checkpoints()
            outcomes["peer"] = table.column("name").to_pylist()
        except BaseException as error:
            outcomes["peer"] = error

    cancelled = threading.Thread(target=cancelled_worker, name="gf-py-cancel")
    peer = threading.Thread(target=peer_worker, name="gf-py-peer")
    cancelled.start()
    peer.start()
    _join("cancellation-isolation", cancelled, deadline)
    _join("cancellation-isolation", peer, deadline)
    assert outcomes["cancelled"] == "GF_CANCELLED", outcomes["cancelled"]
    peer_value = outcomes["peer"]
    if isinstance(peer_value, BaseException):
        raise AssertionError("phase=cancellation-isolation peer failed") from peer_value
    assert peer_value == ["one"], peer_value


def check_stream_early_drop_isolation() -> None:
    deadline = _deadline()
    forge = g.GraphForge()
    forge.execute("UNWIND range(1, 4000) AS i CREATE (:StreamRow {ordinal: i})")
    query = "MATCH (n:StreamRow) RETURN n.ordinal AS ordinal ORDER BY ordinal"
    expected = forge.execute(query).column("ordinal").to_pylist()
    barrier = threading.Barrier(2)
    outcomes: dict[str, object] = {}

    def drop_worker() -> None:
        try:
            reader = forge.execute_stream(query)
            barrier.wait(timeout=_remaining(deadline))
            reader.close()
            outcomes["drop"] = "closed"
        except BaseException as error:
            outcomes["drop"] = error

    def peer_worker() -> None:
        try:
            barrier.wait(timeout=_remaining(deadline))
            outcomes["peer"] = forge.execute(query).column("ordinal").to_pylist()
        except BaseException as error:
            outcomes["peer"] = error

    drop = threading.Thread(target=drop_worker, name="gf-py-stream-drop")
    peer = threading.Thread(target=peer_worker, name="gf-py-stream-peer")
    drop.start()
    peer.start()
    _join("stream-drop", drop, deadline)
    _join("stream-drop", peer, deadline)
    assert outcomes["drop"] == "closed", outcomes["drop"]
    peer_value = outcomes["peer"]
    if isinstance(peer_value, BaseException):
        raise AssertionError("phase=stream-drop peer failed") from peer_value
    assert peer_value == expected


def check_shared_directory_writer_busy_and_reopen() -> None:
    deadline = _deadline()
    with tempfile.TemporaryDirectory() as directory:
        _seed(directory)
        transactions = Path(directory, "transactions")
        long_reader = g.GraphForge(directory)
        assert _names(long_reader) == ["Alice", "Bob", "Carol"]

        native._test_acquire_writer_hold(directory)
        try:
            before = (
                sorted(path.name for path in transactions.iterdir())
                if transactions.exists()
                else []
            )
            barrier = threading.Barrier(2)
            outcomes: dict[str, object] = {}

            def contender() -> None:
                try:
                    writer = g.GraphForge(directory)
                    barrier.wait(timeout=_remaining(deadline))
                    writer.execute("CREATE (:Person {name:'Delta'})")
                    outcomes["contender"] = "published"
                except g.GraphForgeError as error:
                    outcomes["contender"] = error.code
                except BaseException as error:
                    outcomes["contender"] = error

            def reader() -> None:
                try:
                    barrier.wait(timeout=_remaining(deadline))
                    outcomes["reader"] = _names(long_reader)
                except BaseException as error:
                    outcomes["reader"] = error

            contender_thread = threading.Thread(target=contender, name="gf-py-contender")
            reader_thread = threading.Thread(target=reader, name="gf-py-reader")
            contender_thread.start()
            reader_thread.start()
            _join("writer-busy", contender_thread, deadline)
            _join("writer-busy", reader_thread, deadline)
            assert outcomes["contender"] == "GF_WRITER_BUSY", outcomes["contender"]
            reader_value = outcomes["reader"]
            if isinstance(reader_value, BaseException):
                raise AssertionError("phase=writer-busy reader failed") from reader_value
            assert reader_value == ["Alice", "Bob", "Carol"]
            after = (
                sorted(path.name for path in transactions.iterdir())
                if transactions.exists()
                else []
            )
            assert after == before, (before, after)
        finally:
            native._test_release_writer_hold()

        writer = g.GraphForge(directory)
        writer.execute("CREATE (:Person {name:'Delta'})")
        writer.close()
        assert _names(long_reader) == ["Alice", "Bob", "Carol"]
        reopened = g.GraphForge(directory)
        assert _names(reopened) == ["Alice", "Bob", "Carol", "Delta"]


def check_write_mode_options_and_optimistic_agents() -> None:
    for mode in ("single_writer", "queued_writer", "optimistic_multi_writer"):
        with tempfile.TemporaryDirectory() as directory:
            forge = g.GraphForge(
                directory,
                write_mode=mode,
                write_queue_capacity=8,
                max_rebase_attempts=4,
            )
            forge.execute("CREATE (:Person {name:'mode'})")
            forge.close()
            assert _names(g.GraphForge(directory)) == ["mode"]

    for kwargs in (
        {"write_mode": "server"},
        {"write_queue_capacity": 0},
        {"write_queue_capacity": -1},
        {"max_rebase_attempts": -1},
        {"max_rebase_attempts": 33},
    ):
        _expect_constructor_validation(kwargs)

    deadline = _deadline()
    with tempfile.TemporaryDirectory() as directory:
        agents = [
            g.GraphForge(directory, write_mode="optimistic_multi_writer"),
            g.GraphForge(directory, write_mode="optimistic_multi_writer"),
        ]
        operations = [
            "018f0f4e-7b8c-7000-8000-000000002141",
            "018f0f4e-7b8c-7000-8000-000000002142",
        ]
        nodes = [
            "018f0f4e-7b8c-7000-8000-000000002143",
            "018f0f4e-7b8c-7000-8000-000000002144",
        ]
        barrier = threading.Barrier(2)
        outcomes: list[object | None] = [None, None]

        def publish(index: int) -> None:
            try:
                barrier.wait(timeout=_remaining(deadline))
                outcomes[index] = agents[index].publish_composite_transaction(
                    operation_uuid=operations[index],
                    graph_mutations=[
                        {
                            "kind": "create_node",
                            "node_uuid": nodes[index],
                            "label": "Person",
                            "properties": {"name": f"agent-{index}"},
                        }
                    ],
                )
            except BaseException as error:
                outcomes[index] = error

        workers = [threading.Thread(target=publish, args=(index,)) for index in range(2)]
        for worker in workers:
            worker.start()
        for worker in workers:
            _join("optimistic-agents", worker, deadline)
        for index, outcome in enumerate(outcomes):
            if isinstance(outcome, BaseException):
                raise AssertionError(f"optimistic agent {index} failed") from outcome
            assert outcome.column("request_identity").to_pylist() == [UUID(operations[index]).bytes]

        reopened = g.GraphForge(directory)
        assert _names(reopened) == ["agent-0", "agent-1"]
        before = _names(reopened)
        try:
            agents[0].publish_composite_transaction(
                operation_uuid=operations[0],
                graph_mutations=[
                    {
                        "kind": "create_node",
                        "node_uuid": "018f0f4e-7b8c-7000-8000-000000002145",
                        "label": "Person",
                        "properties": {"name": "conflict"},
                    }
                ],
            )
        except g.GraphForgeError as error:
            assert error.code == "GF_IDEMPOTENCY_CONFLICT", error.code
        else:
            raise AssertionError("conflicting agent operation was accepted")
        assert _names(g.GraphForge(directory)) == before


def main() -> None:
    check_gil_release_native_boundary()
    check_independent_and_same_instance_reads()
    check_cancellation_isolation()
    check_stream_early_drop_isolation()
    check_shared_directory_writer_busy_and_reopen()
    check_write_mode_options_and_optimistic_agents()
    print("python concurrency parity passed")


if __name__ == "__main__":
    main()
