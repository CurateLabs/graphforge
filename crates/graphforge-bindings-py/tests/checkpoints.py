"""Native Python checkpoint acceptance smoke test."""

import tempfile
import uuid

import pyarrow as pa

import graphforge as g


def main() -> None:
    with tempfile.TemporaryDirectory() as directory:
        forge = g.GraphForge(directory)
        forge.execute("CREATE (:Person {name: 'before'})")
        receipt = forge.checkpoint(
            name="Before",
            idempotency_key=uuid.UUID("018f0f4e-7b8c-7000-8000-000000002480"),
        )
        assert isinstance(receipt, pa.Table)

        checkpoints = forge.list_checkpoints(limit=1)
        assert checkpoints.column("name").to_pylist() == ["Before"]
        view = forge.open_checkpoint("Before")
        assert view.checkpoint_uuid == str(
            uuid.UUID(bytes=receipt.column("checkpoint_uuid")[0].as_py())
        )
        assert view.generation_uuid == str(
            uuid.UUID(bytes=receipt.column("source_generation_uuid")[0].as_py())
        )
        assert view.execute("MATCH (n) RETURN n").num_rows == 1
        assert isinstance(view.project_capabilities(), pa.Table)
        assert view.inspect_adjacency()["state"] == "missing"
        assert not hasattr(view, "checkpoint")
        assert not hasattr(view, "revert_to_checkpoint")

        forge.execute("CREATE (:Person {name: 'after'})")
        diff = forge.diff_checkpoints(from_checkpoint="Before", scope="graph", detail="records")
        assert diff.num_rows > 0
        reverted = forge.revert_to_checkpoint(
            name="Before",
            reason="python binding acceptance",
            idempotency_key="018f0f4e-7b8c-7000-8000-000000002481",
        )
        assert reverted.column("result_generation_uuid")[0].as_py() is not None
        assert forge.execute("MATCH (n) RETURN n").num_rows == 1

        deleted = forge.delete_checkpoint(
            name="Before",
            idempotency_key="018f0f4e-7b8c-7000-8000-000000002482",
        )
        assert deleted.column("operation").to_pylist() == ["delete_checkpoint"]


if __name__ == "__main__":
    main()
