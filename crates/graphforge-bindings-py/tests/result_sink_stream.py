"""Streaming Parquet/Arrow IPC result sinks through the Python binding."""

from __future__ import annotations

from pathlib import Path
import tempfile


def check_result_sink_stream() -> None:
    import graphforge as gf

    forge = gf.GraphForge()
    for name in ("a", "b", "c"):
        forge.execute(f"CREATE (:Person {{name: '{name}'}})")
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        parquet = root / "stream.parquet"
        ipc = root / "stream.arrow"
        query = "MATCH (p:Person) RETURN p.name AS name ORDER BY name"
        parquet_receipt = forge.execute_to_parquet_stream(
            query, str(parquet), max_batch_rows=64, max_row_group_rows=2
        )
        ipc_receipt = forge.execute_to_arrow_ipc_stream(
            query, str(ipc), max_batch_rows=64, max_row_group_rows=2
        )
        assert parquet_receipt["progress"]["rows"] == 3
        assert ipc_receipt["progress"]["rows"] == 3
        assert parquet_receipt["progress"]["complete"] is True
        assert ipc_receipt["progress"]["complete"] is True
        assert parquet.exists() and ipc.exists()


def main() -> None:
    check_result_sink_stream()


if __name__ == "__main__":
    main()
