"""Portable-v2 export/verify/import identity parity through the Python binding."""

from __future__ import annotations

import json
from pathlib import Path
import tempfile
import uuid


def check_portable_v2_parity() -> None:
    import graphforge as gf

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source = root / "source"
        source.mkdir()
        forge = gf.GraphForge(str(source))
        forge.execute("CREATE (:Person {name: 'Ada'})")
        preview = forge.preview_portable_v2_selection(profile="complete")
        assert preview["package_class"] == "complete"
        assert preview["include_graph_tree"] is True
        assert preview["projected"] == []
        node_bytes = (
            forge.execute("MATCH (n:Person) RETURN n.node_uuid AS id").column("id")[0].as_py()
        )
        node_id = str(uuid.UUID(bytes=node_bytes))
        subset = forge.preview_portable_v2_graph_subset(
            subset={"selector": {"node_uuids": [node_id]}, "closure": "induced_edges"}
        )
        assert subset["selected_node_count"] == 1
        assert subset["selection"]["include_graph_tree"] is True
        assert subset["selection"]["projected"] == []
        expanded = root / "expanded"
        bundle = root / "complete.gfpb"
        expanded_export = forge.export_portable_v2(
            output_path=str(expanded), representation="expanded", profile="complete"
        )
        bundle_export = forge.export_portable_v2(
            output_path=str(bundle), representation="bundle", profile="complete"
        )
        assert expanded_export["package_digest"] == bundle_export["package_digest"]
        assert expanded_export["selection_fingerprint"] == preview["selection_fingerprint"]

        events: list[dict[str, object]] = []

        def progress(event: dict[str, object]) -> None:
            events.append(event)
            raise RuntimeError("progress callback failed")

        try:
            forge.export_portable_v2(
                output_path=str(root / "callback-fail.gfpb"),
                representation="bundle",
                profile="complete",
                progress=progress,
            )
            raise AssertionError("expected progress callback failure to propagate")
        except RuntimeError as error:
            assert "progress callback failed" in str(error)
        assert events, "progress callback must run at least once"

        try:
            forge.export_portable_v2(
                output_path=str(root / "not-callable.gfpb"),
                representation="bundle",
                profile="complete",
                progress=object(),
            )
        except gf.ValidationError as error:
            assert "callable" in str(error).lower()
        else:
            raise AssertionError("expected non-callable progress to fail closed")

        verified = gf.GraphForge.verify_portable_v2(str(bundle), mode="full")
        assert verified["package_digest"] == bundle_export["package_digest"]
        target = root / "target"
        operation = str(uuid.uuid4())
        imported = gf.GraphForge.import_portable_v2(
            str(target),
            input=str(bundle),
            operation_id=operation,
        )
        assert imported["package_digest"] == bundle_export["package_digest"]
        assert not imported["idempotent_replay"]
        replay = gf.GraphForge.import_portable_v2(
            str(target), input=str(bundle), operation_id=operation
        )
        assert replay["idempotent_replay"]
        assert replay["generation_uuid"] == imported["generation_uuid"]
        assert replay["package_digest"] == imported["package_digest"]
        reopened = gf.GraphForge(str(target))
        assert (
            json.loads((target / "CURRENT").read_text())["generation_uuid"]
            == imported["generation_uuid"]
        )
        assert reopened.path is not None
        assert reopened.execute("MATCH (n:Person) RETURN n.name AS name").column(
            "name"
        ).to_pylist() == ["Ada"]


def check_shared_verification_receipts() -> None:
    import graphforge as gf

    root = Path(__file__).resolve().parents[3]
    package = root / "tests/fixtures/hub/generated/v1/objects/openalex-openalex.gfpb"
    expected = json.loads(
        (root / "tests/fixtures/portable-v2/facade-verification-receipts.json").read_text()
    )
    for mode in ("full", "structure_only"):
        assert gf.GraphForge.verify_portable_v2(str(package), mode=mode) == expected[mode]


def main() -> None:
    check_portable_v2_parity()
    check_shared_verification_receipts()


if __name__ == "__main__":
    main()
