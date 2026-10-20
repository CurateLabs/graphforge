"""Portable export/verify/import identity parity through the Python binding."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys
import tempfile
import uuid


def check_portable_parity() -> None:
    import graphforge as gf

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source = root / "source"
        source.mkdir()
        forge = gf.GraphForge(str(source))
        forge.execute("CREATE (:Person {name: 'Ada'})")
        preview = forge.preview_portable_selection(profile="complete")
        assert preview["package_class"] == "complete"
        assert preview["include_graph_tree"] is True
        assert preview["projected"] == []
        node_bytes = (
            forge.execute("MATCH (n:Person) RETURN n.node_uuid AS id").column("id")[0].as_py()
        )
        node_id = str(uuid.UUID(bytes=node_bytes))
        subset = forge.preview_portable_graph_subset(
            subset={"selector": {"node_uuids": [node_id]}, "closure": "induced_edges"}
        )
        assert subset["selected_node_count"] == 1
        assert subset["selection"]["include_graph_tree"] is True
        assert subset["selection"]["projected"] == []
        expanded = root / "expanded"
        bundle = root / "complete.gfpb"
        expanded_export = forge.export_portable(
            output_path=str(expanded), representation="expanded", profile="complete"
        )
        bundle_export = forge.export_portable(
            output_path=str(bundle), representation="bundle", profile="complete"
        )
        assert expanded_export["package_digest"] == bundle_export["package_digest"]
        assert expanded_export["selection_fingerprint"] == preview["selection_fingerprint"]

        events: list[dict[str, object]] = []

        def progress(event: dict[str, object]) -> None:
            events.append(event)
            raise RuntimeError("progress callback failed")

        try:
            forge.export_portable(
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
            forge.export_portable(
                output_path=str(root / "not-callable.gfpb"),
                representation="bundle",
                profile="complete",
                progress=object(),
            )
        except gf.ValidationError as error:
            assert "callable" in str(error).lower()
        else:
            raise AssertionError("expected non-callable progress to fail closed")

        verified = gf.GraphForge.verify_portable(str(bundle), mode="full")
        assert verified["package_digest"] == bundle_export["package_digest"]
        target = root / "target"
        operation = str(uuid.uuid4())
        imported = gf.GraphForge.import_portable(
            str(target),
            input=str(bundle),
            operation_id=operation,
        )
        assert imported["package_digest"] == bundle_export["package_digest"]
        assert not imported["idempotent_replay"]
        replay = gf.GraphForge.import_portable(
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
        (root / "tests/fixtures/portable/facade-verification-receipts.json").read_text()
    )
    for mode in ("full", "structure_only"):
        assert gf.GraphForge.verify_portable(str(package), mode=mode) == expected[mode]


def check_retired_format_is_rejected_before_target_creation() -> None:
    import graphforge as gf

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source = root / "unsupported.bin"
        source.write_bytes(b"graphforge-exp\0\n")
        target = root / "target"
        try:
            gf.GraphForge.import_portable(
                project_root=str(target), input=str(source), operation_id=str(uuid.uuid4())
            )
        except Exception as error:
            assert getattr(error, "code", None) == "GF_UNSUPPORTED_PROJECT_FORMAT"
        else:
            raise AssertionError("retired format was accepted")
        assert not target.exists()


def canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def package_too_large_for_any_disk(root: Path) -> Path:
    """An expanded package whose resealed manifest declares eight tebibytes."""
    import graphforge as gf

    source = root / "source"
    source.mkdir()
    package = root / "too-large.gfproject"
    gf.GraphForge(str(source)).export_portable(
        output_path=str(package), representation="expanded", profile="complete"
    )
    manifest_path = package / "data" / "graphforge-project.json"
    manifest = json.loads(manifest_path.read_text())
    del manifest["package_digest"]
    manifest["components"][0]["files"][0]["length"] += 1 << 43
    preimage = b"graphforge-project/2\0" + canonical(manifest).encode()
    manifest["package_digest"] = "sha256:" + hashlib.sha256(preimage).hexdigest()
    manifest_path.write_text(canonical(manifest))
    return package


def refused_import(target: Path, package: Path) -> Exception:
    import graphforge as gf

    try:
        gf.GraphForge.import_portable(
            project_root=str(target), input=str(package), operation_id=str(uuid.uuid4())
        )
    except Exception as error:
        return error
    raise AssertionError("the import was admitted")


def check_import_refusals_keep_their_identity() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        package = package_too_large_for_any_disk(root)
        target = root / "destination"
        error = refused_import(target, package)
        assert getattr(error, "code", None) == "GF_STORAGE", error
        assert error.diagnostics[0]["code"] == "storage.insufficient_space", error.diagnostics
        assert "bytes available beside the target" in error.diagnostics[0]["message"]
        assert not target.exists()
        if sys.platform.startswith("linux"):
            # `/dev/shm` is tmpfs, which the durable-publication contract does not admit.
            with tempfile.TemporaryDirectory(dir="/dev/shm") as shm:
                target = Path(shm) / "destination"
                error = refused_import(target, package)
                assert getattr(error, "code", None) == "GF_UNSUPPORTED_FILESYSTEM", error
                assert error.diagnostics[0]["code"] == "storage.unsupported_filesystem"
                assert "GF_UNSUPPORTED_FILESYSTEM" in error.diagnostics[0]["message"]
                assert not target.exists()


def main() -> None:
    check_retired_format_is_rejected_before_target_creation()
    check_import_refusals_keep_their_identity()
    check_portable_parity()
    check_shared_verification_receipts()


if __name__ == "__main__":
    main()
