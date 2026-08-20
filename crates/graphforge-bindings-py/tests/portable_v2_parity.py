"""Portable-v2 export/verify/import identity parity through the Python binding."""

from __future__ import annotations

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
        preview = forge.preview_portable_v2_selection(profile="complete")
        assert preview["package_class"] == "complete"
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
        imported = gf.GraphForge.import_portable_v2(
            str(target),
            input=str(bundle),
            operation_id=str(uuid.uuid4()),
        )
        assert imported["package_digest"] == bundle_export["package_digest"]
        assert not imported["idempotent_replay"]
        reopened = gf.GraphForge(str(target))
        assert reopened.path is not None


def main() -> None:
    check_portable_v2_parity()


if __name__ == "__main__":
    main()
