"""Data written by another Core version is reported on the Python surface (#1936).

"Core X" data is made by the real writer and then relabelled: a generation by
changing its one serialized ``producing_core_version`` field and recomputing the
manifest digest ``CURRENT`` records; a package by changing the declared version
and recomputing ``package_digest`` and the BagIt checksums. No digest is typed in.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import tempfile
import uuid

import graphforge as gf

CORE_X = "0.0.1"
RUNNING = gf.__version__


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _stamp(version: str) -> str:
    return f',"producing_core_version":"{version}"}}'


def _written_project(root: Path) -> Path:
    project = root / "project"
    forge = gf.GraphForge(str(project))
    forge.execute("CREATE (:Item {score: 1})")
    forge.close()
    return project


def _relabel_generation(project: Path, to: str | None) -> None:
    """Relabel (or, for ``None``, remove) the CURRENT generation's declared Core."""
    current_path = project / "CURRENT"
    current_text = current_path.read_text()
    current = json.loads(current_text)
    manifest_path = project / "generations" / current["generation_uuid"] / "manifest.json"
    manifest = manifest_path.read_text()
    written = _stamp(RUNNING)
    assert manifest.rstrip().endswith(written), f"the writer did not stamp: {manifest}"
    replacement = "}" if to is None else _stamp(to)
    relabelled = manifest.replace(written, replacement)
    manifest_path.write_text(relabelled)
    current_path.write_text(
        current_text.replace(current["generation_manifest_sha256"], _sha(relabelled.encode()))
    )


def _relabel_package(package: Path, to: str) -> None:
    manifest_path = package / "data" / "graphforge-project.json"
    manifest = json.loads(manifest_path.read_text())
    assert manifest["producing_core_version"] == RUNNING
    manifest["producing_core_version"] = to
    del manifest["package_digest"]
    semantic = json.dumps(manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    manifest["package_digest"] = "sha256:" + _sha(b"graphforge-project/2\0" + semantic.encode())
    manifest_path.write_text(json.dumps(manifest, sort_keys=True, separators=(",", ":")))
    for name in ("manifest-sha256.txt", "tagmanifest-sha256.txt"):
        lines = (package / name).read_text().splitlines()
        resealed = ""
        for line in lines:
            path = line.split("  ", 1)[1]
            resealed += f"{_sha((package / path).read_bytes())}  {path}\n"
        (package / name).write_text(resealed)


def _exported_package(root: Path, project: Path) -> Path:
    forge = gf.GraphForge(str(project))
    package = root / "package"
    forge.export_portable(output_path=str(package), representation="expanded", profile="complete")
    forge.close()
    return package


def _assert_names_both(warning: dict[str, str]) -> None:
    assert warning["code"] == "GF_CORE_VERSION_MISMATCH"
    assert warning["producing_core_version"] == CORE_X
    assert warning["running_core_version"] == RUNNING
    assert CORE_X in warning["message"]
    assert RUNNING in warning["message"]
    assert f"graphforge=={CORE_X}" in warning["message"]


def _item_count(forge: gf.GraphForge) -> int:
    return forge.execute("MATCH (n:Item) RETURN n").num_rows


def check_opening_data_from_another_core_warns_naming_both_versions(tmp_path: Path) -> None:
    project = _written_project(tmp_path)
    _relabel_generation(project, CORE_X)

    forge = gf.GraphForge(str(project))
    assert len(forge.open_warnings) == 1
    _assert_names_both(forge.open_warnings[0])
    # A mismatch alone never refuses: the project is still readable.
    assert _item_count(forge) == 1


def check_opening_data_from_the_running_core_does_not_warn(tmp_path: Path) -> None:
    project = _written_project(tmp_path)
    assert gf.GraphForge(str(project)).open_warnings == []
    assert gf.GraphForge().open_warnings == []


def check_opening_a_generation_without_a_stamp_does_not_warn(tmp_path: Path) -> None:
    project = _written_project(tmp_path)
    _relabel_generation(project, None)

    forge = gf.GraphForge(str(project))
    assert forge.open_warnings == []
    assert _item_count(forge) == 1


def check_importing_a_package_from_another_core_warns_and_still_imports(tmp_path: Path) -> None:
    package = _exported_package(tmp_path, _written_project(tmp_path))
    _relabel_package(package, CORE_X)

    target = tmp_path / "imported"
    result = gf.GraphForge.import_portable(
        str(target), input=str(package), operation_id=str(uuid.uuid4())
    )

    assert len(result["warnings"]) == 1
    _assert_names_both(result["warnings"][0])
    imported = gf.GraphForge(str(target))
    assert _item_count(imported) == 1
    # The imported generation was written by the running Core.
    assert imported.open_warnings == []


def check_importing_a_package_from_the_running_core_does_not_warn(tmp_path: Path) -> None:
    package = _exported_package(tmp_path, _written_project(tmp_path))

    result = gf.GraphForge.import_portable(
        str(tmp_path / "imported"), input=str(package), operation_id=str(uuid.uuid4())
    )

    assert result["warnings"] == []


def main() -> None:
    for check in (
        check_opening_data_from_another_core_warns_naming_both_versions,
        check_opening_data_from_the_running_core_does_not_warn,
        check_opening_a_generation_without_a_stamp_does_not_warn,
        check_importing_a_package_from_another_core_warns_and_still_imports,
        check_importing_a_package_from_the_running_core_does_not_warn,
    ):
        with tempfile.TemporaryDirectory() as tmp:
            check(Path(tmp))


if __name__ == "__main__":
    main()
