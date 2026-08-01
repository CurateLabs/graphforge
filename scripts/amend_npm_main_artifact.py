#!/usr/bin/env python3
"""Create or apply the authorized slim v0.5.0 main npm artifact amendment."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
from typing import Any

PACKAGE = "@curatelabs/graphforge"
VERSION = "0.5.0"
SCHEMA = "graphforge-release-amendment-v1"
MAX_PACKED_BYTES = 100_000_000
PLATFORMS = (
    "darwin-arm64",
    "darwin-x64",
    "linux-arm64-gnu",
    "linux-x64-gnu",
    "win32-x64-msvc",
)
NATIVE_FILES = {f"graphforge.{target}.node" for target in PLATFORMS}
OPTIONAL_DEPENDENCIES = {f"@curatelabs/graphforge-{target}": VERSION for target in PLATFORMS}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return value


def main_item(record: dict[str, Any]) -> dict[str, Any]:
    matches = [
        item
        for item in record.get("artifacts", [])
        if item.get("surface") == "npm" and item.get("name") == PACKAGE
    ]
    if len(matches) != 1:
        raise ValueError(f"release record must contain exactly one {PACKAGE} artifact")
    return matches[0]


def safe_extract(archive: Path, destination: Path) -> None:
    with tarfile.open(archive, "r:gz") as bundle:
        for member in bundle.getmembers():
            parts = Path(member.name).parts
            if not parts or parts[0] != "package" or ".." in parts:
                raise ValueError(f"unsafe npm archive member: {member.name}")
            if member.issym() or member.islnk():
                raise ValueError(f"npm amendment rejects links: {member.name}")
            target = destination.joinpath(*parts)
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
            elif member.isfile():
                target.parent.mkdir(parents=True, exist_ok=True)
                source = bundle.extractfile(member)
                if source is None:
                    raise ValueError(f"cannot read npm archive member: {member.name}")
                with source, target.open("wb") as output:
                    shutil.copyfileobj(source, output)
            else:
                raise ValueError(f"unsupported npm archive member: {member.name}")


def pack_slim_archive(original: Path, output: Path) -> list[str]:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        safe_extract(original, root)
        package_dir = root / "package"
        native = {path.name for path in package_dir.glob("*.node")}
        if native != NATIVE_FILES:
            raise ValueError(
                f"main npm archive native set mismatch: expected={sorted(NATIVE_FILES)} "
                f"actual={sorted(native)}"
            )
        for path in package_dir.glob("*.node"):
            path.unlink()

        manifest_path = package_dir / "package.json"
        manifest = load_json(manifest_path)
        if manifest.get("name") != PACKAGE or manifest.get("version") != VERSION:
            raise ValueError("main npm archive identity mismatch")
        if manifest.get("optionalDependencies") != OPTIONAL_DEPENDENCIES:
            raise ValueError("main npm archive optionalDependencies mismatch")
        files = manifest.get("files")
        if not isinstance(files, list) or "*.node" not in files:
            raise ValueError("main npm archive does not declare its embedded native files")
        manifest["files"] = [entry for entry in files if entry != "*.node"]
        manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

        pack_dir = root / "packed"
        pack_dir.mkdir()
        result = subprocess.run(
            [
                "npm",
                "pack",
                str(package_dir),
                "--ignore-scripts",
                "--pack-destination",
                str(pack_dir),
                "--json",
            ],
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        packed = json.loads(result.stdout)
        if not isinstance(packed, list) or len(packed) != 1:
            raise ValueError("npm pack did not return exactly one amended archive")
        source = pack_dir / packed[0]["filename"]
        output.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, output)
    return sorted(NATIVE_FILES)


def amended_record(record: dict[str, Any], supplement: dict[str, Any]) -> dict[str, Any]:
    updated = copy.deepcopy(record)
    item = main_item(updated)
    item["sha256"] = supplement["amended_artifact"]["sha256"]
    item["bytes"] = supplement["amended_artifact"]["bytes"]
    updated["amendments"] = [
        {
            "schema": SCHEMA,
            "issue": "#287",
            "artifact": PACKAGE,
            "supplement": supplement["asset_name"],
            "reason": supplement["reason"],
        }
    ]
    updated["notes"] = (
        str(updated.get("notes", "")).rstrip()
        + "\n#287 authorizes the unpublished main npm tarball amendment; "
        + "see v0.5.0-npm-amendment.json."
    ).lstrip()
    return updated


def create(record_path: Path, artifacts_dir: Path, archive_out: Path, supplement_out: Path) -> None:
    record = load_json(record_path)
    item = main_item(record)
    original = artifacts_dir / item["path"]
    if sha256_file(original) != item["sha256"]:
        raise ValueError("original main npm archive does not match the release record")
    excluded = pack_slim_archive(original, archive_out)
    if archive_out.stat().st_size >= MAX_PACKED_BYTES:
        raise ValueError("amended main npm archive still exceeds the 100 MB safety limit")
    supplement = {
        "schema": SCHEMA,
        "tag": "v0.5.0",
        "commit_sha": record.get("commit_sha"),
        "issue": "#287",
        "asset_name": supplement_out.name,
        "reason": (
            "npm rejected the unpublished recorded main package with HTTP 413; "
            "remove native binaries already published in optional platform packages"
        ),
        "original_release_record_sha256": sha256_file(record_path),
        "original_artifact": {
            "path": item["path"],
            "sha256": item["sha256"],
            "bytes": item["bytes"],
        },
        "amended_artifact": {
            "asset_name": archive_out.name,
            "path": item["path"],
            "sha256": sha256_file(archive_out),
            "bytes": archive_out.stat().st_size,
        },
        "excluded_files": excluded,
        "unchanged_artifacts": "All other v0.5.0 release-record entries remain unchanged.",
    }
    supplement_out.write_text(json.dumps(supplement, indent=2) + "\n", encoding="utf-8")
    apply(record_path, artifacts_dir, archive_out, supplement_out)


def apply(record_path: Path, artifacts_dir: Path, archive: Path, supplement_path: Path) -> None:
    record = load_json(record_path)
    supplement = load_json(supplement_path)
    if supplement.get("schema") != SCHEMA or supplement.get("issue") != "#287":
        raise ValueError("unexpected npm amendment supplement")
    if sha256_file(record_path) != supplement.get("original_release_record_sha256"):
        raise ValueError("npm amendment does not target this original release record")
    amended = supplement.get("amended_artifact", {})
    if sha256_file(archive) != amended.get("sha256"):
        raise ValueError("amended npm archive checksum mismatch")
    item = main_item(record)
    destination = artifacts_dir / item["path"]
    shutil.copyfile(archive, destination)
    updated = amended_record(record, supplement)
    record_path.write_text(json.dumps(updated, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("create", "apply"))
    parser.add_argument("--record", type=Path, required=True)
    parser.add_argument("--artifacts-dir", type=Path, required=True)
    parser.add_argument("--amended-archive", type=Path, required=True)
    parser.add_argument("--supplement", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "create":
        create(args.record, args.artifacts_dir, args.amended_archive, args.supplement)
    else:
        apply(args.record, args.artifacts_dir, args.amended_archive, args.supplement)
    print(f"npm-main-amendment: {args.command} complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
