#!/usr/bin/env python3
"""Mutation-sensitive tests for the partitioned release-candidate contract."""

from __future__ import annotations

import copy
from datetime import datetime, timedelta, timezone
import importlib.util
import io
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import zipfile

SCRIPT = Path(__file__).with_name("release-candidate.py")
SPEC = importlib.util.spec_from_file_location("release_candidate", SCRIPT)
assert SPEC and SPEC.loader
release_candidate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release_candidate)
manifest_module = __import__("release_candidate_manifest")
SHA = "a" * 40
VERSION = "0.5.0"


def write_tar(path: Path, members: dict[str, bytes]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(path, "w:gz") as archive:
        for name, data in sorted(members.items()):
            info = tarfile.TarInfo(name)
            info.size = len(data)
            info.mtime = 0
            archive.addfile(info, io.BytesIO(data))


def write_wheel(path: Path, *, omit: str | None = None) -> None:
    dist = f"graphforge-{VERSION}.dist-info"
    members = {
        "graphforge/__init__.py": b"from ._graphforge_rs import *\n",
        "graphforge/_graphforge_rs.abi3.so": b"native",
        f"{dist}/METADATA": (
            f"Name: graphforge\nVersion: {VERSION}\nLicense-Expression: Apache-2.0\n"
        ).encode(),
        f"{dist}/WHEEL": b"Wheel-Version: 1.0\n",
        f"{dist}/licenses/LICENSE": b"Apache-2.0",
        f"{dist}/licenses/NOTICE": b"GraphForge",
        f"{dist}/licenses/THIRD_PARTY_NOTICES.md": b"Third party",
    }
    if omit:
        members.pop(omit)
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as archive:
        for name, data in sorted(members.items()):
            archive.writestr(name, data)


def npm_members(name: str, *, package_version: str = VERSION) -> dict[str, bytes]:
    metadata: dict[str, object] = {
        "name": name,
        "version": package_version,
        "license": "Apache-2.0",
    }
    members = {
        "package/LICENSE": b"Apache-2.0",
        "package/NOTICE": b"GraphForge",
    }
    if name in manifest_module.NATIVE_NPM_PACKAGES:
        members["package/THIRD_PARTY_NOTICES.md"] = b"Third party"
        members["package/graphforge.node"] = b"native"
        metadata["main"] = "graphforge.node"
        metadata["files"] = [
            "graphforge.node",
            "LICENSE",
            "NOTICE",
            "THIRD_PARTY_NOTICES.md",
        ]
    elif name == "@curatelabs/graphforge":
        metadata.update(
            {
                "main": "index.js",
                "types": "index.d.ts",
                "optionalDependencies": dict.fromkeys(manifest_module.NATIVE_NPM_PACKAGES, VERSION),
            }
        )
        members.update(
            {
                "package/index.js": (f"exports.version = () => '{package_version}';\n").encode(),
                "package/index.d.ts": b"export declare function version(): string\n",
                "package/lib/index.mjs": b"export const loaded = true\n",
                "package/THIRD_PARTY_NOTICES.md": b"Third party",
            }
        )
    elif name == "@curatelabs/graphforge-cli":
        metadata.update(
            {
                "bin": {"graphforge": "bin/graphforge.js", "gf": "bin/graphforge.js"},
                "dependencies": {"@curatelabs/graphforge": VERSION},
            }
        )
        members.update(
            {
                "package/bin/graphforge.js": (
                    b"#!/usr/bin/env node\n"
                    b"process.stdout.write(JSON.stringify({contract:'fixture'}));\n"
                ),
                "package/lib/run.mjs": b"export function run() {}\n",
                "package/THIRD_PARTY_NOTICES.md": b"Third party",
            }
        )
    else:
        metadata["bin"] = {"graphforge-agent-skills": "bin/graphforge-agent-skills.js"}
        metadata["graphforgeCompatibility"] = {"release": package_version}
        members.update(
            {
                "package/bin/graphforge-agent-skills.js": (
                    "#!/usr/bin/env node\n"
                    f"process.stdout.write(JSON.stringify({{graphforge_release:'{package_version}'}}));\n"
                ).encode(),
                "package/adapter/index.js": b"export {}\n",
                "package/schemas/validator.js": b"export {}\n",
                "package/workflows/index.js": b"export {}\n",
                "package/skills/README.md": b"# GraphForge skills\n",
                "package/skills/graphforge/manifest.json": b"{}\n",
                "package/compatibility.json": json.dumps(
                    {
                        "package_version": package_version,
                        "graphforge_release": package_version,
                    }
                ).encode(),
            }
        )
    members["package/package.json"] = json.dumps(metadata, sort_keys=True).encode()
    return members


def create_candidate(
    root: Path,
    *,
    omit_npm: tuple[str, str] | None = None,
    divergent_npm: str | None = None,
    omit_wheel: str | None = None,
    omit_crate_notice: str | None = None,
) -> tuple[Path, Path, dict[str, object]]:
    artifacts = root / "artifacts"
    for platform in ("linux", "macos", "windows"):
        omit = omit_wheel if platform == "linux" else None
        write_wheel(
            artifacts / "python" / f"graphforge-{VERSION}-{platform}.whl",
            omit=omit,
        )
    sdist_root = f"graphforge-{VERSION}"
    write_tar(
        artifacts / "python" / f"graphforge-{VERSION}.tar.gz",
        {
            f"{sdist_root}/PKG-INFO": (
                f"Name: graphforge\nVersion: {VERSION}\nLicense-Expression: Apache-2.0\n"
            ).encode(),
            f"{sdist_root}/pyproject.toml": b"[project]\nname='graphforge'\n",
            f"{sdist_root}/python/graphforge/__init__.py": b"",
            f"{sdist_root}/crates/graphforge-bindings-py/src/lib.rs": b"",
            f"{sdist_root}/LICENSE": b"Apache-2.0",
            f"{sdist_root}/NOTICE": b"GraphForge",
            f"{sdist_root}/THIRD_PARTY_NOTICES.md": b"Third party",
        },
    )
    for name in manifest_module.NPM_PACKAGES:
        members = npm_members(name, package_version="0.5.1" if name == divergent_npm else VERSION)
        if omit_npm and omit_npm[0] == name:
            members.pop(omit_npm[1])
        filename = name.removeprefix("@curatelabs/").replace("/", "-")
        write_tar(
            artifacts / "npm" / f"curatelabs-{filename}-{VERSION}.tgz",
            members,
        )
    for name in manifest_module.CRATES:
        crate_root = f"{name}-{VERSION}"
        dependency = (
            ""
            if name == "graphforge-core"
            else (f'graphforge-core = {{ version = "{VERSION}" }}\n')
        )
        members = {
            f"{crate_root}/Cargo.toml": (
                f'[package]\nname = "{name}"\nversion = "{VERSION}"\n'
                'license = "Apache-2.0"\n[dependencies]\n' + dependency
            ).encode(),
            f"{crate_root}/src/lib.rs": b"pub fn candidate() {}\n",
            f"{crate_root}/LICENSE": b"Apache-2.0",
            f"{crate_root}/NOTICE": b"GraphForge",
        }
        if name == omit_crate_notice:
            members.pop(f"{crate_root}/NOTICE")
        write_tar(artifacts / "crates" / f"{name}-{VERSION}.crate", members)
    for target in ("darwin-arm64", "darwin-x64", "linux-arm64", "linux-x64", "win32-x64"):
        path = artifacts / "node-addons" / f"graphforge.{target}.node"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(target.encode())
    evidence = artifacts / "evidence" / "offline-rehearsal.json"
    evidence.parent.mkdir(parents=True, exist_ok=True)
    evidence.write_text('{"offline":true}\n', encoding="utf-8")
    recorded_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    manifest = manifest_module.build_manifest(
        version=VERSION,
        dist_dir=artifacts,
        commit_sha=SHA,
        recorded_at=recorded_at,
        notes="deterministic offline fixture",
    )
    manifest_path = root / "candidate-manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    return manifest_path, artifacts, manifest


def rejected(
    manifest_path: Path,
    artifacts: Path,
    message: str,
    *,
    as_of: datetime | None = None,
) -> None:
    try:
        release_candidate.validate(manifest_path, artifacts, SHA, VERSION, as_of=as_of)
    except release_candidate.CandidateError as error:
        assert message in str(error), error
    else:
        raise AssertionError(f"candidate mutation was accepted: {message}")


def write_mutation(root: Path, manifest: dict[str, object]) -> Path:
    path = root / "mutated-manifest.json"
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    return path


def main() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        manifest_path, artifacts, manifest = create_candidate(root)
        validated = release_candidate.validate(manifest_path, artifacts, SHA, VERSION)
        assert len(validated["nodes"]) == 24
        assert len(release_candidate.npm_paths(validated)) == 8
        assert all(
            [value.split("-", 1)[0] for value in item["integrities"]] == ["sha256", "sha512"]
            for item in validated["artifacts"]
        )
        assert set(validated["publication_states"]) == set(manifest_module.PUBLICATION_STATES)
        rebuilt = manifest_module.build_manifest(
            version=VERSION,
            dist_dir=artifacts,
            commit_sha=SHA,
            recorded_at=manifest["recorded_at"],
            notes="deterministic offline fixture",
        )
        assert rebuilt == manifest

        assembled = root / "offline-assembled"
        for item in validated["artifacts"]:
            source = artifacts / item["path"]
            destination = assembled / item["path"]
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, destination)
        release_candidate.validate(manifest_path, assembled, SHA, VERSION)

        target = artifacts / validated["artifacts"][0]["path"]
        target.write_bytes(b"mutated")
        rejected(manifest_path, artifacts, "checksum mismatch")

    completeness_cases = (
        ({"omit_npm": ("@curatelabs/graphforge", "package/index.js")}, "index.js"),
        ({"omit_npm": ("@curatelabs/graphforge", "package/index.d.ts")}, "index.d.ts"),
        ({"omit_npm": ("@curatelabs/graphforge-cli", "package/bin/graphforge.js")}, "bin"),
        (
            {
                "omit_npm": (
                    "@curatelabs/graphforge-agent-skills",
                    "package/skills/graphforge/manifest.json",
                )
            },
            "skill manifests",
        ),
        ({"omit_wheel": "graphforge/_graphforge_rs.abi3.so"}, "native Python module"),
        ({"omit_wheel": "graphforge/__init__.py"}, "graphforge/__init__.py"),
        (
            {
                "omit_npm": (
                    "@curatelabs/graphforge-darwin-arm64",
                    "package/graphforge.node",
                )
            },
            "native package",
        ),
        ({"omit_crate_notice": "graphforge-api"}, "NOTICE"),
        ({"divergent_npm": "@curatelabs/graphforge-cli"}, "root version"),
    )
    for options, message in completeness_cases:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            manifest_path, artifacts, _ = create_candidate(root, **options)
            rejected(manifest_path, artifacts, message)

    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        manifest_path, artifacts, manifest = create_candidate(root)
        mutated = copy.deepcopy(manifest)
        mutated["nodes"][0]["version"] = VERSION
        rejected(write_mutation(root, mutated), artifacts, "may not override")

        mutated = copy.deepcopy(manifest)
        mutated["nodes"].append(copy.deepcopy(mutated["nodes"][0]))
        rejected(write_mutation(root, mutated), artifacts, "duplicate public nodes")

        mutated = copy.deepcopy(manifest)
        mutated["dependencies"] = mutated["dependencies"][1:]
        rejected(write_mutation(root, mutated), artifacts, "dependency metadata")

        mutated = copy.deepcopy(manifest)
        mutated["dependencies"].append(
            {"from": "crates:graphforge-core", "requires": "crates:graphforge-api"}
        )
        rejected(write_mutation(root, mutated), artifacts, "cycle")

        mutated = copy.deepcopy(manifest)
        mutated["artifacts"][0]["path"] = "../escape.whl"
        rejected(write_mutation(root, mutated), artifacts, "unsafe path")

        mutated = copy.deepcopy(manifest)
        mutated["artifacts"][0]["integrities"][1] = "sha512-incorrect"
        rejected(write_mutation(root, mutated), artifacts, "integrity mismatch")

        mutated = copy.deepcopy(manifest)
        mutated["artifact_groups"][1]["artifact_paths"].append(
            mutated["artifact_groups"][0]["artifact_paths"][0]
        )
        rejected(write_mutation(root, mutated), artifacts, "multiple groups")

        mutated = copy.deepcopy(manifest)
        mutated["artifact_groups"] = mutated["artifact_groups"][:-1]
        rejected(write_mutation(root, mutated), artifacts, "exactly python/npm/crates/evidence")

        mutated = copy.deepcopy(manifest)
        mutated["publication_states"].pop("indeterminate")
        rejected(write_mutation(root, mutated), artifacts, "state model")

        expiry = datetime.fromisoformat(manifest["artifact_groups"][0]["expires_at"])
        rejected(
            manifest_path,
            artifacts,
            "retention has expired",
            as_of=expiry + timedelta(seconds=1),
        )

        malformed = copy.deepcopy(manifest)
        malformed["schema"] = "unknown"
        rejected(write_mutation(root, malformed), artifacts, "unexpected candidate manifest schema")

    print("release-candidate tests: ok")
    subprocess.run(
        [sys.executable, str(Path(__file__).with_name("test-release-registry.py"))],
        check=True,
    )
    subprocess.run(
        [sys.executable, str(Path(__file__).with_name("test-release-rehearsal.py"))],
        check=True,
    )
    subprocess.run(
        [sys.executable, str(Path(__file__).with_name("test-release-action.py"))],
        check=True,
    )


if __name__ == "__main__":
    main()
