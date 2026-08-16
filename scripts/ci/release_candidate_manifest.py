#!/usr/bin/env python3
"""Canonical, offline-verifiable GraphForge release-candidate manifest."""

from __future__ import annotations

import base64
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from email.parser import Parser
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import tarfile
from typing import Any
import zipfile

SCHEMA = "graphforge-release-candidate-v2"
SHA_RE = re.compile(r"[0-9a-f]{40}")
HASH_RE = re.compile(r"[0-9a-f]{64}")
GROUP_RETENTION_DAYS = 30
PUBLICATION_STATES = {
    "not_attempted": "no registry write has been attempted",
    "absent": "authoritative registry lookup proves the release identity is absent",
    "accepted_pending_visibility": "a write was accepted but public visibility is not verified",
    "verified": "public registry identity, metadata, and bytes match the candidate",
    "conflict": "the public identity exists but differs from the candidate",
    "indeterminate": "authoritative registry truth cannot be classified safely",
    "failed": "a deterministic local or registry operation failed",
}
CRATES = (
    "graphforge-core",
    "graphforge-filesystem",
    "graphforge-ast",
    "graphforge-knowledge",
    "graphforge-ontology",
    "graphforge-provenance",
    "graphforge-ir",
    "graphforge-plan",
    "graphforge-storage",
    "graphforge-io",
    "graphforge-rel",
    "graphforge-search",
    "graphforge-cypher",
    "graphforge-exec",
    "graphforge-api",
    "graphforge-cli",
)
NATIVE_NPM_PACKAGES = (
    "@curatelabs/graphforge-darwin-arm64",
    "@curatelabs/graphforge-darwin-x64",
    "@curatelabs/graphforge-linux-arm64-gnu",
    "@curatelabs/graphforge-linux-x64-gnu",
    "@curatelabs/graphforge-win32-x64-msvc",
)
# Exact napi platform package.json constraints (os/cpu[/libc]).
NATIVE_PLATFORM_CONSTRAINTS: dict[str, dict[str, list[str]]] = {
    "@curatelabs/graphforge-darwin-arm64": {"os": ["darwin"], "cpu": ["arm64"]},
    "@curatelabs/graphforge-darwin-x64": {"os": ["darwin"], "cpu": ["x64"]},
    "@curatelabs/graphforge-linux-arm64-gnu": {
        "os": ["linux"],
        "cpu": ["arm64"],
        "libc": ["glibc"],
    },
    "@curatelabs/graphforge-linux-x64-gnu": {
        "os": ["linux"],
        "cpu": ["x64"],
        "libc": ["glibc"],
    },
    "@curatelabs/graphforge-win32-x64-msvc": {"os": ["win32"], "cpu": ["x64"]},
}
NPM_PACKAGES = (
    *NATIVE_NPM_PACKAGES,
    "@curatelabs/graphforge",
    "@curatelabs/graphforge-cli",
    "@curatelabs/graphforge-agent-skills",
)
GROUPS = ("python", "npm", "crates", "evidence")


class CandidateError(ValueError):
    """The candidate does not satisfy the immutable release contract."""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _integrities(path: Path) -> list[str]:
    digests = (hashlib.sha256(), hashlib.sha512())
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            for digest in digests:
                digest.update(chunk)
    return [
        f"{digest.name}-" + base64.b64encode(digest.digest()).decode("ascii") for digest in digests
    ]


def _safe_relative(value: str, *, context: str) -> str:
    if not value or "\\" in value or "\x00" in value:
        raise CandidateError(f"{context} has an unsafe path: {value!r}")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise CandidateError(f"{context} has an unsafe path: {value!r}")
    return value


def classify(path: Path) -> str:
    name = path.name.lower()
    if name.endswith(".whl"):
        return "python-wheel"
    if name.endswith(".crate"):
        return "rust-crate"
    if name.endswith(".tgz"):
        return "npm-tarball"
    if name.endswith(".tar.gz"):
        return "python-sdist"
    if name.endswith(".node"):
        return "node-addon"
    if "sbom" in name or name.endswith((".spdx.json", ".cdx.json")):
        return "sbom"
    if "provenance" in name:
        return "provenance"
    return "evidence"


def artifact_identity(path: Path, artifact_class: str, version: str) -> tuple[str, str]:
    if artifact_class in {"python-wheel", "python-sdist"}:
        return "pypi", "graphforge"
    if artifact_class == "npm-tarball":
        suffix = f"-{version}.tgz"
        if path.name.startswith("curatelabs-") and path.name.endswith(suffix):
            return "npm", "@curatelabs/" + path.name[len("curatelabs-") : -len(suffix)]
        return "npm", path.stem
    if artifact_class == "rust-crate":
        suffix = f"-{version}.crate"
        if path.name.endswith(suffix):
            return "crates", path.name[: -len(suffix)]
        return "crates", path.stem
    return "evidence", path.name


class ArchiveView:
    """A path-safe archive inventory with bounded reads for metadata files."""

    def __init__(self, path: Path):
        self.path = path
        self.members: tuple[str, ...]
        self._kind: str
        if path.suffix == ".whl":
            self._kind = "zip"
            try:
                with zipfile.ZipFile(path) as archive:
                    members: list[str] = []
                    for info in archive.infolist():
                        if info.is_dir():
                            continue
                        name = _safe_relative(info.filename, context=f"archive {path.name}")
                        file_type = (info.external_attr >> 16) & 0o170000
                        if file_type == 0o120000:
                            raise CandidateError(f"archive {path.name} contains symlink {name}")
                        members.append(name)
            except zipfile.BadZipFile as error:
                raise CandidateError(f"cannot read archive {path.name}: {error}") from error
        else:
            self._kind = "tar"
            try:
                with tarfile.open(path, mode="r:*") as archive:
                    members = []
                    for info in archive.getmembers():
                        if info.isdir():
                            continue
                        name = _safe_relative(info.name, context=f"archive {path.name}")
                        if not info.isfile():
                            raise CandidateError(
                                f"archive {path.name} contains non-regular member {name}"
                            )
                        members.append(name)
            except tarfile.TarError as error:
                raise CandidateError(f"cannot read archive {path.name}: {error}") from error
        if len(members) > 50_000:
            raise CandidateError(f"archive {path.name} has an unreasonable member count")
        if len(set(members)) != len(members):
            raise CandidateError(f"archive {path.name} contains duplicate member paths")
        self.members = tuple(sorted(members))

    def read(self, member: str) -> bytes:
        if member not in self.members:
            raise CandidateError(f"archive {self.path.name} is missing {member}")
        if self._kind == "zip":
            with zipfile.ZipFile(self.path) as archive:
                data = archive.read(member)
        else:
            with tarfile.open(self.path, mode="r:*") as archive:
                extracted = archive.extractfile(member)
                if extracted is None:
                    raise CandidateError(f"cannot read {member} from {self.path.name}")
                data = extracted.read()
        if len(data) > 4 * 1024 * 1024:
            raise CandidateError(f"metadata member {member} is unexpectedly large")
        return data

    def text(self, member: str) -> str:
        try:
            return self.read(member).decode("utf-8")
        except UnicodeDecodeError as error:
            raise CandidateError(f"archive member is not UTF-8: {member}") from error


def _strip_single_root(members: tuple[str, ...], expected: str) -> tuple[str, ...]:
    prefix = expected.rstrip("/") + "/"
    if not all(member.startswith(prefix) for member in members):
        raise CandidateError(f"archive does not have the required root {expected}/")
    return tuple(member[len(prefix) :] for member in members)


def _inventory_digest(members: tuple[str, ...]) -> str:
    return hashlib.sha256(("\n".join(members) + "\n").encode()).hexdigest()


def _require(members: set[str], required: list[str], *, archive: str) -> None:
    missing = sorted(set(required) - members)
    if missing:
        raise CandidateError(f"{archive} is incomplete; missing required files: {missing}")


def _package_json(view: ArchiveView) -> dict[str, Any]:
    try:
        value = json.loads(view.text("package/package.json"))
    except json.JSONDecodeError as error:
        raise CandidateError(f"{view.path.name} has invalid package.json: {error}") from error
    if not isinstance(value, dict):
        raise CandidateError(f"{view.path.name} package.json must be an object")
    return value


def _validate_npm(view: ArchiveView, version: str) -> dict[str, Any]:
    members = set(view.members)
    metadata = _package_json(view)
    name = metadata.get("name")
    if name not in NPM_PACKAGES:
        raise CandidateError(f"unexpected npm package identity: {name!r}")
    if metadata.get("version") != version:
        raise CandidateError(f"npm package {name} does not derive root version {version}")
    required = ["package/package.json", "package/LICENSE", "package/NOTICE"]
    dependencies: dict[str, str] = {}
    if name in NATIVE_NPM_PACKAGES:
        required.append("package/THIRD_PARTY_NOTICES.md")
        addons = [member for member in members if member.endswith(".node")]
        if len(addons) != 1:
            raise CandidateError(f"npm native package {name} must contain exactly one addon")
        required.append(addons[0])
        if metadata.get("main") != addons[0].removeprefix("package/"):
            raise CandidateError(f"npm native package {name} main entrypoint is incomplete")
        files = metadata.get("files")
        expected_files = {
            addons[0].removeprefix("package/"),
            "LICENSE",
            "NOTICE",
            "THIRD_PARTY_NOTICES.md",
        }
        if not isinstance(files, list) or not expected_files.issubset(set(files)):
            raise CandidateError(f"npm native package {name} files metadata is incomplete")
        expected_platform = NATIVE_PLATFORM_CONSTRAINTS[name]
        for field, expected in expected_platform.items():
            if metadata.get(field) != expected:
                raise CandidateError(
                    f"npm native package {name} {field} metadata must be {expected}"
                )
        if "libc" not in expected_platform and "libc" in metadata:
            raise CandidateError(f"npm native package {name} must not declare libc")
    elif name == "@curatelabs/graphforge":
        required += [
            "package/index.js",
            "package/index.d.ts",
            "package/THIRD_PARTY_NOTICES.md",
        ]
        if metadata.get("main") != "index.js" or metadata.get("types") != "index.d.ts":
            raise CandidateError("npm main entrypoint metadata is incomplete")
        optional = metadata.get("optionalDependencies")
        if not isinstance(optional, dict):
            raise CandidateError("npm main package lacks native optionalDependencies")
        dependencies = {str(key): str(value) for key, value in optional.items()}
        if dependencies != dict.fromkeys(NATIVE_NPM_PACKAGES, version):
            raise CandidateError("npm main native dependency set/version is incomplete")
    elif name == "@curatelabs/graphforge-cli":
        required += [
            "package/bin/graphforge.js",
            "package/lib/run.mjs",
            "package/THIRD_PARTY_NOTICES.md",
        ]
        dependencies_raw = metadata.get("dependencies")
        dependencies = (
            {str(key): str(value) for key, value in dependencies_raw.items()}
            if isinstance(dependencies_raw, dict)
            else {}
        )
        if dependencies.get("@curatelabs/graphforge") != version:
            raise CandidateError("npm CLI must depend on the exact root GraphForge version")
        binaries = metadata.get("bin")
        if not isinstance(binaries, dict) or set(binaries.values()) != {"bin/graphforge.js"}:
            raise CandidateError("npm CLI binary metadata is incomplete")
    else:
        required += [
            "package/bin/graphforge-agent-skills.js",
            "package/adapter/index.js",
            "package/schemas/validator.js",
            "package/workflows/index.js",
            "package/compatibility.json",
            "package/skills/README.md",
        ]
        if not any(
            member.startswith("package/skills/") and member.endswith("/manifest.json")
            for member in members
        ):
            raise CandidateError("agent-skills archive contains no skill manifests")
        try:
            compatibility = json.loads(view.text("package/compatibility.json"))
        except json.JSONDecodeError as error:
            raise CandidateError("agent-skills compatibility.json is invalid") from error
        if (
            compatibility.get("package_version") != version
            or compatibility.get("graphforge_release") != version
        ):
            raise CandidateError("agent-skills compatibility does not derive the root version")
        package_compatibility = metadata.get("graphforgeCompatibility")
        if (
            not isinstance(package_compatibility, dict)
            or package_compatibility.get("release") != version
        ):
            raise CandidateError("agent-skills package metadata does not derive the root version")
    _require(members, required, archive=view.path.name)
    if metadata.get("license") != "Apache-2.0":
        raise CandidateError(f"npm package {name} lacks Apache-2.0 metadata")
    return {
        "name": name,
        "version": version,
        "dependencies": dict(sorted(dependencies.items())),
        "required_files": sorted(required),
    }


def _validate_wheel(view: ArchiveView, version: str) -> dict[str, Any]:
    members = set(view.members)
    metadata_paths = [member for member in members if member.endswith(".dist-info/METADATA")]
    if len(metadata_paths) != 1:
        raise CandidateError(f"{view.path.name} must contain exactly one METADATA file")
    metadata = Parser().parsestr(view.text(metadata_paths[0]))
    if metadata.get("Name") != "graphforge" or metadata.get("Version") != version:
        raise CandidateError(f"{view.path.name} Python identity/version mismatch")
    if (metadata.get("License-Expression") or metadata.get("License")) != "Apache-2.0":
        raise CandidateError(f"{view.path.name} lacks Apache-2.0 metadata")
    required = ["graphforge/__init__.py", metadata_paths[0]]
    native = [
        member
        for member in members
        if member.startswith("graphforge/_graphforge_rs.")
        and member.endswith((".so", ".pyd", ".dylib"))
    ]
    if len(native) != 1:
        raise CandidateError(f"{view.path.name} must contain one native Python module")
    required.append(native[0])
    for legal in ("LICENSE", "NOTICE", "THIRD_PARTY_NOTICES.md"):
        matches = [member for member in members if member.endswith("/" + legal)]
        if len(matches) != 1:
            raise CandidateError(f"{view.path.name} must contain exactly one {legal}")
        required.append(matches[0])
    _require(members, required, archive=view.path.name)
    return {
        "name": "graphforge",
        "version": version,
        "dependencies": {},
        "required_files": sorted(required),
    }


def _validate_sdist(view: ArchiveView, version: str) -> dict[str, Any]:
    root = f"graphforge-{version}"
    stripped = _strip_single_root(view.members, root)
    members = set(stripped)
    required = [
        "PKG-INFO",
        "pyproject.toml",
        "python/graphforge/__init__.py",
        "crates/graphforge-bindings-py/src/lib.rs",
        "LICENSE",
        "NOTICE",
        "THIRD_PARTY_NOTICES.md",
    ]
    _require(members, required, archive=view.path.name)
    metadata = Parser().parsestr(view.text(f"{root}/PKG-INFO"))
    if metadata.get("Name") != "graphforge" or metadata.get("Version") != version:
        raise CandidateError(f"{view.path.name} Python identity/version mismatch")
    if (metadata.get("License-Expression") or metadata.get("License")) != "Apache-2.0":
        raise CandidateError(f"{view.path.name} lacks Apache-2.0 metadata")
    return {
        "name": "graphforge",
        "version": version,
        "dependencies": {},
        "required_files": sorted(f"{root}/{item}" for item in required),
    }


def _validate_crate(view: ArchiveView, version: str) -> dict[str, Any]:
    suffix = f"-{version}.crate"
    if not view.path.name.endswith(suffix):
        raise CandidateError(f"crate filename does not derive root version: {view.path.name}")
    name = view.path.name[: -len(suffix)]
    if name not in CRATES:
        raise CandidateError(f"unexpected crates.io package identity: {name}")
    root = f"{name}-{version}"
    stripped = _strip_single_root(view.members, root)
    members = set(stripped)
    required = ["Cargo.toml", "LICENSE", "NOTICE"]
    if "src/lib.rs" in members:
        required.append("src/lib.rs")
    elif "src/main.rs" in members:
        required.append("src/main.rs")
    else:
        raise CandidateError(f"crate {name} has no Rust entrypoint")
    _require(members, required, archive=view.path.name)
    cargo = view.text(f"{root}/Cargo.toml")
    package_match = re.search(r"(?ms)^\[package\]\s*(.*?)(?=^\[|\Z)", cargo)
    if package_match is None:
        raise CandidateError(f"crate {name} has no [package] metadata")
    package_text = package_match.group(1)
    name_match = re.search(r'(?m)^name\s*=\s*"([^"]+)"', package_text)
    version_match = re.search(r'(?m)^version\s*=\s*"([^"]+)"', package_text)
    if name_match is None or name_match.group(1) != name:
        raise CandidateError(f"crate {name} package metadata mismatch")
    if version_match is None or version_match.group(1) != version:
        raise CandidateError(f"crate {name} does not derive root version {version}")
    license_match = re.search(r'(?m)^license\s*=\s*"([^"]+)"', package_text)
    license_file_match = re.search(r'(?m)^license-file\s*=\s*"([^"]+)"', package_text)
    if not (
        (license_match is not None and license_match.group(1) == "Apache-2.0")
        or (license_file_match is not None and license_file_match.group(1) == "LICENSE")
    ):
        raise CandidateError(f"crate {name} lacks Apache-2.0 license metadata")
    dependencies: dict[str, str] = {}
    dependencies_match = re.search(r"(?ms)^\[dependencies\]\s*(.*?)(?=^\[|\Z)", cargo)
    if dependencies_match:
        for dependency, value in re.findall(
            r'(?m)^(graphforge-[a-z0-9-]+)\s*=\s*("[^"]+"|\{[^}]+\})',
            dependencies_match.group(1),
        ):
            value_match = re.search(r'version\s*=\s*"([^"]+)"', value)
            dep_version = (
                value.strip('"')
                if value.startswith('"')
                else (value_match.group(1) if value_match else None)
            )
            if dep_version != version:
                raise CandidateError(
                    f"crate {name} dependency {dependency} does not use root version {version}"
                )
            dependencies[dependency] = version
    for dependency, body in re.findall(
        r"(?ms)^\[dependencies\.(graphforge-[a-z0-9-]+)\]\s*(.*?)(?=^\[|\Z)", cargo
    ):
        value_match = re.search(r'(?m)^version\s*=\s*"([^"]+)"', body)
        if value_match is None or value_match.group(1) != version:
            raise CandidateError(
                f"crate {name} dependency {dependency} does not use root version {version}"
            )
        dependencies[dependency] = version
    return {
        "name": name,
        "version": version,
        "dependencies": dict(sorted(dependencies.items())),
        "required_files": sorted(f"{root}/{item}" for item in required),
    }


def inspect_archive(path: Path, artifact_class: str, version: str) -> dict[str, Any]:
    view = ArchiveView(path)
    if artifact_class == "npm-tarball":
        package = _validate_npm(view, version)
    elif artifact_class == "python-wheel":
        package = _validate_wheel(view, version)
    elif artifact_class == "python-sdist":
        package = _validate_sdist(view, version)
    elif artifact_class == "rust-crate":
        package = _validate_crate(view, version)
    else:
        raise CandidateError(f"{path.name} is not a package archive")
    return {
        "member_count": len(view.members),
        "inventory_sha256": _inventory_digest(view.members),
        "required_files": package.pop("required_files"),
        "package": package,
    }


def _group_for(relative: str, artifact_class: str) -> str:
    first = PurePosixPath(relative).parts[0]
    expected = {
        "python-wheel": "python",
        "python-sdist": "python",
        "npm-tarball": "npm",
        "rust-crate": "crates",
    }.get(artifact_class, "evidence")
    if first != expected and not (expected == "evidence" and first in {"evidence", "node-addons"}):
        raise CandidateError(
            f"artifact {relative} is routed to {first!r}, expected group {expected!r}"
        )
    return expected


def scan_dist(dist_dir: Path, version: str) -> list[dict[str, Any]]:
    artifacts: list[dict[str, Any]] = []
    if not dist_dir.exists():
        return artifacts
    for path in sorted(dist_dir.rglob("*")):
        if not path.is_file() or path.name.startswith("."):
            continue
        relative = _safe_relative(path.relative_to(dist_dir).as_posix(), context="artifact")
        artifact_class = classify(path)
        group = _group_for(relative, artifact_class)
        surface, name = artifact_identity(path, artifact_class, version)
        archive: dict[str, Any] | None = None
        inspection_error: str | None = None
        if artifact_class in {"python-wheel", "python-sdist", "npm-tarball", "rust-crate"}:
            try:
                archive = inspect_archive(path, artifact_class, version)
                name = archive["package"]["name"]
            except CandidateError as error:
                inspection_error = str(error)
        integrities = _integrities(path)
        artifacts.append(
            {
                "path": relative,
                "group": group,
                "class": artifact_class,
                "surface": surface,
                "name": name,
                "version": version,
                "filename": path.name,
                "bytes": path.stat().st_size,
                "sha256": sha256_file(path),
                "integrity": integrities[0],
                "integrities": integrities,
                "archive": archive,
                **({"inspection_error": inspection_error} if inspection_error else {}),
            }
        )
    return artifacts


def _node_id(surface: str, name: str) -> str:
    return f"{surface}:{name}"


def _build_nodes(
    artifacts: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    paths: dict[str, list[str]] = defaultdict(list)
    dependencies: dict[str, set[str]] = defaultdict(set)
    registries: dict[str, tuple[str, str]] = {}
    for artifact in artifacts:
        if artifact["surface"] not in {"pypi", "npm", "crates"}:
            continue
        node = _node_id(artifact["surface"], artifact["name"])
        paths[node].append(artifact["path"])
        registries[node] = (artifact["surface"], artifact["name"])
        package = (artifact.get("archive") or {}).get("package", {})
        for dependency in package.get("dependencies", {}):
            dep_surface = "npm" if artifact["surface"] == "npm" else "crates"
            dependencies[node].add(_node_id(dep_surface, dependency))
    dependencies[_node_id("npm", "@curatelabs/graphforge-agent-skills")].add(
        _node_id("npm", "@curatelabs/graphforge-cli")
    )
    nodes = [
        {
            "id": node,
            "registry": registries[node][0],
            "name": registries[node][1],
            "artifact_paths": sorted(paths[node]),
        }
        for node in sorted(paths)
    ]
    edges = [
        {"from": node, "requires": dependency}
        for node in sorted(dependencies)
        for dependency in sorted(dependencies[node])
    ]
    return nodes, edges


def build_manifest(
    *,
    version: str,
    dist_dir: Path,
    commit_sha: str,
    recorded_at: str,
    notes: str = "",
) -> dict[str, Any]:
    created = datetime.fromisoformat(recorded_at.replace("Z", "+00:00"))
    if created.tzinfo is None:
        raise CandidateError("recorded_at must include a timezone")
    artifacts = scan_dist(dist_dir, version)
    nodes, dependencies = _build_nodes(artifacts)
    groups = []
    for group in GROUPS:
        paths = sorted(item["path"] for item in artifacts if item["group"] == group)
        groups.append(
            {
                "id": group,
                "directories": [group] if group != "evidence" else ["evidence", "node-addons"],
                "retention_days": GROUP_RETENTION_DAYS,
                "expires_at": (
                    created.astimezone(timezone.utc) + timedelta(days=GROUP_RETENTION_DAYS)
                ).isoformat(),
                "artifact_paths": paths,
            }
        )
    return {
        "schema": SCHEMA,
        "version": version,
        "tag": f"v{version}",
        "commit_sha": commit_sha,
        "recorded_at": created.astimezone(timezone.utc).isoformat(),
        "publication_states": PUBLICATION_STATES,
        "manifest_retention_days": GROUP_RETENTION_DAYS,
        "artifact_groups": groups,
        "nodes": nodes,
        "dependencies": dependencies,
        "artifacts": artifacts,
        "notes": notes,
    }


def _require_exact_public_nodes(nodes: list[dict[str, Any]]) -> None:
    actual = {node["id"] for node in nodes}
    expected = {
        "pypi:graphforge",
        *(_node_id("npm", name) for name in NPM_PACKAGES),
        *(_node_id("crates", name) for name in CRATES),
    }
    if actual != expected:
        raise CandidateError(
            f"candidate public node set mismatch: missing={sorted(expected - actual)} "
            f"extra={sorted(actual - expected)}"
        )


def _validate_graph(nodes: list[dict[str, Any]], edges: list[dict[str, str]]) -> None:
    node_ids = {node["id"] for node in nodes}
    incoming: dict[str, set[str]] = {node: set() for node in node_ids}
    seen: set[tuple[str, str]] = set()
    for edge in edges:
        pair = (edge.get("from", ""), edge.get("requires", ""))
        if pair in seen:
            raise CandidateError(f"duplicate dependency edge: {pair}")
        seen.add(pair)
        if pair[0] not in node_ids or pair[1] not in node_ids:
            raise CandidateError(f"dependency edge references missing node: {pair}")
        incoming[pair[0]].add(pair[1])
    remaining = {node: set(deps) for node, deps in incoming.items()}
    while remaining:
        ready = sorted(node for node, deps in remaining.items() if not deps)
        if not ready:
            raise CandidateError(
                f"candidate dependency graph contains a cycle: {sorted(remaining)}"
            )
        for node in ready:
            del remaining[node]
        for deps in remaining.values():
            deps.difference_update(ready)


def validate(
    manifest_path: Path,
    artifacts_dir: Path,
    expected_sha: str,
    version: str,
    *,
    as_of: datetime | None = None,
) -> dict[str, Any]:
    if SHA_RE.fullmatch(expected_sha) is None:
        raise CandidateError("expected SHA must be 40 lowercase hexadecimal characters")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise CandidateError(f"cannot read candidate manifest {manifest_path}: {error}") from error
    if not isinstance(manifest, dict) or manifest.get("schema") != SCHEMA:
        raise CandidateError(f"unexpected candidate manifest schema: {manifest.get('schema')!r}")
    if manifest.get("version") != version or manifest.get("tag") != f"v{version}":
        raise CandidateError("candidate version/tag does not match the requested root version")
    if manifest.get("commit_sha") != expected_sha:
        raise CandidateError("candidate commit does not match the requested SHA")
    if manifest.get("publication_states") != PUBLICATION_STATES:
        raise CandidateError("candidate publication state model is incomplete")
    if manifest.get("manifest_retention_days") != GROUP_RETENTION_DAYS:
        raise CandidateError("candidate manifest retention metadata is invalid")
    try:
        recorded_at = datetime.fromisoformat(
            str(manifest.get("recorded_at")).replace("Z", "+00:00")
        )
    except ValueError as error:
        raise CandidateError("candidate recorded_at is invalid") from error
    if recorded_at.tzinfo is None:
        raise CandidateError("candidate recorded_at must include a timezone")

    artifacts = manifest.get("artifacts")
    nodes = manifest.get("nodes")
    edges = manifest.get("dependencies")
    groups = manifest.get("artifact_groups")
    if not isinstance(artifacts, list) or not artifacts:
        raise CandidateError("candidate manifest has no artifacts")
    if not isinstance(nodes, list) or not isinstance(edges, list) or not isinstance(groups, list):
        raise CandidateError("candidate nodes, dependencies, and groups must be arrays")
    if any("version" in node for node in nodes):
        raise CandidateError("public nodes may not override the root release version")
    node_ids = [node.get("id") for node in nodes]
    if any(not isinstance(node, str) or not node for node in node_ids):
        raise CandidateError("every public node requires an id")
    if len(set(node_ids)) != len(node_ids):
        raise CandidateError("candidate contains duplicate public nodes")
    _require_exact_public_nodes(nodes)
    _validate_graph(nodes, edges)

    group_by_id = {group.get("id"): group for group in groups if isinstance(group, dict)}
    if set(group_by_id) != set(GROUPS) or len(group_by_id) != len(groups):
        raise CandidateError("candidate artifact groups must be exactly python/npm/crates/evidence")
    now = as_of or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise CandidateError("retention check time must include a timezone")
    grouped_paths: set[str] = set()
    for group_id in GROUPS:
        group = group_by_id[group_id]
        expected_directories = [group_id] if group_id != "evidence" else ["evidence", "node-addons"]
        if group.get("directories") != expected_directories:
            raise CandidateError(f"artifact group {group_id} directories are invalid")
        if group.get("retention_days") != GROUP_RETENTION_DAYS:
            raise CandidateError(f"artifact group {group_id} retention is invalid")
        try:
            expiry = datetime.fromisoformat(str(group.get("expires_at")).replace("Z", "+00:00"))
        except ValueError as error:
            raise CandidateError(f"artifact group {group_id} expiry is invalid") from error
        if expiry != recorded_at + timedelta(days=GROUP_RETENTION_DAYS):
            raise CandidateError(f"artifact group {group_id} expiry does not match retention")
        if expiry <= now:
            raise CandidateError(f"artifact group {group_id} retention has expired")
        paths = group.get("artifact_paths")
        if not isinstance(paths, list) or not paths:
            raise CandidateError(f"artifact group {group_id} is missing")
        for relative in paths:
            _safe_relative(relative, context=f"artifact group {group_id}")
            if relative in grouped_paths:
                raise CandidateError(f"artifact belongs to multiple groups: {relative}")
            grouped_paths.add(relative)

    seen_paths: set[str] = set()
    names_by_surface: dict[str, set[str]] = defaultdict(set)
    class_counts: dict[str, int] = defaultdict(int)
    for index, item in enumerate(artifacts):
        if not isinstance(item, dict):
            raise CandidateError(f"artifacts[{index}] must be an object")
        relative = _safe_relative(item.get("path", ""), context=f"artifacts[{index}]")
        if relative in seen_paths:
            raise CandidateError(f"duplicate artifact path: {relative}")
        seen_paths.add(relative)
        if (
            item.get("group") not in GROUPS
            or relative not in group_by_id[item["group"]]["artifact_paths"]
        ):
            raise CandidateError(f"artifact group membership mismatch: {relative}")
        path = artifacts_dir / relative
        if not path.is_file():
            raise CandidateError(f"recorded artifact is missing: {relative}")
        digest = item.get("sha256")
        if not isinstance(digest, str) or HASH_RE.fullmatch(digest) is None:
            raise CandidateError(f"artifacts[{index}] has an invalid SHA-256")
        if sha256_file(path) != digest:
            raise CandidateError(f"artifact checksum mismatch: {relative}")
        integrities = _integrities(path)
        if item.get("integrity") != integrities[0] or item.get("integrities") != integrities:
            raise CandidateError(f"artifact integrity mismatch: {relative}")
        if item.get("bytes") != path.stat().st_size:
            raise CandidateError(f"artifact byte count mismatch: {relative}")
        if item.get("version") != version:
            raise CandidateError(f"artifact version mismatch: {relative}")
        artifact_class = item.get("class")
        class_counts[str(artifact_class)] += 1
        surface = item.get("surface")
        name = item.get("name")
        if surface in {"pypi", "npm", "crates"} and isinstance(name, str):
            names_by_surface[surface].add(name)
        if artifact_class in {"python-wheel", "python-sdist", "npm-tarball", "rust-crate"}:
            inspected = inspect_archive(path, artifact_class, version)
            if item.get("inspection_error") is not None or item.get("archive") != inspected:
                raise CandidateError(f"archive inventory/completeness mismatch: {relative}")
            if inspected["package"]["name"] != name:
                raise CandidateError(f"archive package identity mismatch: {relative}")
    actual_files = {
        path.relative_to(artifacts_dir).as_posix()
        for path in artifacts_dir.rglob("*")
        if path.is_file() and not path.name.startswith(".")
    }
    if actual_files != seen_paths or grouped_paths != seen_paths:
        raise CandidateError(
            "candidate file/group inventory drift: "
            f"unrecorded={sorted(actual_files - seen_paths)} "
            f"missing={sorted(seen_paths - actual_files)} "
            f"ungrouped={sorted(seen_paths - grouped_paths)}"
        )
    if class_counts["python-wheel"] != 3 or class_counts["python-sdist"] != 1:
        raise CandidateError("candidate must contain three graphforge wheels and one sdist")
    if class_counts["node-addon"] != 5:
        raise CandidateError("candidate evidence must contain the five tested Node addons")
    if names_by_surface["pypi"] != {"graphforge"}:
        raise CandidateError("candidate PyPI identity is incomplete")
    if names_by_surface["npm"] != set(NPM_PACKAGES):
        raise CandidateError("candidate npm package set is incomplete")
    if names_by_surface["crates"] != set(CRATES):
        raise CandidateError("candidate crates.io package set is incomplete")
    expected_nodes, expected_edges = _build_nodes(artifacts)
    if nodes != expected_nodes:
        raise CandidateError("public node artifact membership is incomplete")
    if edges != expected_edges:
        raise CandidateError("candidate dependency metadata is incomplete")
    return manifest


def npm_paths(manifest: dict[str, Any]) -> list[str]:
    by_name = {
        item["name"]: item["path"] for item in manifest["artifacts"] if item.get("surface") == "npm"
    }
    return [by_name[name] for name in NPM_PACKAGES]
