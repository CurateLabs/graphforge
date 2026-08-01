#!/usr/bin/env python3
"""Post-publication clean-environment verification (#167 / M1 #192).

Fails closed when public registry artifacts for the requested version are
missing. Never treats unpublished packages as success.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
from typing import Any
import urllib.error
from urllib.parse import urlparse
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
EVIDENCE_SCHEMA = "graphforge-clean-env-evidence-v1"
RELEASE_RECORD_SCHEMA = "graphforge-release-record-v1"
DEFAULT_VERSION = "0.5.0"
DEFAULT_DOCS_BASE = "https://docs.graphforge.sh"
DEFAULT_CRATES = (
    "graphforge-core",
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

LANE_ISSUES = {
    "pip": 180,
    "npm": 183,
    "cli": 183,
    "skills": 182,
    "cargo": 185,
    "reopen": 184,
    "urls": 186,
    "checksums": 187,
}
ALL_LANES = tuple(LANE_ISSUES)


class VerifyError(RuntimeError):
    """Clean-environment verification failure."""


Fetcher = Callable[[str], tuple[int, bytes, dict[str, str]]]


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def default_fetcher(url: str, timeout: float = 30.0) -> tuple[int, bytes, dict[str, str]]:
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "graphforge-clean-env-verify/1.0"},
        method="GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            headers = {k.lower(): v for k, v in response.headers.items()}
            return int(response.status), response.read(), headers
    except urllib.error.HTTPError as exc:
        body = exc.read() if hasattr(exc, "read") else b""
        headers = {k.lower(): v for k, v in getattr(exc, "headers", {}).items()}
        return int(exc.code), body, headers


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def require_version(version: str) -> str:
    version = version.strip()
    if not version or version.endswith("-dev") or "dev" in version:
        raise VerifyError(
            f"refusing non-release version {version!r}; "
            "clean-env verification targets published release versions only"
        )
    return version


def parse_json(data: bytes, *, context: str) -> Any:
    try:
        return json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise VerifyError(f"invalid JSON from {context}: {exc}") from exc


def validate_release_record(record: dict[str, Any]) -> dict[str, Any]:
    if record.get("schema") != RELEASE_RECORD_SCHEMA:
        raise VerifyError(
            f"release record schema must be {RELEASE_RECORD_SCHEMA!r}, got {record.get('schema')!r}"
        )
    version = record.get("version")
    if not isinstance(version, str) or not version:
        raise VerifyError("release record requires non-empty string 'version'")
    artifacts = record.get("artifacts")
    if not isinstance(artifacts, list) or not artifacts:
        raise VerifyError("release record requires non-empty 'artifacts' list")
    for index, artifact in enumerate(artifacts):
        if not isinstance(artifact, dict):
            raise VerifyError(f"artifacts[{index}] must be an object")
        for key in ("surface", "name", "version", "sha256"):
            value = artifact.get(key)
            if not isinstance(value, str) or not value:
                raise VerifyError(f"artifacts[{index}].{key} must be a non-empty string")
        digest = artifact["sha256"].lower()
        if len(digest) != 64 or any(ch not in "0123456789abcdef" for ch in digest):
            raise VerifyError(f"artifacts[{index}].sha256 must be 64 lowercase hex chars")
        artifact["sha256"] = digest
    return record


def load_release_record(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise VerifyError(f"unable to read release record {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise VerifyError("release record root must be a JSON object")
    return validate_release_record(payload)


def validate_evidence(evidence: dict[str, Any]) -> dict[str, Any]:
    if evidence.get("schema") != EVIDENCE_SCHEMA:
        raise VerifyError(
            f"evidence schema must be {EVIDENCE_SCHEMA!r}, got {evidence.get('schema')!r}"
        )
    if not isinstance(evidence.get("version"), str) or not evidence["version"]:
        raise VerifyError("evidence requires non-empty string 'version'")
    if not isinstance(evidence.get("ok"), bool):
        raise VerifyError("evidence requires boolean 'ok'")
    lanes = evidence.get("lanes")
    if not isinstance(lanes, dict) or not lanes:
        raise VerifyError("evidence requires non-empty 'lanes' object")
    for name, lane in lanes.items():
        if not isinstance(lane, dict):
            raise VerifyError(f"lanes.{name} must be an object")
        if not isinstance(lane.get("ok"), bool):
            raise VerifyError(f"lanes.{name}.ok must be boolean")
        if "issue" in lane and lane["issue"] is not None:
            if not isinstance(lane["issue"], int):
                raise VerifyError(f"lanes.{name}.issue must be an int when present")
    return evidence


@dataclass
class LaneResult:
    name: str
    issue: int | None
    ok: bool
    commands: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    error: str | None = None
    artifacts: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "issue": self.issue,
            "ok": self.ok,
            "commands": list(self.commands),
            "notes": list(self.notes),
            "artifacts": dict(self.artifacts),
        }
        if self.error:
            payload["error"] = self.error
        return payload


@dataclass
class Context:
    version: str
    work_root: Path
    docs_base: str
    crates: tuple[str, ...]
    release_record: dict[str, Any] | None
    fetch: Fetcher
    run_cmd: Callable[..., subprocess.CompletedProcess[str]]
    allow_network_install: bool = True


def registry_urls(version: str, crates: tuple[str, ...], docs_base: str) -> dict[str, str]:
    docs = docs_base.rstrip("/")
    urls = {
        "pypi_json": f"https://pypi.org/pypi/graphforge/{version}/json",
        "pypi_project": f"https://pypi.org/project/graphforge/{version}/",
        "npm_node": f"https://registry.npmjs.org/@curatelabs/graphforge/{version}",
        "npm_node_page": f"https://www.npmjs.com/package/@curatelabs/graphforge/v/{version}",
        "npm_cli": f"https://registry.npmjs.org/@curatelabs/graphforge-cli/{version}",
        "npm_cli_page": f"https://www.npmjs.com/package/@curatelabs/graphforge-cli/v/{version}",
        "npm_skills": f"https://registry.npmjs.org/@curatelabs/graphforge-agent-skills/{version}",
        "npm_skills_page": f"https://www.npmjs.com/package/@curatelabs/graphforge-agent-skills/v/{version}",
        "docs_quickstart": f"{docs}/guide/quickstart/",
        "docs_installation": f"{docs}/guide/installation/",
        "github_release": (f"https://github.com/CurateLabs/graphforge/releases/tag/v{version}"),
    }
    for crate in crates:
        urls[f"crates_{crate}"] = f"https://crates.io/api/v1/crates/{crate}/{version}"
        urls[f"crates_page_{crate}"] = f"https://crates.io/crates/{crate}/{version}"
    return urls


def http_ok(ctx: Context, url: str, *, context: str) -> tuple[bytes, dict[str, str]]:
    status, body, headers = ctx.fetch(url)
    if status != 200:
        raise VerifyError(f"{context}: expected HTTP 200 from {url}, got {status}")
    return body, headers


def probe_published(ctx: Context) -> dict[str, Any]:
    urls = registry_urls(ctx.version, ctx.crates, ctx.docs_base)
    probes: dict[str, Any] = {}
    missing: list[str] = []
    for key in ("pypi_json", "npm_node", "npm_cli", "npm_skills"):
        status, body, _ = ctx.fetch(urls[key])
        probes[key] = {"url": urls[key], "status": status}
        if status != 200:
            missing.append(f"{key} ({status})")
        elif key == "pypi_json":
            payload = parse_json(body, context=urls[key])
            info = payload.get("info") if isinstance(payload, dict) else None
            ver = info.get("version") if isinstance(info, dict) else None
            if ver != ctx.version:
                missing.append(f"pypi version mismatch: {ver!r}")
    for crate in ctx.crates:
        key = f"crates_{crate}"
        status, body, _ = ctx.fetch(urls[key])
        probes[key] = {"url": urls[key], "status": status}
        if status != 200:
            missing.append(f"{key} ({status})")
        else:
            payload = parse_json(body, context=urls[key])
            version_info = payload.get("version") if isinstance(payload, dict) else None
            num = version_info.get("num") if isinstance(version_info, dict) else None
            if num != ctx.version:
                missing.append(f"{crate} version mismatch: {num!r}")
    return {"probes": probes, "missing": missing, "urls": urls}


def run_preflight(ctx: Context) -> LaneResult:
    result = LaneResult(name="preflight", issue=None, ok=False)
    probe = probe_published(ctx)
    result.artifacts = {"probes": probe["probes"]}
    result.commands.append(f"preflight registries for {ctx.version}")
    if probe["missing"]:
        result.error = (
            "public v"
            + ctx.version
            + " artifacts not available; blocked on #192 publication (#195/#198). "
            "missing: " + ", ".join(probe["missing"])
        )
        result.notes.append("do not fake green checkoffs against unpublished packages")
        return result
    result.ok = True
    registry_note = (
        "PyPI and npm (@curatelabs/graphforge, @curatelabs/graphforge-cli, "
        "@curatelabs/graphforge-agent-skills)"
    )
    if ctx.crates:
        registry_note += f", plus crates.io ({', '.join(ctx.crates)})"
    else:
        registry_note += "; no crates.io packages configured"
    result.notes.append(f"{registry_note}; probes OK for v{ctx.version}")
    return result


def _run(ctx: Context, argv: list[str], *, cwd: Path | None = None) -> str:
    completed = ctx.run_cmd(
        argv,
        cwd=str(cwd or ctx.work_root),
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        stderr = (completed.stderr or "").strip()
        stdout = (completed.stdout or "").strip()
        detail = stderr or stdout or f"exit {completed.returncode}"
        raise VerifyError(f"command failed ({' '.join(argv)}): {detail}")
    return completed.stdout or ""


def lane_pip(ctx: Context) -> LaneResult:
    result = LaneResult(name="pip", issue=LANE_ISSUES["pip"], ok=False)
    if not ctx.allow_network_install:
        result.error = "network installs disabled"
        return result
    work = ctx.work_root / "pip"
    work.mkdir(parents=True, exist_ok=True)
    venv = work / "venv"
    py = venv / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    result.commands.append(f"python3 -m venv {venv}")
    _run(ctx, [sys.executable, "-m", "venv", str(venv)])
    result.commands.append(f"{py} -m pip install graphforge=={ctx.version}")
    _run(ctx, [str(py), "-m", "pip", "install", f"graphforge=={ctx.version}"])
    script = work / "quickstart.py"
    script.write_text(
        f"""
from graphforge import GraphForge
import graphforge

assert graphforge.__version__.startswith({ctx.version!r}), graphforge.__version__
forge = GraphForge()
alice = forge.add_node("Person", name="Alice", age=30)
bob = forge.add_node("Person", name="Bob", age=25)
forge.add_edge(alice, "KNOWS", bob, since=2020)
table = forge.execute('''
    MATCH (p:Person)-[:KNOWS]->(friend:Person)
    WHERE p.age > 25
    RETURN p.name AS person, friend.name AS friend, p.age AS age
    ORDER BY p.age DESC
''')
assert table.num_rows == 1, table.num_rows
row = table.to_pylist()[0]
assert row["person"] == "Alice" and row["friend"] == "Bob" and row["age"] == 30, row
print("pip-quickstart-ok", graphforge.__version__, flush=True)
""".lstrip(),
        encoding="utf-8",
    )
    result.commands.append(f"{py} {script}")
    out = _run(ctx, [str(py), str(script)])
    result.notes.append(out.strip().splitlines()[-1] if out.strip() else "quickstart ok")
    result.artifacts["venv"] = str(venv)
    result.ok = True
    return result


def lane_reopen(ctx: Context) -> LaneResult:
    result = LaneResult(name="reopen", issue=LANE_ISSUES["reopen"], ok=False)
    if not ctx.allow_network_install:
        result.error = "network installs disabled"
        return result
    work = ctx.work_root / "reopen"
    work.mkdir(parents=True, exist_ok=True)
    venv = work / "venv"
    py = venv / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    result.commands.append(f"python3 -m venv {venv}")
    _run(ctx, [sys.executable, "-m", "venv", str(venv)])
    result.commands.append(f"{py} -m pip install graphforge=={ctx.version}")
    _run(ctx, [str(py), "-m", "pip", "install", f"graphforge=={ctx.version}"])
    project = work / "research"
    script = work / "reopen.py"
    script.write_text(
        f"""
from pathlib import Path
from graphforge import GraphForge

path = Path({str(project)!r})
path.mkdir(parents=True, exist_ok=True)
forge = GraphForge(str(path))
forge.add_node("Paper", title="Graph Neural Networks", year=2024)
forge.close()
reopened = GraphForge(str(path))
table = reopened.execute("MATCH (p:Paper) RETURN p.title AS title, p.year AS year")
assert table.num_rows == 1, table.num_rows
rows = table.to_pylist()
assert rows[0]["title"] == "Graph Neural Networks", rows
assert rows[0]["year"] == 2024, rows
print("reopen-arrow-ok", table.num_rows, flush=True)
""".lstrip(),
        encoding="utf-8",
    )
    result.commands.append(f"{py} {script}")
    out = _run(ctx, [str(py), str(script)])
    result.notes.append(out.strip().splitlines()[-1] if out.strip() else "reopen ok")
    result.ok = True
    return result


def lane_npm(ctx: Context) -> LaneResult:
    result = LaneResult(name="npm", issue=LANE_ISSUES["npm"], ok=False)
    if not ctx.allow_network_install:
        result.error = "network installs disabled"
        return result
    work = ctx.work_root / "npm"
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)
    result.commands.append("npm init -y")
    _run(ctx, ["npm", "init", "-y"], cwd=work)
    result.commands.append(f"npm install @curatelabs/graphforge@{ctx.version} apache-arrow")
    _run(
        ctx,
        ["npm", "install", f"@curatelabs/graphforge@{ctx.version}", "apache-arrow"],
        cwd=work,
    )
    smoke = work / "smoke.mjs"
    smoke.write_text(
        """
import assert from "node:assert/strict";
import { tableFromIPC } from "apache-arrow";
import { GraphForge, version } from "@curatelabs/graphforge";

const reported = version();
assert.ok(typeof reported === "string" && reported.length > 0, reported);
const forge = new GraphForge();
forge.execute("CREATE (:Person {name: 'Alice', age: 30})");
forge.execute("CREATE (:Person {name: 'Bob', age: 25})");
const table = tableFromIPC(
  forge.execute("MATCH (p:Person) RETURN p.name AS name ORDER BY name"),
);
assert.equal(table.numRows, 2);
const names = [...table.getChild("name").toArray()];
assert.deepEqual(names, ["Alice", "Bob"]);
console.log("npm-smoke-ok", reported);
""".lstrip(),
        encoding="utf-8",
    )
    result.commands.append(f"node {smoke}")
    out = _run(ctx, ["node", str(smoke)], cwd=work)
    result.notes.append(out.strip().splitlines()[-1] if out.strip() else "npm smoke ok")
    result.ok = True
    return result


def lane_cli(ctx: Context) -> LaneResult:
    """Install the published CLI and execute it without workspace resolution."""
    result = LaneResult(name="cli", issue=LANE_ISSUES["cli"], ok=False)
    if not ctx.allow_network_install:
        result.error = "network installs disabled"
        return result
    work = ctx.work_root / "cli"
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)
    result.commands.append("npm init -y")
    _run(ctx, ["npm", "init", "-y"], cwd=work)
    package = f"@curatelabs/graphforge-cli@{ctx.version}"
    result.commands.append(f"npm install {package}")
    _run(ctx, ["npm", "install", package], cwd=work)
    result.commands.append("npx --offline --no-install graphforge --version")
    out = _run(
        ctx,
        ["npx", "--offline", "--no-install", "graphforge", "--version"],
        cwd=work,
    )
    reported = out.strip()
    if ctx.version not in reported:
        raise VerifyError(
            f"@curatelabs/graphforge-cli version mismatch: expected {ctx.version!r} in {reported!r}"
        )
    result.notes.append(f"published CLI executable ok: {reported}")
    result.ok = True
    return result


def lane_skills(ctx: Context) -> LaneResult:
    result = LaneResult(name="skills", issue=LANE_ISSUES["skills"], ok=False)
    if not ctx.allow_network_install:
        result.error = "network installs disabled"
        return result
    work = ctx.work_root / "skills"
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)
    result.commands.append("npm init -y")
    _run(ctx, ["npm", "init", "-y"], cwd=work)
    pkg = f"@curatelabs/graphforge-agent-skills@{ctx.version}"
    result.commands.append(f"npm install {pkg}")
    _run(ctx, ["npm", "install", pkg], cwd=work)
    result.commands.append(
        "npx --offline --no-install graphforge-agent-skills compatibility --json"
    )
    out = _run(
        ctx,
        [
            "npx",
            "--offline",
            "--no-install",
            "graphforge-agent-skills",
            "compatibility",
            "--json",
        ],
        cwd=work,
    )
    payload = json.loads(out)
    release = None
    if isinstance(payload, dict):
        release = payload.get("release")
        compat = payload.get("graphforgeCompatibility")
        if release is None and isinstance(compat, dict):
            release = compat.get("release")
    if (
        release is not None
        and str(release) != ctx.version
        and not str(release).startswith(ctx.version)
    ):
        raise VerifyError(f"skills compatibility release mismatch: {release!r}")
    result.notes.append("skills bootstrap/compatibility ok")
    result.artifacts["compatibility"] = payload if isinstance(payload, dict) else {"raw": out}
    result.ok = True
    return result


def lane_cargo(ctx: Context) -> LaneResult:
    result = LaneResult(name="cargo", issue=LANE_ISSUES["cargo"], ok=False)
    if not ctx.allow_network_install:
        result.error = "network installs disabled"
        return result
    work = ctx.work_root / "cargo"
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)
    crate_dir = work / "graphforge_clean_env_smoke"
    result.commands.append("cargo new --bin graphforge_clean_env_smoke")
    _run(ctx, ["cargo", "new", "--bin", "graphforge_clean_env_smoke"], cwd=work)
    for crate in ctx.crates:
        result.commands.append(f"cargo add {crate}@{ctx.version}")
        _run(ctx, ["cargo", "add", f"{crate}@{ctx.version}"], cwd=crate_dir)
    main_rs = crate_dir / "src" / "main.rs"
    main_rs.write_text(
        'fn main() {\n    println!("cargo-smoke-ok");\n}\n',
        encoding="utf-8",
    )
    result.commands.append("cargo check")
    _run(ctx, ["cargo", "check"], cwd=crate_dir)
    result.notes.append(f"cargo add/check ok for: {', '.join(ctx.crates)}")
    result.ok = True
    return result


def lane_urls(ctx: Context) -> LaneResult:
    result = LaneResult(name="urls", issue=LANE_ISSUES["urls"], ok=False)
    urls = registry_urls(ctx.version, ctx.crates, ctx.docs_base)
    keys = [
        "docs_quickstart",
        "docs_installation",
        "pypi_project",
        "npm_node_page",
        "npm_cli_page",
        "npm_skills_page",
        "github_release",
        *[f"crates_page_{crate}" for crate in ctx.crates],
    ]
    resolved: dict[str, int] = {}
    failures: list[str] = []
    for key in keys:
        url = urls[key]
        status, _, _ = ctx.fetch(url)
        resolved[key] = status
        result.commands.append(f"GET {url}")
        if status >= 400:
            failures.append(f"{key} -> {status}")
    result.artifacts["statuses"] = resolved
    if failures:
        result.error = "URL resolve failures: " + ", ".join(failures)
        return result
    result.ok = True
    result.notes.append(f"resolved {len(keys)} docs/package URLs")
    return result


def _pypi_file_digests(ctx: Context) -> dict[str, str]:
    body, _ = http_ok(
        ctx,
        f"https://pypi.org/pypi/graphforge/{ctx.version}/json",
        context="pypi digests",
    )
    payload = parse_json(body, context="pypi digests")
    urls = payload.get("urls") if isinstance(payload, dict) else None
    if not isinstance(urls, list):
        raise VerifyError("pypi JSON missing urls[]")
    digests: dict[str, str] = {}
    for item in urls:
        if not isinstance(item, dict):
            continue
        filename = item.get("filename")
        digests_obj = item.get("digests") if isinstance(item.get("digests"), dict) else {}
        digest = digests_obj.get("sha256")
        if isinstance(filename, str) and isinstance(digest, str):
            digests[filename] = digest.lower()
    if not digests:
        raise VerifyError("pypi JSON contained no sha256 digests")
    return digests


def _npm_dist_digest(ctx: Context, package: str) -> tuple[str, str]:
    body, _ = http_ok(
        ctx,
        f"https://registry.npmjs.org/{package}/{ctx.version}",
        context=f"npm {package}",
    )
    payload = parse_json(body, context=f"npm {package}")
    dist = payload.get("dist") if isinstance(payload, dict) else None
    if not isinstance(dist, dict):
        raise VerifyError(f"npm {package} missing dist")
    tarball = dist.get("tarball")
    integrity = dist.get("integrity") or dist.get("shasum")
    if not isinstance(tarball, str) or not isinstance(integrity, str):
        raise VerifyError(f"npm {package} missing tarball/integrity")
    filename = Path(urlparse(tarball).path).name
    if integrity.startswith("sha256-"):
        # npm integrity is base64; store the integrity string as opaque expected value
        return filename, integrity
    if len(integrity) == 40:
        # shasum (sha1) — still record for comparison against release record if present
        return filename, f"sha1:{integrity}"
    return filename, integrity


def _crates_digest(ctx: Context, crate: str) -> tuple[str, str]:
    body, _ = http_ok(
        ctx,
        f"https://crates.io/api/v1/crates/{crate}/{ctx.version}",
        context=f"crates {crate}",
    )
    payload = parse_json(body, context=f"crates {crate}")
    version = payload.get("version") if isinstance(payload, dict) else None
    if not isinstance(version, dict):
        raise VerifyError(f"crates.io {crate} missing version object")
    checksum = version.get("checksum")
    filename = f"{crate}-{ctx.version}.crate"
    if not isinstance(checksum, str) or len(checksum) != 64:
        raise VerifyError(f"crates.io {crate} missing sha256 checksum")
    return filename, checksum.lower()


def lane_checksums(ctx: Context) -> LaneResult:
    result = LaneResult(name="checksums", issue=LANE_ISSUES["checksums"], ok=False)
    if ctx.release_record is None:
        result.error = (
            "checksums lane requires --release-record PATH "
            f"({RELEASE_RECORD_SCHEMA}) produced by #2798 and shipped with #2803"
        )
        return result
    record = ctx.release_record
    if record["version"] != ctx.version:
        raise VerifyError(
            f"release record version {record['version']!r} != requested {ctx.version!r}"
        )

    observed: dict[str, dict[str, str]] = {"pypi": {}, "npm": {}, "crates": {}}
    observed["pypi"] = _pypi_file_digests(ctx)
    for package, surface_key in (
        ("@curatelabs/graphforge", "npm"),
        ("@curatelabs/graphforge-cli", "npm"),
        ("@curatelabs/graphforge-agent-skills", "npm"),
    ):
        filename, digest = _npm_dist_digest(ctx, package)
        observed[surface_key][f"{package}:{filename}"] = digest
    for crate in ctx.crates:
        filename, digest = _crates_digest(ctx, crate)
        observed["crates"][filename] = digest

    expected_by_key: dict[str, str] = {}
    for artifact in record["artifacts"]:
        surface = artifact["surface"]
        name = artifact["name"]
        filename = artifact.get("filename") or name
        key = f"{surface}:{filename}"
        expected_by_key[key] = artifact["sha256"]
        # also allow name-qualified npm keys
        expected_by_key[f"{surface}:{name}:{filename}"] = artifact["sha256"]

    mismatches: list[str] = []
    matched: list[str] = []
    for surface, items in observed.items():
        for filename, digest in items.items():
            # normalize npm keys "pkg:file" vs bare file
            candidates = [
                f"{surface}:{filename}",
                f"{surface}:{filename.split(':')[-1]}",
            ]
            expected = next((expected_by_key[c] for c in candidates if c in expected_by_key), None)
            if expected is None:
                continue
            # npm may store integrity strings in observed; only compare sha256 hex records
            if len(digest) == 64 and all(ch in "0123456789abcdef" for ch in digest):
                if digest != expected:
                    mismatches.append(f"{surface}:{filename}")
                else:
                    matched.append(f"{surface}:{filename}")
            elif digest.startswith("sha256-") or digest.startswith("sha1:"):
                result.notes.append(
                    f"observed non-sha256 digest for {surface}:{filename}; "
                    "compare manually against release record integrity fields if present"
                )

    result.artifacts = {
        "observed": observed,
        "matched": matched,
        "release_record_version": record["version"],
        "release_record_artifact_count": len(record["artifacts"]),
    }
    result.commands.append("compare registry digests to release record")
    if not matched and not any(len(v) == 64 for items in observed.values() for v in items.values()):
        result.error = "no sha256 digests available to compare against release record"
        return result
    if mismatches:
        result.error = "checksum mismatches: " + ", ".join(mismatches)
        return result
    if not matched:
        result.error = (
            "release record had no overlapping sha256 artifacts with observed "
            "PyPI digests; update the record filenames/surfaces"
        )
        return result
    result.ok = True
    result.notes.append(f"matched {len(matched)} artifact checksum(s)")
    return result


LANE_RUNNERS: dict[str, Callable[[Context], LaneResult]] = {
    "pip": lane_pip,
    "npm": lane_npm,
    "cli": lane_cli,
    "skills": lane_skills,
    "cargo": lane_cargo,
    "reopen": lane_reopen,
    "urls": lane_urls,
    "checksums": lane_checksums,
}


def run_subprocess(
    argv: list[str],
    *,
    cwd: str | None = None,
    text: bool = True,
    capture_output: bool = True,
    check: bool = False,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv,
        cwd=cwd,
        text=text,
        capture_output=capture_output,
        check=check,
    )


def build_evidence(
    version: str,
    lane_results: list[LaneResult],
    *,
    preflight: LaneResult | None = None,
) -> dict[str, Any]:
    lanes = {lane.name: lane.to_dict() for lane in lane_results}
    if preflight is not None:
        lanes["preflight"] = preflight.to_dict()
    ok = all(lane.ok for lane in lane_results) and (preflight is None or preflight.ok)
    return {
        "schema": EVIDENCE_SCHEMA,
        "version": version,
        "generated_at": utc_now(),
        "ok": ok,
        "lanes": lanes,
        "issue_map": dict(LANE_ISSUES),
        "tracker": 167,
        "umbrella": 192,
    }


def cmd_preflight(args: argparse.Namespace) -> int:
    version = require_version(args.version)
    crates = tuple(args.crate) if args.crate else DEFAULT_CRATES
    with tempfile.TemporaryDirectory(prefix="gf-clean-env-") as tmp:
        ctx = Context(
            version=version,
            work_root=Path(tmp),
            docs_base=args.docs_base,
            crates=crates,
            release_record=None,
            fetch=default_fetcher,
            run_cmd=run_subprocess,
            allow_network_install=False,
        )
        result = run_preflight(ctx)
        evidence = build_evidence(version, [], preflight=result)
        print(json.dumps(evidence, indent=2, sort_keys=True))
        if args.output:
            Path(args.output).write_text(
                json.dumps(evidence, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        if not result.ok:
            print(result.error or "preflight failed", file=sys.stderr)
            return 1
        return 0


def cmd_run(args: argparse.Namespace) -> int:
    version = require_version(args.version)
    crates = tuple(args.crate) if args.crate else DEFAULT_CRATES
    lanes = list(ALL_LANES) if args.all else list(args.lane or [])
    if not lanes:
        raise VerifyError("specify --all or one or more --lane values")
    unknown = [lane for lane in lanes if lane not in LANE_RUNNERS]
    if unknown:
        raise VerifyError(f"unknown lanes: {', '.join(unknown)}")

    record = load_release_record(Path(args.release_record)) if args.release_record else None
    if "checksums" in lanes and record is None:
        raise VerifyError("lane 'checksums' requires --release-record")

    work_root = (
        Path(args.work).resolve() if args.work else Path(tempfile.mkdtemp(prefix="gf-clean-env-"))
    )
    work_root.mkdir(parents=True, exist_ok=True)

    ctx = Context(
        version=version,
        work_root=work_root,
        docs_base=args.docs_base,
        crates=crates,
        release_record=record,
        fetch=default_fetcher,
        run_cmd=run_subprocess,
        allow_network_install=not args.skip_installs,
    )

    preflight = run_preflight(ctx)
    lane_results: list[LaneResult] = []
    if not preflight.ok and not args.allow_unpublished:
        evidence = build_evidence(version, lane_results, preflight=preflight)
        text = json.dumps(evidence, indent=2, sort_keys=True) + "\n"
        print(text, end="")
        if args.output:
            Path(args.output).write_text(text, encoding="utf-8")
        print(preflight.error or "preflight failed", file=sys.stderr)
        return 2

    for name in lanes:
        try:
            lane_results.append(LANE_RUNNERS[name](ctx))
        except VerifyError as exc:  # noqa: PERF203
            lane_results.append(
                LaneResult(
                    name=name,
                    issue=LANE_ISSUES.get(name),
                    ok=False,
                    error=str(exc),
                )
            )

    evidence = build_evidence(version, lane_results, preflight=preflight)
    text = json.dumps(evidence, indent=2, sort_keys=True) + "\n"
    print(text, end="")
    if args.output:
        Path(args.output).write_text(text, encoding="utf-8")
    return 0 if evidence["ok"] else 1


def cmd_validate_evidence(args: argparse.Namespace) -> int:
    payload = json.loads(Path(args.path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise VerifyError("evidence root must be an object")
    validate_evidence(payload)
    if args.require_ok and not payload["ok"]:
        raise VerifyError("evidence.ok is false")
    print("evidence ok")
    return 0


def cmd_validate_release_record(args: argparse.Namespace) -> int:
    load_release_record(Path(args.path))
    print("release record ok")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    pre = sub.add_parser("preflight", help="Probe public registries; fail if unpublished")
    pre.add_argument("--version", default=DEFAULT_VERSION)
    pre.add_argument("--docs-base", default=DEFAULT_DOCS_BASE)
    pre.add_argument("--crate", action="append", default=[])
    pre.add_argument("--output")
    pre.set_defaults(func=cmd_preflight)

    run = sub.add_parser("run", help="Run clean-env lanes against public registries")
    run.add_argument("--version", default=DEFAULT_VERSION)
    run.add_argument("--docs-base", default=DEFAULT_DOCS_BASE)
    run.add_argument("--crate", action="append", default=[])
    run.add_argument("--lane", action="append", choices=list(ALL_LANES))
    run.add_argument("--all", action="store_true")
    run.add_argument("--release-record", help=f"Path to {RELEASE_RECORD_SCHEMA} JSON")
    run.add_argument("--work", help="Work directory (default: temp dir)")
    run.add_argument("--output", help="Write evidence JSON to this path")
    run.add_argument(
        "--allow-unpublished",
        action="store_true",
        help="Continue after failed preflight (debug only; still fails lanes)",
    )
    run.add_argument(
        "--skip-installs",
        action="store_true",
        help="Disable pip/npm/cargo installs (tests / dry structural runs)",
    )
    run.set_defaults(func=cmd_run)

    ve = sub.add_parser("validate-evidence", help=f"Validate {EVIDENCE_SCHEMA}")
    ve.add_argument("path")
    ve.add_argument("--require-ok", action="store_true")
    ve.set_defaults(func=cmd_validate_evidence)

    vr = sub.add_parser("validate-release-record", help=f"Validate {RELEASE_RECORD_SCHEMA}")
    vr.add_argument("path")
    vr.set_defaults(func=cmd_validate_release_record)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except VerifyError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
