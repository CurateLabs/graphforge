"""Pinned dataset acquisition for GDC suites.

Downloads land in ``<name>.partial``, are verified against the pinned SHA-256,
and are renamed into place atomically. A mismatch is a typed
``checksum_mismatch`` and is never retried. Archives are extracted with the
system ``tar --zstd``. The emitted ``graphforge-gdc-acquisition/1`` document is
validated by ``gdc_contracts`` against the suite's selected identity profile.
Network access goes through an injectable opener so tests serve committed bytes.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractContextManager
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
from typing import Any, BinaryIO
from urllib.parse import urlparse
import urllib.request

from graphforge_bench.gdc_contracts import (
    ACQUISITION_SCHEMA,
    GdcContractError,
    load_suite_declaration,
    resolve_pinned_identity,
    validate_acquisition,
    workspace_root,
)

ALLOWED_HOST = "datasets.ldbcouncil.org"
CHUNK_BYTES = 1024 * 1024
Opener = Callable[[str], AbstractContextManager[BinaryIO]]
DeviceOf = Callable[[Path], int]


class DatasetCacheError(ValueError):
    """Acquisition failed with a stable machine-readable cause."""

    def __init__(self, cause: str, message: str) -> None:
        super().__init__(message)
        self.cause = cause


def default_opener(url: str) -> AbstractContextManager[BinaryIO]:
    return urllib.request.urlopen(url, timeout=120)


def _device_of(path: Path) -> int:
    return path.stat().st_dev


def require_cache_root(
    cache_root: Path,
    *,
    repo_root: Path,
    work_root: Path,
    device_of: DeviceOf = _device_of,
) -> Path:
    """Resolve and create the cache root, or refuse it with a typed cause.

    The cache must sit on the same device as ``/`` (durable projects require
    that filesystem class), outside the repository, and disjoint from the
    ladder work root so reclaiming a rung never deletes cached archives.
    """
    resolved = cache_root.resolve()
    repo = repo_root.resolve()
    work = work_root.resolve()
    if resolved == repo or repo in resolved.parents:
        raise DatasetCacheError(
            "cache_root_in_repository", f"cache root {resolved} is inside the repository"
        )
    if resolved == work or work in resolved.parents or resolved in work.parents:
        raise DatasetCacheError(
            "cache_root_overlaps_work_root",
            f"cache root {resolved} overlaps the work root {work}",
        )
    anchor = resolved
    while not anchor.exists():
        anchor = anchor.parent
    if device_of(anchor) != device_of(Path("/")):
        raise DatasetCacheError(
            "cache_root_wrong_device",
            f"cache root {resolved} is not on the same device as /",
        )
    resolved.mkdir(parents=True, exist_ok=True)
    return resolved


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(CHUNK_BYTES):
            digest.update(chunk)
    return digest.hexdigest()


def _archive_name(url: str) -> str:
    parsed = urlparse(url)
    if parsed.scheme != "https" or parsed.hostname != ALLOWED_HOST:
        raise DatasetCacheError(
            "unsupported_source", f"dataset source must be https://{ALLOWED_HOST}/..., got {url}"
        )
    name = Path(parsed.path).name
    if not name or name.startswith(".") or "/" in name or not name.endswith(".tar.zst"):
        raise DatasetCacheError("unsupported_source", f"dataset source is not a .tar.zst: {url}")
    return name


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def acquire_archive(pin: Mapping[str, Any], archive_dir: Path, *, opener: Opener) -> Path:
    """Return the verified archive for ``pin``, downloading it at most once.

    An archive already in place is re-hashed and reused; a cached archive that
    no longer matches its pin is a ``checksum_mismatch`` and is left untouched
    for inspection. No path retries a download.
    """
    expected = pin["checksum_sha256"]
    name = _archive_name(pin["source"])
    archive_dir.mkdir(parents=True, exist_ok=True)
    final = archive_dir / name
    partial = archive_dir / f"{name}.partial"
    if final.exists():
        actual = sha256_file(final)
        if actual != expected:
            raise DatasetCacheError(
                "checksum_mismatch",
                f"cached {final} has sha256 {actual}, pin {pin['id']} expects {expected}",
            )
        return final
    partial.unlink(missing_ok=True)
    digest = hashlib.sha256()
    try:
        with opener(pin["source"]) as response, partial.open("wb") as handle:
            while chunk := response.read(CHUNK_BYTES):
                digest.update(chunk)
                handle.write(chunk)
            handle.flush()
            os.fsync(handle.fileno())
        if digest.hexdigest() != expected:
            raise DatasetCacheError(
                "checksum_mismatch",
                f"download of {pin['source']} has sha256 {digest.hexdigest()}, "
                f"pin {pin['id']} expects {expected}",
            )
        partial.replace(final)
        _fsync_directory(archive_dir)
    except BaseException:
        partial.unlink(missing_ok=True)
        raise
    return final


def extract_archive(archive: Path, destination: Path) -> Path:
    """Extract with system ``tar --zstd`` into a partial directory, then rename."""
    if destination.exists():
        return destination
    partial = destination.with_name(destination.name + ".partial")
    shutil.rmtree(partial, ignore_errors=True)
    partial.mkdir(parents=True)
    try:
        completed = subprocess.run(
            ["tar", "--zstd", "-xf", str(archive), "-C", str(partial)],
            check=False,
            capture_output=True,
            text=True,
        )
        if completed.returncode != 0:
            raise DatasetCacheError(
                "extraction_failed", f"tar failed for {archive}: {completed.stderr.strip()}"
            )
        partial.replace(destination)
    except BaseException:
        shutil.rmtree(partial, ignore_errors=True)
        raise
    return destination


def acquire(
    *,
    suite_path: Path,
    profile: str,
    dataset_ids: Sequence[str],
    cache_root: Path,
    work_root: Path,
    repo_root: Path | None = None,
    root: Path | None = None,
    opener: Opener = default_opener,
    device_of: DeviceOf = _device_of,
    extract: bool = True,
) -> dict[str, Any]:
    """Acquire ``dataset_ids`` and return the validated acquisition result.

    The result carries the ``graphforge-gdc-acquisition/1`` document, the
    ``graphforge-gdc-suite-evidence/1`` document from ``validate_acquisition``,
    and the extracted directory of each dataset. The pin is the one the suite
    selects as ``profile``; validation runs against the requested datasets only,
    so a single archive can be acquired without fetching the others.
    """
    base = root or workspace_root()
    cache = require_cache_root(
        cache_root,
        repo_root=repo_root or base.parent,
        work_root=work_root,
        device_of=device_of,
    )
    suite = load_suite_declaration(suite_path)
    probe = {"identity_profile": profile}
    pin = resolve_pinned_identity(suite, probe, base)
    pinned = {item["id"]: item for item in pin["datasets"]}
    unknown = sorted(set(dataset_ids) - set(pinned))
    if unknown or not dataset_ids:
        raise DatasetCacheError(
            "unknown_dataset",
            f"datasets not pinned by profile {profile}: {unknown or 'none given'}",
        )
    suite_cache = cache / suite["suite_id"]
    archive_dir = suite_cache / "archives"
    extracted: dict[str, Path] = {}
    assets = []
    for dataset_id in dataset_ids:
        archive = acquire_archive(pinned[dataset_id], archive_dir, opener=opener)
        if extract:
            extracted[dataset_id] = extract_archive(archive, suite_cache / "extracted" / dataset_id)
        assets.append(
            {
                "id": dataset_id,
                "path": archive.name,
                "checksum_sha256": pinned[dataset_id]["checksum_sha256"],
                "license": pinned[dataset_id]["license"],
                "acquisition": pinned[dataset_id]["acquisition"],
            }
        )
    acquisition = {
        "schema": ACQUISITION_SCHEMA,
        "suite_id": pin["suite_id"],
        "identity_profile": profile,
        "recorded_spec": pin["spec"],
        "recorded_generator": pin["generator"],
        "recorded_driver": pin["driver"],
        "assets": assets,
        "references": [],
    }
    subset_pin = {**pin, "datasets": [pinned[dataset_id] for dataset_id in dataset_ids]}
    evidence = validate_acquisition(subset_pin, acquisition, archive_dir)
    document_path = suite_cache / f"acquisition-{profile}.json"
    document_path.write_text(json.dumps(acquisition, indent=2) + "\n", encoding="utf-8")
    return {
        "acquisition": acquisition,
        "acquisition_path": document_path,
        "evidence": evidence,
        "archive_dir": archive_dir,
        "extracted": extracted,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", default="graphalytics")
    parser.add_argument("--profile", default="scorecard")
    parser.add_argument("--dataset", action="append", required=True, dest="datasets")
    parser.add_argument("--cache-root", type=Path, required=True)
    parser.add_argument("--work-root", type=Path, required=True)
    args = parser.parse_args(argv)
    base = workspace_root()
    try:
        result = acquire(
            suite_path=base / "suites" / f"gdc-{args.suite}.json",
            profile=args.profile,
            dataset_ids=args.datasets,
            cache_root=args.cache_root,
            work_root=args.work_root,
        )
    except (DatasetCacheError, GdcContractError) as error:
        print(json.dumps({"error": {"cause": error.cause, "message": str(error)}}), file=sys.stderr)
        return 2
    print(
        json.dumps(
            {
                "acquisition": str(result["acquisition_path"]),
                "evidence_status": result["evidence"]["status"],
                "extracted": {key: str(value) for key, value in result["extracted"].items()},
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
