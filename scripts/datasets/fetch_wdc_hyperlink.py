#!/usr/bin/env python3
"""Fetch Web Data Commons Hyperlink Graph artifacts with resume and verify.

Verified against live WDC download pages (2012-08 / 2014-04). Some data.dws
hosts return HTTP 403 without a WDC Referer; this helper always sends one.

Examples:
  python3 scripts/datasets/fetch_wdc_hyperlink.py --artifact example
  python3 scripts/datasets/fetch_wdc_hyperlink.py --artifact pld-2012
  python3 scripts/datasets/fetch_wdc_hyperlink.py --verify-urls
"""

from __future__ import annotations

import argparse
import hashlib
import os
import subprocess
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

USER_AGENT = (
    "GraphForge-wdc-fetch/1.0 (+https://github.com/CurateLabs/graphforge; research)"
)
DEFAULT_CACHE = Path(
    os.environ.get("GF_WDC_CACHE", Path.home() / ".cache/graphforge/wdc-hyperlink")
)


@dataclass(frozen=True)
class ArtifactFile:
    """One downloadable file."""

    relpath: str
    url: str
    referer: str
    expected_bytes: int | None = None
    md5: str | None = None
    # When True, expected_bytes is advisory (line-oriented text without CL).
    text_line_count: int | None = None


@dataclass(frozen=True)
class ArtifactSet:
    name: str
    description: str
    files: tuple[ArtifactFile, ...]


REF_2012 = "https://webdatacommons.org/hyperlinkgraph/2012-08/download.html"
REF_2014 = "https://webdatacommons.org/hyperlinkgraph/2014-04/download.html"
REF_OVERVIEW = "https://webdatacommons.org/hyperlinkgraph/"

ARTIFACTS: dict[str, ArtifactSet] = {
    "example": ArtifactSet(
        name="example",
        description="WDC tiny Index/Arc sample (106 nodes / 141 arcs)",
        files=(
            ArtifactFile(
                "example/example_index",
                "https://webdatacommons.org/hyperlinkgraph/data/example_index",
                REF_OVERVIEW,
                text_line_count=106,
            ),
            ArtifactFile(
                "example/example_arcs",
                "https://webdatacommons.org/hyperlinkgraph/data/example_arcs",
                REF_OVERVIEW,
                text_line_count=141,
            ),
        ),
    ),
    "pld-2012": ArtifactSet(
        name="pld-2012",
        description="2012 PLD Index/Arc (compressed; ~3.0 GiB total)",
        files=(
            ArtifactFile(
                "2012-08/pld-index.gz",
                "https://data.dws.informatik.uni-mannheim.de/hyperlinkgraph/2012-08/pld-index.gz",
                REF_2012,
                expected_bytes=311_068_910,
            ),
            ArtifactFile(
                "2012-08/pld-arc.gz",
                "https://data.dws.informatik.uni-mannheim.de/hyperlinkgraph/2012-08/pld-arc.gz",
                REF_2012,
                expected_bytes=2_912_232_966,
            ),
        ),
    ),
    "host-2012": ArtifactSet(
        name="host-2012",
        description="2012 Host/subdomain Index/Arc (compressed; ~9.4 GiB total)",
        files=(
            ArtifactFile(
                "2012-08/sd-index.gz",
                "https://data.dws.informatik.uni-mannheim.de/hyperlinkgraph/2012-08/sd-index.gz",
                REF_2012,
                expected_bytes=871_791_708,
            ),
            ArtifactFile(
                "2012-08/sd-arc.gz",
                "https://data.dws.informatik.uni-mannheim.de/hyperlinkgraph/2012-08/sd-arc.gz",
                REF_2012,
                expected_bytes=9_216_059_662,
            ),
        ),
    ),
    "pld-2014-webgraph": ArtifactSet(
        name="pld-2014-webgraph",
        description="2014 PLD index + WebGraph (Index/Arc arcs not published)",
        files=(
            ArtifactFile(
                "2014-03/webgraph/index.pld.gz",
                "https://data.dws.informatik.uni-mannheim.de/hyperlinkgraph/2014-03/webgraph/index.pld.gz",
                REF_2014,
                expected_bytes=168_635_660,
                md5="ab13f50eb5ffb4b62c1a0cdd69a4f749",
            ),
            ArtifactFile(
                "2014-03/webgraph/pldgraph.graph",
                "https://data.dws.informatik.uni-mannheim.de/hyperlinkgraph/2014-03/webgraph/pldgraph.graph",
                REF_2014,
                expected_bytes=139_534_900,
                md5="8cafd7e62f198ad4cd13ce8dd1c0e5c4",
            ),
            ArtifactFile(
                "2014-03/webgraph/pldgraph.offsets",
                "https://data.dws.informatik.uni-mannheim.de/hyperlinkgraph/2014-03/webgraph/pldgraph.offsets",
                REF_2014,
                expected_bytes=12_867_176,
                md5="c6c8aaf950e2fbffd893fe00106aefab",
            ),
            ArtifactFile(
                "2014-03/webgraph/pldgraph.properties",
                "https://data.dws.informatik.uni-mannheim.de/hyperlinkgraph/2014-03/webgraph/pldgraph.properties",
                REF_2014,
                expected_bytes=1_163,
                md5="ab377a864493202923a617c755c81785",
            ),
            ArtifactFile(
                "2014-03/webgraph/README",
                "https://data.dws.informatik.uni-mannheim.de/hyperlinkgraph/2014-03/webgraph/README",
                REF_2014,
                expected_bytes=1_890,
            ),
        ),
    ),
    "page-lists-2014": ArtifactSet(
        name="page-lists-2014",
        description="2014 Page shard URL lists only (not the shards)",
        files=(
            ArtifactFile(
                "page-2014/index.list.txt",
                "http://webdatacommons.org/hyperlinkgraph/2014-04/data/index.list.txt",
                REF_2014,
            ),
            ArtifactFile(
                "page-2014/arc.list.txt",
                "http://webdatacommons.org/hyperlinkgraph/2014-04/data/arc.list.txt",
                REF_2014,
            ),
        ),
    ),
    "page-lists-2012": ArtifactSet(
        name="page-lists-2012",
        description="2012 Page shard URL lists only (not the shards)",
        files=(
            ArtifactFile(
                "page-2012/index.list.txt",
                "http://webdatacommons.org/hyperlinkgraph/2012-08/data/index.list.txt",
                REF_2012,
            ),
            ArtifactFile(
                "page-2012/arc.list.txt",
                "http://webdatacommons.org/hyperlinkgraph/2012-08/data/arc.list.txt",
                REF_2012,
            ),
        ),
    ),
}


def _md5_file(path: Path) -> str:
    digest = hashlib.md5()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _line_count(path: Path) -> int:
    count = 0
    with path.open("rb") as handle:
        for _ in handle:
            count += 1
    return count


def _head_content_length(url: str, referer: str) -> tuple[int, int | None]:
    """Return (http_status, content_length_or_None)."""
    request = urllib.request.Request(
        url,
        method="HEAD",
        headers={"User-Agent": USER_AGENT, "Referer": referer},
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            status = getattr(response, "status", 200) or 200
            raw = response.headers.get("Content-Length")
            length = int(raw) if raw is not None else None
            return status, length
    except urllib.error.HTTPError as exc:
        return exc.code, None


def _curl_download(url: str, dest: Path, referer: str) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    partial = dest.with_suffix(dest.suffix + ".partial")
    # Resume into .partial, then rename on success.
    if dest.exists() and not partial.exists():
        # Already complete path handled by caller.
        return
    if dest.exists() and partial.exists():
        dest.unlink()
    cmd = [
        "curl",
        "-fL",
        "--retry",
        "5",
        "--retry-delay",
        "2",
        "--retry-all-errors",
        "-A",
        USER_AGENT,
        "-e",
        referer,
        "--continue-at",
        "-",
        "-o",
        str(partial),
        url,
    ]
    print(f"GET {url}", file=sys.stderr)
    print(f" -> {partial}", file=sys.stderr)
    subprocess.run(cmd, check=True)
    partial.replace(dest)


def _file_ok(path: Path, meta: ArtifactFile) -> bool:
    if not path.is_file():
        return False
    size = path.stat().st_size
    if meta.expected_bytes is not None and size != meta.expected_bytes:
        print(
            f"size mismatch {path}: got {size}, expected {meta.expected_bytes}",
            file=sys.stderr,
        )
        return False
    if meta.md5 is not None:
        actual = _md5_file(path)
        if actual != meta.md5:
            print(
                f"md5 mismatch {path}: got {actual}, expected {meta.md5}",
                file=sys.stderr,
            )
            return False
    if meta.text_line_count is not None:
        lines = _line_count(path)
        if lines != meta.text_line_count:
            print(
                f"line count mismatch {path}: got {lines}, expected {meta.text_line_count}",
                file=sys.stderr,
            )
            return False
    return True


def fetch_files(files: Iterable[ArtifactFile], cache: Path, force: bool) -> int:
    errors = 0
    for meta in files:
        dest = cache / meta.relpath
        if not force and _file_ok(dest, meta):
            print(f"OK  {dest} (cached)", file=sys.stderr)
            continue
        if dest.exists() and force:
            dest.unlink()
        try:
            _curl_download(meta.url, dest, meta.referer)
        except subprocess.CalledProcessError as exc:
            print(f"download failed ({exc.returncode}): {meta.url}", file=sys.stderr)
            errors += 1
            continue
        if not _file_ok(dest, meta):
            print(f"verify failed after download: {dest}", file=sys.stderr)
            errors += 1
            continue
        detail = []
        if meta.expected_bytes is not None:
            detail.append(f"{meta.expected_bytes} bytes")
        if meta.md5:
            detail.append(f"md5={meta.md5}")
        if meta.text_line_count is not None:
            detail.append(f"{meta.text_line_count} lines")
        suffix = f" ({', '.join(detail)})" if detail else ""
        print(f"OK  {dest}{suffix}", file=sys.stderr)
    return errors


def verify_urls(files: Iterable[ArtifactFile]) -> int:
    errors = 0
    for meta in files:
        status, length = _head_content_length(meta.url, meta.referer)
        ok = status == 200
        if meta.expected_bytes is not None and length is not None:
            ok = ok and length == meta.expected_bytes
        mark = "OK" if ok else "FAIL"
        print(
            f"{mark}\tHTTP {status}\tCL={length}\texpected={meta.expected_bytes}\t{meta.url}"
        )
        if not ok:
            errors += 1
    return errors


def list_artifacts() -> None:
    for name, artifact in ARTIFACTS.items():
        print(f"{name}\t{artifact.description}")
        for meta in artifact.files:
            extras = []
            if meta.expected_bytes is not None:
                extras.append(f"{meta.expected_bytes}B")
            if meta.md5:
                extras.append(f"md5={meta.md5}")
            if meta.text_line_count is not None:
                extras.append(f"{meta.text_line_count} lines")
            extra = f" ({', '.join(extras)})" if extras else ""
            print(f"  {meta.relpath}{extra}")
            print(f"    {meta.url}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--cache",
        type=Path,
        default=DEFAULT_CACHE,
        help=f"cache root (default: {DEFAULT_CACHE})",
    )
    parser.add_argument(
        "--artifact",
        action="append",
        choices=sorted(ARTIFACTS),
        help="artifact set to fetch (repeatable); default: example",
    )
    parser.add_argument(
        "--all-safe",
        action="store_true",
        help="fetch example + page-lists only (no multi-GB corpora)",
    )
    parser.add_argument("--list", action="store_true", help="list artifact catalog")
    parser.add_argument(
        "--verify-urls",
        action="store_true",
        help="HEAD-check catalog URLs (and sizes when known)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="re-download even when cached file verifies",
    )
    args = parser.parse_args(argv)

    if args.list:
        list_artifacts()
        return 0

    names: list[str]
    if args.all_safe:
        names = ["example", "page-lists-2014", "page-lists-2012"]
    elif args.artifact:
        names = args.artifact
    else:
        names = ["example"]

    files: list[ArtifactFile] = []
    for name in names:
        files.extend(ARTIFACTS[name].files)

    if args.verify_urls:
        return 1 if verify_urls(files) else 0

    args.cache.mkdir(parents=True, exist_ok=True)
    print(f"cache: {args.cache}", file=sys.stderr)
    return 1 if fetch_files(files, args.cache, force=args.force) else 0


if __name__ == "__main__":
    sys.exit(main())
