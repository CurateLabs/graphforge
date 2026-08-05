#!/usr/bin/env python3
"""Fetch Web Data Commons Hyperlink Graph artifacts with resume and verify.

Verified against live WDC download pages (2012-08 / 2014-04). Some data.dws
hosts return HTTP 403 without a WDC Referer; this helper always sends one for
origin URLs. When ``GF_WDC_MIRROR_BASE`` is set, downloads prefer a CurateLabs-
controlled object-storage mirror (see docs/guide/datasets/wdc-hyperlink-graph.md).

Examples:
  python3 scripts/datasets/fetch_wdc_hyperlink.py --artifact example
  GF_WDC_MIRROR_BASE=https://wdc.example/wdc-hyperlink \\
    python3 scripts/datasets/fetch_wdc_hyperlink.py --artifact pld-2012 --source mirror-only
  python3 scripts/datasets/fetch_wdc_hyperlink.py --verify-urls
"""

from __future__ import annotations

import argparse
from collections.abc import Iterable
from dataclasses import dataclass
import hashlib
import os
from pathlib import Path
import subprocess
import sys
import urllib.error
from urllib.parse import urljoin
import urllib.request

USER_AGENT = "GraphForge-wdc-fetch/1.0 (+https://github.com/CurateLabs/graphforge; research)"
DEFAULT_CACHE = Path(
    os.environ.get("GF_WDC_CACHE", Path.home() / ".cache/graphforge/wdc-hyperlink")
)
SOURCE_CHOICES = ("mirror-first", "mirror-only", "origin")


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


def _normalize_mirror_base(base: str | None) -> str | None:
    if base is None:
        return None
    trimmed = base.strip()
    if not trimmed:
        return None
    if not trimmed.endswith("/"):
        trimmed += "/"
    return trimmed


def _mirror_url(mirror_base: str, relpath: str) -> str:
    return urljoin(mirror_base, relpath)


def _resolve_source(cli_source: str | None, mirror_base: str | None) -> str:
    """Pick download source policy.

    Defaults: ``mirror-first`` when a mirror base is configured, else ``origin``.
    """
    if cli_source is not None:
        return cli_source
    env = os.environ.get("GF_WDC_SOURCE", "").strip().lower()
    if env:
        if env not in SOURCE_CHOICES:
            raise SystemExit(
                f"GF_WDC_SOURCE must be one of {', '.join(SOURCE_CHOICES)}; got {env!r}"
            )
        return env
    return "mirror-first" if mirror_base else "origin"


def _candidate_urls(
    meta: ArtifactFile, *, mirror_base: str | None, source: str
) -> list[tuple[str, str | None]]:
    """Return ordered (url, referer_or_None) candidates.

    Mirror URLs omit the WDC Referer (public object storage). Origin keeps it.
    """
    origin = (meta.url, meta.referer)
    if source == "origin" or not mirror_base:
        return [origin]
    mirror = (_mirror_url(mirror_base, meta.relpath), None)
    if source == "mirror-only":
        return [mirror]
    # mirror-first
    return [mirror, origin]


def _head_content_length(url: str, referer: str | None) -> tuple[int, int | None]:
    """Return (http_status, content_length_or_None)."""
    headers = {"User-Agent": USER_AGENT}
    if referer:
        headers["Referer"] = referer
    request = urllib.request.Request(url, method="HEAD", headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            status = getattr(response, "status", 200) or 200
            raw = response.headers.get("Content-Length")
            length = int(raw) if raw is not None else None
            return status, length
    except urllib.error.HTTPError as exc:
        return exc.code, None
    except urllib.error.URLError:
        return 0, None


def _curl_download(url: str, dest: Path, referer: str | None) -> None:
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
    ]
    if referer:
        cmd.extend(["-e", referer])
    cmd.extend(["--continue-at", "-", "-o", str(partial), url])
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


def fetch_files(
    files: Iterable[ArtifactFile],
    cache: Path,
    force: bool,
    *,
    mirror_base: str | None,
    source: str,
) -> int:
    errors = 0
    for meta in files:
        dest = cache / meta.relpath
        if not force and _file_ok(dest, meta):
            print(f"OK  {dest} (cached)", file=sys.stderr)
            continue
        if dest.exists() and force:
            dest.unlink()
        candidates = _candidate_urls(meta, mirror_base=mirror_base, source=source)
        downloaded = False
        last_error: Exception | None = None
        for url, referer in candidates:
            try:
                _curl_download(url, dest, referer)
            except subprocess.CalledProcessError as exc:
                last_error = exc
                print(
                    f"download failed ({exc.returncode}): {url}",
                    file=sys.stderr,
                )
                # Drop a bad partial before trying the next candidate.
                partial = dest.with_suffix(dest.suffix + ".partial")
                if partial.exists():
                    partial.unlink()
                if dest.exists() and not _file_ok(dest, meta):
                    dest.unlink()
                continue
            if not _file_ok(dest, meta):
                print(f"verify failed after download: {dest} (from {url})", file=sys.stderr)
                if dest.exists():
                    dest.unlink()
                last_error = RuntimeError(f"verify failed for {url}")
                continue
            downloaded = True
            detail = []
            if meta.expected_bytes is not None:
                detail.append(f"{meta.expected_bytes} bytes")
            if meta.md5:
                detail.append(f"md5={meta.md5}")
            if meta.text_line_count is not None:
                detail.append(f"{meta.text_line_count} lines")
            suffix = f" ({', '.join(detail)})" if detail else ""
            via = "mirror" if mirror_base and url.startswith(mirror_base.rstrip("/")) else "origin"
            print(f"OK  {dest}{suffix} via {via}", file=sys.stderr)
            break
        if not downloaded:
            if last_error is not None:
                print(f"all sources failed for {meta.relpath}", file=sys.stderr)
            errors += 1
    return errors


def verify_urls(
    files: Iterable[ArtifactFile],
    *,
    mirror_base: str | None,
    source: str,
) -> int:
    errors = 0
    for meta in files:
        candidates = _candidate_urls(meta, mirror_base=mirror_base, source=source)
        file_ok = False
        for url, referer in candidates:
            status, length = _head_content_length(url, referer)
            ok = status == 200
            if meta.expected_bytes is not None and length is not None:
                ok = ok and length == meta.expected_bytes
            mark = "OK" if ok else "FAIL"
            print(f"{mark}\tHTTP {status}\tCL={length}\texpected={meta.expected_bytes}\t{url}")
            if ok:
                file_ok = True
                if source == "mirror-first":
                    break
        if not file_ok:
            errors += 1
    return errors


def list_artifacts(*, mirror_base: str | None) -> None:
    if mirror_base:
        print(f"mirror_base\t{mirror_base.rstrip('/')}")
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
            print(f"    origin: {meta.url}")
            if mirror_base:
                print(f"    mirror: {_mirror_url(mirror_base, meta.relpath)}")


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
    parser.add_argument(
        "--tier-min",
        action="store_true",
        help="fetch T0-T3 bootstrap set: example + pld-2014-webgraph + pld-2012 (~3.3 GiB)",
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
    parser.add_argument(
        "--mirror-base",
        default=os.environ.get("GF_WDC_MIRROR_BASE"),
        help="controlled mirror base URL (env: GF_WDC_MIRROR_BASE); keys = cache relpaths",
    )
    parser.add_argument(
        "--source",
        choices=SOURCE_CHOICES,
        default=None,
        help=(
            "download source policy (env: GF_WDC_SOURCE). "
            "Default: mirror-first when --mirror-base/GF_WDC_MIRROR_BASE is set, else origin"
        ),
    )
    args = parser.parse_args(argv)

    mirror_base = _normalize_mirror_base(args.mirror_base)
    source = _resolve_source(args.source, mirror_base)
    if source in ("mirror-first", "mirror-only") and not mirror_base:
        print(
            f"error: source {source!r} requires --mirror-base or GF_WDC_MIRROR_BASE",
            file=sys.stderr,
        )
        return 2

    if args.list:
        list_artifacts(mirror_base=mirror_base)
        return 0

    names: list[str]
    if args.tier_min:
        names = ["example", "pld-2014-webgraph", "pld-2012"]
    elif args.all_safe:
        names = ["example", "page-lists-2014", "page-lists-2012"]
    elif args.artifact:
        names = args.artifact
    else:
        names = ["example"]

    files: list[ArtifactFile] = []
    for name in names:
        files.extend(ARTIFACTS[name].files)

    if mirror_base:
        print(f"mirror: {mirror_base.rstrip('/')} (source={source})", file=sys.stderr)
    else:
        print(f"mirror: (none; source={source})", file=sys.stderr)

    if args.verify_urls:
        return 1 if verify_urls(files, mirror_base=mirror_base, source=source) else 0

    args.cache.mkdir(parents=True, exist_ok=True)
    print(f"cache: {args.cache}", file=sys.stderr)
    return (
        1
        if fetch_files(files, args.cache, force=args.force, mirror_base=mirror_base, source=source)
        else 0
    )


if __name__ == "__main__":
    sys.exit(main())
