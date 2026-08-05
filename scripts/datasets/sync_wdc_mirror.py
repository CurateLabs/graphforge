#!/usr/bin/env python3
"""Bootstrap / refresh a CurateLabs-controlled WDC Hyperlink Graph mirror.

Does **not** upload by itself unless ``--execute`` is passed with a configured
S3-compatible endpoint (R2 recommended). Default mode is a dry-run checklist:
verify local cache files against the fetch catalog, then print the exact
``aws s3 sync`` / ``rclone copy`` commands maintainers should run.

Typical workflow (human ops; not CI):

  1. Fetch from WDC origin into the local cache (verify size/md5):
       GF_WDC_SOURCE=origin python3 scripts/datasets/fetch_wdc_hyperlink.py \\
         --artifact example --artifact pld-2014-webgraph --artifact pld-2012

  2. Dry-run sync plan:
       python3 scripts/datasets/sync_wdc_mirror.py --artifact example \\
         --artifact pld-2014-webgraph --artifact pld-2012

  3. Upload (R2 example; requires AWS CLI + R2 API token):
       export AWS_ACCESS_KEY_ID=… AWS_SECRET_ACCESS_KEY=…
       export GF_WDC_MIRROR_S3_URI=s3://graphforge-wdc/wdc-hyperlink
       export GF_WDC_MIRROR_ENDPOINT=https://<ACCOUNT_ID>.r2.cloudflarestorage.com
       python3 scripts/datasets/sync_wdc_mirror.py --execute \\
         --artifact example --artifact pld-2014-webgraph --artifact pld-2012

Checksum policy: never upload a file that fails the catalog size/md5/line-count
checks. Preserve WDC Content-Length and published MD5 values in the catalog;
the mirror object key is the same relative path as the local cache layout.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys

# Allow `python3 scripts/datasets/sync_wdc_mirror.py` without installing a package.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from fetch_wdc_hyperlink import ARTIFACTS, DEFAULT_CACHE, _file_ok


def _verify_local(cache: Path, names: list[str]) -> list[Path]:
    paths: list[Path] = []
    errors = 0
    for name in names:
        for meta in ARTIFACTS[name].files:
            dest = cache / meta.relpath
            if not _file_ok(dest, meta):
                print(f"FAIL verify {dest}", file=sys.stderr)
                errors += 1
                continue
            print(f"OK   {dest}", file=sys.stderr)
            paths.append(dest)
    if errors:
        raise SystemExit(f"{errors} file(s) failed local verification; refusing sync")
    return paths


def _print_commands(cache: Path, s3_uri: str | None, endpoint: str | None) -> None:
    uri = s3_uri or os.environ.get("GF_WDC_MIRROR_S3_URI", "s3://graphforge-wdc/wdc-hyperlink")
    ep = endpoint or os.environ.get(
        "GF_WDC_MIRROR_ENDPOINT",
        "https://<ACCOUNT_ID>.r2.cloudflarestorage.com",
    )
    print()
    print("# Preferred: AWS CLI against Cloudflare R2 (zero egress for runners)")
    print(f"aws s3 sync {cache} {uri} --endpoint-url {ep} --only-show-errors")
    print()
    print("# Alternative: rclone remote `r2` pointing at the same bucket")
    print(f"rclone copy {cache} r2:graphforge-wdc/wdc-hyperlink --checksum -v")
    print()
    print("# After upload, set runner env:")
    print("#   GF_WDC_MIRROR_BASE=https://<public-host>/wdc-hyperlink")
    print("#   GF_WDC_SOURCE=mirror-only   # recommended for controlled scale runs")


def _execute_aws_sync(cache: Path, s3_uri: str, endpoint: str) -> int:
    cmd = [
        "aws",
        "s3",
        "sync",
        str(cache),
        s3_uri,
        "--endpoint-url",
        endpoint,
        "--only-show-errors",
    ]
    print(" ".join(cmd), file=sys.stderr)
    return subprocess.run(cmd, check=False).returncode


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--cache",
        type=Path,
        default=DEFAULT_CACHE,
        help=f"local cache root (default: {DEFAULT_CACHE})",
    )
    parser.add_argument(
        "--artifact",
        action="append",
        choices=sorted(ARTIFACTS),
        help="artifact set already present in cache (repeatable)",
    )
    parser.add_argument(
        "--tier-min",
        action="store_true",
        help="T0-T3 bootstrap set: example + pld-2014-webgraph + pld-2012",
    )
    parser.add_argument(
        "--s3-uri",
        default=os.environ.get("GF_WDC_MIRROR_S3_URI"),
        help="destination s3://bucket/prefix (env: GF_WDC_MIRROR_S3_URI)",
    )
    parser.add_argument(
        "--endpoint",
        default=os.environ.get("GF_WDC_MIRROR_ENDPOINT"),
        help="S3 API endpoint (env: GF_WDC_MIRROR_ENDPOINT); R2 account endpoint",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="run aws s3 sync after verification (requires credentials + --s3-uri + --endpoint)",
    )
    args = parser.parse_args(argv)

    if args.tier_min:
        names = ["example", "pld-2014-webgraph", "pld-2012"]
    elif args.artifact:
        names = args.artifact
    else:
        names = ["example"]

    print(f"cache: {args.cache}", file=sys.stderr)
    print(f"artifacts: {', '.join(names)}", file=sys.stderr)
    _verify_local(args.cache, names)

    if not args.execute:
        _print_commands(args.cache, args.s3_uri, args.endpoint)
        print("dry-run only (pass --execute to upload)", file=sys.stderr)
        return 0

    if not args.s3_uri or not args.endpoint:
        print(
            "error: --execute requires --s3-uri and --endpoint "
            "(or GF_WDC_MIRROR_S3_URI / GF_WDC_MIRROR_ENDPOINT)",
            file=sys.stderr,
        )
        return 2
    if "<ACCOUNT_ID>" in args.endpoint:
        print("error: replace the R2 account id placeholder in --endpoint", file=sys.stderr)
        return 2
    return _execute_aws_sync(args.cache, args.s3_uri, args.endpoint)


if __name__ == "__main__":
    sys.exit(main())
