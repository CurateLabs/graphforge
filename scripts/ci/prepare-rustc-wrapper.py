#!/usr/bin/env python3
"""Make an inherited Rust compiler wrapper safe for later CI subprocesses."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import re
import shutil

SAFE_DIAGNOSTIC = re.compile(r"[^A-Za-z0-9._-]")
MAX_DIAGNOSTIC_LENGTH = 64


def diagnostic(value: str) -> str:
    """Return one bounded, non-secret-bearing diagnostic token."""
    sanitized = SAFE_DIAGNOSTIC.sub("_", value)[:MAX_DIAGNOSTIC_LENGTH]
    return sanitized or "unknown"


def prepare(wrapper: str | None, github_env: Path, platform: str, contract: str) -> bool:
    """Persist an empty wrapper when the inherited command is unavailable."""
    safe_platform = diagnostic(platform)
    safe_contract = diagnostic(contract)
    if not wrapper:
        print(f"rustc-wrapper contract={safe_contract} platform={safe_platform} state=unset")
        return False

    if shutil.which(wrapper) is not None:
        print(
            f"rustc-wrapper contract={safe_contract} platform={safe_platform} "
            f"command={diagnostic(Path(wrapper).name)} state=available"
        )
        return True

    with github_env.open("a", encoding="utf-8", newline="\n") as destination:
        destination.write("RUSTC_WRAPPER=\n")
    print(
        f"rustc-wrapper contract={safe_contract} platform={safe_platform} "
        f"command={diagnostic(Path(wrapper).name)} state=cleared"
    )
    return False


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--platform", required=True)
    parser.add_argument("--contract", required=True)
    args = parser.parse_args()

    github_env = os.environ.get("GITHUB_ENV")
    if not github_env:
        raise SystemExit("rustc-wrapper contract failed: GITHUB_ENV is unavailable")
    prepare(
        os.environ.get("RUSTC_WRAPPER"),
        Path(github_env),
        args.platform,
        args.contract,
    )


if __name__ == "__main__":
    main()
