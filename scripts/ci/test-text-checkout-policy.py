#!/usr/bin/env python3
"""Require deterministic LF checkout bytes for every tracked text file."""

from __future__ import annotations

from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[2]
ATTRIBUTES = ROOT / ".gitattributes"
POLICY = "* text=auto eol=lf"


def tracked_paths() -> list[str]:
    result = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=ROOT,
        check=True,
        capture_output=True,
    )
    return [value.decode() for value in result.stdout.rstrip(b"\0").split(b"\0")]


def resolved_attributes(paths: list[str]) -> dict[str, dict[str, str]]:
    result = subprocess.run(
        ["git", "check-attr", "-z", "--stdin", "text", "eol"],
        cwd=ROOT,
        input=b"\0".join(path.encode() for path in paths) + b"\0",
        check=True,
        capture_output=True,
    )
    fields = result.stdout.rstrip(b"\0").split(b"\0")
    assert len(fields) % 3 == 0, "git check-attr returned a malformed response"
    resolved: dict[str, dict[str, str]] = {}
    for index in range(0, len(fields), 3):
        path, attribute, value = (field.decode() for field in fields[index : index + 3])
        resolved.setdefault(path, {})[attribute] = value
    return resolved


def main() -> None:
    active = [
        line.strip()
        for line in ATTRIBUTES.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    assert active and active[0] == POLICY, "global LF text policy is missing or shadowed"

    paths = tracked_paths()
    assert paths, "repository has no tracked files"
    resolved = resolved_attributes(paths)
    assert set(resolved) == set(paths), "Git attributes did not resolve every tracked path"
    invalid = {
        path: values
        for path, values in resolved.items()
        if values.get("text") not in {"auto", "set", "unset"} or values.get("eol") != "lf"
    }
    assert not invalid, f"tracked paths violate LF checkout policy: {invalid}"
    print(f"text checkout policy passed: {len(paths)} tracked paths resolve to LF")


if __name__ == "__main__":
    main()
