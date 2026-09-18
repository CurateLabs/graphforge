#!/usr/bin/env python3
"""Enforce reviewed physical-line limits for tracked crate source files."""

from __future__ import annotations

import argparse
import json
from pathlib import Path, PurePosixPath
import subprocess
import sys
from typing import NamedTuple

POLICY = "config/source-size-policy.json"


class PolicyError(ValueError):
    """Invalid policy or unavailable source authority."""


class Measurement(NamedTuple):
    path: str
    lines: int
    bound: int
    adr: str | None

    def diagnostic(self) -> str:
        return f"{self.path}: measured={self.lines} bound={self.bound} ADR={self.adr or 'none'}"


def source_path(path: str) -> bool:
    """Git pathspec wildcards can span components; check scope explicitly."""
    parts = path.split("/")
    return len(parts) >= 4 and parts[0] == "crates" and parts[2] == "src"


def tracked_paths(root: Path) -> set[str]:
    try:
        result = subprocess.run(
            ["git", "-C", str(root), "ls-files", "-z", "--cached"],
            check=True,
            capture_output=True,
        )
    except (OSError, subprocess.CalledProcessError) as error:
        raise PolicyError(f"cannot enumerate tracked files: {error}") from error
    return {
        path.decode("utf-8", errors="surrogateescape")
        for path in result.stdout.split(b"\0")
        if path
    }


def physical_lines(data: bytes) -> int:
    return data.count(b"\n") + int(bool(data) and not data.endswith(b"\n"))


def _unique_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise PolicyError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _path(value: object, label: str) -> str:
    if not isinstance(value, str) or not value or "\\" in value:
        raise PolicyError(f"{label}: expected an exact repository-relative path")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in value.split("/")):
        raise PolicyError(f"{label}: invalid exact path {value!r}")
    if any(character in value for character in "*?[]#\0\r\n"):
        raise PolicyError(f"{label}: invalid exact path {value!r}")
    return value


def _bound(value: object, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise PolicyError(f"{label}: expected a positive integer line bound")
    return value


def load_policy(path: Path) -> tuple[int, dict[str, dict[str, object]]]:
    try:
        policy = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=_unique_pairs)
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise PolicyError(f"{path}: cannot read policy: {error}") from error
    if not isinstance(policy, dict) or set(policy) != {"default_max_lines", "exemptions"}:
        raise PolicyError("policy must contain exactly default_max_lines and exemptions")
    default = _bound(policy["default_max_lines"], "default_max_lines")
    if not isinstance(policy["exemptions"], list):
        raise PolicyError("exemptions: expected an array")
    exemptions = {}
    for entry in policy["exemptions"]:
        if not isinstance(entry, dict) or set(entry) != {"path", "max_lines", "adr", "rationale"}:
            raise PolicyError("exemption must contain exactly path, max_lines, adr, rationale")
        source = _path(entry["path"], "exemption path")
        if not source_path(source):
            raise PolicyError(f"{source}: exemption is outside crates/<crate>/src")
        if source in exemptions:
            raise PolicyError(f"{source}: duplicate exemption")
        maximum = _bound(entry["max_lines"], f"{source} max_lines")
        if maximum <= default:
            raise PolicyError(f"{source}: exemption bound must exceed default {default}")
        adr = _path(entry["adr"], f"{source} ADR")
        parts = adr.split("/")
        if len(parts) != 3 or parts[:2] != ["docs", "adr"] or not parts[2].endswith(".md"):
            raise PolicyError(f"{source}: ADR must reference an exact docs/adr/*.md file")
        if not isinstance(entry["rationale"], str) or not entry["rationale"].strip():
            raise PolicyError(f"{source}: exemption rationale must be nonempty")
        exemptions[source] = entry
    return default, exemptions


def _read(root: Path, path: str) -> bytes:
    target = root / path
    if target.is_symlink() or not target.is_file():
        raise PolicyError(f"{path}: missing or non-regular tracked file")
    if not target.resolve().is_relative_to(root.resolve()):
        raise PolicyError(f"{path}: file escapes repository")
    try:
        return target.read_bytes()
    except OSError as error:
        raise PolicyError(f"{path}: cannot read tracked file: {error}") from error


def _adr_statuses(text: str) -> list[str]:
    """Every status an ADR declares, read from its YAML frontmatter.

    ADRs carry ``status:`` in frontmatter (ADR 0038, #1390). The prose
    ``**Status:**`` line they used to carry was removed when the frontmatter
    landed, and this gate reads the same field the ADR index does so the two
    cannot disagree.
    """
    lines = text.splitlines()
    if not lines or lines[0].strip() != "---":
        return []
    try:
        close = lines.index("---", 1)
    except ValueError:
        return []
    found = []
    for raw in lines[1:close]:
        key, sep, value = raw.partition(":")
        if sep and key.strip() == "status":
            found.append(value.strip().strip('"'))
    return found


def check(root: Path, policy_path: Path) -> tuple[list[Measurement], list[str]]:
    default, exemptions = load_policy(policy_path)
    tracked = tracked_paths(root)
    errors = []
    for path, entry in sorted(exemptions.items()):
        if path not in tracked:
            errors.append(f"{path}: exemption source is not tracked")
        adr = str(entry["adr"])
        if adr not in tracked:
            errors.append(f"{path}: ADR={adr} is not tracked")
            continue
        try:
            text = _read(root, adr).decode("utf-8")
            statuses = _adr_statuses(text)
            if statuses != ["Accepted"]:
                errors.append(f"{path}: ADR={adr} must have one accepted status; got {statuses}")
        except (PolicyError, UnicodeError) as error:
            errors.append(f"{path}: ADR={adr}: {error}")
    measurements = []
    for path in sorted(filter(source_path, tracked)):
        entry = exemptions.get(path)
        bound = int(entry["max_lines"]) if entry else default
        adr = str(entry["adr"]) if entry else None
        try:
            lines = physical_lines(_read(root, path))
        except PolicyError as error:
            errors.append(
                f"{path}: measured=unavailable bound={bound} ADR={adr or 'none'}: {error}"
            )
            continue
        measurement = Measurement(path, lines, bound, adr)
        measurements.append(measurement)
        if lines > bound:
            errors.append(f"{measurement.diagnostic()}: exceeds line bound")
        elif entry and lines <= default:
            errors.append(f"{measurement.diagnostic()}: obsolete exemption; fits default {default}")
    return measurements, errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--policy", type=Path, default=Path(POLICY))
    parser.add_argument("--inventory", action="store_true", help="report every tracked source")
    args = parser.parse_args()
    try:
        measurements, errors = check(args.root, args.root / args.policy)
    except PolicyError as error:
        print(f"source-size policy: {error}", file=sys.stderr)
        return 1
    for measurement in measurements:
        if args.inventory or measurement.adr:
            print(measurement.diagnostic())
    for error in errors:
        print(error, file=sys.stderr)
    print(f"source-size policy: {len(measurements)} files checked; {len(errors)} violations")
    return int(bool(errors))


if __name__ == "__main__":
    raise SystemExit(main())
