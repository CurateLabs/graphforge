"""Retain completed clean-ladder evidence before the run is reported (#1530).

Every number a decision rests on must survive loss of the ladder work root.
This module copies a completed clean ladder's rung JSON, result
JSON, receipts, and the controller summary into a retained directory — either
`docs/development/evidence/ladder/<commit>/` in a PR or an out-of-tree archive
whose path and digest are recorded on the citing issues — and writes a
`MANIFEST.sha256` whose digest identifies the archive. Raw BenchExec output is
excluded unless requested; each `s<scale>-result.json` records its digest.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
from typing import Any

RUNG_NAME = re.compile(r"^s(\d+)-rung\.json$")
COMMIT = re.compile(r"^[0-9a-f]{40}$")
SUMMARY_HEADER = re.compile(r"^=== clean ladder ([0-9a-f]{40})\b")
SUMMARY_NAME = "controller-summary.log"
MANIFEST_NAME = "MANIFEST.sha256"
RETENTION_SCHEMA = "graphforge-ladder-evidence-retention/1"
# Per-scale retained receipts, ordered for stable manifests. Raw BenchExec
# output is deliberately absent; `--include-benchexec` opts in.
PER_SCALE_RETAINED = ("plan.json", "graphforge.json", "projection.json", "result.json", "rung.json")
PER_SCALE_RAW = ("benchexec.json",)
# Result artifacts whose recorded digest must match the copied receipt.
VERIFIED_ARTIFACTS = {
    "plan_sha256": "plan.json",
    "graphforge_sha256": "graphforge.json",
    "rung_sha256": "rung.json",
    "benchexec_sha256": "benchexec.json",
}


class LadderEvidenceRetentionError(ValueError):
    """The ladder evidence is missing, malformed, or fails digest verification."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _read_json(path: Path, message: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise LadderEvidenceRetentionError(message) from error


def _rung_scales(evidence_dir: Path) -> list[int]:
    scales: list[int] = []
    for path in evidence_dir.glob("s*-rung.json"):
        match = RUNG_NAME.fullmatch(path.name)
        if match is not None:
            scales.append(int(match.group(1)))
    return sorted(set(scales))


def _commit_from_plan(evidence_dir: Path, scales: list[int]) -> str | None:
    commit: str | None = None
    for scale in scales:
        plan_path = evidence_dir / f"s{scale}-plan.json"
        if not plan_path.is_file():
            continue
        plan = _read_json(plan_path, f"{plan_path.name} is malformed")
        identities = plan.get("identities") if isinstance(plan, dict) else None
        found = identities.get("commit") if isinstance(identities, dict) else None
        if not isinstance(found, str) or COMMIT.fullmatch(found) is None:
            raise LadderEvidenceRetentionError(
                f"{plan_path.name} identities.commit must be a lowercase Git object ID"
            )
        if commit is not None and found != commit:
            raise LadderEvidenceRetentionError(
                f"rungs disagree on the ladder commit: {commit} vs {found} in {plan_path.name}"
            )
        commit = found
    return commit


def _commit_from_summary(summary_log: Path) -> str | None:
    try:
        with summary_log.open(encoding="utf-8", errors="replace") as handle:
            for line in handle:
                match = SUMMARY_HEADER.match(line)
                if match is not None:
                    return match.group(1)
    except OSError as error:
        raise LadderEvidenceRetentionError(
            f"controller summary {summary_log} is unreadable"
        ) from error
    return None


def _resolve_commit(
    evidence_dir: Path | None,
    scales: list[int],
    summary_log: Path | None,
    sha: str | None,
) -> str:
    candidates: list[tuple[str, str | None]] = [
        ("--sha", sha),
        (
            "plan receipts",
            None if evidence_dir is None else _commit_from_plan(evidence_dir, scales),
        ),
        (
            "controller summary header",
            None if summary_log is None else _commit_from_summary(summary_log),
        ),
    ]
    resolved: str | None = None
    for source, value in candidates:
        if value is None:
            continue
        if not isinstance(value, str) or COMMIT.fullmatch(value) is None:
            raise LadderEvidenceRetentionError(
                f"ladder commit from {source} must be a lowercase 40-hex Git object ID"
            )
        if resolved is not None and value != resolved:
            raise LadderEvidenceRetentionError(
                f"ladder commit from {source} disagrees with {resolved}"
            )
        resolved = value
    if resolved is not None:
        return resolved
    raise LadderEvidenceRetentionError(
        "cannot resolve the ladder commit; pass --sha with the full 40-hex commit"
    )


def _verify_receipts(evidence_dir: Path, scales: list[int], include_benchexec: bool) -> None:
    for scale in scales:
        result_path = evidence_dir / f"s{scale}-result.json"
        if not result_path.is_file():
            raise LadderEvidenceRetentionError(f"{result_path.name} is required for each rung")
        result = _read_json(result_path, f"{result_path.name} is malformed")
        artifacts = result.get("artifacts") if isinstance(result, dict) else None
        if not isinstance(artifacts, dict):
            raise LadderEvidenceRetentionError(f"{result_path.name} carries no artifacts to verify")
        for key, suffix in VERIFIED_ARTIFACTS.items():
            recorded = artifacts.get(key)
            path = evidence_dir / f"s{scale}-{suffix}"
            if suffix == "benchexec.json" and not include_benchexec:
                continue
            if not isinstance(recorded, str):
                raise LadderEvidenceRetentionError(f"{result_path.name} artifacts.{key} is missing")
            if not path.is_file():
                raise LadderEvidenceRetentionError(
                    f"{path.name} is required by {result_path.name} artifacts.{key}"
                )
            if _sha256(path) != recorded:
                raise LadderEvidenceRetentionError(
                    f"{path.name} does not match {result_path.name} artifacts.{key}"
                )
        plan_path = evidence_dir / f"s{scale}-plan.json"
        plan = _read_json(plan_path, f"{plan_path.name} is malformed")
        identities = plan.get("identities") if isinstance(plan, dict) else None
        projection = evidence_dir / f"s{scale}-projection.json"
        projection_digest = (
            identities.get("admitted_projection_sha256") if isinstance(identities, dict) else None
        )
        if projection_digest is not None or projection.exists():
            if not isinstance(projection_digest, str) or not projection.is_file():
                raise LadderEvidenceRetentionError(
                    f"{projection.name} requires the plan's admitted projection digest and payload"
                )
            if _sha256(projection) != projection_digest:
                raise LadderEvidenceRetentionError(
                    f"{projection.name} does not match the plan's admitted projection digest"
                )


def _plan_retained_files(
    evidence_dir: Path, scales: list[int], include_benchexec: bool
) -> list[str]:
    names: list[str] = []
    for scale in scales:
        for suffix in (*PER_SCALE_RETAINED, *(PER_SCALE_RAW if include_benchexec else ())):
            path = evidence_dir / f"s{scale}-{suffix}"
            if path.is_file():
                names.append(path.name)
    return sorted(names)


def retain_ladder_evidence(
    destination: Path,
    evidence_dir: Path | None = None,
    summary_log: Path | None = None,
    include_benchexec: bool = False,
    sha: str | None = None,
) -> dict[str, Any]:
    """Copy one completed clean ladder's evidence into a retained directory."""
    if evidence_dir is None and summary_log is None:
        raise LadderEvidenceRetentionError(
            "nothing to retain: pass --evidence-dir, --summary-log, or both"
        )
    if evidence_dir is not None and not evidence_dir.is_dir():
        raise LadderEvidenceRetentionError(f"evidence directory {evidence_dir} is missing")
    if summary_log is not None and not summary_log.is_file():
        raise LadderEvidenceRetentionError(f"controller summary {summary_log} is missing")

    scales = [] if evidence_dir is None else _rung_scales(evidence_dir)
    if evidence_dir is not None and not scales:
        raise LadderEvidenceRetentionError(
            f"{evidence_dir} holds no s<scale>-rung.json rung evidence"
        )
    if evidence_dir is not None:
        _verify_receipts(evidence_dir, scales, include_benchexec)
    commit = _resolve_commit(evidence_dir, scales, summary_log, sha)

    retained: list[str] = (
        []
        if evidence_dir is None
        else _plan_retained_files(evidence_dir, scales, include_benchexec)
    )
    if summary_log is not None:
        retained.append(SUMMARY_NAME)
    retained = sorted(retained)
    if not retained:
        raise LadderEvidenceRetentionError("nothing to retain")

    destination.mkdir(parents=True, exist_ok=True)
    conflicts = [name for name in (*retained, MANIFEST_NAME) if (destination / name).exists()]
    if conflicts:
        raise LadderEvidenceRetentionError(
            "retention is append-only; destination already retains: " + ", ".join(conflicts)
        )

    for name in retained:
        if name == SUMMARY_NAME:
            assert summary_log is not None
            source: Path = summary_log
        else:
            assert evidence_dir is not None
            source = evidence_dir / name
        shutil.copy2(source, destination / name)

    lines = [f"{_sha256(destination / name)}  {name}\n" for name in retained]
    manifest = "".join(lines).encode("utf-8")
    (destination / MANIFEST_NAME).write_bytes(manifest)
    return {
        "schema": RETENTION_SCHEMA,
        "commit": commit,
        "source_evidence_dir": None if evidence_dir is None else str(evidence_dir),
        "controller_summary": None if summary_log is None else summary_log.name,
        "destination": str(destination),
        "retained_files": retained,
        "file_count": len(retained),
        "retained_bytes": sum((destination / name).stat().st_size for name in retained),
        "archive_digest": hashlib.sha256(manifest).hexdigest(),
    }


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    result.add_argument(
        "--evidence-dir",
        type=Path,
        help="completed clean ladder evidence directory (clean-<sha>-evidence)",
    )
    result.add_argument("--summary-log", type=Path, help="controller summary log for the run")
    result.add_argument("--destination", type=Path, required=True)
    result.add_argument(
        "--include-benchexec",
        action="store_true",
        help="also retain the raw per-rung BenchExec JSON output",
    )
    result.add_argument(
        "--sha",
        help="full 40-hex ladder commit when the plan receipts and summary header cannot supply it",
    )
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        report = retain_ladder_evidence(
            destination=args.destination,
            evidence_dir=args.evidence_dir,
            summary_log=args.summary_log,
            include_benchexec=args.include_benchexec,
            sha=args.sha,
        )
    except LadderEvidenceRetentionError as error:
        print(f"ladder evidence retention refused: {error}")
        return 2
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
