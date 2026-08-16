"""Fail-closed structural helpers for checked-in GitHub workflow policy tests."""

from __future__ import annotations

import re


def _unquote_scalar(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def workflow_jobs(text: str) -> dict[str, str]:
    """Split a workflow into exact top-level job ID to job-body mappings."""
    lines = text.splitlines()
    try:
        jobs_index = next(index for index, line in enumerate(lines) if line == "jobs:")
    except StopIteration as exc:
        raise AssertionError("workflow is missing a top-level jobs: mapping") from exc
    jobs: dict[str, str] = {}
    current: str | None = None
    body: list[str] = []
    for line in lines[jobs_index + 1 :]:
        match = re.fullmatch(r"  ([A-Za-z0-9_-]+):", line)
        if match:
            if current is not None:
                jobs[current] = "\n".join(body)
            current = match.group(1)
            body = []
            continue
        if current is None:
            continue
        if not line.strip() or line.lstrip().startswith("#"):
            body.append(line)
            continue
        if line and not line.startswith("  "):
            break
        body.append(line)
    if current is not None:
        jobs[current] = "\n".join(body)
    assert jobs, "workflow jobs: mapping is empty"
    return jobs


def job_scalar(job_body: str, field: str) -> str | None:
    """Return one active job-level scalar declared at exactly indent four."""
    prefix = f"    {field}:"
    matches = [
        line[len(prefix) :].strip() for line in job_body.splitlines() if line.startswith(prefix)
    ]
    assert len(matches) <= 1, f"job has duplicate job-level {field}: fields"
    if not matches:
        return None
    value = matches[0]
    assert value and value not in {"|", "|-", ">", ">-"}, (
        f"job-level {field}: must be one explicit scalar"
    )
    return _unquote_scalar(value)


def job_needs(job_body: str) -> set[str]:
    """Return only active job-level needs entries at indents four and six."""
    lines = job_body.splitlines()
    indices = [index for index, line in enumerate(lines) if line.startswith("    needs:")]
    assert len(indices) <= 1, "job has duplicate job-level needs: fields"
    if not indices:
        return set()
    index = indices[0]
    value = lines[index][len("    needs:") :].strip()
    if value.startswith("[") and value.endswith("]"):
        return {_unquote_scalar(item.strip()) for item in value[1:-1].split(",") if item.strip()}
    if value:
        return {_unquote_scalar(value)}
    needed: set[str] = set()
    for follow in lines[index + 1 :]:
        if not follow.strip():
            continue
        if follow.startswith("      - "):
            needed.add(_unquote_scalar(follow[len("      - ") :].strip()))
            continue
        if not follow.startswith("      "):
            break
        raise AssertionError("job-level needs: contains a nested or malformed entry")
    return needed


def job_run_scalars(job_body: str) -> list[str]:
    """Return active step run scalars, excluding comments and nested mappings."""
    lines = job_body.splitlines()
    steps = [index for index, line in enumerate(lines) if line == "    steps:"]
    assert len(steps) <= 1, "job has duplicate job-level steps: fields"
    if not steps:
        return []
    scalars: list[str] = []
    index = steps[0] + 1
    while index < len(lines):
        line = lines[index]
        if line.strip() and line.startswith("    ") and not line.startswith("      "):
            break
        match = re.fullmatch(r"(?:      - |        )run:\s*(.*)", line)
        if not match:
            index += 1
            continue
        value = match.group(1).strip()
        if value in {"|", "|-", ">", ">-"}:
            block: list[str] = []
            index += 1
            while index < len(lines):
                follow = lines[index]
                if follow.strip() and not follow.startswith("          "):
                    break
                stripped = follow.strip()
                if stripped and not stripped.startswith("#"):
                    block.append(stripped)
                index += 1
            scalars.append("\n".join(block))
            continue
        if value and not value.startswith("#"):
            scalars.append(value)
        index += 1
    return scalars


def normalize_run(value: str) -> str:
    """Normalize folded/literal shell layout without accepting other YAML fields."""
    return " ".join(value.split())


def run_scalar_fails_closed(value: str) -> bool:
    """Reject shell forms that can turn a required command failure into success."""
    if "||" in value or re.search(r"(?:^|[;\n])\s*!\s*(?!=)", value):
        return False
    if "&&" in value:
        return False
    if re.search(r"(?<![&>])&(?![&>])", value):
        return False
    if re.search(r"(?:^|[;\n])\s*(?:true|:)(?:\s*(?:$|[;#]))", value):
        return False
    if re.search(r"(?:^|[;\n])\s*(?:if|while|until)\b", value):
        return False
    if re.search(r"(?:^|[;&|\n])\s*exit\s+0(?:\s*(?:$|[;#]))", value):
        return False
    return not re.search(r"(?:^|[;&|\n])\s*set\s+\+", value)


def _has_command_prefix(value: str, expected: str) -> bool:
    return normalize_run(value).startswith(expected)


def job_required_run_scalars(job_body: str, command_prefix: str) -> list[str]:
    """Return active fail-closed run scalars that execute a required command."""
    expected = normalize_run(command_prefix)
    claims = [scalar for scalar in job_run_scalars(job_body) if expected in normalize_run(scalar)]
    for scalar in claims:
        if _has_command_prefix(scalar, expected):
            assert run_scalar_fails_closed(scalar), (
                f"required command may suppress failure: {command_prefix}"
            )
    return [scalar for scalar in claims if _has_command_prefix(scalar, expected)]


def job_runs_exact(job_body: str, command: str) -> bool:
    expected = normalize_run(command)
    matches = job_required_run_scalars(job_body, command)
    return any(normalize_run(scalar) == expected for scalar in matches)


def job_run_contains(job_body: str, command_prefix: str) -> bool:
    """Match an active command prefix, never a comment, env value, or echo."""
    return bool(job_required_run_scalars(job_body, command_prefix))
