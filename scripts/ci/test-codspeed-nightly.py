#!/usr/bin/env python3
"""Regression tests for the nightly-only CodSpeed workflow."""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github/workflows/codspeed.yml"

workflow = yaml.load(WORKFLOW.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
triggers = workflow["on"]
assert "pull_request" not in triggers
assert "push" not in triggers
assert "workflow_dispatch" in triggers
assert triggers["schedule"] == [{"cron": "17 7 * * *"}]

jobs = workflow["jobs"]
assert workflow["permissions"]["actions"] == "read"
nightly = jobs["nightly"]
assert nightly["outputs"]["sha"] == "${{ steps.main.outputs.sha }}"
assert nightly["outputs"]["should-run"] == "${{ steps.decision.outputs.should-run }}"
checkout = nightly["steps"][0]
assert checkout["with"]["ref"] == "main"
previous = next(step for step in nightly["steps"] if step.get("id") == "previous")["run"]
assert "actions/workflows/codspeed.yml/runs" in previous
assert "event=schedule" in previous
assert "status=success" in previous
assert 'previous=""' in previous
decision = next(step for step in nightly["steps"] if step.get("id") == "decision")["run"]
assert 'github.event_name }}" == "schedule"' in decision
assert "steps.previous.outputs.sha" in decision
assert "steps.main.outputs.sha" in decision
assert "should_run=true" in decision
assert "should_run=false" in decision

for job_name in ("benchmarks", "m6-walltime"):
    job = jobs[job_name]
    assert job["needs"] == "nightly"
    assert job["if"] == "needs.nightly.outputs.should-run == 'true'"
    checkout = next(step for step in job["steps"] if "actions/checkout@" in step.get("uses", ""))
    assert checkout["with"]["ref"] == "${{ needs.nightly.outputs.sha }}"

memory = jobs["m6-memory-fallback"]
assert memory["if"] == (
    "github.event_name == 'workflow_dispatch' && "
    "needs.nightly.outputs.should-run == 'true'"
)

print("CodSpeed nightly-only policy verified")
