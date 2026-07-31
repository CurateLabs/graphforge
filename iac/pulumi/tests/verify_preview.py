"""Verify that a real Pulumi preview stayed provider-free and secret-free."""

from __future__ import annotations

import json
from pathlib import Path
import sys
from typing import Any

ALLOWED_TYPES = {
    "pulumi:pulumi:Stack",
    "graphforge:deployment:DeploymentSpec",
    "graphforge:static:TargetValidation",
}
GOLDEN_DIGEST = "eb9bd7c49ae277c62892b028d2c93e8328c24dd3e3fd5ecbabb4f2c94637e9e7"


def resource_type(urn: str) -> str:
    return urn.split("::")[-2].rsplit("$", maxsplit=1)[-1]


def main() -> None:
    preview_path = Path(sys.argv[1])
    expected_failure = len(sys.argv) == 3 and sys.argv[2] == "--expected-failure"
    raw = preview_path.read_text()
    sentinel = "_".join(("GRAPHFORGE_SECRET", "SENTINEL"))
    if sentinel in raw:
        raise AssertionError("preview leaked a secret sentinel")
    preview: dict[str, Any] = json.loads(raw)
    steps = preview.get("steps", [])
    if not isinstance(steps, list):
        raise AssertionError("preview steps must be an array")
    for step in steps:
        if not isinstance(step, dict):
            raise AssertionError("preview step must be an object")
        urn = step.get("urn")
        if not isinstance(urn, str):
            raise AssertionError("preview step is missing a URN")
        kind = resource_type(urn)
        if kind not in ALLOWED_TYPES:
            raise AssertionError(f"preview attempted an unexpected resource type: {kind}")
        if kind.startswith("pulumi:providers:") or step.get("provider"):
            raise AssertionError(f"preview attempted a provider operation: {urn}")
    diagnostics = preview.get("diagnostics", [])
    errors = [
        item.get("message", "")
        for item in diagnostics
        if isinstance(item, dict) and item.get("severity") == "error"
    ]
    if expected_failure:
        if not errors or not any("unknown field credential" in message for message in errors):
            raise AssertionError(f"preview did not safely reject the forbidden field: {preview!r}")
        return
    if not steps:
        raise AssertionError(f"preview did not report resource steps: {preview!r}")
    summary = preview.get("changeSummary")
    if summary != {"create": 3}:
        raise AssertionError(f"unexpected preview change summary: {summary!r}")
    if errors:
        raise AssertionError("preview reported an error diagnostic")
    if GOLDEN_DIGEST not in raw or "GraphForge golden receipt" not in raw:
        raise AssertionError("preview did not prove the shared golden receipt")
    if "GraphForge golden deployment spec" not in raw:
        raise AssertionError("preview did not prove the shared deployment specification")


if __name__ == "__main__":
    main()
