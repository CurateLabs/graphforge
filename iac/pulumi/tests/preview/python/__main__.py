from __future__ import annotations

import json
import os
from pathlib import Path

from graphforge_pulumi import TargetValidation, validate_target
import pulumi

root_text = os.environ.get("GRAPHFORGE_REPO_ROOT")
if root_text is None:
    raise RuntimeError("GRAPHFORGE_REPO_ROOT is required")

root = Path(root_text)

resolved_config = json.loads(
    (root / "docs" / "contracts" / "examples" / "graphforge-resolved-v1.json").read_text()
)
golden_receipt = json.loads(
    (
        root
        / "docs"
        / "contracts"
        / "examples"
        / "graphforge-infra-validation-production-v1.json"
    ).read_text()
)
if os.environ.get("GRAPHFORGE_INJECT_FORBIDDEN") == "1":
    production = next(
        target for target in resolved_config["targets"] if target["id"] == "production"
    )
    production["credential"] = os.environ["GRAPHFORGE_TEST_SECRET"]

assert validate_target(resolved_config, "production") == golden_receipt
pulumi.log.info(f"GraphForge golden receipt {golden_receipt['resolved_config_sha256']} verified")
validation = TargetValidation(
    "production",
    resolved_config=resolved_config,
    target_id="production",
)
pulumi.export("receipt", validation.receipt)
