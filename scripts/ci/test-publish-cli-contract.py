#!/usr/bin/env python3
"""Static contract for ordered, clean-consumer @graphforge/cli publication."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "publish.yaml"
VERIFY = ROOT / "scripts" / "ci" / "verify-node-cli-release-package.mjs"


def main() -> None:
    text = WORKFLOW.read_text(encoding="utf-8")
    _, found, tail = text.partition("  publish-npm:\n")
    assert found
    npm_job, found, _ = tail.partition("\n  publish-crates:\n")
    assert found
    assert "needs: [candidate-preflight, publish-pypi]" in npm_job
    assert "pnpm --filter @graphforge/cli test:offline" in npm_job
    assert "node scripts/ci/verify-node-cli-release-package.mjs" in npm_job
    assert npm_job.count("scripts/publish_npm_artifacts.py") == 3
    assert npm_job.index("--group native") < npm_job.index("verify-node-cli-release-package.mjs")
    assert npm_job.index("verify-node-cli-release-package.mjs") < npm_job.index("--group cli")
    assert npm_job.index("--group cli") < npm_job.index("--group skills")
    assert "continue-on-error" not in npm_job
    assert "|| true" not in npm_job
    assert VERIFY.is_file()
    print("publish CLI contract tests passed")


if __name__ == "__main__":
    main()
