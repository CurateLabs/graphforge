#!/usr/bin/env python3
"""Static contract for ordered, clean-consumer @graphforge/cli publication."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "publish.yaml"
VERIFY = ROOT / "scripts" / "ci" / "verify-node-cli-release-package.mjs"


def section(text: str, start: str, end: str) -> str:
    _, found, tail = text.partition(start)
    assert found, f"missing workflow marker: {start.strip()}"
    body, found, _ = tail.partition(end)
    assert found, f"missing workflow marker: {end.strip()}"
    return body


def main() -> None:
    text = WORKFLOW.read_text(encoding="utf-8")
    native = section(text, "  publish-node:\n", "  # ---- NPX lifecycle CLI")
    cli = section(text, "  publish-node-cli:\n", "  # ---- NPX agent skills")

    assert "needs: [build-node, publish]" in native
    assert "needs: [publish, publish-node]" in cli
    assert "pnpm --filter @graphforge/cli test:offline" in cli
    assert "node scripts/ci/verify-node-cli-release-package.mjs" in cli
    assert cli.index("verify-node-cli-release-package.mjs") < cli.index("Publish @graphforge/cli")
    assert "continue-on-error" not in cli
    assert "|| true" not in cli
    assert VERIFY.is_file()
    print("publish CLI contract tests passed")


if __name__ == "__main__":
    main()
