#!/usr/bin/env python3
"""Static contract for npm native fan-in, CLI, and skills publication."""

from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "publish.yaml"


def main() -> None:
    text = WORKFLOW.read_text(encoding="utf-8")
    native = text.split("  npm-native:\n", 1)[1].split("\n  npm-main:", 1)[0]
    main_job = text.split("  npm-main:\n", 1)[1].split("\n  npm-cli:", 1)[0]
    cli = text.split("  npm-cli:\n", 1)[1].split("\n  npm-skills:", 1)[0]
    skills = text.split("  npm-skills:\n", 1)[1].split("\n  publish-crates:", 1)[0]
    native_packages = re.findall(r"^          - (graphforge-[a-z0-9-]+)$", native, re.MULTILINE)
    assert native_packages == [
        "graphforge-darwin-arm64",
        "graphforge-darwin-x64",
        "graphforge-linux-arm64-gnu",
        "graphforge-linux-x64-gnu",
        "graphforge-win32-x64-msvc",
    ]
    assert "fail-fast: false" in native
    assert "needs: [candidate-preflight, npm-native]" in main_job
    assert "needs: [candidate-preflight, npm-main]" in cli
    assert "needs: [candidate-preflight, npm-cli]" in skills
    assert "npm:@curatelabs/graphforge" in main_job
    assert "npm:@curatelabs/graphforge-cli" in cli
    assert "npm:@curatelabs/graphforge-agent-skills" in skills
    for job in (native, main_job, cli, skills):
        assert "--registry npm" in job
        assert job.count("release_action.py authorize") == 1
        assert job.count("scripts/publish_npm_artifacts.py") == 1
        assert "secrets.NPM_TOKEN" in job
        assert not re.search(r"(?m)(?:^|\s|[;&|])sleep(?:\s|$)", job)
    print("publish CLI contract tests passed")


if __name__ == "__main__":
    main()
