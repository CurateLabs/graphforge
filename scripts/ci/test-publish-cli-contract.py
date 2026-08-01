#!/usr/bin/env python3
"""Static contract for npm native fan-in, CLI, and skills publication."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "publish.yaml"


def main() -> None:
    text = WORKFLOW.read_text(encoding="utf-8")
    native = text.split("  npm-native:\n", 1)[1].split("\n  npm-main:", 1)[0]
    main_job = text.split("  npm-main:\n", 1)[1].split("\n  npm-cli:", 1)[0]
    cli = text.split("  npm-cli:\n", 1)[1].split("\n  npm-skills:", 1)[0]
    skills = text.split("  npm-skills:\n", 1)[1].split("\n  publish-crates:", 1)[0]
    assert native.count("- graphforge-") == 5
    assert "fail-fast: false" in native
    assert "needs: [candidate-preflight, npm-native]" in main_job
    assert "needs: [candidate-preflight, npm-main]" in cli
    assert "needs: [candidate-preflight, npm-cli]" in skills
    assert "npm:@curatelabs/graphforge" in main_job
    assert "npm:@curatelabs/graphforge-cli" in cli
    assert "npm:@curatelabs/graphforge-agent-skills" in skills
    for job in (native, main_job, cli, skills):
        assert "--registry npm" in job
        assert "release_action.py authorize" in job
        assert "scripts/publish_npm_artifacts.py" in job
        assert "secrets.NPM_TOKEN" in job
        assert "time.sleep" not in job
    print("publish CLI contract tests passed")


if __name__ == "__main__":
    main()
