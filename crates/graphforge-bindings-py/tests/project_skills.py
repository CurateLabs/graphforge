"""Fresh-wheel lifecycle acceptance for packaged project-local skill assets."""

from __future__ import annotations

import hashlib
from importlib.resources import files
import json
from pathlib import Path
import subprocess
import sys
import tempfile

bundle = files("graphforge").joinpath("_project_skills")
manifest = json.loads(bundle.joinpath("manifest.json").read_text(encoding="utf-8"))
assert manifest["schema_version"] == 1
assert manifest["bundle_version"] == 1
assert manifest["graphforge_compatibility"] == ">=0.5.0 <0.6.0"
assert manifest["skills"] == [
    "graphforge-bootstrap",
    "graphforge-build-knowledge",
]
for entry in manifest["files"]:
    payload = bundle.joinpath(*entry["path"].split("/")).read_bytes()
    assert hashlib.sha256(payload).hexdigest() == entry["sha256"]

executable = Path(sys.executable).with_name(
    "graphforge.exe" if sys.platform == "win32" else "graphforge"
)
assert executable.is_file(), "fresh wheel console entry point is missing"

with tempfile.TemporaryDirectory(prefix="graphforge-wheel-skills-") as temporary:
    repository = Path(temporary) / "repository"
    repository.mkdir()
    subprocess.run(["git", "init", "-q", str(repository)], check=True)

    def run(*arguments: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                str(executable),
                "--project-dir",
                str(repository),
                "--json",
                *arguments,
            ],
            cwd=repository,
            check=False,
            capture_output=True,
            text=True,
        )

    def success(*arguments: str) -> dict[str, object]:
        result = run(*arguments)
        assert result.returncode == 0, result.stderr
        return json.loads(result.stdout)

    initialized = success("init")
    assert initialized["skills"]["changed"] is True
    managed = repository / ".agents" / "skills"
    for skill in manifest["skills"]:
        assert (managed / skill / "SKILL.md").read_bytes() == bundle.joinpath(
            skill, "SKILL.md"
        ).read_bytes()
    assert success("skills", "status")["status"] == "current"
    assert success("skills", "install")["changed"] is False

    # Recover an interrupted publication where the prior complete bundle was
    # moved aside and partial replacement files became visible.
    lifecycle = repository / ".graphforge" / "imports" / "skills-lifecycle"
    backup = lifecycle / "backup"
    backup.mkdir()
    for skill in manifest["skills"]:
        (managed / skill).rename(backup / skill)
        (managed / skill).mkdir()
        (managed / skill / "SKILL.md").write_text("interrupted\n")
    (managed / ".graphforge-managed.json").rename(backup / ".graphforge-managed.json")
    (lifecycle / "transaction").write_text("graphforge-skills/1\n")
    assert success("skills", "status")["status"] == "current"
    assert not (lifecycle / "transaction").exists()

    edited = managed / "graphforge-bootstrap" / "SKILL.md"
    edited.write_bytes(edited.read_bytes() + b"\nuser edit\n")
    before_conflict = edited.read_bytes()
    assert success("skills", "status")["status"] == "conflict"
    update_conflict = run("skills", "update")
    assert update_conflict.returncode != 0
    assert edited.read_bytes() == before_conflict
    assert success("skills", "update", "--force")["changed"] is True

    # Corrupting only the installed wheel copy must make native validation
    # fail. The wrapper therefore cannot be using a separately embedded bundle.
    packaged_manifest = Path(str(bundle.joinpath("manifest.json")))
    packaged_bytes = packaged_manifest.read_bytes()
    try:
        packaged_manifest.write_text("{}\n")
        assert run("skills", "status").returncode != 0
    finally:
        packaged_manifest.write_bytes(packaged_bytes)

    edited.write_bytes(edited.read_bytes() + b"\nsecond user edit\n")
    remove_conflict = run("skills", "remove")
    assert remove_conflict.returncode != 0
    assert edited.exists()
    assert success("skills", "remove", "--force")["changed"] is True
    assert not (managed / "graphforge-bootstrap").exists()
    assert success("skills", "install")["changed"] is True

print("python wheel project skill lifecycle: verified")
