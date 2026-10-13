#!/usr/bin/env python3
"""Deterministic tests for the crates.io publisher."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

SCRIPT = Path(__file__).parents[1] / "publish_crates.py"


def load_module():
    spec = importlib.util.spec_from_file_location("publish_crates", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


mod = load_module()
assert mod.VERSION
v = mod.VERSION

assert mod.normalize_registry_token("  abc\n") == "abc"
assert mod.normalize_registry_token("abc\r\n") == "abc"
try:
    mod.normalize_registry_token("   \n")
    raise AssertionError("expected empty token after trim to fail")
except ValueError as exc:
    assert "empty after trim" in str(exc)
try:
    mod.normalize_registry_token("abc\x00def")
    raise AssertionError("expected control character to fail")
except ValueError as exc:
    assert "non-printable" in str(exc)
    assert "\x00" not in str(exc)
try:
    mod.normalize_registry_token("abc\x85def")
    raise AssertionError("expected ISO-8859-1 C1 control to fail")
except ValueError as exc:
    assert "non-printable" in str(exc)
    assert "\x85" not in str(exc)

# Trusted Publishing performs a fresh OIDC exchange for every cargo attempt.
original_environ = os.environ.copy()
original_urlopen = mod.urllib.request.urlopen
requests = []


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self, *_args):
        return json.dumps(self.payload).encode("utf-8")


def fake_urlopen(request, timeout):
    requests.append((request, timeout))
    if request.full_url.startswith(os.environ["ACTIONS_ID_TOKEN_REQUEST_URL"]):
        return FakeResponse({"value": "signed-oidc-jwt"})
    if request.get_method() == "POST":
        return FakeResponse({"token": "trusted-token"})
    return FakeResponse({})


os.environ.update(
    {
        mod.TRUSTED_PUBLISHING_ENV: "true",
        "ACTIONS_ID_TOKEN_REQUEST_URL": "https://oidc.example/token",
        "ACTIONS_ID_TOKEN_REQUEST_TOKEN": "request-token",
    }
)
mod.urllib.request.urlopen = fake_urlopen
try:
    assert mod.request_trusted_publishing_token() == "trusted-token"
    assert requests[0][0].full_url == "https://oidc.example/token?audience=crates.io"
    assert requests[0][0].get_header("Authorization") == "Bearer request-token"
    assert json.loads(requests[1][0].data) == {"jwt": "signed-oidc-jwt"}
    mod.revoke_trusted_publishing_token("trusted-token")
    assert requests[2][0].get_method() == "DELETE"
    assert requests[2][0].get_header("Authorization") == "Bearer trusted-token"
finally:
    mod.urllib.request.urlopen = original_urlopen
    os.environ.clear()
    os.environ.update(original_environ)

RATE_BODY = (
    f"error: failed to publish graphforge-plan v{v} to registry at https://crates.io\n"
    "Caused by:\n"
    "  the remote server responded with an error (status 429 Too Many Requests): "
    "You have published too many new crates in a short period of time. "
    "Please try again after Sat, 01 Aug 2026 19:40:15 GMT and see "
    "https://crates.io/docs/rate-limits for more details.\n"
)
fixed_now = datetime(2026, 8, 1, 19, 35, 47, tzinfo=timezone.utc)
wait = mod.parse_rate_limit_retry_wait(RATE_BODY, now=fixed_now)
# 19:40:15 - 19:35:47 = 268s, plus the small post-window buffer.
assert wait == 268 + mod.RATE_LIMIT_BUFFER_SECONDS
assert mod.parse_rate_limit_retry_wait("error: something else\n") is None
assert (
    mod.parse_rate_limit_retry_wait(
        "status 429 Too Many Requests\nRetry-After: 42\n",
        now=fixed_now,
    )
    == 42 + mod.RATE_LIMIT_BUFFER_SECONDS
)
# 429 without a parseable wait hint must not invent a backoff.
assert (
    mod.parse_rate_limit_retry_wait(
        "the remote server responded with an error (status 429 Too Many Requests)\n",
        now=fixed_now,
    )
    is None
)
# Past retry timestamps still apply the buffer rather than sleeping forever or negative.
past = mod.parse_rate_limit_retry_wait(
    "status 429 Too Many Requests: Please try again after Sat, 01 Aug 2026 19:30:00 GMT\n",
    now=fixed_now,
)
assert past == float(mod.RATE_LIMIT_BUFFER_SECONDS)

publish_calls: list[list[str]] = []
sleeps: list[float] = []


def fake_publish(command: list[str]) -> subprocess.CompletedProcess[str]:
    publish_calls.append(command)
    if len(publish_calls) == 1:
        return subprocess.CompletedProcess(command, 101, stdout="", stderr=RATE_BODY)
    return subprocess.CompletedProcess(command, 0, stdout="ok\n", stderr="")


mod.cargo_publish(
    "graphforge-plan",
    sleep=sleeps.append,
    run_publish=fake_publish,
    now=lambda: fixed_now,
)
assert publish_calls == [
    ["cargo", "publish", "-p", "graphforge-plan", "--locked"],
    ["cargo", "publish", "-p", "graphforge-plan", "--locked"],
]
assert sleeps == [268 + mod.RATE_LIMIT_BUFFER_SECONDS]

# Non-429 failures must surface immediately without sleeping.
publish_calls.clear()
sleeps.clear()


def permanent_failure(command: list[str]) -> subprocess.CompletedProcess[str]:
    publish_calls.append(command)
    return subprocess.CompletedProcess(
        command,
        101,
        stdout="",
        stderr="error: failed to publish: checksum mismatch\n",
    )


try:
    mod.cargo_publish(
        "graphforge-plan",
        sleep=sleeps.append,
        run_publish=permanent_failure,
        now=lambda: fixed_now,
    )
    raise AssertionError("expected non-429 publish failure")
except subprocess.CalledProcessError as exc:
    assert exc.returncode == 101
assert publish_calls == [["cargo", "publish", "-p", "graphforge-plan", "--locked"]]
assert sleeps == []

# A revoke outage is visible to the operator but cannot turn an accepted publish
# into a failed, potentially duplicate recovery attempt.
original_request_token = mod.request_trusted_publishing_token
original_revoke_token = mod.revoke_trusted_publishing_token
original_environ = os.environ.copy()
os.environ[mod.TRUSTED_PUBLISHING_ENV] = "true"
mod.request_trusted_publishing_token = lambda: "trusted-token"


def revoke_failure(_token):
    raise mod.urllib.error.URLError("unavailable")


mod.revoke_trusted_publishing_token = revoke_failure
try:
    mod.cargo_publish(
        "graphforge-plan",
        run_publish=lambda command: subprocess.CompletedProcess(
            command, 0, stdout="ok\n", stderr=""
        ),
    )
finally:
    mod.request_trusted_publishing_token = original_request_token
    mod.revoke_trusted_publishing_token = original_revoke_token
    os.environ.clear()
    os.environ.update(original_environ)

# A re-run skips versions the registry already has and publishes the rest in
# plan order.
calls: list[tuple[str, bool | None]] = []
on_registry = {"graphforge-core"}
existing_crates = {"graphforge-core", "graphforge-io"}
mod.version_record = lambda name: {"num": v} if name in on_registry else None
mod.crate_exists = lambda name: name in existing_crates
mod.cargo_publish = lambda name, trusted=None, **_kwargs: calls.append((name, trusted))
os.environ.pop("CARGO_REGISTRY_TOKEN_NEW_CRATES", None)
mod.publish(["graphforge-core", "graphforge-io", "graphforge-value"])
assert calls == [("graphforge-io", None), ("graphforge-value", None)], calls

# Trusted Publishing cannot create a crate: a never-published name uses the
# scoped token once, and the token does not outlive that publish.
calls.clear()
seen_tokens: list[str | None] = []


def record_publish(name, trusted=None, **_kwargs):
    calls.append((name, trusted))
    seen_tokens.append(os.environ.get("CARGO_REGISTRY_TOKEN"))


mod.cargo_publish = record_publish
os.environ[mod.TRUSTED_PUBLISHING_ENV] = "true"
os.environ["CARGO_REGISTRY_TOKEN_NEW_CRATES"] = " scoped-token\n"
os.environ.pop("CARGO_REGISTRY_TOKEN", None)
try:
    mod.publish(["graphforge-core", "graphforge-io", "graphforge-value"])
    assert calls == [("graphforge-io", None), ("graphforge-value", False)], calls
    assert seen_tokens == [None, "scoped-token"], seen_tokens
    assert "CARGO_REGISTRY_TOKEN" not in os.environ
finally:
    os.environ.clear()
    os.environ.update(original_environ)

# The dry run packages every crate in order, in one cargo invocation, and
# never publishes.
commands: list[list[str]] = []
mod.run = commands.append
mod.package(["graphforge-core", "graphforge-io"])
assert commands == [
    [
        "cargo",
        "package",
        "--locked",
        "--allow-dirty",
        "-p",
        "graphforge-core",
        "-p",
        "graphforge-io",
    ]
], commands

# Exercise Cargo's pristine-package build with the real CLI build script and
# its shipped inputs, outside the workspace. Restoring the original escaping
# input path must fail the same verification used before release uploads.
with tempfile.TemporaryDirectory(prefix="graphforge-packaged-build-") as directory:
    root = Path(directory)
    fixture = root / "source"
    fixture.mkdir()
    (fixture / "src").mkdir()
    (fixture / "src" / "lib.rs").write_text("", encoding="utf-8")
    (fixture / "Cargo.toml").write_text(
        '[package]\nname = "graphforge-packaged-build"\nversion = "0.0.0"\n'
        'edition = "2024"\nlicense = "Apache-2.0"\n',
        encoding="utf-8",
    )
    cli = SCRIPT.parents[1] / "crates" / "graphforge-cli"
    build_script = (cli / "build.rs").read_text(encoding="utf-8")
    (fixture / "build.rs").write_text(build_script, encoding="utf-8")
    shutil.copytree(cli / "project-skills", fixture / "project-skills")
    environment = {**os.environ, "CARGO_TARGET_DIR": str(root / "target")}
    subprocess.run(
        ["cargo", "generate-lockfile", "--offline"],
        cwd=fixture,
        env=environment,
        check=True,
    )
    command = ["cargo", "package", "--locked", "--allow-dirty", "--offline"]
    verified = subprocess.run(
        command, cwd=fixture, env=environment, capture_output=True, text=True, check=False
    )
    assert verified.returncode == 0, verified.stderr
    # Inspect build outputs instead of Cargo's terminal-colored progress text.
    # These files are absent if package verification does not run the script.
    modules = list((root / "target").rglob("project_skills.rs"))
    assert len(modules) == 1, modules
    embedded = modules[0].parent / "project_skills_embed"
    manifest = (fixture / "project-skills" / "manifest.json").read_bytes()
    assert (embedded / "manifest.json").read_bytes() == manifest
    for entry in json.loads(manifest)["files"]:
        relative = entry["path"]
        assert (embedded / relative).read_bytes() == (
            fixture / "project-skills" / relative
        ).read_bytes()

    # Known positive: the exact repository-relative input from the broken CLI.
    assert 'manifest_dir.join("project-skills")' in build_script
    (fixture / "build.rs").write_text(
        build_script.replace(
            'manifest_dir.join("project-skills")',
            'manifest_dir.join("../../project-skills")',
        ),
        encoding="utf-8",
    )
    rejected = subprocess.run(
        command, cwd=fixture, env=environment, capture_output=True, text=True, check=False
    )
    assert rejected.returncode != 0, rejected.stderr
    assert "read canonical project-skills directory" in rejected.stderr

print("publish crates tests passed")
