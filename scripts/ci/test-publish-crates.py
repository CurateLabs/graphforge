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


def record_package(command):
    commands.append(command)
    target = Path(command[command.index("--target-dir") + 1])
    assert target.is_dir()
    (target / "output").write_text("packaged", encoding="utf-8")


original_root = mod.ROOT
original_environ = os.environ.copy()
with tempfile.TemporaryDirectory(prefix="graphforge-package-target-") as directory:
    mod.ROOT = Path(directory)
    mod.run = record_package
    try:
        for configured_target in (None, "relative-target", str(mod.ROOT / "absolute-target")):
            if configured_target is None:
                os.environ.pop("CARGO_TARGET_DIR", None)
            else:
                os.environ["CARGO_TARGET_DIR"] = configured_target
            before = os.environ.copy()
            mod.package(["graphforge-core", "graphforge-io"])
            assert os.environ == before
            command = commands[-1]
            target = Path(command[5])
            assert command == [
                "cargo",
                "package",
                "--locked",
                "--allow-dirty",
                "--target-dir",
                str(target),
                "-p",
                "graphforge-core",
                "-p",
                "graphforge-io",
            ], command
            assert target.parent == (mod.ROOT / (configured_target or "target")).resolve()
            assert not target.exists()
        assert len(commands) == 3
    finally:
        mod.ROOT = original_root
        os.environ.clear()
        os.environ.update(original_environ)

# Cargo treats a registry's name/version sources as immutable. Reusing its
# package/tmp-registry path must reproduce stale dependency source, while the
# production helper must verify current tarballs at the same versions/home.
with tempfile.TemporaryDirectory(prefix="graphforge-package-sources-") as directory:
    root = Path(directory)
    fixture = root / "source"
    for name in ("producer", "consumer"):
        (fixture / name / "src").mkdir(parents=True)
        manifest = (
            f'[package]\nname = "gf-package-{name}"\nversion = "0.0.0"\n'
            'edition = "2024"\nlicense = "Apache-2.0"\n'
        )
        if name == "consumer":
            manifest += (
                '[dependencies]\ngf-package-producer = {version = "0.0.0", path = "../producer"}\n'
            )
        (fixture / name / "Cargo.toml").write_text(manifest, encoding="utf-8")
    (fixture / "Cargo.toml").write_text(
        '[workspace]\nmembers = ["producer", "consumer"]\nresolver = "3"\n', encoding="utf-8"
    )

    def write_sources(*, new_api):
        producer = "pub fn old_api() -> u8 { 1 }\n"
        if new_api:
            producer += "pub fn new_api() -> u8 { 2 }\n"
        (fixture / "producer/src/lib.rs").write_text(producer, encoding="utf-8")
        api = "new_api" if new_api else "old_api"
        (fixture / "consumer/src/lib.rs").write_text(
            f"pub fn use_api() -> u8 {{ gf_package_producer::{api}() }}\n", encoding="utf-8"
        )

    environment = {
        **os.environ,
        "CARGO_HOME": str(root / "cargo-home"),
        "CARGO_TARGET_DIR": str(root / "target"),
        "CARGO_NET_OFFLINE": "true",
    }
    write_sources(new_api=False)
    subprocess.run(
        ["cargo", "generate-lockfile", "--offline"], cwd=fixture, env=environment, check=True
    )
    order = ["gf-package-producer", "gf-package-consumer"]
    fixed_command = ["cargo", "package", "--locked", "--allow-dirty", "--offline"]
    for name in order:
        fixed_command += ["-p", name]
    initial = subprocess.run(
        fixed_command, cwd=fixture, env=environment, capture_output=True, text=True, check=False
    )
    assert initial.returncode == 0, initial.stderr
    write_sources(new_api=True)
    stale = subprocess.run(
        fixed_command, cwd=fixture, env=environment, capture_output=True, text=True, check=False
    )
    assert stale.returncode == 101, stale.stderr
    assert "error[E0425]" in stale.stderr, stale.stderr
    assert "cannot find function `new_api`" in stale.stderr, stale.stderr
    cached_sources = list(
        (root / "cargo-home/registry/src").glob("*/gf-package-producer-0.0.0/src/lib.rs")
    )
    assert len(cached_sources) == 1, cached_sources
    cached_source = cached_sources[0].read_bytes()
    assert b"new_api" not in cached_source

    # Keep the caller's target outputs and Cargo source cache intact, including
    # the stale entry used above. Only each helper invocation's target is removed.
    sentinel = root / "target" / "caller-output"
    sentinel.write_text("keep", encoding="utf-8")
    target_entries = set((root / "target").iterdir())
    package_mod = load_module()
    package_mod.ROOT = fixture
    original_environ = os.environ.copy()
    os.environ.clear()
    os.environ.update(environment)
    try:
        for new_api in (False, True):
            write_sources(new_api=new_api)
            package_mod.package(order)
            assert set((root / "target").iterdir()) == target_entries
            assert cached_sources[0].read_bytes() == cached_source
            assert sentinel.read_text(encoding="utf-8") == "keep"
            assert os.environ == environment

        # Real compiler failures propagate without retry and clean partial output.
        (fixture / "consumer/src/lib.rs").write_text(
            "pub fn use_api() -> u8 { gf_package_producer::missing_api() }\n", encoding="utf-8"
        )
        try:
            package_mod.package(order)
            raise AssertionError("expected package verification failure")
        except subprocess.CalledProcessError as exc:
            assert exc.returncode == 101
            failed_target = Path(exc.cmd[exc.cmd.index("--target-dir") + 1])
            assert failed_target.parent == root / "target"
            assert not failed_target.exists()
        assert set((root / "target").iterdir()) == target_entries
        assert cached_sources[0].read_bytes() == cached_source
        assert sentinel.read_text(encoding="utf-8") == "keep"
        assert os.environ == environment
    finally:
        os.environ.clear()
        os.environ.update(original_environ)

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
