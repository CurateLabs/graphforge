#!/usr/bin/env python3
"""Deterministic tests for the checksum-safe crates.io publisher."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import importlib.util
import json
from pathlib import Path
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
assert mod.VERSION == "0.5.1"
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

RATE_BODY = (
    "error: failed to publish graphforge-plan v0.5.1 to registry at https://crates.io\n"
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
    ["cargo", "publish", "-p", "graphforge-plan", "--locked", "--no-verify"],
    ["cargo", "publish", "-p", "graphforge-plan", "--locked", "--no-verify"],
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
assert publish_calls == [["cargo", "publish", "-p", "graphforge-plan", "--locked", "--no-verify"]]
assert sleeps == []

published: list[str] = []
mod.package_checksum = lambda _name, expected=None: expected or "abc123"
mod.owner_logins = lambda _name: {"DecisionNerd"}
mod.cargo_publish = lambda name, **_kwargs: published.append(name)

mod.version_record = lambda _name: {"checksum": "abc123"}
assert (
    mod.publish_one("graphforge-core", expected_checksum="abc123")
    == "already published; checksum and owner match"
)
assert published == []

mod.version_record = lambda _name: None
assert (
    mod.publish_one("graphforge-core")
    == "accepted; public checksum and owner verification required"
)
assert published == ["graphforge-core"]

published.clear()
assert (
    mod.publish_authorized("graphforge-core", "abc123")
    == "accepted; public checksum and owner verification required"
)
assert published == ["graphforge-core"]

mod.version_record = lambda _name: {"checksum": "different"}
try:
    mod.publish_one("graphforge-core")
    raise AssertionError("expected an existing-version checksum mismatch")
except RuntimeError as exc:
    assert "refusing to resume" in str(exc)

mod.version_record = lambda _name: {"checksum": "abc123"}
mod.owner_logins = lambda _name: {"someone-else"}
try:
    mod.publish_one("graphforge-core")
    raise AssertionError("expected the owner assertion to fail")
except RuntimeError as exc:
    assert "DecisionNerd is not an owner" in str(exc)

with tempfile.TemporaryDirectory() as temp:
    root = Path(temp)
    artifacts = root / "artifacts"
    artifacts.mkdir()
    archive = artifacts / f"graphforge-core-{v}.crate"
    archive.write_bytes(b"certified crate")
    sha = hashlib.sha256(archive.read_bytes()).hexdigest()
    record = {
        "schema": "graphforge-release-record-v1",
        "version": v,
        "tag": f"v{v}",
        "commit_sha": "release-sha",
        "artifacts": [
            {
                "surface": "crates",
                "name": "graphforge-core",
                "version": v,
                "path": archive.name,
                "sha256": sha,
            }
        ],
    }
    record_path = root / "record.json"
    original_run = subprocess.run

    def release_sha(*args, **_kwargs):
        return subprocess.CompletedProcess(args[0], 0, stdout="release-sha\n")

    mod.subprocess.run = release_sha
    record_path.write_text(json.dumps(record), encoding="utf-8")
    assert mod.release_record_checksums(record_path, artifacts) == {"graphforge-core": sha}

    for escaped in ("../outside.crate", "nested/../../outside.crate", "/etc/passwd"):
        record["artifacts"][0]["path"] = escaped
        record_path.write_text(json.dumps(record), encoding="utf-8")
        try:
            mod.release_record_checksums(record_path, artifacts)
            raise AssertionError("expected escaped artifact path to fail")
        except RuntimeError as exc:
            assert "escapes artifact root" in str(exc)

    record["artifacts"][0]["path"] = archive.name
    # Intentional mismatch: distinct wrong literal, not another copy of current.
    record["artifacts"][0]["version"] = "0.0.0"
    record_path.write_text(json.dumps(record), encoding="utf-8")
    try:
        mod.release_record_checksums(record_path, artifacts)
        raise AssertionError("expected artifact version mismatch to fail")
    except RuntimeError as exc:
        assert "version mismatch" in str(exc)
    mod.subprocess.run = original_run

print("publish crates tests passed")
