#!/usr/bin/env python3
"""Prove the Cargo↔Bazel drift check fails closed on intentional divergence."""

from __future__ import annotations

from contextlib import redirect_stderr
import copy
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import tempfile
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
CHECK = ROOT / "scripts/ci/cargo-bazel-drift-check.py"


def test_locked_crates() -> None:
    spec = importlib.util.spec_from_file_location("cargo_bazel_drift", CHECK)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    cargo_lock = """version = 4
[[package]]
name = "bytes"
version = "1.12.1"
source = "registry+https://github.com/rust-lang/crates.io-index"
checksum = "CHECKSUM"
""".replace("CHECKSUM", "a" * 64)
    package = {
        "name": "bytes",
        "version": "1.12.1",
        "repository": {
            "Http": {
                "url": "https://static.crates.io/crates/bytes/1.12.1/download",
                "sha256": "a" * 64,
            }
        },
    }
    good = {"crates": {"bytes 1.12.1": package}}
    bad_version = copy.deepcopy(good)
    old = bad_version["crates"].pop("bytes 1.12.1")
    old["version"] = "1.11.1"
    old["repository"]["Http"]["url"] = "https://static.crates.io/crates/bytes/1.11.1/download"
    bad_version["crates"]["bytes 1.11.1"] = old
    mutations = {
        "stale_version": bad_version,
        "missing_package": {"crates": {}},
        "wrong_checksum": copy.deepcopy(good),
        "wrong_source": copy.deepcopy(good),
        "missing_repository": copy.deepcopy(good),
        "wrong_identity": copy.deepcopy(good),
        "extra_package": copy.deepcopy(good),
    }
    mutations["wrong_checksum"]["crates"]["bytes 1.12.1"]["repository"]["Http"]["sha256"] = "b" * 64
    mutations["wrong_source"]["crates"]["bytes 1.12.1"]["repository"]["Http"]["url"] = (
        "https://other.invalid/bytes"
    )
    mutations["missing_repository"]["crates"]["bytes 1.12.1"].pop("repository")
    mutations["wrong_identity"]["crates"]["bytes 1.12.1"]["version"] = "1.11.1"
    extra = copy.deepcopy(package)
    extra["version"] = "1.11.1"
    mutations["extra_package"]["crates"]["bytes 1.11.1"] = extra
    with tempfile.TemporaryDirectory(prefix="graphforge-lock-drift-") as tmp:
        root = Path(tmp)
        (root / "Cargo.lock").write_text(cargo_lock)
        bazel = root / "cargo-bazel-lock.json"
        bazel.write_text(json.dumps(good))
        module.validate_locked_crates(root)
        for name, document in mutations.items():
            bazel.write_text(json.dumps(document))
            try:
                module.validate_locked_crates(root)
            except ValueError:
                pass
            else:
                raise AssertionError(f"accepted {name} Cargo/Bazel lock drift")

        # Rewriting the unchanged first-party feature fingerprint cannot bless
        # a dependency update that Bazel has not adopted.
        bazel.write_text(json.dumps(bad_version))
        fingerprint = root / "fingerprint.json"
        metadata = {"workspace_members": [], "packages": []}
        module.write_fingerprint(fingerprint, module.fingerprint_payload([]))
        with (
            patch.object(module, "cargo_metadata", return_value=metadata),
            redirect_stderr(io.StringIO()) as errors,
        ):
            result = module.main(["--root", str(root), "--fingerprint", str(fingerprint)])
        assert result == 1, "fresh feature fingerprint hid stale Bazel dependency versions"
        assert "locked dependency drift" in errors.getvalue()


def main() -> None:
    test_locked_crates()
    with tempfile.TemporaryDirectory(prefix="graphforge-drift-") as tmp:
        tmp_path = Path(tmp)
        good = tmp_path / "good.json"
        bad = tmp_path / "bad.json"

        subprocess.check_call(
            ["python3", str(CHECK), "--write", "--fingerprint", str(good)],
            cwd=ROOT,
        )
        payload = json.loads(good.read_text(encoding="utf-8"))
        # Intentional divergence: drop a dependency feature entry.
        assert payload["entries"], "fingerprint must contain workspace packages"
        payload["entries"][0]["dependencies"] = payload["entries"][0]["dependencies"][:-1] or [
            {
                "name": "intentionally-missing-dep",
                "req": "1.0.0",
                "features": ["drift"],
                "optional": False,
                "uses_default_features": True,
                "kind": None,
                "target": None,
            }
        ]
        # Keep a stale sha so either sha or entries mismatch fails closed.
        bad.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

        ok = subprocess.run(
            ["python3", str(CHECK), "--fingerprint", str(good)],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        if ok.returncode != 0:
            raise SystemExit(f"expected matching fingerprint to pass:\n{ok.stderr}")

        drifted = subprocess.run(
            ["python3", str(CHECK), "--fingerprint", str(bad)],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        if drifted.returncode == 0:
            raise SystemExit("expected intentional divergence to fail closed")
        if "drifted" not in drifted.stderr.lower() and "drift" not in drifted.stderr.lower():
            raise SystemExit(f"unexpected failure output:\n{drifted.stderr}")

    print("cargo-bazel drift check tests passed")


if __name__ == "__main__":
    main()
