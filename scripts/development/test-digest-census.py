#!/usr/bin/env python3
"""Regression fixtures for the static digest census method; no Rust build."""

import hashlib
import json
import pathlib
import re
import subprocess
import tempfile

method = pathlib.Path(__file__).with_name("digest-census.py")
with tempfile.TemporaryDirectory(prefix="gf-census-method-fixture-") as tmp:
    container = pathlib.Path(tmp)
    root = container / "repo"
    p = root / "crates/demo/src/lib.rs"
    p.parent.mkdir(parents=True)
    p.write_text("""use sha2::Sha256 as Crypto;
fn fingerprint() {
 let literal = r###"Sha256::new(); /* not code */"###;
 /* nested /* Sha256::new(); */ Sha256::new(); */
 let byte = b'\"';
 Crypto::digest(b"actual");
}
#[cfg(all(feature = "x", test))]
impl Synthetic { fn test_only() { Sha256::new(); } }
fn runtime() { #[cfg(test)] { Sha256::new(); } }
#[cfg(any(test, feature = "production"))]
fn alternative() { Crypto::new(); }
fn callback() { let state = true.then(Crypto::new); }
fn shard_set_identity() { Crypto::digest(b"descriptors"); }
fn wrapper() { shard_set_identity(); }
#[cfg(test)] mod tests { fn helper() { Sha256::new(); } }
""")
    source = p.read_text()

    def body_sha(name):
        # Fixture functions are unambiguously named; match original literals too.
        pattern = (
            rf"fn {name}\(\) \{{.*?^\}}"
            if name == "fingerprint"
            else rf"fn {name}\(\) \{{[^\n]*\}}"
        )
        body = re.search(pattern, source, re.S | re.M).group()
        return [hashlib.sha256(body.encode()).hexdigest()]

    overrides = root / "overrides.json"
    overrides.write_text(
        json.dumps(
            {
                "function_overrides": [
                    {
                        "path": "crates/demo/src/lib.rs",
                        "function": "shard_set_identity",
                        "function_bodies_sha256": body_sha("shard_set_identity"),
                        "role": "contract_identity",
                        "input_contract": "Fixture immutable descriptor identity.",
                    },
                    {
                        "path": "crates/demo/src/lib.rs",
                        "function": "fingerprint",
                        "function_bodies_sha256": body_sha("fingerprint"),
                        "role": "contract_identity",
                        "input_contract": "Fixture contract bytes.",
                    },
                    {
                        "path": "crates/demo/src/lib.rs",
                        "function": "alternative",
                        "function_bodies_sha256": body_sha("alternative"),
                        "role": "contract_identity",
                        "input_contract": "Feature-enabled production contract.",
                    },
                    {
                        "path": "crates/demo/src/lib.rs",
                        "function": "callback",
                        "function_bodies_sha256": body_sha("callback"),
                        "role": "contract_identity",
                        "input_contract": "Conditional producer constructor callback.",
                    },
                ],
                "nonproducer_symbols": [],
            }
        )
    )
    subprocess.run(["git", "init", "-q"], cwd=root, check=True)
    subprocess.run(["git", "add", "."], cwd=root, check=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=Census fixture",
            "-c",
            "user.email=census@localhost",
            "commit",
            "-qm",
            "fixture",
        ],
        cwd=root,
        check=True,
    )
    out = container / "output"
    run = subprocess.run(
        [
            "python3",
            method,
            "--repo",
            str(root),
            "--output",
            str(out),
            "--overrides",
            str(overrides),
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    summary = json.loads((out / "summary.json").read_text())["summary"]
    assert run.returncode == 0, (
        run.stdout,
        run.stderr,
        json.loads((out / "review-gaps.json").read_text())["gaps"],
    )
    assert summary["application_producer_sites"] == 4, summary
    assert summary["excluded_test_constructors"] == 3, summary
    assert summary["producer_classes"] == {"b": 4}, summary
    delegates = json.loads((out / "delegates.json").read_text())["edges"]
    assert any(
        edge["delegate"] == "shard_set_identity"
        and edge["caller"] == "wrapper"
        and edge["class"] == "b"
        and not edge["count_as_application_producer"]
        for edge in delegates
    ), delegates
    # Raw evidence must never enter the source repository.
    refused = subprocess.run(
        [
            "python3",
            method,
            "--repo",
            str(root),
            "--output",
            str(root / "evidence"),
            "--overrides",
            str(overrides),
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    assert refused.returncode == 2 and not (root / "evidence").exists(), refused.stderr
    # The same function name cannot inherit a role after its actual input changes.
    p.write_text(source.replace('b"actual"', 'b"changed input"'))
    run = subprocess.run(
        [
            "python3",
            method,
            "--repo",
            str(root),
            "--output",
            str(out),
            "--overrides",
            str(overrides),
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    assert run.returncode == 1, (run.stdout, run.stderr)
    gaps = json.loads((out / "review-gaps.json").read_text())["gaps"]
    assert any(
        g["kind"] == "reviewed_input_changed" and g["function"] == "fingerprint" for g in gaps
    ), gaps
    assert not any(g["kind"] == "stale_override" for g in gaps), gaps
    p.write_text(source)
    # Adding a second producer inside the reviewed function also requires review.
    p.write_text(
        source.replace('Crypto::digest(b"actual");', 'Crypto::digest(b"actual"); Crypto::new();')
    )
    run = subprocess.run(
        [
            "python3",
            method,
            "--repo",
            str(root),
            "--output",
            str(out),
            "--overrides",
            str(overrides),
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    assert run.returncode == 1, (run.stdout, run.stderr)
    gaps = json.loads((out / "review-gaps.json").read_text())["gaps"]
    assert any(g["kind"] == "reviewed_input_changed" for g in gaps), gaps
    # Missing pins cannot silently become a reviewed classification.
    p.write_text(source)
    decision = json.loads(overrides.read_text())
    del decision["function_overrides"][0]["function_bodies_sha256"]
    missing = container / "unpinned.json"
    missing.write_text(json.dumps(decision))
    run = subprocess.run(
        ["python3", method, "--repo", str(root), "--output", str(out), "--overrides", str(missing)],
        text=True,
        capture_output=True,
        check=False,
    )
    assert run.returncode == 1, (run.stdout, run.stderr)
    gaps = json.loads((out / "review-gaps.json").read_text())["gaps"]
    assert any(g["kind"] == "reviewed_input_unpinned" for g in gaps), gaps
    # New unknown primitive spelling must block an exhaustive claim.
    p.write_text(p.read_text() + "\nfn unknown(){sha3::Sha3_256::new();}\n")
    run = subprocess.run(
        [
            "python3",
            method,
            "--repo",
            str(root),
            "--output",
            str(out),
            "--overrides",
            str(overrides),
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    assert run.returncode == 1, (run.stdout, run.stderr)
    gaps = json.loads((out / "review-gaps.json").read_text())["gaps"]
    assert any(g["kind"] == "unreviewed_crypto_algorithm" for g in gaps), gaps
    print(
        "PASS: alias, constructor callback, nested comments, raw/byte literals, "
        "test impl/block/module exclusion, "
        "mixed cfg feature retention, output refusal, changed-input/new-producer/unpinned refusal, "
        "unknown crypto fail-closed"
    )
