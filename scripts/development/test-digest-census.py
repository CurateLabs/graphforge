#!/usr/bin/env python3
"""Regression fixtures for the static digest census method; no Rust build."""

import hashlib
import json
import os
import pathlib
import re
import subprocess
import tempfile

method = pathlib.Path(__file__).with_name("digest-census.py")
with tempfile.TemporaryDirectory(prefix="gf-census-method-fixture-") as tmp:
    container = pathlib.Path(tmp)
    # Git subprocesses belong to this fixture, including the census's Git reads.
    # Inherited repository selectors or user configuration must not redirect them.
    git_env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    templates = container / "empty-templates"
    hooks = container / "empty-hooks"
    templates.mkdir()
    hooks.mkdir()
    git_env.update(
        GIT_CONFIG_GLOBAL=os.devnull,
        GIT_CONFIG_NOSYSTEM="1",
        GIT_TEMPLATE_DIR=str(templates),
    )
    fixture_git = ["git", "-c", f"core.hooksPath={hooks}", "-c", "commit.gpgsign=false"]
    root = container / "repo"
    p = root / "crates/demo/src/lib.rs"
    p.parent.mkdir(parents=True)
    p.write_text("""use sha2::Sha256 as Crypto;
use graphforge_core::hash_observation::PortableSha256 as Transport;
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
fn portable_boundary() { Transport::digest(b"archive bytes"); }
fn portable_domain() { ObservedSha256::for_domain(HashDomain::PortableAuthentication); }
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
                        "function": "portable_boundary",
                        "function_bodies_sha256": body_sha("portable_boundary"),
                        "role": "portable_authentication",
                        "input_contract": "Fixture actual portable archive bytes.",
                    },
                    {
                        "path": "crates/demo/src/lib.rs",
                        "function": "portable_domain",
                        "function_bodies_sha256": body_sha("portable_domain"),
                        "role": "portable_authentication",
                        "input_contract": "Fixture actual portable member authentication.",
                    },
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
    subprocess.run([*fixture_git, "init", "-q"], cwd=root, env=git_env, check=True)
    subprocess.run([*fixture_git, "add", "."], cwd=root, env=git_env, check=True)
    subprocess.run(
        [
            *fixture_git,
            "-c",
            "user.name=Census fixture",
            "-c",
            "user.email=census@localhost",
            "commit",
            "-qm",
            "fixture",
        ],
        cwd=root,
        env=git_env,
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
        env=git_env,
        capture_output=True,
        check=False,
    )
    summary = json.loads((out / "summary.json").read_text())["summary"]
    assert run.returncode == 0, (
        run.stdout,
        run.stderr,
        json.loads((out / "review-gaps.json").read_text())["gaps"],
    )
    assert summary["application_producer_sites"] == 6, summary
    assert summary["excluded_test_constructors"] == 3, summary
    assert summary["producer_classes"] == {"b": 4, "portable_authentication": 2}, summary
    producers = json.loads((out / "producers.json").read_text())["producers"]
    portable = [row for row in producers if row["function"].startswith("portable_")]
    assert len(portable) == 2, portable
    assert all(
        row["role_by_explicit_type"] == "portable_authentication"
        and row["semantic_role"] == "portable_authentication"
        and row["class"] == "portable_authentication"
        for row in portable
    ), portable
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
        env=git_env,
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
        env=git_env,
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
    # A known portable alias does not bypass semantic review or its input pin.
    p.write_text(source.replace('b"archive bytes"', 'b"changed archive bytes"'))
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
        env=git_env,
        capture_output=True,
        check=False,
    )
    assert run.returncode == 1, (run.stdout, run.stderr)
    gaps = json.loads((out / "review-gaps.json").read_text())["gaps"]
    assert any(
        g["kind"] == "reviewed_input_changed" and g["function"] == "portable_boundary" for g in gaps
    ), gaps
    p.write_text(source)
    p.write_text(source + "\nfn unreviewed_portable() { Transport::new(); }\n")
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
        env=git_env,
        capture_output=True,
        check=False,
    )
    assert run.returncode == 1, (run.stdout, run.stderr)
    gaps = json.loads((out / "review-gaps.json").read_text())["gaps"]
    assert any(
        g["kind"] == "unclassified_producer" and g["function"] == "unreviewed_portable"
        for g in gaps
    ), gaps
    p.write_text(source)
    p.write_text(
        source.replace('fn portable_boundary() { Transport::digest(b"archive bytes"); }\n', "")
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
        env=git_env,
        capture_output=True,
        check=False,
    )
    assert run.returncode == 1, (run.stdout, run.stderr)
    gaps = json.loads((out / "review-gaps.json").read_text())["gaps"]
    assert any(
        g["kind"] == "stale_override" and g["function"] == "portable_boundary" for g in gaps
    ), gaps
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
        env=git_env,
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
        env=git_env,
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
        env=git_env,
        capture_output=True,
        check=False,
    )
    assert run.returncode == 1, (run.stdout, run.stderr)
    gaps = json.loads((out / "review-gaps.json").read_text())["gaps"]
    assert any(g["kind"] == "unreviewed_crypto_algorithm" for g in gaps), gaps
    # Only the pinned private writer getter is exempt; other digest receivers
    # stay unresolved, and adding a producer to that getter invalidates its pin.
    p.write_text(source)
    storage = root / "crates/graphforge-storage/src"
    storage.mkdir(parents=True)
    digest_producer = "fn digest() { Transport::new(); }"
    p.write_text(source + digest_producer + "\n")
    fixtures = {
        "project_portable_v2_export.rs": "fn export() { written.digest(); }\n",
        "project_portable_v2_export/transport.rs": (
            "use graphforge_core::hash_observation::{ControlSha256, PortableSha256 as Sha256};\n"
            "fn digest(&self) -> [u8; 32] { self.digest }\n"
            'fn copy() { ControlSha256::digest(b"control"); Sha256::new(); }\n'
        ),
        "project_portable_v2.rs": (
            "fn hash_file() { StreamHash::new(); }\nfn scan() { hash_file(); }\n"
        ),
        "project_portable_v2/authenticated_entries.rs": (
            "use graphforge_core::hash_observation::PortableSha256 as Transport;\n"
            "fn new() { Transport::new(); }\n"
        ),
    }
    decision = json.loads(overrides.read_text())
    decision["function_overrides"].append(
        {
            "path": str(p.relative_to(root)),
            "function": "digest",
            "function_bodies_sha256": [hashlib.sha256(digest_producer.encode()).hexdigest()],
            "role": "portable_authentication",
            "input_contract": "Fixture real producer namesake.",
        }
    )
    for relative, text in fixtures.items():
        path = storage / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        name = {
            "project_portable_v2.rs": "hash_file",
            "project_portable_v2_export/transport.rs": "digest",
            "project_portable_v2/authenticated_entries.rs": "new",
        }.get(relative)
        if name:
            body = re.search(rf"fn {name}\b[^\n]*", text).group()
            decision["function_overrides"].append(
                {
                    "path": str(path.relative_to(root)),
                    "function": name,
                    "function_bodies_sha256": [hashlib.sha256(body.encode()).hexdigest()],
                    "role": "portable_authentication" if name == "new" else "producer_delegate",
                    "input_contract": "Fixture private stored getter or bounded source delegate.",
                }
            )
    overrides.write_text(json.dumps(decision))
    mixed_body = re.search(
        r"fn copy\b[^\n]*", fixtures["project_portable_v2_export/transport.rs"]
    ).group()
    decision["function_overrides"].append(
        {
            "path": str((storage / "project_portable_v2_export/transport.rs").relative_to(root)),
            "function": "copy",
            "function_bodies_sha256": [hashlib.sha256(mixed_body.encode()).hexdigest()],
            "role": "caller_selected",
            "input_contract": "Fixture separates resident control and payload SHA.",
        }
    )
    overrides.write_text(json.dumps(decision))

    def probe_private_getter():
        return subprocess.run(
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
            env=git_env,
            capture_output=True,
            check=False,
        )

    run = probe_private_getter()
    assert run.returncode == 0, (run.stdout, run.stderr)
    producers = json.loads((out / "producers.json").read_text())["producers"]
    mixed = [
        row
        for row in producers
        if row["path"].endswith("/transport.rs") and row["function"] == "copy"
    ]
    assert {row["class"] for row in mixed} == {
        "control_authentication",
        "portable_authentication",
    }, mixed
    getter = storage / "project_portable_v2_export/transport.rs"
    getter.write_text(
        getter.read_text().replace("self.digest", 'Transport::digest(b"new producer")')
    )
    run = probe_private_getter()
    assert run.returncode == 1, (run.stdout, run.stderr)
    gaps = json.loads((out / "review-gaps.json").read_text())["gaps"]
    assert any(g["kind"] == "reviewed_input_changed" and g["function"] == "digest" for g in gaps), (
        gaps
    )
    getter.write_text(fixtures["project_portable_v2_export/transport.rs"])
    caller = storage / "project_portable_v2_export.rs"
    caller.write_text(caller.read_text().replace("written.digest()", "untrusted.digest()"))
    run = probe_private_getter()
    assert run.returncode == 1, (run.stdout, run.stderr)
    gaps = json.loads((out / "review-gaps.json").read_text())["gaps"]
    assert any(g["kind"] == "unresolved_helper" and g["callee"] == "digest" for g in gaps), gaps
    print(
        "PASS: alias, typed portable authentication and changed portable input refusal, "
        "constructor callback, nested comments, raw/byte literals, "
        "test impl/block/module exclusion, "
        "mixed cfg feature retention, output refusal, changed-input/new-producer/unpinned refusal, "
        "unknown crypto fail-closed, pinned writer getter and wrong-receiver refusal"
    )
