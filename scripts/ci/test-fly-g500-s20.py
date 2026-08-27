#!/usr/bin/env python3
from __future__ import annotations

import argparse
import copy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import urllib.error

import pytest

ROOT = Path(__file__).resolve().parents[2]


def load(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


controller = load(ROOT / "scripts/fly-g500-s20.py", "fly_s20_controller")
validator = load(ROOT / "scripts/ci/validate-fly-g500-s20.py", "fly_s20_validator")
attestation = load(ROOT / "scripts/ci/fly-s20-source-attestation.py", "fly_s20_source_attestation")


def args(tmp_path: Path, **changes):
    values = {
        "expected_sha": "a" * 40,
        "image_source_sha": "a" * 40,
        "image": "registry.example/graphforge@sha256:" + "b" * 64,
        "region": "den",
        "org": "curatelabs",
        "app_name": "gf-s20-unique",
        "volume_name": "gf_s20_unique",
        "machine_name": "gf-s20-machine",
        "volume_size_gb": 500,
        "timeout_s": 14_400,
        "ledger": tmp_path / "ledger.json",
        "evidence_out": tmp_path / "evidence.json",
        "diagnostic_out": tmp_path / "diagnostic.json",
        "execute": False,
        "confirm_disposable": False,
    }
    values.update(changes)
    return argparse.Namespace(**values)


def stub_run_credential(monkeypatch, fly):
    credential = controller.RunCredential(
        "token-test-id",
        "gf-s20-test",
        "test-secret-value-long-enough",
        controller.time.monotonic() + controller.RUN_TOKEN_LIFETIME_SECONDS,
    )
    monkeypatch.setattr(controller, "mint_run_credential", lambda *_args: credential)
    monkeypatch.setattr(controller, "revoke_run_credential", lambda *_args: None)
    monkeypatch.setattr(controller, "run_scoped_flyctl", lambda _credential: fly)
    if not hasattr(fly, "auth_token"):
        fly.auth_token = lambda **_kwargs: "test-secret-value-long-enough"


def token_table(rows):
    header = " ID │ NAME │ CREATED BY │ EXPIRES AT │ REVOKED AT "
    return (
        'Tokens for organization "personal":\n'
        + header
        + "\n"
        + "\n".join(" │ ".join(row) for row in rows)
        + "\n"
    )


def test_org_token_table_parses_active_and_revoked_go_times():
    output = token_table(
        [
            ("id-active", "gf-s20-a", "owner", "2026-08-28 01:02:03.123 +0000 UTC", "-"),
            (
                "id-old",
                "gf-s20-old",
                "owner",
                "2026-08-28 01:02:03 +0000 UTC",
                "2026-08-27 20:00:00 +0000 UTC",
            ),
        ]
    )
    rows = controller.parse_org_token_list(output)
    assert controller.token_is_active(rows["id-active"])
    assert not controller.token_is_active(rows["id-old"])
    with pytest.raises(controller.ControllerError, match="unsafe formatting"):
        controller.parse_org_token_list("\x1b[31m" + output)


def test_org_token_parser_accepts_exact_flyctl_v0487_empty_output():
    observed = (
        'Tokens for organization "personal":\n'
        " ID │ NAME │ CREATED BY │ EXPIRES AT │ REVOKED AT \n\n"
    )
    assert controller.parse_org_token_list(observed) == {}


def test_mint_resolves_exact_new_org_token_and_binds_provider_expiry(monkeypatch, tmp_path):
    now = datetime.now(timezone.utc)
    expiry = now + timedelta(hours=6)
    expiry_text = expiry.strftime("%Y-%m-%d %H:%M:%S +0000 UTC")
    before = token_table([])
    after = token_table(
        [
            ("token-new-id", "gf-s20-gf-s20-unique", "owner", expiry_text, "-"),
        ]
    )
    outputs = iter((before, json.dumps({"token": "secret-value-that-is-never-logged"}), after))

    class Bootstrap:
        def run(self, command, **_kwargs):
            return argparse.Namespace(returncode=0, stdout=next(outputs), stderr="")

    monkeypatch.setattr(controller.time, "time", now.timestamp)
    monkeypatch.setattr(controller.time, "monotonic", lambda: 100.0)
    credential = controller.mint_run_credential(args(tmp_path, org="personal"), Bootstrap())
    assert credential.token_id == "token-new-id"
    assert credential.secret == "secret-value-that-is-never-logged"
    assert credential.expires_at_monotonic > 100 + 5 * 60 * 60


def test_run_token_403_is_typed_closed_and_never_leaks_body_or_token(monkeypatch):
    raw_body = b"sensitive provider body"
    calls = []

    def urlopen(_request, **_kwargs):
        calls.append(1)
        raise urllib.error.HTTPError(
            "url",
            403,
            "forbidden",
            {"Fly-Request-Id": "private-request-id"},
            __import__("io").BytesIO(raw_body),
        )

    credential = controller.RunCredential(
        "id",
        "name",
        "very-secret-run-token-value",
        controller.time.monotonic() + 999,
    )
    fly = controller.Flyctl(credential)
    monkeypatch.setattr(controller.urllib.request, "urlopen", urlopen)
    with pytest.raises(controller.ProviderRequestError) as captured:
        fly.api_json("GET", "/v1/apps/app/machines/id", operation="machine_runtime_get")
    assert len(calls) == 2
    assert captured.value.details["http_class"] == "permission_denied"
    assert captured.value.details["request_attempts"] == 2
    assert captured.value.details["read_reloaded_once"] is True
    serialized = json.dumps(captured.value.details)
    assert raw_body.decode() not in serialized
    assert credential.secret not in serialized


def test_machine_post_malformed_success_is_typed_without_replay(monkeypatch):
    calls = []

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return b'{"truncated":'

    def urlopen(_request, **_kwargs):
        calls.append(1)
        return Response()

    fly = controller.Flyctl(
        controller.RunCredential(
            "token-id",
            "name",
            "very-secret-run-token-value",
            controller.time.monotonic() + 999,
        )
    )
    monkeypatch.setattr(controller.urllib.request, "urlopen", urlopen)
    with pytest.raises(controller.ProviderRequestError) as captured:
        fly.api_json("POST", "/v1/apps/app/machines", data={}, operation="machine_create")
    assert len(calls) == 1
    assert captured.value.details["http_class"] == "malformed_response"


def test_provider_diagnostic_rejects_boolean_attempts_and_invalid_status():
    valid = {
        "operation": "machine_runtime_get",
        "outcome": "http_error",
        "http_class": "permission_denied",
        "http_status": 403,
        "elapsed_seconds": 1,
        "body_prefix_sha256": "sha256:" + "a" * 64,
        "body_truncated": False,
        "request_id_sha256": None,
        "request_attempts": 2,
        "read_reloaded_once": True,
    }
    controller.validate_provider_request_diagnostic(valid)
    for key, bad in (("request_attempts", True), ("elapsed_seconds", -1), ("http_status", "403")):
        mutated = dict(valid)
        mutated[key] = bad
        with pytest.raises(controller.ControllerError):
            controller.validate_provider_request_diagnostic(mutated)


def test_revoke_uses_post_state_even_when_cli_reports_failure():
    revoked = token_table(
        [
            (
                "token-id",
                "gf-s20-run",
                "owner",
                "2026-08-28 01:00:00 +0000 UTC",
                "2026-08-27 20:00:00 +0000 UTC",
            ),
        ]
    )

    class Bootstrap:
        def run(self, command, **_kwargs):
            if command[:2] == ["tokens", "revoke"]:
                return argparse.Namespace(returncode=1, stdout="", stderr="rejected")
            return argparse.Namespace(returncode=0, stdout=revoked, stderr="")

    controller.revoke_token_id(Bootstrap(), "token-id", "personal")


def test_revoke_timeout_still_uses_authoritative_post_state():
    revoked = token_table(
        [
            (
                "token-id",
                "gf-s20-run",
                "owner",
                "2026-08-28 01:00:00 +0000 UTC",
                "2026-08-27 20:00:00 +0000 UTC",
            ),
        ]
    )

    class Bootstrap:
        def run(self, command, **_kwargs):
            if command[:2] == ["tokens", "revoke"]:
                raise subprocess.TimeoutExpired(command, 1)
            return argparse.Namespace(returncode=0, stdout=revoked, stderr="")

    controller.revoke_token_id(Bootstrap(), "token-id", "personal")
    with pytest.raises(controller.ControllerError, match="ID is unsafe"):
        controller.revoke_token_id(Bootstrap(), "--token", "personal")


def test_machine_post_network_failure_reconciles_exact_unique_name(tmp_path):
    calls = []
    machine = {"id": "machine-id", "name": "gf-s20-machine"}

    class Fly:
        def api_json(self, method, _path, **kwargs):
            calls.append((method, kwargs["operation"]))
            if method == "POST":
                raise controller.ProviderRequestError("network", {"http_class": "network_error"})
            return [machine]

    result = controller.create_machine(
        args(tmp_path),
        Fly(),
        "volume-id",
        "registry.fly.io/app@sha256:" + "d" * 64,
        "sha256:" + "d" * 64,
        "c" * 64,
        deadline=controller.time.monotonic() + 60,
    )
    assert result == machine
    assert calls == [("POST", "machine_create"), ("GET", "machine_create_reconcile")]


def test_credential_admission_includes_setup_runtime_and_cleanup(monkeypatch):
    monkeypatch.setattr(controller.time, "monotonic", lambda: 100.0)
    required = controller.TOKEN_SETUP_RESERVE_SECONDS + 14_400 + controller.CLEANUP_RESERVE_SECONDS
    controller.admit_run_credential(
        controller.RunCredential("token-id", "name", "secret", 100 + required + 1),
        14_400,
    )
    with pytest.raises(controller.ControllerError, match="expires before"):
        controller.admit_run_credential(
            controller.RunCredential("token-id", "name", "secret", 100 + required),
            14_400,
        )


def test_source_attestation_ignores_untracked_caches_but_binds_tracked_bytes_and_mode(
    tmp_path,
):
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    (tmp_path / ".gitignore").write_text(".pytest_cache/\n__pycache__/\n")
    tracked = tmp_path / "tracked.sh"
    tracked.write_text("one\n")
    subprocess.run(["git", "add", ".gitignore", "tracked.sh"], cwd=tmp_path, check=True)
    initial = attestation.snapshot_sha256(tmp_path)
    (tmp_path / ".pytest_cache").mkdir()
    (tmp_path / ".pytest_cache" / "state").write_text("noise")
    (tmp_path / "__pycache__").mkdir()
    (tmp_path / "__pycache__" / "module.pyc").write_bytes(b"noise")
    assert attestation.snapshot_sha256(tmp_path) == initial
    tracked.write_text("two\n")
    changed_bytes = attestation.snapshot_sha256(tmp_path)
    assert changed_bytes != initial
    tracked.chmod(0o755)
    assert attestation.snapshot_sha256(tmp_path) != changed_bytes


def test_source_attestation_binds_tracked_symlink_target(tmp_path):
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    (tmp_path / "one").write_text("one")
    (tmp_path / "two").write_text("two")
    (tmp_path / "link").symlink_to("one")
    subprocess.run(["git", "add", "one", "two", "link"], cwd=tmp_path, check=True)
    initial = attestation.snapshot_sha256(tmp_path)
    (tmp_path / "link").unlink()
    (tmp_path / "link").symlink_to("two")
    assert attestation.snapshot_sha256(tmp_path) != initial


def construction():
    keys = {
        "input_rows",
        "input_batches",
        "parquet_shards",
        "write_bytes",
        "write_operations",
        "authentication_read_bytes",
        "authentication_read_operations",
        "peak_batch_rows",
        "peak_batch_bytes",
        "peak_accounted_live_bytes",
        "peak_run_records",
        "merge_read_records",
        "merge_written_records",
        "merge_groups",
        "peak_merge_inputs",
        "merge_read_bytes",
        "merge_written_bytes",
        "merge_fsync_operations",
        "parquet_read_bytes",
        "parquet_read_operations",
        "parquet_write_bytes",
        "parquet_write_operations",
        "retained_probe_read_bytes",
        "retained_probe_block_loads",
        "storage_transient_peak_allocated_bytes",
    }
    value = dict.fromkeys(keys, 1)
    value.update(
        {
            "input_batches": 428,
            "edge_batch_commits": 300,
            "peak_accounted_live_bytes": 268_435_456,
            "publication_commits": 1,
            "recovery_replay": True,
            "published_generation_sha256": "sha256:" + "9" * 64,
            "recovered_generation_sha256": "sha256:" + "9" * 64,
        }
    )
    return value


def query_proof():
    return {
        "fingerprint": "sha256:" + "c" * 64,
        "evidence": {
            "hops": [
                {
                    "id": 1,
                    "input_rows": 1,
                    "candidates_generated": 1,
                    "rows_emitted": 1,
                    "edge_rows_scanned": 1,
                    "edge_full_reads": 0,
                    "node_rows_scanned": 1,
                    "node_full_reads": 0,
                }
            ],
            "sorts": [],
            "memory_reserved_before": 0,
            "memory_reserved_after": 0,
            "returned_batch_bytes": 1,
            "operator_rss": {"expand_peak_bytes": 1, "sort_peak_bytes": 0},
        },
    }


def attribution():
    totals = {
        "logical_references": 1,
        "logical_bytes": 1,
        "physical_objects": 1,
        "physical_logical_bytes": 1,
        "allocated_bytes": 4096,
    }
    zero = dict.fromkeys(totals, 0)
    categories = {
        name: dict(zero)
        for name in (
            "topology_nodes",
            "topology_edges",
            "properties",
            "uuid_and_surrogates",
            "adjacency",
            "catalog_and_manifests",
            "other",
        )
    }
    categories["topology_nodes"] = dict(totals)
    return {
        "generation_manifest_sha256": [1] * 32,
        "categories": categories,
        **totals,
    }


def evidence(**changes):
    value = {
        "schema": "graphforge-fly-g500-s20/1",
        "git_sha": "a" * 40,
        "build_provenance": {
            "schema": "graphforge-fly-s20-build-provenance/1",
            "source_sha": "a" * 40,
            "source_snapshot_sha256": "sha256:" + "c" * 64,
        },
        "image_digest": "sha256:" + "b" * 64,
        "provider": "fly.io",
        "region": "den",
        "scale": 20,
        "machine": {"class": "performance", "cpus": 2, "memory_mb": 4096},
        "volume_gb": 500,
        "result": "passed",
        "counts": {
            "generated_edges": 16_000_000,
            "source_edges": 16_000_000,
            "imported_edges": 16_000_000,
            "raw_attempts": 16_777_216,
            "self_loops_rejected": 100_000,
            "duplicates_rejected": 677_216,
        },
        "phase_memory": {
            phase: {
                "rss_peak_bytes": 500_000_000,
                "process_global_hwm_bytes": 600_000_000,
                "anonymous_peak_bytes": 300_000_000,
                "file_peak_bytes": 200_000_000,
                "volume_used_delta_peak_bytes": 16_384,
                "sample_interval_ms": 250,
            }
            for phase in (
                "generate",
                "ingest",
                "source_reopen",
                "source_query_1hop",
                "source_query_2hop",
                "export",
                "verify",
                "import",
                "imported_reopen",
                "imported_query_1hop",
                "imported_query_2hop",
                "finalize",
            )
        },
        "ingest_memory_windows": {
            "early_rss_peak_bytes": 500_000_000,
            "middle_rss_peak_bytes": 600_000_000,
            "late_rss_peak_bytes": 700_000_000,
            "early_sample_count": 100,
            "middle_sample_count": 100,
            "late_sample_count": 100,
            "early_progress_start": 1,
            "early_progress_end": 100,
            "middle_progress_start": 101,
            "middle_progress_end": 200,
            "late_progress_start": 201,
            "late_progress_end": 300,
            "final_committed_chunks": 300,
            "sampling_source": "authenticated_edge_chunk_commit",
            "bounded_working_set_bytes": 268_435_456,
            "sampling_tolerance_bytes": 67_108_864,
            "allowed_growth_bytes": 335_544_320,
            "observed_growth_bytes": 200_000_000,
            "plateau_pass": True,
            "envelope_bytes": 4_294_967_296,
            "headroom_bytes": 3_594_967_296,
        },
        "storage": {
            "logical_bytes": 1_003,
            "allocated_bytes": 16_384,
            "peak_allocated_bytes": 16_384,
            "generator_logical_bytes": 1_000,
            "generator_allocated_bytes": 4_096,
            "construction_transient_peak_allocated_bytes": 4_096,
            "capacity_bytes": 500_000_000_000,
        },
        "run": {"scale": 20, "edgefactor": 16, "seed": 1},
        "rung": {"pass": True, "reconciles": True, "construction": construction()},
        "lifecycle": {
            "source_nodes": 1_048_576,
            "source_edges": 16_000_000,
            "imported_nodes": 1_048_576,
            "imported_edges": 16_000_000,
            "source_export_generation_authenticated": True,
            "import_receipt_reopen_authenticated": True,
            "source_import_generations_distinct": True,
            "durable_steps": [
                "construction_published",
                "construction_recovered",
                "source_reopened",
                "source_query_1hop_completed",
                "source_query_2hop_completed",
                "export_completed",
                "verify_completed",
                "import_completed",
                "imported_reopened",
                "imported_query_1hop_completed",
                "imported_query_2hop_completed",
            ],
            "publication": {
                "commits": 1,
                "recovery_replay": True,
                "published_generation_sha256": "sha256:" + "9" * 64,
                "recovered_generation_sha256": "sha256:" + "9" * 64,
            },
            "package_digest": "sha256:" + "d" * 64,
            "portable_contract": "graphforge-portable-verify/2",
            "source_one_hop": query_proof(),
            "source_two_hop": query_proof(),
            "imported_one_hop": query_proof(),
            "imported_two_hop": query_proof(),
            "source_authority_fingerprint": "sha256:" + "e" * 64,
            "imported_authority_fingerprint": "sha256:" + "e" * 64,
            "source_storage": attribution(),
            "imported_storage": attribution(),
            "package_storage": {
                "category": "portable_bundle",
                "logical_bytes": 1,
                "allocated_bytes": 4096,
                "logical_references": 1,
                "physical_objects": 1,
                "source": "portable_bundle_exact_descriptor",
            },
        },
        "memory": {"rss_bytes": 1, "hwm_bytes": 1, "anonymous_bytes": 1, "file_bytes": 0},
        "wall_time_s": 1.0,
        "first_failure": None,
    }
    value.update(changes)
    return value


def pricing_html(*, region="den", rate="0.00002484", duplicate=False, volume="0.15"):
    row = f"""
      <tr><th>performance-2x</th><td>2 performance</td><td>4GB</td>
      <td>${rate}</td><td>$0.0894</td><td>$64.39</td></tr>
    """
    return f"""
      <div id="started-machines-pricing-matrix-{region}">
        <table>{row}{row if duplicate else ""}</table>
      </div>
      <p>Fly Volumes are local persistent storage for Machines.</p>
      <p>${volume}/GB per month</p><p>Volume billing is pro-rated to the hour.</p>
    """


def test_contract_fixes_resources_and_rejects_unsafe_local_image(tmp_path):
    controller.validate_inputs(args(tmp_path))
    digest = "sha256:" + "b" * 64
    private_image = "registry.fly.io/gf-s20-unique@" + digest
    payload = controller.machine_payload(args(tmp_path), "vol-id", private_image, digest, "c" * 64)
    assert payload["config"]["image"] == private_image
    assert payload["config"]["guest"] == {"cpu_kind": "performance", "cpus": 2, "memory_mb": 4096}
    assert payload["config"]["services"] == []
    assert payload["config"]["restart"] == {"policy": "no"}
    assert payload["config"]["mounts"] == [{"volume": "vol-id", "path": "/work"}]
    assert payload["config"]["env"]["GF_G500_CERTIFICATION_SCALE"] == "20"
    assert payload["config"]["env"]["GF_G500_S20_EXPECTED_SHA"] == "a" * 40
    assert payload["config"]["env"]["GF_G500_S20_VOLUME_GB"] == "500"
    assert payload["config"]["env"]["GF_G500_S20_EVIDENCE_OUT"] == "/work/s20-evidence.json"
    assert payload["config"]["env"]["GF_G500_S20_RESULT_OUT"] == "/work/container-result.json"
    assert payload["config"]["env"]["GF_G500_S20_TIMEOUT_SECONDS"] == "13800"
    with pytest.raises(controller.ControllerError, match="safe local"):
        controller.validate_inputs(args(tmp_path, image="registry.example/graphforge bad"))


@pytest.mark.parametrize("size", [0, 501])
def test_volume_is_explicitly_bounded_by_fly_limit(tmp_path, size):
    with pytest.raises(controller.ControllerError, match=r"1\.\.500"):
        controller.validate_inputs(args(tmp_path, volume_size_gb=size))


def test_execute_requires_confirmation(tmp_path):
    with pytest.raises(controller.ControllerError, match="confirm-disposable"):
        controller.validate_inputs(args(tmp_path, execute=True))


def test_durable_budget_reservations_survive_and_accumulate(tmp_path):
    ledger = tmp_path / "ledger.json"
    first = controller.price_reservation(500)
    assert first["reserved_usd"] == 1.11
    assert first["runtime_seconds"] == 14_400
    assert first["cleanup_reserve_seconds"] == 600
    assert first["volume_billing_hours"] == 5
    controller.reserve_budget(ledger, "run-one", first)
    controller.reserve_budget(ledger, "run-two", first)
    state = json.loads(ledger.read_text())
    assert sum(run["reservation"]["reserved_usd"] for run in state["runs"]) == 2.22
    assert state["runs"][0]["reservation"]["pricing_source"] == "https://fly.io/docs/about/pricing/"
    anchor = json.loads(ledger.with_suffix(".json.anchor").read_text())
    assert anchor["records"] == 2
    assert anchor["reserved_cents"] == 222
    oversized = controller.price_reservation(500, Decimal("0.0006"), Decimal("0.15"))
    with pytest.raises(controller.ControllerError, match="exceed"):
        controller.reserve_budget(ledger, "run-three", oversized)
    with pytest.raises(controller.ControllerError, match="already reserved"):
        controller.reserve_budget(ledger, "run-one", first)


@pytest.mark.parametrize("bad", ["NaN", "Infinity", -0.01, "garbage", None])
def test_budget_ledger_rejects_nonfinite_negative_and_malformed_existing_amounts(tmp_path, bad):
    ledger = tmp_path / "ledger.json"
    reservation = controller.price_reservation(500)
    ledger.write_text(
        json.dumps(
            {
                "schema": "graphforge-fly-cost-ledger/1",
                "limit_usd": 10.0,
                "runs": [{"run_id": "old", **reservation, "reserved_usd": bad}],
            }
        )
    )
    with pytest.raises(controller.ControllerError, match="cost ledger"):
        controller.reserve_budget(ledger, "new", reservation)


def test_budget_ledger_recomputes_existing_reservations_in_decimal_cents(tmp_path):
    ledger = tmp_path / "ledger.json"
    reservation = controller.price_reservation(500)
    controller.reserve_budget(ledger, "old", reservation)
    state = json.loads(ledger.read_text())
    state["runs"][0]["reservation"]["reserved_usd"] -= 0.01
    ledger.write_text(json.dumps(state))
    with pytest.raises(controller.ControllerError, match="authentication"):
        controller.reserve_budget(ledger, "new", reservation)


@pytest.mark.parametrize("missing", ["ledger", "anchor"])
def test_budget_ledger_rejects_deleted_history_or_anchor(tmp_path, missing):
    ledger = tmp_path / "ledger.json"
    controller.reserve_budget(ledger, "old", controller.price_reservation(500))
    target = ledger if missing == "ledger" else ledger.with_suffix(".json.anchor")
    target.unlink()
    with pytest.raises(controller.ControllerError, match="ledger or durable anchor is missing"):
        controller.reserve_budget(ledger, "new", controller.price_reservation(500))


def test_budget_ledger_rejects_valid_prefix_truncation(tmp_path):
    ledger = tmp_path / "ledger.json"
    reservation = controller.price_reservation(500)
    controller.reserve_budget(ledger, "one", reservation)
    prefix = ledger.read_text()
    controller.reserve_budget(ledger, "two", reservation)
    ledger.write_text(prefix)
    with pytest.raises(controller.ControllerError, match="history regressed"):
        controller.reserve_budget(ledger, "three", reservation)


def test_current_official_pricing_selects_fixed_region_and_derives_ledger():
    compute, volume = controller.parse_current_pricing(pricing_html(), "den")
    assert (compute, volume) == (Decimal("0.00002484"), Decimal("0.15"))
    reservation = controller.price_reservation(500, compute, volume)
    assert reservation["compute_rate_usd_per_second"] == 0.00002484
    assert reservation["reserved_usd"] == 0.90


@pytest.mark.parametrize(
    ("html", "region", "message"),
    [
        (pricing_html(region="ord"), "den", "no table"),
        (pricing_html(duplicate=True), "den", "one applicable"),
        (pricing_html(rate="0.00009999"), "den", "exceeds"),
        (
            pricing_html() + "<p>Fly Volumes $0.20/GB per month Volume billing is pro-rated</p>",
            "den",
            "ambiguous volume",
        ),
    ],
)
def test_current_official_pricing_fails_closed_on_wrong_ambiguous_or_higher_rows(
    html, region, message
):
    with pytest.raises(controller.ControllerError, match=message):
        controller.parse_current_pricing(html, region)


def test_oci_inspection_authenticates_repo_digest_revision_and_runtime(monkeypatch):
    image = "registry.example/graphforge@sha256:" + "b" * 64
    inspected = [
        {
            "Os": "linux",
            "Architecture": "amd64",
            "RepoDigests": [image],
            "Config": {
                "Labels": {
                    "org.opencontainers.image.revision": "a" * 40,
                    "dev.graphforge.fly-s20": "graphforge-fly-s20-runtime/1",
                }
            },
        }
    ]
    calls = []

    def run(command, **_kwargs):
        calls.append(command)
        if command[1:3] == ["image", "inspect"]:
            stdout = json.dumps(inspected)
        elif command[1:2] == ["create"]:
            stdout = "container-id\n"
        else:
            stdout = ""
        if command[1:2] == ["cp"]:
            Path(command[-1]).write_text(
                json.dumps(
                    {
                        "schema": "graphforge-fly-s20-build-provenance/1",
                        "source_sha": "a" * 40,
                        "source_snapshot_sha256": "c" * 64,
                    }
                )
            )
        return argparse.Namespace(stdout=stdout)

    monkeypatch.setattr(controller.subprocess, "run", run)
    monkeypatch.setattr(controller, "expected_source_snapshot", lambda: "c" * 64)
    assert controller.inspect_image(image, "a" * 40, "sha256:" + "b" * 64) == "c" * 64
    assert [call[1] for call in calls] == ["pull", "image", "create", "cp", "rm"]
    assert calls[0] == ["docker", "pull", "--platform", "linux/amd64", image]
    assert calls[2] == ["docker", "create", "--platform", "linux/amd64", image]


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"Architecture": "arm64"}, "linux/amd64"),
        ({"Os": "windows"}, "linux/amd64"),
        ({"RepoDigests": ["registry.example/other@sha256:" + "b" * 64]}, "repo digest"),
        (
            {
                "Config": {
                    "Labels": {
                        "org.opencontainers.image.revision": "c" * 40,
                        "dev.graphforge.fly-s20": "graphforge-fly-s20-runtime/1",
                    }
                }
            },
            "revision",
        ),
        (
            {
                "Config": {
                    "Labels": {
                        "org.opencontainers.image.revision": "a" * 40,
                        "dev.graphforge.fly-s20": "unknown",
                    }
                }
            },
            "runtime schema",
        ),
    ],
)
def test_oci_inspection_rejects_provenance_mismatch(monkeypatch, change, message):
    image = "registry.example/graphforge@sha256:" + "b" * 64
    inspected = {
        "Os": "linux",
        "Architecture": "amd64",
        "RepoDigests": [image],
        "Config": {
            "Labels": {
                "org.opencontainers.image.revision": "a" * 40,
                "dev.graphforge.fly-s20": "graphforge-fly-s20-runtime/1",
            }
        },
    }
    inspected.update(change)
    monkeypatch.setattr(
        controller.subprocess,
        "run",
        lambda command, **_kwargs: argparse.Namespace(
            stdout=json.dumps([inspected]) if command[1:3] == ["image", "inspect"] else ""
        ),
    )
    with pytest.raises(controller.ControllerError, match=message):
        controller.inspect_image(image, "a" * 40, "sha256:" + "b" * 64)


def test_private_registry_publication_targets_only_the_owned_app(monkeypatch, tmp_path):
    run = args(tmp_path)
    local_image_id = "sha256:" + "f" * 64
    pushed_digest = "sha256:" + "d" * 64
    docker_calls = []

    def docker(command, **_kwargs):
        docker_calls.append(command)
        output = f"digest: {pushed_digest}\n" if command[1:2] == ["push"] else ""
        return argparse.Namespace(returncode=0, stdout=output, stderr="")

    inspected = []

    def inspect(image, expected_sha, digest, environment):
        assert environment["DOCKER_CONFIG"]
        inspected.append((image, expected_sha, digest))
        return "c" * 64

    monkeypatch.setattr(controller.subprocess, "run", docker)
    monkeypatch.setattr(controller, "inspect_image", inspect)
    image, digest, snapshot = controller.publish_to_fly_registry(run, local_image_id, object())

    repository = "registry.fly.io/gf-s20-unique"
    tag = f"{repository}:{'a' * 40}"
    immutable = f"{repository}@{pushed_digest}"
    assert (image, digest, snapshot) == (immutable, pushed_digest, "c" * 64)
    assert docker_calls == [
        ["flyctl", "auth", "docker"],
        ["docker", "tag", local_image_id, tag],
        ["docker", "push", tag],
        ["docker", "manifest", "inspect", immutable],
    ]
    assert inspected == [(immutable, "a" * 40, pushed_digest)]


def test_private_registry_auth_failure_prevents_any_push(monkeypatch, tmp_path):
    docker_calls = []

    def docker(command, **_kwargs):
        docker_calls.append(command)
        raise controller.subprocess.CalledProcessError(1, command)

    monkeypatch.setattr(controller.subprocess, "run", docker)
    with pytest.raises(controller.subprocess.CalledProcessError):
        controller.publish_to_fly_registry(args(tmp_path), "sha256:" + "f" * 64, object())
    assert docker_calls == [["flyctl", "auth", "docker"]]


def test_local_preflight_resolves_tag_to_immutable_id_before_publication(monkeypatch):
    image_id = "sha256:" + "f" * 64
    calls = []

    def run(command, **_kwargs):
        calls.append(command)
        return argparse.Namespace(stdout=json.dumps([{"Id": image_id}]))

    monkeypatch.setattr(controller.subprocess, "run", run)
    monkeypatch.setattr(controller, "inspect_image", lambda image, sha: (image, sha)[0])
    assert controller.inspect_local_image("graphforge-fly-s20:exact", "a" * 40) == (
        image_id,
        image_id,
    )
    assert calls == [["docker", "image", "inspect", "graphforge-fly-s20:exact"]]


def test_ambiguous_app_create_claims_only_exact_target_org(tmp_path):
    class Fly:
        def run(self, *_args, **_kwargs):
            raise controller.subprocess.TimeoutExpired("flyctl", 120)

        def json(self, _command):
            return [{"name": "gf-s20-unique", "organization": {"slug": "other"}}]

    with pytest.raises(controller.ControllerError, match="target org"):
        controller.create_owned_app(args(tmp_path), Fly())


def test_post_create_cli_failure_is_marked_owned_for_cleanup(tmp_path):
    class Fly:
        def run(self, *_args, **_kwargs):
            raise controller.subprocess.CalledProcessError(1, "flyctl")

        def json(self, _command):
            return [{"name": "gf-s20-unique", "organization": {"slug": "curatelabs"}}]

    with pytest.raises(controller.OwnedAppCreationError, match="became observable"):
        controller.create_owned_app(args(tmp_path), Fly())


def test_reconciled_owned_app_is_cleaned_before_error_escapes(monkeypatch, tmp_path):
    class Fly:
        def json(self, command, **_kwargs):
            assert command == ["apps", "list"]
            return []

    observed = []
    monkeypatch.setattr(
        controller, "fetch_current_pricing", lambda _region: (Decimal("0"), Decimal("0"))
    )
    monkeypatch.setattr(controller, "reserve_budget", lambda *_args: None)
    monkeypatch.setattr(
        controller,
        "create_owned_app",
        lambda *_args: (_ for _ in ()).throw(controller.OwnedAppCreationError("ambiguous")),
    )
    monkeypatch.setattr(
        controller,
        "cleanup_owned",
        lambda _fly, app, machine, volume, owned: observed.append((app, machine, volume, owned)),
    )
    fly = Fly()
    stub_run_credential(monkeypatch, fly)
    run = args(tmp_path, execute=True, confirm_disposable=True)
    with pytest.raises(controller.OwnedAppCreationError):
        controller.execute(run, fly, "sha256:" + "f" * 64, "c" * 64)
    assert observed == [(run.app_name, None, None, True)]


def test_primary_failure_and_typed_diagnostic_survive_cleanup_failure(monkeypatch, tmp_path):
    class Fly:
        def json(self, command, **_kwargs):
            assert command == ["apps", "list"]
            return []

    monkeypatch.setattr(
        controller, "fetch_current_pricing", lambda _region: (Decimal("0"), Decimal("0"))
    )
    monkeypatch.setattr(controller, "reserve_budget", lambda *_args: None)
    monkeypatch.setattr(
        controller,
        "create_owned_app",
        lambda *_args: (_ for _ in ()).throw(controller.ControllerError("primary create failure")),
    )
    monkeypatch.setattr(
        controller,
        "cleanup_owned",
        lambda *_args: (_ for _ in ()).throw(controller.ControllerError("cleanup failure")),
    )
    fly = Fly()
    stub_run_credential(monkeypatch, fly)
    run = args(tmp_path, execute=True, confirm_disposable=True)
    with pytest.raises(controller.ControllerError, match="primary create failure") as captured:
        controller.execute(run, fly, "sha256:" + "f" * 64, "c" * 64)
    assert any("cleanup also failed" in note for note in captured.value.__notes__)
    assert json.loads(run.diagnostic_out.read_text()) == {
        "schema": "graphforge-fly-g500-s20-diagnostic/1",
        "status": "failure",
        "phase": "runner",
        "code": "controller_controllererror",
    }


def test_machine_mismatch_diagnostic_is_closed_boolean_bitmap(tmp_path):
    run = args(tmp_path)
    digest = "sha256:" + "d" * 64
    machine = {
        "region": "wrong",
        "image_ref": {"registry": "raw-provider-string", "repository": "wrong", "digest": "wrong"},
        "config": {},
    }
    checks = controller.machine_response_checks(machine, run, digest, "volume")
    controller.persist_controller_failure(
        run, controller.machine_assertion_code(checks), observed=checks
    )
    diagnostic = json.loads(run.diagnostic_out.read_text())
    assert diagnostic["code"] == "machine_image_identity_mismatch"
    assert set(diagnostic["observed_machine"]) == {
        "identity_match",
        "guest_match",
        "disposable_match",
        "private_match",
        "mount_match",
    }
    assert set(diagnostic["observed_machine"].values()) <= {True, False}
    assert "raw-provider-string" not in run.diagnostic_out.read_text()


def test_machine_invariants_use_fresh_get_not_create_echo(monkeypatch, tmp_path):
    machine_id = "machine-observed"
    fresh = {"id": machine_id, "region": "ord"}

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return json.dumps(fresh).encode()

    class Fly:
        def auth_token(self, **_kwargs):
            return "token"

    observed = []

    def urlopen(request, **_kwargs):
        observed.append(request.full_url)
        return Response()

    monkeypatch.setattr(controller.urllib.request, "urlopen", urlopen)
    run = args(tmp_path, app_name="gf-s20-owned")
    assert (
        controller.get_machine(run, Fly(), machine_id, deadline=controller.time.monotonic() + 120)
        == fresh
    )
    assert observed == ["https://api.machines.dev/v1/apps/gf-s20-owned/machines/machine-observed"]


def test_machine_provisioning_calls_share_remaining_outer_deadline(monkeypatch, tmp_path):
    timeouts = []
    auth_deadlines = []
    machine_id = "machine-observed"

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return json.dumps({"id": machine_id}).encode()

    class Fly:
        def auth_token(self, **kwargs):
            auth_deadlines.append(kwargs["deadline"])
            return "test-token"

    def urlopen(_request, **kwargs):
        timeouts.append(kwargs["timeout"])
        return Response()

    monkeypatch.setattr(controller.time, "monotonic", lambda: 100.0)
    monkeypatch.setattr(controller.urllib.request, "urlopen", urlopen)
    run = args(tmp_path)
    deadline = 107.5
    created = controller.create_machine(
        run,
        Fly(),
        "volume",
        "registry.fly.io/app@sha256:" + "d" * 64,
        "sha256:" + "d" * 64,
        "c" * 64,
        deadline=deadline,
    )
    assert created["id"] == machine_id
    assert controller.get_machine(run, Fly(), machine_id, deadline=deadline)["id"] == machine_id
    assert auth_deadlines == [deadline, deadline]
    assert timeouts == [7.5, 7.5]
    with pytest.raises(controller.ControllerError, match="deadline exhausted"):
        controller._bounded_timeout(99.0, 120)


def test_machine_guest_allows_harmless_provider_additive_fields(tmp_path):
    run = args(tmp_path)
    digest = "sha256:" + "d" * 64
    machine = {
        "region": run.region,
        "image_ref": {
            "registry": "registry.fly.io",
            "repository": run.app_name,
            "digest": digest,
        },
        "config": {
            "guest": {
                "cpu_kind": "performance",
                "cpus": 2,
                "memory_mb": 4096,
                "provider_added_metadata": "ignored",
            },
            "auto_destroy": True,
            "restart": {"policy": "no", "provider_added_metadata": True},
            "services": [],
            "mounts": [{"path": "/work", "volume": "volume", "name": "provider-added"}],
        },
    }
    checks = controller.machine_response_checks(machine, run, digest, "volume")
    assert all(checks.values())
    controller.assert_machine(machine, run, digest, "volume")


def test_inner_runtime_timeout_leaves_bounded_result_handoff():
    assert controller.RESULT_HANDOFF_SECONDS == 300
    assert controller.PROVISIONING_SECONDS == 300
    assert (
        controller.RUN_SECONDS - controller.RESULT_HANDOFF_SECONDS - controller.PROVISIONING_SECONDS
        == 13_800
    )
    assert controller.MONITOR_INTERVAL_SECONDS == 30
    assert controller.MONITOR_TRANSFER_TIMEOUT_SECONDS == 20
    assert controller.REGISTRY_TRANSFER_TIMEOUT_SECONDS == 1_800
    assert controller.RUN_SECONDS // controller.MONITOR_INTERVAL_SECONDS == 480


def test_runtime_status_envelope_is_closed_and_keeps_last_good_phase(tmp_path):
    status = tmp_path / "status.json"
    status.write_text(
        json.dumps(
            {
                "schema": "graphforge-fly-s20-status/1",
                "status": "running",
                "phase": "ingest",
            }
        )
    )
    assert controller.observed_status(status) == {"status": "running", "phase": "ingest"}
    leaked = json.loads(status.read_text())
    leaked["machine_id"] = "provider-secret"
    status.write_text(json.dumps(leaked))
    with pytest.raises(controller.ControllerError, match="unknown fields"):
        controller.observed_status(status)


def test_monitor_transfer_timeout_is_capped_by_shared_absolute_deadline(monkeypatch, tmp_path):
    observed = []

    class Fly:
        def run(self, _command, **kwargs):
            observed.append(kwargs["timeout"])
            return argparse.Namespace(returncode=1)

    monkeypatch.setattr(controller.time, "monotonic", lambda: 100.0)
    controller.fetch(
        Fly(),
        args(tmp_path),
        "machine",
        controller.ACTIVE_PHASE_PATH,
        tmp_path / "status.json",
        deadline=107.5,
    )
    assert observed == [7.5]


def test_optional_ack_near_deadline_cannot_turn_validated_success_into_failure(
    monkeypatch, tmp_path
):
    class Fly:
        def run(self, *_args, **_kwargs):
            raise AssertionError("near-deadline acknowledgement must be skipped")

    monkeypatch.setattr(controller.time, "monotonic", lambda: 100.0)
    assert controller.acknowledge_result(Fly(), args(tmp_path), "machine", 100.5) is False


def test_optional_ack_timeout_is_bounded_and_nonfatal(monkeypatch, tmp_path):
    class Fly:
        def run(self, command, **kwargs):
            assert command[:2] == ["machine", "exec"]
            assert kwargs["timeout"] == 5.0
            raise controller.subprocess.TimeoutExpired(command, kwargs["timeout"])

    monkeypatch.setattr(controller.time, "monotonic", lambda: 100.0)
    assert controller.acknowledge_result(Fly(), args(tmp_path), "machine", 105.0) is False


def test_private_registry_push_failure_stops_before_manifest_or_inspection(monkeypatch, tmp_path):
    docker_calls = []

    def docker(command, **_kwargs):
        docker_calls.append(command)
        if command[1:2] == ["push"]:
            raise controller.subprocess.CalledProcessError(
                1, command, stderr="denied: secret-token gf-s20-unique"
            )
        return argparse.Namespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(controller.subprocess, "run", docker)
    monkeypatch.setattr(
        controller,
        "inspect_image",
        lambda *_args: (_ for _ in ()).throw(AssertionError("must not inspect a failed push")),
    )
    run = args(tmp_path)
    with pytest.raises(controller.ControllerError, match="registry push failed"):
        controller.publish_to_fly_registry(run, "sha256:" + "f" * 64, object())
    assert [command[:2] for command in docker_calls] == [
        ["flyctl", "auth"],
        ["docker", "tag"],
        ["docker", "push"],
    ]
    diagnostic_text = run.diagnostic_out.read_text()
    diagnostic = json.loads(diagnostic_text)
    assert diagnostic["code"] == "registry_push_failed"
    assert diagnostic["command_failure"]["operation"] == "docker_push"
    assert diagnostic["command_failure"]["outcome"] == "nonzero_exit"
    assert diagnostic["command_failure"]["exit_code"] == 1
    assert diagnostic["command_failure"]["elapsed_seconds"] in (0, 1)
    assert diagnostic["command_failure"]["timeout_seconds"] == 1800
    assert diagnostic["command_failure"]["stderr_class"] == "permission_rejected"
    assert diagnostic["command_failure"]["stderr_sha256"].startswith("sha256:")
    assert "secret-token" not in diagnostic_text
    assert "gf-s20-unique" not in diagnostic_text


def test_slow_registry_push_below_bound_is_not_misclassified_as_timeout(monkeypatch, tmp_path):
    pushed_digest = "sha256:" + "d" * 64
    push_timeout = []

    def docker(command, **kwargs):
        if command[1:2] == ["push"]:
            push_timeout.append(kwargs["timeout"])
            return argparse.Namespace(returncode=0, stdout=f"digest: {pushed_digest}\n", stderr="")
        return argparse.Namespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(controller.subprocess, "run", docker)
    monkeypatch.setattr(controller, "inspect_image", lambda *_args: "c" * 64)
    run = args(tmp_path)
    assert (
        controller.publish_to_fly_registry(run, "sha256:" + "f" * 64, object())[1] == pushed_digest
    )
    assert push_timeout == [1800]
    assert not run.diagnostic_out.exists()


def test_stalled_registry_push_has_distinct_bounded_timeout_diagnostic(monkeypatch, tmp_path):
    def docker(command, **_kwargs):
        if command[1:2] == ["push"]:
            raise controller.subprocess.TimeoutExpired(
                command, 1800, stderr=b"network timeout secret-token"
            )
        return argparse.Namespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(controller.subprocess, "run", docker)
    run = args(tmp_path)
    with pytest.raises(controller.ControllerError, match="bounded timeout"):
        controller.publish_to_fly_registry(run, "sha256:" + "f" * 64, object())
    diagnostic_text = run.diagnostic_out.read_text()
    diagnostic = json.loads(diagnostic_text)
    assert diagnostic["code"] == "registry_push_timeout"
    assert diagnostic["command_failure"]["outcome"] == "timeout"
    assert diagnostic["command_failure"]["exit_code"] is None
    assert diagnostic["command_failure"]["timeout_seconds"] == 1800
    assert diagnostic["command_failure"]["stderr_class"] == "transport_timeout"
    assert "secret-token" not in diagnostic_text


def test_registry_rejection_in_stdout_is_classified_and_hashed_without_leak(monkeypatch, tmp_path):
    def docker(command, **_kwargs):
        if command[1:2] == ["push"]:
            raise controller.subprocess.CalledProcessError(
                1,
                command,
                output="unauthorized private-token gf-s20-private-name",
                stderr="",
            )
        return argparse.Namespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(controller.subprocess, "run", docker)
    run = args(tmp_path)
    with pytest.raises(controller.ControllerError, match="registry push failed"):
        controller.publish_to_fly_registry(run, "sha256:" + "f" * 64, object())
    text = run.diagnostic_out.read_text()
    diagnostic = json.loads(text)
    details = diagnostic["command_failure"]
    assert details["stderr_class"] == "authentication_rejected"
    assert details["stdout_sha256"].startswith("sha256:")
    assert "private-token" not in text
    assert "gf-s20-private-name" not in text


def test_private_registry_digest_mismatch_fails_closed(monkeypatch, tmp_path):
    pushed_digest = "sha256:" + "d" * 64

    def docker(command, **_kwargs):
        output = f"digest: {pushed_digest}\n" if command[1:2] == ["push"] else ""
        return argparse.Namespace(returncode=0, stdout=output, stderr="")

    monkeypatch.setattr(controller.subprocess, "run", docker)
    monkeypatch.setattr(
        controller,
        "inspect_image",
        lambda *_args: (_ for _ in ()).throw(
            controller.ControllerError(
                "pulled OCI image does not authenticate the requested repo digest"
            )
        ),
    )
    with pytest.raises(controller.ControllerError, match="repo digest"):
        controller.publish_to_fly_registry(args(tmp_path), "sha256:" + "f" * 64, object())


def test_failed_private_image_publication_cleans_app_before_volume_or_machine(
    monkeypatch, tmp_path
):
    calls = []

    class Fly:
        def json(self, command, **_kwargs):
            calls.append(command)
            if command == ["apps", "list"]:
                return []
            raise AssertionError(f"unexpected provider read: {command}")

        def run(self, command, check=True, **_kwargs):
            calls.append(command)
            return argparse.Namespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(
        controller, "fetch_current_pricing", lambda _region: (Decimal("0"), Decimal("0"))
    )
    monkeypatch.setattr(controller, "reserve_budget", lambda *_args: None)
    monkeypatch.setattr(
        controller,
        "publish_to_fly_registry",
        lambda *_args: (_ for _ in ()).throw(controller.ControllerError("registry push failed")),
    )
    fly = Fly()
    stub_run_credential(monkeypatch, fly)
    run = args(tmp_path, execute=True, confirm_disposable=True)
    with pytest.raises(controller.ControllerError, match="registry push failed"):
        controller.execute(run, fly, "sha256:" + "b" * 64, "c" * 64)

    assert ["apps", "create", run.app_name, "--org", run.org] in calls
    assert ["apps", "destroy", run.app_name, "--yes"] in calls
    assert not any(command[:2] == ["volumes", "create"] for command in calls)
    assert not any(command[:2] == ["machine", "create"] for command in calls)


def test_existing_app_is_refused_before_budget_or_creation(tmp_path):
    class ExistingFly:
        def json(self, command, **_kwargs):
            assert command == ["apps", "list"]
            return [{"name": "gf-s20-unique"}]

        def run(self, *_args, **_kwargs):
            raise AssertionError("must not mutate provider state")

    run = args(tmp_path, execute=True, confirm_disposable=True)
    with pytest.raises(controller.ControllerError, match="existing app"):
        controller.execute(run, ExistingFly(), "sha256:" + "b" * 64, "c" * 64)
    assert not run.ledger.exists()


def test_cleanup_only_uses_observed_owned_identifiers():
    calls = []

    class Fly:
        def run(self, command, check=True, **_kwargs):
            calls.append((command, check))
            return argparse.Namespace(returncode=1 if command[1] in {"status", "show"} else 0)

        def json(self, command, **_kwargs):
            calls.append((command, True))
            return []

        def resource_absent(self, kind, app, resource_id, **_kwargs):
            calls.append((["provider", kind, resource_id, app], True))
            return True

    controller.cleanup_owned(Fly(), "gf-s20-unique", None, None, False)
    assert calls == []
    controller.cleanup_owned(Fly(), "gf-s20-unique", "machine-observed", "volume-observed", True)
    assert [call[0][0:2] for call in calls[:6]] == [
        ["machine", "destroy"],
        ["provider", "machines"],
        ["volumes", "destroy"],
        ["provider", "volumes"],
        ["apps", "destroy"],
        ["apps", "list"],
    ]
    assert [call[0][0:2] for call in calls[6:]] == [
        ["provider", "machines"],
        ["provider", "volumes"],
        ["apps", "list"],
    ]


def test_cleanup_converges_child_first_across_async_deletion(monkeypatch):
    monkeypatch.setattr(controller.time, "sleep", lambda _seconds: None)
    calls = []
    probes = {"machines": 0, "volumes": 0}

    class Fly:
        def run(self, command, check=True, **_kwargs):
            calls.append(command[:2])
            if command[1] in {"status", "show"}:
                probes[command[0]] += 1
                return argparse.Namespace(returncode=0 if probes[command[0]] == 1 else 1)
            return argparse.Namespace(returncode=0)

        def json(self, command, **_kwargs):
            calls.append(command[:2])
            return []

        def resource_absent(self, kind, _app, _resource_id, **_kwargs):
            calls.append(["provider", kind])
            probes[kind] += 1
            return probes[kind] > 1

    controller.cleanup_owned(Fly(), "gf-s20-unique", "machine", "volume", True)
    volume_destroy = calls.index(["volumes", "destroy"])
    app_destroy = calls.index(["apps", "destroy"])
    assert volume_destroy > calls.index(["machine", "destroy"])
    assert app_destroy > volume_destroy
    assert calls[-3:] == [
        ["provider", "machines"],
        ["provider", "volumes"],
        ["apps", "list"],
    ]


def test_cleanup_survivor_fails_after_bounded_retries(monkeypatch):
    monkeypatch.setattr(controller, "CLEANUP_ATTEMPTS", 2)
    monkeypatch.setattr(controller.time, "sleep", lambda _seconds: None)

    class Fly:
        def run(self, command, check=True, **_kwargs):
            return argparse.Namespace(returncode=0)

        def resource_absent(self, *_args, **_kwargs):
            return False

    with pytest.raises(controller.ControllerError, match="bounded cleanup"):
        controller.cleanup_owned(Fly(), "gf-s20-unique", "machine", None, False)


def test_cleanup_attempts_all_resources_and_aggregates_survivors(monkeypatch):
    monkeypatch.setattr(controller, "CLEANUP_ATTEMPTS", 2)
    monkeypatch.setattr(controller.time, "sleep", lambda _seconds: None)
    calls = []

    class Fly:
        def run(self, command, check=True, **_kwargs):
            calls.append(command[:2])
            return argparse.Namespace(returncode=0)

        def json(self, command, **_kwargs):
            calls.append(command[:2])
            return [{"name": "gf-s20-unique"}]

        def resource_absent(self, kind, _app, _resource_id, **_kwargs):
            calls.append(["provider", kind])
            return False

    with pytest.raises(controller.ControllerError, match=r"Machine.*volume.*app"):
        controller.cleanup_owned(Fly(), "gf-s20-unique", "machine", "volume", True)
    assert ["machine", "destroy"] in calls
    assert ["volumes", "destroy"] in calls
    assert ["apps", "destroy"] in calls


def test_provider_absence_requires_authenticated_http_404(monkeypatch):
    fly = controller.Flyctl()
    monkeypatch.setattr(
        fly,
        "run",
        lambda *_args, **_kwargs: argparse.Namespace(stdout="token\n"),
    )

    def http_error(code):
        def raise_error(*_args, **_kwargs):
            raise controller.urllib.error.HTTPError("url", code, "failure", None, None)

        return raise_error

    monkeypatch.setattr(controller.urllib.request, "urlopen", http_error(404))
    assert fly.resource_absent(
        "machines", "app", "machine", deadline=controller.time.monotonic() + 3
    )
    monkeypatch.setattr(controller.urllib.request, "urlopen", http_error(401))
    with pytest.raises(controller.ProviderRequestError, match="authentication_invalid"):
        fly.resource_absent("machines", "app", "machine", deadline=controller.time.monotonic() + 3)


def test_provider_probe_recomputes_timeout_between_token_and_http(monkeypatch):
    fly = controller.Flyctl()
    clock = [0.0]
    observed = []
    monkeypatch.setattr(controller.time, "monotonic", lambda: clock[0])

    def token(*_args, **kwargs):
        observed.append(kwargs["timeout"])
        clock[0] += kwargs["timeout"]
        return argparse.Namespace(stdout="token\n")

    def not_found(*_args, **kwargs):
        observed.append(kwargs["timeout"])
        raise controller.urllib.error.HTTPError("url", 404, "missing", None, None)

    monkeypatch.setattr(fly, "run", token)
    monkeypatch.setattr(controller.urllib.request, "urlopen", not_found)
    assert fly.resource_absent("machines", "app", "machine", deadline=50)
    assert observed == [30, 20]


def test_cleanup_caps_every_call_inside_one_reservation_deadline(monkeypatch):
    monkeypatch.setattr(controller, "CLEANUP_RESERVE_SECONDS", 100)
    monkeypatch.setattr(controller, "CLEANUP_ATTEMPTS", 1)
    clock = [0.0]
    monkeypatch.setattr(controller.time, "monotonic", lambda: clock[0])
    timeouts = []
    calls = []

    class Fly:
        def run(self, command, **kwargs):
            calls.append(command[:2])
            timeouts.append(kwargs["timeout"])
            clock[0] += kwargs["timeout"]
            return argparse.Namespace(returncode=0)

        def json(self, command, **kwargs):
            calls.append(command[:2])
            timeouts.append(kwargs["timeout"])
            clock[0] += kwargs["timeout"]
            return [{"name": "gf-s20-unique"}]

        def resource_absent(self, *_args, **kwargs):
            calls.append(["provider", _args[0]])
            clock[0] = kwargs["deadline"]
            return False

    with pytest.raises(controller.ControllerError):
        controller.cleanup_owned(Fly(), "gf-s20-unique", "machine", "volume", True)
    assert timeouts
    assert all(0 < timeout <= 30 for timeout in timeouts)
    assert clock[0] <= 100
    assert ["machine", "destroy"] in calls
    assert ["volumes", "destroy"] in calls
    assert ["apps", "destroy"] in calls


def test_post_cleanup_absence_is_verified_child_to_parent():
    calls = []

    class Fly:
        def json(self, command, **_kwargs):
            calls.append(command)
            return []

        def resource_absent(self, kind, app, resource_id, **_kwargs):
            calls.append(["provider", kind, resource_id, app])
            return True

    controller.verify_absent(Fly(), "gf-s20-unique", "machine-observed", "volume-observed", True)
    assert calls == [
        ["provider", "machines", "machine-observed", "gf-s20-unique"],
        ["provider", "volumes", "volume-observed", "gf-s20-unique"],
        ["apps", "list"],
    ]


def test_post_cleanup_detects_each_surviving_owned_resource():
    class Fly:
        def __init__(self, survivor):
            self.survivor = survivor

        def resource_absent(self, kind, _app, _resource_id, **_kwargs):
            return kind != self.survivor

        def json(self, _command, **_kwargs):
            return [{"name": "gf-s20-unique"}] if self.survivor == "app" else []

    for survivor, message in (("machines", "Machine"), ("volumes", "volume"), ("app", "app")):
        with pytest.raises(controller.ControllerError, match=message):
            controller.verify_absent(
                Fly(survivor), "gf-s20-unique", "machine-observed", "volume-observed", True
            )


def test_container_result_is_closed_typed_and_sanitized():
    assert controller.validate_container_result({"status": "success"}) == {
        "schema": "graphforge-fly-g500-s20-diagnostic/1",
        "status": "success",
    }
    failure = controller.validate_container_result(
        {"status": "failure", "phase": "ingest", "code": "GF_RESOURCE_EXHAUSTED"}
    )
    assert failure["phase"] == "ingest"
    with pytest.raises(controller.ControllerError, match="unknown fields"):
        controller.validate_container_result(
            {"status": "failure", "phase": "ingest", "code": "failed", "path": "/secret"}
        )
    with pytest.raises(controller.ControllerError, match="invalid code"):
        controller.validate_container_result(
            {"status": "failure", "phase": "ingest", "code": "token=secret"}
        )


def test_fetch_binds_only_declared_runtime_paths(tmp_path):
    calls = []

    class Fly:
        def run(self, command, check=True, **_kwargs):
            calls.append(command)
            return argparse.Namespace(returncode=1)

    run = args(tmp_path)
    controller.fetch(Fly(), run, "machine-observed", controller.RESULT_PATH, tmp_path / "r")
    controller.fetch(Fly(), run, "machine-observed", controller.EVIDENCE_PATH, tmp_path / "e")
    assert [call[3] for call in calls] == ["/work/container-result.json", "/work/s20-evidence.json"]


def test_runtime_preserves_allowlisted_internal_failure_phase():
    entrypoint = (ROOT / "containers/fly-g500-s20/run-s20.sh").read_text()
    assert "GF_G500_S20_ACTIVE_PHASE_OUT=/work/s20-active-phase.json" in entrypoint
    for phase in controller.PHASES - {"runner"}:
        assert phase in entrypoint
    assert "${GF_G500_S20_TIMEOUT_SECONDS}s" in entrypoint
    assert "14400s" not in entrypoint
    assert '"phase":"%s","code":"process_exit_%s"' in entrypoint


def test_terminal_failure_is_persisted_promptly_and_still_verified_cleaned(tmp_path, monkeypatch):
    digest = "sha256:" + "b" * 64
    machine = {
        "id": "machine-observed",
        "region": "den",
        "image_ref": {
            "registry": "registry.fly.io",
            "repository": "gf-s20-unique",
            "digest": digest,
        },
        "config": {
            "guest": {"cpu_kind": "performance", "cpus": 2, "memory_mb": 4096},
            "auto_destroy": True,
            "restart": {"policy": "no"},
            "services": [],
            "mounts": [{"path": "/work", "volume": "volume-observed"}],
        },
    }
    monkeypatch.setattr(controller, "create_machine", lambda *_args, **_kwargs: machine)
    monkeypatch.setattr(controller, "get_machine", lambda *_args, **_kwargs: machine)
    monkeypatch.setattr(
        controller,
        "publish_to_fly_registry",
        lambda *_args: ("registry.fly.io/gf-s20-unique@" + digest, digest, "c" * 64),
    )
    monkeypatch.setattr(
        controller,
        "fetch_current_pricing",
        lambda _region: (Decimal("0.00002484"), Decimal("0.15")),
    )

    class Fly:
        def __init__(self):
            self.app_lists = 0

        def json(self, command, **_kwargs):
            if command[:2] == ["apps", "list"]:
                self.app_lists += 1
                return []
            if command[:2] == ["volumes", "create"]:
                return {"id": "volume-observed", "region": "den", "size_gb": 500}
            raise AssertionError(command)

        def run(self, command, check=True, **_kwargs):
            if command[:3] == ["ssh", "sftp", "get"]:
                if command[3] == controller.ACTIVE_PHASE_PATH:
                    Path(command[4]).write_text(
                        json.dumps(
                            {
                                "schema": "graphforge-fly-s20-status/1",
                                "status": "failure",
                                "phase": "ingest",
                                "code": "GF_OOM",
                            }
                        )
                    )
                elif command[3] == controller.RESULT_PATH:
                    Path(command[4]).write_text(
                        json.dumps({"status": "failure", "phase": "ingest", "code": "GF_OOM"})
                    )
                return argparse.Namespace(returncode=0)
            if command[:2] in (["machine", "status"], ["volumes", "show"]):
                return argparse.Namespace(returncode=1)
            return argparse.Namespace(returncode=0)

        def resource_absent(self, *_args, **_kwargs):
            return True

    run = args(tmp_path, execute=True, confirm_disposable=True)
    fly = Fly()
    stub_run_credential(monkeypatch, fly)
    with pytest.raises(controller.ControllerError, match="GF_OOM"):
        controller.execute(run, fly, digest, "c" * 64)
    assert json.loads(run.diagnostic_out.read_text()) == {
        "schema": "graphforge-fly-g500-s20-diagnostic/1",
        "status": "failure",
        "phase": "ingest",
        "code": "GF_OOM",
    }
    assert fly.app_lists == 3


def test_observed_machine_must_match_private_fixed_plan(tmp_path):
    digest = "sha256:" + "b" * 64
    machine = {
        "region": "den",
        "image_ref": {
            "registry": "registry.fly.io",
            "repository": "gf-s20-unique",
            "digest": digest,
        },
        "config": {
            "guest": {"cpu_kind": "performance", "cpus": 2, "memory_mb": 4096},
            "auto_destroy": True,
            "restart": {"policy": "no"},
            "services": [],
            "mounts": [{"path": "/work", "volume": "volume-observed"}],
        },
    }
    controller.assert_machine(machine, args(tmp_path), digest, "volume-observed")
    machine["config"]["services"] = [{"ports": [443]}]
    with pytest.raises(controller.ControllerError, match="service"):
        controller.assert_machine(machine, args(tmp_path), digest, "volume-observed")


def test_observed_volume_and_machine_mount_are_exact(tmp_path):
    run = args(tmp_path)
    assert (
        controller.assert_volume({"id": "volume-observed", "region": "den", "size_gb": 500}, run)
        == "volume-observed"
    )
    with pytest.raises(controller.ControllerError, match="region/size"):
        controller.assert_volume({"id": "volume-observed", "region": "ord", "size_gb": 500}, run)


def test_terminal_machine_oom_uses_last_allowlisted_phase(tmp_path):
    active = tmp_path / "active.json"
    active.write_text(json.dumps({"phase": "ingest", "private": "/secret"}))
    assert controller.observed_phase(active) == "ingest"
    assert controller.terminal_machine_diagnostic(
        controller.normalize_machine_runtime(
            {
                "state": "stopped",
                "events": [{"request": {"exit_event": {"oom_killed": True}}}],
            }
        ),
        "ingest",
    ) == {
        "schema": "graphforge-fly-g500-s20-diagnostic/1",
        "status": "failure",
        "phase": "ingest",
        "code": "machine_oom",
    }


def test_machine_runtime_uses_authenticated_api_and_closed_projection(monkeypatch):
    observed = []

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return json.dumps(
                {
                    "id": "provider-id",
                    "state": "stopped",
                    "private": "must-not-leak",
                    "events": [
                        {
                            "type": "exit",
                            "request": {"exit_event": {"oom_killed": True}},
                        }
                    ],
                }
            ).encode()

    def urlopen(request, **kwargs):
        observed.append((request.full_url, request.headers, kwargs))
        return Response()

    monkeypatch.setattr(controller.urllib.request, "urlopen", urlopen)
    fly = controller.Flyctl()
    fly._cached_auth_token = "token"
    fly._cached_auth_token_at = controller.time.monotonic()
    assert fly.machine_runtime("gf-s20-owned", "machine-observed") == {
        "state": "stopped",
        "oom": True,
    }
    assert observed[0][0].endswith("/apps/gf-s20-owned/machines/machine-observed")
    assert observed[0][1]["Authorization"] == "Bearer token"
    assert observed[0][2]["timeout"] == 30


def test_machine_runtime_oom_uses_only_official_boolean_fields():
    assert (
        controller.normalize_machine_runtime(
            {
                "state": "stopped",
                "events": [{"request": {"MonitorEvent": {"exit_event": {"oom_killed": True}}}}],
            }
        )["oom"]
        is True
    )
    assert (
        controller.normalize_machine_runtime(
            {
                "state": "stopped",
                "events": [
                    {
                        "type": "exit",
                        "secret": "out of memory",
                        "request": {"exit_event": {"oom_killed": False}},
                    }
                ],
            }
        )["oom"]
        is False
    )
    for unsupported in ("oom", "out_of_memory"):
        assert (
            controller.normalize_machine_runtime(
                {"state": "stopped", "events": [{"type": unsupported}]}
            )["oom"]
            is False
        )


def test_machine_runtime_normalizes_provider_404_to_destroyed(monkeypatch):
    def missing(*_args, **_kwargs):
        raise urllib.error.HTTPError("url", 404, "not found", {}, None)

    monkeypatch.setattr(controller.urllib.request, "urlopen", missing)
    fly = controller.Flyctl()
    fly._cached_auth_token = "token"
    fly._cached_auth_token_at = controller.time.monotonic()
    assert fly.machine_runtime("gf-s20-owned", "machine-observed") == {
        "state": "destroyed",
        "oom": False,
    }


def test_bootstrap_api_refreshes_expired_token_once_on_safe_get(monkeypatch):
    tokens = []
    responses = [401, {"state": "started"}]

    class Response:
        def __init__(self, value):
            self.value = value

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return json.dumps(self.value).encode()

    def urlopen(request, **_kwargs):
        tokens.append(request.headers["Authorization"])
        value = responses.pop(0)
        if value == 401:
            raise urllib.error.HTTPError("url", 401, "unauthorized", {}, None)
        return Response(value)

    fly = controller.Flyctl()
    issued = iter(("expired-test-token", "fresh-test-token"))
    monkeypatch.setattr(
        fly,
        "auth_token",
        lambda **_kwargs: next(issued),
    )
    monkeypatch.setattr(controller.urllib.request, "urlopen", urlopen)
    assert fly.api_json("GET", "/v1/apps/app/machines/id", operation="machine_runtime_get") == {
        "state": "started"
    }
    assert tokens == ["Bearer expired-test-token", "Bearer fresh-test-token"]


def test_cleanup_forces_credential_refresh_before_teardown_and_final_proof(monkeypatch):
    refreshes = []
    fly = controller.Flyctl()
    monkeypatch.setattr(
        fly,
        "auth_token",
        lambda **kwargs: refreshes.append(kwargs.get("force_refresh")) or "test-token",
    )
    monkeypatch.setattr(fly, "json", lambda *_args, **_kwargs: [])
    controller.cleanup_owned(fly, "gf-s20-owned", None, None, True)
    assert refreshes == [True, True]


def test_failed_teardown_token_refresh_does_not_skip_any_destroy(monkeypatch):
    calls = []

    class Fly(controller.Flyctl):
        def auth_token(self, **_kwargs):
            raise controller.ControllerError("test credential failure")

        def run(self, command, **_kwargs):
            calls.append(command)
            return argparse.Namespace(returncode=0)

        def resource_absent(self, *_args, **_kwargs):
            return True

        def json(self, command, **_kwargs):
            calls.append(command)
            return []

    monkeypatch.setattr(controller, "CLEANUP_ATTEMPTS", 1)
    with pytest.raises(controller.ControllerError, match="credential refresh"):
        controller.cleanup_owned(Fly(), "gf-s20-owned", "machine-observed", "volume-observed", True)
    assert [command[:2] for command in calls if command[1] == "destroy"] == [
        ["machine", "destroy"],
        ["volumes", "destroy"],
        ["apps", "destroy"],
    ]


def test_attempt_two_regression_monitor_never_uses_machine_status_cli():
    calls = []

    class Fly:
        def machine_runtime(self, app, machine_id):
            calls.append((app, machine_id))
            return {"state": "stopped", "oom": False}

        def json(self, command, **_kwargs):
            assert command[:2] != ["machine", "status"]
            return []

    diagnostic = controller.terminal_machine_diagnostic(
        Fly().machine_runtime("gf-s20-owned", "machine-observed"), "runner"
    )
    assert diagnostic == {
        "schema": "graphforge-fly-g500-s20-diagnostic/1",
        "status": "failure",
        "phase": "runner",
        "code": "machine_exit",
    }
    assert calls == [("gf-s20-owned", "machine-observed")]


@pytest.mark.skipif(shutil.which("flyctl") is None, reason="flyctl is not installed")
@pytest.mark.parametrize(
    ("command", "required_flags"),
    [
        (("apps", "create"), ("--org",)),
        (("apps", "list"), ("--json",)),
        (
            ("volumes", "create"),
            ("--app", "--region", "--size", "--scheduled-snapshots", "--yes", "--json"),
        ),
        (("machine", "destroy"), ("--app", "--force")),
        (("volumes", "destroy"), ("--app", "--yes")),
        (("apps", "destroy"), ("--yes",)),
        (("auth", "docker"), ()),
        (("auth", "token"), ()),
        (("ssh", "sftp", "get"), ("--app", "--machine")),
        (("machine", "exec"), ("--app",)),
    ],
)
def test_installed_flyctl_supports_every_controller_cli_flag(command, required_flags):
    result = subprocess.run(
        ["flyctl", *command, "--help"], text=True, capture_output=True, check=False
    )
    assert result.returncode == 0, result.stderr
    output = result.stdout + result.stderr
    for flag in required_flags:
        assert flag in output


def test_closed_evidence_accepts_only_pinned_sanitized_s20():
    validator.validate(evidence(), "a" * 40, "sha256:" + "b" * 64, "den")
    with pytest.raises(validator.EvidenceError, match="schema violation"):
        validator.validate(
            evidence(machine_id="provider-secret-id"), "a" * 40, "sha256:" + "b" * 64, "den"
        )
    with pytest.raises(validator.EvidenceError, match="identity"):
        validator.validate(evidence(region="ord"), "a" * 40, "sha256:" + "b" * 64, "den")


def test_closed_evidence_rejects_deleted_falsified_and_nested_unknown_proof():
    missing = evidence()
    del missing["lifecycle"]["package_digest"]
    with pytest.raises(validator.EvidenceError, match="schema violation"):
        validator.validate(missing, "a" * 40, "sha256:" + "b" * 64, "den")
    falsified = evidence()
    falsified["lifecycle"]["imported_one_hop"]["fingerprint"] = "sha256:" + "f" * 64
    with pytest.raises(validator.EvidenceError, match="one_hop fingerprints"):
        validator.validate(falsified, "a" * 40, "sha256:" + "b" * 64, "den")
    leaked = copy.deepcopy(evidence())
    leaked["lifecycle"]["source_one_hop"]["evidence"]["provider_reference"] = "opaque"
    with pytest.raises(validator.EvidenceError, match="schema violation"):
        validator.validate(leaked, "a" * 40, "sha256:" + "b" * 64, "den")
    unclassified = evidence()
    unclassified["lifecycle"]["source_storage"]["categories"]["other"] = {
        "logical_references": 1,
        "logical_bytes": 1,
        "physical_objects": 1,
        "physical_logical_bytes": 1,
        "allocated_bytes": 4096,
    }
    with pytest.raises(validator.EvidenceError, match=r"does not reconcile|unclassified"):
        validator.validate(unclassified, "a" * 40, "sha256:" + "b" * 64, "den")


@pytest.mark.parametrize(
    "field",
    [
        "source_export_generation_authenticated",
        "import_receipt_reopen_authenticated",
        "source_import_generations_distinct",
    ],
)
def test_generation_proof_is_required_true_and_closed(field):
    missing = evidence()
    del missing["lifecycle"][field]
    with pytest.raises(validator.EvidenceError, match="schema violation"):
        validator.validate(missing, "a" * 40, "sha256:" + "b" * 64, "den")

    falsified = evidence()
    falsified["lifecycle"][field] = False
    with pytest.raises(validator.EvidenceError, match="schema violation"):
        validator.validate(falsified, "a" * 40, "sha256:" + "b" * 64, "den")

    unknown = evidence()
    unknown["lifecycle"][f"{field}_detail"] = "opaque"
    with pytest.raises(validator.EvidenceError, match="schema violation"):
        validator.validate(unknown, "a" * 40, "sha256:" + "b" * 64, "den")


@pytest.mark.parametrize("location", ["lifecycle", "source_storage", "imported_storage"])
def test_raw_generation_uuid_is_forbidden_everywhere(location):
    leaked = evidence()
    target = leaked["lifecycle"] if location == "lifecycle" else leaked["lifecycle"][location]
    target["generation_uuid"] = "00000000-0000-4000-8000-000000000001"
    with pytest.raises(validator.EvidenceError, match="schema violation"):
        validator.validate(leaked, "a" * 40, "sha256:" + "b" * 64, "den")


def test_durable_lifecycle_order_is_required_closed_and_immutable():
    missing = evidence()
    del missing["lifecycle"]["durable_steps"]
    with pytest.raises(validator.EvidenceError, match="schema violation"):
        validator.validate(missing, "a" * 40, "sha256:" + "b" * 64, "den")

    reordered = evidence()
    reordered["lifecycle"]["durable_steps"][3:5] = reversed(
        reordered["lifecycle"]["durable_steps"][3:5]
    )
    with pytest.raises(validator.EvidenceError, match="schema violation"):
        validator.validate(reordered, "a" * 40, "sha256:" + "b" * 64, "den")

    unknown = evidence()
    unknown["lifecycle"]["publication"]["receipt_path"] = "/private/work"
    with pytest.raises(validator.EvidenceError, match="schema violation"):
        validator.validate(unknown, "a" * 40, "sha256:" + "b" * 64, "den")


@pytest.mark.parametrize("location", ["construction", "publication"])
def test_one_publication_recovery_proof_rejects_deletion_and_falsification(location):
    def target(value):
        if location == "construction":
            return value["rung"]["construction"]
        return value["lifecycle"]["publication"]

    missing = evidence()
    del target(missing)["recovery_replay"]
    with pytest.raises(validator.EvidenceError, match="schema violation"):
        validator.validate(missing, "a" * 40, "sha256:" + "b" * 64, "den")

    falsified = evidence()
    target(falsified)["recovered_generation_sha256"] = "sha256:" + "8" * 64
    with pytest.raises(validator.EvidenceError, match="generation identity differs"):
        validator.validate(falsified, "a" * 40, "sha256:" + "b" * 64, "den")


def test_ingest_windows_recompute_growth_headroom_and_plateau():
    missing = evidence()
    del missing["ingest_memory_windows"]["middle_rss_peak_bytes"]
    with pytest.raises(validator.EvidenceError, match="schema violation"):
        validator.validate(missing, "a" * 40, "sha256:" + "b" * 64, "den")

    wrong_growth = evidence()
    wrong_growth["ingest_memory_windows"]["observed_growth_bytes"] -= 1
    with pytest.raises(validator.EvidenceError, match="observed growth"):
        validator.validate(wrong_growth, "a" * 40, "sha256:" + "b" * 64, "den")

    wrong_headroom = evidence()
    wrong_headroom["ingest_memory_windows"]["headroom_bytes"] -= 1
    with pytest.raises(validator.EvidenceError, match="headroom"):
        validator.validate(wrong_headroom, "a" * 40, "sha256:" + "b" * 64, "den")

    unexplained_allowance = evidence()
    unexplained_allowance["ingest_memory_windows"]["allowed_growth_bytes"] = 1
    with pytest.raises(validator.EvidenceError, match="working set plus tolerance"):
        validator.validate(unexplained_allowance, "a" * 40, "sha256:" + "b" * 64, "den")

    falsified_budget = evidence()
    falsified_budget["ingest_memory_windows"]["bounded_working_set_bytes"] += 1
    falsified_budget["ingest_memory_windows"]["allowed_growth_bytes"] += 1
    with pytest.raises(validator.EvidenceError, match="construction evidence"):
        validator.validate(falsified_budget, "a" * 40, "sha256:" + "b" * 64, "den")


@pytest.mark.parametrize(
    "field", ["early_sample_count", "middle_sample_count", "late_sample_count"]
)
def test_ingest_progress_bands_reject_sparse_samples(field):
    sparse = evidence()
    sparse["ingest_memory_windows"][field] = 2
    with pytest.raises(validator.EvidenceError, match="schema violation"):
        validator.validate(sparse, "a" * 40, "sha256:" + "b" * 64, "den")


def test_ingest_progress_bands_reject_gaps_and_missing_final_coverage():
    gap = evidence()
    gap["ingest_memory_windows"]["middle_progress_start"] += 1
    with pytest.raises(validator.EvidenceError, match="exactly contiguous"):
        validator.validate(gap, "a" * 40, "sha256:" + "b" * 64, "den")

    incomplete = evidence()
    incomplete["ingest_memory_windows"]["final_committed_chunks"] += 1
    with pytest.raises(validator.EvidenceError, match="final committed progress"):
        validator.validate(incomplete, "a" * 40, "sha256:" + "b" * 64, "den")

    duplicate_or_missing_sample = evidence()
    duplicate_or_missing_sample["ingest_memory_windows"]["middle_sample_count"] -= 1
    with pytest.raises(validator.EvidenceError, match="distinct committed chunk coverage"):
        validator.validate(duplicate_or_missing_sample, "a" * 40, "sha256:" + "b" * 64, "den")

    wrong_source = evidence()
    wrong_source["ingest_memory_windows"]["sampling_source"] = "timer_poll"
    with pytest.raises(validator.EvidenceError, match="schema violation"):
        validator.validate(wrong_source, "a" * 40, "sha256:" + "b" * 64, "den")


def test_authenticated_edge_commit_count_is_required_and_cross_reconciled():
    missing = evidence()
    del missing["rung"]["construction"]["edge_batch_commits"]
    with pytest.raises(validator.EvidenceError, match="schema violation"):
        validator.validate(missing, "a" * 40, "sha256:" + "b" * 64, "den")

    wrong_total = evidence()
    wrong_total["rung"]["construction"]["input_batches"] += 1
    with pytest.raises(validator.EvidenceError, match="batch count does not reconcile"):
        validator.validate(wrong_total, "a" * 40, "sha256:" + "b" * 64, "den")

    wrong_progress = evidence()
    wrong_progress["rung"]["construction"]["edge_batch_commits"] += 1
    wrong_progress["rung"]["construction"]["input_batches"] += 1
    with pytest.raises(validator.EvidenceError, match="RSS progress differs"):
        validator.validate(wrong_progress, "a" * 40, "sha256:" + "b" * 64, "den")


def test_machine_relative_allowance_cannot_hide_growth_over_bounded_workset():
    growing = evidence()
    windows = growing["ingest_memory_windows"]
    windows.update(
        {
            "early_rss_peak_bytes": 500_000_000,
            "middle_rss_peak_bytes": 700_000_000,
            "late_rss_peak_bytes": 900_000_000,
            "observed_growth_bytes": 400_000_000,
            "headroom_bytes": windows["envelope_bytes"] - 900_000_000,
        }
    )
    assert windows["observed_growth_bytes"] < windows["envelope_bytes"] // 8
    with pytest.raises(validator.EvidenceError, match="does not plateau"):
        validator.validate(growing, "a" * 40, "sha256:" + "b" * 64, "den")


@pytest.mark.parametrize(
    ("early", "middle", "late"),
    [
        (1_500_000_000, 500_000_000, 1_200_000_000),
        (500_000_000, 800_000_000, 1_100_000_000),
    ],
)
def test_ingest_plateau_rejects_regrowth_and_cumulative_growth(early, middle, late):
    growing = evidence()
    windows = growing["ingest_memory_windows"]
    windows.update(
        {
            "early_rss_peak_bytes": early,
            "middle_rss_peak_bytes": middle,
            "late_rss_peak_bytes": late,
            "observed_growth_bytes": max(0, middle - early, late - middle, late - early),
            "headroom_bytes": windows["envelope_bytes"] - max(early, middle, late),
        }
    )
    with pytest.raises(validator.EvidenceError, match="does not plateau"):
        validator.validate(growing, "a" * 40, "sha256:" + "b" * 64, "den")


def test_evidence_rejects_tiny_relabel_cross_count_and_memory_overage():
    tiny = evidence()
    tiny["counts"].update(
        {
            "raw_attempts": 16,
            "generated_edges": 15,
            "source_edges": 15,
            "imported_edges": 15,
            "self_loops_rejected": 1,
            "duplicates_rejected": 0,
        }
    )
    tiny["lifecycle"].update(
        {"source_nodes": 1, "imported_nodes": 1, "source_edges": 15, "imported_edges": 15}
    )
    with pytest.raises(validator.EvidenceError, match=r"2\^20"):
        validator.validate(tiny, "a" * 40, "sha256:" + "b" * 64, "den")
    cross_count = evidence()
    cross_count["lifecycle"]["source_edges"] -= 1
    with pytest.raises(validator.EvidenceError, match="not bound"):
        validator.validate(cross_count, "a" * 40, "sha256:" + "b" * 64, "den")
    over_memory = evidence()
    over_memory["phase_memory"]["ingest"]["rss_peak_bytes"] = 5 * 1024**3
    over_memory["phase_memory"]["ingest"]["process_global_hwm_bytes"] = 5 * 1024**3
    over_memory["phase_memory"]["ingest"]["anonymous_peak_bytes"] = 5 * 1024**3
    with pytest.raises(validator.EvidenceError, match="4096 MiB"):
        validator.validate(over_memory, "a" * 40, "sha256:" + "b" * 64, "den")


def test_no_lower_rung_or_dynamic_sizing_contract_exists():
    source = (ROOT / "scripts/fly-g500-s20.py").read_text()
    for forbidden in ("S18", "S19", "lower_rung", "rss_ratio", "dynamic_memory", "S26"):
        assert forbidden not in source
