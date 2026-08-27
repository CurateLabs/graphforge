#!/usr/bin/env python3
from __future__ import annotations

import argparse
import copy
from decimal import Decimal
import importlib.util
import json
from pathlib import Path

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


def args(tmp_path: Path, **changes):
    values = {
        "expected_sha": "a" * 40,
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
            "early_sample_count": 4,
            "middle_sample_count": 4,
            "late_sample_count": 4,
            "early_progress_start": 1,
            "early_progress_end": 100,
            "middle_progress_start": 101,
            "middle_progress_end": 200,
            "late_progress_start": 201,
            "late_progress_end": 300,
            "final_committed_chunks": 300,
            "bounded_working_set_bytes": 268_435_456,
            "sampling_tolerance_bytes": 67_108_864,
            "allowed_growth_bytes": 335_544_320,
            "observed_growth_bytes": 200_000_000,
            "plateau_pass": True,
            "envelope_bytes": 4_294_967_296,
            "headroom_bytes": 3_594_967_296,
            "sample_interval_ms": 250,
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


def test_contract_fixes_resources_and_rejects_mutable_image(tmp_path):
    digest = controller.validate_inputs(args(tmp_path))
    payload = controller.machine_payload(args(tmp_path), "vol-id", digest, "c" * 64)
    assert payload["config"]["guest"] == {"cpu_kind": "performance", "cpus": 2, "memory_mb": 4096}
    assert payload["config"]["services"] == []
    assert payload["config"]["restart"] == {"policy": "no"}
    assert payload["config"]["mounts"] == [{"volume": "vol-id", "path": "/work"}]
    assert payload["config"]["env"]["GF_G500_CERTIFICATION_SCALE"] == "20"
    assert payload["config"]["env"]["GF_G500_S20_EXPECTED_SHA"] == "a" * 40
    assert payload["config"]["env"]["GF_G500_S20_VOLUME_GB"] == "500"
    assert payload["config"]["env"]["GF_G500_S20_EVIDENCE_OUT"] == "/work/s20-evidence.json"
    assert payload["config"]["env"]["GF_G500_S20_RESULT_OUT"] == "/work/container-result.json"
    assert payload["config"]["env"]["GF_G500_S20_TIMEOUT_SECONDS"] == "14400"
    with pytest.raises(controller.ControllerError, match="immutable"):
        controller.validate_inputs(args(tmp_path, image="registry.example/graphforge:latest"))


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


@pytest.mark.parametrize(
    ("change", "message"),
    [
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


def test_existing_app_is_refused_before_budget_or_creation(tmp_path):
    class ExistingFly:
        def json(self, command, **_kwargs):
            assert command == ["apps", "list"]
            return [{"name": "gf-s20-unique"}]

        def run(self, *_args, **_kwargs):
            raise AssertionError("must not mutate provider state")

    run = args(tmp_path, execute=True, confirm_disposable=True)
    with pytest.raises(controller.ControllerError, match="existing app"):
        controller.execute(run, ExistingFly(), "sha256:" + "b" * 64)
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
    with pytest.raises(controller.ControllerError, match="HTTP 401"):
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
    phases = "source_reopen|source_query|export|verify|import|import_reopen|import_query"
    assert phases in entrypoint
    assert '"phase":"%s","code":"process_exit_%s"' in entrypoint


def test_terminal_failure_is_persisted_promptly_and_still_verified_cleaned(tmp_path, monkeypatch):
    digest = "sha256:" + "b" * 64
    machine = {
        "id": "machine-observed",
        "region": "den",
        "image_ref": {"digest": digest},
        "config": {
            "guest": {"cpu_kind": "performance", "cpus": 2, "memory_mb": 4096},
            "auto_destroy": True,
            "restart": {"policy": "no"},
            "services": [],
            "mounts": [{"path": "/work", "volume": "volume-observed"}],
        },
    }
    monkeypatch.setattr(controller, "create_machine", lambda *_args: machine)
    monkeypatch.setattr(controller, "inspect_image", lambda *_args: None)
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
                assert command[3] == "/work/container-result.json"
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
    with pytest.raises(controller.ControllerError, match="GF_OOM"):
        controller.execute(run, fly, digest)
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
        "image_ref": {"digest": digest},
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
        {"state": "stopped", "events": [{"type": "oom", "secret": "hidden"}]}, "ingest"
    ) == {
        "schema": "graphforge-fly-g500-s20-diagnostic/1",
        "status": "failure",
        "phase": "ingest",
        "code": "machine_oom",
    }


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
    sparse["ingest_memory_windows"][field] = 3
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
            "observed_growth_bytes": max(
                0, middle - early, late - middle, late - early
            ),
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
