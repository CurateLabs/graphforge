//! Same-boundary workflow observations and the closed receipt diagnostics contract.
use std::time::Instant;

use serde_json::{Value, json};

pub(crate) struct WorkflowSample {
    started: Instant,
    cpu: Option<(u64, u64)>,
    sampling_ns: u64,
}

impl WorkflowSample {
    pub(crate) fn start(started: Instant) -> Self {
        let cpu = cpu_sample();
        let sampling_ns = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        Self {
            started,
            cpu,
            sampling_ns,
        }
    }

    pub(crate) fn finish(&self) -> Value {
        let ended = Instant::now();
        let wall_ns =
            u64::try_from(ended.duration_since(self.started).as_nanos()).unwrap_or(u64::MAX);
        let delta = self
            .cpu
            .zip(cpu_sample())
            .and_then(|(a, b)| Some((b.0.checked_sub(a.0)?, b.1.checked_sub(a.1)?)));
        json!({
            "contract": "graphforge-workflow-timing/1",
            "cpu_scope": "runner_and_waited_children_shared",
            "wall_ns": wall_ns,
            "sampling_uncertainty_ns": self.sampling_ns.saturating_add(u64::try_from(ended.elapsed().as_nanos()).unwrap_or(u64::MAX)),
            "runner_cpu_ns": delta.map(|d| d.0),
            "children_cpu_ns": delta.map(|d| d.1),
        })
    }
}

#[cfg(target_os = "linux")]
fn cpu_sample() -> Option<(u64, u64)> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    parse_cpu(&stat)
}

#[cfg(target_os = "linux")]
fn parse_cpu(stat: &str) -> Option<(u64, u64)> {
    let fields: Vec<_> = stat
        .get(stat.rfind(')')? + 1..)?
        .split_whitespace()
        .collect();
    let nanos = |a: usize, b: usize| {
        fields
            .get(a)?
            .parse::<u64>()
            .ok()?
            .checked_add(fields.get(b)?.parse::<u64>().ok()?)?
            .checked_mul(10_000_000)
    };
    Some((nanos(11, 12)?, nanos(13, 14)?))
}

#[cfg(not(target_os = "linux"))]
fn cpu_sample() -> Option<(u64, u64)> {
    None
}

const MEASUREMENTS: [&str; 14] = [
    "wall_ns",
    "sampling_uncertainty_ns",
    "process_cpu_ns",
    "thread_running_ns",
    "thread_runnable_ns",
    "thread_sleeping_ns",
    "thread_uninterruptible_ns",
    "thread_iowait_ns",
    "thread_unknown_ns",
    "written_bytes",
    "hashed_bytes",
    "hash_elapsed_ns",
    "fsync_calls",
    "fsync_elapsed_ns",
];
const WORK_UNITS_V1: [&str; 6] = [
    "rows",
    "bytes",
    "nodes",
    "edges",
    "hashed_bytes",
    "written_bytes",
];
const WORK_UNITS_V2: [&str; 11] = [
    "rows",
    "bytes",
    "nodes",
    "edges",
    "hashed_bytes",
    "written_bytes",
    "participant_materialized_bytes",
    "participant_reused_bytes",
    "participant_payload_read_bytes",
    "observed_bytes",
    "reread_bytes",
];
const REGIONS: [&str; 58] = [
    "import_command",
    "begin_import",
    "resume_import",
    "register_arrow",
    "register_parquet",
    "checkpoint",
    "stage+seal",
    "bulk_build",
    "append_nodes",
    "append_edges",
    "source_read",
    "manifest_persistence",
    "journal_append",
    "journal_sync",
    "journal_namespace_publication",
    "source_publication",
    "source_cleanup",
    "validate",
    "commit",
    "open_construction",
    "append",
    "seal",
    "publish",
    "shaping",
    "canonical_encoding",
    "membership_encoding",
    "node_encoding",
    "edge_encoding",
    "adjacency_grouping",
    "adjacency_csr",
    "seal_authentication",
    "fsync",
    "normalization",
    "lock_wait",
    "artifact_authentication",
    "inventory_authentication",
    "inventory_payload_authentication",
    "prepare_encoding",
    "publication_authentication",
    "cas_install",
    "publication_intent",
    "generation_commit",
    "publication_receipt",
    "hydration",
    "read_authority",
    "shape_planning",
    "shape_routing",
    "shape_family_finish",
    "partition_load_wait",
    "surrogate_assignment",
    "endpoint_resolution",
    "shape_row_finish",
    "runtime_catalog",
    "shape_completion",
    "derivation_input_validation",
    "participant_materialization",
    "participant_carry_forward",
    "participant_reuse_validation",
];

pub(crate) fn valid_snapshot(value: &Value) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };
    let legacy = value["contract"] == "graphforge-region-diagnostics/1";
    let measurements = if legacy {
        &MEASUREMENTS[..9]
    } else {
        &MEASUREMENTS[..]
    };
    let work_units = if legacy {
        &WORK_UNITS_V1[..]
    } else {
        &WORK_UNITS_V2[..]
    };
    if object.len() != if legacy { 5 } else { 6 }
        || value["complete"] != true
        || (!legacy
            && (value["contract"] != "graphforge-region-diagnostics/2"
                || value["io_scope"]
                    != "shared_process_inclusive_write_syscalls_instrumented_sha256_and_barriers"))
        || value["cpu_scope"] != "shared_process_inclusive_do_not_sum"
        || value["scheduler_scope"] != "calling_thread_only_unknown_is_not_blocked_or_psi"
    {
        return false;
    }
    let Some(regions) = value["regions"].as_object() else {
        return false;
    };
    !regions.is_empty()
        && regions.len() <= 256
        && regions.iter().all(|(path, row)| {
            path.starts_with("import_command")
                && path.split('/').count() <= 16
                && path.split('/').all(|part| {
                    REGIONS.contains(&part)
                        && if legacy {
                            !matches!(
                                part,
                                "stage+seal"
                                    | "append_nodes"
                                    | "append_edges"
                                    | "participant_materialization"
                                    | "participant_carry_forward"
                                    | "participant_reuse_validation"
                                    | "source_read"
                                    | "manifest_persistence"
                                    | "journal_append"
                                    | "journal_sync"
                                    | "journal_namespace_publication"
                                    | "source_publication"
                                    | "source_cleanup"
                                    | "derivation_input_validation"
                            )
                        } else {
                            part != "validate"
                        }
                })
                && row.as_object().is_some_and(|r| r.len() == 4)
                && row["work"].as_object().is_some_and(|work| {
                    work.iter()
                        .all(|(k, v)| work_units.contains(&k.as_str()) && v.as_u64().is_some())
                })
                && row["calls"].as_u64().is_some_and(|n| n > 0)
                && ["inclusive", "residual"].iter().all(|key| {
                    row[*key].as_object().is_some_and(|m| {
                        m.len() == measurements.len()
                            && measurements.iter().all(|field| {
                                m.get(*field).is_some_and(|n| {
                                    n.as_u64().is_some()
                                        || (!matches!(
                                            *field,
                                            "wall_ns" | "sampling_uncertainty_ns"
                                        ) && n.is_null())
                                })
                            })
                    })
                })
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_os = "linux")]
    #[test]
    fn workflow_cpu_includes_reaped_children_but_keeps_components_separate() {
        let stat = "42 (odd ) name) S 1 2 3 4 5 6 7 8 9 10 11 12 13 14";
        assert_eq!(parse_cpu(stat), Some((230_000_000, 270_000_000)));
        assert_eq!(parse_cpu("malformed"), None);
    }

    #[test]
    fn snapshot_contract_preserves_null_and_refuses_untrusted_names() {
        let capture =
            graphforge_storage::concurrency_attribution::RegionCapture::start("import_command");
        let mut value = serde_json::to_value(capture.finish()).unwrap();
        assert!(valid_snapshot(&value));
        value["regions"]["import_command"]["inclusive"]["thread_runnable_ns"] = Value::Null;
        assert!(valid_snapshot(&value));
        value["regions"]["private/path"] = value["regions"]["import_command"].clone();
        assert!(!valid_snapshot(&value));
    }

    #[test]
    fn snapshot_contract_accepts_successful_byte_work_and_refuses_unknown_units() {
        let capture =
            graphforge_storage::concurrency_attribution::RegionCapture::start("import_command");
        graphforge_storage::concurrency_attribution::RegionScope::record_work("hashed_bytes", 17);
        graphforge_storage::concurrency_attribution::RegionScope::record_work("written_bytes", 11);
        graphforge_storage::concurrency_attribution::RegionScope::record_work(
            "participant_materialized_bytes",
            13,
        );
        graphforge_storage::concurrency_attribution::RegionScope::record_work(
            "participant_reused_bytes",
            17,
        );
        graphforge_storage::concurrency_attribution::RegionScope::record_work(
            "participant_payload_read_bytes",
            19,
        );
        graphforge_storage::concurrency_attribution::RegionScope::record_work("observed_bytes", 23);
        graphforge_storage::concurrency_attribution::RegionScope::record_work("reread_bytes", 29);
        let mut value = serde_json::to_value(capture.finish()).unwrap();
        assert!(valid_snapshot(&value));
        value["regions"]["import_command"]["work"]["attempted_bytes"] = json!(17);
        assert!(!valid_snapshot(&value));
        value["regions"]["import_command"]["work"]
            .as_object_mut()
            .unwrap()
            .remove("attempted_bytes");
        value["regions"]["import_command"]["work"]["hashed_bytes"] = Value::Null;
        assert!(!valid_snapshot(&value));
    }

    #[test]
    fn derivation_validation_region_is_v2_only() {
        use graphforge_storage::concurrency_attribution::{RegionCapture, RegionScope};

        let capture = RegionCapture::start("import_command");
        {
            let _scope = RegionScope::named("derivation_input_validation");
            RegionScope::record_work("bytes", 100);
            RegionScope::record_work("rows", 2);
        }
        let value = serde_json::to_value(capture.finish()).unwrap();
        assert!(valid_snapshot(&value));
        let mut unknown = value.clone();
        unknown["regions"]["import_command/private_derivation_subject"] =
            unknown["regions"]["import_command/derivation_input_validation"].clone();
        assert!(!valid_snapshot(&unknown));
        let mut legacy = value;
        legacy["contract"] = json!("graphforge-region-diagnostics/1");
        legacy.as_object_mut().unwrap().remove("io_scope");
        for row in legacy["regions"].as_object_mut().unwrap().values_mut() {
            for scope in ["inclusive", "residual"] {
                row[scope]
                    .as_object_mut()
                    .unwrap()
                    .retain(|key, _| MEASUREMENTS[..9].contains(&key.as_str()));
            }
        }
        assert!(!valid_snapshot(&legacy));
        legacy["regions"]
            .as_object_mut()
            .unwrap()
            .remove("import_command/derivation_input_validation");
        assert!(valid_snapshot(&legacy));
    }

    #[test]
    fn journal_region_contract_accepts_only_production_leaves() {
        let journal_regions = [
            "journal_append",
            "journal_sync",
            "journal_namespace_publication",
            "source_publication",
            "source_cleanup",
        ];
        let capture =
            graphforge_storage::concurrency_attribution::RegionCapture::start("import_command");
        {
            let _begin =
                graphforge_storage::concurrency_attribution::RegionScope::named("begin_import");
            // Names emitted by the journal writer, independent of the validator
            // allowlist. The real begin receipt includes the namespace and sync
            // leaves; subsequent mutation receipts include the remaining leaves.
            for name in journal_regions {
                let _scope = graphforge_storage::concurrency_attribution::RegionScope::named(name);
            }
        }
        let value = serde_json::to_value(capture.finish()).unwrap();
        assert!(valid_snapshot(&value));

        let mut unknown = value.clone();
        unknown["regions"]["import_command/begin_import/private_source_path"] =
            unknown["regions"]["import_command/begin_import/journal_sync"].clone();
        assert!(!valid_snapshot(&unknown));

        let mut malformed = value.clone();
        malformed["regions"]["import_command/begin_import/journal_sync"]["inclusive"]["fsync_calls"] =
            json!("one");
        assert!(!valid_snapshot(&malformed));

        let mut legacy = value;
        legacy["contract"] = json!("graphforge-region-diagnostics/1");
        legacy.as_object_mut().unwrap().remove("io_scope");
        for row in legacy["regions"].as_object_mut().unwrap().values_mut() {
            for scope in ["inclusive", "residual"] {
                row[scope]
                    .as_object_mut()
                    .unwrap()
                    .retain(|key, _| MEASUREMENTS[..9].contains(&key.as_str()));
            }
        }
        assert!(!valid_snapshot(&legacy));
        legacy["regions"]
            .as_object_mut()
            .unwrap()
            .retain(|path, _| {
                matches!(
                    path.as_str(),
                    "import_command" | "import_command/begin_import"
                )
            });
        assert!(valid_snapshot(&legacy));
        for name in [
            "journal_append",
            "journal_sync",
            "journal_namespace_publication",
            "source_publication",
            "source_cleanup",
            "participant_materialization",
            "participant_carry_forward",
            "participant_reuse_validation",
        ] {
            let mut invalid_legacy = legacy.clone();
            invalid_legacy["regions"][format!("import_command/begin_import/{name}")] =
                legacy["regions"]["import_command/begin_import"].clone();
            assert!(!valid_snapshot(&invalid_legacy), "{name} accepted as v1");
        }
    }

    #[test]
    fn snapshot_contract_accepts_captured_encoding_lane_receipt() {
        // A `receipt-3-validate.json` captured by the #1600 encoding-lane
        // candidate run `curve-s18-c8-r1`; retained here as a contract fixture.
        let receipt: Value =
            serde_json::from_str(include_str!("../fixtures/region-diagnostics-receipt.json"))
                .unwrap();
        assert!(valid_snapshot(&receipt["region_diagnostics"]));
    }

    #[test]
    fn snapshot_contract_accepts_every_allowlisted_region_name() {
        let capture =
            graphforge_storage::concurrency_attribution::RegionCapture::start("import_command");
        for name in REGIONS.iter().skip(1).filter(|name| **name != "validate") {
            let _scope = graphforge_storage::concurrency_attribution::RegionScope::named(name);
        }
        let value = serde_json::to_value(capture.finish()).unwrap();
        assert!(valid_snapshot(&value));
        for name in REGIONS.iter().skip(1).filter(|name| **name != "validate") {
            assert!(
                value["regions"][format!("import_command/{name}")].is_object(),
                "{name} missing from the capture"
            );
        }
    }

    #[test]
    fn snapshot_contract_accepts_the_bulk_build_region_below_stage_and_seal() {
        use graphforge_storage::concurrency_attribution::{RegionCapture, RegionScope};

        let capture = RegionCapture::start("import_command");
        {
            let _stage = RegionScope::named("stage+seal");
            let _build = RegionScope::named("bulk_build");
            RegionScope::record_work("nodes", 3);
            RegionScope::record_work("edges", 5);
        }
        let value = serde_json::to_value(capture.finish()).unwrap();
        assert!(valid_snapshot(&value));
        assert!(value["regions"]["import_command/stage+seal/bulk_build"].is_object());
    }

    #[test]
    fn snapshot_contract_accepts_participant_reuse_regions() {
        let capture =
            graphforge_storage::concurrency_attribution::RegionCapture::start("import_command");
        for name in [
            "participant_materialization",
            "participant_carry_forward",
            "participant_reuse_validation",
        ] {
            let _region = graphforge_storage::concurrency_attribution::RegionScope::named(name);
        }
        let value = serde_json::to_value(capture.finish()).unwrap();
        assert!(valid_snapshot(&value));
        for name in [
            "participant_materialization",
            "participant_carry_forward",
            "participant_reuse_validation",
        ] {
            assert!(value["regions"][format!("import_command/{name}")].is_object());
        }
    }

    #[test]
    fn successful_work_units_match_each_closed_certification_schema() {
        let schema: Value =
            serde_json::from_str(include_str!("../../../schemas/certification-evidence.json"))
                .unwrap();
        for contract in ["regionDiagnosticsV2", "regionDiagnosticsV1"] {
            let work = &schema["$defs"][contract]["properties"]["regions"]["additionalProperties"]
                ["properties"]["work"];
            assert_eq!(work["additionalProperties"], false);
            let expected = if contract == "regionDiagnosticsV1" {
                &WORK_UNITS_V1[..]
            } else {
                &WORK_UNITS_V2[..]
            };
            let mut names: Vec<_> = work["properties"]
                .as_object()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect();
            names.sort_unstable();
            let mut expected = expected.to_vec();
            expected.sort_unstable();
            assert_eq!(names, expected);
            for unit in expected {
                assert_eq!(work["properties"][unit]["type"], "integer");
                assert_eq!(work["properties"][unit]["minimum"], 0);
            }
        }
    }

    #[test]
    fn region_allowlist_matches_certification_schema_pattern() {
        let schema = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../schemas/certification-evidence.json"
        ))
        .unwrap();
        let schema: Value = serde_json::from_str(&schema).unwrap();
        let pattern = schema["$defs"]["regionDiagnosticsV2"]["properties"]["regions"]["propertyNames"]["pattern"]
            .as_str().unwrap().replace("\\+", "+");
        let start = "^import_command(/(".len();
        let end = start + pattern[start..].find("))").unwrap();
        let mut schema_names: Vec<&str> = pattern[start..end].split('|').collect();
        schema_names.sort_unstable();
        let mut rust_names: Vec<&str> = REGIONS
            .iter()
            .copied()
            .skip(1)
            .filter(|name| *name != "validate")
            .collect();
        rust_names.sort_unstable();
        assert_eq!(schema_names, rust_names);
    }
}
