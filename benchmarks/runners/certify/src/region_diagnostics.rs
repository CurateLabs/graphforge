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

const MEASUREMENTS: [&str; 9] = [
    "wall_ns",
    "sampling_uncertainty_ns",
    "process_cpu_ns",
    "thread_running_ns",
    "thread_runnable_ns",
    "thread_sleeping_ns",
    "thread_uninterruptible_ns",
    "thread_iowait_ns",
    "thread_unknown_ns",
];
const REGIONS: [&str; 30] = [
    "import_command",
    "begin_import",
    "resume_import",
    "register_arrow",
    "register_parquet",
    "checkpoint",
    "validate",
    "commit",
    "open_construction",
    "append",
    "seal",
    "publish",
    "shaping",
    "canonical_encoding",
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
    "adjacency_encoding",
];

pub(crate) fn valid_snapshot(value: &Value) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };
    if object.len() != 5
        || value["complete"] != true
        || value["contract"] != "graphforge-region-diagnostics/1"
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
                && path.split('/').all(|part| REGIONS.contains(&part))
                && row.as_object().is_some_and(|r| r.len() == 4)
                && row["work"].as_object().is_some_and(|work| {
                    work.iter().all(|(k, v)| {
                        matches!(k.as_str(), "rows" | "bytes" | "nodes" | "edges")
                            && v.as_u64().is_some()
                    })
                })
                && row["calls"].as_u64().is_some_and(|n| n > 0)
                && ["inclusive", "residual"].iter().all(|key| {
                    row[*key].as_object().is_some_and(|m| {
                        m.len() == MEASUREMENTS.len()
                            && MEASUREMENTS.iter().all(|field| {
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
}
