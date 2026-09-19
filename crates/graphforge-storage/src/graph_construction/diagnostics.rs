//! Opt-in, non-durable measurements for the ingestion attribution study.
//! Stock-build scopes; optional event output contains no paths, UUIDs or payloads.

#[cfg(any(test, feature = "test-support"))]
use super::GraphConstructionEvidence;
use serde_json::json;
#[cfg(any(test, feature = "test-support"))]
use std::cell::Cell;
use std::io::Write;
use std::sync::OnceLock;
use std::time::Instant;

fn clock_ns() -> u128 {
    static ORIGIN: OnceLock<Instant> = OnceLock::new();
    ORIGIN.get_or_init(Instant::now).elapsed().as_nanos()
}

pub(crate) struct Scope {
    name: &'static str,
    _region: crate::concurrency_attribution::RegionScope,
    started_ns: u128,
}

impl Scope {
    pub(crate) fn start(name: &'static str) -> Self {
        use crate::storage_attribution::StorageIoPhase;
        let phase = match name {
            "shaping" => StorageIoPhase::ShapeConsumeReauthentication,
            "canonical_encoding"
            | "inventory_authentication"
            | "inventory_payload_authentication" => {
                StorageIoPhase::EncodeWritePostwriteAuthentication
            }
            _ => StorageIoPhase::SealAuthentication,
        };
        Self {
            name,
            _region: crate::concurrency_attribution::RegionScope::enter_named(phase, name),
            started_ns: clock_ns(),
        }
    }
}

impl Drop for Scope {
    fn drop(&mut self) {
        if !enabled() {
            return;
        }
        emit(
            &json!({"event":"scope", "scope":self.name, "start_ns":self.started_ns,
            "end_ns":clock_ns()}),
        );
    }
}

#[cfg(any(test, feature = "test-support"))]
thread_local! {
    static ROW_GROUP: Cell<u64> = const { Cell::new(0) };
}

fn enabled() -> bool {
    std::env::var_os("GRAPHFORGE_INGEST_DIAGNOSTICS").is_some_and(|v| v == "1282")
}

#[cfg(any(test, feature = "test-support"))]
pub(super) fn row_family(authority: &str) -> String {
    let kind = if authority.starts_with("0-") {
        "node-rows"
    } else {
        "edge-rows"
    };
    ROW_GROUP.with(|ordinal| {
        let next = ordinal.get() + 1;
        ordinal.set(next);
        format!("{kind}-{next}")
    })
}

fn emit(value: &serde_json::Value) {
    // Diagnostic output cannot turn an otherwise successful publication into an
    // error. The collector requires complete successful events independently.
    let _ = writeln!(std::io::stderr().lock(), "INGEST_DIAGNOSTIC {value}");
}

#[cfg(any(test, feature = "test-support"))]
pub(super) fn inputs(family: &str, count: u64) {
    if enabled() {
        emit(&json!({"event":"inputs", "family":family, "runs":count}));
    }
}

#[cfg(any(test, feature = "test-support"))]
fn counters(e: &GraphConstructionEvidence) -> [u64; 7] {
    [
        e.merge_read_records,
        e.merge_written_records,
        e.merge_read_bytes,
        e.merge_written_bytes,
        e.parquet_read_bytes,
        e.parquet_write_bytes,
        e.merge_fsync_operations,
    ]
}

#[cfg(any(test, feature = "test-support"))]
pub(super) struct Group {
    started: Instant,
    start_ns: u128,
    before: [u64; 7],
}

#[cfg(any(test, feature = "test-support"))]
impl Group {
    pub(super) fn start(e: &GraphConstructionEvidence) -> Option<Self> {
        enabled().then(|| Self {
            started: Instant::now(),
            start_ns: clock_ns(),
            before: counters(e),
        })
    }

    pub(super) fn finish(
        self,
        family: &str,
        level: usize,
        inputs: usize,
        e: &GraphConstructionEvidence,
        success: bool,
    ) {
        let after = counters(e);
        let delta = std::array::from_fn::<_, 7, _>(|i| after[i].checked_sub(self.before[i]));
        emit(&json!({"event":"group", "family":family, "level":level,
            "inputs":inputs, "success":success,
            "inclusive_wall_ns":self.started.elapsed().as_nanos(),
            "start_ns":self.start_ns, "end_ns":clock_ns(),
            "rows_read":delta[0], "rows_written":delta[1],
            "fixed_read_bytes":delta[2], "fixed_written_bytes":delta[3],
            "parquet_read_bytes":delta[4], "parquet_written_bytes":delta[5],
            "sync_calls":delta[6]}));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diagnostic_row_family_contains_only_kind_and_ordinal() {
        let family = row_family("0-private-schema-authority");
        assert!(family.starts_with("node-rows-"));
        assert!(family[10..].parse::<u64>().is_ok());
        assert!(!family.contains("private"));
    }
}
