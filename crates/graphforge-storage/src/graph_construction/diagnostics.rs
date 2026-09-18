//! Opt-in, non-durable measurements for the ingestion attribution study.
//! Compiled only with test support; no paths, UUIDs, schema contents or payloads.

use super::GraphConstructionEvidence;
use serde_json::json;
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
    started_ns: u128,
    cpu_before: Option<std::time::Duration>,
}

impl Scope {
    pub(crate) fn start(name: &'static str) -> Option<Self> {
        enabled().then(|| Self {
            name,
            started_ns: clock_ns(),
            cpu_before: crate::concurrency_attribution::process_cpu_time(),
        })
    }
}

impl Drop for Scope {
    fn drop(&mut self) {
        // Process CPU alongside wall, so a scope reports how many cores' worth
        // it used rather than only how long it took (#1462). `seal` is 68-80%
        // of ingest and #1387 budgets the serialized fraction, so which stage
        // holds that fraction up is the question these scopes exist to answer.
        let cpu_ns = match (
            self.cpu_before,
            crate::concurrency_attribution::process_cpu_time(),
        ) {
            (Some(before), Some(after)) => {
                serde_json::Value::from(after.saturating_sub(before).as_nanos() as u64)
            }
            _ => serde_json::Value::Null,
        };
        emit(
            &json!({"event":"scope", "scope":self.name, "start_ns":self.started_ns,
            "end_ns":clock_ns(), "cpu_ns":cpu_ns}),
        );
    }
}

thread_local! {
    static ROW_GROUP: Cell<u64> = const { Cell::new(0) };
}

fn enabled() -> bool {
    std::env::var_os("GRAPHFORGE_INGEST_DIAGNOSTICS").is_some_and(|v| v == "1282")
}

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

pub(super) fn inputs(family: &str, count: u64) {
    if enabled() {
        emit(&json!({"event":"inputs", "family":family, "runs":count}));
    }
}

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

pub(super) struct Group {
    started: Instant,
    start_ns: u128,
    before: [u64; 7],
}

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
