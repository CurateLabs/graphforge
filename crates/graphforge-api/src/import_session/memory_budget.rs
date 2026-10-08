//! Plan-time memory budget of the bulk builder (ADR 0058).
//!
//! The budget chooses between two builds of the same bytes: all resident, or
//! through scratch files that keep the peak inside it (#1900). It is the memory the
//! process can still claim, read from `/proc/meminfo` and from the cgroup the
//! process actually runs in, scaled by [`BUDGET_NUMERATOR`] / [`BUDGET_DENOMINATOR`].
//!
//! The cgroup is found from `/proc/self/cgroup`, never assumed to be the root:
//! the root cgroup has no `memory.max`, and a runner such as BenchExec places the
//! process in a nested cgroup whose limit sits in that cgroup's own directory or
//! in an ancestor's. Every level from the process's cgroup to the root bounds the
//! process, so the tightest remaining limit wins.

use graphforge_core::GfError;

/// Share of the claimable memory the builder may plan to use; the rest covers
/// the page cache the encoded files fill, the publisher, and other tenants.
const BUDGET_NUMERATOR: u64 = 3;
const BUDGET_DENOMINATOR: u64 = 5;
/// Budget where neither `/proc/meminfo` nor a cgroup limit can be read.
const FALLBACK_BYTES: u64 = 4 << 30;

/// Bytes the process can still claim: the smaller of `MemAvailable` and the
/// remaining headroom (`memory.max` - `memory.current`) of every cgroup level
/// that has a numeric limit. `None` when nothing is readable.
pub(super) fn claimable_bytes(
    meminfo: Option<&str>,
    self_cgroup: Option<&str>,
    read: &dyn Fn(&str) -> Option<String>,
) -> Option<u64> {
    let available = meminfo.and_then(|text| {
        text.lines()
            .find_map(|line| line.strip_prefix("MemAvailable:"))
            .and_then(|rest| rest.split_ascii_whitespace().next()?.parse::<u64>().ok())
            .map(|kib| kib.saturating_mul(1024))
    });
    let mut claimable = available;
    // Unified (v2) hierarchy: the line `0::<path>`.
    let path = self_cgroup.and_then(|text| {
        text.lines()
            .find_map(|line| line.strip_prefix("0::"))
            .map(|path| path.split(" (deleted)").next().unwrap_or(path).trim_end())
    });
    if let Some(path) = path {
        let mut level = path.trim_end_matches('/');
        loop {
            let directory = format!("/sys/fs/cgroup{level}");
            let number = |name: &str| {
                read(&format!("{directory}/{name}"))
                    .and_then(|text| text.trim().parse::<u64>().ok())
            };
            // `max` is not a number: this level imposes no limit.
            if let Some(limit) = number("memory.max") {
                let remaining = limit.saturating_sub(number("memory.current").unwrap_or(0));
                claimable = Some(claimable.map_or(remaining, |bytes| bytes.min(remaining)));
            }
            match level.rfind('/') {
                Some(cut) => level = &level[..cut],
                None => break,
            }
        }
    }
    claimable
}

/// The budget for `claimable` bytes of memory.
pub(super) fn budget_for(claimable: Option<u64>) -> u64 {
    claimable.map_or(FALLBACK_BYTES, |claimable| {
        claimable / BUDGET_DENOMINATOR * BUDGET_NUMERATOR
    })
}

/// Environment variable that pins the budget, in bytes, in place of the one
/// derived from the host. It lets an operator keep a build inside a share of
/// memory the cgroup walk cannot see, and lets a measurement force the
/// over-budget route on a small input.
pub(super) const BUDGET_ENV: &str = "GF_BULK_BUILD_MEMORY_BUDGET_BYTES";

/// The budget a pinned `value` asks for.
pub(super) fn parse_override(value: &str) -> Result<u64, GfError> {
    value
        .trim()
        .parse::<u64>()
        .ok()
        .filter(|bytes| *bytes > 0)
        .ok_or_else(|| {
            GfError::Storage(format!(
                "{BUDGET_ENV} must be a positive number of bytes, got {value:?}"
            ))
        })
}

/// The budget on this host, for this process.
pub(super) fn bulk_build_memory_budget() -> Result<u64, GfError> {
    if let Some(value) = std::env::var_os(BUDGET_ENV) {
        return parse_override(&value.to_string_lossy());
    }
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok();
    let cgroup = std::fs::read_to_string("/proc/self/cgroup").ok();
    Ok(budget_for(claimable_bytes(
        meminfo.as_deref(),
        cgroup.as_deref(),
        &|path| std::fs::read_to_string(path).ok(),
    )))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    const GIB: u64 = 1 << 30;

    fn files(entries: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map = entries
            .iter()
            .map(|(path, text)| ((*path).to_owned(), (*text).to_owned()))
            .collect::<HashMap<_, _>>();
        move |path| map.get(path).cloned()
    }

    fn meminfo(gib: u64) -> String {
        format!("MemTotal: 1 kB\nMemAvailable:   {} kB\n", gib * GIB / 1024)
    }

    #[test]
    fn a_nested_cgroup_limit_is_read_from_the_process_own_directory() {
        // The root has no memory.max; BenchExec's run cgroup carries the limit.
        let read = files(&[
            (
                "/sys/fs/cgroup/benchexec/run_1/memory.max",
                "103079215104\n",
            ),
            (
                "/sys/fs/cgroup/benchexec/run_1/memory.current",
                "3221225472\n",
            ),
            ("/sys/fs/cgroup/benchexec/memory.max", "max\n"),
        ]);
        let claimable = claimable_bytes(Some(&meminfo(120)), Some("0::/benchexec/run_1\n"), &read);
        assert_eq!(claimable, Some(96 * GIB - 3 * GIB));
    }

    #[test]
    fn the_tightest_level_from_the_process_to_the_root_wins() {
        let read = files(&[
            ("/sys/fs/cgroup/a/b/c/memory.max", "max"),
            ("/sys/fs/cgroup/a/b/memory.max", "17179869184"),
            ("/sys/fs/cgroup/a/b/memory.current", "1073741824"),
            ("/sys/fs/cgroup/a/memory.max", "34359738368"),
            ("/sys/fs/cgroup/memory.max", "68719476736"),
        ]);
        let claimable = claimable_bytes(Some(&meminfo(120)), Some("0::/a/b/c"), &read);
        assert_eq!(claimable, Some(15 * GIB));
    }

    #[test]
    fn a_cgroup_namespace_root_reads_the_mounted_root() {
        let read = files(&[("/sys/fs/cgroup/memory.max", "8589934592")]);
        assert_eq!(
            claimable_bytes(Some(&meminfo(120)), Some("0::/\n"), &read),
            Some(8 * GIB)
        );
    }

    #[test]
    fn without_a_limit_the_available_memory_decides_and_nothing_readable_has_no_answer() {
        let read = files(&[]);
        assert_eq!(
            claimable_bytes(Some(&meminfo(100)), Some("0::/user.slice/x.service"), &read),
            Some(100 * GIB)
        );
        assert_eq!(claimable_bytes(None, None, &read), None);
        // A v1-only host has no `0::` line: only MemAvailable applies.
        assert_eq!(
            claimable_bytes(Some(&meminfo(10)), Some("4:memory:/x\n"), &read),
            Some(10 * GIB)
        );
    }

    /// Which ladder rungs plan onto the bulk builder: under a 96 GB cgroup (the
    /// BenchExec limit, with the run's first 4 GB already charged) and outside
    /// one on this host's 128 GB (MemAvailable 125 GB when idle).
    #[test]
    fn ladder_rungs_route_by_the_fitted_model() {
        use graphforge_storage::{BulkBatchReader, BulkBuildPlan, BulkSource};

        struct Never;
        impl BulkBatchReader for Never {
            fn task_rows(&self, _: usize) -> usize {
                unreachable!("planning only")
            }
            fn read_task(
                &self,
                _: usize,
                _: &mut dyn FnMut(
                    arrow::record_batch::RecordBatch,
                ) -> Result<(), graphforge_core::GfError>,
            ) -> Result<(), graphforge_core::GfError> {
                unreachable!("planning only")
            }
        }
        let rung = |scale: u32| {
            let source = |rows: u64| BulkSource {
                reader: std::sync::Arc::new(Never),
                tasks: 1,
                rows,
                property_free: true,
                decoded_bytes: 0,
            };
            BulkBuildPlan {
                nodes: vec![source(1 << scale)],
                edges: vec![source(16 << scale)],
            }
        };
        let under_benchexec = budget_for(claimable_bytes(
            Some(&meminfo(120)),
            Some("0::/benchexec/run_1\n"),
            &files(&[
                ("/sys/fs/cgroup/benchexec/run_1/memory.max", "96000000000"),
                (
                    "/sys/fs/cgroup/benchexec/run_1/memory.current",
                    "4000000000",
                ),
            ]),
        ));
        let outside = budget_for(claimable_bytes(
            Some("MemAvailable: 122500000 kB\n"),
            Some("0::/user.slice/x.service"),
            &files(&[]),
        ));
        for (scale, benchexec, host) in [
            (22, true, true),
            (24, true, true),
            (25, true, true),
            (26, true, true),
        ] {
            let estimate = rung(scale).estimated_resident_bytes();
            assert_eq!(
                estimate <= under_benchexec,
                benchexec,
                "S{scale}: {estimate} against the BenchExec budget {under_benchexec}"
            );
            assert_eq!(
                estimate <= outside,
                host,
                "S{scale}: {estimate} against the host budget {outside}"
            );
        }
    }
}
