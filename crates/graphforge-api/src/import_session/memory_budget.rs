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
//!
//! A level's usage (`memory.current`, or v1 `memory.usage_in_bytes`) includes the
//! page cache charged to it, which the kernel reclaims on demand before the limit
//! is hit (#2360). Like `MemAvailable` does machine-wide, a level's headroom adds
//! back its clean, unmapped file-LRU pages: see [`reclaimable_file_bytes`].

use graphforge_core::GfError;

/// Share of the claimable memory the builder may plan to use; the rest covers
/// the page cache the encoded files fill, the publisher, and other tenants.
const BUDGET_NUMERATOR: u64 = 3;
const BUDGET_DENOMINATOR: u64 = 5;
/// Budget where neither `/proc/meminfo` nor a cgroup limit can be read.
const FALLBACK_BYTES: u64 = 4 << 30;

/// The interface files of one cgroup hierarchy version.
#[derive(Clone, Copy)]
enum Hierarchy {
    /// The unified hierarchy, mounted at `/sys/fs/cgroup`.
    V2,
    /// The v1 `memory` controller, mounted at `/sys/fs/cgroup/memory`.
    V1,
}

impl Hierarchy {
    fn directory(self, level: &str) -> String {
        match self {
            Self::V2 => format!("/sys/fs/cgroup{level}"),
            Self::V1 => format!("/sys/fs/cgroup/memory{level}"),
        }
    }

    /// The hard limit; v2 writes `max` for none, v1 a value near `i64::MAX`.
    fn limit(self) -> &'static str {
        match self {
            Self::V2 => "memory.max",
            Self::V1 => "memory.limit_in_bytes",
        }
    }

    /// Bytes charged to the level and its descendants, page cache included.
    fn usage(self) -> &'static str {
        match self {
            Self::V2 => "memory.current",
            Self::V1 => "memory.usage_in_bytes",
        }
    }

    /// `memory.stat` keys, all counting the level and its descendants:
    /// the two file LRU lists, then the pages among them that cannot be
    /// dropped at once (dirty, under writeback, mapped). v2 counters are
    /// hierarchical; v1's are only under their `total_` names.
    fn file_stat_keys(self) -> ([&'static str; 2], [&'static str; 3]) {
        match self {
            Self::V2 => (
                ["active_file", "inactive_file"],
                ["file_dirty", "file_writeback", "file_mapped"],
            ),
            Self::V1 => (
                ["total_active_file", "total_inactive_file"],
                ["total_dirty", "total_writeback", "total_mapped_file"],
            ),
        }
    }
}

/// Every cgroup level that can bound this process, from its own cgroup to the
/// root, on each hierarchy `/proc/self/cgroup` places it in: the v2 `0::` line
/// and a v1 line whose controllers include `memory`.
fn cgroup_levels(self_cgroup: Option<&str>) -> Vec<(Hierarchy, String)> {
    fn strip(path: &str) -> &str {
        path.split(" (deleted)").next().unwrap_or(path).trim_end()
    }
    let mut levels = Vec::new();
    for line in self_cgroup.unwrap_or_default().lines() {
        let (hierarchy, path) = if let Some(path) = line.strip_prefix("0::") {
            (Hierarchy::V2, path)
        } else if let Some((_, rest)) = line.split_once(':')
            && let Some((controllers, path)) = rest.split_once(':')
            && controllers.split(',').any(|name| name == "memory")
        {
            (Hierarchy::V1, path)
        } else {
            continue;
        };
        levels.extend(
            cgroup_ancestors(strip(path)).map(|level| (hierarchy, hierarchy.directory(level))),
        );
    }
    levels
}

/// Page-cache bytes charged to a cgroup level that the kernel can reclaim
/// without I/O and without evicting a mapped page, from its `memory.stat`:
/// `active_file + inactive_file - (dirty + writeback + mapped)`, saturating.
///
/// This is the per-cgroup analogue of the page-cache term of the kernel's
/// `MemAvailable` (`si_mem_available`), which counts both file LRU lists. Both
/// lists, not `inactive_file` alone: the split is an ageing heuristic, not a
/// reclaimability boundary. The kernel deactivates active pages as reclaim needs
/// them, and with the multi-generational LRU the "active" pages are just the two
/// youngest generations. On a warm host the cache is nearly all active (on the
/// #2042 machine, 83.8 GB active and 1.8 GB inactive of 85.6 GB `file`), so
/// `inactive_file` alone leaves a cgroup that the cache has filled to its limit
/// with almost no headroom. The subset is still conservative where `file` is
/// not: `file` includes shmem/tmpfs, which sits on the anon LRU and needs swap,
/// while the file LRU excludes it; dirty and writeback pages need I/O before
/// they can be dropped; and mapped pages (running binaries and libraries) would
/// fault straight back in, so all three are subtracted even where they overlap.
/// Unevictable pages have their own LRU and are never counted.
///
/// `0` when `memory.stat` or any of its keys is missing or unparseable: the
/// level's whole usage then counts against its limit, as it did before.
fn reclaimable_file_bytes(stat: Option<&str>, hierarchy: Hierarchy) -> u64 {
    let Some(stat) = stat else { return 0 };
    let value = |key: &str| {
        stat.lines().find_map(|line| {
            let (name, value) = line.split_once(' ')?;
            (name == key).then(|| value.trim().parse::<u64>().ok())?
        })
    };
    let (lru, pinned) = hierarchy.file_stat_keys();
    let sum = |keys: &[&str]| {
        keys.iter()
            .try_fold(0u64, |total, key| Some(total.saturating_add(value(key)?)))
    };
    match (sum(&lru), sum(&pinned)) {
        (Some(lru), Some(pinned)) => lru.saturating_sub(pinned),
        _ => 0,
    }
}

/// Bytes the process can still claim: the smaller of `MemAvailable` and the
/// remaining headroom of every cgroup level, v2 or v1, that has a numeric
/// limit. A level's headroom is its limit minus the part of its usage that is
/// not reclaimable page cache ([`reclaimable_file_bytes`]), so it never exceeds
/// the limit. `None` when nothing is readable.
pub(super) fn claimable_bytes(
    meminfo: Option<&str>,
    self_cgroup: Option<&str>,
    read: &dyn Fn(&str) -> Option<String>,
) -> Option<u64> {
    let mut claimable = meminfo.and_then(|text| {
        text.lines()
            .find_map(|line| line.strip_prefix("MemAvailable:"))
            .and_then(|rest| rest.split_ascii_whitespace().next()?.parse::<u64>().ok())
            .map(|kib| kib.saturating_mul(1024))
    });
    for (hierarchy, directory) in cgroup_levels(self_cgroup) {
        let number = |name: &str| {
            read(&format!("{directory}/{name}")).and_then(|text| text.trim().parse::<u64>().ok())
        };
        // v2 `max` is not a number: this level imposes no limit.
        let Some(limit) = number(hierarchy.limit()) else {
            continue;
        };
        let stat = read(&format!("{directory}/memory.stat"));
        let reclaimable = reclaimable_file_bytes(stat.as_deref(), hierarchy);
        let unreclaimable = number(hierarchy.usage())
            .unwrap_or(0)
            .saturating_sub(reclaimable);
        let remaining = limit.saturating_sub(unreclaimable);
        claimable = Some(claimable.map_or(remaining, |bytes| bytes.min(remaining)));
    }
    claimable
}

/// Paths from `path` up to and including the root: `/a/b` gives `/a/b`, `/a`, ``.
fn cgroup_ancestors(path: &str) -> impl Iterator<Item = &str> {
    let mut level = Some(path.trim_end_matches('/'));
    std::iter::from_fn(move || {
        let current = level?;
        level = current.rfind('/').map(|cut| &current[..cut]);
        Some(current)
    })
}

/// The hard memory limit of this process, in bytes: the smallest of `MemTotal`
/// and every numeric cgroup limit from the process's cgroup to the root, v2
/// (`memory.max`) and v1 (`memory.limit_in_bytes`, whose "no limit" is a value
/// near `i64::MAX` that `MemTotal` undercuts). Unlike [`claimable_bytes`] this
/// ignores current usage: it sizes a pool that lives for the instance, not a
/// one-shot plan. `None` when nothing is readable.
pub(crate) fn process_memory_limit_bytes(
    meminfo: Option<&str>,
    self_cgroup: Option<&str>,
    read: &dyn Fn(&str) -> Option<String>,
) -> Option<u64> {
    let mut limit = meminfo.and_then(|text| {
        text.lines()
            .find_map(|line| line.strip_prefix("MemTotal:"))
            .and_then(|rest| rest.split_ascii_whitespace().next()?.parse::<u64>().ok())
            .map(|kib| kib.saturating_mul(1024))
    });
    for (hierarchy, directory) in cgroup_levels(self_cgroup) {
        if let Some(bytes) = read(&format!("{directory}/{}", hierarchy.limit()))
            .and_then(|text| text.trim().parse::<u64>().ok())
        {
            limit = Some(limit.map_or(bytes, |current| current.min(bytes)));
        }
    }
    limit
}

/// The hard memory limit on this host, for this process.
pub(crate) fn host_process_memory_limit_bytes() -> Option<u64> {
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok();
    let cgroup = std::fs::read_to_string("/proc/self/cgroup").ok();
    process_memory_limit_bytes(meminfo.as_deref(), cgroup.as_deref(), &|path| {
        std::fs::read_to_string(path).ok()
    })
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

    fn files(entries: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> + use<> {
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
    fn a_pinned_budget_must_be_a_positive_number_of_bytes() {
        assert_eq!(parse_override(" 1073741824\n").unwrap(), GIB);
        for bad in ["", "0", "-1", "1G", "1.5", "abc"] {
            let error = parse_override(bad).unwrap_err().to_string();
            assert!(error.contains(BUDGET_ENV), "{bad:?}: {error}");
        }
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
        // A v1 memory line without a readable limit: only MemAvailable applies.
        assert_eq!(
            claimable_bytes(Some(&meminfo(10)), Some("4:memory:/x\n"), &read),
            Some(10 * GIB)
        );
    }

    /// A v2 `memory.stat` with the given GiB on each file LRU list and pinned.
    fn v2_stat(active: u64, inactive: u64, dirty: u64, writeback: u64, mapped: u64) -> String {
        format!(
            "anon {}\nfile {}\nshmem {}\nfile_mapped {}\nfile_dirty {}\n\
             file_writeback {}\ninactive_file {}\nactive_file {}\nunevictable {}\n",
            3 * GIB,
            (active + inactive + 5) * GIB,
            5 * GIB,
            mapped * GIB,
            dirty * GIB,
            writeback * GIB,
            inactive * GIB,
            active * GIB,
            7 * GIB,
        )
    }

    #[test]
    fn page_cache_charged_to_a_cgroup_counts_as_claimable() {
        // A 120 GiB cgroup whose 110 GiB of usage is mostly page cache, nearly all
        // of it on the active list: 100 GiB active and 2 GiB inactive, of which
        // 1 GiB dirty, 1 GiB under writeback and 1 GiB mapped. Its 5 GiB of shmem
        // and 7 GiB unevictable are not on the file LRU and stay charged.
        let stat = v2_stat(100, 2, 1, 1, 1);
        let read = files(&[
            ("/sys/fs/cgroup/agents/memory.max", "128849018880\n"),
            ("/sys/fs/cgroup/agents/memory.current", "118111600640\n"),
            ("/sys/fs/cgroup/agents/memory.stat", &stat),
        ]);
        let cgroup = Some("0::/agents\n");
        // Unreclaimable: 110 - (102 - 3) = 11 GiB, leaving 109 of the 120.
        assert_eq!(
            claimable_bytes(Some(&meminfo(126)), cgroup, &read),
            Some(109 * GIB)
        );
        // The same readings give the same answer, and MemAvailable still caps it.
        assert_eq!(
            claimable_bytes(Some(&meminfo(126)), cgroup, &read),
            Some(109 * GIB)
        );
        assert_eq!(
            claimable_bytes(Some(&meminfo(50)), cgroup, &read),
            Some(50 * GIB)
        );
    }

    #[test]
    fn dirty_writeback_and_mapped_pages_stay_charged_and_headroom_never_exceeds_the_limit() {
        let headroom = |stat: &str| {
            let read = files(&[
                ("/sys/fs/cgroup/a/memory.max", "17179869184"),
                ("/sys/fs/cgroup/a/memory.current", "15032385536"),
                ("/sys/fs/cgroup/a/memory.stat", stat),
            ]);
            claimable_bytes(Some(&meminfo(120)), Some("0::/a"), &read).map(|bytes| bytes / GIB)
        };
        // 16 GiB limit, 14 GiB used.
        assert_eq!(headroom(&v2_stat(8, 2, 0, 0, 0)), Some(12));
        assert_eq!(headroom(&v2_stat(8, 2, 3, 0, 0)), Some(9));
        assert_eq!(headroom(&v2_stat(8, 2, 1, 1, 1)), Some(9));
        // Pinned pages beyond the LRU total reclaim nothing.
        assert_eq!(headroom(&v2_stat(1, 1, 2, 1, 1)), Some(2));
        // A stat that claims more cache than the usage leaves the limit, no more.
        assert_eq!(headroom(&v2_stat(40, 2, 0, 0, 0)), Some(16));
    }

    #[test]
    fn a_missing_or_partial_memory_stat_charges_the_whole_usage() {
        let headroom = |stat: Option<&str>| {
            let mut entries = vec![
                ("/sys/fs/cgroup/a/memory.max", "17179869184"),
                ("/sys/fs/cgroup/a/memory.current", "15032385536"),
            ];
            entries.extend(stat.map(|text| ("/sys/fs/cgroup/a/memory.stat", text)));
            claimable_bytes(Some(&meminfo(120)), Some("0::/a"), &files(&entries))
        };
        assert_eq!(headroom(None), Some(2 * GIB));
        // Without the pinned counters the cache cannot be shown clean.
        let unaccounted = "active_file 8589934592\ninactive_file 2147483648\n";
        assert_eq!(headroom(Some(unaccounted)), Some(2 * GIB));
        let garbled = v2_stat(8, 2, 0, 0, 0).replace("file_dirty 0", "file_dirty x");
        assert_eq!(headroom(Some(&garbled)), Some(2 * GIB));
        assert_eq!(headroom(Some("")), Some(2 * GIB));
    }

    #[test]
    fn a_v1_level_counts_its_hierarchical_page_cache() {
        // v1 memory.stat: the plain keys count this cgroup alone, the `total_`
        // keys it and its descendants, as `memory.usage_in_bytes` does.
        let stat = format!(
            "cache 0\nmapped_file 0\ndirty 0\nwriteback 0\ninactive_file 0\nactive_file 0\n\
             total_cache {t}\ntotal_mapped_file {g}\ntotal_dirty {g}\ntotal_writeback 0\n\
             total_inactive_file {g}\ntotal_active_file {five}\n",
            t = 6 * GIB,
            g = GIB,
            five = 5 * GIB,
        );
        let cgroup = "12:cpu,cpuacct:/x\n4:memory:/x/y\n0::/x/y\n";
        let read = files(&[
            (
                "/sys/fs/cgroup/memory/x/y/memory.limit_in_bytes",
                "8589934592\n",
            ),
            (
                "/sys/fs/cgroup/memory/x/y/memory.usage_in_bytes",
                "7516192768\n",
            ),
            ("/sys/fs/cgroup/memory/x/y/memory.stat", &stat),
            (
                "/sys/fs/cgroup/memory/x/memory.limit_in_bytes",
                "9223372036854771712\n",
            ),
        ]);
        // 8 GiB limit, 7 GiB used, 6 - 2 GiB of it reclaimable: 5 GiB remain.
        assert_eq!(
            claimable_bytes(Some(&meminfo(120)), Some(cgroup), &read),
            Some(5 * GIB)
        );
        // Without its memory.stat the v1 level charges its whole usage.
        let bare = files(&[
            (
                "/sys/fs/cgroup/memory/x/y/memory.limit_in_bytes",
                "8589934592\n",
            ),
            (
                "/sys/fs/cgroup/memory/x/y/memory.usage_in_bytes",
                "7516192768\n",
            ),
        ]);
        assert_eq!(
            claimable_bytes(Some(&meminfo(120)), Some(cgroup), &bare),
            Some(GIB)
        );
    }

    #[test]
    fn the_tightest_level_wins_across_v1_and_v2_hierarchies() {
        // A hybrid host: the v2 level has 4 GiB left after its cache, the v1
        // memory controller 3 GiB, so the v1 level decides.
        let cgroup = "4:memory:/job\n0::/job\n";
        let read = files(&[
            ("/sys/fs/cgroup/job/memory.max", "8589934592"),
            ("/sys/fs/cgroup/job/memory.current", "6442450944"),
            ("/sys/fs/cgroup/job/memory.stat", &v2_stat(1, 1, 0, 0, 0)),
            (
                "/sys/fs/cgroup/memory/job/memory.limit_in_bytes",
                "4294967296",
            ),
            (
                "/sys/fs/cgroup/memory/job/memory.usage_in_bytes",
                "1073741824",
            ),
        ]);
        assert_eq!(
            claimable_bytes(Some(&meminfo(120)), Some(cgroup), &read),
            Some(3 * GIB)
        );
    }

    #[test]
    fn the_process_limit_is_the_tightest_level_ignoring_current_usage() {
        let meminfo = "MemTotal: 125829120 kB\nMemAvailable: 1 kB\n";
        let read = files(&[
            ("/sys/fs/cgroup/a/b/memory.max", "4294967296"),
            ("/sys/fs/cgroup/a/b/memory.current", "4000000000"),
            ("/sys/fs/cgroup/a/memory.max", "max"),
            ("/sys/fs/cgroup/memory.max", "68719476736"),
        ]);
        assert_eq!(
            process_memory_limit_bytes(Some(meminfo), Some("0::/a/b\n"), &read),
            Some(4 * GIB)
        );
        // No cgroup limit: the machine's memory.
        assert_eq!(
            process_memory_limit_bytes(Some(meminfo), Some("0::/a/b\n"), &files(&[])),
            Some(120 * GIB)
        );
        assert_eq!(process_memory_limit_bytes(None, None, &files(&[])), None);
    }

    #[test]
    fn a_v1_hierarchy_limit_applies_and_its_unlimited_value_does_not() {
        let meminfo = "MemTotal: 33554432 kB\n";
        let cgroup = "12:cpu,cpuacct:/x\n4:memory:/x/y\n";
        let limited = files(&[(
            "/sys/fs/cgroup/memory/x/y/memory.limit_in_bytes",
            "2147483648\n",
        )]);
        assert_eq!(
            process_memory_limit_bytes(Some(meminfo), Some(cgroup), &limited),
            Some(2 * GIB)
        );
        let unlimited = files(&[(
            "/sys/fs/cgroup/memory/x/y/memory.limit_in_bytes",
            "9223372036854771712\n",
        )]);
        assert_eq!(
            process_memory_limit_bytes(Some(meminfo), Some(cgroup), &unlimited),
            Some(32 * GIB)
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
                memory_budget: None,
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
