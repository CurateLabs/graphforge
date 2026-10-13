//! Resolve edge UUIDs to the surrogate `edge_id` of their canonical topology
//! row (#1984).
//!
//! The edge property index is keyed by `edge_uuid`, but a traversal knows an
//! edge by its surrogate id (its position in the CSR and in the canonical edge
//! file). Publishing each index shard's `edge_id`s next to its UUIDs lets a
//! one-hop read address the index by the id it already holds, so the
//! 16-byte `edge_uuid` column is never forced. This resolver is how the
//! builders learn those ids: it answers sorted UUID windows from the topology
//! Parquet with the same statistics pruning identity probes use, decoding only
//! the pages whose UUID bounds can hold a requested UUID.
//!
//! Pruning only helps when the topology's UUID pages are narrow. Bulk
//! construction ranks edges by UUID, so its pages are disjoint and every window
//! decodes about its own rows. Topology written by mutations assigns ids in
//! arrival order, so its pages span the whole UUID space and a window would
//! decode them all. [`EdgeIdResolver::stabbing_number`] measures that overlap
//! once at open, and the builders refuse to resolve when it passes
//! [`MAX_STABBING_PAGES`] ([`EdgeIdResolver::is_point_readable`]): such a route
//! keeps its UUID-keyed index.

use std::path::Path;
use std::sync::Arc;

use graphforge_core::GfError;
use rayon::prelude::*;

use super::{Fragment, load_fragment, storage};

/// The most identity pages that may hold one UUID before a window is no longer
/// a cheap point read. A window decodes every page whose bounds hold any of its
/// UUIDs, so this bounds the decode amplification of resolving one window at a
/// small multiple of the window's own pages.
const MAX_STABBING_PAGES: u64 = 64;

/// UUID to `edge_id` lookups over one route's canonical topology files.
pub(crate) struct EdgeIdResolver {
    fragments: Vec<Arc<Fragment>>,
}

impl EdgeIdResolver {
    /// Open the footers of `paths`, the canonical edge files of one route.
    ///
    /// # Errors
    /// Returns an error when a file fails admission or lacks `edge_uuid` or
    /// `edge_id`.
    pub(crate) fn open(paths: &[&Path]) -> Result<Self, GfError> {
        let fragments = paths
            .par_iter()
            .map(|path| load_fragment(path, "edge_uuid", Some("edge_id")))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { fragments })
    }

    /// The most identity pages whose bounds hold one UUID, counting a page or
    /// row group without recorded bounds as holding every UUID. One means the
    /// files are disjoint, as bulk construction writes them.
    pub(crate) fn stabbing_number(&self) -> u64 {
        let mut unbounded = 0_u64;
        // `(value, is_close)`: at an equal value an open sorts before a close,
        // so inclusive bounds that touch count as overlapping.
        let mut events: Vec<([u8; 16], bool)> = Vec::new();
        let mut interval = |bounds: Option<([u8; 16], [u8; 16])>| match bounds {
            Some((min, max)) => {
                events.push((min, false));
                events.push((max, true));
            }
            None => unbounded += 1,
        };
        for fragment in &self.fragments {
            for group in fragment.groups.iter() {
                if group.pages.is_empty() {
                    interval(group.bounds);
                }
                for page in &group.pages {
                    interval(page.bounds.or(group.bounds));
                }
            }
        }
        events.sort_unstable();
        let (mut open, mut peak) = (0_u64, 0_u64);
        for (_, close) in events {
            if close {
                open -= 1;
            } else {
                open += 1;
                peak = peak.max(open);
            }
        }
        unbounded + peak
    }

    /// Whether a window of UUIDs decodes about its own pages: the files'
    /// [`stabbing_number`](Self::stabbing_number) is within the bound that
    /// keeps a join a set of point reads. When false, the builders keep the
    /// route's UUID-keyed index instead of joining ids.
    pub(crate) fn is_point_readable(&self) -> bool {
        self.stabbing_number() <= MAX_STABBING_PAGES
    }

    /// The `edge_id` of each of the strictly ascending `candidates`, in
    /// candidate order, or `None` for a UUID no topology row holds.
    ///
    /// # Errors
    /// Returns an error when `candidates` are not strictly ascending, when a
    /// file changed since its footer was read or cannot be decoded, or when two
    /// topology rows hold one UUID, which is corrupt topology.
    pub(crate) fn resolve(&self, candidates: &[[u8; 16]]) -> Result<Vec<Option<u64>>, GfError> {
        if !candidates.windows(2).all(|pair| pair[0] < pair[1]) {
            return Err(storage("edge UUIDs to resolve are not strictly ascending"));
        }
        let mut tasks = Vec::new();
        for (fragment_index, fragment) in self.fragments.iter().enumerate() {
            for group in 0..fragment.groups.len() {
                if let Some(selection) = fragment.plan_group(group, candidates) {
                    tasks.push((fragment_index, group, selection.rows));
                }
            }
        }
        // Workers carry the caller's I/O measurement, so the bytes a build reads
        // while joining are attributed to the operation that asked.
        let capture = crate::lifecycle_io::CaptureContext::current();
        let hits = tasks
            .par_iter()
            .map(|(fragment_index, group, rows)| {
                let _scope = capture.attach();
                let mut found = Vec::new();
                self.fragments[*fragment_index].scan_group(
                    *group,
                    rows.as_deref(),
                    candidates,
                    &mut found,
                )?;
                Ok(found)
            })
            .collect::<Result<Vec<Vec<(usize, u64)>>, GfError>>()?;
        let mut ids = vec![None; candidates.len()];
        for (index, id) in hits.into_iter().flatten() {
            if ids[index].replace(id).is_some() {
                return Err(storage("a UUID is held by more than one topology row"));
            }
        }
        Ok(ids)
    }
}
