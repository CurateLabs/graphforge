//! Report facts about what a package's bytes are, taken from authenticated
//! lengths and from the Parquet footers the page walk already decodes.
//!
//! Nothing here depends on how the package was read: a file and a stream feed
//! the same functions, so they report the same facts.

use super::{Entry, ManifestComponent};
use crate::parquet_encoding_profile::ParquetShape;
use graphforge_core::portable::{PortableParquetSignals, PortablePlausibility};
use std::collections::BTreeMap;

/// The kind of bytes outside every component: BagIt tags, the inventory, the
/// semantic manifest and any future control entry.
const PACKAGE_CONTROL: &str = "package-control";
const GRAPH_DATA: &str = "graph-data";
const GRAPH_TREE: &str = "data/components/graph-data/graph-tree/";

/// Entry bytes by the kind of the component that declares the entry. An entry
/// no component declares is package control, so the values sum to the sum of
/// the entry lengths. Every declared kind has a total, zero when its components
/// list no file, so the keys cover the component histogram.
pub(super) fn bytes_by_kind(
    entries: &[Entry],
    components: &[ManifestComponent],
) -> BTreeMap<String, u64> {
    let kinds: BTreeMap<&str, &str> = components
        .iter()
        .flat_map(|component| {
            component
                .files
                .iter()
                .map(|file| (file.path.as_str(), component.kind.as_str()))
        })
        .collect();
    let mut bytes: BTreeMap<String, u64> = components
        .iter()
        .map(|component| (component.kind.clone(), 0))
        .collect();
    for entry in entries {
        let kind = kinds.get(entry.path.as_str()).unwrap_or(&PACKAGE_CONTROL);
        let total = bytes.entry((*kind).to_owned()).or_default();
        *total = total.saturating_add(entry.length);
    }
    bytes
}

/// The byte-ratio signals; the Parquet rows are added by whoever walks the
/// Parquet entries.
pub(super) fn plausibility(
    bytes: &BTreeMap<String, u64>,
    payload_bytes: u64,
) -> PortablePlausibility {
    let graph_data_bytes = bytes.get(GRAPH_DATA).copied().unwrap_or(0);
    let ratio = u128::from(graph_data_bytes) * 1_000_000 / u128::from(payload_bytes.max(1));
    PortablePlausibility {
        graph_data_bytes,
        graph_data_ratio_ppm: u64::try_from(ratio).unwrap_or(u64::MAX),
        parquet: None,
    }
}

/// Footer facts of the graph-tree Parquet files, accumulated one entry at a
/// time as the page walk admits them.
pub(super) struct ParquetSignals {
    signals: PortableParquetSignals,
    /// Footers larger than this are admitted but not reported.
    max_footer_bytes: u64,
}

impl ParquetSignals {
    pub(super) fn new(max_footer_bytes: u64) -> Self {
        Self {
            signals: PortableParquetSignals::default(),
            max_footer_bytes,
        }
    }

    /// Record one component entry: its package path and, when the page walk
    /// admitted it as Parquet, the shape its footer declares.
    pub(super) fn observe(&mut self, path: &str, shape: Option<ParquetShape>) {
        let Some(relative) = path.strip_prefix(GRAPH_TREE) else {
            return;
        };
        if !relative.to_ascii_lowercase().ends_with(".parquet") {
            return;
        }
        let signals = &mut self.signals;
        signals.files = signals.files.saturating_add(1);
        let Some(shape) = shape.filter(|shape| shape.footer_bytes <= self.max_footer_bytes) else {
            return;
        };
        signals.files_with_footer = signals.files_with_footer.saturating_add(1);
        signals.row_groups = signals.row_groups.saturating_add(shape.row_groups);
        let rows = if crate::topology_files::is_node(relative) {
            &mut signals.node_rows
        } else if crate::topology_files::is_edge(relative) {
            &mut signals.edge_rows
        } else {
            &mut signals.property_rows
        };
        *rows = rows.saturating_add(shape.rows);
    }

    pub(super) fn finish(self) -> PortableParquetSignals {
        self.signals
    }
}
