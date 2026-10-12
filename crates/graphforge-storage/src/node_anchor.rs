//! Narrow a node-topology scan to the nodes a property-equality index selects
//! (#1983).
//!
//! `MATCH (n:L {id: $x})` joins the node topology to the property route that
//! holds `id`. Where a current [equality index](crate::property_equality_index)
//! answers `id = $x`, the nodes that can survive that join are exactly the ones
//! the index names, so the topology scan need not read the rest:
//!
//! 1. the index returns the UUIDs of the matching nodes, reading the shards
//!    whose range holds the value;
//! 2. the topology identity columns resolve each UUID to its `node_id` and the
//!    fragment that holds it, pruned by row-group and page statistics;
//! 3. each holding fragment decodes exactly those rows by ordinal.
//!
//! The scan above still applies the label and the join still applies the
//! equality, so the answer is the one the unanchored scan gives. Only the rows
//! read change: they grow with the matches, not with the graph.
//!
//! An anchor is a hint. It applies only while the index is current when the
//! plan is made and again when the scan runs; otherwise the scan reads every
//! row exactly as it did before. A corrupt index is refused, never bypassed.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use datafusion::scalar::ScalarValue;
use graphforge_core::GfError;

use crate::parquet_scan::ParquetFragment;
use crate::property_overlay::{AuthenticatedPropertyInventory, EqualityValue, PropertyEquality};

/// What an anchored scan reads from each of its fragments.
#[derive(Clone)]
pub(crate) enum Resolution {
    /// No current index answers: every fragment is read whole.
    Unrestricted,
    /// The `node_id`s to read from each fragment, by fragment index. An empty
    /// set means the fragment is not opened.
    Fragments(Arc<Vec<HashSet<u64>>>),
}

/// A property equality that selects the nodes of a topology scan.
pub(crate) struct NodeAnchor {
    project: PathBuf,
    inventory: Arc<AuthenticatedPropertyInventory>,
    route: String,
    equality: PropertyEquality,
    /// Resolved once per plan: every partition of the scan shares it.
    resolved: Mutex<Option<Resolution>>,
}

impl std::fmt::Debug for NodeAnchor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NodeAnchor")
            .field("route", &self.route)
            .field("column", &self.equality.column)
            .finish_non_exhaustive()
    }
}

impl NodeAnchor {
    /// The anchor `column = value` on node route `route`, or `None` when the
    /// equality is not one an index covers or no inventory pins the route.
    pub(crate) fn new(
        project: PathBuf,
        inventory: Option<Arc<AuthenticatedPropertyInventory>>,
        route: String,
        column: &str,
        value: &ScalarValue,
    ) -> Option<Self> {
        let value = match value {
            ScalarValue::Int64(Some(value)) => EqualityValue::Int(*value),
            ScalarValue::Utf8(Some(value)) => EqualityValue::Str(value.clone()),
            ScalarValue::Boolean(Some(value)) => EqualityValue::Bool(*value),
            _ => return None,
        };
        if !crate::property_equality_index::is_indexed_column(column) {
            return None;
        }
        Some(Self {
            project,
            inventory: inventory?,
            route,
            equality: PropertyEquality {
                column: column.to_owned(),
                value,
            },
            resolved: Mutex::new(None),
        })
    }

    /// The indexed property column.
    pub(crate) fn column(&self) -> &str {
        &self.equality.column
    }

    /// Whether a current index answers the equality now.
    ///
    /// # Errors
    /// A manifest that fails its checksum or does not decode.
    pub(crate) fn is_current(&self) -> Result<bool, GfError> {
        crate::property_equality_index::is_current(
            &self.project,
            &self.inventory,
            &self.route,
            &self.equality,
        )
    }

    /// The rows to read from each of `fragments`, resolved on first use.
    ///
    /// # Errors
    /// A corrupt index, or a topology fragment that fails admission.
    pub(crate) fn resolve(&self, fragments: &[ParquetFragment]) -> Result<Resolution, GfError> {
        let mut resolved = self
            .resolved
            .lock()
            .map_err(|_| GfError::Storage("node anchor resolution lock poisoned".into()))?;
        if let Some(done) = resolved.as_ref() {
            return Ok(done.clone());
        }
        let outcome = self.resolve_now(fragments)?;
        *resolved = Some(outcome.clone());
        Ok(outcome)
    }

    fn resolve_now(&self, fragments: &[ParquetFragment]) -> Result<Resolution, GfError> {
        let Some(uuids) = crate::property_equality_index::lookup(
            &self.project,
            &self.inventory,
            &self.route,
            &self.equality,
        )?
        else {
            return Ok(Resolution::Unrestricted);
        };
        let mut by_fragment = vec![HashSet::new(); fragments.len()];
        if !uuids.is_empty() {
            let existing: Vec<(usize, &std::path::Path)> = fragments
                .iter()
                .enumerate()
                .filter(|(_, fragment)| fragment.exists)
                .map(|(index, fragment)| (index, fragment.path.as_path()))
                .collect();
            let paths: Vec<&std::path::Path> = existing.iter().map(|(_, path)| *path).collect();
            let found = crate::topology_identity::node_surrogates_by_fragment(&paths, &uuids)?;
            for ((index, _), ids) in existing.iter().zip(found) {
                by_fragment[*index].extend(ids);
            }
        }
        Ok(Resolution::Fragments(Arc::new(by_fragment)))
    }
}
