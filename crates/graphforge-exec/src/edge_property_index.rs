//! Edge properties of traversed hops read through the persisted edge property
//! index (#1984).
//!
//! [`indexed_edge_property_batches`] is the index's counterpart of the
//! property-overlay read that `expand_exec::read_target_edge_properties`
//! performs for one route: it returns the rows of the requested edges in the
//! same shape (a batch of `edge_uuid` and the demanded properties), so that
//! `load_edge_prop_source` and `edge_prop_children` assemble hop order from
//! either source unchanged. One-hop expansion and variable-length expansion
//! both resolve their edge properties through `load_edge_prop_source`, so both
//! use the index.
//!
//! A route with no current index is not answered here; the caller reads the
//! property overlay for that route, exactly as before the index existed.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, FixedSizeBinaryBuilder, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use graphforge_storage::AuthenticatedPropertyInventory;
use graphforge_storage::edge_property_index::{EdgePropertyLookup, lookup_edge_properties};

use crate::GfError;

/// The requested edges' rows in the index of one edge property route.
///
/// - `Ok(None)`: no current index describes the route; read the overlay.
/// - `Ok(Some(vec![]))`: the route holds no property fragment.
/// - `Ok(Some(vec![batch]))`: `edge_uuid` and each demanded property the index
///   stores, one row per requested edge the route holds.
///
/// `owners` collects every edge the route owns; an edge two routes own is
/// corruption, not precedence.
///
/// # Errors
/// An index manifest or shard file that fails its checksum, or an edge two
/// routes both own.
pub(crate) fn indexed_edge_property_batches(
    inventory: &AuthenticatedPropertyInventory,
    dir: &Path,
    route: &str,
    targets: &BTreeSet<[u8; 16]>,
    property_names: &[String],
    owners: &mut BTreeSet<[u8; 16]>,
) -> Result<Option<Vec<RecordBatch>>, GfError> {
    let rows = match lookup_edge_properties(dir, inventory, route, targets, property_names)? {
        EdgePropertyLookup::Absent => return Ok(Some(Vec::new())),
        EdgePropertyLookup::NotIndexed => return Ok(None),
        EdgePropertyLookup::Indexed(rows) => rows,
    };
    if rows.uuids.iter().any(|uuid| !owners.insert(*uuid)) {
        return Err(GfError::Project {
            code: graphforge_core::ProjectErrorCode::ProjectCorrupt,
            message: "edge properties have multiple authenticated owners".into(),
        });
    }
    let mut uuids = FixedSizeBinaryBuilder::with_capacity(rows.uuids.len(), 16);
    for uuid in &rows.uuids {
        uuids
            .append_value(uuid)
            .map_err(|error| GfError::Execution(error.to_string()))?;
    }
    let mut fields = vec![Field::new(
        "edge_uuid",
        DataType::FixedSizeBinary(16),
        false,
    )];
    let mut columns: Vec<ArrayRef> = vec![Arc::new(uuids.finish())];
    for (name, column) in rows.columns {
        fields.push(Field::new(name, column.data_type().clone(), true));
        columns.push(column);
    }
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|error| GfError::Execution(error.to_string()))?;
    Ok(Some(vec![batch]))
}
