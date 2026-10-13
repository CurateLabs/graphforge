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
//! [`indexed_edge_property_batches_by_id`] answers the same question by the
//! surrogate `edge_id` a traversal already holds, for an index whose ids were
//! joined from the route's current topology. One-hop expansion uses it so that
//! reading `r.prop` never reads the edge file's `edge_uuid` column.
//!
//! A route with no current index is not answered here; the caller reads the
//! property overlay for that route, exactly as before the index existed.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, FixedSizeBinaryBuilder, RecordBatch, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use graphforge_storage::AuthenticatedPropertyInventory;
use graphforge_storage::edge_property_index::{
    EdgePropertyIdLookup, EdgePropertyLookup, lookup_edge_properties, lookup_edge_properties_by_id,
};

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

/// As [`indexed_edge_property_batches`], keyed by the surrogate `edge_id` a
/// traversal holds, so the 16-byte `edge_uuid` column of the edge file is never
/// read to find the properties of a hop.
///
/// - `Ok(None)`: no current index addresses the route by edge id (none exists,
///   it is stale, or it predates ids); the caller keys the read by UUID.
/// - `Ok(Some(vec![]))`: the route holds no property fragment.
/// - `Ok(Some(vec![batch]))`: `edge_id` and each demanded property the index
///   stores, one row per requested edge the route holds.
///
/// `owners` collects every edge id the route owns; an edge two routes own is
/// corruption, not precedence.
///
/// # Errors
/// An index manifest or shard file that fails its checksum, or an edge two
/// routes both own.
pub(crate) fn indexed_edge_property_batches_by_id(
    inventory: &AuthenticatedPropertyInventory,
    dir: &Path,
    route: &str,
    targets: &BTreeSet<u64>,
    property_names: &[String],
    owners: &mut BTreeSet<u64>,
) -> Result<Option<Vec<RecordBatch>>, GfError> {
    let rows = match lookup_edge_properties_by_id(dir, inventory, route, targets, property_names)? {
        EdgePropertyIdLookup::Absent => return Ok(Some(Vec::new())),
        EdgePropertyIdLookup::NotAddressable => return Ok(None),
        EdgePropertyIdLookup::Indexed(rows) => rows,
    };
    if rows.ids.iter().any(|id| !owners.insert(*id)) {
        return Err(GfError::Project {
            code: graphforge_core::ProjectErrorCode::ProjectCorrupt,
            message: "edge properties have multiple authenticated owners".into(),
        });
    }
    let mut fields = vec![Field::new("edge_id", DataType::UInt64, false)];
    let mut columns: Vec<ArrayRef> = vec![Arc::new(UInt64Array::from(rows.ids))];
    for (name, column) in rows.columns {
        fields.push(Field::new(name, column.data_type().clone(), true));
        columns.push(column);
    }
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|error| GfError::Execution(error.to_string()))?;
    Ok(Some(vec![batch]))
}
