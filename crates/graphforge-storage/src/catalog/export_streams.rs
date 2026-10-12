//! Projected, streaming topology and property reads for analyst exports.
//!
//! An analyst export needs three narrow facts about the graph: which nodes it
//! selects (`node_id`, `node_uuid`, and label membership), which UUID belongs to
//! an edge surrogate (`edge_id`, `edge_uuid`), and the value of one edge
//! property per edge UUID. The general readers materialize whole tables, or
//! every column, and the node reader also builds two node-keyed hash sets. These
//! visitors decode only the named columns, hand each batch to the caller, and
//! retain nothing, so a caller can hold its own sorted representation and pay
//! only for that.

use super::admitted_parquet;
use arrow::array::{Array, FixedSizeBinaryArray, ListArray, RecordBatch, UInt32Array, UInt64Array};
use datafusion::error::DataFusionError;
use graphforge_core::{GfError, OntologyMode};
use graphforge_value::{EntityTypeId, PrimaryEntityTypeId};
use parquet::arrow::ProjectionMask;

/// Rows decoded per batch by the projected readers below.
pub const EXPORT_STREAM_BATCH_ROWS: usize = 64 * 1024;
const STREAM_BATCH_ROWS: usize = EXPORT_STREAM_BATCH_ROWS;

fn read_error(error: impl std::fmt::Display) -> GfError {
    GfError::Storage(error.to_string())
}

/// A read failure as a [`GfError`], keeping the typed error an admitted read
/// raised (an integrity refusal stays `GF_VALIDATION`) instead of flattening it
/// to a storage error.
fn data_fusion_error(error: DataFusionError) -> GfError {
    if let DataFusionError::External(inner) = error.find_root()
        && let Some(typed) = inner.downcast_ref::<GfError>()
    {
        return typed.clone();
    }
    read_error(error)
}

/// Visit `(node_id, node_uuid)` of every node of `files` that carries `label`
/// (every node when `label` is `None`), in file order.
///
/// Membership is read from the projected `type_ids` list; a legacy file that
/// has only the primary `type_id` is a singleton membership of that route's
/// label, exactly as the normalizing full read treats it. Only the identity
/// columns and the one membership column are decoded.
///
/// Canonical node tables keep `node_id` strictly increasing across their files
/// in order. That is checked from the values as they stream, so the visited ids
/// are a proof the caller can rely on to place nodes by position. A table that
/// breaks it, or holds a null identity, is refused. This read does not dedupe
/// UUIDs.
///
/// # Errors
/// Returns [`GfError::Storage`] when a file cannot be read, a column has the
/// wrong type, an identity is null, or `node_id` does not strictly increase; a
/// visitor error stops the read and is returned.
pub fn visit_node_identities(
    files: &crate::TopologyFiles,
    label: Option<EntityTypeId>,
    mut visit: impl FnMut(u64, &[u8; 16]) -> Result<(), GfError>,
) -> Result<(), GfError> {
    let mut previous: Option<u64> = None;
    for (path, _) in &files.nodes {
        let builder = admitted_parquet(path).map_err(data_fusion_error)?;
        let schema = builder.schema().clone();
        let membership_column = label.map(|_| {
            if schema.index_of("type_ids").is_ok() {
                "type_ids"
            } else {
                "type_id"
            }
        });
        let mut roots = vec![
            schema
                .index_of("node_id")
                .map_err(|_| read_error("node_id column is missing"))?,
            schema
                .index_of("node_uuid")
                .map_err(|_| read_error("node_uuid column is missing"))?,
        ];
        if let Some(name) = membership_column {
            roots.push(
                schema
                    .index_of(name)
                    .map_err(|_| read_error(format!("{name} column is missing")))?,
            );
        }
        let rows = u64::try_from(builder.metadata().file_metadata().num_rows()).unwrap_or(0);
        let mask = ProjectionMask::roots(builder.parquet_schema(), roots);
        let reader = builder
            .with_projection(mask)
            .with_batch_size(STREAM_BATCH_ROWS)
            .build()
            .map_err(read_error)?;
        for batch in reader {
            let batch = batch.map_err(read_error)?;
            visit_node_batch(&batch, label, membership_column, &mut previous, &mut visit)?;
        }
        crate::io_stats::record_node_full_read(rows);
    }
    Ok(())
}

/// Rows of every node file of `files`, summed from the Parquet footers.
///
/// Nothing is decoded. A caller that has just selected nodes compares its count
/// with this to learn whether the selection is the whole table.
///
/// # Errors
/// Returns [`GfError::Storage`] when a footer cannot be read.
pub fn count_node_rows(files: &crate::TopologyFiles) -> Result<u64, GfError> {
    let mut total = 0_u64;
    for (path, _) in &files.nodes {
        let rows = admitted_parquet(path)
            .map_err(data_fusion_error)?
            .metadata()
            .file_metadata()
            .num_rows();
        total = total
            .checked_add(u64::try_from(rows).map_err(|_| read_error("negative row count"))?)
            .ok_or_else(|| read_error("node row count overflows u64"))?;
    }
    Ok(total)
}

fn visit_node_batch(
    batch: &RecordBatch,
    label: Option<EntityTypeId>,
    membership_column: Option<&str>,
    previous: &mut Option<u64>,
    visit: &mut impl FnMut(u64, &[u8; 16]) -> Result<(), GfError>,
) -> Result<(), GfError> {
    let ids = batch
        .column_by_name("node_id")
        .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
        .ok_or_else(|| read_error("node_id is not UInt64"))?;
    let uuids = batch
        .column_by_name("node_uuid")
        .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
        .filter(|column| column.value_length() == 16)
        .ok_or_else(|| read_error("node_uuid is not FixedSizeBinary(16)"))?;
    if ids.null_count() != 0 || uuids.null_count() != 0 {
        return Err(read_error("node table holds a null node_id or node_uuid"));
    }
    let membership = match (label, membership_column) {
        (Some(label), Some(name)) => Some((label, name, batch.column_by_name(name))),
        _ => None,
    };
    for row in 0..batch.num_rows() {
        let id = ids.value(row);
        if previous.is_some_and(|previous| id <= previous) {
            return Err(read_error(format!(
                "node table node_id {id} does not strictly increase"
            )));
        }
        *previous = Some(id);
        if let Some((label, name, column)) = membership {
            let column = column.ok_or_else(|| read_error(format!("{name} column is missing")))?;
            if !carries_label(column.as_ref(), name, row, label)? {
                continue;
            }
        }
        let uuid: &[u8; 16] = uuids
            .value(row)
            .try_into()
            .map_err(|_| read_error("node_uuid is not 16 bytes"))?;
        visit(id, uuid)?;
    }
    Ok(())
}

fn carries_label(
    column: &dyn Array,
    name: &str,
    row: usize,
    label: EntityTypeId,
) -> Result<bool, GfError> {
    if name == "type_id" {
        let primary = column
            .as_any()
            .downcast_ref::<UInt32Array>()
            .filter(|values| values.null_count() == 0)
            .ok_or_else(|| read_error("type_id is not a non-null UInt32"))?;
        let primary = PrimaryEntityTypeId::decode(primary.value(row))
            .map_err(|error| read_error(format!("invalid node type identity: {error}")))?;
        return Ok(primary.label() == Some(label));
    }
    let lists = column
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| read_error("node topology type_ids is not List"))?;
    if lists.is_null(row) {
        return Err(read_error("node topology type_ids holds a null list"));
    }
    let members = lists
        .values()
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| read_error("node topology type_ids values are not UInt32"))?;
    let offsets = lists.value_offsets();
    let start = usize::try_from(offsets[row]).map_err(|_| read_error("negative list offset"))?;
    let end = usize::try_from(offsets[row + 1]).map_err(|_| read_error("negative list offset"))?;
    let encoded = label.encode();
    let mut found = false;
    for index in start..end {
        if members.is_null(index) {
            return Err(read_error("node topology type_ids holds a null membership"));
        }
        found |= members.value(index) == encoded;
    }
    Ok(found)
}

/// Visit `(edge_id, edge_uuid)` batches of every edge file that can hold the
/// edges of `rel_name`, in file order, decoding only those two columns.
///
/// The files read are the ones the relation's adjacency is built from: every
/// file for `"*"`, the relation's own file in a strict project, the relation's
/// file plus the shared exploratory file in an advisory project, and the
/// exploratory file in an exploratory one. A file may therefore hold edges the
/// relation does not own; the caller matches by `edge_id`. Edge files are
/// ascending in `edge_id`, which a caller may exploit but must not assume.
///
/// # Errors
/// Returns [`GfError::Storage`] when a file cannot be read or a column has the
/// wrong type or a null; a visitor error stops the read and is returned.
pub fn visit_edge_identities(
    files: &crate::TopologyFiles,
    rel_name: &str,
    mode: OntologyMode,
    mut visit: impl FnMut(&UInt64Array, &FixedSizeBinaryArray) -> Result<(), GfError>,
) -> Result<(), GfError> {
    for (route, path, _) in &files.edges {
        let wanted = match mode {
            OntologyMode::Exploratory => route == "_exploratory",
            OntologyMode::Strict => rel_name == "*" || route == rel_name,
            OntologyMode::Advisory => {
                rel_name == "*" || route == rel_name || route == "_exploratory"
            }
        };
        if !wanted {
            continue;
        }
        let builder = admitted_parquet(path).map_err(data_fusion_error)?;
        let schema = builder.schema().clone();
        let roots = vec![
            schema
                .index_of("edge_id")
                .map_err(|_| read_error("edge_id column is missing"))?,
            schema
                .index_of("edge_uuid")
                .map_err(|_| read_error("edge_uuid column is missing"))?,
        ];
        let rows = u64::try_from(builder.metadata().file_metadata().num_rows()).unwrap_or(0);
        let mask = ProjectionMask::roots(builder.parquet_schema(), roots);
        let reader = builder
            .with_projection(mask)
            .with_batch_size(STREAM_BATCH_ROWS)
            .build()
            .map_err(read_error)?;
        for batch in reader {
            let batch = batch.map_err(read_error)?;
            let ids = batch
                .column_by_name("edge_id")
                .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
                .ok_or_else(|| read_error("edge_id is not UInt64"))?;
            let uuids = batch
                .column_by_name("edge_uuid")
                .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
                .filter(|column| column.value_length() == 16)
                .ok_or_else(|| read_error("edge_uuid is not FixedSizeBinary(16)"))?;
            if ids.null_count() != 0 || uuids.null_count() != 0 {
                return Err(read_error("edge table holds a null edge_id or edge_uuid"));
            }
            visit(ids, uuids)?;
        }
        crate::io_stats::record_edge_full_read(rows);
    }
    Ok(())
}

/// Visit the newest live rows of one edge-property route in bounded batches,
/// decoding only the edge UUID key and `properties`.
///
/// This is the streaming form of
/// [`read_edge_properties_projected_from_inventory`](super::read_edge_properties_projected_from_inventory):
/// nothing is retained, so a caller that needs one value per edge keeps just
/// that value. `inventory` is the caller's pinned authority; `None` captures one
/// for the route.
///
/// # Errors
/// Returns [`GfError::Storage`] when the route cannot be read; a visitor error
/// stops the read and is returned unchanged.
pub fn visit_edge_property_batches(
    dir: &std::path::Path,
    inventory: Option<&crate::AuthenticatedPropertyInventory>,
    route: &str,
    properties: &[String],
    mut visit: impl FnMut(&RecordBatch) -> Result<(), GfError>,
) -> Result<(), GfError> {
    let selected: std::collections::BTreeSet<String> = properties.iter().cloned().collect();
    let mut failure: Option<GfError> = None;
    let outcome = super::property_readers::visit_property_overlay_batched_projected(
        dir,
        inventory,
        route,
        true,
        8_192,
        Some(&selected),
        |batch| match visit(batch) {
            Ok(()) => Ok(true),
            Err(error) => {
                failure = Some(error);
                Ok(false)
            }
        },
    );
    if let Some(error) = failure {
        return Err(error);
    }
    outcome.map(|_| ()).map_err(data_fusion_error)
}
