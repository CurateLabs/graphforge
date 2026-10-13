//! Destination node rows answered from the per-ordinal label index.
//!
//! A destination label test lowers to `array_has(type_ids, id)`, so a hop whose
//! destination is demanded for nothing beyond identity and labels would
//! otherwise read the node files only to project `type_ids`. The provider's
//! [`NodeLabelIndex`](graphforge_storage::NodeLabelIndex) already holds every
//! label set by ordinal, so those columns are gathered from it instead.

use super::V4OrdinalIdentitySession;
use crate::adjacency::AdjacencyProvider;
use arrow::array::ArrayRef;
use arrow::array::BooleanArray;
use arrow::array::FixedSizeBinaryBuilder;
use arrow::array::RecordBatch;
use arrow::compute::filter;
use arrow::datatypes::SchemaRef;
use graphforge_core::GfError;
use graphforge_storage::V4OrdinalLookupMetrics;
use std::collections::HashSet;
use std::sync::Arc;

/// Which destination columns a hop demands, by name in the output schema.
struct DemandedColumns {
    node_uuid: bool,
    type_id: bool,
    type_ids: bool,
}

impl DemandedColumns {
    /// `None` when the projection holds a column the label index cannot supply
    /// or holds no label column at all, so the node files must be read.
    fn classify(out_schema: &SchemaRef, edge_end: usize, projection: &[usize]) -> Option<Self> {
        let mut demanded = Self {
            node_uuid: false,
            type_id: false,
            type_ids: false,
        };
        for offset in projection {
            match out_schema.fields().get(edge_end + offset)?.name().as_str() {
                "node_id" => {}
                "node_uuid" => demanded.node_uuid = true,
                "type_id" => demanded.type_id = true,
                "type_ids" => demanded.type_ids = true,
                _ => return None,
            }
        }
        (demanded.type_id || demanded.type_ids).then_some(demanded)
    }
}

/// Destination rows answered from the label index, with the identity lookup
/// that supplied `node_uuid` (absent when the hop does not demand it).
pub(super) struct SynthesisedDestination {
    pub(super) batch: RecordBatch,
    pub(super) identity_lookup: Option<V4OrdinalLookupMetrics>,
}

/// Synthesise the destination rows of `reached` from the label index.
///
/// Returns `Ok(None)` when the hop demands a column the index cannot supply,
/// when the provider holds no index for this generation, or when the identity
/// column is demanded without an ordinal identity session. The caller then
/// reads the node files exactly as before.
///
/// Ordinals the index lacks (a deleted node, a tombstone) are omitted from the
/// batch, so the caller's missing-destination check fires with the same error
/// the file read produces. The batch is always returned, even with no rows,
/// because an absent batch means "no node files" to the caller.
///
/// # Errors
/// Fails if the index cannot be built or an identity lookup fails.
pub(super) fn synthesise_destination_batch(
    provider: &dyn AdjacencyProvider,
    ordinal_identities: Option<&Arc<V4OrdinalIdentitySession>>,
    out_schema: &SchemaRef,
    edge_end: usize,
    projection: &[usize],
    reached: &HashSet<u64>,
) -> Result<Option<SynthesisedDestination>, GfError> {
    let Some(demanded) = DemandedColumns::classify(out_schema, edge_end, projection) else {
        return Ok(None);
    };
    if demanded.node_uuid && ordinal_identities.is_none() {
        return Ok(None);
    }
    let Some(index) = provider.node_labels()? else {
        return Ok(None);
    };
    let mut ordinals = reached.iter().copied().collect::<Vec<_>>();
    ordinals.sort_unstable();
    let gathered = index.gather(&ordinals, demanded.type_id, demanded.type_ids)?;
    let present = gathered.node_ids.values().to_vec();
    let mut columns: Vec<(&str, ArrayRef)> = vec![("node_id", Arc::new(gathered.node_ids))];
    if let Some(type_id) = gathered.type_id {
        columns.push(("type_id", Arc::new(type_id)));
    }
    if let Some(type_ids) = gathered.type_ids {
        columns.push(("type_ids", Arc::new(type_ids)));
    }
    let mut identity_lookup = None;
    if let (true, Some(identities)) = (demanded.node_uuid, ordinal_identities) {
        let lookup = identities.lookup_node_uuids(&present)?;
        let mut builder = FixedSizeBinaryBuilder::with_capacity(present.len(), 16);
        let mut keep = Vec::with_capacity(present.len());
        for uuid in &lookup.values {
            match uuid {
                Some(uuid) => {
                    builder
                        .append_value(uuid.as_bytes())
                        .map_err(|error| GfError::Execution(error.to_string()))?;
                    keep.push(true);
                }
                None => keep.push(false),
            }
        }
        identity_lookup = Some(lookup.metrics);
        let uuids: ArrayRef = Arc::new(builder.finish());
        columns.push(("node_uuid", uuids));
        if keep.iter().any(|kept| !kept) {
            // The identity authority disowns a node the label index lists. Drop
            // it so the caller reports it missing instead of inventing a UUID;
            // `node_uuid` holds only the kept rows, so it is already narrowed.
            let keep = BooleanArray::from(keep);
            for (name, column) in &mut columns {
                if *name != "node_uuid" {
                    *column = filter(column, &keep)
                        .map_err(|error| GfError::Execution(error.to_string()))?;
                }
            }
        }
    }
    let batch = RecordBatch::try_from_iter(columns)
        .map_err(|error| GfError::Execution(error.to_string()))?;
    Ok(Some(SynthesisedDestination {
        batch,
        identity_lookup,
    }))
}
