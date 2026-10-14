//! Destination node rows answered without reading node files.
//!
//! A hop whose destination is demanded for nothing beyond identity and labels
//! would otherwise read the node files only to project `node_id`, `node_uuid`
//! and `type_ids`. Both are already held elsewhere, keyed by ordinal:
//!
//! - labels in the provider's [`NodeLabelIndex`](graphforge_storage::NodeLabelIndex);
//! - the destination UUID in the v4 ordinal identity authority.
//!
//! The ordinal is the `node_id`, so the hop's CSR neighbours already carry the
//! `node_id` column. The edge columns are independent of all of this: the hop
//! materialises them from the edge files exactly as before, and only the
//! destination block is built here.

use super::V4OrdinalIdentitySession;
use crate::adjacency::AdjacencyProvider;
use arrow::array::ArrayRef;
use arrow::array::BooleanArray;
use arrow::array::FixedSizeBinaryBuilder;
use arrow::array::RecordBatch;
use arrow::array::UInt64Array;
use arrow::compute::filter;
use arrow::datatypes::SchemaRef;
use graphforge_core::GfError;
use graphforge_storage::V4OrdinalLookup;
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
    /// `None` when the projection holds a column neither the label index nor
    /// the identity authority can supply, so the node files must be read.
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
        Some(demanded)
    }

    /// The hop demands a label column, which only the label index supplies.
    fn labels(&self) -> bool {
        self.type_id || self.type_ids
    }
}

/// Destination rows answered without node files.
pub(super) struct SynthesisedDestination {
    pub(super) batch: RecordBatch,
    /// The rows came from the label index, as opposed to identity alone.
    pub(super) label_index_used: bool,
    /// One entry per bounded identity-authority request (none when the hop
    /// needed no identity lookup).
    pub(super) identity_lookups: Vec<V4OrdinalLookupMetrics>,
}

/// What the identity authority said about a set of requested ordinals.
pub(super) struct ResolvedIdentities {
    /// `node_uuid` of the owned ordinals, in request order; `None` when the
    /// hop does not demand it.
    uuids: Option<ArrayRef>,
    /// Per requested ordinal, whether the authority owns it.
    owned: Vec<bool>,
    lookups: Vec<V4OrdinalLookupMetrics>,
}

/// Ask `authority` about `requested` in requests of at most `max_requested`
/// ordinals.
///
/// The authority bounds one request and the node-file read this replaces had
/// no such bound, so a large frontier is looked up in chunks. An ordinal the
/// authority does not own (a deleted node, a tombstone) is reported in
/// `owned`; no UUID is invented for it.
///
/// # Errors
/// Fails if a request fails or answers a different number of ordinals than it
/// was given.
pub(super) fn resolve_identities<F>(
    requested: &[u64],
    max_requested: usize,
    want_uuids: bool,
    mut authority: F,
) -> Result<ResolvedIdentities, GfError>
where
    F: FnMut(&[u64]) -> Result<V4OrdinalLookup, GfError>,
{
    let mut builder =
        want_uuids.then(|| FixedSizeBinaryBuilder::with_capacity(requested.len(), 16));
    let mut owned = Vec::with_capacity(requested.len());
    let mut lookups = Vec::new();
    for chunk in requested.chunks(max_requested.max(1)) {
        let lookup = authority(chunk)?;
        if lookup.values.len() != chunk.len() {
            return Err(GfError::Execution(format!(
                "ordinal identity answered {} of {} requested destination node_ids",
                lookup.values.len(),
                chunk.len()
            )));
        }
        for uuid in &lookup.values {
            owned.push(uuid.is_some());
            if let (Some(builder), Some(uuid)) = (builder.as_mut(), uuid) {
                builder
                    .append_value(uuid.as_bytes())
                    .map_err(|error| GfError::Execution(error.to_string()))?;
            }
        }
        lookups.push(lookup.metrics);
    }
    Ok(ResolvedIdentities {
        uuids: builder.map(|mut builder| Arc::new(builder.finish()) as ArrayRef),
        owned,
        lookups,
    })
}

/// Assemble the destination batch from `columns`, which hold one row per
/// requested ordinal, and the identity authority's answer for those ordinals.
///
/// Rows the authority does not own are dropped from every column so the
/// caller's missing-destination check reports them, instead of a UUID being
/// invented or the node silently surviving. The batch is always returned, even
/// with no rows, because an absent batch means "no node files" to the caller.
///
/// # Errors
/// Fails if a column cannot be filtered or the columns disagree in length.
pub(super) fn assemble_batch(
    mut columns: Vec<(&'static str, ArrayRef)>,
    resolved: Option<ResolvedIdentities>,
) -> Result<(RecordBatch, Vec<V4OrdinalLookupMetrics>), GfError> {
    let mut lookups = Vec::new();
    if let Some(resolved) = resolved {
        if resolved.owned.iter().any(|owned| !owned) {
            let owned = BooleanArray::from(resolved.owned);
            for (_, column) in &mut columns {
                *column = filter(column, &owned)
                    .map_err(|error| GfError::Execution(error.to_string()))?;
            }
        }
        // The UUIDs were appended for owned ordinals only, so they are already
        // narrowed to the surviving rows.
        if let Some(uuids) = resolved.uuids {
            columns.push(("node_uuid", uuids));
        }
        lookups = resolved.lookups;
    }
    let batch = RecordBatch::try_from_iter(columns)
        .map_err(|error| GfError::Execution(error.to_string()))?;
    Ok((batch, lookups))
}

/// Synthesise the destination rows of `reached` without reading node files.
///
/// Labels come from the label index. `node_uuid` comes from the identity
/// authority, and so does the existence check of a hop that demands neither:
/// the authority is the source of which nodes exist, as the node files were.
/// A hop that demands labels but not `node_uuid` needs no identity request.
///
/// Returns `Ok(None)` when the hop demands a column neither source can supply,
/// when it needs an identity authority and has no ordinal identity session, or
/// when it needs the label index and the provider holds none for this
/// generation. The caller then reads the node files exactly as before.
///
/// Ordinals the label index or the identity authority lacks are omitted from
/// the batch, so the caller's missing-destination check fires with the same
/// error the file read produces.
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
    let labels = demanded.labels();
    let identity_lookup = demanded.node_uuid || !labels;
    if identity_lookup && ordinal_identities.is_none() {
        return Ok(None);
    }
    let mut ordinals = reached.iter().copied().collect::<Vec<_>>();
    ordinals.sort_unstable();
    let mut columns: Vec<(&'static str, ArrayRef)> = Vec::with_capacity(4);
    let requested = if labels {
        let Some(index) = provider.node_labels()? else {
            return Ok(None);
        };
        let gathered = index.gather(&ordinals, demanded.type_id, demanded.type_ids)?;
        let present = gathered.node_ids.values().to_vec();
        columns.push(("node_id", Arc::new(gathered.node_ids)));
        if let Some(type_id) = gathered.type_id {
            columns.push(("type_id", Arc::new(type_id)));
        }
        if let Some(type_ids) = gathered.type_ids {
            columns.push(("type_ids", Arc::new(type_ids)));
        }
        present
    } else {
        columns.push(("node_id", Arc::new(UInt64Array::from(ordinals.clone()))));
        ordinals
    };
    let resolved = ordinal_identities
        .filter(|_| identity_lookup)
        .map(|identities| {
            resolve_identities(
                &requested,
                identities.max_requested_ids(),
                demanded.node_uuid,
                |chunk| identities.lookup_node_uuids(chunk),
            )
        })
        .transpose()?;
    let (batch, identity_lookups) = assemble_batch(columns, resolved)?;
    Ok(Some(SynthesisedDestination {
        batch,
        label_index_used: labels,
        identity_lookups,
    }))
}
