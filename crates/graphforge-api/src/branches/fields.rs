//! Typed semantic field commitments, independent of runtime catalog IDs.
use crate::{CancellationToken, GfError, GraphForge};
use arrow::{
    array::{Array, FixedSizeBinaryArray, ListArray, StringArray, StructArray},
    datatypes::{Field, Schema},
    record_batch::RecordBatch,
};
use graphforge_storage::concurrency_attribution::ObservedSha256 as Sha256;
use sha2::Digest;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use uuid::Uuid;
pub(crate) type Key = (String, Uuid, String);
pub(crate) type Fields = BTreeMap<Key, [u8; 32]>;

pub(crate) fn read(
    graph: &GraphForge,
    cancellation: &CancellationToken,
) -> Result<Fields, GfError> {
    read_selected(graph, None, cancellation)
}
pub(crate) type Objects = std::collections::BTreeSet<(String, Uuid)>;
pub(crate) fn read_selected(
    graph: &GraphForge,
    selected: Option<&Objects>,
    cancellation: &CancellationToken,
) -> Result<Fields, GfError> {
    let mut sink = MapSink::new();
    visit(graph, selected, true, cancellation, &mut sink)?;
    if let Some(selected) = selected {
        sink.fields
            .retain(|key, _| key.0 != "reference" || selected.contains(&(key.0.clone(), key.1)));
    }
    Ok(sink.fields)
}

/// Which streaming scan a decoded batch belongs to. The object scan and the
/// owner-qualified semantic scan each keep their own cumulative bound.
#[derive(Clone, Copy)]
pub(crate) enum Scan {
    Objects,
    Semantic,
}

/// Receiver for the typed semantic fields decoded by one streaming scan.
/// Implementations own their own bounds; [`MapSink`] reproduces the whole-map
/// write-path budgets, [`KeySink`] retains only the requested keys.
pub(crate) trait FieldSink {
    /// A later emission of a key replaces an earlier one.
    fn field(&mut self, key: Key, digest: [u8; 32]) -> Result<(), GfError>;
    /// Account cumulative decoded Arrow bytes of one streaming scan.
    fn scanned(&mut self, scan: Scan, bytes: usize) -> Result<(), GfError>;
    /// Keys this sink retains; the knowledge-domain whole-map ceiling counts them.
    fn len(&self) -> usize;
    /// Whether the sink needs this key's owner-qualified property value, so the
    /// semantic scan buffers only what its owner-conflict check must compare.
    fn wants(&self, key: &Key) -> bool;
}

/// Whole-graph field map collector for Branch write paths, reproducing the
/// cumulative scanned bounds and the reserved 64 MiB map budget byte for byte.
pub(crate) struct MapSink {
    pub(crate) fields: Fields,
    pub(crate) bytes: usize,
    scanned: [usize; 2],
}
impl MapSink {
    pub(crate) fn new() -> Self {
        Self {
            fields: Fields::new(),
            bytes: 0,
            scanned: [0; 2],
        }
    }
}
impl FieldSink for MapSink {
    fn field(&mut self, key: Key, digest: [u8; 32]) -> Result<(), GfError> {
        insert(&mut self.fields, &mut self.bytes, key, digest)
    }
    fn scanned(&mut self, scan: Scan, bytes: usize) -> Result<(), GfError> {
        let total = &mut self.scanned[scan as usize];
        *total = total.saturating_add(bytes);
        if *total > 64 * 1024 * 1024 {
            return Err(limit());
        }
        Ok(())
    }
    fn len(&self) -> usize {
        self.fields.len()
    }
    fn wants(&self, _key: &Key) -> bool {
        true
    }
}

/// Streaming lookup of requested keys. Every other field is still decoded, so
/// malformed values refuse as they do on the write paths, but only the
/// requested keys are retained: memory is the request plus one engine batch,
/// and scanned bytes are not charged separately because the opened view's
/// private query pool already bounds every batch.
struct KeySink<'a> {
    wanted: &'a BTreeSet<Key>,
    seen: BTreeMap<Key, [u8; 32]>,
}
impl FieldSink for KeySink<'_> {
    fn field(&mut self, key: Key, digest: [u8; 32]) -> Result<(), GfError> {
        if self.wanted.contains(&key) {
            self.seen.insert(key, digest);
        }
        Ok(())
    }
    fn scanned(&mut self, _scan: Scan, _bytes: usize) -> Result<(), GfError> {
        Ok(())
    }
    fn len(&self) -> usize {
        self.seen.len()
    }
    fn wants(&self, key: &Key) -> bool {
        self.wanted.contains(key)
    }
}

/// Decode every typed semantic field of the selected objects into `sink`.
/// Budgeted scans keep the shared Branch default slice budget; verification of
/// retained Versions streams under cancellation only.
fn visit(
    graph: &GraphForge,
    selected: Option<&Objects>,
    budgeted: bool,
    cancellation: &CancellationToken,
    sink: &mut dyn FieldSink,
) -> Result<(), GfError> {
    for (kind, pattern, identity, returns) in [
        (
            "node",
            "(n)",
            "n.node_uuid",
            "n.node_uuid AS object_uuid, labels(n) AS labels, properties(n) AS properties",
        ),
        (
            "edge",
            "(s)-[r]->(t)",
            "r.edge_uuid",
            "r.edge_uuid AS object_uuid, type(r) AS relationship_type, s.node_uuid AS source_uuid, t.node_uuid AS target_uuid, properties(r) AS properties",
        ),
    ] {
        if let Some(selected) = selected {
            for (_, id) in selected.iter().filter(|(k, _)| k == kind) {
                cancellation.checkpoint()?;
                let params = std::collections::HashMap::from([(
                    "id".into(),
                    graphforge_ir::IrLiteral::Uuid(*id.as_bytes()),
                )]);
                let query = format!("MATCH {pattern} WHERE {identity} = $id RETURN {returns}");
                stream(graph, &query, &params, budgeted, cancellation, |batch| {
                    collect(batch, kind, cancellation, sink)
                })?;
            }
        } else {
            stream(
                graph,
                &format!("MATCH {pattern} RETURN {returns}"),
                &std::collections::HashMap::new(),
                budgeted,
                cancellation,
                |batch| collect(batch, kind, cancellation, sink),
            )?;
        }
    }
    super::semantic_fields::read(graph, selected, budgeted, sink, cancellation)?;
    domain_objects(graph, sink, selected, cancellation)?;
    super::claim_fields::read(graph, sink, selected, cancellation)?;
    super::source_fields::read(graph, sink, selected, cancellation)?;
    super::context_fields::read(graph, sink, cancellation)?;
    Ok(())
}

/// The effective value, as [`read`] would hold it, of each requested key that
/// is present. Streams every field once under cancellation only, retaining
/// only the requested keys, so a Version of any field count is readable.
pub(crate) fn read_keys(
    graph: &GraphForge,
    wanted: &BTreeSet<Key>,
    cancellation: &CancellationToken,
) -> Result<BTreeMap<Key, [u8; 32]>, GfError> {
    let mut sink = KeySink {
        wanted,
        seen: BTreeMap::new(),
    };
    visit(graph, None, false, cancellation, &mut sink)?;
    Ok(sink.seen)
}

fn stream(
    graph: &GraphForge,
    query: &str,
    params: &std::collections::HashMap<String, graphforge_ir::IrLiteral>,
    budgeted: bool,
    cancellation: &CancellationToken,
    consume: impl FnMut(&RecordBatch) -> Result<(), GfError>,
) -> Result<(), GfError> {
    if budgeted {
        crate::slices::stream_branch_params(graph, query, params, cancellation, consume)
    } else {
        crate::slices::stream_branch_unbudgeted(graph, query, params, cancellation, consume)
    }
}
fn collect(
    batch: &RecordBatch,
    kind: &str,
    cancellation: &CancellationToken,
    sink: &mut dyn FieldSink,
) -> Result<(), GfError> {
    sink.scanned(Scan::Objects, batch.get_array_memory_size())?;
    let ids = batch
        .column_by_name("object_uuid")
        .and_then(|a| a.as_any().downcast_ref::<FixedSizeBinaryArray>())
        .ok_or_else(invalid)?;
    for row in 0..batch.num_rows() {
        cancellation.checkpoint()?;
        let id = Uuid::from_slice(ids.value(row)).map_err(|_| invalid())?;
        sink.field(
            (kind.into(), id, "$object".into()),
            Sha256::digest(b"present").into(),
        )?;
        for (field, array) in batch.schema().fields().iter().zip(batch.columns()) {
            match field.name().as_str() {
                "object_uuid" => {}
                "properties" => {
                    for (name, digest) in properties(array.as_ref(), row)? {
                        sink.field((kind.into(), id, format!("property:{name}")), digest)?;
                    }
                }
                name => sink.field(
                    (kind.into(), id, format!("${name}")),
                    fingerprint(array.slice(row, 1))?,
                )?,
            }
        }
    }
    Ok(())
}

fn fingerprint(array: arrow::array::ArrayRef) -> Result<[u8; 32], GfError> {
    if array.is_null(0) {
        return Ok(Sha256::digest(b"graphforge-branch-null/1").into());
    }
    if let Some(value) = array.as_any().downcast_ref::<StructArray>()
        && graphforge_value::heterogeneous::recognize(value.data_type())
            .map_err(|_| invalid())?
            .is_some()
    {
        return match graphforge_value::heterogeneous::decode_row(value, 0).map_err(|_| invalid())? {
            graphforge_value::heterogeneous::Decoded::Null => {
                Ok(Sha256::digest(b"graphforge-branch-null/1").into())
            }
            graphforge_value::heterogeneous::Decoded::Payload(payload) => {
                fingerprint(payload.slice(0, 1))
            }
            graphforge_value::heterogeneous::Decoded::Map(_) => {
                let fields = properties(value, 0)?;
                let mut digest = Sha256::new();
                digest.update(b"graphforge-branch-map/1");
                digest.update(serde_json::to_vec(&fields).map_err(|_| invalid())?);
                Ok(digest.finalize().into())
            }
        };
    }
    if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
        let values = list.value(0);
        let mut digest = Sha256::new();
        digest.update(b"graphforge-branch-list/1");
        digest.update((values.len() as u64).to_le_bytes());
        for row in 0..values.len() {
            digest.update(fingerprint(values.slice(row, 1))?);
        }
        return Ok(digest.finalize().into());
    }

    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        array.data_type().clone(),
        true,
    )]));
    let batch = RecordBatch::try_new(schema, vec![array]).map_err(|_| invalid())?;
    crate::canonical_arrow::result_fingerprint(&[batch])
        .map_err(|error| GfError::Validation(format!("Branch field fingerprint: {error}")))
}
pub(super) fn properties(
    array: &dyn Array,
    row: usize,
) -> Result<BTreeMap<String, [u8; 32]>, GfError> {
    let values = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| GfError::Validation("Branch properties must be a native struct".into()))?;
    if graphforge_value::heterogeneous::recognize(values.data_type())
        .map_err(|_| invalid())?
        .is_none()
    {
        return values
            .fields()
            .iter()
            .zip(values.columns())
            .map(|(field, array)| Ok((field.name().clone(), fingerprint(array.slice(row, 1))?)))
            .collect();
    }
    graphforge_value::heterogeneous::validate_array(values).map_err(|_| invalid())?;
    let map =
        match graphforge_value::heterogeneous::decode_row(values, row).map_err(|_| invalid())? {
            graphforge_value::heterogeneous::Decoded::Null => return Ok(BTreeMap::new()),
            graphforge_value::heterogeneous::Decoded::Map(map) => map,
            graphforge_value::heterogeneous::Decoded::Payload(_) => return Err(invalid()),
        };
    let map = map
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(invalid)?;
    if map.is_null(row) {
        return Ok(BTreeMap::new());
    }
    let entries = map.value(row);
    let entries = entries
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(invalid)?;
    let keys = entries
        .column_by_name(graphforge_value::heterogeneous::MAP_KEY)
        .and_then(|a| a.as_any().downcast_ref::<StringArray>())
        .ok_or_else(invalid)?;
    let values = entries
        .column_by_name(graphforge_value::heterogeneous::MAP_VALUE)
        .ok_or_else(invalid)?;
    (0..entries.len())
        .map(|i| Ok((keys.value(i).into(), fingerprint(values.slice(i, 1))?)))
        .collect()
}
fn invalid() -> GfError {
    GfError::Validation("invalid native Branch semantic field".into())
}
pub(super) fn limit() -> GfError {
    GfError::Project {
        code: graphforge_core::ProjectErrorCode::ResourceLimit,
        message: "Branch semantic fields exceed creation limits".into(),
    }
}

fn domain_objects(
    graph: &GraphForge,
    sink: &mut dyn FieldSink,
    selected: Option<&Objects>,
    cancellation: &CancellationToken,
) -> Result<(), GfError> {
    let generation = graph.generation_for_read()?;
    crate::branches::domain_bounds::preflight(&generation)?;
    for (family, kind, identity) in [
        ("sources", "source", "source_uuid"),
        ("artifacts", "artifact", "artifact_uuid"),
        ("assertions", "assertion", "assertion_uuid"),
    ] {
        let Some(snapshot) = generation.participant_snapshot("knowledge", family)? else {
            continue;
        };
        for batch in crate::knowledge::read_parquet(&snapshot.bytes)? {
            let ids = batch
                .column_by_name(identity)
                .and_then(|a| a.as_any().downcast_ref::<FixedSizeBinaryArray>())
                .ok_or_else(invalid)?;
            for row in 0..batch.num_rows() {
                cancellation.checkpoint()?;
                let id = Uuid::from_slice(ids.value(row)).map_err(|_| invalid())?;
                if selected.is_some_and(|set| !set.contains(&(kind.into(), id))) {
                    continue;
                }
                sink.field(
                    (kind.into(), id, "$object".into()),
                    Sha256::digest(b"present").into(),
                )?;
                for (field, array) in batch.schema().fields().iter().zip(batch.columns()) {
                    sink.field(
                        (kind.into(), id, field.name().clone()),
                        fingerprint(array.slice(row, 1))?,
                    )?;
                }
                if sink.len() > 1_000_000 {
                    return Err(limit());
                }
            }
        }
    }
    Ok(())
}

// Reserve for map keys, digests and the expanded twelve-column baseline before
// inserting, including caller-controlled field-name lengths.
pub(super) fn insert(
    fields: &mut Fields,
    bytes: &mut usize,
    key: Key,
    digest: [u8; 32],
) -> Result<(), GfError> {
    if !fields.contains_key(&key) {
        let cost = 1536_usize
            .saturating_add(key.0.len().saturating_mul(3))
            .saturating_add(key.2.len().saturating_mul(3));
        let next = bytes.saturating_add(cost);
        if next > 64 * 1024 * 1024 {
            return Err(limit());
        }
        *bytes = next;
    }
    fields.insert(key, digest);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn field_budget_refuses_before_insertion_and_charges_field_names() {
        let mut fields = Fields::new();
        let mut bytes = 64 * 1024 * 1024 - 1536;
        let key = (
            "assertion".into(),
            Uuid::now_v7(),
            "large-domain-field".into(),
        );
        assert!(insert(&mut fields, &mut bytes, key, [0; 32]).is_err());
        assert!(fields.is_empty());
        let mut bytes = 0;
        for i in 0..100_000 {
            if insert(
                &mut fields,
                &mut bytes,
                ("source".into(), Uuid::now_v7(), format!("field:{i}")),
                [0; 32],
            )
            .is_err()
            {
                assert!(fields.len() < 44_000);
                assert!(bytes <= 64 * 1024 * 1024);
                return;
            }
        }
        panic!("domain fields exceeded bounded allocation");
    }
    fn resource_limit(error: &GfError) -> bool {
        matches!(
            error,
            GfError::Project {
                code: graphforge_core::ProjectErrorCode::ResourceLimit,
                ..
            }
        )
    }
    fn key(i: usize) -> Key {
        ("node".into(), Uuid::from_u128(i as u128), "field".into())
    }
    #[test]
    fn key_sink_stays_bounded_where_the_map_sink_refuses() {
        let wanted: BTreeSet<Key> = (0..3).map(key).collect();
        let mut keys = KeySink {
            wanted: &wanted,
            seen: BTreeMap::new(),
        };
        for i in 0..200_000 {
            keys.field(key(i), [1; 32]).unwrap();
        }
        assert_eq!(keys.seen.len(), 3);
        assert_eq!(keys.len(), 3);

        let mut map = MapSink::new();
        let refused = (0..200_000).find_map(|i| map.field(key(i), [1; 32]).err());
        assert!(resource_limit(&refused.expect("map sink must refuse")));
        assert!(map.fields.len() < 44_000);
    }
    #[test]
    fn key_sink_holds_each_requested_key_once_at_its_last_value() {
        let wanted: BTreeSet<Key> = [key(1), key(2)].into();
        let mut sink = KeySink {
            wanted: &wanted,
            seen: BTreeMap::new(),
        };
        // One key streamed twice (as the generic and semantic scans may) while
        // the other never appears: the repeat must not stand in for it.
        sink.field(key(1), [1; 32]).unwrap();
        sink.field(key(1), [2; 32]).unwrap();
        sink.field(key(9), [3; 32]).unwrap();
        assert_eq!(sink.seen, BTreeMap::from([(key(1), [2; 32])]));
        assert!(sink.wants(&key(2)) && !sink.wants(&key(9)));

        // Requesting only the repeated key is satisfied by the repeat.
        let wanted: BTreeSet<Key> = [key(1)].into();
        let mut sink = KeySink {
            wanted: &wanted,
            seen: BTreeMap::new(),
        };
        sink.field(key(1), [1; 32]).unwrap();
        sink.field(key(1), [1; 32]).unwrap();
        assert_eq!(sink.seen, BTreeMap::from([(key(1), [1; 32])]));
    }
    #[test]
    fn map_sink_bounds_each_scan_by_itself() {
        const MIB: usize = 1024 * 1024;
        let mut sink = MapSink::new();
        sink.scanned(Scan::Objects, 40 * MIB).unwrap();
        sink.scanned(Scan::Semantic, 40 * MIB).unwrap();
        assert!(resource_limit(
            &sink.scanned(Scan::Objects, 40 * MIB).unwrap_err()
        ));
        let mut sink = MapSink::new();
        sink.scanned(Scan::Semantic, 40 * MIB).unwrap();
        assert!(resource_limit(
            &sink.scanned(Scan::Semantic, 40 * MIB).unwrap_err()
        ));
    }
}
