//! Typed semantic field commitments, independent of runtime catalog IDs.
use crate::{CancellationToken, GfError, GraphForge};
use arrow::{
    array::{Array, FixedSizeBinaryArray, ListArray, StringArray, StructArray},
    datatypes::{Field, Schema},
    record_batch::RecordBatch,
};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::Arc};
use uuid::Uuid;
pub(super) type Key = (String, Uuid, String);
pub(super) type Fields = BTreeMap<Key, [u8; 32]>;

pub(super) fn read(
    graph: &GraphForge,
    cancellation: &CancellationToken,
) -> Result<Fields, GfError> {
    let mut fields = BTreeMap::new();
    let mut field_bytes = 0;
    for (kind, query) in [
        (
            "node",
            "MATCH (n) RETURN n.node_uuid AS object_uuid, labels(n) AS labels, properties(n) AS properties",
        ),
        (
            "edge",
            "MATCH (s)-[r]->(t) RETURN r.edge_uuid AS object_uuid, s.node_uuid AS source_uuid, t.node_uuid AS target_uuid, type(r) AS relationship_type, properties(r) AS properties",
        ),
    ] {
        cancellation.checkpoint()?;
        let mut bytes = 0_usize;
        crate::slices::stream_branch(graph, query, cancellation, |batch| {
            bytes = bytes.saturating_add(batch.get_array_memory_size());
            if bytes > 64 * 1024 * 1024 {
                return Err(limit());
            }
            let ids = batch
                .column_by_name("object_uuid")
                .and_then(|a| a.as_any().downcast_ref::<FixedSizeBinaryArray>())
                .ok_or_else(invalid)?;
            for row in 0..batch.num_rows() {
                cancellation.checkpoint()?;
                let id = Uuid::from_slice(ids.value(row)).map_err(|_| invalid())?;
                insert(
                    &mut fields,
                    &mut field_bytes,
                    (kind.into(), id, "$object".into()),
                    Sha256::digest(b"present").into(),
                )?;
                for (field, array) in batch.schema().fields().iter().zip(batch.columns()) {
                    match field.name().as_str() {
                        "object_uuid" => {}
                        "properties" => {
                            for (name, digest) in properties(array.as_ref(), row)? {
                                insert(
                                    &mut fields,
                                    &mut field_bytes,
                                    (kind.into(), id, format!("property:{name}")),
                                    digest,
                                )?;
                            }
                        }
                        name => {
                            insert(
                                &mut fields,
                                &mut field_bytes,
                                (kind.into(), id, format!("${name}")),
                                fingerprint(array.slice(row, 1))?,
                            )?;
                        }
                    }
                }
                if fields.len().saturating_mul(256) > 64 * 1024 * 1024 {
                    return Err(limit());
                }
            }
            Ok(())
        })?;
    }
    domain_objects(graph, &mut fields, &mut field_bytes, cancellation)?;
    let claims = crate::research_claims::ledger::read_claims(&graph.generation_for_read()?)?;
    for row in claims.claims() {
        insert(
            &mut fields,
            &mut field_bytes,
            (
                "assertion".into(),
                row.assertion_uuid,
                "$research_classification".into(),
            ),
            row.fingerprint()
                .map_err(crate::knowledge::knowledge_error)?,
        )?;
    }
    for row in claims.relations() {
        insert(
            &mut fields,
            &mut field_bytes,
            (
                "assertion".into(),
                row.source_assertion_uuid,
                format!("$claim_relation:{}", row.relation_uuid),
            ),
            row.fingerprint()
                .map_err(crate::knowledge::knowledge_error)?,
        )?;
    }
    Ok(fields)
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
fn properties(array: &dyn Array, row: usize) -> Result<BTreeMap<String, [u8; 32]>, GfError> {
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
fn limit() -> GfError {
    GfError::Project {
        code: graphforge_core::ProjectErrorCode::ResourceLimit,
        message: "Branch semantic fields exceed creation limits".into(),
    }
}

fn domain_objects(
    graph: &GraphForge,
    fields: &mut Fields,
    field_bytes: &mut usize,
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
                insert(
                    fields,
                    field_bytes,
                    (kind.into(), id, "$object".into()),
                    Sha256::digest(b"present").into(),
                )?;
                for (field, array) in batch.schema().fields().iter().zip(batch.columns()) {
                    insert(
                        fields,
                        field_bytes,
                        (kind.into(), id, field.name().clone()),
                        fingerprint(array.slice(row, 1))?,
                    )?;
                }
                if fields.len() > 1_000_000 {
                    return Err(limit());
                }
            }
        }
    }
    Ok(())
}

// Reserve for map keys, digests and the expanded twelve-column baseline before
// inserting, including caller-controlled field-name lengths.
fn insert(
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
}
