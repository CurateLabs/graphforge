//! Preserve semantic UUIDs through the existing composite construction owner.
use crate::{
    CancellationToken, CompositeGraphMutation, CompositeTransactionRequest, GfError, GraphForge,
    IrLiteral, OperationId, WriteContext,
};
use arrow::{
    array::{Array, FixedSizeBinaryArray, ListArray, StringArray},
    record_batch::RecordBatch,
};
use std::collections::HashMap;
use uuid::Uuid;

pub(super) fn incorporate(
    destination: &GraphForge,
    source: &GraphForge,
    batch: &RecordBatch,
    row: usize,
    object: (&str, Uuid),
    cancellation: &CancellationToken,
) -> Result<bool, GfError> {
    let (kind, id) = object;
    let names = if kind == "node" {
        let list = batch
            .column_by_name("labels")
            .and_then(|a| a.as_any().downcast_ref::<ListArray>())
            .ok_or_else(invalid)?
            .value(row);
        let names = list
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(invalid)?;
        (0..names.len())
            .map(|i| names.value(i).to_owned())
            .collect::<Vec<_>>()
    } else {
        vec![
            batch
                .column_by_name("relationship_type")
                .and_then(|a| a.as_any().downcast_ref::<StringArray>())
                .ok_or_else(invalid)?
                .value(row)
                .to_owned(),
        ]
    };
    let mut semantic = false;
    let mut resolved_names = Vec::new();
    for name in names {
        let resolved = resolve_name(destination, source, kind, &name)?;
        semantic |= resolved.is_some();
        resolved_names.push(resolved.unwrap_or(name));
    }
    if !semantic {
        return Ok(false);
    }
    let name = resolved_names.first().ok_or_else(invalid)?.clone();
    let mutation = if kind == "node" {
        CompositeGraphMutation::CreateNode {
            node_uuid: id,
            label: name,
            properties: HashMap::new(),
        }
    } else {
        let uuid = |name| -> Result<Uuid, GfError> {
            Uuid::from_slice(
                batch
                    .column_by_name(name)
                    .and_then(|a| a.as_any().downcast_ref::<FixedSizeBinaryArray>())
                    .ok_or_else(invalid)?
                    .value(row),
            )
            .map_err(|_| invalid())
        };
        CompositeGraphMutation::CreateEdge {
            edge_uuid: id,
            rel_type: name,
            source_uuid: uuid("source_uuid")?,
            target_uuid: uuid("target_uuid")?,
            properties: HashMap::new(),
        }
    };
    destination.publish_composite_transaction_with_cancellation(
        CompositeTransactionRequest {
            contract_version: crate::COMPOSITE_TRANSACTION_CONTRACT_VERSION,
            context: WriteContext {
                operation_uuid: OperationId(Uuid::now_v7()),
                actor_uuid: None,
            },
            graph_mutations: vec![mutation],
            knowledge: crate::CompositeKnowledgeParticipants::default(),
        },
        Some(cancellation.clone()),
    )?;
    if kind == "node" {
        for label in resolved_names.iter().skip(1) {
            cancellation.checkpoint()?;
            destination.execute_with_params(
                &format!(
                    "MATCH (n) WHERE n.node_uuid=$id SET n:`{}`",
                    label.replace('`', "``")
                ),
                &HashMap::from([("id".into(), IrLiteral::Uuid(*id.as_bytes()))]),
            )?;
        }
    }
    copy_properties(destination, source, kind, id, cancellation)?;
    Ok(true)
}
fn copy_properties(
    destination: &GraphForge,
    source: &GraphForge,
    kind: &str,
    id: Uuid,
    cancellation: &CancellationToken,
) -> Result<(), GfError> {
    let fields =
        super::fields::read_selected(source, Some(&[(kind.to_owned(), id)].into()), cancellation)?;
    for key in fields.keys().filter(|key| key.0 == kind && key.1 == id) {
        let Some(name) = key.2.strip_prefix("property:") else {
            continue;
        };
        cancellation.checkpoint()?;
        let (_pattern, object, identity) = if kind == "node" {
            ("(n)", "n", "node_uuid")
        } else {
            ("()-[r]->()", "r", "edge_uuid")
        };
        let pattern =
            super::semantic_fields::property_pattern(source, kind, id, name, cancellation)?;
        let params = HashMap::from([("id".into(), IrLiteral::Uuid(*id.as_bytes()))]);
        let mut value = None;
        crate::slices::stream_branch_params(
            source,
            &format!(
                "MATCH {pattern} WHERE {object}.{identity} = $id RETURN {object}.`{}`",
                name.replace('`', "``")
            ),
            &params,
            cancellation,
            |batch| {
                if batch.num_rows() == 0 {
                    return Ok(());
                }
                if batch.num_rows() != 1 || value.is_some() {
                    return Err(invalid());
                }
                value = Some(graphforge_storage::decode_property_value(
                    batch.column(0),
                    batch.schema().field(0),
                    0,
                )?);
                Ok(())
            },
        )?;
        let value = value.ok_or_else(invalid)?;
        destination.execute_with_params(
            &format!(
                "MATCH {pattern} WHERE {object}.{identity} = $id SET {object}.`{}` = $value",
                name.replace('`', "``")
            ),
            &HashMap::from([
                ("id".into(), IrLiteral::Uuid(*id.as_bytes())),
                ("value".into(), value),
            ]),
        )?;
    }
    Ok(())
}

pub(crate) fn resolve_name(
    destination: &GraphForge,
    source: &GraphForge,
    kind: &str,
    name: &str,
) -> Result<Option<String>, GfError> {
    let route_kind = if kind == "node" {
        graphforge_storage::SemanticRouteKind::Entity
    } else {
        graphforge_storage::SemanticRouteKind::Relation
    };
    let bindings = source
        .semantic_storage_bindings
        .lock()
        .expect("semantic binding lock");
    let Some(binding) = bindings.as_ref().and_then(|b| {
        b.bindings.iter().find(|b| {
            b.route_kind == route_kind
                && (b.route == name
                    || b.symbol.display() == name
                    || b.symbol.ambiguity_candidate() == name)
        })
    }) else {
        return Ok(None);
    };
    let symbol = binding.symbol.clone();
    drop(bindings);
    let name = symbol.ambiguity_candidate();
    let context = destination
        .default_composition_snapshot()
        .ok_or_else(invalid)?;
    let (resolved, _) = context.resolve(symbol.kind, &name).map_err(|_| invalid())?;
    if resolved != graphforge_ir::SymbolBinding::Qualified(symbol) {
        return Err(invalid());
    }
    Ok(Some(name))
}

fn invalid() -> GfError {
    GfError::Validation(
        "selected graph symbol does not match destination semantic authority".into(),
    )
}
