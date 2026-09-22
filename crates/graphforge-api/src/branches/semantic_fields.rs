//! Owner-qualified property inspection for composition-bound research.
use super::fields::{self, Fields, Objects};
use crate::{CancellationToken, GfError, GraphForge, IrLiteral};
use arrow::array::FixedSizeBinaryArray;
use std::collections::{BTreeMap, HashMap};
use uuid::Uuid;

pub(super) fn read(
    graph: &GraphForge,
    selected: Option<&Objects>,
    fields: &mut Fields,
    bytes: &mut usize,
    cancellation: &CancellationToken,
) -> Result<(), GfError> {
    let bindings = graph
        .semantic_storage_bindings
        .lock()
        .expect("semantic bindings")
        .clone();
    let Some(bindings) = bindings else {
        return Ok(());
    };
    let mut semantic = BTreeMap::new();
    let mut scanned = 0usize;
    for binding in &bindings.bindings {
        let (kind, object, identity, pattern) = match binding.route_kind {
            graphforge_storage::SemanticRouteKind::Entity => (
                "node",
                "n",
                "node_uuid",
                format!(
                    "(n:`{}`)",
                    binding.symbol.ambiguity_candidate().replace('`', "``")
                ),
            ),
            graphforge_storage::SemanticRouteKind::Relation => (
                "edge",
                "r",
                "edge_uuid",
                format!(
                    "()-[r:`{}`]->()",
                    binding.symbol.ambiguity_candidate().replace('`', "``")
                ),
            ),
            _ => continue,
        };
        cancellation.checkpoint()?;
        let identities: Vec<_> = selected.map_or_else(
            || vec![None],
            |set| {
                set.iter()
                    .filter(|(k, _)| k == kind)
                    .map(|(_, id)| Some(*id))
                    .collect()
            },
        );
        for selected_id in identities {
            let params = selected_id.map_or_else(HashMap::new, |id| {
                HashMap::from([("id".into(), IrLiteral::Uuid(*id.as_bytes()))])
            });
            let filter = if selected_id.is_some() {
                format!(" WHERE {object}.{identity} = $id")
            } else {
                String::new()
            };
            let query = format!(
                "MATCH {pattern}{filter} RETURN {object}.{identity} AS object_uuid, properties({object}) AS properties"
            );
            crate::slices::stream_branch_params(graph, &query, &params, cancellation, |batch| {
                scanned = scanned.saturating_add(batch.get_array_memory_size());
                if scanned > 64 * 1024 * 1024 {
                    return Err(fields::limit());
                }
                let ids = batch
                    .column_by_name("object_uuid")
                    .and_then(|a| a.as_any().downcast_ref::<FixedSizeBinaryArray>())
                    .ok_or_else(invalid)?;
                for row in 0..batch.num_rows() {
                    cancellation.checkpoint()?;
                    let id = Uuid::from_slice(ids.value(row)).map_err(|_| invalid())?;
                    for (property, digest) in fields::properties(
                        batch
                            .column_by_name("properties")
                            .ok_or_else(invalid)?
                            .as_ref(),
                        row,
                    )? {
                        let key = (kind.into(), id, format!("property:{property}"));
                        // A single native field cannot represent two distinct owner values.
                        if semantic.get(&key).is_some_and(|prior| *prior != digest) {
                            return Err(invalid());
                        }
                        semantic.insert(key, digest);
                    }
                }
                Ok(())
            })?;
        }
    }
    for (key, digest) in semantic {
        fields::insert(fields, bytes, key, digest)?;
    }
    Ok(())
}

pub(crate) fn property_pattern(
    graph: &GraphForge,
    kind: &str,
    id: Uuid,
    property: &str,
    cancellation: &CancellationToken,
) -> Result<String, GfError> {
    let (plain, object, identity, projection) = if kind == "node" {
        ("(n)", "n", "node_uuid", "labels(n)")
    } else {
        ("()-[r]->()", "r", "edge_uuid", "type(r)")
    };
    let Some(context) = graph.default_composition_snapshot() else {
        return Ok(plain.into());
    };
    let bindings = graph
        .semantic_storage_bindings
        .lock()
        .expect("semantic bindings")
        .clone();
    let Some(bindings) = bindings else {
        return Ok(plain.into());
    };
    let mut names = Vec::new();
    crate::slices::stream_branch_params(
        graph,
        &format!("MATCH {plain} WHERE {object}.{identity}=$id RETURN {projection}"),
        &HashMap::from([("id".into(), IrLiteral::Uuid(*id.as_bytes()))]),
        cancellation,
        |batch| {
            for row in 0..batch.num_rows() {
                let value = graphforge_storage::decode_property_value(
                    batch.column(0),
                    batch.schema().field(0),
                    row,
                )?;
                match value {
                    IrLiteral::Str(name) => names.push(name),
                    IrLiteral::List(values) => {
                        for value in values {
                            if let IrLiteral::Str(name) = value {
                                names.push(name);
                            }
                        }
                    }
                    _ => return Err(invalid()),
                }
            }
            Ok(())
        },
    )?;
    let route = if kind == "node" {
        graphforge_storage::SemanticRouteKind::Entity
    } else {
        graphforge_storage::SemanticRouteKind::Relation
    };
    let mut found = None;
    for binding in bindings.bindings.iter().filter(|b| {
        b.route_kind == route
            && names.iter().any(|n| {
                *n == b.symbol.display() || *n == b.route || *n == b.symbol.ambiguity_candidate()
            })
    }) {
        let name = binding.symbol.ambiguity_candidate();
        if let Ok((symbol, _)) =
            context.resolve_owned_property(binding.symbol.kind, &name, property)
        {
            if found.as_ref().is_some_and(|(prior, _)| *prior != symbol) {
                return Err(invalid());
            }
            found = Some((symbol, name));
        }
    }
    Ok(found.map_or_else(
        || plain.into(),
        |(_, name)| {
            if kind == "node" {
                format!("(n:`{}`)", name.replace('`', "``"))
            } else {
                format!("()-[r:`{}`]->()", name.replace('`', "``"))
            }
        },
    ))
}
fn invalid() -> GfError {
    GfError::Validation("research property has ambiguous or invalid semantic owner context".into())
}
