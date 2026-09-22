//! Apply reviewed graph fields through native typed mutation and construction.
use crate::{CancellationToken, GfError, GraphForge, IrLiteral};
use std::collections::{BTreeSet, HashMap};

pub(crate) fn apply(
    destination: &GraphForge,
    source: &GraphForge,
    items: &[FieldChange],
    cancellation: &CancellationToken,
) -> Result<(), GfError> {
    crate::branches::import_graph::incorporate_new(destination, source, cancellation)?;
    apply_labels(destination, source, items, cancellation, true)?;
    for item in items {
        cancellation.checkpoint()?;
        let (object, id) = match item.unit.object_kind.as_str() {
            "node" => ("n", "node_uuid"),
            "edge" => ("r", "edge_uuid"),
            _ => continue,
        };
        let mut params = HashMap::from([(
            "id".into(),
            IrLiteral::Uuid(*item.unit.object_uuid.as_bytes()),
        )]);
        if let Some(property) = item.unit.field.strip_prefix("property:") {
            let pattern = crate::branches::semantic_fields::property_pattern(
                if item.value_sha256.is_some() {
                    source
                } else {
                    destination
                },
                &item.unit.object_kind,
                item.unit.object_uuid,
                property,
                cancellation,
            )?;
            let selector = format!("MATCH {pattern} WHERE {object}.{id} = $id");
            let property = property.replace('`', "``");
            let query = if item.value_sha256.is_some() {
                let value = scalar(
                    source,
                    &format!("{selector} RETURN {object}.`{property}` AS value"),
                    &params,
                    cancellation,
                )?;
                params.insert("value".into(), value);
                format!("{selector} SET {object}.`{property}` = $value")
            } else {
                format!("{selector} REMOVE {object}.`{property}`")
            };
            destination.execute_with_params(&query, &params)?;
        }
    }
    apply_labels(destination, source, items, cancellation, false)?;
    // Edges are removed first. A node deletion must never detach unselected edges.
    for kind in ["edge", "node"] {
        for item in items.iter().filter(|item| {
            item.unit.object_kind == kind
                && item.unit.field == "$object"
                && item.value_sha256.is_none()
        }) {
            cancellation.checkpoint()?;
            let (pattern, object, id) = if kind == "node" {
                ("(n)", "n", "node_uuid")
            } else {
                ("()-[r]->()", "r", "edge_uuid")
            };
            destination.execute_with_params(
                &format!("MATCH {pattern} WHERE {object}.{id} = $id DELETE {object}"),
                &HashMap::from([(
                    "id".into(),
                    IrLiteral::Uuid(*item.unit.object_uuid.as_bytes()),
                )]),
            )?;
        }
    }
    Ok(())
}

fn apply_labels(
    destination: &GraphForge,
    source: &GraphForge,
    items: &[FieldChange],
    cancellation: &CancellationToken,
    additions: bool,
) -> Result<(), GfError> {
    for item in items.iter().filter(|item| {
        item.unit.object_kind == "node"
            && item.unit.field == "$labels"
            && item.value_sha256.is_some()
    }) {
        cancellation.checkpoint()?;
        let params = HashMap::from([(
            "id".into(),
            IrLiteral::Uuid(*item.unit.object_uuid.as_bytes()),
        )]);
        let selector = "MATCH (n) WHERE n.node_uuid = $id";
        let query = format!("{selector} RETURN labels(n) AS value");
        let desired = labels(scalar(source, &query, &params, cancellation)?)?;
        let current = labels(scalar(destination, &query, &params, cancellation)?)?;
        let (changes, owner, verb) = if additions {
            (desired.difference(&current), source, "SET")
        } else {
            (current.difference(&desired), destination, "REMOVE")
        };
        for label in changes {
            let name =
                crate::branches::import_semantic::resolve_name(destination, owner, "node", label)?
                    .unwrap_or_else(|| label.clone());
            destination.execute_with_params(
                &format!("{selector} {verb} n:`{}`", name.replace('`', "``")),
                &params,
            )?;
        }
    }
    Ok(())
}

pub(crate) fn scalar(
    graph: &GraphForge,
    query: &str,
    params: &HashMap<String, IrLiteral>,
    cancellation: &CancellationToken,
) -> Result<IrLiteral, GfError> {
    let mut value = None;
    crate::slices::stream_branch_params(graph, query, params, cancellation, |batch| {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        if batch.num_rows() != 1
            || value.is_some()
            || batch.get_array_memory_size() > 8 * 1024 * 1024
        {
            return Err(invalid(
                "reviewed property identity is ambiguous or exceeds bounds",
            ));
        }
        value = Some(graphforge_storage::decode_property_value(
            batch.column(0),
            batch.schema().field(0),
            0,
        )?);
        Ok(())
    })?;
    value.ok_or_else(|| invalid("reviewed property source object is unavailable"))
}

fn labels(value: IrLiteral) -> Result<BTreeSet<String>, GfError> {
    let IrLiteral::List(values) = value else {
        return Err(invalid("invalid native node labels"));
    };
    values
        .into_iter()
        .map(|value| match value {
            IrLiteral::Str(label) => Ok(label),
            _ => Err(invalid("invalid native node label")),
        })
        .collect()
}

/// One exact native field revision, shared by proposal review and upstream incorporation.
pub(crate) struct FieldChange {
    pub unit: crate::ResearchFieldIdentity,
    pub value_sha256: Option<[u8; 32]>,
}

pub(crate) struct MutationContext {
    pub operation_uuid: uuid::Uuid,
    pub actor_uuid: uuid::Uuid,
}

pub(crate) fn identity(operation: uuid::Uuid, role: &str) -> uuid::Uuid {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    digest.update(b"graphforge-native-field-mutation/1");
    digest.update(operation.as_bytes());
    digest.update(role.as_bytes());
    graphforge_core::canonical::uuid_v8(digest.finalize().into())
}

fn invalid(message: &str) -> GfError {
    GfError::Validation(message.into())
}

pub(crate) fn redact_properties(
    view: &GraphForge,
    keys: &BTreeSet<super::fields::Key>,
    cancellation: &CancellationToken,
) -> Result<(), GfError> {
    for key in super::fields::read(view, cancellation)?.keys() {
        let Some(property) = key.2.strip_prefix("property:") else {
            continue;
        };
        if keys.contains(key) {
            continue;
        }
        cancellation.checkpoint()?;
        let (_pattern, object, uuid) = match key.0.as_str() {
            "node" => ("(n)", "n", "node_uuid"),
            "edge" => ("()-[r]->()", "r", "edge_uuid"),
            _ => continue,
        };
        let pattern = crate::branches::semantic_fields::property_pattern(
            view,
            &key.0,
            key.1,
            property,
            cancellation,
        )?;
        // Property removal uses the native mutation path; do not remove labels or
        // interpret values in a binding-side representation.
        let query = format!(
            "MATCH {pattern} WHERE {object}.{uuid} = $id REMOVE {object}.`{}`",
            property.replace('`', "``")
        );
        view.execute_with_params(
            &query,
            &HashMap::from([("id".into(), crate::IrLiteral::Uuid(*key.1.as_bytes()))]),
        )?;
    }
    Ok(())
}
