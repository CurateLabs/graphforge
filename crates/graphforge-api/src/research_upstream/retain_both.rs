//! Retaining both preserves two existing list sequences without inventing scalar casts.
use super::invalid;
use crate::{GfError, IrLiteral};

pub(super) fn lists(local: IrLiteral, upstream: IrLiteral) -> Result<IrLiteral, GfError> {
    let (IrLiteral::List(mut local), IrLiteral::List(upstream)) = (local, upstream) else {
        return Err(invalid(
            "retain-both requires an existing multivalued property; use keep-local, adopt-upstream or an explanatory assertion for scalar conflicts",
        ));
    };
    if local.len().saturating_add(upstream.len()) > 65_536 {
        return Err(GfError::Api {
            code: graphforge_core::ApiErrorCode::ResourceLimit,
            message: "retain-both list exceeds 65536 values".into(),
        });
    }
    let mut kind = None;
    for value in local
        .iter()
        .chain(&upstream)
        .filter(|value| !matches!(value, IrLiteral::Null))
    {
        let actual = std::mem::discriminant(value);
        if kind.is_some_and(|expected| expected != actual) {
            return Err(invalid(
                "retain-both cannot combine incompatible native list element types; choose another explicit resolution",
            ));
        }
        kind = Some(actual);
    }
    // Lists are ordered and may contain intentional repetitions. Ontology and
    // native property admission still validate this value before publication.
    local.extend(upstream);
    Ok(IrLiteral::List(local))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn retain_both_preserves_order_and_repetitions_and_rejects_scalar_or_mixed_types() {
        assert_eq!(
            lists(
                IrLiteral::List(vec![IrLiteral::Int(1), IrLiteral::Int(1)]),
                IrLiteral::List(vec![IrLiteral::Int(2)])
            )
            .unwrap(),
            IrLiteral::List(vec![
                IrLiteral::Int(1),
                IrLiteral::Int(1),
                IrLiteral::Int(2)
            ])
        );
        assert!(lists(IrLiteral::Int(1), IrLiteral::Int(2)).is_err());
        assert!(
            lists(
                IrLiteral::List(vec![IrLiteral::Int(1)]),
                IrLiteral::List(vec![IrLiteral::Str("2".into())])
            )
            .is_err()
        );
    }
}

pub(super) fn apply(
    destination: &crate::GraphForge,
    source: &crate::GraphForge,
    key: &crate::branches::fields::Key,
    cancel: &crate::CancellationToken,
) -> Result<(), GfError> {
    use std::collections::HashMap;
    let (object, id) = match key.0.as_str() {
        "node" => ("n", "node_uuid"),
        "edge" => ("r", "edge_uuid"),
        _ => {
            return Err(invalid(
                "retain-both requires a native multivalued property; choose an explicit owner resolution",
            ));
        }
    };
    let property = key
        .2
        .strip_prefix("property:")
        .ok_or_else(|| invalid("retain-both requires a native multivalued property"))?;
    let mut params = HashMap::from([("id".into(), IrLiteral::Uuid(*key.1.as_bytes()))]);
    let read = |graph: &crate::GraphForge| {
        let pattern = crate::branches::semantic_fields::property_pattern(
            graph, &key.0, key.1, property, cancel,
        )?;
        crate::branches::field_application::scalar(
            graph,
            &format!(
                "MATCH {pattern} WHERE {object}.{id} = $id RETURN {object}.`{}` AS value",
                property.replace('`', "``")
            ),
            &params,
            cancel,
        )
    };
    let combined = lists(read(destination)?, read(source)?)?;
    let pattern = crate::branches::semantic_fields::property_pattern(
        destination,
        &key.0,
        key.1,
        property,
        cancel,
    )?;
    params.insert("value".into(), combined);
    cancel.checkpoint()?;
    destination.execute_with_params(
        &format!(
            "MATCH {pattern} WHERE {object}.{id} = $id SET {object}.`{}` = $value",
            property.replace('`', "``")
        ),
        &params,
    )?;
    Ok(())
}
