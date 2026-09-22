//! Semantic ontology and citation fields extend the existing incorporated baseline.
use super::fields::{Fields, insert};
use crate::{CancellationToken, GfError, GraphForge};
use arrow::array::StringArray;
use sha2::{Digest, Sha256};
use uuid::Uuid;
pub(super) fn read(
    graph: &GraphForge,
    fields: &mut Fields,
    bytes: &mut usize,
    cancellation: &CancellationToken,
) -> Result<(), GfError> {
    cancellation.checkpoint()?;
    let ontology = graph.workspace_ontology()?;
    record(
        fields,
        bytes,
        "ontology",
        "workspace",
        "mode",
        &ontology.mode,
    )?;
    record(
        fields,
        bytes,
        "ontology",
        "workspace",
        "document",
        &ontology.canonical_ontology,
    )?;
    if let Some(composition) = graph.workspace_ontology_composition()? {
        record(
            fields,
            bytes,
            "ontology",
            "workspace",
            "profile_default",
            &composition.profile_default,
        )?;
        for module in &composition.modules {
            cancellation.checkpoint()?;
            record(
                fields,
                bytes,
                "ontology_module",
                &module.id.ontology_id,
                &format!("version:{}", module.id.authored_version),
                module,
            )?;
        }
        for bridge in &composition.bridges {
            record(
                fields,
                bytes,
                "ontology_bridge",
                &bridge.bridge_id,
                &format!("version:{}", bridge.authored_version),
                bridge,
            )?;
        }
        for activation in &composition.activation {
            record(
                fields,
                bytes,
                "ontology_activation",
                &format!("{:?}:{}", activation.scope, activation.subject),
                "mode",
                &activation.mode,
            )?;
        }
    }
    let references = super::reference::inspect(graph)?;
    for batch in references.batches {
        let ids = batch
            .column_by_name("reference_uuid")
            .and_then(|a| a.as_any().downcast_ref::<StringArray>())
            .ok_or_else(invalid)?;
        for row in 0..batch.num_rows() {
            cancellation.checkpoint()?;
            let id = Uuid::parse_str(ids.value(row)).map_err(|_| invalid())?;
            for name in ["source_version_uuid", "label"] {
                let value = batch
                    .column_by_name(name)
                    .and_then(|a| a.as_any().downcast_ref::<StringArray>())
                    .ok_or_else(invalid)?
                    .value(row);
                insert(
                    fields,
                    bytes,
                    ("reference".into(), id, name.into()),
                    Sha256::digest(value.as_bytes()).into(),
                )?;
            }
        }
    }
    Ok(())
}
fn record(
    fields: &mut Fields,
    bytes: &mut usize,
    kind: &str,
    identity: &str,
    field: &str,
    value: &impl serde::Serialize,
) -> Result<(), GfError> {
    let encoded = serde_json::to_vec(value).map_err(|_| invalid())?;
    if encoded.len() > 64 * 1024 * 1024 {
        return Err(GfError::Validation(
            "ontology comparison exceeds byte bound".into(),
        ));
    }
    let id = context_identity(kind, identity);
    insert(
        fields,
        bytes,
        (kind.into(), id, field.into()),
        Sha256::digest(&encoded).into(),
    )
}
pub(crate) fn context_identity(kind: &str, identity: &str) -> Uuid {
    graphforge_core::canonical::uuid_v8(
        Sha256::digest(format!("graphforge-research-context-field/1:{kind}:{identity}").as_bytes())
            .into(),
    )
}
fn invalid() -> GfError {
    GfError::Validation("invalid semantic context field".into())
}
