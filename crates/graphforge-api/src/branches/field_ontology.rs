//! Apply selected ontology fields through native compilation and data validation.
use super::field_application::{FieldChange, MutationContext, identity};
use crate::{
    CancellationToken, CompositionChangeRequest, CompositionDataDisposition, GfError, GraphForge,
    OperationId, WriteContext, branches::context_fields::context_identity,
};
use graphforge_ontology::{
    AuthoredModule, BridgeSetId, CompositionLimits, InventoryCompileRequest,
    bridge_document_digest, compile_inventory,
};
use graphforge_storage::{WorkspaceCompositionModule, WorkspaceOntologyComposition};

pub(crate) fn apply(
    destination: &mut GraphForge,
    source: &GraphForge,
    request: &MutationContext,
    items: &[FieldChange],
    cancellation: &CancellationToken,
) -> Result<(), GfError> {
    if !items
        .iter()
        .any(|item| item.unit.object_kind.starts_with("ontology"))
    {
        return Ok(());
    }
    let composition_selected = items.iter().any(|item| {
        item.unit.object_kind.starts_with("ontology_")
            || (item.unit.object_kind == "ontology" && item.unit.field == "profile_default")
    });
    if !composition_selected {
        return apply_legacy(destination, source, request, items, cancellation);
    }
    let before = destination.workspace_ontology_composition()?;
    let incoming = source.workspace_ontology_composition()?;
    let (modules, bridges, activation, profile_default) =
        merge_composition(before.as_ref(), incoming.as_ref(), items);
    let authored: Vec<_> = modules
        .into_iter()
        .map(|module| AuthoredModule {
            id: module.id,
            dependencies: module.dependencies,
            doc: module.document,
            allow_projected_identity: module.allow_projected_identity,
        })
        .collect();
    let bridge_ids = bridges
        .iter()
        .map(|bridge| {
            Ok(BridgeSetId {
                bridge_id: bridge.bridge_id.clone(),
                authored_version: bridge.authored_version.clone(),
                canonical_digest: bridge_document_digest(bridge)
                    .map_err(|error| GfError::Validation(error.clone()))?,
            })
        })
        .collect::<Result<Vec<_>, GfError>>()?;
    let compiled = compile_inventory(InventoryCompileRequest {
        modules: &authored,
        bridges: &bridge_ids,
        activation: &activation,
        profile_default,
        limits: CompositionLimits::default(),
        cancelled: Some(cancellation.flag()),
    })
    .map_err(|error| GfError::Validation(error.to_string()))?;
    let candidate = WorkspaceOntologyComposition::from_compiled(&compiled, bridges);
    let change = CompositionChangeRequest {
        context: WriteContext {
            operation_uuid: OperationId(identity(request.operation_uuid, "ontology")),
            actor_uuid: Some(request.actor_uuid),
        },
        expected_project_generation_uuid: destination.generation_for_read()?.generation_uuid(),
        expected_composition_fingerprint: before
            .as_ref()
            .map(|c| c.composition_fingerprint.clone()),
        candidate: candidate.clone(),
        data_disposition: CompositionDataDisposition::RequireConforming,
    };
    let preview = destination.preview_ontology_composition_change(&change, Some(cancellation))?;
    destination.publish_ontology_composition_change(&change, &preview, Some(cancellation))?;
    apply_legacy(destination, source, request, items, cancellation)
}

type MergedComposition = (
    Vec<WorkspaceCompositionModule>,
    Vec<graphforge_ontology::BridgeDocument>,
    Vec<graphforge_ontology::ActivationRecord>,
    graphforge_ontology::ActivationMode,
);
fn merge_composition(
    before: Option<&WorkspaceOntologyComposition>,
    incoming: Option<&WorkspaceOntologyComposition>,
    items: &[FieldChange],
) -> MergedComposition {
    let selected = |kind: &str, id: &str, field: &str| {
        items.iter().any(|item| {
            item.unit.object_kind == kind
                && item.unit.object_uuid == context_identity(kind, id)
                && item.unit.field == field
        })
    };
    let mut modules = before
        .as_ref()
        .map(|c| c.modules.clone())
        .unwrap_or_default();
    let mut bridges = before
        .as_ref()
        .map(|c| c.bridges.clone())
        .unwrap_or_default();
    let mut activation = before
        .as_ref()
        .map(|c| c.activation.clone())
        .unwrap_or_default();
    let module_selected = |m: &WorkspaceCompositionModule| {
        selected(
            "ontology_module",
            &m.id.ontology_id,
            &format!("version:{}", m.id.authored_version),
        )
    };
    modules.retain(|module| !module_selected(module));
    bridges.retain(|bridge| {
        !selected(
            "ontology_bridge",
            &bridge.bridge_id,
            &format!("version:{}", bridge.authored_version),
        )
    });
    activation.retain(|record| {
        !selected(
            "ontology_activation",
            &format!("{:?}:{}", record.scope, record.subject),
            "mode",
        )
    });
    if let Some(incoming) = &incoming {
        modules.extend(
            incoming
                .modules
                .iter()
                .filter(|module| module_selected(module))
                .cloned(),
        );
        bridges.extend(
            incoming
                .bridges
                .iter()
                .filter(|bridge| {
                    selected(
                        "ontology_bridge",
                        &bridge.bridge_id,
                        &format!("version:{}", bridge.authored_version),
                    )
                })
                .cloned(),
        );
        activation.extend(
            incoming
                .activation
                .iter()
                .filter(|record| {
                    selected(
                        "ontology_activation",
                        &format!("{:?}:{}", record.scope, record.subject),
                        "mode",
                    )
                })
                .cloned(),
        );
    }
    let profile_default = if selected("ontology", "workspace", "profile_default") {
        incoming
            .as_ref()
            .map_or(graphforge_ontology::ActivationMode::Exploratory, |c| {
                c.profile_default
            })
    } else {
        before
            .as_ref()
            .map_or(graphforge_ontology::ActivationMode::Exploratory, |c| {
                c.profile_default
            })
    };
    (modules, bridges, activation, profile_default)
}

fn apply_legacy(
    destination: &mut GraphForge,
    source: &GraphForge,
    request: &MutationContext,
    items: &[FieldChange],
    cancellation: &CancellationToken,
) -> Result<(), GfError> {
    let selected = |field: &str| {
        items.iter().any(|item| {
            item.unit.object_kind == "ontology"
                && item.unit.object_uuid == context_identity("ontology", "workspace")
                && item.unit.field == field
        })
    };
    let mut legacy = destination.workspace_ontology()?;
    let original = legacy.clone();
    let source_legacy = source.workspace_ontology()?;
    if selected("mode") {
        legacy.mode = source_legacy.mode;
    }
    if selected("document") {
        legacy.canonical_ontology = source_legacy.canonical_ontology;
        legacy.canonical_ontology_sha256 = source_legacy.canonical_ontology_sha256;
        legacy.source_format = source_legacy.source_format;
    }
    if legacy != original {
        let configuration = destination.workspace_configuration()?;
        let composition = destination.persisted_workspace_ontology_composition()?;
        let bindings = destination
            .semantic_storage_bindings
            .lock()
            .expect("semantic bindings")
            .clone();
        crate::workspace_ontology::publish_workspace_records(
            destination,
            identity(request.operation_uuid, "legacy_ontology"),
            Some(request.actor_uuid),
            &legacy,
            &configuration,
            composition.as_ref(),
            bindings.as_ref(),
            None,
            Some(cancellation),
        )?;
    }
    Ok(())
}
