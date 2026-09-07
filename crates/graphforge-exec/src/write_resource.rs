//! Explicit session-owned mutation authority, separate from read access.
use crate::read_resource::GraphReadContext;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::SessionState;
use graphforge_core::OntologyMode;
use graphforge_plan::GraphReadContract;
use graphforge_value::EntityTypeId;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub(crate) struct GraphWriteContext {
    pub resource: Arc<GraphReadContext>,
    pub writable: bool,
}

/// An admitted target retained by physical write operators.
#[derive(Debug, Clone)]
pub struct BoundWriteResource {
    pub(crate) health: crate::mutation::MutationHealth,
    pub(crate) dir: PathBuf,
    pub(crate) mode: OntologyMode,
    contract: GraphReadContract,
    pub(crate) composition: Option<String>,
    pub(crate) type_map: HashMap<EntityTypeId, String>,
}

impl BoundWriteResource {
    pub(crate) fn validate(&self, expected: Option<&GraphReadContract>) -> Result<()> {
        self.health
            .check()
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        if expected.is_some_and(|expected| *expected != self.contract) {
            return Err(DataFusionError::Plan(
                "GF_WRITE_RESOURCE_INCOMPATIBLE: logical identities".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_composition(&self, expected: Option<&str>) -> Result<()> {
        if expected.is_some() && expected != self.composition.as_deref() {
            return Err(DataFusionError::Plan(
                "GF_WRITE_RESOURCE_INCOMPATIBLE: semantic composition".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn same_authority(&self, other: &Self) -> bool {
        self.dir == other.dir
            && self.mode == other.mode
            && self.contract == other.contract
            && self.composition == other.composition
            && self.type_map == other.type_map
    }

    pub(crate) fn validate_catalog(&self, catalog: &graphforge_ir::RuntimeCatalog) -> Result<()> {
        let labels: std::collections::HashMap<_, _> = catalog
            .entity_type_names_with_ids()
            .map(|(id, name)| (graphforge_value::EntityTypeId::runtime(id), name.to_owned()))
            .collect();
        let relations: std::collections::HashMap<_, _> = catalog
            .relation_type_names_with_ids()
            .map(|(id, name)| {
                (
                    graphforge_value::RelationTypeId::runtime(id),
                    name.to_owned(),
                )
            })
            .collect();
        let actual_labels: std::collections::HashMap<_, _> = self
            .contract
            .labels
            .iter()
            .filter(|(id, _)| id.tagged().runtime_entity_id().is_some())
            .cloned()
            .collect();
        let actual_relations: std::collections::HashMap<_, _> = self
            .contract
            .relations
            .iter()
            .filter(|(id, _)| id.tagged().runtime_relation_id().is_some())
            .cloned()
            .collect();
        if labels != actual_labels || relations != actual_relations {
            return Err(DataFusionError::Plan(
                "GF_WRITE_RESOURCE_INCOMPATIBLE: mutation catalog identities".into(),
            ));
        }
        Ok(())
    }

    /// The explicit admitted destination, never recovered from a logical node.
    #[must_use]
    pub fn directory(&self) -> &Path {
        &self.dir
    }
}

pub(crate) fn required(
    state: &SessionState,
    contract: Option<&GraphReadContract>,
) -> Result<BoundWriteResource> {
    let binding = state
        .config()
        .get_extension::<GraphWriteContext>()
        .ok_or_else(|| {
            DataFusionError::Plan(
                "GF_WRITE_RESOURCE_MISSING: mutation requires a write target".into(),
            )
        })?;
    if !binding.writable {
        return Err(DataFusionError::Plan(
            "GF_WRITE_RESOURCE_READ_ONLY: session does not authorize writes".into(),
        ));
    }
    if binding.resource.dir.as_os_str().is_empty() {
        return Err(DataFusionError::Plan(
            "GF_WRITE_RESOURCE_MISSING: mutation requires a write target".into(),
        ));
    }
    binding
        .resource
        .validate(contract)
        .map_err(|e| DataFusionError::Plan(format!("GF_WRITE_RESOURCE_INCOMPATIBLE: {e}")))?;
    let lowerer = graphforge_rel::GraphPlanLowerer::new(
        Some(
            &graphforge_storage::lowering_snapshot(Some(&binding.resource.catalog), None)
                .map_err(|e| DataFusionError::Plan(e.to_string()))?,
        ),
        binding.resource.ontology.as_ref(),
    )
    .map_err(|e| DataFusionError::Plan(e.to_string()))?;
    Ok(BoundWriteResource {
        health: binding.resource.health.clone(),
        dir: binding.resource.dir.clone(),
        mode: binding.resource.mode,
        contract: lowerer.read_contract(),
        composition: binding
            .resource
            .catalog
            .semantic_composition_fingerprint()
            .map(str::to_owned),
        type_map: lowerer.entity_name_map(),
    })
}
