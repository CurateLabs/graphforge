//! One transaction owns authenticated closure adoption and its source receipt.
use super::ResolvedOntologyModule;
use crate::{
    CancellationToken, CompositionChangeRequest, CompositionDataDisposition, GfError, GraphForge,
    WriteContext,
};
use graphforge_ontology::{ActivationRecord, ActivationScope};
use graphforge_storage::WorkspaceOntologyComposition;
use serde::{Deserialize, Serialize};

const ADOPTIONS: &str = "graphforge.ontology_adoption:";

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AdoptionSource {
    module: graphforge_ontology::OntologyModuleId,
    repository: String,
    package_digest: String,
}

/// Durable outcome of adopting an exact discovered module and its full closure.
#[derive(Debug, Serialize)]
pub struct OntologyAdoptionReceipt {
    /// Exact root identity, without regenerating any authored ID.
    pub module: graphforge_ontology::OntologyModuleId,
    /// Source repository authenticated by discovery.
    pub repository: String,
    /// Verified source package digest, distinct from the root content digest.
    pub package_digest: String,
    /// Complete exact adopted closure.
    pub modules: Vec<graphforge_ontology::OntologyModuleId>,
    /// Generation authoritative after adoption (or an unchanged exact retry).
    pub project_generation_uuid: uuid::Uuid,
    /// Resulting complete composition fingerprint.
    pub composition_fingerprint: String,
    /// True when this source and closure were already adopted.
    pub already_adopted: bool,
}

impl GraphForge {
    /// Adopt a verified discovered closure and record its source in the same
    /// generation. Existing graph data, modules, metadata and enforcement remain
    /// subject to the ordinary composition preflight and publication owner.
    ///
    /// Exact repeat adoption is a no-op and retains the first verified source. Conflicting exact module content or
    /// dependencies fail before publication; no dependency is adopted alone.
    pub fn adopt_resolved_ontology_module(
        &mut self,
        resolved: &ResolvedOntologyModule,
        context: WriteContext,
        cancellation: Option<&CancellationToken>,
    ) -> Result<OntologyAdoptionReceipt, GfError> {
        if let Some(token) = cancellation {
            token.checkpoint()?;
        }
        let invalid = || {
            GfError::Validation(
                "discovered ontology closure conflicts with Project authority".into(),
            )
        };
        resolved.composition.compile()?;
        if !resolved
            .composition
            .modules
            .iter()
            .any(|module| module.id == resolved.module)
        {
            return Err(invalid());
        }
        let state = self
            .ontology_authority_state()
            .map_err(|error| GfError::Validation(error.to_string()))?;
        self.require_private_workspace()?;
        if self.read_only {
            return Err(GfError::Project {
                code: crate::ProjectErrorCode::ReadOnlyView,
                message: "historical views cannot adopt ontologies".into(),
            });
        }
        if *self
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned")
            != state.project_generation_uuid
        {
            return Err(GfError::Validation(
                "Project changed; reopen before ontology adoption".into(),
            ));
        }
        let candidate = merge_closure(self.workspace_ontology_composition()?, resolved)?;
        let mut metadata = self.research_project_metadata()?;
        let key = format!("{ADOPTIONS}{}", resolved.module.canonical_digest);
        let source = if let Some(prior) = metadata.extensions.get(&key) {
            // Keep the first authenticated source. Carrying packages can change
            // when the same exact module is exported from another generation.
            let source: AdoptionSource =
                serde_json::from_str(prior.as_str().ok_or_else(invalid)?).map_err(|_| invalid())?;
            if source.module != resolved.module {
                return Err(invalid());
            }
            source
        } else {
            AdoptionSource {
                module: resolved.module.clone(),
                repository: resolved.repository.canonical_name(),
                package_digest: resolved.package_digest.clone(),
            }
        };
        let already_adopted = metadata.extensions.contains_key(&key)
            && state.composition_fingerprint.as_ref() == Some(&candidate.composition_fingerprint);
        if !already_adopted {
            // The existing metadata extension contract admits scalar values only.
            metadata.extensions.insert(
                key,
                serde_json::Value::String(serde_json::to_string(&source).map_err(|_| invalid())?),
            );
            // Metadata validation bounds the durable record; its canonical bytes
            // bind provenance and retained unrelated metadata into exact replay.
            let metadata_bytes = metadata.to_canonical_json()?;
            let change = CompositionChangeRequest {
                context,
                expected_project_generation_uuid: state.project_generation_uuid,
                expected_composition_fingerprint: state.composition_fingerprint,
                candidate,
                data_disposition: CompositionDataDisposition::RequireConformingOperation {
                    operation: format!(
                        "ontology.adopt:{}",
                        String::from_utf8(metadata_bytes).map_err(|_| invalid())?
                    ),
                },
            };
            let preview = self.preview_ontology_composition_change(&change, cancellation)?;
            self.publish_ontology_composition_with_metadata(
                &change,
                &preview,
                cancellation,
                Some(&metadata),
            )?;
        }
        let state = self
            .ontology_authority_state()
            .map_err(|error| GfError::Validation(error.to_string()))?;
        Ok(OntologyAdoptionReceipt {
            module: resolved.module.clone(),
            repository: source.repository,
            package_digest: source.package_digest,
            modules: resolved
                .composition
                .modules
                .iter()
                .map(|module| module.id.clone())
                .collect(),
            project_generation_uuid: state.project_generation_uuid,
            composition_fingerprint: state.composition_fingerprint.ok_or_else(invalid)?,
            already_adopted,
        })
    }
}

/// Merge immutable identities and scoped source enforcement without replacing
/// unrelated Project authority. Compilation validates the combined graph once.
fn merge_closure(
    existing: Option<WorkspaceOntologyComposition>,
    resolved: &ResolvedOntologyModule,
) -> Result<WorkspaceOntologyComposition, GfError> {
    let invalid = || {
        GfError::Validation("discovered ontology closure conflicts with Project authority".into())
    };
    let mut candidate = existing.unwrap_or_else(|| WorkspaceOntologyComposition {
        contract_version: 1,
        modules: Vec::new(),
        bridges: Vec::new(),
        activation: Vec::new(),
        profile_default: resolved.composition.profile_default,
        composition_fingerprint: String::new(),
    });
    for module in &resolved.composition.modules {
        if let Some(existing) = candidate
            .modules
            .iter()
            .find(|existing| existing.id == module.id)
        {
            if existing != module {
                return Err(invalid());
            }
        } else {
            candidate.modules.push(module.clone());
            let subject = module.id.display_ref();
            let mode = resolved
                .composition
                .activation
                .iter()
                .find(|entry| entry.scope == ActivationScope::Module && entry.subject == subject)
                .map_or(resolved.composition.profile_default, |entry| entry.mode);
            candidate.activation.push(ActivationRecord {
                scope: ActivationScope::Module,
                subject,
                mode,
            });
        }
    }
    for bridge in &resolved.composition.bridges {
        if let Some(existing) = candidate.bridges.iter().find(|existing| {
            existing.bridge_id == bridge.bridge_id
                && existing.authored_version == bridge.authored_version
        }) {
            if existing != bridge {
                return Err(invalid());
            }
        } else {
            candidate.bridges.push(bridge.clone());
            let digest =
                graphforge_ontology::bridge_document_digest(bridge).map_err(GfError::Validation)?;
            let subject = format!("{}@{}#{digest}", bridge.bridge_id, bridge.authored_version);
            let mode = resolved
                .composition
                .activation
                .iter()
                .find(|entry| entry.scope == ActivationScope::Bridge && entry.subject == subject)
                .map_or(resolved.composition.profile_default, |entry| entry.mode);
            candidate.activation.push(ActivationRecord {
                scope: ActivationScope::Bridge,
                subject,
                mode,
            });
        }
    }
    WorkspaceOntologyComposition::from_authored(
        candidate.modules,
        candidate.bridges,
        candidate.activation,
        candidate.profile_default,
    )
}
