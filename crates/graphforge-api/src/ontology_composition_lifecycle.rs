//! Atomic project-generation lifecycle for ontology composition authority.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use graphforge_core::{GfError, ProjectErrorCode};
use graphforge_ontology::{
    ActivationMode, MigrationEngine, OntologyDoc, ResolveRequest, SymbolKind, TransformKind,
};
use graphforge_storage::{
    WORKSPACE_CAPABILITY_ID, WORKSPACE_ONTOLOGY_COMPOSITION_FAMILY, WorkspaceOntologyComposition,
    WorkspaceOntologyMode,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{CancellationToken, GraphForge, WriteContext};

/// Explicit disposition for runtime observations encountered by strict promotion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompositionDataDisposition {
    /// No runtime-only observations may remain.
    RequireConforming,
    /// The same data policy, bound to one Rust-owned semantic operation class.
    RequireConformingOperation {
        /// Stable operation token included in idempotency identity.
        operation: String,
    },
}

/// Request bound to one exact current generation and composition identity.
#[derive(Debug, Clone)]
pub struct CompositionChangeRequest {
    /// Idempotency and actor identity.
    pub context: WriteContext,
    /// Project generation previewed by the caller.
    pub expected_project_generation_uuid: Uuid,
    /// Current composition fingerprint, or `None` for an empty legacy project.
    pub expected_composition_fingerprint: Option<String>,
    /// Complete replacement authority; deltas are never durable authority.
    pub candidate: WorkspaceOntologyComposition,
    /// Explicit stored-data disposition.
    pub data_disposition: CompositionDataDisposition,
}

/// Stable attributable lifecycle diagnostic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompositionChangeDiagnostic {
    /// Stable machine code.
    pub code: String,
    /// Exact bounded subject.
    pub subject: String,
    /// Actionable remediation.
    pub remediation: String,
}

/// Deterministic preview bound to both authority identities.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompositionChangePreview {
    /// Expected current project generation.
    pub expected_project_generation_uuid: Uuid,
    /// Expected current composition.
    pub expected_composition_fingerprint: Option<String>,
    /// Candidate exact composition.
    pub candidate_composition_fingerprint: String,
    /// Canonical candidate content hash.
    pub candidate_sha256: String,
    /// Identity-sorted affected modules.
    pub affected_modules: Vec<String>,
    /// Identity-sorted affected bridges.
    pub affected_bridges: Vec<String>,
    /// Portable-v2 disposition until #841 adds authenticated package support.
    pub portable_compatibility: CompositionPortableReceipt,
    /// Bounded diagnostics; empty means publishable.
    pub diagnostics: Vec<CompositionChangeDiagnostic>,
}

/// Semantic portable-v2 verdict for a candidate composition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompositionPortableCompatibility {
    /// Canonical authority is representable, while current package readers
    /// deliberately fail closed on the required `ontology-composition@1` token.
    RepresentableReaderUnsupported,
    /// Candidate cannot be represented under the bounded semantic profile.
    Unrepresentable,
}

/// Pure ADR-0022 representability evidence; this does not construct or admit a package.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompositionPortableReceipt {
    /// Current reader disposition until #841 implements the authenticated codec.
    pub disposition: CompositionPortableCompatibility,
    /// Exact required feature token.
    pub required_feature: String,
    /// Exact transitive dependency selection rule.
    pub dependency_rule: String,
    /// Candidate composition identity.
    pub composition_fingerprint: String,
    /// Canonical participant payload bytes.
    pub canonical_bytes: u64,
    /// Canonically ordered module identities.
    pub modules: Vec<String>,
    /// Canonically ordered bridge identities.
    pub bridges: Vec<String>,
}

/// Durable publication receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompositionChangeReceipt {
    /// Published project generation.
    pub project_generation_uuid: Uuid,
    /// Published exact composition identity.
    pub composition_fingerprint: String,
    /// Canonical request content hash.
    pub candidate_sha256: String,
}

impl GraphForge {
    /// Read exact composition authority, or a virtual legacy projection without mutation.
    pub fn workspace_ontology_composition(
        &self,
    ) -> Result<Option<WorkspaceOntologyComposition>, GfError> {
        if let Some(composition) = self.persisted_workspace_ontology_composition()? {
            return Ok(Some(composition));
        }
        WorkspaceOntologyComposition::virtual_legacy(&self.workspace_ontology()?)
    }

    pub(crate) fn persisted_workspace_ontology_composition(
        &self,
    ) -> Result<Option<WorkspaceOntologyComposition>, GfError> {
        let current = self.generation_for_read()?;
        if let Some(snapshot) = current.participant_snapshot(
            WORKSPACE_CAPABILITY_ID,
            WORKSPACE_ONTOLOGY_COMPOSITION_FAMILY,
        )? {
            return WorkspaceOntologyComposition::from_canonical_json(&snapshot.bytes).map(Some);
        }
        Ok(None)
    }

    /// Preflight a complete replacement against exact current authority.
    #[allow(clippy::too_many_lines)] // keeps the bounded aggregate preflight in one ordered pass
    pub fn preview_ontology_composition_change(
        &self,
        request: &CompositionChangeRequest,
        cancellation: Option<&CancellationToken>,
    ) -> Result<CompositionChangePreview, GfError> {
        if let Some(token) = cancellation {
            token.checkpoint()?;
        }
        let current = self.generation_for_read()?;
        if current.generation_uuid() != request.expected_project_generation_uuid {
            return Err(GfError::Validation(
                "composition preview project generation is stale".into(),
            ));
        }
        let existing = self.workspace_ontology_composition()?;
        let existing_fingerprint = existing
            .as_ref()
            .map(|composition| composition.composition_fingerprint.clone());
        if existing_fingerprint != request.expected_composition_fingerprint {
            return Err(GfError::Validation(
                "composition preview fingerprint is stale".into(),
            ));
        }
        let (compiled, compile_error) = match request.candidate.compile() {
            Ok(compiled) => (Some(compiled), None),
            Err(error) => (None, Some(error.to_string())),
        };
        if let Some(token) = cancellation {
            token.checkpoint()?;
        }
        // Preview must remain capable of returning bounded diagnostics for an
        // invalid candidate; the validated canonical encoder intentionally
        // rejects such records before a receipt could otherwise be formed.
        let mut candidate_bytes = serde_json::to_vec(&request.candidate)
            .map_err(|_| GfError::Validation("candidate composition cannot be encoded".into()))?;
        candidate_bytes.push(b'\n');
        if candidate_bytes.len() > 16 * 1024 * 1024 {
            return Err(GfError::Validation(
                "candidate composition exceeds preview byte limit".into(),
            ));
        }
        let (runtime_symbols, catalog_drift) = runtime_symbols(self, cancellation)?;
        let mut diagnostics = Vec::new();
        if compiled.is_none() {
            diagnostics.push(CompositionChangeDiagnostic {
                code: "candidate_composition_invalid".into(),
                subject: request.candidate.composition_fingerprint.clone(),
                remediation: compile_error.unwrap_or_else(|| "repair candidate authority".into()),
            });
        }
        if catalog_drift {
            diagnostics.push(CompositionChangeDiagnostic {
                code: "runtime_catalog_generation_drift".into(),
                subject: current.generation_uuid().hyphenated().to_string(),
                remediation: "reopen the exact current generation before composition preflight"
                    .into(),
            });
        }
        if let Some(compiled) = &compiled {
            let identity_equivalent =
                identity_equivalent_upgrades(existing.as_ref(), &request.candidate);
            let current_bindings = self
                .semantic_storage_bindings
                .lock()
                .expect("semantic storage binding lock poisoned")
                .clone();
            if let Err(error) = graphforge_storage::SemanticStorageBindings::project_with_graph_scan_identity_equivalent(
                compiled,
                current_bindings.as_ref(),
                &self.dir,
                &identity_equivalent,
            ) {
                let retained = compiled
                    .modules
                    .iter()
                    .flat_map(|module| module.symbols.iter().cloned())
                    .collect::<HashSet<_>>();
                let removed = current_bindings
                    .as_ref()
                    .into_iter()
                    .flat_map(|bindings| bindings.bindings.iter())
                    .filter(|binding| !retained.contains(&binding.symbol))
                    .map(|binding| {
                        graphforge_storage::SemanticStorageBindings::binding_has_retained_data(
                            binding, &self.dir,
                        )
                        .map(|has_data| has_data.then_some(binding))
                    })
                    .collect::<Result<Vec<Option<_>>, _>>()?
                    .into_iter()
                    .flatten()
                    .take(64usize.saturating_sub(diagnostics.len()))
                    .collect::<Vec<_>>();
                if removed.is_empty() {
                    diagnostics.push(CompositionChangeDiagnostic {
                        code: "stored_semantic_data_incompatible".into(),
                        subject: request.candidate.composition_fingerprint.clone(),
                        remediation: error.to_string(),
                    });
                } else {
                    for binding in removed {
                        let owner = binding.owner.as_ref().map_or_else(
                            || "none".into(),
                            |owner| {
                                format!(
                                    "{}:{}:{}",
                                    owner.module.display_ref(),
                                    owner.kind.as_str(),
                                    owner.local_id
                                )
                            },
                        );
                        diagnostics.push(CompositionChangeDiagnostic {
                            code: "stored_semantic_data_incompatible".into(),
                            subject: format!(
                                "{}:{}:{} owner={owner}",
                                binding.symbol.module.display_ref(),
                                binding.symbol.kind.as_str(),
                                binding.symbol.local_id
                            ),
                            remediation: error.to_string(),
                        });
                    }
                }
            }
        }
        diagnostics.extend(migration_diagnostics(
            existing.as_ref(),
            &request.candidate,
            &runtime_symbols,
            cancellation,
        )?);
        diagnostics.extend(removed_authority_diagnostics(
            existing.as_ref(),
            &request.candidate,
            &runtime_symbols,
        ));
        let has_strict_scope = compiled.as_ref().is_some_and(|compiled| {
            compiled.profile_default == ActivationMode::Strict
                || compiled.modules.iter().any(|module| {
                    compiled.effective_module_mode(&module.id) == ActivationMode::Strict
                })
        });
        if has_strict_scope {
            let nonconforming = runtime_symbols.iter().filter(|symbol| {
                compiled
                    .as_ref()
                    .is_none_or(|compiled| !symbol_declared(compiled, symbol))
            });
            for symbol in nonconforming.take(64usize.saturating_sub(diagnostics.len())) {
                diagnostics.push(CompositionChangeDiagnostic {
                    code: "strict_runtime_observation".into(),
                    subject: symbol.clone(),
                    remediation: "declare the symbol before strict publication".into(),
                });
            }
        }
        let old_modules = existing
            .as_ref()
            .into_iter()
            .flat_map(|composition| composition.modules.iter())
            .map(|module| module.id.display_ref())
            .collect::<BTreeSet<_>>();
        let new_modules = request
            .candidate
            .modules
            .iter()
            .map(|module| module.id.display_ref())
            .collect::<BTreeSet<_>>();
        let old_bridges = existing
            .as_ref()
            .into_iter()
            .flat_map(|composition| composition.bridges.iter())
            .map(|bridge| {
                (
                    format!("{}@{}", bridge.bridge_id, bridge.authored_version),
                    serde_json::to_vec(bridge).unwrap_or_default(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let new_bridges = request
            .candidate
            .bridges
            .iter()
            .map(|bridge| {
                (
                    format!("{}@{}", bridge.bridge_id, bridge.authored_version),
                    serde_json::to_vec(bridge).unwrap_or_default(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let old_activation = existing
            .as_ref()
            .into_iter()
            .flat_map(|composition| composition.activation.iter())
            .map(|record| (record.subject.clone(), record.mode))
            .collect::<BTreeMap<_, _>>();
        let new_activation = request
            .candidate
            .activation
            .iter()
            .map(|record| (record.subject.clone(), record.mode))
            .collect::<BTreeMap<_, _>>();
        let mut affected_modules = old_modules
            .symmetric_difference(&new_modules)
            .cloned()
            .collect::<BTreeSet<_>>();
        for subject in old_activation.keys().chain(new_activation.keys()) {
            if old_activation.get(subject) != new_activation.get(subject) {
                affected_modules.insert(subject.clone());
            }
        }
        let affected_bridges = old_bridges
            .keys()
            .chain(new_bridges.keys())
            .filter(|identity| old_bridges.get(*identity) != new_bridges.get(*identity))
            .cloned()
            .collect::<BTreeSet<_>>();
        let portable_compatibility = match portable_receipt(&request.candidate, &candidate_bytes) {
            Ok(receipt) => receipt,
            Err(error) => {
                diagnostics.push(CompositionChangeDiagnostic {
                    code: "portable_composition_unrepresentable".into(),
                    subject: request.candidate.composition_fingerprint.clone(),
                    remediation: error.to_string(),
                });
                CompositionPortableReceipt {
                    disposition: CompositionPortableCompatibility::Unrepresentable,
                    required_feature: "ontology-composition@1".into(),
                    dependency_rule: "required-transitive-closure/1".into(),
                    composition_fingerprint: request.candidate.composition_fingerprint.clone(),
                    canonical_bytes: u64::try_from(candidate_bytes.len()).unwrap_or(u64::MAX),
                    modules: Vec::new(),
                    bridges: Vec::new(),
                }
            }
        };
        diagnostics
            .sort_by(|left, right| (&left.code, &left.subject).cmp(&(&right.code, &right.subject)));
        diagnostics.truncate(64);
        Ok(CompositionChangePreview {
            expected_project_generation_uuid: request.expected_project_generation_uuid,
            expected_composition_fingerprint: existing_fingerprint,
            candidate_composition_fingerprint: compiled.as_ref().map_or_else(
                || request.candidate.composition_fingerprint.clone(),
                |value| value.fingerprint.clone(),
            ),
            candidate_sha256: hex(&Sha256::digest(&candidate_bytes)),
            affected_modules: affected_modules.into_iter().collect(),
            affected_bridges: affected_bridges.into_iter().collect(),
            portable_compatibility,
            diagnostics,
        })
    }

    /// Revalidate and atomically publish one complete composition replacement.
    #[allow(clippy::too_many_lines)] // replay, preflight, atomic publish, and live swap are one transaction
    pub fn publish_ontology_composition_change(
        &mut self,
        request: &CompositionChangeRequest,
        preview: &CompositionChangePreview,
        cancellation: Option<&CancellationToken>,
    ) -> Result<CompositionChangeReceipt, GfError> {
        if let Some(receipt) = self.replay_ontology_composition_change(request)? {
            return Ok(receipt);
        }
        let request_fingerprint = composition_request_fingerprint(request)?;
        let mut generation_hasher = Sha256::new();
        generation_hasher.update(b"graphforge-composition-generation/1");
        generation_hasher.update(request.context.operation_uuid.0.as_bytes());
        generation_hasher.update(request_fingerprint);
        let expected_generation_uuid =
            graphforge_core::canonical::uuid_v8(generation_hasher.finalize().into());
        let fresh = self.preview_ontology_composition_change(request, cancellation)?;
        if &fresh != preview {
            return Err(GfError::Validation(
                "composition preview does not match request".into(),
            ));
        }
        if !fresh.diagnostics.is_empty() {
            return Err(GfError::Validation(
                "composition preflight contains unresolved diagnostics".into(),
            ));
        }
        if let Some(token) = cancellation {
            token.checkpoint()?;
        }
        let ontology = self.workspace_ontology()?;
        let mut configuration = self.workspace_configuration()?;
        configuration.ontology_mode = match request.candidate.profile_default {
            ActivationMode::Exploratory => WorkspaceOntologyMode::None,
            ActivationMode::Advisory => WorkspaceOntologyMode::Advisory,
            ActivationMode::Strict => WorkspaceOntologyMode::Strict,
        };
        let compiled_candidate = request.candidate.compile()?;
        let previous_bindings = self
            .semantic_storage_bindings
            .lock()
            .expect("semantic storage binding lock poisoned")
            .clone();
        let existing = self.workspace_ontology_composition()?;
        let identity_equivalent =
            identity_equivalent_upgrades(existing.as_ref(), &request.candidate);
        let published_bindings =
            graphforge_storage::SemanticStorageBindings::project_with_graph_scan_identity_equivalent(
                &compiled_candidate,
                previous_bindings.as_ref(),
                &self.dir,
                &identity_equivalent,
            )?;
        let published_binding = graphforge_ir::CompositionBindingContext::new(
            std::sync::Arc::new(compiled_candidate),
            request.candidate.bridges.clone(),
            graphforge_ir::CompositionBindingLimits::default(),
        )
        .with_generation_storage_ids(
            published_bindings
                .bindings
                .iter()
                .map(|binding| (binding.symbol.clone(), binding.storage_id)),
        );
        crate::workspace_ontology::publish_workspace_records(
            self,
            request.context.operation_uuid.0,
            request.context.actor_uuid,
            &ontology,
            &configuration,
            Some(&request.candidate),
            Some(&published_bindings),
            Some(expected_generation_uuid),
            cancellation,
        )?;
        *self
            .semantic_storage_bindings
            .lock()
            .expect("semantic storage binding lock poisoned") = Some(published_bindings);
        *self
            .default_composition_context
            .lock()
            .expect("composition binding lock poisoned") =
            Some(std::sync::Arc::new(published_binding));
        self.ontology_mode = configuration.ontology_mode.execution_mode();
        self.adjacency_provider = std::sync::Arc::new(
            graphforge_exec::PersistentAdjacencyProvider::new(self.dir.clone(), self.ontology_mode),
        );
        Ok(CompositionChangeReceipt {
            project_generation_uuid: *self
                .current_generation_uuid
                .lock()
                .expect("generation UUID lock poisoned"),
            composition_fingerprint: fresh.candidate_composition_fingerprint,
            candidate_sha256: fresh.candidate_sha256,
        })
    }

    pub(crate) fn replay_ontology_composition_change(
        &self,
        request: &CompositionChangeRequest,
    ) -> Result<Option<CompositionChangeReceipt>, GfError> {
        let request_fingerprint = composition_request_fingerprint(request)?;
        let mut generation_hasher = Sha256::new();
        generation_hasher.update(b"graphforge-composition-generation/1");
        generation_hasher.update(request.context.operation_uuid.0.as_bytes());
        generation_hasher.update(request_fingerprint);
        let expected_generation_uuid =
            graphforge_core::canonical::uuid_v8(generation_hasher.finalize().into());
        let root = self.resolved_generation.container_root();
        let Some(published) = graphforge_storage::published_project_transaction(
            root,
            request.context.operation_uuid.0,
        )?
        else {
            return Ok(None);
        };
        if published.generation_uuid != expected_generation_uuid {
            return Err(GfError::Project {
                code: ProjectErrorCode::TransactionConflict,
                message: "composition operation UUID was reused with different content".into(),
            });
        }
        let generation = graphforge_storage::resolve_verified_generation(
            root,
            published.generation_uuid,
            published.generation_manifest_sha256,
        )?;
        let snapshot = generation
            .participant_snapshot(
                WORKSPACE_CAPABILITY_ID,
                WORKSPACE_ONTOLOGY_COMPOSITION_FAMILY,
            )?
            .ok_or_else(|| GfError::Validation("published composition is missing".into()))?;
        let composition = WorkspaceOntologyComposition::from_canonical_json(&snapshot.bytes)?;
        if composition != request.candidate {
            return Err(GfError::Project {
                code: ProjectErrorCode::TransactionConflict,
                message: "published composition content does not match retry".into(),
            });
        }
        Ok(Some(CompositionChangeReceipt {
            project_generation_uuid: published.generation_uuid,
            composition_fingerprint: composition.composition_fingerprint,
            candidate_sha256: hex(&Sha256::digest(request.candidate.to_canonical_json()?)),
        }))
    }
}

fn removed_authority_diagnostics(
    existing: Option<&WorkspaceOntologyComposition>,
    candidate: &WorkspaceOntologyComposition,
    runtime_symbols: &BTreeSet<String>,
) -> Vec<CompositionChangeDiagnostic> {
    let Some(existing) = existing else {
        return Vec::new();
    };
    let new_ids = candidate
        .modules
        .iter()
        .map(|module| module.id.ontology_id.as_str())
        .collect::<BTreeSet<_>>();
    let mut diagnostics = Vec::new();
    for module in existing
        .modules
        .iter()
        .filter(|module| !new_ids.contains(module.id.ontology_id.as_str()))
    {
        let symbols = module
            .document
            .entity_types
            .iter()
            .map(|item| format!("entity:{}", item.name))
            .chain(
                module
                    .document
                    .relation_types
                    .iter()
                    .map(|item| format!("relation:{}", item.name)),
            )
            .chain(
                module
                    .document
                    .properties
                    .iter()
                    .map(|item| format!("property:{}", item.name)),
            );
        for subject in symbols.filter(|symbol| runtime_symbols.contains(symbol)) {
            diagnostics.push(CompositionChangeDiagnostic {
                code: "removed_authority_has_stored_data".into(),
                subject,
                remediation: "retain the module or remove the affected stored data before publication".into(),
            });
        }
    }
    diagnostics
}

fn migration_diagnostics(
    existing: Option<&WorkspaceOntologyComposition>,
    candidate: &WorkspaceOntologyComposition,
    runtime_symbols: &BTreeSet<String>,
    cancellation: Option<&CancellationToken>,
) -> Result<Vec<CompositionChangeDiagnostic>, GfError> {
    let Some(existing) = existing else {
        return Ok(Vec::new());
    };
    let mut diagnostics = Vec::new();
    for old in &existing.modules {
        if let Some(token) = cancellation {
            token.checkpoint()?;
        }
        let Some(new) = candidate
            .modules
            .iter()
            .find(|module| module.id.ontology_id == old.id.ontology_id)
        else {
            continue;
        };
        if old.id.authored_version == new.id.authored_version {
            continue;
        }
        if documents_equal_except_version_and_migrations(&old.document, &new.document) {
            continue;
        }
        let migrations = new
            .document
            .migrations
            .iter()
            .chain(old.document.migrations.iter())
            .cloned()
            .collect::<Vec<_>>();
        match MigrationEngine::plan(
            &old.id.authored_version,
            &new.id.authored_version,
            &migrations,
        ) {
            Err(_) => diagnostics.push(CompositionChangeDiagnostic {
                code: "migration_unreachable".into(),
                subject: new.id.ontology_id.clone(),
                remediation: format!(
                    "author a migration path from {} to {}",
                    old.id.authored_version, new.id.authored_version
                ),
            }),
            Ok(steps) => {
                for step in &steps {
                    if let Some(token) = cancellation {
                        token.checkpoint()?;
                    }
                    if let TransformKind::Unknown { raw } = &step.transform_kind {
                        diagnostics.push(CompositionChangeDiagnostic {
                            code: "migration_transform_unknown".into(),
                            subject: format!(
                                "{}:{}->{}",
                                new.id.ontology_id, step.from_version, step.to_version
                            ),
                            remediation: format!(
                                "replace unsupported migration transform `{raw}` with a known transform"
                            ),
                        });
                    }
                    if let Some(subject) = migration_runtime_subject(&step.transform_kind)
                        && runtime_symbols.contains(&subject)
                    {
                        diagnostics.push(CompositionChangeDiagnostic {
                            code: "migration_requires_data_rewrite".into(),
                            subject,
                            remediation: "publish a migration that stages and validates the affected graph/catalog data".into(),
                        });
                    }
                }
                if !steps
                    .iter()
                    .any(|step| matches!(step.transform_kind, TransformKind::Unknown { .. }))
                {
                    match MigrationEngine::apply_document(old.document.clone(), &steps) {
                        Ok(mut migrated) => {
                            migrated.migrations.clone_from(&new.document.migrations);
                            if migrated != new.document {
                                diagnostics.push(CompositionChangeDiagnostic {
                                    code: "migration_result_mismatch".into(),
                                    subject: new.id.ontology_id.clone(),
                                    remediation: "make the authored migration result exactly match the candidate ontology document".into(),
                                });
                            }
                        }
                        Err(error) => diagnostics.push(CompositionChangeDiagnostic {
                            code: "migration_apply_failed".into(),
                            subject: new.id.ontology_id.clone(),
                            remediation: error.to_string(),
                        }),
                    }
                }
            }
        }
        if diagnostics.len() == 64 {
            break;
        }
    }
    Ok(diagnostics)
}

fn documents_equal_except_version_and_migrations(left: &OntologyDoc, right: &OntologyDoc) -> bool {
    let mut left = left.clone();
    left.version.clone_from(&right.version);
    left.migrations.clone_from(&right.migrations);
    left == *right
}

fn identity_equivalent_upgrades(
    existing: Option<&WorkspaceOntologyComposition>,
    candidate: &WorkspaceOntologyComposition,
) -> Vec<(
    graphforge_ontology::OntologyModuleId,
    graphforge_ontology::OntologyModuleId,
)> {
    existing
        .into_iter()
        .flat_map(|composition| composition.modules.iter())
        .filter_map(|old| {
            candidate.modules.iter().find_map(|new| {
                (old.id != new.id
                    && old.id.ontology_id == new.id.ontology_id
                    && documents_equal_except_version_and_migrations(&old.document, &new.document))
                .then(|| (old.id.clone(), new.id.clone()))
            })
        })
        .collect()
}

fn migration_runtime_subject(transform: &TransformKind) -> Option<String> {
    match transform {
        TransformKind::RenameType { old_name, .. }
        | TransformKind::RemoveType { name: old_name } => {
            Some(format!("{}:{old_name}", SymbolKind::Entity.as_str()))
        }
        TransformKind::RenameProperty { old_name, .. }
        | TransformKind::RemoveProperty { name: old_name, .. } => {
            Some(format!("{}:{old_name}", SymbolKind::Property.as_str()))
        }
        TransformKind::AddProperty { .. }
        | TransformKind::AddType { .. }
        | TransformKind::Unknown { .. } => None,
    }
}

fn composition_request_fingerprint(
    request: &CompositionChangeRequest,
) -> Result<[u8; 32], GfError> {
    let mut hasher = Sha256::new();
    hasher.update(b"graphforge-composition-request/1");
    hasher.update(request.expected_project_generation_uuid.as_bytes());
    match &request.expected_composition_fingerprint {
        Some(fingerprint) => {
            hasher.update([1]);
            hasher.update(fingerprint.as_bytes());
        }
        None => hasher.update([0]),
    }
    match request.context.actor_uuid {
        Some(actor) => {
            hasher.update([1]);
            hasher.update(actor.as_bytes());
        }
        None => hasher.update([0]),
    }
    hasher.update(request.candidate.to_canonical_json()?);
    match &request.data_disposition {
        CompositionDataDisposition::RequireConforming => {
            hasher.update(b"require_conforming");
        }
        CompositionDataDisposition::RequireConformingOperation { operation } => {
            hasher.update(b"require_conforming_operation\0");
            hasher.update(operation.as_bytes());
        }
    }
    Ok(hasher.finalize().into())
}

fn symbol_declared(composition: &graphforge_ontology::CompiledComposition, encoded: &str) -> bool {
    let Some((kind, local_id)) = encoded.split_once(':') else {
        return false;
    };
    let kind = match kind {
        "entity" => SymbolKind::Entity,
        "relation" => SymbolKind::Relation,
        "property" => SymbolKind::Property,
        _ => return false,
    };
    composition
        .resolve(&ResolveRequest {
            module: None,
            kind,
            local_id,
            max_candidates: 2,
        })
        .is_ok()
}

fn runtime_symbols(
    graph: &GraphForge,
    cancellation: Option<&CancellationToken>,
) -> Result<(BTreeSet<String>, bool), GfError> {
    let current = graph.generation_for_read()?;
    let persisted = crate::load_runtime_catalog(&current.graph_tree_root())?;
    let live = graph
        .runtime_catalog
        .lock()
        .expect("runtime catalog poisoned")
        .clone();
    let catalog_symbols = persisted
        .entity_types()
        .into_iter()
        .map(|name| format!("{}:{name}", SymbolKind::Entity.as_str()))
        .chain(
            persisted
                .relation_types()
                .into_iter()
                .map(|name| format!("{}:{name}", SymbolKind::Relation.as_str())),
        )
        .chain(
            persisted
                .property_names()
                .map(|(_, name)| format!("{}:{name}", SymbolKind::Property.as_str())),
        )
        .collect::<BTreeSet<_>>();
    if let Some(token) = cancellation {
        token.checkpoint()?;
    }
    // Tagged runtime observations are governed by the pinned RuntimeCatalog.
    // Ontology-owned persisted data is validated separately through the exact
    // generation semantic binding authority and its bounded graph scan.
    let symbols = catalog_symbols.clone();
    let live_symbols = live
        .entity_types()
        .into_iter()
        .map(|name| format!("{}:{name}", SymbolKind::Entity.as_str()))
        .chain(
            live.relation_types()
                .into_iter()
                .map(|name| format!("{}:{name}", SymbolKind::Relation.as_str())),
        )
        .chain(
            live.property_names()
                .map(|(_, name)| format!("{}:{name}", SymbolKind::Property.as_str())),
        )
        .collect::<BTreeSet<_>>();
    Ok((
        symbols.clone(),
        catalog_symbols != live_symbols || !symbols.is_subset(&catalog_symbols),
    ))
}

fn portable_receipt(
    candidate: &WorkspaceOntologyComposition,
    canonical_bytes: &[u8],
) -> Result<CompositionPortableReceipt, GfError> {
    let limits = graphforge_storage::PortableV2Limits::default();
    let component_count = candidate
        .modules
        .len()
        .checked_add(candidate.bridges.len())
        .ok_or_else(|| GfError::Validation("portable composition count overflows".into()))?;
    let byte_count = u64::try_from(canonical_bytes.len())
        .map_err(|_| GfError::Validation("portable composition bytes overflow".into()))?;
    if u64::try_from(component_count).unwrap_or(u64::MAX) > limits.max_components
        || byte_count > limits.max_entry_bytes
        || byte_count > limits.max_manifest_bytes
        || byte_count > limits.max_total_bytes
    {
        return Err(GfError::Validation(
            "ontology composition is not representable within portable-v2 limits".into(),
        ));
    }
    let mut modules = candidate
        .modules
        .iter()
        .map(|module| module.id.display_ref())
        .collect::<Vec<_>>();
    modules.sort();
    modules.dedup();
    if modules.len() != candidate.modules.len() {
        return Err(GfError::Validation(
            "portable composition module identities are not unique".into(),
        ));
    }
    let mut bridges = candidate
        .bridges
        .iter()
        .map(|bridge| format!("{}@{}", bridge.bridge_id, bridge.authored_version))
        .collect::<Vec<_>>();
    bridges.sort();
    bridges.dedup();
    if bridges.len() != candidate.bridges.len() {
        return Err(GfError::Validation(
            "portable composition bridge identities are not unique".into(),
        ));
    }
    Ok(CompositionPortableReceipt {
        disposition: CompositionPortableCompatibility::RepresentableReaderUnsupported,
        required_feature: "ontology-composition@1".into(),
        dependency_rule: "required-transitive-closure/1".into(),
        composition_fingerprint: candidate.composition_fingerprint.clone(),
        canonical_bytes: byte_count,
        modules,
        bridges,
    })
}

fn hex(bytes: &[u8]) -> String {
    const TABLE: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(TABLE[(byte >> 4) as usize] as char);
        output.push(TABLE[(byte & 0x0f) as usize] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use graphforge_ontology::{
        ActivationRecord, ActivationScope, AuthoredModule, BridgeAssertion, BridgeDocument,
        BridgePredicate, BridgeProvenance, BridgeSetId, CompositionLimits, EntityTypeDef,
        InventoryCompileRequest, MappingMethod, MigrationDef, OntologyDoc, OntologyModuleId,
        QualifiedSymbol, SymbolKind, bridge_document_digest, compile_inventory,
        module_document_digest,
    };

    use super::*;
    use crate::{CheckpointRequest, OperationId, RevertCheckpointRequest};

    fn composition(
        version: &str,
        mode: ActivationMode,
        entity_names: &[&str],
        migrations: Vec<MigrationDef>,
    ) -> WorkspaceOntologyComposition {
        composition_named("acme", version, mode, entity_names, migrations)
    }

    fn composition_named(
        ontology_id: &str,
        version: &str,
        mode: ActivationMode,
        entity_names: &[&str],
        migrations: Vec<MigrationDef>,
    ) -> WorkspaceOntologyComposition {
        let document = OntologyDoc {
            ontology_id: ontology_id.into(),
            version: version.into(),
            entity_types: entity_names
                .iter()
                .map(|name| EntityTypeDef {
                    name: (*name).into(),
                    r#abstract: false,
                    parent: None,
                })
                .collect(),
            relation_types: Vec::new(),
            properties: Vec::new(),
            constraints: Vec::new(),
            migrations,
        };
        let authored = AuthoredModule {
            id: OntologyModuleId {
                ontology_id: document.ontology_id.clone(),
                authored_version: document.version.clone(),
                canonical_digest: module_document_digest(&document).unwrap(),
            },
            dependencies: Vec::new(),
            doc: document,
            allow_projected_identity: false,
        };
        let compiled = compile_inventory(InventoryCompileRequest {
            modules: std::slice::from_ref(&authored),
            bridges: &[],
            activation: &[],
            profile_default: mode,
            limits: CompositionLimits::default(),
            cancelled: None,
        })
        .unwrap();
        WorkspaceOntologyComposition::from_compiled(&compiled, Vec::new())
    }

    fn context(seed: u128) -> WriteContext {
        WriteContext {
            operation_uuid: OperationId(Uuid::from_u128(seed)),
            actor_uuid: None,
        }
    }

    fn bridged_composition() -> WorkspaceOntologyComposition {
        let module = |ontology_id: &str| {
            let document = OntologyDoc {
                ontology_id: ontology_id.into(),
                version: "1".into(),
                entity_types: vec![EntityTypeDef {
                    name: "Person".into(),
                    r#abstract: false,
                    parent: None,
                }],
                relation_types: Vec::new(),
                properties: Vec::new(),
                constraints: Vec::new(),
                migrations: Vec::new(),
            };
            AuthoredModule {
                id: OntologyModuleId {
                    ontology_id: ontology_id.into(),
                    authored_version: "1".into(),
                    canonical_digest: module_document_digest(&document).unwrap(),
                },
                dependencies: Vec::new(),
                doc: document,
                allow_projected_identity: false,
            }
        };
        let source = module("source");
        let target = module("target");
        let qualified = |module: &AuthoredModule| QualifiedSymbol {
            module: module.id.clone(),
            kind: SymbolKind::Entity,
            local_id: "Person".into(),
        };
        let bridge = BridgeDocument {
            bridge_id: "https://graphforge.dev/bridge/person".into(),
            authored_version: "1".into(),
            source_modules: vec![source.id.clone()],
            target_modules: vec![target.id.clone()],
            dependencies: Vec::new(),
            shared_surfaces: Vec::new(),
            assertions: vec![BridgeAssertion {
                source: qualified(&source),
                target: qualified(&target),
                predicate: BridgePredicate::Equivalent,
                directional: false,
                provenance: BridgeProvenance {
                    method: MappingMethod::Authored,
                    confidence: None,
                    justification: "governed identity".into(),
                    evidence_refs: Vec::new(),
                },
                valid_from: None,
                valid_to: None,
            }],
            enforcement: Some(ActivationMode::Strict),
        };
        let bridge_id = BridgeSetId {
            bridge_id: bridge.bridge_id.clone(),
            authored_version: bridge.authored_version.clone(),
            canonical_digest: bridge_document_digest(&bridge).unwrap(),
        };
        let activation = vec![ActivationRecord {
            scope: ActivationScope::Module,
            subject: source.id.display_ref(),
            mode: ActivationMode::Advisory,
        }];
        let compiled = compile_inventory(InventoryCompileRequest {
            modules: &[source, target],
            bridges: &[bridge_id],
            activation: &activation,
            profile_default: ActivationMode::Strict,
            limits: CompositionLimits::default(),
            cancelled: None,
        })
        .unwrap();
        WorkspaceOntologyComposition::from_compiled(&compiled, vec![bridge])
    }

    fn request(
        graph: &GraphForge,
        seed: u128,
        candidate: WorkspaceOntologyComposition,
    ) -> CompositionChangeRequest {
        CompositionChangeRequest {
            context: context(seed),
            expected_project_generation_uuid: *graph.current_generation_uuid.lock().unwrap(),
            expected_composition_fingerprint: graph
                .workspace_ontology_composition()
                .unwrap()
                .map(|value| value.composition_fingerprint),
            candidate,
            data_disposition: CompositionDataDisposition::RequireConforming,
        }
    }

    #[test]
    fn publish_reopen_retry_and_rollback_preserve_exact_authority() {
        let project = tempfile::tempdir().unwrap();
        let path = project.path().to_str().unwrap();
        let mut graph = GraphForge::new(Some(path)).unwrap();
        let first = request(
            &graph,
            8401,
            composition("1", ActivationMode::Strict, &["Person"], Vec::new()),
        );
        let first_preview = graph
            .preview_ontology_composition_change(&first, None)
            .unwrap();
        assert_eq!(
            first_preview.portable_compatibility.required_feature,
            "ontology-composition@1"
        );
        assert_eq!(
            first_preview.portable_compatibility.dependency_rule,
            "required-transitive-closure/1"
        );
        assert_eq!(first_preview.portable_compatibility.modules.len(), 1);
        let first_receipt = graph
            .publish_ontology_composition_change(&first, &first_preview, None)
            .unwrap();
        graph
            .checkpoint(CheckpointRequest {
                name: "composition-v1".into(),
                description: Some("retained exact composition generation".into()),
                idempotency_key: OperationId(Uuid::from_u128(8404)),
                actor_uuid: None,
            })
            .unwrap();
        graph
            .execute("MATCH (n:Person) RETURN n")
            .expect("normal execution consumes published composition authority");
        assert!(graph.execute("MATCH (n:Unknown) RETURN n").is_err());
        graph.set_graph_directedness(&context(8499), None).unwrap();
        assert_eq!(
            graph
                .publish_ontology_composition_change(&first, &first_preview, None)
                .unwrap(),
            first_receipt
        );
        drop(graph);

        let mut reopened = GraphForge::new(Some(path)).unwrap();
        let observed = reopened.workspace_ontology_composition().unwrap().unwrap();
        assert_eq!(observed.profile_default, ActivationMode::Strict);
        assert_eq!(observed.modules[0].document.version, "1");
        reopened
            .execute("MATCH (n:Person) RETURN n")
            .expect("reopen hydrates normal composition binding authority");

        let upgraded = request(
            &reopened,
            8402,
            composition(
                "2",
                ActivationMode::Strict,
                &["Person", "Company"],
                vec![MigrationDef {
                    from_version: "1".into(),
                    to_version: "2".into(),
                    transform_kind: "add_type:Company".into(),
                    script_ref: None,
                    checksum: None,
                }],
            ),
        );
        let upgraded_preview = reopened
            .preview_ontology_composition_change(&upgraded, None)
            .unwrap();
        assert!(upgraded_preview.diagnostics.is_empty());
        reopened
            .publish_ontology_composition_change(&upgraded, &upgraded_preview, None)
            .unwrap();

        let downgrade = request(&reopened, 8403, observed.clone());
        assert_eq!(
            reopened
                .preview_ontology_composition_change(&downgrade, None)
                .unwrap()
                .diagnostics[0]
                .code,
            "migration_unreachable"
        );
        reopened
            .revert_to_checkpoint(RevertCheckpointRequest {
                name: "composition-v1".into(),
                reason: "restore retained validated authority".into(),
                idempotency_key: OperationId(Uuid::from_u128(8405)),
                actor_uuid: None,
            })
            .unwrap();
        drop(reopened);
        assert_eq!(
            GraphForge::new(Some(path))
                .unwrap()
                .workspace_ontology_composition()
                .unwrap(),
            Some(observed)
        );
    }

    #[test]
    fn stale_cancelled_and_conflicting_replay_leave_generation_unchanged() {
        let mut graph = GraphForge::new(None).unwrap();
        let request = request(
            &graph,
            8410,
            composition("1", ActivationMode::Advisory, &["Person"], Vec::new()),
        );
        let preview = graph
            .preview_ontology_composition_change(&request, None)
            .unwrap();
        let before = *graph.current_generation_uuid.lock().unwrap();
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        assert!(
            graph
                .publish_ontology_composition_change(&request, &preview, Some(&cancellation))
                .is_err()
        );
        assert_eq!(*graph.current_generation_uuid.lock().unwrap(), before);
        graph
            .publish_ontology_composition_change(&request, &preview, None)
            .unwrap();
        let published = *graph.current_generation_uuid.lock().unwrap();

        let mut conflicting = request.clone();
        conflicting.candidate =
            composition("1", ActivationMode::Advisory, &["Company"], Vec::new());
        assert!(
            graph
                .publish_ontology_composition_change(&conflicting, &preview, None)
                .is_err()
        );
        assert_eq!(*graph.current_generation_uuid.lock().unwrap(), published);

        let mut different_actor = request.clone();
        different_actor.context.actor_uuid = Some(Uuid::from_u128(841_001));
        let error = graph
            .publish_ontology_composition_change(&different_actor, &preview, None)
            .unwrap_err();
        assert_eq!(error.code(), "GF_IDEMPOTENCY_CONFLICT");

        let mut different_operation = request;
        different_operation.data_disposition =
            CompositionDataDisposition::RequireConformingOperation {
                operation: "activation.change".into(),
            };
        let error = graph
            .publish_ontology_composition_change(&different_operation, &preview, None)
            .unwrap_err();
        assert_eq!(error.code(), "GF_IDEMPOTENCY_CONFLICT");
        assert_eq!(*graph.current_generation_uuid.lock().unwrap(), published);
    }

    #[test]
    fn scoped_activation_reopens_and_bridge_invalidation_cannot_publish() {
        let project = tempfile::tempdir().unwrap();
        let path = project.path().to_str().unwrap();
        let mut graph = GraphForge::new(Some(path)).unwrap();
        let initial = request(&graph, 8411, bridged_composition());
        let preview = graph
            .preview_ontology_composition_change(&initial, None)
            .unwrap();
        assert!(preview.diagnostics.is_empty(), "{:?}", preview.diagnostics);
        graph
            .publish_ontology_composition_change(&initial, &preview, None)
            .unwrap();
        let published = *graph.current_generation_uuid.lock().unwrap();
        drop(graph);

        let mut reopened = GraphForge::new(Some(path)).unwrap();
        let authority = reopened.workspace_ontology_composition().unwrap().unwrap();
        assert_eq!(authority.activation.len(), 1);
        assert_eq!(authority.activation[0].mode, ActivationMode::Advisory);
        reopened
            .execute("MATCH (n:`source:Person`) RETURN n")
            .expect("reopened scoped authority must drive normal execution");

        let mut invalid = authority;
        invalid
            .modules
            .retain(|module| module.document.ontology_id != "target");
        let invalid_request = request(&reopened, 8412, invalid);
        let invalid_preview = reopened
            .preview_ontology_composition_change(&invalid_request, None)
            .unwrap();
        assert!(
            invalid_preview
                .diagnostics
                .iter()
                .any(|diagnostic| { diagnostic.code == "candidate_composition_invalid" })
        );
        assert!(
            reopened
                .publish_ontology_composition_change(&invalid_request, &invalid_preview, None)
                .is_err()
        );
        assert_eq!(*reopened.current_generation_uuid.lock().unwrap(), published);
    }

    #[test]
    fn intervening_generation_and_interrupted_pointer_leave_authority_old_or_complete() {
        let project = tempfile::tempdir().unwrap();
        let path = project.path().to_str().unwrap();
        let mut graph = GraphForge::new(Some(path)).unwrap();
        let initial = request(
            &graph,
            8413,
            composition("1", ActivationMode::Strict, &["Person"], Vec::new()),
        );
        let preview = graph
            .preview_ontology_composition_change(&initial, None)
            .unwrap();
        graph
            .publish_ontology_composition_change(&initial, &preview, None)
            .unwrap();
        let retained = graph.workspace_ontology_composition().unwrap().unwrap();

        let stale = request(
            &graph,
            8414,
            composition(
                "2",
                ActivationMode::Strict,
                &["Person", "Company"],
                Vec::new(),
            ),
        );
        let stale_preview = graph
            .preview_ontology_composition_change(&stale, None)
            .unwrap();
        graph.set_graph_directedness(&context(8415), None).unwrap();
        let intervening = *graph.current_generation_uuid.lock().unwrap();
        assert!(
            graph
                .publish_ontology_composition_change(&stale, &stale_preview, None)
                .is_err()
        );
        assert_eq!(*graph.current_generation_uuid.lock().unwrap(), intervening);
        assert_eq!(
            graph.workspace_ontology_composition().unwrap(),
            Some(retained.clone())
        );
        drop(graph);

        let reopened = GraphForge::new(Some(path)).unwrap();
        assert_eq!(
            *reopened.current_generation_uuid.lock().unwrap(),
            intervening
        );
        assert_eq!(
            reopened.workspace_ontology_composition().unwrap(),
            Some(retained)
        );
        reopened.execute("MATCH (n:Person) RETURN n").unwrap();
    }

    #[test]
    fn composition_publication_failpoint_child() {
        let Ok(root) = std::env::var("GRAPHFORGE_COMPOSITION_FAILPOINT_ROOT") else {
            return;
        };
        let mut graph = GraphForge::new(Some(&root)).unwrap();
        let upgrade = request(
            &graph,
            8417,
            composition(
                "2",
                ActivationMode::Strict,
                &["Person", "Company"],
                vec![MigrationDef {
                    from_version: "1".into(),
                    to_version: "2".into(),
                    transform_kind: "add_type:Company".into(),
                    script_ref: None,
                    checksum: None,
                }],
            ),
        );
        let preview = graph
            .preview_ontology_composition_change(&upgrade, None)
            .unwrap();
        graph
            .publish_ontology_composition_change(&upgrade, &preview, None)
            .expect("configured composition publication failpoint did not terminate");
    }

    #[test]
    fn interrupted_real_publication_reopens_old_or_complete_authority() {
        for (failpoint, expected_version) in [
            ("project.before_current_replace", "1"),
            ("project.after_current_replace", "2"),
        ] {
            let project = tempfile::tempdir().unwrap();
            let path = project.path().to_str().unwrap();
            let mut graph = GraphForge::new(Some(path)).unwrap();
            let initial = request(
                &graph,
                8416,
                composition("1", ActivationMode::Strict, &["Person"], Vec::new()),
            );
            let preview = graph
                .preview_ontology_composition_change(&initial, None)
                .unwrap();
            graph
                .publish_ontology_composition_change(&initial, &preview, None)
                .unwrap();
            drop(graph);
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "ontology_composition_lifecycle::tests::composition_publication_failpoint_child",
                    "--nocapture",
                ])
                .env("GRAPHFORGE_COMPOSITION_FAILPOINT_ROOT", path)
                .env("GRAPHFORGE_PROJECT_FAILPOINTS", "graphforge-internal-subprocess-v1")
                .env("GRAPHFORGE_PROJECT_FAILPOINT", failpoint)
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(86), "{failpoint}");
            let reopened = GraphForge::new(Some(path)).unwrap();
            let authority = reopened.workspace_ontology_composition().unwrap().unwrap();
            assert_eq!(
                authority.modules[0].document.version, expected_version,
                "{failpoint}"
            );
            reopened.execute("MATCH (n:Person) RETURN n").unwrap();
            if expected_version == "2" {
                reopened.execute("MATCH (n:Company) RETURN n").unwrap();
            } else {
                assert!(reopened.execute("MATCH (n:Company) RETURN n").is_err());
            }
        }
    }

    #[test]
    fn replace_and_remove_are_safe_only_without_retained_semantic_data() {
        let project = tempfile::tempdir().unwrap();
        let path = project.path().to_str().unwrap();
        let mut graph = GraphForge::new(Some(path)).unwrap();
        let initial = request(
            &graph,
            8430,
            composition("1", ActivationMode::Strict, &["Person"], Vec::new()),
        );
        let preview = graph
            .preview_ontology_composition_change(&initial, None)
            .unwrap();
        graph
            .publish_ontology_composition_change(&initial, &preview, None)
            .unwrap();

        let replacement = request(
            &graph,
            8431,
            composition_named(
                "replacement",
                "1",
                ActivationMode::Strict,
                &["Company"],
                Vec::new(),
            ),
        );
        let preview = graph
            .preview_ontology_composition_change(&replacement, None)
            .unwrap();
        assert!(preview.diagnostics.is_empty(), "{:?}", preview.diagnostics);
        graph
            .publish_ontology_composition_change(&replacement, &preview, None)
            .unwrap();

        let removal = request(
            &graph,
            8432,
            composition("1", ActivationMode::Strict, &[], Vec::new()),
        );
        let preview = graph
            .preview_ontology_composition_change(&removal, None)
            .unwrap();
        assert!(preview.diagnostics.is_empty(), "{:?}", preview.diagnostics);
        graph
            .publish_ontology_composition_change(&removal, &preview, None)
            .unwrap();
        drop(graph);
        assert!(
            GraphForge::new(Some(path))
                .unwrap()
                .workspace_ontology_composition()
                .unwrap()
                .unwrap()
                .modules[0]
                .document
                .entity_types
                .is_empty()
        );

        let mut populated = GraphForge::new(None).unwrap();
        let initial = request(
            &populated,
            8433,
            composition(
                "1",
                ActivationMode::Strict,
                &["Person", "UnusedCompany"],
                Vec::new(),
            ),
        );
        let preview = populated
            .preview_ontology_composition_change(&initial, None)
            .unwrap();
        populated
            .publish_ontology_composition_change(&initial, &preview, None)
            .unwrap();
        populated.execute("CREATE (n:Person)").unwrap();
        let before = *populated.current_generation_uuid.lock().unwrap();
        let removal = request(
            &populated,
            8434,
            composition("1", ActivationMode::Strict, &[], Vec::new()),
        );
        let preview = populated
            .preview_ontology_composition_change(&removal, None)
            .unwrap();
        assert!(
            preview
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "stored_semantic_data_incompatible")
        );
        let incompatible = preview
            .diagnostics
            .iter()
            .filter(|diagnostic| diagnostic.code == "stored_semantic_data_incompatible")
            .map(|diagnostic| diagnostic.subject.as_str())
            .collect::<Vec<_>>();
        assert_eq!(incompatible.len(), 1, "{incompatible:?}");
        assert!(incompatible[0].contains(":entity:Person"));
        assert!(!incompatible[0].contains("UnusedCompany"));
        assert!(
            populated
                .publish_ontology_composition_change(&removal, &preview, None)
                .is_err()
        );
        assert_eq!(*populated.current_generation_uuid.lock().unwrap(), before);
    }

    #[test]
    fn preflight_rejects_live_catalog_drift_from_bound_generation() {
        let graph = GraphForge::new(None).unwrap();
        {
            let mut catalog = graph.runtime_catalog.lock().unwrap();
            for index in 0..80 {
                catalog.intern_label(&format!("Exploratory{index:02}"));
            }
        }
        let request = request(
            &graph,
            8420,
            composition("1", ActivationMode::Strict, &["Person"], Vec::new()),
        );
        let preview = graph
            .preview_ontology_composition_change(&request, None)
            .unwrap();
        assert_eq!(preview.diagnostics.len(), 1);
        assert_eq!(
            preview.diagnostics[0].code,
            "runtime_catalog_generation_drift"
        );
    }

    #[test]
    fn migration_preflight_is_reachable_ordered_and_fail_closed() {
        let mut graph = GraphForge::new(None).unwrap();
        let initial = request(
            &graph,
            8430,
            composition("1", ActivationMode::Advisory, &["Person"], Vec::new()),
        );
        let preview = graph
            .preview_ontology_composition_change(&initial, None)
            .unwrap();
        graph
            .publish_ontology_composition_change(&initial, &preview, None)
            .unwrap();
        graph
            .execute("CREATE (:Person {name: 'retained'})")
            .unwrap();

        let unreachable = request(
            &graph,
            8431,
            composition(
                "3",
                ActivationMode::Advisory,
                &["Person", "Company"],
                Vec::new(),
            ),
        );
        assert_eq!(
            graph
                .preview_ontology_composition_change(&unreachable, None)
                .unwrap()
                .diagnostics[0]
                .code,
            "migration_unreachable"
        );
        let unknown = request(
            &graph,
            8432,
            composition(
                "2",
                ActivationMode::Advisory,
                &["Person", "Company"],
                vec![MigrationDef {
                    from_version: "1".into(),
                    to_version: "2".into(),
                    transform_kind: "execute_magic".into(),
                    script_ref: None,
                    checksum: None,
                }],
            ),
        );
        assert_eq!(
            graph
                .preview_ontology_composition_change(&unknown, None)
                .unwrap()
                .diagnostics[0]
                .code,
            "migration_transform_unknown"
        );
    }
}
