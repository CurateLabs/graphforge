//! Atomic project-generation lifecycle for ontology composition authority.

use std::collections::{BTreeMap, BTreeSet};

use graphforge_core::GfError;
use graphforge_ontology::{
    ActivationMode, MigrationEngine, ResolveRequest, SymbolKind, TransformKind,
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
    /// Caller names every runtime symbol handled by a separately validated migration.
    ValidatedMigration {
        /// Sorted exact `kind:local` observations covered by that migration.
        migrated_symbols: Vec<String>,
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
    /// Bounded diagnostics; empty means publishable.
    pub diagnostics: Vec<CompositionChangeDiagnostic>,
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
        let current = self.generation_for_read()?;
        if let Some(snapshot) = current.participant_snapshot(
            WORKSPACE_CAPABILITY_ID,
            WORKSPACE_ONTOLOGY_COMPOSITION_FAMILY,
        )? {
            return WorkspaceOntologyComposition::from_canonical_json(&snapshot.bytes).map(Some);
        }
        WorkspaceOntologyComposition::virtual_legacy(&self.workspace_ontology()?)
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
        let compiled = request.candidate.compile()?;
        if let Some(token) = cancellation {
            token.checkpoint()?;
        }
        let candidate_bytes = request.candidate.to_canonical_json()?;
        let runtime_symbols = runtime_symbols(self);
        let mut diagnostics = Vec::new();
        diagnostics.extend(migration_diagnostics(existing.as_ref(), &request.candidate));
        let has_strict_scope = compiled.profile_default == ActivationMode::Strict
            || compiled
                .modules
                .iter()
                .any(|module| compiled.effective_module_mode(&module.id) == ActivationMode::Strict);
        if has_strict_scope {
            let covered = disposition_symbols(&request.data_disposition)?;
            let nonconforming = runtime_symbols
                .iter()
                .filter(|symbol| !symbol_declared(&compiled, symbol))
                .filter(|symbol| !covered.contains(*symbol));
            for symbol in nonconforming.take(64usize.saturating_sub(diagnostics.len())) {
                diagnostics.push(CompositionChangeDiagnostic {
                    code: "strict_runtime_observation".into(),
                    subject: symbol.clone(),
                    remediation: "declare the symbol or provide a validated migration disposition"
                        .into(),
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
        Ok(CompositionChangePreview {
            expected_project_generation_uuid: request.expected_project_generation_uuid,
            expected_composition_fingerprint: existing_fingerprint,
            candidate_composition_fingerprint: compiled.fingerprint,
            candidate_sha256: hex(&Sha256::digest(candidate_bytes)),
            affected_modules: affected_modules.into_iter().collect(),
            affected_bridges: affected_bridges.into_iter().collect(),
            diagnostics,
        })
    }

    /// Revalidate and atomically publish one complete composition replacement.
    pub fn publish_ontology_composition_change(
        &mut self,
        request: &CompositionChangeRequest,
        preview: &CompositionChangePreview,
        cancellation: Option<&CancellationToken>,
    ) -> Result<CompositionChangeReceipt, GfError> {
        let current = self.generation_for_read()?;
        if current.generation_uuid() != request.expected_project_generation_uuid
            && current.parent_generation_uuid() == Some(request.expected_project_generation_uuid)
            && current.transaction_uuid() == request.context.operation_uuid.0
        {
            let candidate_sha256 = hex(&Sha256::digest(request.candidate.to_canonical_json()?));
            let published = self.workspace_ontology_composition()?.ok_or_else(|| {
                GfError::Validation(
                    "idempotent composition publication is missing authority".into(),
                )
            })?;
            if published != request.candidate
                || preview.candidate_sha256 != candidate_sha256
                || preview.candidate_composition_fingerprint
                    != request.candidate.composition_fingerprint
            {
                return Err(GfError::Validation(
                    "composition operation UUID was reused with different content".into(),
                ));
            }
            return Ok(CompositionChangeReceipt {
                project_generation_uuid: current.generation_uuid(),
                composition_fingerprint: published.composition_fingerprint,
                candidate_sha256,
            });
        }
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
        crate::workspace_ontology::publish_workspace_records(
            self,
            request.context.operation_uuid.0,
            request.context.actor_uuid,
            &ontology,
            &configuration,
            Some(&request.candidate),
        )?;
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
}

fn migration_diagnostics(
    existing: Option<&WorkspaceOntologyComposition>,
    candidate: &WorkspaceOntologyComposition,
) -> Vec<CompositionChangeDiagnostic> {
    let Some(existing) = existing else {
        return Vec::new();
    };
    let mut diagnostics = Vec::new();
    for old in &existing.modules {
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
        // A rollback republishes previously validated immutable authority. It
        // still receives the full current-data/enforcement preflight, but does
        // not invent a reverse transform that authored ontologies forbid.
        if new.id.authored_version < old.id.authored_version {
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
                for step in steps {
                    if let TransformKind::Unknown { raw } = step.transform_kind {
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
                }
            }
        }
        if diagnostics.len() == 64 {
            break;
        }
    }
    diagnostics
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

fn runtime_symbols(graph: &GraphForge) -> BTreeSet<String> {
    let catalog = graph
        .runtime_catalog
        .lock()
        .expect("runtime catalog poisoned");
    catalog
        .entity_types()
        .into_iter()
        .map(|name| format!("{}:{name}", SymbolKind::Entity.as_str()))
        .chain(
            catalog
                .relation_types()
                .into_iter()
                .map(|name| format!("{}:{name}", SymbolKind::Relation.as_str())),
        )
        .chain(
            catalog
                .property_names()
                .map(|(_, name)| format!("{}:{name}", SymbolKind::Property.as_str())),
        )
        .collect()
}

fn disposition_symbols(
    disposition: &CompositionDataDisposition,
) -> Result<BTreeSet<String>, GfError> {
    match disposition {
        CompositionDataDisposition::RequireConforming => Ok(BTreeSet::new()),
        CompositionDataDisposition::ValidatedMigration { migrated_symbols } => {
            let set = migrated_symbols.iter().cloned().collect::<BTreeSet<_>>();
            if set.len() != migrated_symbols.len()
                || migrated_symbols.windows(2).any(|pair| pair[0] >= pair[1])
            {
                return Err(GfError::Validation(
                    "migrated symbols must be unique and sorted".into(),
                ));
            }
            Ok(set)
        }
    }
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
        CompositionLimits, EntityTypeDef, MigrationDef, OntologyDoc, compile_legacy_single_ontology,
    };

    use super::*;
    use crate::OperationId;

    fn composition(
        version: &str,
        mode: ActivationMode,
        entity_names: &[&str],
        migrations: Vec<MigrationDef>,
    ) -> WorkspaceOntologyComposition {
        let document = OntologyDoc {
            ontology_id: "acme".into(),
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
        let compiled =
            compile_legacy_single_ontology(&document, false, CompositionLimits::default()).unwrap();
        let mut composition = WorkspaceOntologyComposition::from_compiled(&compiled, Vec::new());
        composition.profile_default = mode;
        composition
    }

    fn context(seed: u128) -> WriteContext {
        WriteContext {
            operation_uuid: OperationId(Uuid::from_u128(seed)),
            actor_uuid: None,
        }
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
            composition("1", ActivationMode::Advisory, &["Person"], Vec::new()),
        );
        let first_preview = graph
            .preview_ontology_composition_change(&first, None)
            .unwrap();
        let first_receipt = graph
            .publish_ontology_composition_change(&first, &first_preview, None)
            .unwrap();
        assert_eq!(
            graph
                .publish_ontology_composition_change(&first, &first_preview, None)
                .unwrap(),
            first_receipt
        );
        drop(graph);

        let mut reopened = GraphForge::new(Some(path)).unwrap();
        let observed = reopened.workspace_ontology_composition().unwrap().unwrap();
        assert_eq!(observed.profile_default, ActivationMode::Advisory);
        assert_eq!(observed.modules[0].document.version, "1");

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

        let rollback = request(&reopened, 8403, observed.clone());
        let rollback_preview = reopened
            .preview_ontology_composition_change(&rollback, None)
            .unwrap();
        reopened
            .publish_ontology_composition_change(&rollback, &rollback_preview, None)
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

        let mut conflicting = request;
        conflicting.candidate =
            composition("1", ActivationMode::Advisory, &["Company"], Vec::new());
        assert!(
            graph
                .publish_ontology_composition_change(&conflicting, &preview, None)
                .is_err()
        );
        assert_eq!(*graph.current_generation_uuid.lock().unwrap(), published);
    }

    #[test]
    fn strict_preflight_reports_bounded_nonconforming_data() {
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
        assert_eq!(preview.diagnostics.len(), 64);
        assert!(
            preview
                .diagnostics
                .windows(2)
                .all(|pair| pair[0].subject < pair[1].subject)
        );
        assert!(preview.diagnostics.iter().all(|diagnostic| {
            diagnostic.code == "strict_runtime_observation"
                && diagnostic.subject.starts_with("entity:Exploratory")
                && !diagnostic.remediation.is_empty()
        }));
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

        let unreachable = request(
            &graph,
            8431,
            composition("3", ActivationMode::Advisory, &["Person"], Vec::new()),
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
                &["Person"],
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
