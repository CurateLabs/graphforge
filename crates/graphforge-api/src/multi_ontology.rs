//! Rust-owned public lifecycle facade for multi-ontology authority.
//!
//! Bindings project these request and receipt types. They never rebuild
//! inventory closure, identity, bridge, activation, or resolution semantics.

use std::collections::HashSet;

use graphforge_core::{GfError as EngineError, ProjectErrorCode};
use graphforge_ontology::{
    ActivationMode, ActivationRecord, ActivationScope, BridgeDocument, BridgeExportFormat,
    BridgeImportFormatHint, BridgeInspect, BridgeInventory, BridgeListEntry, BridgeSelector,
    BridgeSetId, BridgeUpdatePreview, CompositionDiagnostic, CompositionLimits, ExportFormat,
    ImportFormatHint, InventorySnapshot, ModuleInspect, ModuleListEntry, ModuleSelector,
    OntologyDoc, OntologyInventory, OntologyModuleId, ResolutionOutcome, ResolveRequest,
    SymbolKind, UpdatePreview,
};
use graphforge_storage::{WorkspaceCompositionModule, WorkspaceOntologyComposition};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    CancellationToken, CompositionChangePreview, CompositionChangeRequest,
    CompositionDataDisposition, GraphForge, WriteContext,
};

const MAX_ERROR_TEXT_BYTES: usize = 4096;
const MAX_ERROR_DIAGNOSTICS: usize = 64;

/// One stable bounded Rust-owned multi-ontology diagnostic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MultiOntologyDiagnostic {
    /// Stable dotted semantic code.
    pub code: String,
    /// Stable lifecycle phase.
    pub phase: String,
    /// Bounded path-free explanation.
    pub message: String,
    /// Sorted bounded semantic subjects.
    pub subjects: Vec<String>,
    /// Sorted bounded ambiguity candidates.
    pub candidates: Vec<String>,
    /// Bounded remediation owned by Rust.
    pub remediation: String,
    /// Applied per-diagnostic item cap.
    pub limit: usize,
}

/// Stable error envelope returned by every multi-ontology facade method.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MultiOntologyError {
    /// Existing GraphForge outer code, or `GF_ONTOLOGY_DIAGNOSTIC`.
    pub code: String,
    /// Bounded safe summary; clients branch on codes, never this string.
    pub message: String,
    /// Deterministically ordered bounded semantic diagnostics.
    pub diagnostics: Vec<MultiOntologyDiagnostic>,
}

/// Canonical Rust-owned validation result returned unchanged by every host.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MultiOntologyValidationReceipt {
    /// True only when no validation diagnostics were produced.
    pub valid: bool,
    /// Stable bounded diagnostics; empty exactly when `valid` is true.
    pub diagnostics: Vec<MultiOntologyDiagnostic>,
}

/// Host-neutral semantic case result for four-surface conformance comparison.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MultiOntologyCaseResult {
    /// Frozen comparison schema.
    pub contract: String,
    /// Canonical case identifier from the shared contract.
    pub case_id: String,
    /// `success` or `error`.
    pub outcome: String,
    /// Stable outer code (`GF_OK` on success).
    pub code: String,
    /// Sorted stable dotted diagnostic codes.
    pub diagnostic_codes: Vec<String>,
    /// Whether durable ontology authority changed during the case.
    pub authority_changed: bool,
    /// Semantic result cardinality, when the case returns a collection.
    pub item_count: Option<usize>,
    /// Canonically ordered semantic identities, never runtime UUIDs or paths.
    pub ordered_ids: Vec<String>,
}

/// Complete Rust semantic report compared byte-for-semantics with host reports.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MultiOntologyParityReport {
    /// `graphforge-multi-ontology-parity-result/1`.
    pub contract: String,
    /// Exactly the ten canonical ledger cases.
    pub cases: std::collections::BTreeMap<String, serde_json::Value>,
}

/// Retained graph evidence derived by the Rust certification query.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MultiOntologyRetainedDataReport {
    /// Bounded rows scanned while constructing the migration plan.
    pub rows_scanned: u64,
    /// Exact retained name after the authored property rename.
    pub name: String,
    /// Exact retained birth year after migration.
    pub birth_year: i64,
}

/// Canonical lifecycle certification report returned unchanged by thin hosts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MultiOntologyCertificationReport {
    /// `graphforge-multi-ontology-certification-result/1`.
    pub contract: String,
    /// Public surface producing this report.
    pub surface: String,
    /// Exact authority before the migration.
    pub composition_before: String,
    /// Exact current authority after the migration and reopen.
    pub composition_after: String,
    /// Rust-derived semantic migration plan identity.
    pub migration_plan_digest: String,
    /// Sorted exact content-derived module identities.
    pub module_ids: Vec<String>,
    /// Sorted exact content-derived bridge identities.
    pub bridge_ids: Vec<String>,
    /// Real retained-data query evidence.
    pub retained_data: MultiOntologyRetainedDataReport,
    /// Rust-derived lifecycle outcomes; portable/TCK evidence remains separate.
    pub cases: std::collections::BTreeMap<String, serde_json::Value>,
}

impl MultiOntologyError {
    #[allow(non_snake_case)]
    fn Validation(message: impl AsRef<str>) -> Self {
        Self {
            code: "GF_VALIDATION".into(),
            message: bounded_text(message.as_ref()),
            diagnostics: Vec::new(),
        }
    }

    /// Stable outer code.
    #[must_use]
    pub fn code(&self) -> &str {
        &self.code
    }
}

impl std::fmt::Display for MultiOntologyError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for MultiOntologyError {}

impl From<EngineError> for MultiOntologyError {
    fn from(error: EngineError) -> Self {
        Self {
            code: error.code().into(),
            message: bounded_text(&error.to_string()),
            diagnostics: Vec::new(),
        }
    }
}

impl From<graphforge_storage::PortableV2Error> for MultiOntologyError {
    fn from(error: graphforge_storage::PortableV2Error) -> Self {
        portable_error(error)
    }
}

type GfError = MultiOntologyError;

/// Exact authority identities every mutation is bound to.
#[derive(Debug, Clone)]
pub struct OntologyAuthorityExpectation {
    /// Caller operation and actor identity.
    pub context: WriteContext,
    /// Exact project generation observed by the caller.
    pub expected_project_generation_uuid: Uuid,
    /// Exact current composition fingerprint.
    pub expected_composition_fingerprint: Option<String>,
}

/// Immutable identities callers use for optimistic lifecycle requests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OntologyAuthorityState {
    /// Current selected project generation.
    pub project_generation_uuid: Uuid,
    /// Current composition identity, absent before first adoption.
    pub composition_fingerprint: Option<String>,
}

/// Validated but non-authoritative ontology module candidate.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModuleCandidate {
    /// Exact content-derived module identity.
    pub id: OntologyModuleId,
    /// Authored document; retained without flattening.
    pub document: OntologyDoc,
    /// Exact dependency identities.
    pub dependencies: Vec<OntologyModuleId>,
    /// Optional module activation override.
    pub enforcement: Option<ActivationMode>,
    /// Candidate state distinguishes create from text import.
    pub status: String,
}

/// Validated but non-authoritative bridge candidate.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BridgeCandidate {
    /// Exact content-derived bridge identity.
    pub id: BridgeSetId,
    /// Authored bridge document.
    pub document: BridgeDocument,
    /// Candidate state distinguishes create from text import.
    pub status: String,
}

/// Explicit module adoption request.
#[derive(Debug, Clone)]
pub struct ModuleAdoptionRequest {
    /// Exact authority expectation.
    pub authority: OntologyAuthorityExpectation,
    /// Candidate returned by create/import.
    pub candidate: ModuleCandidate,
}

/// Atomic module replacement request.
#[derive(Debug, Clone)]
pub struct ModuleUpdateRequest {
    /// Exact authority expectation.
    pub authority: OntologyAuthorityExpectation,
    /// Exact current module selector.
    pub selector: ModuleSelector,
    /// Replacement document.
    pub document: OntologyDoc,
    /// Replacement exact dependencies.
    pub dependencies: Vec<OntologyModuleId>,
    /// Optional replacement activation override.
    pub enforcement: Option<ActivationMode>,
}

/// Retained-data module migration bound to exact current authority.
#[derive(Debug, Clone)]
pub struct ModuleMigrationRequest {
    /// Exact authority expectation.
    pub authority: OntologyAuthorityExpectation,
    /// Exact current module selector.
    pub selector: ModuleSelector,
    /// Authored target document containing the complete migration route.
    pub document: OntologyDoc,
    /// Exact target dependencies.
    pub dependencies: Vec<OntologyModuleId>,
    /// Optional target activation override.
    pub enforcement: Option<ActivationMode>,
}

/// Deterministic retained-data migration preview.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModuleMigrationPreview {
    /// Exact module identity being replaced.
    pub previous_module: OntologyModuleId,
    /// Exact target module identity.
    pub next_module: OntologyModuleId,
    /// Bridges whose exact endpoint identity is rewritten.
    pub affected_bridges: Vec<String>,
    /// Rust-owned physical migration plan.
    pub plan: graphforge_storage::SemanticMigrationPlan,
}

/// Atomic retained-data migration publication receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModuleMigrationReceipt {
    /// Published project generation.
    pub project_generation_uuid: Uuid,
    /// Exact target composition.
    pub composition_fingerprint: String,
    /// Exact Rust-owned plan identity.
    pub plan_digest: String,
    /// Rows inspected by the deterministic pinned-parent plan.
    pub retained_rows_scanned: u64,
    /// Caller operation identity.
    pub operation_uuid: Uuid,
}

/// Safe module deletion request.
#[derive(Debug, Clone)]
pub struct ModuleDeleteRequest {
    /// Exact authority expectation.
    pub authority: OntologyAuthorityExpectation,
    /// Exact or uniquely-resolved selector.
    pub selector: ModuleSelector,
}

/// Explicit bridge adoption request.
#[derive(Debug, Clone)]
pub struct BridgeAdoptionRequest {
    /// Exact authority expectation.
    pub authority: OntologyAuthorityExpectation,
    /// Candidate returned by create/import.
    pub candidate: BridgeCandidate,
}

/// Atomic bridge replacement request.
#[derive(Debug, Clone)]
pub struct BridgeUpdateRequest {
    /// Exact authority expectation.
    pub authority: OntologyAuthorityExpectation,
    /// Exact current bridge selector.
    pub selector: BridgeSelector,
    /// Replacement bridge document.
    pub document: BridgeDocument,
}

/// Safe bridge deletion request.
#[derive(Debug, Clone)]
pub struct BridgeDeleteRequest {
    /// Exact authority expectation.
    pub authority: OntologyAuthorityExpectation,
    /// Exact or uniquely-resolved selector.
    pub selector: BridgeSelector,
}

/// Atomic activation-profile replacement request.
#[derive(Debug, Clone)]
pub struct ActivationProfileChangeRequest {
    /// Exact authority expectation.
    pub authority: OntologyAuthorityExpectation,
    /// New default mode.
    pub profile_default: ActivationMode,
    /// Complete replacement activation records.
    pub activation: Vec<ActivationRecord>,
}

/// Stable receipt shared by module, bridge, and activation mutations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MultiOntologyMutationReceipt {
    /// Published project generation.
    pub project_generation_uuid: Uuid,
    /// Published composition identity.
    pub composition_fingerprint: String,
    /// Canonical candidate digest.
    pub candidate_sha256: String,
    /// Caller operation identity.
    pub operation_uuid: Uuid,
}

/// Pure composition validation receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompositionValidationReceipt {
    /// Recomputed exact composition identity.
    pub composition_fingerprint: String,
    /// Identity-sorted module closure.
    pub modules: Vec<String>,
    /// Identity-sorted bridge closure.
    pub bridges: Vec<String>,
}

/// Request for Rust-owned qualified/unqualified resolution explanation.
#[derive(Debug, Clone)]
pub struct ResolutionExplainRequest {
    /// Optional exact module identity.
    pub module: Option<OntologyModuleId>,
    /// Symbol kind.
    pub kind: SymbolKind,
    /// Local symbol identifier.
    pub local_id: String,
    /// Bounded ambiguity candidate count.
    pub max_candidates: usize,
}

/// Stable resolution success or bounded diagnostic explanation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolutionExplanation {
    /// Successful outcome, absent on failure.
    pub outcome: Option<ResolutionOutcome>,
    /// Stable Rust-owned diagnostics, absent on success.
    pub diagnostics: Vec<CompositionDiagnostic>,
}

impl GraphForge {
    /// Preview an authored module migration against the pinned graph without mutation.
    pub fn preview_migrate_ontology_module(
        &self,
        request: &ModuleMigrationRequest,
    ) -> Result<ModuleMigrationPreview, GfError> {
        let (candidate, previous_module, next_module, affected_bridges) =
            self.module_migration_candidate(request)?;
        let parent = graphforge_storage::resolve_generation_by_uuid(
            self.resolved_generation.container_root(),
            request.authority.expected_project_generation_uuid,
        )
        .map_err(MultiOntologyError::from)?;
        let previous = composition_from_generation(&parent)?
            .ok_or_else(|| GfError::Validation("migration parent composition is missing"))?;
        let previous_compiled = previous.compile().map_err(MultiOntologyError::from)?;
        let next_compiled = candidate.compile().map_err(MultiOntologyError::from)?;
        let bindings = graphforge_storage::semantic_storage_bindings(&parent)
            .map_err(MultiOntologyError::from)?
            .ok_or_else(|| GfError::Validation("semantic storage bindings are missing"))?;
        let plan = graphforge_storage::SemanticStorageBindings::plan_retained_data_migration(
            &previous_compiled,
            &next_compiled,
            &bindings,
            &parent.graph_tree_root(),
        )
        .map_err(MultiOntologyError::from)?;
        Ok(ModuleMigrationPreview {
            previous_module,
            next_module,
            affected_bridges,
            plan,
        })
    }

    /// Re-derive and atomically publish an authored retained-data migration.
    #[allow(clippy::too_many_lines)] // replay authentication, private staging, and publication are one transaction
    pub fn migrate_ontology_module(
        &mut self,
        request: &ModuleMigrationRequest,
        preview: &ModuleMigrationPreview,
        cancellation: Option<&CancellationToken>,
    ) -> Result<ModuleMigrationReceipt, GfError> {
        let fresh = self.preview_migrate_ontology_module(request)?;
        if &fresh != preview {
            return Err(GfError::Validation(
                "module migration preview does not match exact parent",
            ));
        }
        let expected_generation = migration_generation_uuid(request, &fresh);
        if let Some(published) = graphforge_storage::published_project_transaction(
            self.resolved_generation.container_root(),
            request.authority.context.operation_uuid.0,
        )
        .map_err(MultiOntologyError::from)?
        {
            if published.generation_uuid != expected_generation {
                return Err(MultiOntologyError {
                    code: "GF_IDEMPOTENCY_CONFLICT".into(),
                    message: "migration operation UUID was reused with different content".into(),
                    diagnostics: Vec::new(),
                });
            }
            return Ok(ModuleMigrationReceipt {
                project_generation_uuid: published.generation_uuid,
                composition_fingerprint: fresh.plan.to_composition_fingerprint,
                plan_digest: fresh.plan.plan_digest,
                retained_rows_scanned: fresh.plan.retained_rows_scanned,
                operation_uuid: request.authority.context.operation_uuid.0,
            });
        }
        cancellation.map_or(Ok(()), CancellationToken::checkpoint)?;
        let (candidate, _, _, _) = self.module_migration_candidate(request)?;
        let private = tempfile::tempdir().map_err(|_| {
            GfError::Validation("module migration private staging cannot be created")
        })?;
        let candidate_graph = private.path().join("graph");
        let evidence = graphforge_storage::materialize_semantic_migration(
            &preview.plan,
            &self.resolved_generation.graph_tree_root(),
            &candidate_graph,
            graphforge_storage::SemanticMigrationLimits::default(),
            || cancellation.map_or(Ok(()), CancellationToken::checkpoint),
        )
        .map_err(MultiOntologyError::from)?;
        if evidence.plan_digest != preview.plan.plan_digest {
            return Err(GfError::Validation(
                "materialized migration evidence does not match preview plan",
            ));
        }
        cancellation.map_or(Ok(()), CancellationToken::checkpoint)?;
        let ontology = self
            .workspace_ontology()
            .map_err(MultiOntologyError::from)?;
        let mut configuration = self
            .workspace_configuration()
            .map_err(MultiOntologyError::from)?;
        configuration.ontology_mode = match candidate.profile_default {
            ActivationMode::Exploratory => graphforge_storage::WorkspaceOntologyMode::None,
            ActivationMode::Advisory => graphforge_storage::WorkspaceOntologyMode::Advisory,
            ActivationMode::Strict => graphforge_storage::WorkspaceOntologyMode::Strict,
        };
        crate::workspace_ontology::publish_workspace_records_with_graph_tree(
            self,
            request.authority.context.operation_uuid.0,
            request.authority.context.actor_uuid,
            &ontology,
            &configuration,
            &candidate,
            &preview.plan.bindings,
            expected_generation,
            &candidate_graph,
            cancellation,
        )
        .map_err(MultiOntologyError::from)?;
        crate::rematerialize_graph_workspace(&self.resolved_generation, &self.dir)
            .map_err(MultiOntologyError::from)?;
        *self
            .semantic_storage_bindings
            .lock()
            .expect("semantic storage binding lock poisoned") = Some(preview.plan.bindings.clone());
        let compiled = candidate.compile().map_err(MultiOntologyError::from)?;
        let binding = graphforge_ir::CompositionBindingContext::new(
            std::sync::Arc::new(compiled),
            candidate.bridges.clone(),
            graphforge_ir::CompositionBindingLimits::default(),
        )
        .with_generation_storage_ids(
            preview
                .plan
                .bindings
                .bindings
                .iter()
                .map(|binding| (binding.symbol.clone(), binding.storage_id)),
        );
        *self
            .default_composition_context
            .lock()
            .expect("composition binding lock poisoned") = Some(std::sync::Arc::new(binding));
        self.ontology_mode = configuration.ontology_mode.execution_mode();
        *self
            .runtime_catalog
            .lock()
            .expect("runtime catalog poisoned") =
            crate::load_runtime_catalog(&self.dir).map_err(MultiOntologyError::from)?;
        self.adjacency_provider.invalidate();
        Ok(ModuleMigrationReceipt {
            project_generation_uuid: expected_generation,
            composition_fingerprint: preview.plan.to_composition_fingerprint.clone(),
            plan_digest: preview.plan.plan_digest.clone(),
            retained_rows_scanned: preview.plan.retained_rows_scanned,
            operation_uuid: request.authority.context.operation_uuid.0,
        })
    }

    /// Produce the canonical post-migration report from current Rust authority
    /// and a real query over retained data. Hosts serialize this value unchanged.
    pub fn multi_ontology_certification_report(
        &self,
        surface: &str,
        composition_before: &str,
        migration_plan_digest: &str,
        rows_scanned: u64,
    ) -> Result<MultiOntologyCertificationReport, GfError> {
        if !matches!(surface, "rust" | "python" | "node" | "cli") {
            return Err(GfError::Validation(
                "certification surface must be rust, python, node, or cli",
            ));
        }

        let composition = self.required_composition()?;
        let mut module_ids = composition
            .modules
            .iter()
            .map(|module| module.id.display_ref())
            .collect::<Vec<_>>();
        module_ids.sort();
        module_ids.dedup();
        let mut bridge_ids = composition
            .bridges
            .iter()
            .map(bridge_id)
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .map(|id| id.display_ref())
            .collect::<Vec<_>>();
        bridge_ids.sort();
        bridge_ids.dedup();
        if module_ids.len() != 6 || bridge_ids.len() != 3 {
            return Err(GfError::Validation(
                "certification requires exact six-module, three-bridge authority",
            ));
        }
        let result = self
            .execute(
                "MATCH (person:Human) RETURN person.display_name AS name, \
                 person.birth_year AS birth_year",
            )
            .map_err(MultiOntologyError::from)?;
        if result.batches.len() != 1 || result.batches[0].num_rows() != 1 {
            return Err(GfError::Validation(
                "certification requires exactly one retained Human row",
            ));
        }
        let batch = &result.batches[0];
        let name_column = batch
            .column_by_name("name")
            .ok_or_else(|| GfError::Validation("retained name column is missing"))?;
        let name = arrow::util::display::array_value_to_string(name_column, 0)
            .map_err(|_| GfError::Validation("retained name cannot be rendered"))?;
        let birth_year_column = batch
            .column_by_name("birth_year")
            .ok_or_else(|| GfError::Validation("retained birth_year column is missing"))?;
        let birth_year_text = arrow::util::display::array_value_to_string(birth_year_column, 0)
            .map_err(|_| GfError::Validation("retained birth_year cannot be rendered"))?;
        let birth_year = birth_year_text
            .parse::<i64>()
            .map_err(|_| GfError::Validation("retained birth_year must be an integer"))?;
        let retained_data = MultiOntologyRetainedDataReport {
            rows_scanned,
            name,
            birth_year,
        };
        let mut cases = std::collections::BTreeMap::new();
        cases.insert(
            "authority_reopened".into(),
            serde_json::json!({"composition_fingerprint": composition.composition_fingerprint.clone()}),
        );
        cases.insert(
            "bridge_set_retained".into(),
            serde_json::json!({"bridge_ids": bridge_ids.clone()}),
        );
        cases.insert(
            "module_set_retained".into(),
            serde_json::json!({"module_ids": module_ids.clone()}),
        );
        cases.insert(
            "migration_receipt".into(),
            serde_json::json!({"plan_digest": migration_plan_digest}),
        );
        cases.insert(
            "retained_data_query".into(),
            serde_json::to_value(&retained_data)
                .map_err(|_| GfError::Validation("retained report cannot be encoded"))?,
        );
        Ok(MultiOntologyCertificationReport {
            contract: "graphforge-multi-ontology-certification-result/1".into(),
            surface: surface.into(),
            composition_before: composition_before.to_owned(),
            composition_after: composition.composition_fingerprint,
            migration_plan_digest: migration_plan_digest.to_owned(),
            module_ids,
            bridge_ids,
            retained_data,
            cases,
        })
    }

    /// Inspect exact authority identities for a subsequent mutation request.
    pub fn ontology_authority_state(&self) -> Result<OntologyAuthorityState, GfError> {
        Ok(OntologyAuthorityState {
            project_generation_uuid: self.generation_for_read()?.generation_uuid(),
            composition_fingerprint: self
                .workspace_ontology_composition()?
                .map(|composition| composition.composition_fingerprint),
        })
    }

    /// List durable ontology modules in exact identity order.
    pub fn ontology_modules(&self) -> Result<Vec<ModuleListEntry>, GfError> {
        Ok(module_inventory(&self.required_composition()?)?.list())
    }

    /// Inspect one durable ontology module.
    pub fn inspect_ontology_module(
        &self,
        selector: &ModuleSelector,
    ) -> Result<ModuleInspect, GfError> {
        module_inventory(&self.required_composition()?)?
            .inspect(selector)
            .map_err(composition_error)
    }

    /// Validate a module without changing staging or durable authority.
    pub fn validate_ontology_module(
        &self,
        document: &OntologyDoc,
    ) -> Result<MultiOntologyValidationReceipt, GfError> {
        let inventory = module_inventory(&self.required_composition()?)?;
        Ok(validation_receipt(inventory.validate_document(document)))
    }

    /// Create/register a validated non-authoritative module candidate.
    pub fn create_ontology_module(
        &self,
        document: OntologyDoc,
        dependencies: Vec<OntologyModuleId>,
        enforcement: Option<ActivationMode>,
    ) -> Result<ModuleCandidate, GfError> {
        let composition = self.required_composition()?;
        let mut inventory = module_inventory(&composition)?;
        inventory
            .validate_document(&document)
            .map_err(composition_error)?;
        let exact_id = OntologyModuleId {
            ontology_id: document.ontology_id.clone(),
            authored_version: document.version.clone(),
            canonical_digest: graphforge_ontology::module_document_digest(&document)
                .map_err(GfError::Validation)?,
        };
        if composition.modules.iter().any(|module| {
            module.id == exact_id
                && module.document == document
                && module.dependencies == dependencies
        }) {
            return Ok(ModuleCandidate {
                id: exact_id,
                document,
                dependencies,
                enforcement,
                status: "validated".into(),
            });
        }
        let id = inventory
            .create_register(
                document.clone(),
                dependencies.clone(),
                enforcement,
                "candidate",
            )
            .map_err(composition_error)?;
        Ok(ModuleCandidate {
            id,
            document,
            dependencies,
            enforcement,
            status: "validated".into(),
        })
    }

    /// Parse and validate a non-authoritative module import candidate.
    pub fn import_ontology_module(
        &self,
        text: &str,
        format: ImportFormatHint,
        dependencies: Vec<OntologyModuleId>,
    ) -> Result<ModuleCandidate, GfError> {
        let mut inventory = module_inventory(&self.required_composition()?)?;
        let id = inventory
            .import_text(text, format, dependencies.clone(), "candidate")
            .map_err(composition_error)?;
        let document = staged_module_document(text, format)?;
        Ok(ModuleCandidate {
            id,
            document,
            dependencies,
            enforcement: None,
            status: "candidate".into(),
        })
    }

    /// Explicitly adopt a validated/imported module and publish atomically.
    pub fn adopt_ontology_module(
        &mut self,
        request: &ModuleAdoptionRequest,
        cancellation: Option<&CancellationToken>,
    ) -> Result<MultiOntologyMutationReceipt, GfError> {
        let mut candidate = self.checked_candidate(&request.authority)?;
        reject_duplicate_module(&candidate, &request.candidate.id)?;
        candidate.modules.push(WorkspaceCompositionModule {
            id: request.candidate.id.clone(),
            dependencies: request.candidate.dependencies.clone(),
            document: request.candidate.document.clone(),
            allow_projected_identity: false,
        });
        if let Some(mode) = request.candidate.enforcement {
            candidate.activation.push(ActivationRecord {
                scope: ActivationScope::Module,
                subject: request.candidate.id.display_ref(),
                mode,
            });
        }
        let operation = format!("module.adopt:{}", request.candidate.id.display_ref());
        self.publish_multi_ontology_candidate(
            &request.authority,
            candidate,
            operation,
            cancellation,
        )
    }

    /// Preview one exact module replacement using inventory authority.
    pub fn preview_update_ontology_module(
        &self,
        selector: &ModuleSelector,
        document: &OntologyDoc,
        dependencies: &[OntologyModuleId],
    ) -> Result<UpdatePreview, GfError> {
        module_inventory(&self.required_composition()?)?
            .preview_update(selector, document, dependencies)
            .map_err(composition_error)
    }

    /// Replace one exact module and publish atomically.
    pub fn update_ontology_module(
        &mut self,
        request: &ModuleUpdateRequest,
        cancellation: Option<&CancellationToken>,
    ) -> Result<MultiOntologyMutationReceipt, GfError> {
        let mut candidate = self.checked_candidate(&request.authority)?;
        let index = resolve_module_index(&candidate, &request.selector)?;
        let prior = candidate.modules[index].id.clone();
        if request.document.ontology_id != candidate.modules[index].document.ontology_id {
            return Err(GfError::Validation("module update must retain ontology_id"));
        }
        let mut inventory = module_inventory(&candidate)?;
        let next_id = inventory
            .create_register(
                request.document.clone(),
                request.dependencies.clone(),
                request.enforcement,
                "update-validation",
            )
            .map_err(composition_error)?;
        let next = ModuleCandidate {
            id: next_id,
            document: request.document.clone(),
            dependencies: request.dependencies.clone(),
            enforcement: request.enforcement,
            status: "validated".into(),
        };
        candidate.modules[index] = WorkspaceCompositionModule {
            id: next.id.clone(),
            dependencies: next.dependencies,
            document: next.document,
            allow_projected_identity: false,
        };
        rewrite_module_identity(&mut candidate, &prior, &next.id)?;
        set_module_activation(&mut candidate, &next.id, request.enforcement);
        let operation = format!("module.update:{}", prior.display_ref());
        self.publish_multi_ontology_candidate(
            &request.authority,
            candidate,
            operation,
            cancellation,
        )
    }

    fn module_migration_candidate(
        &self,
        request: &ModuleMigrationRequest,
    ) -> Result<
        (
            WorkspaceOntologyComposition,
            OntologyModuleId,
            OntologyModuleId,
            Vec<String>,
        ),
        GfError,
    > {
        let mut candidate = self.checked_candidate(&request.authority)?;
        let index = resolve_module_index(&candidate, &request.selector)?;
        let previous_module = candidate.modules[index].id.clone();
        if request.document.ontology_id != previous_module.ontology_id {
            return Err(GfError::Validation(
                "module migration must retain ontology_id",
            ));
        }
        let mut inventory = module_inventory(&candidate)?;
        let next_module = inventory
            .create_register(
                request.document.clone(),
                request.dependencies.clone(),
                request.enforcement,
                "migration-validation",
            )
            .map_err(composition_error)?;
        candidate.modules[index] = WorkspaceCompositionModule {
            id: next_module.clone(),
            dependencies: request.dependencies.clone(),
            document: request.document.clone(),
            allow_projected_identity: false,
        };
        let mut affected_bridges = candidate
            .bridges
            .iter()
            .filter(|bridge| {
                bridge.source_modules.contains(&previous_module)
                    || bridge.target_modules.contains(&previous_module)
            })
            .map(bridge_id)
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .map(|id| id.display_ref())
            .collect::<Vec<_>>();
        affected_bridges.sort();
        rewrite_bridge_symbols_for_migration(
            &mut candidate,
            &previous_module,
            &next_module,
            &request.document,
        )?;
        rewrite_module_identity(&mut candidate, &previous_module, &next_module)?;
        set_module_activation(&mut candidate, &next_module, request.enforcement);
        let compiled = compile_candidate(&candidate)?;
        candidate = WorkspaceOntologyComposition::from_compiled(&compiled, candidate.bridges);
        Ok((candidate, previous_module, next_module, affected_bridges))
    }

    /// Preview safe module deletion, including dependency/activation blockers.
    pub fn preview_delete_ontology_module(
        &self,
        selector: &ModuleSelector,
    ) -> Result<graphforge_ontology::DeletePreview, GfError> {
        let composition = self.required_composition()?;
        let preview = module_inventory(&composition)?
            .preview_delete(selector)
            .map_err(composition_error)?;
        enrich_module_delete_preview(preview, &composition)
    }

    /// Delete one unreferenced module and publish atomically.
    pub fn delete_ontology_module(
        &mut self,
        request: &ModuleDeleteRequest,
        cancellation: Option<&CancellationToken>,
    ) -> Result<MultiOntologyMutationReceipt, GfError> {
        let mut candidate = self.checked_candidate(&request.authority)?;
        let preview = module_inventory(&candidate)?
            .preview_delete(&request.selector)
            .map_err(composition_error)?;
        let preview = enrich_module_delete_preview(preview, &candidate)?;
        if !preview.safe {
            return Err(dependency_blocked_error(&preview));
        }
        candidate
            .modules
            .retain(|module| module.id != preview.target);
        candidate
            .activation
            .retain(|record| record.subject != preview.target.display_ref());
        let operation = format!("module.delete:{}", preview.target.display_ref());
        self.publish_multi_ontology_candidate(
            &request.authority,
            candidate,
            operation,
            cancellation,
        )
    }

    /// Deterministically export one module.
    pub fn export_ontology_module(
        &self,
        selector: &ModuleSelector,
        format: ExportFormat,
    ) -> Result<String, GfError> {
        module_inventory(&self.required_composition()?)?
            .export_module(selector, format)
            .map_err(composition_error)
    }

    /// List durable bridges in exact identity order.
    pub fn ontology_bridges(&self) -> Result<Vec<BridgeListEntry>, GfError> {
        Ok(bridge_inventory(&self.required_composition()?)?.list())
    }

    /// Inspect one durable bridge.
    pub fn inspect_ontology_bridge(
        &self,
        selector: &BridgeSelector,
    ) -> Result<BridgeInspect, GfError> {
        bridge_inventory(&self.required_composition()?)?
            .inspect(selector)
            .map_err(composition_error)
    }

    /// Validate a bridge without mutation.
    pub fn validate_ontology_bridge(
        &self,
        document: &BridgeDocument,
    ) -> Result<MultiOntologyValidationReceipt, GfError> {
        let inventory = bridge_inventory(&self.required_composition()?)?;
        Ok(validation_receipt(inventory.validate_document(document)))
    }

    /// Create/register a validated non-authoritative bridge candidate.
    pub fn create_ontology_bridge(
        &self,
        document: BridgeDocument,
    ) -> Result<BridgeCandidate, GfError> {
        let mut inventory = bridge_inventory(&self.required_composition()?)?;
        let id = inventory
            .create_register(document.clone(), "candidate")
            .map_err(composition_error)?;
        Ok(BridgeCandidate {
            id,
            document,
            status: "validated".into(),
        })
    }

    /// Parse and validate a non-authoritative bridge import candidate.
    pub fn import_ontology_bridge(
        &self,
        text: &str,
        format: BridgeImportFormatHint,
    ) -> Result<BridgeCandidate, GfError> {
        let document: BridgeDocument = match format {
            BridgeImportFormatHint::Json => serde_json::from_str(text),
            BridgeImportFormatHint::Yaml => serde_yaml::from_str(text)
                .map_err(|e| serde_json::Error::io(std::io::Error::other(e))),
            BridgeImportFormatHint::Auto if text.trim_start().starts_with(char::from(0x7b)) => {
                serde_json::from_str(text)
            }
            BridgeImportFormatHint::Auto => serde_yaml::from_str(text)
                .map_err(|e| serde_json::Error::io(std::io::Error::other(e))),
        }
        .map_err(|_| GfError::Validation("bridge import is malformed"))?;
        let mut inventory = bridge_inventory(&self.required_composition()?)?;
        let id = inventory
            .import_text(text, format, "candidate")
            .map_err(composition_error)?;
        Ok(BridgeCandidate {
            id,
            document,
            status: "candidate".into(),
        })
    }

    /// Explicitly adopt a bridge and publish atomically.
    pub fn adopt_ontology_bridge(
        &mut self,
        request: &BridgeAdoptionRequest,
        cancellation: Option<&CancellationToken>,
    ) -> Result<MultiOntologyMutationReceipt, GfError> {
        let mut candidate = self.checked_candidate(&request.authority)?;
        reject_duplicate_bridge(&candidate, &request.candidate.id)?;
        let mut inventory = bridge_inventory(&candidate)?;
        let computed = inventory
            .create_register(request.candidate.document.clone(), "adopt-validation")
            .map_err(composition_error)?;
        if computed != request.candidate.id {
            return Err(GfError::Validation(
                "bridge candidate identity does not match document",
            ));
        }
        candidate.bridges.push(request.candidate.document.clone());
        let operation = format!("bridge.adopt:{}", request.candidate.id.display_ref());
        self.publish_multi_ontology_candidate(
            &request.authority,
            candidate,
            operation,
            cancellation,
        )
    }

    /// Preview one bridge replacement.
    pub fn preview_update_ontology_bridge(
        &self,
        selector: &BridgeSelector,
        document: &BridgeDocument,
    ) -> Result<BridgeUpdatePreview, GfError> {
        bridge_inventory(&self.required_composition()?)?
            .preview_update(selector, document)
            .map_err(composition_error)
    }

    /// Replace one exact bridge and publish atomically.
    pub fn update_ontology_bridge(
        &mut self,
        request: &BridgeUpdateRequest,
        cancellation: Option<&CancellationToken>,
    ) -> Result<MultiOntologyMutationReceipt, GfError> {
        let mut candidate = self.checked_candidate(&request.authority)?;
        let index = resolve_bridge_index(&candidate, &request.selector)?;
        let prior_ids = candidate
            .bridges
            .iter()
            .map(bridge_id)
            .collect::<Result<Vec<_>, _>>()?;
        let operation = format!("bridge.update:{}", prior_ids[index].display_ref());
        if request.document.bridge_id != candidate.bridges[index].bridge_id {
            return Err(GfError::Validation("bridge update must retain bridge_id"));
        }
        let mut inventory = bridge_inventory(&candidate)?;
        let computed = inventory
            .create_register(request.document.clone(), "update-validation")
            .map_err(composition_error)?;
        if computed != bridge_id(&request.document)? {
            return Err(GfError::Validation(
                "bridge replacement identity does not match document",
            ));
        }
        candidate.bridges[index] = request.document.clone();
        cascade_bridge_identities(&mut candidate, prior_ids)?;
        self.publish_multi_ontology_candidate(
            &request.authority,
            candidate,
            operation,
            cancellation,
        )
    }

    /// Preview safe bridge deletion.
    pub fn preview_delete_ontology_bridge(
        &self,
        selector: &BridgeSelector,
    ) -> Result<graphforge_ontology::BridgeDeletePreview, GfError> {
        bridge_inventory(&self.required_composition()?)?
            .preview_delete(selector)
            .map_err(composition_error)
    }

    /// Delete one unreferenced bridge and publish atomically.
    pub fn delete_ontology_bridge(
        &mut self,
        request: &BridgeDeleteRequest,
        cancellation: Option<&CancellationToken>,
    ) -> Result<MultiOntologyMutationReceipt, GfError> {
        let mut candidate = self.checked_candidate(&request.authority)?;
        let preview = bridge_inventory(&candidate)?
            .preview_delete(&request.selector)
            .map_err(composition_error)?;
        if !preview.safe {
            return Err(GfError::Validation("bridge deletion is dependency-blocked"));
        }
        candidate
            .bridges
            .retain(|doc| bridge_id(doc).ok().as_ref() != Some(&preview.target));
        candidate
            .activation
            .retain(|record| record.subject != preview.target.display_ref());
        let operation = format!("bridge.delete:{}", preview.target.display_ref());
        self.publish_multi_ontology_candidate(
            &request.authority,
            candidate,
            operation,
            cancellation,
        )
    }

    /// Deterministically export one bridge.
    pub fn export_ontology_bridge(
        &self,
        selector: &BridgeSelector,
        format: BridgeExportFormat,
    ) -> Result<String, GfError> {
        bridge_inventory(&self.required_composition()?)?
            .export_bridge(selector, format)
            .map_err(composition_error)
    }

    /// Inspect the complete current activation profile.
    pub fn ontology_activation_profile(
        &self,
    ) -> Result<(ActivationMode, Vec<ActivationRecord>), GfError> {
        let composition = self.required_composition()?;
        Ok((composition.profile_default, composition.activation))
    }

    /// Replace the activation profile and publish atomically.
    pub fn change_ontology_activation_profile(
        &mut self,
        request: &ActivationProfileChangeRequest,
        cancellation: Option<&CancellationToken>,
    ) -> Result<MultiOntologyMutationReceipt, GfError> {
        let mut candidate = self.checked_candidate(&request.authority)?;
        candidate.profile_default = request.profile_default;
        candidate.activation.clone_from(&request.activation);
        self.publish_multi_ontology_candidate(
            &request.authority,
            candidate,
            "activation.change".into(),
            cancellation,
        )
    }

    /// Validate and deterministically inventory one complete composition.
    pub fn validate_ontology_composition(
        &self,
        candidate: &WorkspaceOntologyComposition,
    ) -> Result<CompositionValidationReceipt, GfError> {
        let compiled = candidate.compile()?;
        let mut bridges = candidate
            .bridges
            .iter()
            .map(bridge_id)
            .collect::<Result<Vec<_>, _>>()?;
        bridges.sort_by_key(BridgeSetId::sort_key);
        Ok(CompositionValidationReceipt {
            composition_fingerprint: compiled.fingerprint,
            modules: compiled
                .modules
                .iter()
                .map(|m| m.id.display_ref())
                .collect(),
            bridges: bridges.into_iter().map(|id| id.display_ref()).collect(),
        })
    }

    /// Run the #840 full stored-data and portability preflight authority.
    pub fn preflight_ontology_composition(
        &self,
        request: &CompositionChangeRequest,
        cancellation: Option<&CancellationToken>,
    ) -> Result<CompositionChangePreview, GfError> {
        Ok(self.preview_ontology_composition_change(request, cancellation)?)
    }

    /// Inspect a verified durable portable candidate without adopting it.
    pub fn portable_ontology_staging(
        &self,
        limits: graphforge_storage::PortableV2Limits,
    ) -> Result<Option<graphforge_storage::WorkspacePortableOntologyStaging>, GfError> {
        graphforge_storage::load_portable_ontology_staging(&self.generation_for_read()?, limits)
            .map_err(MultiOntologyError::from)
    }

    /// Explicitly adopt the verified post-import portable ontology candidate.
    pub fn adopt_portable_ontology_staging(
        &mut self,
        authority: &OntologyAuthorityExpectation,
        limits: graphforge_storage::PortableV2Limits,
        cancellation: Option<&CancellationToken>,
    ) -> Result<MultiOntologyMutationReceipt, GfError> {
        let parent = graphforge_storage::resolve_generation_by_uuid(
            self.resolved_generation.container_root(),
            authority.expected_project_generation_uuid,
        )?;
        let staged = graphforge_storage::load_portable_ontology_staging(&parent, limits)
            .map_err(MultiOntologyError::from)?
            .ok_or_else(|| GfError::Validation("portable ontology staging is absent"))?;
        // Bind adoption to the current authority even though the candidate is
        // stored as a separate, deliberately non-authoritative participant.
        let _ = self.checked_candidate(authority)?;
        let operation = format!("portable.staging.adopt:{}", staged.package_digest);
        self.publish_multi_ontology_candidate(
            authority,
            staged.composition,
            operation,
            cancellation,
        )
    }

    /// Explain qualified/unqualified resolution with stable bounded diagnostics.
    pub fn explain_ontology_resolution(
        &self,
        request: &ResolutionExplainRequest,
    ) -> Result<ResolutionExplanation, GfError> {
        let compiled = self.required_composition()?.compile()?;
        match compiled.resolve(&ResolveRequest {
            module: request.module.as_ref(),
            kind: request.kind,
            local_id: &request.local_id,
            max_candidates: request.max_candidates.clamp(1, 64),
        }) {
            Ok(outcome) => Ok(ResolutionExplanation {
                outcome: Some(outcome),
                diagnostics: Vec::new(),
            }),
            Err(error) => Ok(ResolutionExplanation {
                outcome: None,
                diagnostics: error.diagnostics,
            }),
        }
    }

    fn required_composition(&self) -> Result<WorkspaceOntologyComposition, GfError> {
        self.workspace_ontology_composition()?
            .map_or_else(empty_composition, Ok)
    }

    fn checked_candidate(
        &self,
        expected: &OntologyAuthorityExpectation,
    ) -> Result<WorkspaceOntologyComposition, GfError> {
        let replay_exists = graphforge_storage::published_project_transaction(
            self.resolved_generation.container_root(),
            expected.context.operation_uuid.0,
        )?
        .is_some();
        let generation = graphforge_storage::resolve_generation_by_uuid(
            self.resolved_generation.container_root(),
            expected.expected_project_generation_uuid,
        )
        .map_err(|error| replay_authority_error(replay_exists, error))?;
        let persisted = composition_from_generation(&generation)?;
        let actual_fingerprint = persisted
            .as_ref()
            .map(|composition| composition.composition_fingerprint.clone());
        if actual_fingerprint != expected.expected_composition_fingerprint {
            return if replay_exists {
                Err(transaction_conflict(
                    "operation UUID was already published with different ontology authority",
                ))
            } else {
                Err(GfError::Validation(
                    "ontology authority composition fingerprint is stale",
                ))
            };
        }
        persisted.map_or_else(empty_composition, Ok)
    }

    fn publish_multi_ontology_candidate(
        &mut self,
        expected: &OntologyAuthorityExpectation,
        candidate: WorkspaceOntologyComposition,
        operation: String,
        cancellation: Option<&CancellationToken>,
    ) -> Result<MultiOntologyMutationReceipt, GfError> {
        let compiled = compile_candidate(&candidate)?;
        let candidate = WorkspaceOntologyComposition::from_compiled(&compiled, candidate.bridges);
        let change = CompositionChangeRequest {
            context: expected.context.clone(),
            expected_project_generation_uuid: expected.expected_project_generation_uuid,
            expected_composition_fingerprint: expected.expected_composition_fingerprint.clone(),
            candidate,
            data_disposition: CompositionDataDisposition::RequireConformingOperation { operation },
        };
        if let Some(receipt) = self.replay_ontology_composition_change(&change)? {
            return Ok(MultiOntologyMutationReceipt {
                project_generation_uuid: receipt.project_generation_uuid,
                composition_fingerprint: receipt.composition_fingerprint,
                candidate_sha256: receipt.candidate_sha256,
                operation_uuid: expected.context.operation_uuid.0,
            });
        }
        let preview = self.preview_ontology_composition_change(&change, cancellation)?;
        if !preview.diagnostics.is_empty() {
            return Err(composition_change_error(preview.diagnostics));
        }
        let receipt = self.publish_ontology_composition_change(&change, &preview, cancellation)?;
        Ok(MultiOntologyMutationReceipt {
            project_generation_uuid: receipt.project_generation_uuid,
            composition_fingerprint: receipt.composition_fingerprint,
            candidate_sha256: receipt.candidate_sha256,
            operation_uuid: expected.context.operation_uuid.0,
        })
    }
}

fn replay_authority_error(replay_exists: bool, error: EngineError) -> GfError {
    if replay_exists {
        transaction_conflict(
            "operation UUID was already published with different ontology authority",
        )
    } else {
        error.into()
    }
}

fn transaction_conflict(message: &str) -> GfError {
    EngineError::Project {
        code: ProjectErrorCode::TransactionConflict,
        message: message.into(),
    }
    .into()
}

fn module_inventory(
    composition: &WorkspaceOntologyComposition,
) -> Result<OntologyInventory, GfError> {
    if let [module] = composition.modules.as_slice()
        && module.allow_projected_identity
        && module.dependencies.is_empty()
        && composition.bridges.is_empty()
        && composition.activation.is_empty()
    {
        return OntologyInventory::from_legacy_single(
            module.document.clone(),
            true,
            composition.profile_default,
        )
        .map_err(composition_error);
    }
    OntologyInventory::reopen(InventorySnapshot {
        schema_version: 1,
        generation: 0,
        profile_default: composition.profile_default,
        activation: composition.activation.clone(),
        bridges: composition
            .bridges
            .iter()
            .map(bridge_id)
            .collect::<Result<Vec<_>, _>>()?,
        adopted: composition
            .modules
            .iter()
            .map(|m| graphforge_ontology::SnapshotModule {
                id: m.id.clone(),
                dependencies: m.dependencies.clone(),
                doc: m.document.clone(),
                enforcement: None,
            })
            .collect(),
        composition_fingerprint: composition.composition_fingerprint.clone(),
        receipts: Vec::new(),
    })
    .map_err(composition_error)
}

fn bridge_inventory(
    composition: &WorkspaceOntologyComposition,
) -> Result<BridgeInventory, GfError> {
    let compiled = composition.compile()?;
    let modules = compiled
        .modules
        .iter()
        .map(|module| graphforge_ontology::SnapshotModuleSymbols {
            id: module.id.clone(),
            entities: module
                .symbols
                .iter()
                .filter(|s| s.kind == SymbolKind::Entity)
                .map(|s| s.local_id.clone())
                .collect(),
            relations: module
                .symbols
                .iter()
                .filter(|s| s.kind == SymbolKind::Relation)
                .map(|s| s.local_id.clone())
                .collect(),
            properties: module
                .symbols
                .iter()
                .filter(|s| s.kind == SymbolKind::Property)
                .map(|s| s.local_id.clone())
                .collect(),
        })
        .collect();
    let adopted = composition
        .bridges
        .iter()
        .map(|document| {
            Ok(graphforge_ontology::SnapshotBridge {
                id: bridge_id(document)?,
                dependencies: document.dependencies.clone(),
                doc: document.clone(),
            })
        })
        .collect::<Result<Vec<_>, GfError>>()?;
    let known = adopted
        .iter()
        .map(|bridge| bridge.id.display_ref())
        .collect::<HashSet<_>>();
    let activation_subjects = composition
        .activation
        .iter()
        .filter(|record| record.scope == ActivationScope::Bridge && known.contains(&record.subject))
        .map(|record| record.subject.clone())
        .collect();
    BridgeInventory::reopen(graphforge_ontology::BridgeSnapshot {
        schema_version: 1,
        generation: 0,
        profile_default: composition.profile_default,
        activation_subjects,
        modules,
        adopted,
        receipts: Vec::new(),
    })
    .map_err(composition_error)
}

fn enrich_module_delete_preview(
    mut preview: graphforge_ontology::DeletePreview,
    composition: &WorkspaceOntologyComposition,
) -> Result<graphforge_ontology::DeletePreview, GfError> {
    preview.bridge_refs = composition
        .bridges
        .iter()
        .filter(|bridge| bridge_references_module(bridge, &preview.target))
        .map(bridge_id)
        .collect::<Result<Vec<_>, _>>()?;
    preview.bridge_refs.sort_by_key(BridgeSetId::sort_key);
    preview.bridge_refs.dedup();
    preview.safe = preview.safe && preview.bridge_refs.is_empty();
    Ok(preview)
}

fn bridge_references_module(bridge: &BridgeDocument, module: &OntologyModuleId) -> bool {
    bridge.source_modules.iter().any(|id| id == module)
        || bridge.target_modules.iter().any(|id| id == module)
        || bridge.assertions.iter().any(|assertion| {
            assertion.source.module == *module || assertion.target.module == *module
        })
}

fn compile_candidate(
    candidate: &WorkspaceOntologyComposition,
) -> Result<graphforge_ontology::CompiledComposition, GfError> {
    let modules = candidate
        .modules
        .iter()
        .map(|m| graphforge_ontology::AuthoredModule {
            id: m.id.clone(),
            dependencies: m.dependencies.clone(),
            doc: m.document.clone(),
            allow_projected_identity: m.allow_projected_identity,
        })
        .collect::<Vec<_>>();
    let bridges = candidate
        .bridges
        .iter()
        .map(bridge_id)
        .collect::<Result<Vec<_>, _>>()?;
    graphforge_ontology::compile_inventory(graphforge_ontology::InventoryCompileRequest {
        modules: &modules,
        bridges: &bridges,
        activation: &candidate.activation,
        profile_default: candidate.profile_default,
        limits: CompositionLimits::default(),
        cancelled: None,
    })
    .map_err(composition_error)
}

fn empty_composition() -> Result<WorkspaceOntologyComposition, GfError> {
    let compiled =
        graphforge_ontology::compile_inventory(graphforge_ontology::InventoryCompileRequest {
            modules: &[],
            bridges: &[],
            activation: &[],
            profile_default: ActivationMode::Exploratory,
            limits: CompositionLimits::default(),
            cancelled: None,
        })
        .map_err(composition_error)?;
    Ok(WorkspaceOntologyComposition::from_compiled(
        &compiled,
        Vec::new(),
    ))
}

fn composition_from_generation(
    generation: &graphforge_storage::ResolvedProjectGeneration,
) -> Result<Option<WorkspaceOntologyComposition>, GfError> {
    if let Some(snapshot) = generation.participant_snapshot(
        graphforge_storage::WORKSPACE_CAPABILITY_ID,
        graphforge_storage::WORKSPACE_ONTOLOGY_COMPOSITION_FAMILY,
    )? {
        return Ok(Some(WorkspaceOntologyComposition::from_canonical_json(
            &snapshot.bytes,
        )?));
    }
    let ontology = generation
        .participant_snapshot(
            graphforge_storage::WORKSPACE_CAPABILITY_ID,
            graphforge_storage::WORKSPACE_ONTOLOGY_FAMILY,
        )?
        .ok_or_else(|| GfError::Validation("workspace ontology authority is missing"))?;
    let ontology = graphforge_storage::WorkspaceOntology::from_canonical_json(&ontology.bytes)?;
    Ok(WorkspaceOntologyComposition::virtual_legacy(&ontology)?)
}

fn bridge_id(document: &BridgeDocument) -> Result<BridgeSetId, GfError> {
    Ok(BridgeSetId {
        bridge_id: document.bridge_id.clone(),
        authored_version: document.authored_version.clone(),
        canonical_digest: graphforge_ontology::bridge_document_digest(document)
            .map_err(GfError::Validation)?,
    })
}

fn migration_generation_uuid(
    request: &ModuleMigrationRequest,
    preview: &ModuleMigrationPreview,
) -> Uuid {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(b"graphforge-retained-data-migration-generation/1");
    hasher.update(request.authority.context.operation_uuid.0.as_bytes());
    hasher.update(preview.plan.plan_digest.as_bytes());
    hasher.update(preview.next_module.display_ref().as_bytes());
    graphforge_core::canonical::uuid_v8(hasher.finalize().into())
}

fn rewrite_bridge_symbols_for_migration(
    candidate: &mut WorkspaceOntologyComposition,
    previous: &OntologyModuleId,
    next: &OntologyModuleId,
    document: &OntologyDoc,
) -> Result<(), GfError> {
    let steps = graphforge_ontology::MigrationEngine::plan(
        &previous.authored_version,
        &next.authored_version,
        &document.migrations,
    )
    .map_err(|error| GfError::Validation(error.to_string()))?;
    for bridge in &mut candidate.bridges {
        for assertion in &mut bridge.assertions {
            for symbol in [&mut assertion.source, &mut assertion.target] {
                if symbol.module != *previous {
                    continue;
                }
                for step in &steps {
                    match &step.transform_kind {
                        graphforge_ontology::TransformKind::RenameType { old_name, new_name }
                            if symbol.kind == SymbolKind::Entity
                                && symbol.local_id == *old_name =>
                        {
                            symbol.local_id.clone_from(new_name);
                        }
                        graphforge_ontology::TransformKind::RenameProperty {
                            owner,
                            old_name,
                            new_name,
                        } if symbol.kind == SymbolKind::Property
                            && symbol.local_id == format!("{owner}:{old_name}") =>
                        {
                            symbol.local_id = format!("{owner}:{new_name}");
                        }
                        graphforge_ontology::TransformKind::RenameType { old_name, new_name }
                            if symbol.kind == SymbolKind::Property
                                && symbol.local_id.starts_with(&format!("{old_name}:")) =>
                        {
                            symbol.local_id = format!(
                                "{new_name}:{}",
                                symbol.local_id.split_once(':').map_or("", |(_, name)| name)
                            );
                        }
                        _ => {}
                    }
                }
            }
        }
    }
    Ok(())
}

fn composition_error(error: graphforge_ontology::CompositionError) -> GfError {
    let diagnostics = project_diagnostics(error);
    let message = diagnostics.first().map_or_else(
        || "ontology composition failed".into(),
        |diagnostic| diagnostic.message.clone(),
    );
    MultiOntologyError {
        code: "GF_ONTOLOGY_DIAGNOSTIC".into(),
        message,
        diagnostics,
    }
}

fn validation_receipt(
    result: Result<(), graphforge_ontology::CompositionError>,
) -> MultiOntologyValidationReceipt {
    match result {
        Ok(()) => MultiOntologyValidationReceipt {
            valid: true,
            diagnostics: Vec::new(),
        },
        Err(error) => MultiOntologyValidationReceipt {
            valid: false,
            diagnostics: project_diagnostics(error),
        },
    }
}

fn project_diagnostics(
    error: graphforge_ontology::CompositionError,
) -> Vec<MultiOntologyDiagnostic> {
    let mut diagnostics = error
        .diagnostics
        .into_iter()
        .take(MAX_ERROR_DIAGNOSTICS)
        .map(|diagnostic| MultiOntologyDiagnostic {
            code: diagnostic.code.as_str().into(),
            phase: diagnostic.phase.as_str().into(),
            message: bounded_text(&diagnostic.message),
            subjects: bounded_items(diagnostic.subjects, diagnostic.limit),
            candidates: bounded_items(diagnostic.candidates, diagnostic.limit),
            remediation: remediation_for(diagnostic.code).into(),
            limit: diagnostic.limit.clamp(1, MAX_ERROR_DIAGNOSTICS),
        })
        .collect::<Vec<_>>();
    diagnostics.sort_by(|left, right| {
        (&left.code, &left.subjects, &left.candidates).cmp(&(
            &right.code,
            &right.subjects,
            &right.candidates,
        ))
    });
    diagnostics
}

fn composition_change_error(
    diagnostics: Vec<crate::CompositionChangeDiagnostic>,
) -> MultiOntologyError {
    let diagnostics = diagnostics
        .into_iter()
        .take(MAX_ERROR_DIAGNOSTICS)
        .map(|diagnostic| MultiOntologyDiagnostic {
            code: diagnostic.code,
            phase: "preflight".into(),
            message: "composition preflight contains unresolved diagnostics".into(),
            subjects: bounded_items(vec![diagnostic.subject], MAX_ERROR_DIAGNOSTICS),
            candidates: Vec::new(),
            remediation: bounded_text(&diagnostic.remediation),
            limit: MAX_ERROR_DIAGNOSTICS,
        })
        .collect();
    MultiOntologyError {
        code: "GF_ONTOLOGY_DIAGNOSTIC".into(),
        message: "composition preflight contains unresolved diagnostics".into(),
        diagnostics,
    }
}

fn dependency_blocked_error(preview: &graphforge_ontology::DeletePreview) -> GfError {
    let mut subjects = preview
        .dependent_modules
        .iter()
        .map(OntologyModuleId::display_ref)
        .chain(preview.activation_refs.iter().cloned())
        .chain(preview.bridge_refs.iter().map(BridgeSetId::display_ref))
        .collect::<Vec<_>>();
    subjects.push(preview.target.display_ref());
    MultiOntologyError {
        code: "GF_ONTOLOGY_DIAGNOSTIC".into(),
        message: "module deletion is dependency-blocked".into(),
        diagnostics: vec![MultiOntologyDiagnostic {
            code: "dependency.in_use".into(),
            phase: "inventory".into(),
            message: "module deletion is dependency-blocked".into(),
            subjects: bounded_items(subjects, MAX_ERROR_DIAGNOSTICS),
            candidates: Vec::new(),
            remediation: "remove exact module, bridge, and activation references first".into(),
            limit: MAX_ERROR_DIAGNOSTICS,
        }],
    }
}

fn portable_error(error: graphforge_storage::PortableV2Error) -> GfError {
    use graphforge_storage::PortableV2ErrorCode;
    let (outer, diagnostic, remediation) = match error.code {
        PortableV2ErrorCode::Cancelled => (
            "GF_CANCELLED",
            "lifecycle.cancelled",
            "retry with an active cancellation token",
        ),
        PortableV2ErrorCode::UnsupportedFuture => (
            "GF_UNSUPPORTED_FUTURE",
            "interchange.unsupported_future",
            "upgrade GraphForge or use a supported portable-v2 producer",
        ),
        PortableV2ErrorCode::LimitExceeded => (
            "GF_LIMIT_EXCEEDED",
            "resource.bytes",
            "raise an explicit bounded limit or reduce the package",
        ),
        PortableV2ErrorCode::ConcurrentMutation => (
            "GF_IDEMPOTENCY_CONFLICT",
            "inventory.generation_conflict",
            "refresh the exact authority state and retry",
        ),
        PortableV2ErrorCode::Incompatible => (
            "GF_VALIDATION",
            "interchange.selection",
            "select a compatible portable-v2 ontology candidate",
        ),
        PortableV2ErrorCode::Io | PortableV2ErrorCode::InvalidPath => (
            "GF_STORAGE",
            "interchange.io",
            "verify the portable staging authority and retry",
        ),
        PortableV2ErrorCode::DigestMismatch
        | PortableV2ErrorCode::DuplicateEntry
        | PortableV2ErrorCode::InvalidStructure => (
            "GF_VALIDATION",
            "interchange.integrity",
            "repair and re-import the portable-v2 package",
        ),
    };
    let subjects = error.entry.into_iter().collect::<Vec<_>>();
    MultiOntologyError {
        code: outer.into(),
        message: "portable-v2 ontology staging failed".into(),
        diagnostics: vec![MultiOntologyDiagnostic {
            code: diagnostic.into(),
            phase: "interchange".into(),
            message: "portable-v2 ontology staging failed".into(),
            subjects: bounded_items(subjects, MAX_ERROR_DIAGNOSTICS),
            candidates: Vec::new(),
            remediation: remediation.into(),
            limit: MAX_ERROR_DIAGNOSTICS,
        }],
    }
}

fn bounded_text(value: &str) -> String {
    value.chars().take(MAX_ERROR_TEXT_BYTES).collect()
}

fn bounded_items(mut values: Vec<String>, requested_limit: usize) -> Vec<String> {
    values.sort();
    values.dedup();
    values.truncate(requested_limit.clamp(1, MAX_ERROR_DIAGNOSTICS));
    values
        .into_iter()
        .map(|value| bounded_text(&value))
        .collect()
}

fn remediation_for(code: graphforge_ontology::DiagnosticCode) -> &'static str {
    use graphforge_ontology::DiagnosticCode;
    match code {
        DiagnosticCode::InventoryDuplicate => "select or author one unique exact identity",
        DiagnosticCode::InventoryNotFound => "select an existing exact inventory identity",
        DiagnosticCode::InventoryMalformed => "repair and validate the authored document",
        DiagnosticCode::InventoryGenerationConflict => "refresh the exact authority state",
        DiagnosticCode::DependencyMissing => "adopt every exact dependency first",
        DiagnosticCode::DependencyCycle => "remove the dependency cycle",
        DiagnosticCode::DependencyInUse => "remove dependent authority references first",
        DiagnosticCode::CollisionQualifiedDuplicate => "remove the conflicting qualified symbol",
        DiagnosticCode::ResourceModules
        | DiagnosticCode::ResourceBridges
        | DiagnosticCode::ResourceSymbols
        | DiagnosticCode::ResourceDiagnostics => "reduce the request below the registered limit",
        DiagnosticCode::LifecycleCancelled => "retry with an active cancellation token",
        DiagnosticCode::LifecycleInvalidTransition => "perform the required prior lifecycle step",
        DiagnosticCode::ResolutionAmbiguous => "supply an exact qualified selector",
        DiagnosticCode::ResolutionNotFound | DiagnosticCode::ResolutionKindMismatch => {
            "select a declared symbol with the correct kind"
        }
        DiagnosticCode::InterchangeIntegrity => "repair or regenerate the authenticated content",
        DiagnosticCode::CollisionMetadata => "retain the required stable identity metadata",
        DiagnosticCode::BridgeEndpointMissing => "adopt every exact bridge endpoint module",
        DiagnosticCode::BridgeContradiction => "remove contradictory bridge assertions",
        DiagnosticCode::BridgeProvenanceMissing => "add bounded authored provenance",
    }
}

fn staged_module_document(text: &str, format: ImportFormatHint) -> Result<OntologyDoc, GfError> {
    match format {
        ImportFormatHint::Json => graphforge_ontology::OntologyLoader::load_json(text.as_bytes()),
        ImportFormatHint::Auto if text.trim_start().starts_with('{') => {
            graphforge_ontology::OntologyLoader::load_json(text.as_bytes())
        }
        ImportFormatHint::Yaml | ImportFormatHint::Auto => {
            graphforge_ontology::OntologyLoader::load_yaml(text.as_bytes())
        }
    }
    .map_err(|_| GfError::Validation("ontology module import is malformed"))
}

fn reject_duplicate_module(
    candidate: &WorkspaceOntologyComposition,
    id: &OntologyModuleId,
) -> Result<(), GfError> {
    if candidate.modules.iter().any(|m| m.id == *id) {
        Err(GfError::Validation("module identity already adopted"))
    } else {
        Ok(())
    }
}
fn reject_duplicate_bridge(
    candidate: &WorkspaceOntologyComposition,
    id: &BridgeSetId,
) -> Result<(), GfError> {
    if candidate
        .bridges
        .iter()
        .filter_map(|document| bridge_id(document).ok())
        .any(|value| value == *id)
    {
        Err(GfError::Validation("bridge identity already adopted"))
    } else {
        Ok(())
    }
}
fn resolve_module_index(
    candidate: &WorkspaceOntologyComposition,
    selector: &ModuleSelector,
) -> Result<usize, GfError> {
    let matches = candidate
        .modules
        .iter()
        .enumerate()
        .filter(|(_, m)| match selector {
            ModuleSelector::Exact(id) => m.id == *id,
            ModuleSelector::OntologyId(id) => m.id.ontology_id == *id,
        })
        .map(|(i, _)| i)
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [index] => Ok(*index),
        [] => Err(GfError::Validation("module not found")),
        _ => Err(GfError::Validation("module selector is ambiguous")),
    }
}
fn resolve_bridge_index(
    candidate: &WorkspaceOntologyComposition,
    selector: &BridgeSelector,
) -> Result<usize, GfError> {
    let matches = candidate
        .bridges
        .iter()
        .enumerate()
        .filter(|(_, d)| match selector {
            BridgeSelector::Exact(id) => bridge_id(d).is_ok_and(|value| value == *id),
            BridgeSelector::BridgeId(id) => d.bridge_id == *id,
        })
        .map(|(i, _)| i)
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [index] => Ok(*index),
        [] => Err(GfError::Validation("bridge not found")),
        _ => Err(GfError::Validation("bridge selector is ambiguous")),
    }
}
fn rewrite_module_identity(
    candidate: &mut WorkspaceOntologyComposition,
    prior: &OntologyModuleId,
    next: &OntologyModuleId,
) -> Result<(), GfError> {
    let prior_bridge_ids = candidate
        .bridges
        .iter()
        .map(bridge_id)
        .collect::<Result<Vec<_>, _>>()?;
    for module in &mut candidate.modules {
        for dep in &mut module.dependencies {
            if dep == prior {
                *dep = next.clone();
            }
        }
    }
    for bridge in &mut candidate.bridges {
        for id in bridge
            .source_modules
            .iter_mut()
            .chain(bridge.target_modules.iter_mut())
        {
            if id == prior {
                *id = next.clone();
            }
        }
        for assertion in &mut bridge.assertions {
            if assertion.source.module == *prior {
                assertion.source.module = next.clone();
            }
            if assertion.target.module == *prior {
                assertion.target.module = next.clone();
            }
        }
    }
    for record in &mut candidate.activation {
        if record.subject == prior.display_ref() {
            record.subject = next.display_ref();
        }
    }
    cascade_bridge_identities(candidate, prior_bridge_ids)
}

fn cascade_bridge_identities(
    candidate: &mut WorkspaceOntologyComposition,
    mut prior_ids: Vec<BridgeSetId>,
) -> Result<(), GfError> {
    for _ in 0..=candidate.bridges.len() {
        let current_ids = candidate
            .bridges
            .iter()
            .map(bridge_id)
            .collect::<Result<Vec<_>, _>>()?;
        let changes = prior_ids
            .iter()
            .zip(&current_ids)
            .filter(|(prior, current)| prior != current)
            .map(|(prior, current)| (prior.clone(), current.clone()))
            .collect::<Vec<_>>();
        if changes.is_empty() {
            return Ok(());
        }
        for bridge in &mut candidate.bridges {
            for dependency in &mut bridge.dependencies {
                if let Some((_, replacement)) =
                    changes.iter().find(|(prior, _)| dependency == prior)
                {
                    *dependency = replacement.clone();
                }
            }
        }
        for record in &mut candidate.activation {
            if let Some((_, replacement)) = changes
                .iter()
                .find(|(prior, _)| record.subject == prior.display_ref())
            {
                record.subject = replacement.display_ref();
            }
        }
        prior_ids = current_ids;
    }
    Err(GfError::Validation(
        "bridge identity cascade did not converge",
    ))
}
fn set_module_activation(
    candidate: &mut WorkspaceOntologyComposition,
    id: &OntologyModuleId,
    mode: Option<ActivationMode>,
) {
    candidate
        .activation
        .retain(|r| !(r.scope == ActivationScope::Module && r.subject == id.display_ref()));
    if let Some(mode) = mode {
        candidate.activation.push(ActivationRecord {
            scope: ActivationScope::Module,
            subject: id.display_ref(),
            mode,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OperationId;
    use graphforge_ontology::{DiagnosticCode, DiagnosticLimit, EntityTypeDef};
    use sha2::{Digest, Sha256};
    use std::sync::atomic::AtomicBool;

    fn document(ontology_id: &str, entity: &str) -> OntologyDoc {
        OntologyDoc {
            ontology_id: ontology_id.into(),
            version: "1".into(),
            entity_types: vec![EntityTypeDef {
                name: entity.into(),
                r#abstract: false,
                parent: None,
            }],
            relation_types: Vec::new(),
            properties: Vec::new(),
            constraints: Vec::new(),
            migrations: Vec::new(),
        }
    }

    fn expectation(graph: &GraphForge, seed: u128) -> OntologyAuthorityExpectation {
        let state = graph.ontology_authority_state().unwrap();
        OntologyAuthorityExpectation {
            context: WriteContext {
                operation_uuid: OperationId(Uuid::from_u128(seed)),
                actor_uuid: None,
            },
            expected_project_generation_uuid: state.project_generation_uuid,
            expected_composition_fingerprint: state.composition_fingerprint,
        }
    }

    fn parity_fixture() -> serde_json::Value {
        serde_json::from_str(include_str!(
            "../../../tests/fixtures/multi-ontology-v1/binding-parity-v1.json"
        ))
        .unwrap()
    }

    fn sha256_hex(bytes: &[u8]) -> String {
        use std::fmt::Write as _;
        let digest = Sha256::digest(bytes);
        let mut encoded = String::with_capacity(digest.len() * 2);
        for byte in digest {
            write!(&mut encoded, "{byte:02x}").unwrap();
        }
        encoded
    }

    fn future_portable_package() -> tempfile::TempDir {
        const MANIFEST_PATH: &str = "data/graphforge-project.json";
        const PAYLOAD_PATH: &str = "data/components/ontology/core-ontology/ontology.json";
        const BAGIT: &[u8] = b"BagIt-Version: 1.0\nTag-File-Character-Encoding: UTF-8\n";
        const BAG_INFO: &[u8] =
            b"Bag-Software-Agent: GraphForge portable-v2\nBagging-Date: 1970-01-01\n";

        let root = tempfile::tempdir().unwrap();
        let mut manifest: serde_json::Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/portable-v2/ontology-only.manifest.json"
        ))
        .unwrap();
        manifest["package_class"] = "future-package-class".into();
        manifest.as_object_mut().unwrap().remove("package_digest");
        let semantic = serde_json::to_vec(&manifest).unwrap();
        manifest["package_digest"] = format!(
            "sha256:{}",
            sha256_hex(&[b"graphforge-project/2\0".as_slice(), semantic.as_slice()].concat())
        )
        .into();
        let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
        let manifest_path = root.path().join(MANIFEST_PATH);
        std::fs::create_dir_all(manifest_path.parent().unwrap()).unwrap();
        std::fs::write(&manifest_path, &manifest_bytes).unwrap();
        let payload = root.path().join(PAYLOAD_PATH);
        std::fs::create_dir_all(payload.parent().unwrap()).unwrap();
        std::fs::write(payload, b"{}").unwrap();
        std::fs::write(root.path().join("bagit.txt"), BAGIT).unwrap();
        std::fs::write(root.path().join("bag-info.txt"), BAG_INFO).unwrap();
        let data_manifest = format!(
            "{}  {PAYLOAD_PATH}\n{}  {MANIFEST_PATH}\n",
            sha256_hex(b"{}"),
            sha256_hex(&manifest_bytes)
        );
        std::fs::write(root.path().join("manifest-sha256.txt"), &data_manifest).unwrap();
        let tag_manifest = format!(
            "{}  bag-info.txt\n{}  bagit.txt\n{}  manifest-sha256.txt\n",
            sha256_hex(BAG_INFO),
            sha256_hex(BAGIT),
            sha256_hex(data_manifest.as_bytes())
        );
        std::fs::write(root.path().join("tagmanifest-sha256.txt"), tag_manifest).unwrap();
        root
    }

    fn observed_unsupported_future_error() -> MultiOntologyError {
        let package = future_portable_package();
        let error = crate::verify_portable_v2(
            &crate::PortableVerifyRequest {
                input: package.path().to_path_buf(),
                mode: graphforge_storage::PortableV2Mode::Full,
                limits: graphforge_storage::PortableV2Limits::default(),
            },
            None,
        )
        .unwrap_err();
        MultiOntologyError::from(error)
    }

    fn parity_document(name: &str) -> OntologyDoc {
        serde_json::from_value(parity_fixture()["modules"][name].clone()).unwrap()
    }

    fn adopt_parity_module(
        graph: &mut GraphForge,
        name: &str,
        dependencies: Vec<OntologyModuleId>,
        seed: u128,
    ) -> ModuleCandidate {
        let candidate = graph
            .create_ontology_module(parity_document(name), dependencies, None)
            .unwrap();
        graph
            .adopt_ontology_module(
                &ModuleAdoptionRequest {
                    authority: expectation(graph, seed),
                    candidate: candidate.clone(),
                },
                None,
            )
            .unwrap();
        candidate
    }

    fn parity_bridge(base: &OntologyModuleId, dependent: &OntologyModuleId) -> BridgeDocument {
        let mut encoded = serde_json::to_string(&parity_fixture()["bridge"]).unwrap();
        encoded = encoded.replace("\"$base\"", &serde_json::to_string(base).unwrap());
        encoded = encoded.replace("\"$dependent\"", &serde_json::to_string(dependent).unwrap());
        serde_json::from_str(&encoded).unwrap()
    }

    fn adopt_parity_pair(graph: &mut GraphForge, seed: u128) -> (ModuleCandidate, ModuleCandidate) {
        let base = adopt_parity_module(graph, "base", Vec::new(), seed);
        let dependent = adopt_parity_module(graph, "dependent", vec![base.id.clone()], seed + 1);
        (base, dependent)
    }

    /// Executable Rust-side inventory for every operation in the four-surface contract.
    #[test]
    fn four_surface_operation_conformance() {
        // Contract authority: tests/contracts/multi-ontology-surface-v1.json
        let contract: serde_json::Value = serde_json::from_str(include_str!(
            "../../../tests/contracts/multi-ontology-surface-v1.json"
        ))
        .expect("four-surface contract must parse");
        assert_eq!(
            contract["contract"],
            "graphforge-multi-ontology-four-surface/1"
        );
        let operations = contract["operations"]
            .as_array()
            .expect("operations must be an array");
        assert_eq!(operations.len(), 35);
        let actual = operations
            .iter()
            .map(|operation| {
                (
                    operation["id"].as_str().expect("operation id"),
                    operation["rust"].as_str().expect("Rust member"),
                )
            })
            .collect::<Vec<_>>();
        let expected = [
            ("module.list", "GraphForge.ontology_modules"),
            ("module.get", "GraphForge.inspect_ontology_module[exact]"),
            (
                "module.inspect",
                "GraphForge.inspect_ontology_module[selector]",
            ),
            ("module.validate", "GraphForge.validate_ontology_module"),
            (
                "module.create_register",
                "GraphForge.create_ontology_module",
            ),
            ("module.import", "GraphForge.import_ontology_module"),
            ("module.adopt", "GraphForge.adopt_ontology_module"),
            (
                "module.preview_update",
                "GraphForge.preview_update_ontology_module",
            ),
            ("module.update_replace", "GraphForge.update_ontology_module"),
            (
                "module.preview_delete",
                "GraphForge.preview_delete_ontology_module",
            ),
            ("module.delete", "GraphForge.delete_ontology_module"),
            ("module.export", "GraphForge.export_ontology_module"),
            ("bridge.list", "GraphForge.ontology_bridges"),
            ("bridge.get", "GraphForge.inspect_ontology_bridge[exact]"),
            (
                "bridge.inspect",
                "GraphForge.inspect_ontology_bridge[selector]",
            ),
            ("bridge.validate", "GraphForge.validate_ontology_bridge"),
            (
                "bridge.create_register",
                "GraphForge.create_ontology_bridge",
            ),
            ("bridge.import", "GraphForge.import_ontology_bridge"),
            ("bridge.adopt", "GraphForge.adopt_ontology_bridge"),
            (
                "bridge.preview_update",
                "GraphForge.preview_update_ontology_bridge",
            ),
            ("bridge.update_replace", "GraphForge.update_ontology_bridge"),
            (
                "bridge.preview_delete",
                "GraphForge.preview_delete_ontology_bridge",
            ),
            ("bridge.delete", "GraphForge.delete_ontology_bridge"),
            ("bridge.export", "GraphForge.export_ontology_bridge"),
            (
                "activation.inspect",
                "GraphForge.ontology_activation_profile",
            ),
            (
                "activation.change",
                "GraphForge.change_ontology_activation_profile",
            ),
            (
                "composition.validate",
                "GraphForge.validate_ontology_composition",
            ),
            (
                "composition.preflight",
                "GraphForge.preflight_ontology_composition",
            ),
            (
                "composition.resolution_explain",
                "GraphForge.explain_ontology_resolution",
            ),
            (
                "portable.inspect",
                "crate.verify_portable_v2[structure_only]",
            ),
            ("portable.verify", "crate.verify_portable_v2[full]"),
            ("portable.export", "GraphForge.export_portable_v2"),
            ("portable.import", "GraphForge.import_portable_v2"),
            (
                "portable.post_import_inspect",
                "GraphForge.portable_ontology_staging",
            ),
            (
                "portable.post_import_adopt",
                "GraphForge.adopt_portable_ontology_staging",
            ),
        ];
        assert_eq!(actual, expected);
    }

    #[test]
    fn portable_error_conversion_is_complete_and_stable() {
        use graphforge_storage::{PortableV2Error, PortableV2ErrorCode};
        let cases = [
            (
                PortableV2ErrorCode::Cancelled,
                "GF_CANCELLED",
                "lifecycle.cancelled",
            ),
            (
                PortableV2ErrorCode::LimitExceeded,
                "GF_LIMIT_EXCEEDED",
                "resource.bytes",
            ),
            (PortableV2ErrorCode::Io, "GF_STORAGE", "interchange.io"),
            (
                PortableV2ErrorCode::InvalidStructure,
                "GF_VALIDATION",
                "interchange.integrity",
            ),
            (
                PortableV2ErrorCode::InvalidPath,
                "GF_STORAGE",
                "interchange.io",
            ),
            (
                PortableV2ErrorCode::DuplicateEntry,
                "GF_VALIDATION",
                "interchange.integrity",
            ),
            (
                PortableV2ErrorCode::UnsupportedFuture,
                "GF_UNSUPPORTED_FUTURE",
                "interchange.unsupported_future",
            ),
            (
                PortableV2ErrorCode::Incompatible,
                "GF_VALIDATION",
                "interchange.selection",
            ),
            (
                PortableV2ErrorCode::DigestMismatch,
                "GF_VALIDATION",
                "interchange.integrity",
            ),
            (
                PortableV2ErrorCode::ConcurrentMutation,
                "GF_IDEMPOTENCY_CONFLICT",
                "inventory.generation_conflict",
            ),
        ];
        for (source, outer, diagnostic) in cases {
            let error = MultiOntologyError::from(PortableV2Error::new(source, "test"));
            assert_eq!(error.code, outer);
            assert_eq!(error.diagnostics[0].code, diagnostic);
        }
    }

    #[test]
    fn conformance_positive_crud_import_export() {
        let mut graph = GraphForge::new(None).unwrap();
        let base = graph
            .create_ontology_module(parity_document("base"), Vec::new(), None)
            .unwrap();
        let imported = graph
            .import_ontology_module(
                &serde_json::to_string(&parity_document("dependent")).unwrap(),
                ImportFormatHint::Json,
                vec![base.id.clone()],
            )
            .unwrap();
        graph
            .adopt_ontology_module(
                &ModuleAdoptionRequest {
                    authority: expectation(&graph, 9_001),
                    candidate: base.clone(),
                },
                None,
            )
            .unwrap();
        graph
            .adopt_ontology_module(
                &ModuleAdoptionRequest {
                    authority: expectation(&graph, 9_002),
                    candidate: imported.clone(),
                },
                None,
            )
            .unwrap();
        assert_eq!(graph.ontology_modules().unwrap().len(), 2);
        assert!(
            graph
                .export_ontology_module(&ModuleSelector::Exact(base.id.clone()), ExportFormat::Json)
                .unwrap()
                .contains("Person")
        );
        let bridge = graph
            .create_ontology_bridge(parity_bridge(&base.id, &imported.id))
            .unwrap();
        graph
            .adopt_ontology_bridge(
                &BridgeAdoptionRequest {
                    authority: expectation(&graph, 9_003),
                    candidate: bridge.clone(),
                },
                None,
            )
            .unwrap();
        assert_eq!(
            graph
                .inspect_ontology_bridge(&BridgeSelector::Exact(bridge.id.clone()))
                .unwrap()
                .entry
                .id,
            bridge.id
        );
        let exported = graph
            .export_ontology_bridge(&BridgeSelector::Exact(bridge.id), BridgeExportFormat::Json)
            .unwrap();
        assert!(exported.contains("urn:graphforge:parity:bridge")); // export_ontology_bridge
    }

    #[test]
    fn conformance_exact_identity_and_ambiguity() {
        let mut inventory = OntologyInventory::default();
        let first = inventory
            .create_register(parity_document("dependent"), Vec::new(), None, "first")
            .unwrap();
        let second = inventory
            .create_register(
                parity_document("dependent_update"),
                Vec::new(),
                None,
                "second",
            )
            .unwrap();
        inventory
            .adopt(&ModuleSelector::Exact(first.clone()), 0, "adopt-first")
            .unwrap();
        inventory
            .adopt(&ModuleSelector::Exact(second.clone()), 1, "adopt-second")
            .unwrap();
        assert_eq!(
            inventory
                .inspect(&ModuleSelector::Exact(first.clone()))
                .unwrap()
                .entry
                .id,
            first
        );
        let error = inventory
            .inspect(&ModuleSelector::OntologyId(second.ontology_id))
            .unwrap_err();
        assert_eq!(error.code(), Some(DiagnosticCode::ResolutionAmbiguous));
        // OntologyModuleSelector exact selection succeeds; SelectorAmbiguous fails closed.
    }

    #[test]
    fn conformance_dependency_blocked_deletion() {
        let mut graph = GraphForge::new(None).unwrap();
        let (base, dependent) = adopt_parity_pair(&mut graph, 9_020);
        let bridge = graph
            .create_ontology_bridge(parity_bridge(&base.id, &dependent.id))
            .unwrap();
        graph
            .adopt_ontology_bridge(
                &BridgeAdoptionRequest {
                    authority: expectation(&graph, 9_022),
                    candidate: bridge.clone(),
                },
                None,
            )
            .unwrap();
        let preview = graph
            .preview_delete_ontology_module(&ModuleSelector::Exact(base.id.clone()))
            .unwrap();
        assert!(!preview.safe);
        assert_eq!(preview.dependent_modules, vec![dependent.id]);
        assert_eq!(preview.bridge_refs, vec![bridge.id]);
        let error = graph
            .delete_ontology_module(
                &ModuleDeleteRequest {
                    authority: expectation(&graph, 9_023),
                    selector: ModuleSelector::Exact(base.id),
                },
                None,
            )
            .unwrap_err();
        assert_eq!(error.diagnostics[0].code, "dependency.in_use"); // DependencyBlocked
    }

    #[test]
    fn conformance_unsupported_future_portability() {
        let error = observed_unsupported_future_error();
        assert_eq!(error.code, "GF_UNSUPPORTED_FUTURE");
        assert_eq!(error.diagnostics[0].code, "interchange.unsupported_future");
        let cancelled = AtomicBool::new(false);
        let missing = crate::verify_portable_v2(
            &crate::PortableVerifyRequest {
                input: std::path::PathBuf::from("missing-portable-v2"),
                mode: graphforge_storage::PortableV2Mode::StructureOnly,
                limits: graphforge_storage::PortableV2Limits::default(),
            },
            Some(&cancelled),
        )
        .unwrap_err();
        assert_ne!(
            missing.code,
            graphforge_storage::PortableV2ErrorCode::UnsupportedFuture
        ); // UnsupportedFutureVersion mapping is distinct.
    }

    #[test]
    fn conformance_cancellation() {
        let mut graph = GraphForge::new(None).unwrap();
        let candidate = graph
            .create_ontology_module(parity_document("base"), Vec::new(), None)
            .unwrap();
        let before = graph.ontology_authority_state().unwrap();
        let token = CancellationToken::new();
        token.cancel();
        let error = graph
            .adopt_ontology_module(
                &ModuleAdoptionRequest {
                    authority: expectation(&graph, 9_030),
                    candidate,
                },
                Some(&token),
            )
            .unwrap_err();
        assert_eq!(error.code(), "GF_CANCELLED");
        assert_eq!(graph.ontology_authority_state().unwrap(), before);
    }

    #[test]
    fn conformance_idempotent_replay() {
        let mut graph = GraphForge::new(None).unwrap();
        let candidate = graph
            .create_ontology_module(parity_document("base"), Vec::new(), None)
            .unwrap();
        let request = ModuleAdoptionRequest {
            authority: expectation(&graph, 9_040),
            candidate,
        };
        let first = graph.adopt_ontology_module(&request, None).unwrap();
        let replay = graph.adopt_ontology_module(&request, None).unwrap();
        assert_eq!(first, replay); // IdempotentReplay uses the same operation_uuid.
        let mut actor = request.clone();
        actor.authority.context.actor_uuid = Some(Uuid::from_u128(77));
        assert_eq!(
            graph
                .adopt_ontology_module(&actor, None)
                .unwrap_err()
                .code(),
            "GF_IDEMPOTENCY_CONFLICT"
        );
        let cross_op = ActivationProfileChangeRequest {
            authority: request.authority,
            profile_default: ActivationMode::Strict,
            activation: Vec::new(),
        };
        assert_eq!(
            graph
                .change_ontology_activation_profile(&cross_op, None)
                .unwrap_err()
                .code(),
            "GF_IDEMPOTENCY_CONFLICT"
        );
    }

    #[test]
    fn durable_reopen_reconstructs_candidate_before_exact_replay() {
        let project = tempfile::tempdir().unwrap();
        let path = project.path().to_str().unwrap();
        let mut first = GraphForge::new(Some(path)).unwrap();
        let candidate = first
            .create_ontology_module(parity_document("base"), Vec::new(), None)
            .unwrap();
        let request = ModuleAdoptionRequest {
            authority: expectation(&first, 9_045),
            candidate,
        };
        let receipt = first.adopt_ontology_module(&request, None).unwrap();
        drop(first);

        let mut reopened = GraphForge::new(Some(path)).unwrap();
        let reconstructed = reopened
            .create_ontology_module(parity_document("base"), Vec::new(), None)
            .unwrap();
        let replay = reopened
            .adopt_ontology_module(
                &ModuleAdoptionRequest {
                    authority: request.authority,
                    candidate: reconstructed,
                },
                None,
            )
            .unwrap();
        assert_eq!(replay, receipt);
    }

    #[test]
    fn version_only_module_update_preserves_semantic_storage_bindings() {
        let mut graph = GraphForge::new(None).unwrap();
        let (base, dependent) = adopt_parity_pair(&mut graph, 9_046);
        let receipt = graph
            .update_ontology_module(
                &ModuleUpdateRequest {
                    authority: expectation(&graph, 9_048),
                    selector: ModuleSelector::Exact(dependent.id),
                    document: parity_document("dependent_update"),
                    dependencies: vec![base.id],
                    enforcement: None,
                },
                None,
            )
            .unwrap();
        assert_eq!(
            graph
                .ontology_authority_state()
                .unwrap()
                .project_generation_uuid,
            receipt.project_generation_uuid
        );
    }

    #[test]
    fn conformance_no_partial_import_or_authority_change() {
        let mut graph = GraphForge::new(None).unwrap();
        let before = graph.ontology_authority_state().unwrap();
        assert!(
            graph
                .import_ontology_module("{bad", ImportFormatHint::Json, Vec::new())
                .is_err()
        );
        assert_eq!(graph.ontology_authority_state().unwrap(), before);
        assert!(
            graph
                .adopt_portable_ontology_staging(
                    &expectation(&graph, 9_050),
                    graphforge_storage::PortableV2Limits::default(),
                    None
                )
                .is_err()
        );
        assert_eq!(graph.ontology_authority_state().unwrap(), before); // NoAuthorityChange
    }

    #[test]
    fn failing_portable_v2_import_is_atomic_and_preserves_authority() {
        let project = tempfile::tempdir().unwrap();
        let path = project.path().to_str().unwrap();
        let graph = GraphForge::new(Some(path)).unwrap();
        let before = graph.ontology_authority_state().unwrap();
        drop(graph);
        let invalid = project.path().join("invalid.gfpb");
        std::fs::write(&invalid, b"not-a-portable-v2-package").unwrap();
        let error = GraphForge::import_portable_v2(
            project.path(),
            &crate::PortableV2ImportRequest {
                input: invalid,
                operation_id: OperationId(Uuid::from_u128(9_051)),
                limits: graphforge_storage::PortableV2Limits::default(),
            },
            None,
        )
        .unwrap_err();
        assert_eq!(
            error.code,
            graphforge_storage::PortableV2ErrorCode::InvalidStructure
        );
        let reopened = GraphForge::new(Some(path)).unwrap();
        assert_eq!(reopened.ontology_authority_state().unwrap(), before);
        assert!(
            reopened
                .portable_ontology_staging(graphforge_storage::PortableV2Limits::default())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn validation_receipts_are_rust_owned_and_structured() {
        let graph = GraphForge::new(None).unwrap();
        let valid = graph
            .validate_ontology_module(&parity_document("base"))
            .unwrap();
        assert!(valid.valid);
        assert!(valid.diagnostics.is_empty());
        let mut invalid = parity_document("base");
        invalid.entity_types.push(invalid.entity_types[0].clone());
        let receipt = graph.validate_ontology_module(&invalid).unwrap();
        assert!(!receipt.valid);
        assert_eq!(receipt.diagnostics[0].code, "inventory.malformed");
        assert_eq!(
            serde_json::from_str::<MultiOntologyValidationReceipt>(
                &serde_json::to_string(&receipt).unwrap()
            )
            .unwrap(),
            receipt
        );
    }

    #[test]
    fn writes_canonical_rust_semantic_parity_report_when_requested() {
        use std::collections::BTreeMap;
        let mut cases = BTreeMap::new();
        let mut graph = GraphForge::new(None).unwrap();
        let (base, dependent) = adopt_parity_pair(&mut graph, 9_100);
        let module_export = graph
            .export_ontology_module(&ModuleSelector::Exact(base.id.clone()), ExportFormat::Json)
            .unwrap();
        let bridge_doc = parity_bridge(&base.id, &dependent.id);
        let bridge = graph.create_ontology_bridge(bridge_doc.clone()).unwrap();
        graph
            .adopt_ontology_bridge(
                &BridgeAdoptionRequest {
                    authority: expectation(&graph, 9_102),
                    candidate: bridge.clone(),
                },
                None,
            )
            .unwrap();
        let bridge_export = graph
            .export_ontology_bridge(
                &BridgeSelector::Exact(bridge.id.clone()),
                BridgeExportFormat::Json,
            )
            .unwrap();
        cases.insert("positive_crud_import_export".into(), serde_json::json!({
            "module_ids": [base.id.ontology_id, dependent.id.ontology_id],
            "bridge_id": bridge.id.bridge_id,
            "module_export_match": serde_json::from_str::<serde_json::Value>(&module_export).unwrap() == serde_json::to_value(parity_document("base")).unwrap(),
            "bridge_export_match": serde_json::from_str::<serde_json::Value>(&bridge_export).unwrap() == serde_json::to_value(bridge_doc).unwrap()
        }));
        let blocked = graph
            .preview_delete_ontology_module(&ModuleSelector::Exact(base.id.clone()))
            .unwrap();
        let blocked_error = graph
            .delete_ontology_module(
                &ModuleDeleteRequest {
                    authority: expectation(&graph, 9_103),
                    selector: ModuleSelector::Exact(base.id.clone()),
                },
                None,
            )
            .unwrap_err();
        cases.insert(
            "dependency_blocked_deletion".into(),
            serde_json::json!({"safe": blocked.safe, "diagnostic_code": blocked_error.diagnostics[0].code}),
        );

        let mut inventory = OntologyInventory::default();
        let exact = inventory
            .create_register(parity_document("dependent"), Vec::new(), None, "one")
            .unwrap();
        let other = inventory
            .create_register(parity_document("dependent_update"), Vec::new(), None, "two")
            .unwrap();
        inventory
            .adopt(&ModuleSelector::Exact(exact.clone()), 0, "a")
            .unwrap();
        inventory
            .adopt(&ModuleSelector::Exact(other.clone()), 1, "b")
            .unwrap();
        let exact_match = inventory.inspect(&ModuleSelector::Exact(exact)).is_ok();
        let ambiguity = inventory
            .inspect(&ModuleSelector::OntologyId(other.ontology_id))
            .unwrap_err();
        cases.insert("exact_identity_and_ambiguity".into(), serde_json::json!({"exact_match": exact_match, "diagnostic_code": ambiguity.code().unwrap().as_str()}));

        let unsupported = observed_unsupported_future_error();
        cases.insert("unsupported_future_portability".into(), serde_json::json!({"error_code": unsupported.code, "diagnostic_code": unsupported.diagnostics[0].code}));

        let mut cancelled_graph = GraphForge::new(None).unwrap();
        let (cancel_base, _) = adopt_parity_pair(&mut cancelled_graph, 9_108);
        let cancellation_before = cancelled_graph.ontology_modules().unwrap();
        let cancel_candidate = cancelled_graph
            .create_ontology_module(
                parity_document("dependent_update"),
                vec![cancel_base.id],
                None,
            )
            .unwrap();
        let token = CancellationToken::new();
        token.cancel();
        let cancelled = cancelled_graph
            .adopt_ontology_module(
                &ModuleAdoptionRequest {
                    authority: expectation(&cancelled_graph, 9_110),
                    candidate: cancel_candidate,
                },
                Some(&token),
            )
            .unwrap_err();
        let cancellation_after = cancelled_graph.ontology_modules().unwrap();
        cases.insert(
            "cancellation".into(),
            serde_json::json!({
                "error_code": cancelled.code,
                "before_modules": cancellation_before,
                "after_modules": cancellation_after
            }),
        );

        let mut replay_graph = GraphForge::new(None).unwrap();
        let replay_candidate = replay_graph
            .create_ontology_module(parity_document("base"), Vec::new(), None)
            .unwrap();
        let replay_request = ModuleAdoptionRequest {
            authority: expectation(&replay_graph, 9_120),
            candidate: replay_candidate,
        };
        let first = replay_graph
            .adopt_ontology_module(&replay_request, None)
            .unwrap();
        let replay_receipt = replay_graph
            .adopt_ontology_module(&replay_request, None)
            .unwrap();
        let mut conflict = replay_request;
        conflict.authority.context.actor_uuid = Some(Uuid::from_u128(1));
        let conflict_code = replay_graph
            .adopt_ontology_module(&conflict, None)
            .unwrap_err()
            .code;
        cases.insert(
            "idempotent_replay".into(),
            serde_json::json!({
                "first_receipt": first,
                "replay_receipt": replay_receipt,
                "conflict_code": conflict_code
            }),
        );

        let project = tempfile::tempdir().unwrap();
        let durable = GraphForge::new(Some(project.path().to_str().unwrap())).unwrap();
        let import_before = durable.ontology_authority_state().unwrap();
        drop(durable);
        let import_source = tempfile::tempdir().unwrap();
        let invalid = import_source.path().join("invalid.gfpb");
        std::fs::write(&invalid, b"invalid").unwrap();
        let mut before_entries = std::fs::read_dir(project.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        before_entries.sort();
        assert!(
            GraphForge::import_portable_v2(
                project.path(),
                &crate::PortableV2ImportRequest {
                    input: invalid,
                    operation_id: OperationId(Uuid::from_u128(9_130)),
                    limits: graphforge_storage::PortableV2Limits::default()
                },
                None
            )
            .is_err()
        );
        let durable = GraphForge::new(Some(project.path().to_str().unwrap())).unwrap();
        let import_after = durable.ontology_authority_state().unwrap();
        let mut after_entries = std::fs::read_dir(project.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        after_entries.sort();
        cases.insert(
            "no_partial_import_or_authority_change".into(),
            serde_json::json!({
                "before_entries": before_entries,
                "after_entries": after_entries,
                "authority_before": import_before,
                "authority_after": import_after
            }),
        );

        let bounded = dependency_blocked_error(&blocked);
        let bounded_json = serde_json::to_string(&bounded).unwrap();
        cases.insert("bounded_structured_diagnostics".into(), serde_json::json!({"outer_code": bounded.code, "diagnostic_code": bounded.diagnostics[0].code, "bounded": bounded.diagnostics.len() <= MAX_ERROR_DIAGNOSTICS, "path_free": !bounded_json.contains("/Users/")}));
        let deterministic_first =
            serde_json::to_string(&graph.ontology_modules().unwrap()).unwrap();
        let deterministic_second =
            serde_json::to_string(&graph.ontology_modules().unwrap()).unwrap();
        cases.insert(
            "deterministic_path_free_cli_json".into(),
            serde_json::json!({
                "first_serialized": deterministic_first,
                "second_serialized": deterministic_second,
                "forbidden_path": graph.dir.to_string_lossy()
            }),
        );
        let packaged = GraphForge::new(None).unwrap();
        let packaged_modules = packaged.ontology_modules().unwrap();
        cases.insert(
            "packaged_clean_install".into(),
            serde_json::json!({
                "package_origin": env!("CARGO_PKG_NAME"),
                "operation": "ontology_modules",
                "module_count": packaged_modules.len()
            }),
        );

        let report = MultiOntologyParityReport {
            contract: "graphforge-multi-ontology-parity-result/1".into(),
            cases,
        };
        assert_eq!(report.cases.len(), 10);
        if let Ok(path) = std::env::var("GRAPHFORGE_MULTI_ONTOLOGY_PARITY_REPORT") {
            std::fs::write(path, serde_json::to_vec(&report).unwrap()).unwrap();
        }
    }

    #[test]
    fn conformance_bounded_structured_diagnostics() {
        let values: Vec<String> = (0..100)
            .map(|index| format!("subject-{index:03}"))
            .collect();
        let error = composition_error(graphforge_ontology::CompositionError::one(
            CompositionDiagnostic::new(
                DiagnosticCode::ResolutionAmbiguous,
                "ambiguous",
                values.clone(),
                values,
                DiagnosticLimit { max_candidates: 2 },
            ),
        ));
        assert_eq!(error.diagnostics.len(), 1);
        assert_eq!(error.diagnostics[0].limit, 2);
        assert_eq!(error.diagnostics[0].subjects.len(), 2);
        assert_eq!(error.diagnostics[0].candidates.len(), 2);
    }

    #[test]
    fn conformance_deterministic_path_free_serialization() {
        let document = parity_document("base");
        let error = dependency_blocked_error(&graphforge_ontology::DeletePreview {
            source_generation: 1,
            target: OntologyModuleId {
                ontology_id: document.ontology_id.clone(),
                authored_version: document.version.clone(),
                canonical_digest: graphforge_ontology::module_document_digest(&document).unwrap(),
            },
            dependent_modules: Vec::new(),
            activation_refs: vec!["module:stable".into()],
            bridge_refs: Vec::new(),
            safe: false,
        });
        let first = serde_json::to_string(&error).unwrap();
        let second = serde_json::to_string(&error).unwrap();
        assert_eq!(first, second);
        assert!(!first.contains("/Users/"));
        assert!(
            serde_json::to_string(&OntologyAuthorityState {
                project_generation_uuid: Uuid::nil(),
                composition_fingerprint: None
            })
            .unwrap()
            .contains("project_generation_uuid")
        );
    }

    #[test]
    fn conformance_packaged_rust_surface() {
        let graph = GraphForge::new(None).unwrap();
        assert!(graph.ontology_modules().unwrap().is_empty());
        assert!(
            graph
                .portable_ontology_staging(graphforge_storage::PortableV2Limits::default())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn multi_ontology_public_facade_inventory_is_runtime_covered() {
        let mut graph = GraphForge::new(None).unwrap();
        let _authority = graph.ontology_authority_state().unwrap();
        assert!(graph.ontology_modules().unwrap().is_empty());
        assert!(
            graph
                .inspect_ontology_module(&ModuleSelector::OntologyId("urn:missing".into()))
                .is_err()
        );
        assert!(
            graph
                .validate_ontology_module(&parity_document("base"))
                .unwrap()
                .valid
        );
        let base = graph
            .create_ontology_module(parity_document("base"), Vec::new(), None)
            .unwrap();
        let dependent = graph
            .import_ontology_module(
                &serde_json::to_string(&parity_document("dependent")).unwrap(),
                ImportFormatHint::Json,
                vec![base.id.clone()],
            )
            .unwrap();
        for (seed, candidate) in [(9_200, base.clone()), (9_201, dependent.clone())] {
            graph
                .adopt_ontology_module(
                    &ModuleAdoptionRequest {
                        authority: expectation(&graph, seed),
                        candidate,
                    },
                    None,
                )
                .unwrap();
        }
        graph
            .inspect_ontology_module(&ModuleSelector::Exact(base.id.clone()))
            .unwrap();
        graph
            .export_ontology_module(&ModuleSelector::Exact(base.id.clone()), ExportFormat::Json)
            .unwrap();

        let bridge_document = parity_bridge(&base.id, &dependent.id);
        assert!(
            graph
                .validate_ontology_bridge(&bridge_document)
                .unwrap()
                .valid
        );
        let created_bridge = graph
            .create_ontology_bridge(bridge_document.clone())
            .unwrap();
        let imported_bridge = graph
            .import_ontology_bridge(
                &serde_json::to_string(&bridge_document).unwrap(),
                BridgeImportFormatHint::Json,
            )
            .unwrap();
        assert_eq!(created_bridge.id, imported_bridge.id);
        graph
            .adopt_ontology_bridge(
                &BridgeAdoptionRequest {
                    authority: expectation(&graph, 9_202),
                    candidate: imported_bridge,
                },
                None,
            )
            .unwrap();
        let bridge_id = graph.ontology_bridges().unwrap()[0].id.clone();
        graph
            .inspect_ontology_bridge(&BridgeSelector::Exact(bridge_id.clone()))
            .unwrap();
        graph
            .export_ontology_bridge(
                &BridgeSelector::Exact(bridge_id.clone()),
                BridgeExportFormat::Json,
            )
            .unwrap();
        let mut bridge_update = bridge_document;
        bridge_update.authored_version = "2.0.0".into();
        graph
            .preview_update_ontology_bridge(
                &BridgeSelector::Exact(bridge_id.clone()),
                &bridge_update,
            )
            .unwrap();
        graph
            .update_ontology_bridge(
                &BridgeUpdateRequest {
                    authority: expectation(&graph, 9_203),
                    selector: BridgeSelector::Exact(bridge_id),
                    document: bridge_update,
                },
                None,
            )
            .unwrap();
        let updated_bridge = graph.ontology_bridges().unwrap()[0].id.clone();
        assert!(
            graph
                .preview_delete_ontology_bridge(&BridgeSelector::Exact(updated_bridge.clone()))
                .unwrap()
                .safe
        );
        graph
            .delete_ontology_bridge(
                &BridgeDeleteRequest {
                    authority: expectation(&graph, 9_204),
                    selector: BridgeSelector::Exact(updated_bridge),
                },
                None,
            )
            .unwrap();

        let (profile_default, activation) = graph.ontology_activation_profile().unwrap();
        graph
            .change_ontology_activation_profile(
                &ActivationProfileChangeRequest {
                    authority: expectation(&graph, 9_205),
                    profile_default,
                    activation,
                },
                None,
            )
            .unwrap();
        let composition = graph.required_composition().unwrap();
        graph.validate_ontology_composition(&composition).unwrap();
        graph
            .preflight_ontology_composition(
                &CompositionChangeRequest {
                    context: WriteContext {
                        operation_uuid: OperationId(Uuid::from_u128(9_206)),
                        actor_uuid: None,
                    },
                    expected_project_generation_uuid: graph
                        .ontology_authority_state()
                        .unwrap()
                        .project_generation_uuid,
                    expected_composition_fingerprint: graph
                        .ontology_authority_state()
                        .unwrap()
                        .composition_fingerprint,
                    candidate: composition,
                    data_disposition: CompositionDataDisposition::RequireConforming,
                },
                None,
            )
            .unwrap();
        graph
            .explain_ontology_resolution(&ResolutionExplainRequest {
                module: Some(base.id.clone()),
                kind: SymbolKind::Entity,
                local_id: "Person".into(),
                max_candidates: 4,
            })
            .unwrap();
        assert!(
            graph
                .portable_ontology_staging(graphforge_storage::PortableV2Limits::default())
                .unwrap()
                .is_none()
        );
        let authority = expectation(&graph, 9_207);
        assert!(
            graph
                .adopt_portable_ontology_staging(
                    &authority,
                    graphforge_storage::PortableV2Limits::default(),
                    None,
                )
                .is_err()
        );

        graph
            .preview_update_ontology_module(
                &ModuleSelector::Exact(dependent.id.clone()),
                &parity_document("dependent_update"),
                &[base.id.clone()],
            )
            .unwrap();
        graph
            .update_ontology_module(
                &ModuleUpdateRequest {
                    authority: expectation(&graph, 9_208),
                    selector: ModuleSelector::Exact(dependent.id),
                    document: parity_document("dependent_update"),
                    dependencies: vec![base.id],
                    enforcement: None,
                },
                None,
            )
            .unwrap();
        let updated_module = graph.ontology_modules().unwrap()[1].id.clone();
        assert!(
            graph
                .preview_delete_ontology_module(&ModuleSelector::Exact(updated_module.clone()))
                .unwrap()
                .safe
        );
        graph
            .delete_ontology_module(
                &ModuleDeleteRequest {
                    authority: expectation(&graph, 9_209),
                    selector: ModuleSelector::Exact(updated_module),
                },
                None,
            )
            .unwrap();
    }

    #[test]
    fn rust_facade_bootstrap_replay_resolution_and_dependency_blocker() {
        let project = tempfile::tempdir().unwrap();
        let mut graph = GraphForge::new(Some(project.path().to_str().unwrap())).unwrap();
        assert!(graph.ontology_modules().unwrap().is_empty());

        let parent = graph
            .create_ontology_module(
                document("https://example.test/parent", "Person"),
                Vec::new(),
                None,
            )
            .unwrap();
        let adopt_parent = ModuleAdoptionRequest {
            authority: expectation(&graph, 842_001),
            candidate: parent.clone(),
        };
        let published = graph.adopt_ontology_module(&adopt_parent, None).unwrap();
        let replayed = graph.adopt_ontology_module(&adopt_parent, None).unwrap();
        assert_eq!(published, replayed);

        let mut changed_actor = adopt_parent.clone();
        changed_actor.authority.context.actor_uuid = Some(Uuid::from_u128(842_901));
        assert_eq!(
            graph
                .adopt_ontology_module(&changed_actor, None)
                .unwrap_err()
                .code(),
            "GF_IDEMPOTENCY_CONFLICT"
        );
        let mut changed_authority = adopt_parent.clone();
        changed_authority.authority.expected_project_generation_uuid = Uuid::from_u128(842_902);
        assert_eq!(
            graph
                .adopt_ontology_module(&changed_authority, None)
                .unwrap_err()
                .code(),
            "GF_IDEMPOTENCY_CONFLICT"
        );

        let child = graph
            .create_ontology_module(
                document("https://example.test/child", "Organization"),
                vec![parent.id.clone()],
                None,
            )
            .unwrap();
        let child_authority = expectation(&graph, 842_002);
        graph
            .adopt_ontology_module(
                &ModuleAdoptionRequest {
                    authority: child_authority,
                    candidate: child,
                },
                None,
            )
            .unwrap();
        assert_eq!(graph.ontology_modules().unwrap().len(), 2);
        let blocked = graph
            .preview_delete_ontology_module(&ModuleSelector::Exact(parent.id.clone()))
            .unwrap();
        assert!(!blocked.safe);
        assert_eq!(blocked.dependent_modules.len(), 1);

        let explanation = graph
            .explain_ontology_resolution(&ResolutionExplainRequest {
                module: None,
                kind: SymbolKind::Entity,
                local_id: "Person".into(),
                max_candidates: 4,
            })
            .unwrap();
        assert!(explanation.diagnostics.is_empty());
        assert_eq!(explanation.outcome.unwrap().symbol.module, parent.id);
    }
}
