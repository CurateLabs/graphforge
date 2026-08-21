//! Ontology inventory authority store and lifecycle operations.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::composition::{
    ActivationMode, ActivationRecord, AuthoredModule, BridgeSetId, CompiledComposition,
    CompositionDiagnostic, CompositionError, CompositionLimits, DiagnosticCode, DiagnosticLimit,
    InventoryCompileRequest, OntologyModuleId, compile_inventory, module_document_digest,
};
use crate::loader::OntologyLoader;
use crate::ontology::OntologyDoc;
use crate::validator::OntologyValidator;

/// Lifecycle status for a module record (contract state machine).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModuleLifecycleStatus {
    /// Staged, not validated.
    Candidate,
    /// Validated but not durable authority.
    Validated,
    /// Durable inventory authority.
    Adopted,
    /// Replaced by a newer exact module version.
    Superseded,
    /// Explicitly removed from authority.
    Removed,
}

impl ModuleLifecycleStatus {
    /// Stable wire token for the lifecycle status.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Candidate => "candidate",
            Self::Validated => "validated",
            Self::Adopted => "adopted",
            Self::Superseded => "superseded",
            Self::Removed => "removed",
        }
    }
}

/// How callers select a module for read/export/delete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModuleSelector {
    /// Exact module identity (required for unambiguous durable ops).
    Exact(OntologyModuleId),
    /// Ontology ID only — succeeds only when exactly one non-removed match exists.
    OntologyId(String),
}

/// Export encoding for a single module document.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportFormat {
    /// Canonical JSON (sorted keys via serde_json Value round-trip).
    Json,
    /// YAML document.
    Yaml,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct ModuleRecord {
    id: OntologyModuleId,
    status: ModuleLifecycleStatus,
    dependencies: Vec<OntologyModuleId>,
    doc: OntologyDoc,
    enforcement: Option<ActivationMode>,
}

/// List row returned in identity order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModuleListEntry {
    /// Exact module identity.
    pub id: OntologyModuleId,
    /// Lifecycle status.
    pub status: ModuleLifecycleStatus,
    /// Effective enforcement (override or inventory default).
    pub enforcement: ActivationMode,
    /// Exact dependencies.
    pub dependencies: Vec<OntologyModuleId>,
    /// Canonical digest (same as `id.canonical_digest`).
    pub digest: String,
}

/// Detailed inspect receipt for one module.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModuleInspect {
    /// List metadata.
    pub entry: ModuleListEntry,
    /// Authored document (authority copy; not flattened).
    pub doc: OntologyDoc,
    /// Current inventory generation.
    pub generation: u64,
    /// Current composition fingerprint over adopted authority.
    pub composition_fingerprint: String,
}

/// Non-mutating update impact preview.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdatePreview {
    /// Source generation the preview was computed against.
    pub source_generation: u64,
    /// Module that would be superseded.
    pub prior: OntologyModuleId,
    /// Replacement module identity.
    pub next: OntologyModuleId,
    /// Dependants that reference `prior`.
    pub affected_dependants: Vec<OntologyModuleId>,
    /// Whether the replacement document validates.
    pub document_valid: bool,
}

/// Non-mutating delete impact preview.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeletePreview {
    /// Source generation the preview was computed against.
    pub source_generation: u64,
    /// Module under consideration.
    pub target: OntologyModuleId,
    /// Adopted modules that list `target` as a dependency.
    pub dependent_modules: Vec<OntologyModuleId>,
    /// Activation subjects referencing the target.
    pub activation_refs: Vec<String>,
    /// Exact bridge identities whose module endpoints reference the target.
    #[serde(default)]
    pub bridge_refs: Vec<BridgeSetId>,
    /// True when delete would succeed without remediation.
    pub safe: bool,
}

/// Mutation receipt published with a successful authority change.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InventoryMutationReceipt {
    /// Caller operation identity (idempotency key).
    pub operation_id: String,
    /// Generation before the mutation.
    pub prior_generation: u64,
    /// Generation after the mutation.
    pub new_generation: u64,
    /// Exact module affected (when applicable).
    pub affected_module: Option<OntologyModuleId>,
    /// Digest of the affected module document.
    pub digest: Option<String>,
    /// Composition fingerprint after publication.
    pub composition_fingerprint: String,
    /// True when this call replayed a prior successful operation.
    pub idempotent_replay: bool,
}

/// Flattening-free inventory metadata export.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InventoryMetadata {
    /// Authority generation.
    pub generation: u64,
    /// Composition fingerprint.
    pub composition_fingerprint: String,
    /// Default enforcement mode.
    pub profile_default: ActivationMode,
    /// Adopted module identities in closure/identity order.
    pub modules: Vec<OntologyModuleId>,
    /// Bridge identities (empty until #838).
    pub bridges: Vec<BridgeSetId>,
}

/// Durable snapshot for reopen / persistence tests.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InventorySnapshot {
    /// Schema version for this snapshot encoding.
    pub schema_version: u32,
    /// Authority generation.
    pub generation: u64,
    /// Default enforcement.
    pub profile_default: ActivationMode,
    /// Scoped activation overrides.
    pub activation: Vec<ActivationRecord>,
    /// Bridge identities.
    pub bridges: Vec<BridgeSetId>,
    /// Adopted (and retained superseded/removed history optional — only adopted here).
    pub adopted: Vec<SnapshotModule>,
    /// Composition fingerprint at snapshot time.
    pub composition_fingerprint: String,
    /// Completed operation receipts for idempotency.
    pub receipts: Vec<InventoryMutationReceipt>,
}

/// One adopted module in a snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotModule {
    /// Exact identity.
    pub id: OntologyModuleId,
    /// Dependencies.
    pub dependencies: Vec<OntologyModuleId>,
    /// Document.
    pub doc: OntologyDoc,
    /// Optional enforcement override.
    pub enforcement: Option<ActivationMode>,
}

/// Durable ontology inventory with session staging.
#[derive(Debug, Clone)]
pub struct OntologyInventory {
    generation: u64,
    profile_default: ActivationMode,
    activation: Vec<ActivationRecord>,
    bridges: Vec<BridgeSetId>,
    /// Adopted authority keyed by display_ref.
    adopted: HashMap<String, ModuleRecord>,
    /// Session-only staging (candidates / validated imports); never durable authority.
    staging: HashMap<String, ModuleRecord>,
    fingerprint: String,
    receipts: HashMap<String, InventoryMutationReceipt>,
    limits: CompositionLimits,
}

impl Default for OntologyInventory {
    fn default() -> Self {
        Self::new(ActivationMode::Exploratory, CompositionLimits::default())
    }
}

impl OntologyInventory {
    /// Create an empty inventory at generation 0.
    #[must_use]
    pub fn new(profile_default: ActivationMode, limits: CompositionLimits) -> Self {
        Self {
            generation: 0,
            profile_default,
            activation: Vec::new(),
            bridges: Vec::new(),
            adopted: HashMap::new(),
            staging: HashMap::new(),
            fingerprint: empty_fingerprint(profile_default),
            receipts: HashMap::new(),
            limits,
        }
    }

    /// Current authority generation.
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Current composition fingerprint.
    #[must_use]
    pub fn composition_fingerprint(&self) -> &str {
        &self.fingerprint
    }

    /// Validate a document without mutating authority or staging.
    pub fn validate_document(&self, doc: &OntologyDoc) -> Result<(), CompositionError> {
        let dlimit = self.diag_limit();
        OntologyValidator::validate(doc).map_err(|errors| {
            CompositionError::one(CompositionDiagnostic::with_subjects(
                DiagnosticCode::InventoryMalformed,
                format!("document failed validation ({} error(s))", errors.len()),
                vec![doc.ontology_id.clone()],
                dlimit,
            ))
        })?;
        let _ = module_document_digest(doc).map_err(|e| {
            CompositionError::one(CompositionDiagnostic::with_subjects(
                DiagnosticCode::InterchangeIntegrity,
                format!("failed to digest document: {e}"),
                vec![doc.ontology_id.clone()],
                dlimit,
            ))
        })?;
        Ok(())
    }

    /// Create/register a validated authored module into session staging.
    ///
    /// Does not change durable authority until [`Self::adopt`].
    pub fn create_register(
        &mut self,
        doc: OntologyDoc,
        dependencies: Vec<OntologyModuleId>,
        enforcement: Option<ActivationMode>,
        operation_id: impl Into<String>,
    ) -> Result<OntologyModuleId, CompositionError> {
        let _operation_id = operation_id.into();
        self.validate_document(&doc)?;
        let digest = module_document_digest(&doc).map_err(|e| {
            CompositionError::one(CompositionDiagnostic::with_subjects(
                DiagnosticCode::InterchangeIntegrity,
                format!("failed to digest document: {e}"),
                vec![doc.ontology_id.clone()],
                self.diag_limit(),
            ))
        })?;
        let id = OntologyModuleId {
            ontology_id: doc.ontology_id.clone(),
            authored_version: doc.version.clone(),
            canonical_digest: digest,
        };
        let key = id.display_ref();
        if self.adopted.contains_key(&key) {
            return Err(CompositionError::one(CompositionDiagnostic::for_module(
                DiagnosticCode::InventoryDuplicate,
                "module identity already adopted in durable inventory",
                &id,
                self.diag_limit(),
            )));
        }
        let record = ModuleRecord {
            id: id.clone(),
            status: ModuleLifecycleStatus::Validated,
            dependencies,
            doc,
            enforcement,
        };
        self.staging.insert(key, record);
        Ok(id)
    }

    /// Import YAML or JSON text as a non-authoritative staged candidate.
    pub fn import_text(
        &mut self,
        text: &str,
        format_hint: ImportFormatHint,
        dependencies: Vec<OntologyModuleId>,
        operation_id: impl Into<String>,
    ) -> Result<OntologyModuleId, CompositionError> {
        let _operation_id = operation_id.into();
        let doc = parse_document(text, format_hint, self.diag_limit())?;
        self.validate_document(&doc)?;
        let digest = module_document_digest(&doc).map_err(|e| {
            CompositionError::one(CompositionDiagnostic::with_subjects(
                DiagnosticCode::InterchangeIntegrity,
                e,
                vec![doc.ontology_id.clone()],
                self.diag_limit(),
            ))
        })?;
        let id = OntologyModuleId {
            ontology_id: doc.ontology_id.clone(),
            authored_version: doc.version.clone(),
            canonical_digest: digest,
        };
        let key = id.display_ref();
        self.staging.insert(
            key,
            ModuleRecord {
                id: id.clone(),
                status: ModuleLifecycleStatus::Candidate,
                dependencies,
                doc,
                enforcement: None,
            },
        );
        Ok(id)
    }

    /// Explicitly adopt a staged (or already-validated) module into durable authority.
    pub fn adopt(
        &mut self,
        selector: &ModuleSelector,
        source_generation: u64,
        operation_id: impl Into<String>,
    ) -> Result<InventoryMutationReceipt, CompositionError> {
        let operation_id = operation_id.into();
        if let Some(prior) = self.receipts.get(&operation_id) {
            return Ok(replay(prior));
        }
        self.require_generation(source_generation)?;
        let staged = self.take_staged(selector)?;
        if staged.status != ModuleLifecycleStatus::Validated
            && staged.status != ModuleLifecycleStatus::Candidate
        {
            return Err(CompositionError::one(CompositionDiagnostic::for_module(
                DiagnosticCode::LifecycleInvalidTransition,
                format!("cannot adopt module in status {}", staged.status.as_str()),
                &staged.id,
                self.diag_limit(),
            )));
        }
        // Candidate must be revalidated before adoption.
        self.validate_document(&staged.doc)?;
        let key = staged.id.display_ref();
        if self.adopted.contains_key(&key) {
            return Err(CompositionError::one(CompositionDiagnostic::for_module(
                DiagnosticCode::InventoryDuplicate,
                "module already adopted",
                &staged.id,
                self.diag_limit(),
            )));
        }
        let mut record = staged;
        record.status = ModuleLifecycleStatus::Adopted;
        let prior = self.generation;
        self.adopted.insert(key, record.clone());
        self.publish(
            prior,
            Some(record.id.clone()),
            Some(record.id.canonical_digest.clone()),
            operation_id,
        )
    }

    /// List adopted modules in deterministic identity order.
    #[must_use]
    pub fn list(&self) -> Vec<ModuleListEntry> {
        let mut entries: Vec<ModuleListEntry> = self
            .adopted
            .values()
            .filter(|r| r.status == ModuleLifecycleStatus::Adopted)
            .map(|r| self.to_list_entry(r))
            .collect();
        entries.sort_by_key(|e| e.id.sort_key());
        entries
    }

    /// Get/inspect one exact module from durable authority.
    pub fn inspect(&self, selector: &ModuleSelector) -> Result<ModuleInspect, CompositionError> {
        let record = self.resolve_adopted(selector)?;
        Ok(ModuleInspect {
            entry: self.to_list_entry(record),
            doc: record.doc.clone(),
            generation: self.generation,
            composition_fingerprint: self.fingerprint.clone(),
        })
    }

    /// Preview replacing an adopted module with a new document version.
    pub fn preview_update(
        &self,
        selector: &ModuleSelector,
        next_doc: &OntologyDoc,
        next_dependencies: &[OntologyModuleId],
    ) -> Result<UpdatePreview, CompositionError> {
        let prior = self.resolve_adopted(selector)?;
        let document_valid = self.validate_document(next_doc).is_ok();
        let digest = module_document_digest(next_doc).unwrap_or_default();
        let next = OntologyModuleId {
            ontology_id: next_doc.ontology_id.clone(),
            authored_version: next_doc.version.clone(),
            canonical_digest: digest,
        };
        let _ = next_dependencies;
        Ok(UpdatePreview {
            source_generation: self.generation,
            prior: prior.id.clone(),
            next,
            affected_dependants: self.dependants_of(&prior.id),
            document_valid,
        })
    }

    /// Atomically replace one adopted module version.
    pub fn update(
        &mut self,
        selector: &ModuleSelector,
        next_doc: OntologyDoc,
        next_dependencies: Vec<OntologyModuleId>,
        source_generation: u64,
        operation_id: impl Into<String>,
    ) -> Result<InventoryMutationReceipt, CompositionError> {
        let operation_id = operation_id.into();
        if let Some(prior) = self.receipts.get(&operation_id) {
            return Ok(replay(prior));
        }
        self.require_generation(source_generation)?;
        let preview = self.preview_update(selector, &next_doc, &next_dependencies)?;
        if !preview.document_valid {
            self.validate_document(&next_doc)?;
        }
        if preview.next.ontology_id != preview.prior.ontology_id {
            return Err(CompositionError::one(CompositionDiagnostic::with_subjects(
                DiagnosticCode::CollisionMetadata,
                "update replacement must retain the same ontology_id",
                vec![preview.prior.display_ref(), preview.next.display_ref()],
                self.diag_limit(),
            )));
        }
        let prior_key = preview.prior.display_ref();
        let mut prior_record = self.adopted.remove(&prior_key).ok_or_else(|| {
            CompositionError::one(CompositionDiagnostic::for_module(
                DiagnosticCode::InventoryNotFound,
                "module disappeared during update",
                &preview.prior,
                self.diag_limit(),
            ))
        })?;
        prior_record.status = ModuleLifecycleStatus::Superseded;

        let next_record = ModuleRecord {
            id: preview.next.clone(),
            status: ModuleLifecycleStatus::Adopted,
            dependencies: next_dependencies,
            doc: next_doc,
            enforcement: prior_record.enforcement,
        };
        // Rewrite dependants that pointed at the prior exact identity.
        for dep_key in self.adopted.keys().cloned().collect::<Vec<_>>() {
            if let Some(dep) = self.adopted.get_mut(&dep_key) {
                for edge in &mut dep.dependencies {
                    if edge == &preview.prior {
                        *edge = preview.next.clone();
                    }
                }
            }
        }
        self.adopted
            .insert(preview.next.display_ref(), next_record.clone());
        let prior_gen = self.generation;
        self.publish(
            prior_gen,
            Some(next_record.id.clone()),
            Some(next_record.id.canonical_digest.clone()),
            operation_id,
        )
    }

    /// Preview deletion impact.
    pub fn preview_delete(
        &self,
        selector: &ModuleSelector,
    ) -> Result<DeletePreview, CompositionError> {
        let target = self.resolve_adopted(selector)?;
        let dependent_modules = self.dependants_of(&target.id);
        let activation_refs: Vec<String> = self
            .activation
            .iter()
            .filter(|a| a.subject == target.id.display_ref())
            .map(|a| a.subject.clone())
            .collect();
        let bridge_refs = Vec::new();
        let safe = dependent_modules.is_empty() && activation_refs.is_empty();
        Ok(DeletePreview {
            source_generation: self.generation,
            target: target.id.clone(),
            dependent_modules,
            activation_refs,
            bridge_refs,
            safe,
        })
    }

    /// Atomically remove an adopted module when safe.
    pub fn delete(
        &mut self,
        selector: &ModuleSelector,
        source_generation: u64,
        operation_id: impl Into<String>,
    ) -> Result<InventoryMutationReceipt, CompositionError> {
        let operation_id = operation_id.into();
        if let Some(prior) = self.receipts.get(&operation_id) {
            return Ok(replay(prior));
        }
        self.require_generation(source_generation)?;
        let preview = self.preview_delete(selector)?;
        if !preview.safe {
            let mut subjects = vec![preview.target.display_ref()];
            subjects.extend(
                preview
                    .dependent_modules
                    .iter()
                    .map(OntologyModuleId::display_ref),
            );
            return Err(CompositionError::one(CompositionDiagnostic::with_subjects(
                DiagnosticCode::DependencyInUse,
                "module is referenced by dependants or activation; remove those first",
                subjects,
                self.diag_limit(),
            )));
        }
        let key = preview.target.display_ref();
        let Some(mut record) = self.adopted.remove(&key) else {
            return Err(CompositionError::one(CompositionDiagnostic::for_module(
                DiagnosticCode::InventoryNotFound,
                "module not found for delete",
                &preview.target,
                self.diag_limit(),
            )));
        };
        record.status = ModuleLifecycleStatus::Removed;
        let digest = record.id.canonical_digest.clone();
        let id = record.id.clone();
        let prior = self.generation;
        // Drop activation overrides for the removed module.
        self.activation.retain(|a| a.subject != id.display_ref());
        self.publish(prior, Some(id), Some(digest), operation_id)
    }

    /// Deterministically export one adopted module as YAML or JSON.
    pub fn export_module(
        &self,
        selector: &ModuleSelector,
        format: ExportFormat,
    ) -> Result<String, CompositionError> {
        let record = self.resolve_adopted(selector)?;
        match format {
            ExportFormat::Json => {
                let value = serde_json::to_value(&record.doc).map_err(|e| {
                    CompositionError::one(CompositionDiagnostic::with_subjects(
                        DiagnosticCode::InterchangeIntegrity,
                        format!("json encode failed: {e}"),
                        vec![record.id.display_ref()],
                        self.diag_limit(),
                    ))
                })?;
                // Stable key order via serde_json Map iteration is insertion; re-parse
                // through composition canonical for identity-safe bytes then pretty.
                let pretty = serde_json::to_string_pretty(&value).map_err(|e| {
                    CompositionError::one(CompositionDiagnostic::with_subjects(
                        DiagnosticCode::InterchangeIntegrity,
                        format!("json pretty failed: {e}"),
                        vec![record.id.display_ref()],
                        self.diag_limit(),
                    ))
                })?;
                Ok(pretty)
            }
            ExportFormat::Yaml => serde_yaml::to_string(&record.doc).map_err(|e| {
                CompositionError::one(CompositionDiagnostic::with_subjects(
                    DiagnosticCode::InterchangeIntegrity,
                    format!("yaml encode failed: {e}"),
                    vec![record.id.display_ref()],
                    self.diag_limit(),
                ))
            }),
        }
    }

    /// Export inventory metadata without flattening module documents.
    #[must_use]
    pub fn export_metadata(&self) -> InventoryMetadata {
        let mut modules: Vec<OntologyModuleId> = self
            .adopted
            .values()
            .filter(|r| r.status == ModuleLifecycleStatus::Adopted)
            .map(|r| r.id.clone())
            .collect();
        modules.sort_by_key(OntologyModuleId::sort_key);
        let mut bridges = self.bridges.clone();
        bridges.sort_by_key(BridgeSetId::sort_key);
        InventoryMetadata {
            generation: self.generation,
            composition_fingerprint: self.fingerprint.clone(),
            profile_default: self.profile_default,
            modules,
            bridges,
        }
    }

    /// Compile adopted authority into a [`CompiledComposition`].
    pub fn compile(&self) -> Result<CompiledComposition, CompositionError> {
        let authored = self.authored_adopted();
        compile_inventory(InventoryCompileRequest {
            modules: &authored,
            bridges: &self.bridges,
            activation: &self.activation,
            profile_default: self.profile_default,
            limits: self.limits,
            cancelled: None,
        })
    }

    /// Serialize durable authority (staging excluded).
    #[must_use]
    pub fn snapshot(&self) -> InventorySnapshot {
        let mut adopted: Vec<SnapshotModule> = self
            .adopted
            .values()
            .filter(|r| r.status == ModuleLifecycleStatus::Adopted)
            .map(|r| SnapshotModule {
                id: r.id.clone(),
                dependencies: r.dependencies.clone(),
                doc: r.doc.clone(),
                enforcement: r.enforcement,
            })
            .collect();
        adopted.sort_by_key(|m| m.id.sort_key());
        let mut receipts: Vec<_> = self.receipts.values().cloned().collect();
        receipts.sort_by(|a, b| a.operation_id.cmp(&b.operation_id));
        InventorySnapshot {
            schema_version: 1,
            generation: self.generation,
            profile_default: self.profile_default,
            activation: self.activation.clone(),
            bridges: self.bridges.clone(),
            adopted,
            composition_fingerprint: self.fingerprint.clone(),
            receipts,
        }
    }

    /// Reopen durable authority from a snapshot (staging starts empty).
    pub fn reopen(snapshot: InventorySnapshot) -> Result<Self, CompositionError> {
        if snapshot.schema_version != 1 {
            return Err(CompositionError::one(CompositionDiagnostic::with_subjects(
                DiagnosticCode::InterchangeIntegrity,
                format!(
                    "unsupported inventory snapshot version {}",
                    snapshot.schema_version
                ),
                Vec::new(),
                DiagnosticLimit::default(),
            )));
        }
        let mut inv = Self::new(snapshot.profile_default, CompositionLimits::default());
        inv.generation = snapshot.generation;
        inv.activation = snapshot.activation;
        inv.bridges = snapshot.bridges;
        for module in snapshot.adopted {
            inv.adopted.insert(
                module.id.display_ref(),
                ModuleRecord {
                    id: module.id,
                    status: ModuleLifecycleStatus::Adopted,
                    dependencies: module.dependencies,
                    doc: module.doc,
                    enforcement: module.enforcement,
                },
            );
        }
        for receipt in snapshot.receipts {
            inv.receipts.insert(receipt.operation_id.clone(), receipt);
        }
        inv.recompute_fingerprint()?;
        if inv.fingerprint != snapshot.composition_fingerprint {
            return Err(CompositionError::one(CompositionDiagnostic::with_subjects(
                DiagnosticCode::InterchangeIntegrity,
                "reopened composition fingerprint does not match snapshot",
                vec![snapshot.composition_fingerprint, inv.fingerprint.clone()],
                inv.diag_limit(),
            )));
        }
        Ok(inv)
    }

    /// Project a legacy single-ontology document into a one-module adopted inventory.
    pub fn from_legacy_single(
        doc: OntologyDoc,
        publish_m9_identity: bool,
        profile_default: ActivationMode,
    ) -> Result<Self, CompositionError> {
        let mut inv = Self::new(profile_default, CompositionLimits::default());
        let digest = module_document_digest(&doc).map_err(|e| {
            CompositionError::one(CompositionDiagnostic::with_subjects(
                DiagnosticCode::InterchangeIntegrity,
                e,
                vec![doc.ontology_id.clone()],
                inv.diag_limit(),
            ))
        })?;
        let id = if publish_m9_identity {
            OntologyModuleId {
                ontology_id: format!("legacy:{digest}"),
                authored_version: "legacy-v1".to_owned(),
                canonical_digest: digest,
            }
        } else {
            OntologyModuleId {
                ontology_id: doc.ontology_id.clone(),
                authored_version: doc.version.clone(),
                canonical_digest: digest,
            }
        };
        inv.adopted.insert(
            id.display_ref(),
            ModuleRecord {
                id,
                status: ModuleLifecycleStatus::Adopted,
                dependencies: Vec::new(),
                doc,
                enforcement: None,
            },
        );
        let prior = inv.generation;
        inv.publish(prior, None, None, "legacy-bootstrap".to_owned())?;
        Ok(inv)
    }

    fn publish(
        &mut self,
        prior_generation: u64,
        affected: Option<OntologyModuleId>,
        digest: Option<String>,
        operation_id: String,
    ) -> Result<InventoryMutationReceipt, CompositionError> {
        self.recompute_fingerprint()?;
        self.generation = prior_generation.checked_add(1).ok_or_else(|| {
            CompositionError::one(CompositionDiagnostic::with_subjects(
                DiagnosticCode::ResourceDiagnostics,
                "generation counter overflow",
                Vec::new(),
                self.diag_limit(),
            ))
        })?;
        let receipt = InventoryMutationReceipt {
            operation_id: operation_id.clone(),
            prior_generation,
            new_generation: self.generation,
            affected_module: affected,
            digest,
            composition_fingerprint: self.fingerprint.clone(),
            idempotent_replay: false,
        };
        self.receipts.insert(operation_id, receipt.clone());
        Ok(receipt)
    }

    fn recompute_fingerprint(&mut self) -> Result<(), CompositionError> {
        let authored = self.authored_adopted();
        let compiled = compile_inventory(InventoryCompileRequest {
            modules: &authored,
            bridges: &self.bridges,
            activation: &self.activation,
            profile_default: self.profile_default,
            limits: self.limits,
            cancelled: None,
        })?;
        self.fingerprint = compiled.fingerprint;
        Ok(())
    }

    fn authored_adopted(&self) -> Vec<AuthoredModule> {
        self.adopted
            .values()
            .filter(|r| r.status == ModuleLifecycleStatus::Adopted)
            .map(|r| AuthoredModule {
                id: r.id.clone(),
                dependencies: r.dependencies.clone(),
                doc: r.doc.clone(),
                allow_projected_identity: r.id.ontology_id.starts_with("legacy:"),
            })
            .collect()
    }

    fn dependants_of(&self, target: &OntologyModuleId) -> Vec<OntologyModuleId> {
        let mut deps: Vec<OntologyModuleId> = self
            .adopted
            .values()
            .filter(|r| r.status == ModuleLifecycleStatus::Adopted)
            .filter(|r| r.dependencies.iter().any(|d| d == target))
            .map(|r| r.id.clone())
            .collect();
        deps.sort_by_key(OntologyModuleId::sort_key);
        deps
    }

    fn require_generation(&self, source: u64) -> Result<(), CompositionError> {
        if source != self.generation {
            return Err(CompositionError::one(CompositionDiagnostic::with_subjects(
                DiagnosticCode::InventoryGenerationConflict,
                format!(
                    "stale source generation {source}; current is {}",
                    self.generation
                ),
                vec![
                    format!("source={source}"),
                    format!("current={}", self.generation),
                ],
                self.diag_limit(),
            )));
        }
        Ok(())
    }

    fn resolve_adopted<'a>(
        &'a self,
        selector: &ModuleSelector,
    ) -> Result<&'a ModuleRecord, CompositionError> {
        match selector {
            ModuleSelector::Exact(id) => self
                .adopted
                .get(&id.display_ref())
                .filter(|r| r.status == ModuleLifecycleStatus::Adopted)
                .ok_or_else(|| {
                    CompositionError::one(CompositionDiagnostic::for_module(
                        DiagnosticCode::InventoryNotFound,
                        "adopted module not found",
                        id,
                        self.diag_limit(),
                    ))
                }),
            ModuleSelector::OntologyId(ontology_id) => {
                let matches: Vec<&ModuleRecord> = self
                    .adopted
                    .values()
                    .filter(|r| {
                        r.status == ModuleLifecycleStatus::Adopted
                            && r.id.ontology_id == *ontology_id
                    })
                    .collect();
                match matches.as_slice() {
                    [only] => Ok(*only),
                    [] => Err(CompositionError::one(CompositionDiagnostic::with_subjects(
                        DiagnosticCode::InventoryNotFound,
                        "no adopted module for ontology_id",
                        vec![ontology_id.clone()],
                        self.diag_limit(),
                    ))),
                    many => {
                        let candidates: Vec<String> =
                            many.iter().map(|r| r.id.display_ref()).collect();
                        Err(CompositionError::one(CompositionDiagnostic::new(
                            DiagnosticCode::ResolutionAmbiguous,
                            "ontology_id matches multiple adopted modules; use exact identity",
                            Vec::new(),
                            candidates,
                            self.diag_limit(),
                        )))
                    }
                }
            }
        }
    }

    fn take_staged(&mut self, selector: &ModuleSelector) -> Result<ModuleRecord, CompositionError> {
        let key = match selector {
            ModuleSelector::Exact(id) => id.display_ref(),
            ModuleSelector::OntologyId(ontology_id) => {
                let matches: Vec<String> = self
                    .staging
                    .values()
                    .filter(|r| r.id.ontology_id == *ontology_id)
                    .map(|r| r.id.display_ref())
                    .collect();
                match matches.as_slice() {
                    [only] => only.clone(),
                    [] => {
                        return Err(CompositionError::one(CompositionDiagnostic::with_subjects(
                            DiagnosticCode::InventoryNotFound,
                            "no staged module for ontology_id",
                            vec![ontology_id.clone()],
                            self.diag_limit(),
                        )));
                    }
                    many => {
                        return Err(CompositionError::one(CompositionDiagnostic::new(
                            DiagnosticCode::ResolutionAmbiguous,
                            "ontology_id matches multiple staged modules",
                            Vec::new(),
                            many.to_vec(),
                            self.diag_limit(),
                        )));
                    }
                }
            }
        };
        self.staging.remove(&key).ok_or_else(|| {
            CompositionError::one(CompositionDiagnostic::with_subjects(
                DiagnosticCode::InventoryNotFound,
                "staged module not found",
                vec![key],
                self.diag_limit(),
            ))
        })
    }

    fn to_list_entry(&self, record: &ModuleRecord) -> ModuleListEntry {
        ModuleListEntry {
            id: record.id.clone(),
            status: record.status,
            enforcement: record.enforcement.unwrap_or(self.profile_default),
            dependencies: record.dependencies.clone(),
            digest: record.id.canonical_digest.clone(),
        }
    }

    fn diag_limit(&self) -> DiagnosticLimit {
        DiagnosticLimit {
            max_candidates: self.limits.diagnostic_candidates.max(1),
        }
    }
}

/// Hint for import text decoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImportFormatHint {
    /// Parse as YAML.
    Yaml,
    /// Parse as JSON.
    Json,
    /// Infer from leading `{` / content.
    Auto,
}

fn parse_document(
    text: &str,
    hint: ImportFormatHint,
    dlimit: DiagnosticLimit,
) -> Result<OntologyDoc, CompositionError> {
    let trimmed = text.trim_start();
    let as_json = match hint {
        ImportFormatHint::Json => true,
        ImportFormatHint::Yaml => false,
        ImportFormatHint::Auto => trimmed.starts_with('{'),
    };
    if as_json {
        OntologyLoader::load_json(text.as_bytes()).map_err(|e| {
            CompositionError::one(CompositionDiagnostic::with_subjects(
                DiagnosticCode::InventoryMalformed,
                format!("json import failed: {e}"),
                Vec::new(),
                dlimit,
            ))
        })
    } else {
        OntologyLoader::load_yaml(text.as_bytes()).map_err(|e| {
            CompositionError::one(CompositionDiagnostic::with_subjects(
                DiagnosticCode::InventoryMalformed,
                format!("yaml import failed: {e}"),
                Vec::new(),
                dlimit,
            ))
        })
    }
}

fn empty_fingerprint(profile_default: ActivationMode) -> String {
    compile_inventory(InventoryCompileRequest {
        modules: &[],
        bridges: &[],
        activation: &[],
        profile_default,
        limits: CompositionLimits::default(),
        cancelled: None,
    })
    .map(|c| c.fingerprint)
    .unwrap_or_default()
}

fn replay(prior: &InventoryMutationReceipt) -> InventoryMutationReceipt {
    let mut out = prior.clone();
    out.idempotent_replay = true;
    out
}
