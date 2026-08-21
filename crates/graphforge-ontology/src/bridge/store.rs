//! Bridge-set authority store and lifecycle operations.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::composition::{
    ActivationMode, BridgeSetId, CompositionDiagnostic, CompositionError, DiagnosticCode,
    DiagnosticLimit, OntologyModuleId, SymbolKind, bridge_document_digest,
};

use super::types::{BridgeDocument, BridgeLifecycleStatus};
use super::validate::validate_bridge_document;

/// Known module symbols used to validate bridge endpoints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModuleSymbolTable {
    /// Exact module identity.
    pub id: OntologyModuleId,
    /// Entity local IDs.
    pub entities: HashSet<String>,
    /// Relation local IDs.
    pub relations: HashSet<String>,
    /// Property local IDs.
    pub properties: HashSet<String>,
}

impl ModuleSymbolTable {
    /// Whether the table contains a symbol of the given kind.
    #[must_use]
    pub fn contains(&self, kind: SymbolKind, local_id: &str) -> bool {
        match kind {
            SymbolKind::Entity => self.entities.contains(local_id),
            SymbolKind::Relation => self.relations.contains(local_id),
            SymbolKind::Property => self.properties.contains(local_id),
            SymbolKind::Constraint | SymbolKind::Migration => false,
        }
    }
}

/// How callers select a bridge for read/export/delete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BridgeSelector {
    /// Exact bridge identity.
    Exact(BridgeSetId),
    /// Bridge ID only — succeeds only when exactly one non-removed match exists.
    BridgeId(String),
}

/// Export encoding for a bridge document.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BridgeExportFormat {
    /// Canonical JSON.
    Json,
    /// YAML document.
    Yaml,
}

/// Import format hint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BridgeImportFormatHint {
    /// Parse as JSON.
    Json,
    /// Parse as YAML.
    Yaml,
    /// Detect from leading non-whitespace (`{` → JSON, else YAML).
    Auto,
}

/// List row returned in identity order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BridgeListEntry {
    /// Exact bridge identity.
    pub id: BridgeSetId,
    /// Lifecycle status.
    pub status: BridgeLifecycleStatus,
    /// Effective enforcement.
    pub enforcement: ActivationMode,
    /// Exact bridge dependencies.
    pub dependencies: Vec<BridgeSetId>,
    /// Canonical digest.
    pub digest: String,
}

/// Detailed inspect receipt.
#[derive(Debug, Clone, PartialEq)]
pub struct BridgeInspect {
    /// List metadata.
    pub entry: BridgeListEntry,
    /// Authored bridge document.
    pub doc: BridgeDocument,
    /// Current bridge inventory generation.
    pub generation: u64,
}

/// Non-mutating update impact preview.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BridgeUpdatePreview {
    /// Source generation.
    pub source_generation: u64,
    /// Bridge that would be superseded.
    pub prior: BridgeSetId,
    /// Replacement identity.
    pub next: BridgeSetId,
    /// Dependants that reference `prior`.
    pub affected_dependants: Vec<BridgeSetId>,
    /// Whether the replacement document validates.
    pub document_valid: bool,
}

/// Non-mutating delete impact preview.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BridgeDeletePreview {
    /// Source generation.
    pub source_generation: u64,
    /// Bridge under consideration.
    pub target: BridgeSetId,
    /// Adopted bridges that list `target` as a dependency.
    pub dependent_bridges: Vec<BridgeSetId>,
    /// Activation subjects referencing the target.
    pub activation_refs: Vec<String>,
    /// True when delete would succeed without remediation.
    pub safe: bool,
}

/// Mutation receipt published with a successful authority change.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BridgeMutationReceipt {
    /// Caller operation identity (idempotency key).
    pub operation_id: String,
    /// Generation before the mutation.
    pub prior_generation: u64,
    /// Generation after the mutation.
    pub new_generation: u64,
    /// Exact bridge affected (when applicable).
    pub affected_bridge: Option<BridgeSetId>,
    /// Digest of the affected bridge document.
    pub digest: Option<String>,
    /// True when this call replayed a prior successful operation.
    pub idempotent_replay: bool,
}

/// Durable snapshot for reopen / persistence tests.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BridgeSnapshot {
    /// Schema version for this snapshot encoding.
    pub schema_version: u32,
    /// Authority generation.
    pub generation: u64,
    /// Default enforcement for bridges without overrides.
    pub profile_default: ActivationMode,
    /// Activation subjects that reference bridges (opaque subject strings).
    pub activation_subjects: Vec<String>,
    /// Known module symbol tables (reopen authority context).
    pub modules: Vec<SnapshotModuleSymbols>,
    /// Adopted bridges.
    pub adopted: Vec<SnapshotBridge>,
    /// Completed operation receipts for idempotency.
    pub receipts: Vec<BridgeMutationReceipt>,
}

/// Serializable module symbol table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotModuleSymbols {
    /// Exact module identity.
    pub id: OntologyModuleId,
    /// Entity local IDs.
    pub entities: Vec<String>,
    /// Relation local IDs.
    pub relations: Vec<String>,
    /// Property local IDs.
    pub properties: Vec<String>,
}

/// One adopted bridge in a snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotBridge {
    /// Exact identity.
    pub id: BridgeSetId,
    /// Dependencies.
    pub dependencies: Vec<BridgeSetId>,
    /// Document.
    pub doc: BridgeDocument,
}

#[derive(Debug, Clone, PartialEq)]
struct BridgeRecord {
    id: BridgeSetId,
    status: BridgeLifecycleStatus,
    dependencies: Vec<BridgeSetId>,
    doc: BridgeDocument,
}

/// Durable bridge inventory with session staging.
#[derive(Debug, Clone)]
pub struct BridgeInventory {
    generation: u64,
    profile_default: ActivationMode,
    modules: HashMap<String, ModuleSymbolTable>,
    adopted: HashMap<String, BridgeRecord>,
    staging: HashMap<String, BridgeRecord>,
    /// Opaque activation subjects that reference bridge display_refs.
    activation_subjects: HashSet<String>,
    receipts: HashMap<String, BridgeMutationReceipt>,
    diag_limit: DiagnosticLimit,
}

impl Default for BridgeInventory {
    fn default() -> Self {
        Self::new(ActivationMode::Exploratory, DiagnosticLimit::default())
    }
}

impl BridgeInventory {
    /// Create an empty bridge inventory at generation 0.
    #[must_use]
    pub fn new(profile_default: ActivationMode, diag_limit: DiagnosticLimit) -> Self {
        Self {
            generation: 0,
            profile_default,
            modules: HashMap::new(),
            adopted: HashMap::new(),
            staging: HashMap::new(),
            activation_subjects: HashSet::new(),
            receipts: HashMap::new(),
            diag_limit,
        }
    }

    /// Current authority generation.
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Register or replace a known module symbol table (session/authority context).
    ///
    /// Does not mutate bridge authority generation. Equal local names across
    /// modules never create bridges.
    pub fn register_module(&mut self, table: ModuleSymbolTable) {
        self.modules.insert(table.id.display_ref(), table);
    }

    /// Record that an activation profile references a bridge (blocks delete).
    pub fn note_activation(&mut self, bridge: &BridgeSetId) {
        self.activation_subjects.insert(bridge.display_ref());
    }

    /// Validate a document without mutating authority or staging.
    pub fn validate_document(&self, doc: &BridgeDocument) -> Result<(), CompositionError> {
        let modules = self.module_tables();
        validate_bridge_document(doc, &modules, self.diag_limit)
    }

    fn require_authoritative(&self, doc: &BridgeDocument) -> Result<(), CompositionError> {
        if doc
            .assertions
            .iter()
            .any(|assertion| !assertion.provenance.method.is_authoritative())
        {
            return Err(CompositionError::one(CompositionDiagnostic::with_subjects(
                DiagnosticCode::LifecycleInvalidTransition,
                "suggested/inferred mappings remain non-authoritative until rewritten as authored",
                vec![doc.bridge_id.clone()],
                self.diag_limit,
            )));
        }
        Ok(())
    }

    /// Create/register a validated authored bridge into session staging.
    pub fn create_register(
        &mut self,
        doc: BridgeDocument,
        operation_id: impl Into<String>,
    ) -> Result<BridgeSetId, CompositionError> {
        let _operation_id = operation_id.into();
        self.validate_document(&doc)?;
        // Suggested/inferred-only documents may stage as candidates but create_register
        // requires authored assertions for the validated path.
        self.require_authoritative(&doc)?;
        let id = self.identity_for(&doc)?;
        let key = id.display_ref();
        if self.adopted.contains_key(&key) {
            return Err(CompositionError::one(CompositionDiagnostic::with_subjects(
                DiagnosticCode::InventoryDuplicate,
                "bridge identity already adopted",
                vec![id.display_ref()],
                self.diag_limit,
            )));
        }
        self.staging.insert(
            key,
            BridgeRecord {
                id: id.clone(),
                status: BridgeLifecycleStatus::Validated,
                dependencies: doc.dependencies.clone(),
                doc,
            },
        );
        Ok(id)
    }

    /// Import YAML/JSON as a non-authoritative staged candidate.
    pub fn import_text(
        &mut self,
        text: &str,
        format_hint: BridgeImportFormatHint,
        operation_id: impl Into<String>,
    ) -> Result<BridgeSetId, CompositionError> {
        let _operation_id = operation_id.into();
        let doc = parse_document(text, format_hint, self.diag_limit)?;
        // Import stages even when suggested; full validation still applies to structure.
        self.validate_document(&doc)?;
        let id = self.identity_for(&doc)?;
        let key = id.display_ref();
        self.staging.insert(
            key,
            BridgeRecord {
                id: id.clone(),
                status: BridgeLifecycleStatus::Candidate,
                dependencies: doc.dependencies.clone(),
                doc,
            },
        );
        Ok(id)
    }

    /// Explicitly adopt a staged bridge into durable authority.
    pub fn adopt(
        &mut self,
        selector: &BridgeSelector,
        source_generation: u64,
        operation_id: impl Into<String>,
    ) -> Result<BridgeMutationReceipt, CompositionError> {
        let operation_id = operation_id.into();
        if let Some(prior) = self.receipts.get(&operation_id) {
            return Ok(replay(prior));
        }
        self.require_generation(source_generation)?;
        let staged = self.take_staged(selector)?;
        if staged.status != BridgeLifecycleStatus::Validated
            && staged.status != BridgeLifecycleStatus::Candidate
        {
            return Err(CompositionError::one(CompositionDiagnostic::with_subjects(
                DiagnosticCode::LifecycleInvalidTransition,
                format!("cannot adopt bridge in status {}", staged.status.as_str()),
                vec![staged.id.display_ref()],
                self.diag_limit,
            )));
        }
        self.validate_document(&staged.doc)?;
        // Adoption requires authored mappings (suggested stay non-authoritative).
        self.require_authoritative(&staged.doc)?;
        let key = staged.id.display_ref();
        if self.adopted.contains_key(&key) {
            return Err(CompositionError::one(CompositionDiagnostic::with_subjects(
                DiagnosticCode::InventoryDuplicate,
                "bridge already adopted",
                vec![staged.id.display_ref()],
                self.diag_limit,
            )));
        }
        self.require_bridge_dependencies(&staged.dependencies)?;
        let mut record = staged;
        record.status = BridgeLifecycleStatus::Adopted;
        let prior = self.generation;
        let digest = record.id.canonical_digest.clone();
        let id = record.id.clone();
        self.adopted.insert(key, record);
        self.publish(prior, Some(id), Some(digest), operation_id)
    }

    /// List adopted bridges in deterministic identity order.
    #[must_use]
    pub fn list(&self) -> Vec<BridgeListEntry> {
        let mut entries: Vec<BridgeListEntry> = self
            .adopted
            .values()
            .filter(|r| r.status == BridgeLifecycleStatus::Adopted)
            .map(|r| self.to_list_entry(r))
            .collect();
        entries.sort_by_key(|e| e.id.sort_key());
        entries
    }

    /// Get/inspect one exact adopted bridge.
    pub fn inspect(&self, selector: &BridgeSelector) -> Result<BridgeInspect, CompositionError> {
        let record = self.resolve_adopted(selector)?;
        Ok(BridgeInspect {
            entry: self.to_list_entry(record),
            doc: record.doc.clone(),
            generation: self.generation,
        })
    }

    /// Preview replacing an adopted bridge with a new document version.
    pub fn preview_update(
        &self,
        selector: &BridgeSelector,
        next_doc: &BridgeDocument,
    ) -> Result<BridgeUpdatePreview, CompositionError> {
        let prior = self.resolve_adopted(selector)?;
        let document_valid = self.validate_document(next_doc).is_ok();
        let digest = bridge_document_digest(next_doc).unwrap_or_default();
        let next = BridgeSetId {
            bridge_id: next_doc.bridge_id.clone(),
            authored_version: next_doc.authored_version.clone(),
            canonical_digest: digest,
        };
        Ok(BridgeUpdatePreview {
            source_generation: self.generation,
            prior: prior.id.clone(),
            next,
            affected_dependants: self.dependants_of(&prior.id),
            document_valid,
        })
    }

    /// Atomically replace one adopted bridge version.
    pub fn update(
        &mut self,
        selector: &BridgeSelector,
        next_doc: BridgeDocument,
        source_generation: u64,
        operation_id: impl Into<String>,
    ) -> Result<BridgeMutationReceipt, CompositionError> {
        let operation_id = operation_id.into();
        if let Some(prior) = self.receipts.get(&operation_id) {
            return Ok(replay(prior));
        }
        self.require_generation(source_generation)?;
        let preview = self.preview_update(selector, &next_doc)?;
        if !preview.document_valid {
            self.validate_document(&next_doc)?;
        }
        self.require_authoritative(&next_doc)?;
        if preview.next.bridge_id != preview.prior.bridge_id {
            return Err(CompositionError::one(CompositionDiagnostic::with_subjects(
                DiagnosticCode::CollisionMetadata,
                "update replacement must retain the same bridge_id",
                vec![preview.prior.display_ref(), preview.next.display_ref()],
                self.diag_limit,
            )));
        }
        self.require_bridge_dependencies(&next_doc.dependencies)?;
        let prior_key = preview.prior.display_ref();
        let Some(mut prior_record) = self.adopted.remove(&prior_key) else {
            return Err(CompositionError::one(CompositionDiagnostic::with_subjects(
                DiagnosticCode::InventoryNotFound,
                "bridge disappeared during update",
                vec![preview.prior.display_ref()],
                self.diag_limit,
            )));
        };
        prior_record.status = BridgeLifecycleStatus::Superseded;

        let next_record = BridgeRecord {
            id: preview.next.clone(),
            status: BridgeLifecycleStatus::Adopted,
            dependencies: next_doc.dependencies.clone(),
            doc: next_doc,
        };
        for dep_key in self.adopted.keys().cloned().collect::<Vec<_>>() {
            if let Some(dep) = self.adopted.get_mut(&dep_key) {
                for edge in &mut dep.dependencies {
                    if edge == &preview.prior {
                        *edge = preview.next.clone();
                    }
                }
            }
        }
        // Rewrite activation subjects that pointed at the prior identity.
        if self
            .activation_subjects
            .remove(&preview.prior.display_ref())
        {
            self.activation_subjects.insert(preview.next.display_ref());
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
        selector: &BridgeSelector,
    ) -> Result<BridgeDeletePreview, CompositionError> {
        let target = self.resolve_adopted(selector)?;
        let dependent_bridges = self.dependants_of(&target.id);
        let activation_refs: Vec<String> = self
            .activation_subjects
            .iter()
            .filter(|s| *s == &target.id.display_ref())
            .cloned()
            .collect();
        let safe = dependent_bridges.is_empty() && activation_refs.is_empty();
        Ok(BridgeDeletePreview {
            source_generation: self.generation,
            target: target.id.clone(),
            dependent_bridges,
            activation_refs,
            safe,
        })
    }

    /// Atomically remove an adopted bridge when safe.
    pub fn delete(
        &mut self,
        selector: &BridgeSelector,
        source_generation: u64,
        operation_id: impl Into<String>,
    ) -> Result<BridgeMutationReceipt, CompositionError> {
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
                    .dependent_bridges
                    .iter()
                    .map(BridgeSetId::display_ref),
            );
            return Err(CompositionError::one(CompositionDiagnostic::with_subjects(
                DiagnosticCode::DependencyInUse,
                "bridge is referenced by dependants or activation; remove those first",
                subjects,
                self.diag_limit,
            )));
        }
        let key = preview.target.display_ref();
        let Some(mut record) = self.adopted.remove(&key) else {
            return Err(CompositionError::one(CompositionDiagnostic::with_subjects(
                DiagnosticCode::InventoryNotFound,
                "bridge not found for delete",
                vec![preview.target.display_ref()],
                self.diag_limit,
            )));
        };
        record.status = BridgeLifecycleStatus::Removed;
        let digest = record.id.canonical_digest.clone();
        let id = record.id.clone();
        let prior = self.generation;
        self.activation_subjects.retain(|s| s != &id.display_ref());
        self.publish(prior, Some(id), Some(digest), operation_id)
    }

    /// Deterministically export one adopted bridge as YAML or JSON.
    pub fn export_bridge(
        &self,
        selector: &BridgeSelector,
        format: BridgeExportFormat,
    ) -> Result<String, CompositionError> {
        let record = self.resolve_adopted(selector)?;
        match format {
            BridgeExportFormat::Json => {
                let value = serde_json::to_value(&record.doc).map_err(|e| {
                    CompositionError::one(CompositionDiagnostic::with_subjects(
                        DiagnosticCode::InterchangeIntegrity,
                        format!("json encode failed: {e}"),
                        vec![record.id.display_ref()],
                        self.diag_limit,
                    ))
                })?;
                serde_json::to_string_pretty(&value).map_err(|e| {
                    CompositionError::one(CompositionDiagnostic::with_subjects(
                        DiagnosticCode::InterchangeIntegrity,
                        format!("json pretty failed: {e}"),
                        vec![record.id.display_ref()],
                        self.diag_limit,
                    ))
                })
            }
            BridgeExportFormat::Yaml => serde_yaml::to_string(&record.doc).map_err(|e| {
                CompositionError::one(CompositionDiagnostic::with_subjects(
                    DiagnosticCode::InterchangeIntegrity,
                    format!("yaml encode failed: {e}"),
                    vec![record.id.display_ref()],
                    self.diag_limit,
                ))
            }),
        }
    }

    /// Adopted bridge identities in deterministic order (for composition closure).
    #[must_use]
    pub fn adopted_ids(&self) -> Vec<BridgeSetId> {
        let mut ids: Vec<BridgeSetId> = self
            .adopted
            .values()
            .filter(|r| r.status == BridgeLifecycleStatus::Adopted)
            .map(|r| r.id.clone())
            .collect();
        ids.sort_by_key(BridgeSetId::sort_key);
        ids
    }

    /// Adopted bridge documents in deterministic identity order.
    ///
    /// Consumers use these immutable documents to compile binding behavior;
    /// candidate and validated staging records are deliberately excluded.
    #[must_use]
    pub fn adopted_documents(&self) -> Vec<BridgeDocument> {
        let mut records: Vec<_> = self
            .adopted
            .values()
            .filter(|record| record.status == BridgeLifecycleStatus::Adopted)
            .collect();
        records.sort_by_key(|record| record.id.sort_key());
        records
            .into_iter()
            .map(|record| record.doc.clone())
            .collect()
    }

    /// Serialize durable authority (staging excluded).
    #[must_use]
    pub fn snapshot(&self) -> BridgeSnapshot {
        let mut adopted: Vec<SnapshotBridge> = self
            .adopted
            .values()
            .filter(|r| r.status == BridgeLifecycleStatus::Adopted)
            .map(|r| SnapshotBridge {
                id: r.id.clone(),
                dependencies: r.dependencies.clone(),
                doc: r.doc.clone(),
            })
            .collect();
        adopted.sort_by_key(|b| b.id.sort_key());
        let mut modules: Vec<SnapshotModuleSymbols> = self
            .modules
            .values()
            .map(|m| {
                let mut entities: Vec<_> = m.entities.iter().cloned().collect();
                let mut relations: Vec<_> = m.relations.iter().cloned().collect();
                let mut properties: Vec<_> = m.properties.iter().cloned().collect();
                entities.sort();
                relations.sort();
                properties.sort();
                SnapshotModuleSymbols {
                    id: m.id.clone(),
                    entities,
                    relations,
                    properties,
                }
            })
            .collect();
        modules.sort_by_key(|m| m.id.sort_key());
        let mut activation_subjects: Vec<_> = self.activation_subjects.iter().cloned().collect();
        activation_subjects.sort();
        let mut receipts: Vec<_> = self.receipts.values().cloned().collect();
        receipts.sort_by(|a, b| a.operation_id.cmp(&b.operation_id));
        BridgeSnapshot {
            schema_version: 1,
            generation: self.generation,
            profile_default: self.profile_default,
            activation_subjects,
            modules,
            adopted,
            receipts,
        }
    }

    /// Reopen durable authority from a snapshot (staging starts empty).
    pub fn reopen(snapshot: BridgeSnapshot) -> Result<Self, CompositionError> {
        if snapshot.schema_version != 1 {
            return Err(CompositionError::one(CompositionDiagnostic::with_subjects(
                DiagnosticCode::InterchangeIntegrity,
                format!(
                    "unsupported bridge snapshot version {}",
                    snapshot.schema_version
                ),
                Vec::new(),
                DiagnosticLimit::default(),
            )));
        }
        let mut inv = Self::new(snapshot.profile_default, DiagnosticLimit::default());
        inv.generation = snapshot.generation;
        inv.activation_subjects = snapshot.activation_subjects.into_iter().collect();
        for module in snapshot.modules {
            inv.register_module(ModuleSymbolTable {
                id: module.id,
                entities: module.entities.into_iter().collect(),
                relations: module.relations.into_iter().collect(),
                properties: module.properties.into_iter().collect(),
            });
        }
        for bridge in snapshot.adopted {
            inv.validate_document(&bridge.doc)?;
            inv.require_authoritative(&bridge.doc)?;
            let computed_id = inv.identity_for(&bridge.doc)?;
            if bridge.id != computed_id || bridge.dependencies != bridge.doc.dependencies {
                return Err(CompositionError::one(CompositionDiagnostic::with_subjects(
                    DiagnosticCode::InterchangeIntegrity,
                    "snapshot bridge identity or dependency projection does not match its document",
                    vec![bridge.id.display_ref(), computed_id.display_ref()],
                    inv.diag_limit,
                )));
            }
            inv.adopted.insert(
                bridge.id.display_ref(),
                BridgeRecord {
                    id: bridge.id,
                    status: BridgeLifecycleStatus::Adopted,
                    dependencies: bridge.dependencies,
                    doc: bridge.doc,
                },
            );
        }
        for receipt in snapshot.receipts {
            inv.receipts.insert(receipt.operation_id.clone(), receipt);
        }
        // Re-validate the adopted dependency closure after every identity is loaded.
        for record in inv.adopted.values() {
            inv.require_bridge_dependencies(&record.dependencies)?;
        }
        Ok(inv)
    }

    fn identity_for(&self, doc: &BridgeDocument) -> Result<BridgeSetId, CompositionError> {
        let digest = bridge_document_digest(doc).map_err(|e| {
            CompositionError::one(CompositionDiagnostic::with_subjects(
                DiagnosticCode::InterchangeIntegrity,
                format!("failed to digest bridge document: {e}"),
                vec![doc.bridge_id.clone()],
                self.diag_limit,
            ))
        })?;
        Ok(BridgeSetId {
            bridge_id: doc.bridge_id.clone(),
            authored_version: doc.authored_version.clone(),
            canonical_digest: digest,
        })
    }

    fn module_tables(&self) -> Vec<ModuleSymbolTable> {
        self.modules.values().cloned().collect()
    }

    fn publish(
        &mut self,
        prior_generation: u64,
        affected: Option<BridgeSetId>,
        digest: Option<String>,
        operation_id: String,
    ) -> Result<BridgeMutationReceipt, CompositionError> {
        self.generation = prior_generation.checked_add(1).ok_or_else(|| {
            CompositionError::one(CompositionDiagnostic::with_subjects(
                DiagnosticCode::ResourceDiagnostics,
                "generation counter overflow",
                Vec::new(),
                self.diag_limit,
            ))
        })?;
        let receipt = BridgeMutationReceipt {
            operation_id: operation_id.clone(),
            prior_generation,
            new_generation: self.generation,
            affected_bridge: affected,
            digest,
            idempotent_replay: false,
        };
        self.receipts.insert(operation_id, receipt.clone());
        Ok(receipt)
    }

    fn dependants_of(&self, target: &BridgeSetId) -> Vec<BridgeSetId> {
        let mut deps: Vec<BridgeSetId> = self
            .adopted
            .values()
            .filter(|r| r.status == BridgeLifecycleStatus::Adopted)
            .filter(|r| r.dependencies.iter().any(|d| d == target))
            .map(|r| r.id.clone())
            .collect();
        deps.sort_by_key(BridgeSetId::sort_key);
        deps
    }

    fn require_bridge_dependencies(
        &self,
        dependencies: &[BridgeSetId],
    ) -> Result<(), CompositionError> {
        for dep in dependencies {
            if !self
                .adopted
                .get(&dep.display_ref())
                .is_some_and(|r| r.status == BridgeLifecycleStatus::Adopted)
            {
                return Err(CompositionError::one(CompositionDiagnostic::with_subjects(
                    DiagnosticCode::DependencyMissing,
                    "required bridge dependency is not adopted",
                    vec![dep.display_ref()],
                    self.diag_limit,
                )));
            }
        }
        Ok(())
    }

    fn require_generation(&self, source: u64) -> Result<(), CompositionError> {
        if source != self.generation {
            return Err(CompositionError::one(CompositionDiagnostic::with_subjects(
                DiagnosticCode::InventoryGenerationConflict,
                format!(
                    "stale source generation {source}; current is {}",
                    self.generation
                ),
                vec![source.to_string(), self.generation.to_string()],
                self.diag_limit,
            )));
        }
        Ok(())
    }

    fn to_list_entry(&self, record: &BridgeRecord) -> BridgeListEntry {
        BridgeListEntry {
            id: record.id.clone(),
            status: record.status,
            enforcement: record.doc.enforcement.unwrap_or(self.profile_default),
            dependencies: record.dependencies.clone(),
            digest: record.id.canonical_digest.clone(),
        }
    }

    fn resolve_adopted<'a>(
        &'a self,
        selector: &BridgeSelector,
    ) -> Result<&'a BridgeRecord, CompositionError> {
        match selector {
            BridgeSelector::Exact(id) => self
                .adopted
                .get(&id.display_ref())
                .filter(|r| r.status == BridgeLifecycleStatus::Adopted)
                .ok_or_else(|| {
                    CompositionError::one(CompositionDiagnostic::with_subjects(
                        DiagnosticCode::InventoryNotFound,
                        "bridge not found in durable inventory",
                        vec![id.display_ref()],
                        self.diag_limit,
                    ))
                }),
            BridgeSelector::BridgeId(bridge_id) => {
                let matches: Vec<_> = self
                    .adopted
                    .values()
                    .filter(|r| {
                        r.status == BridgeLifecycleStatus::Adopted && r.id.bridge_id == *bridge_id
                    })
                    .collect();
                match matches.as_slice() {
                    [one] => Ok(*one),
                    [] => Err(CompositionError::one(CompositionDiagnostic::with_subjects(
                        DiagnosticCode::InventoryNotFound,
                        "no adopted bridge matches bridge_id",
                        vec![bridge_id.clone()],
                        self.diag_limit,
                    ))),
                    many => Err(CompositionError::one(CompositionDiagnostic::new(
                        DiagnosticCode::ResolutionAmbiguous,
                        "bridge_id selects more than one adopted bridge",
                        vec![bridge_id.clone()],
                        many.iter().map(|r| r.id.display_ref()).collect(),
                        self.diag_limit,
                    ))),
                }
            }
        }
    }

    fn take_staged(&mut self, selector: &BridgeSelector) -> Result<BridgeRecord, CompositionError> {
        let key = match selector {
            BridgeSelector::Exact(id) => id.display_ref(),
            BridgeSelector::BridgeId(bridge_id) => {
                let matches: Vec<_> = self
                    .staging
                    .values()
                    .filter(|r| r.id.bridge_id == *bridge_id)
                    .map(|r| r.id.display_ref())
                    .collect();
                match matches.as_slice() {
                    [one] => one.clone(),
                    [] => {
                        return Err(CompositionError::one(CompositionDiagnostic::with_subjects(
                            DiagnosticCode::InventoryNotFound,
                            "no staged bridge matches bridge_id",
                            vec![bridge_id.clone()],
                            self.diag_limit,
                        )));
                    }
                    many => {
                        return Err(CompositionError::one(CompositionDiagnostic::new(
                            DiagnosticCode::ResolutionAmbiguous,
                            "bridge_id selects more than one staged bridge",
                            vec![bridge_id.clone()],
                            many.to_vec(),
                            self.diag_limit,
                        )));
                    }
                }
            }
        };
        self.staging.remove(&key).ok_or_else(|| {
            CompositionError::one(CompositionDiagnostic::with_subjects(
                DiagnosticCode::InventoryNotFound,
                "staged bridge not found",
                vec![key],
                self.diag_limit,
            ))
        })
    }
}

fn replay(prior: &BridgeMutationReceipt) -> BridgeMutationReceipt {
    let mut out = prior.clone();
    out.idempotent_replay = true;
    out
}

fn parse_document(
    text: &str,
    format_hint: BridgeImportFormatHint,
    limit: DiagnosticLimit,
) -> Result<BridgeDocument, CompositionError> {
    let trimmed = text.trim_start();
    let as_json = match format_hint {
        BridgeImportFormatHint::Json => true,
        BridgeImportFormatHint::Yaml => false,
        BridgeImportFormatHint::Auto => trimmed.starts_with('{'),
    };
    if as_json {
        serde_json::from_str(text).map_err(|e| {
            CompositionError::one(CompositionDiagnostic::with_subjects(
                DiagnosticCode::InventoryMalformed,
                format!("bridge json parse failed: {e}"),
                Vec::new(),
                limit,
            ))
        })
    } else {
        serde_yaml::from_str(text).map_err(|e| {
            CompositionError::one(CompositionDiagnostic::with_subjects(
                DiagnosticCode::InventoryMalformed,
                format!("bridge yaml parse failed: {e}"),
                Vec::new(),
                limit,
            ))
        })
    }
}
