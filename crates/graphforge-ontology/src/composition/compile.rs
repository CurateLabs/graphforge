//! Inventory closure compilation and composition fingerprinting.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};

use serde_json::{Map, Value, json};
use unicode_normalization::UnicodeNormalization;

use crate::compiler::{OntologyCompiler, OntologyRuntime};
use crate::ontology::OntologyDoc;
use crate::validator::OntologyValidator;

use super::canonical::{domain_digest, module_document_digest};
use super::diagnostic::{CompositionDiagnostic, CompositionError, DiagnosticCode, DiagnosticLimit};
use super::identity::{
    ActivationMode, ActivationRecord, BridgeSetId, COMPOSITION_DOMAIN, DIGEST_HEX_LEN,
    OntologyModuleId, QualifiedSymbol, SymbolKind,
};

/// Finite maxima for composition (adversarial fixture defaults).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompositionLimits {
    /// Maximum modules in the inventory / closure.
    pub modules: usize,
    /// Maximum bridge identities.
    pub bridges: usize,
    /// Maximum symbols across all modules.
    pub symbols: usize,
    /// Maximum dependency edges traversed.
    pub dependency_edges: usize,
    /// Maximum diagnostics retained on multi-error collection.
    pub diagnostics: usize,
    /// Maximum candidates/subjects per diagnostic.
    pub diagnostic_candidates: usize,
}

impl Default for CompositionLimits {
    fn default() -> Self {
        Self {
            modules: 10_000,
            bridges: 10_000,
            symbols: 10_000_000,
            dependency_edges: 1_000_000,
            diagnostics: 1_000,
            diagnostic_candidates: 64,
        }
    }
}

impl CompositionLimits {
    fn diagnostic_limit(self) -> DiagnosticLimit {
        DiagnosticLimit {
            max_candidates: self.diagnostic_candidates.max(1),
        }
    }
}

/// One authored module presented for composition.
#[derive(Debug, Clone, PartialEq)]
pub struct AuthoredModule {
    /// Exact identity the caller claims for this document.
    pub id: OntologyModuleId,
    /// Required exact module dependencies.
    pub dependencies: Vec<OntologyModuleId>,
    /// Validated independently before composition.
    pub doc: OntologyDoc,
    /// When true, `id` may use a projected `legacy:<digest>` identity while the
    /// document retains its authored ontology_id/version (M9 migration path).
    pub allow_projected_identity: bool,
}

/// Request to compile an inventory into a deterministic composition.
#[derive(Debug, Clone, Copy)]
pub struct InventoryCompileRequest<'a> {
    /// Authored modules (order MUST NOT affect semantics).
    pub modules: &'a [AuthoredModule],
    /// Bridge identities participating in the fingerprint (#838 owns lifecycle).
    pub bridges: &'a [BridgeSetId],
    /// Scoped activation overrides.
    pub activation: &'a [ActivationRecord],
    /// Default enforcement when no override matches.
    pub profile_default: ActivationMode,
    /// Finite resource maxima.
    pub limits: CompositionLimits,
    /// Optional cooperative cancellation flag.
    pub cancelled: Option<&'a AtomicBool>,
}

/// One closed, compiled module retaining source authority (not flattened).
pub struct CompiledModule {
    /// Exact module identity.
    pub id: OntologyModuleId,
    /// Exact dependencies (identity-sorted).
    pub dependencies: Vec<OntologyModuleId>,
    /// Source document (authority preserved).
    pub doc: OntologyDoc,
    /// Per-module Arrow runtime / lookups (IDs are runtime-local, never semantic).
    pub runtime: OntologyRuntime,
    /// Symbols declared by this module.
    pub symbols: Vec<QualifiedSymbol>,
}

/// Deterministic compiled composition.
pub struct CompiledComposition {
    /// Closure-ordered modules (identity sort).
    pub modules: Vec<CompiledModule>,
    /// Identity-sorted bridge identities.
    pub bridges: Vec<BridgeSetId>,
    /// Identity-sorted activation records.
    pub activation: Vec<ActivationRecord>,
    /// Default enforcement mode.
    pub profile_default: ActivationMode,
    /// Contract composition fingerprint (lowercase hex SHA-256).
    pub fingerprint: String,
    /// `(kind, local_id)` → modules declaring that unqualified name.
    pub(crate) unqualified_index: HashMap<(SymbolKind, String), Vec<OntologyModuleId>>,
    /// Fully qualified lookup: module display_ref + kind + local_id.
    pub(crate) qualified_index: HashMap<(String, SymbolKind, String), QualifiedSymbol>,
}

impl std::fmt::Debug for CompiledComposition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompiledComposition")
            .field("module_count", &self.modules.len())
            .field("bridges", &self.bridges)
            .field("activation", &self.activation)
            .field("profile_default", &self.profile_default)
            .field("fingerprint", &self.fingerprint)
            .finish_non_exhaustive()
    }
}

/// Compile an inventory into a deterministic composition.
///
/// # Errors
/// Returns [`CompositionError`] when validation, identity, dependency, limit, or
/// cancellation checks fail. Failures never mutate project authority (this API
/// is pure).
pub fn compile_inventory(
    request: InventoryCompileRequest<'_>,
) -> Result<CompiledComposition, CompositionError> {
    let limits = request.limits;
    let dlimit = limits.diagnostic_limit();
    check_cancelled(request.cancelled, dlimit)?;

    if request.modules.len() > limits.modules {
        return Err(CompositionError::one(CompositionDiagnostic::with_subjects(
            DiagnosticCode::ResourceModules,
            format!(
                "module count {} exceeds limit {}",
                request.modules.len(),
                limits.modules
            ),
            vec![format!("count={}", request.modules.len())],
            dlimit,
        )));
    }
    if request.bridges.len() > limits.bridges {
        return Err(CompositionError::one(CompositionDiagnostic::with_subjects(
            DiagnosticCode::ResourceBridges,
            format!(
                "bridge count {} exceeds limit {}",
                request.bridges.len(),
                limits.bridges
            ),
            vec![format!("count={}", request.bridges.len())],
            dlimit,
        )));
    }

    let mut by_id: HashMap<OntologyModuleId, &AuthoredModule> = HashMap::new();
    for module in request.modules {
        validate_module_identity(&module.id, dlimit)?;
        for dep in &module.dependencies {
            validate_module_identity(dep, dlimit)?;
        }
        check_cancelled(request.cancelled, dlimit)?;

        if let Err(errors) = OntologyValidator::validate(&module.doc) {
            return Err(CompositionError::one(CompositionDiagnostic::for_module(
                DiagnosticCode::InventoryMalformed,
                format!(
                    "module failed independent validation ({} error(s))",
                    errors.len()
                ),
                &module.id,
                dlimit,
            )));
        }

        let computed = module_document_digest(&module.doc).map_err(|e| {
            CompositionError::one(CompositionDiagnostic::for_module(
                DiagnosticCode::InterchangeIntegrity,
                format!("failed to digest module document: {e}"),
                &module.id,
                dlimit,
            ))
        })?;
        if !digests_equal(&computed, &module.id.canonical_digest) {
            return Err(CompositionError::one(CompositionDiagnostic::for_module(
                DiagnosticCode::InterchangeIntegrity,
                "declared canonical_digest does not match module document",
                &module.id,
                dlimit,
            )));
        }

        if !module.allow_projected_identity {
            if module.doc.ontology_id != module.id.ontology_id {
                return Err(CompositionError::one(CompositionDiagnostic::for_module(
                    DiagnosticCode::CollisionMetadata,
                    "document ontology_id does not match module identity ontology_id",
                    &module.id,
                    dlimit,
                )));
            }
            if module.doc.version != module.id.authored_version {
                return Err(CompositionError::one(CompositionDiagnostic::for_module(
                    DiagnosticCode::CollisionMetadata,
                    "document version does not match module identity authored_version",
                    &module.id,
                    dlimit,
                )));
            }
        }

        if let Some(prior) = by_id.insert(module.id.clone(), module) {
            if prior.id.canonical_digest != module.id.canonical_digest {
                return Err(CompositionError::one(CompositionDiagnostic::with_subjects(
                    DiagnosticCode::InventoryDuplicate,
                    "duplicate module identity with conflicting digests",
                    vec![module.id.display_ref(), prior.id.display_ref()],
                    dlimit,
                )));
            }
            return Err(CompositionError::one(CompositionDiagnostic::for_module(
                DiagnosticCode::InventoryDuplicate,
                "duplicate module identity in inventory",
                &module.id,
                dlimit,
            )));
        }
    }

    for bridge in request.bridges {
        validate_bridge_identity(bridge, dlimit)?;
    }
    let mut bridge_seen = HashSet::new();
    for bridge in request.bridges {
        if !bridge_seen.insert(bridge.clone()) {
            return Err(CompositionError::one(CompositionDiagnostic::with_subjects(
                DiagnosticCode::InventoryDuplicate,
                "duplicate bridge identity in inventory",
                vec![format!(
                    "{}@{}#{}",
                    bridge.bridge_id, bridge.authored_version, bridge.canonical_digest
                )],
                dlimit,
            )));
        }
    }

    check_cancelled(request.cancelled, dlimit)?;

    // Closure over all inventory roots (every registered module is a root).
    let roots: Vec<OntologyModuleId> = by_id.keys().cloned().collect();
    let (closure_ids, _edge_count) =
        compute_closure(&roots, &by_id, limits, request.cancelled, dlimit)?;

    let mut compiled_modules = Vec::with_capacity(closure_ids.len());
    let mut unqualified_index: HashMap<(SymbolKind, String), Vec<OntologyModuleId>> =
        HashMap::new();
    let mut qualified_index: HashMap<(String, SymbolKind, String), QualifiedSymbol> =
        HashMap::new();
    let mut symbol_count = 0usize;

    for id in &closure_ids {
        check_cancelled(request.cancelled, dlimit)?;
        let authored = by_id.get(id).ok_or_else(|| {
            CompositionError::one(CompositionDiagnostic::for_module(
                DiagnosticCode::InventoryNotFound,
                "closure references a module absent from the inventory",
                id,
                dlimit,
            ))
        })?;

        let runtime = OntologyCompiler::compile(&authored.doc).map_err(|e| {
            CompositionError::one(CompositionDiagnostic::for_module(
                DiagnosticCode::InventoryMalformed,
                format!("failed to compile module runtime tables: {e}"),
                id,
                dlimit,
            ))
        })?;

        let symbols = extract_symbols(id, &authored.doc);
        symbol_count = symbol_count.saturating_add(symbols.len());
        if symbol_count > limits.symbols {
            return Err(CompositionError::one(CompositionDiagnostic::with_subjects(
                DiagnosticCode::ResourceSymbols,
                format!("symbol count exceeds limit {}", limits.symbols),
                vec![format!("count={symbol_count}")],
                dlimit,
            )));
        }

        for symbol in &symbols {
            let key = (symbol.kind, symbol.local_id.clone());
            unqualified_index.entry(key).or_default().push(id.clone());

            let qkey = (id.display_ref(), symbol.kind, symbol.local_id.clone());
            if qualified_index.contains_key(&qkey) {
                return Err(CompositionError::one(CompositionDiagnostic::with_subjects(
                    DiagnosticCode::CollisionQualifiedDuplicate,
                    format!(
                        "duplicate qualified symbol {}:{} in module",
                        symbol.kind.as_str(),
                        symbol.local_id
                    ),
                    vec![symbol.display()],
                    dlimit,
                )));
            }
            qualified_index.insert(qkey, symbol.clone());
        }

        let mut deps = authored.dependencies.clone();
        deps.sort_by(|a, b| a.sort_key().cmp(&b.sort_key()));
        deps.dedup();

        compiled_modules.push(CompiledModule {
            id: id.clone(),
            dependencies: deps,
            doc: authored.doc.clone(),
            runtime,
            symbols,
        });
    }

    for candidates in unqualified_index.values_mut() {
        candidates.sort_by(|a, b| a.sort_key().cmp(&b.sort_key()));
        candidates.dedup();
    }

    let mut bridges = request.bridges.to_vec();
    bridges.sort_by(|a, b| a.sort_key().cmp(&b.sort_key()));

    let mut activation = request.activation.to_vec();
    for record in &activation {
        require_nfc(&record.subject, "activation.subject", dlimit)?;
    }
    activation.sort_by(|a, b| a.sort_key().cmp(&b.sort_key()));

    let fingerprint = composition_fingerprint(&closure_ids, &bridges, &activation)?;

    Ok(CompiledComposition {
        modules: compiled_modules,
        bridges,
        activation,
        profile_default: request.profile_default,
        fingerprint,
        unqualified_index,
        qualified_index,
    })
}

/// Project a legacy single-ontology document into a one-module composition.
///
/// Uses a deterministic `legacy:<digest>` ontology ID and authored version
/// `legacy-v1` only when `publish_m9_identity` is true; otherwise keeps the
/// document's own `ontology_id` / `version` as the module identity fields while
/// still computing the canonical digest over the document.
pub fn compile_legacy_single_ontology(
    doc: &OntologyDoc,
    publish_m9_identity: bool,
    limits: CompositionLimits,
) -> Result<CompiledComposition, CompositionError> {
    let dlimit = limits.diagnostic_limit();
    if let Err(errors) = OntologyValidator::validate(doc) {
        return Err(CompositionError::one(CompositionDiagnostic::with_subjects(
            DiagnosticCode::InventoryMalformed,
            format!(
                "legacy ontology failed validation ({} error(s))",
                errors.len()
            ),
            vec![doc.ontology_id.clone()],
            dlimit,
        )));
    }
    let digest = module_document_digest(doc).map_err(|e| {
        CompositionError::one(CompositionDiagnostic::with_subjects(
            DiagnosticCode::InterchangeIntegrity,
            format!("failed to digest legacy ontology: {e}"),
            vec![doc.ontology_id.clone()],
            dlimit,
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

    let authored = AuthoredModule {
        id,
        dependencies: Vec::new(),
        doc: doc.clone(),
        allow_projected_identity: publish_m9_identity,
    };
    compile_inventory(InventoryCompileRequest {
        modules: &[authored],
        bridges: &[],
        activation: &[],
        profile_default: ActivationMode::Exploratory,
        limits,
        cancelled: None,
    })
}

fn composition_fingerprint(
    modules: &[OntologyModuleId],
    bridges: &[BridgeSetId],
    activation: &[ActivationRecord],
) -> Result<String, CompositionError> {
    let dlimit = DiagnosticLimit::default();

    let mut modules_sorted = modules.to_vec();
    modules_sorted.sort_by(|a, b| a.sort_key().cmp(&b.sort_key()));
    let mut bridges_sorted = bridges.to_vec();
    bridges_sorted.sort_by(|a, b| a.sort_key().cmp(&b.sort_key()));
    let mut activation_sorted = activation.to_vec();
    activation_sorted.sort_by(|a, b| a.sort_key().cmp(&b.sort_key()));

    let mut module_values = Vec::with_capacity(modules_sorted.len());
    for m in &modules_sorted {
        let mut obj = Map::new();
        obj.insert(
            "authored_version".into(),
            Value::String(m.authored_version.clone()),
        );
        obj.insert(
            "canonical_digest".into(),
            Value::String(m.canonical_digest.clone()),
        );
        obj.insert("ontology_id".into(), Value::String(m.ontology_id.clone()));
        module_values.push(Value::Object(obj));
    }

    let mut bridge_values = Vec::with_capacity(bridges_sorted.len());
    for b in &bridges_sorted {
        let mut obj = Map::new();
        obj.insert(
            "authored_version".into(),
            Value::String(b.authored_version.clone()),
        );
        obj.insert("bridge_id".into(), Value::String(b.bridge_id.clone()));
        obj.insert(
            "canonical_digest".into(),
            Value::String(b.canonical_digest.clone()),
        );
        bridge_values.push(Value::Object(obj));
    }

    let mut activation_values = Vec::with_capacity(activation_sorted.len());
    for a in &activation_sorted {
        let mut obj = Map::new();
        obj.insert("mode".into(), Value::String(a.mode.as_str().to_owned()));
        obj.insert("scope".into(), Value::String(a.scope.as_str().to_owned()));
        obj.insert("subject".into(), Value::String(a.subject.clone()));
        activation_values.push(Value::Object(obj));
    }

    let semantic = json!({
        "activation": activation_values,
        "bridges": bridge_values,
        "modules": module_values,
    });

    domain_digest(COMPOSITION_DOMAIN, &semantic).map_err(|e| {
        CompositionError::one(CompositionDiagnostic::with_subjects(
            DiagnosticCode::InterchangeIntegrity,
            format!("failed to compute composition fingerprint: {e}"),
            Vec::new(),
            dlimit,
        ))
    })
}

fn compute_closure(
    roots: &[OntologyModuleId],
    by_id: &HashMap<OntologyModuleId, &AuthoredModule>,
    limits: CompositionLimits,
    cancelled: Option<&AtomicBool>,
    dlimit: DiagnosticLimit,
) -> Result<(Vec<OntologyModuleId>, usize), CompositionError> {
    let mut visiting = HashSet::new();
    let mut visited = HashSet::new();
    let mut edge_count = 0usize;
    let mut stack_path: Vec<OntologyModuleId> = Vec::new();

    fn dfs(
        id: &OntologyModuleId,
        by_id: &HashMap<OntologyModuleId, &AuthoredModule>,
        limits: CompositionLimits,
        cancelled: Option<&AtomicBool>,
        dlimit: DiagnosticLimit,
        visiting: &mut HashSet<OntologyModuleId>,
        visited: &mut HashSet<OntologyModuleId>,
        edge_count: &mut usize,
        stack_path: &mut Vec<OntologyModuleId>,
    ) -> Result<(), CompositionError> {
        check_cancelled(cancelled, dlimit)?;
        if visited.contains(id) {
            return Ok(());
        }
        if !visiting.insert(id.clone()) {
            let mut subjects: Vec<String> = stack_path
                .iter()
                .map(OntologyModuleId::display_ref)
                .collect();
            subjects.push(id.display_ref());
            return Err(CompositionError::one(CompositionDiagnostic::with_subjects(
                DiagnosticCode::DependencyCycle,
                "module dependency cycle is forbidden",
                subjects,
                dlimit,
            )));
        }
        stack_path.push(id.clone());

        let module = by_id.get(id).ok_or_else(|| {
            CompositionError::one(CompositionDiagnostic::for_module(
                DiagnosticCode::DependencyMissing,
                "required module dependency is missing from the inventory",
                id,
                dlimit,
            ))
        })?;

        for dep in &module.dependencies {
            *edge_count = edge_count.saturating_add(1);
            if *edge_count > limits.dependency_edges {
                return Err(CompositionError::one(CompositionDiagnostic::with_subjects(
                    DiagnosticCode::ResourceDiagnostics,
                    format!(
                        "dependency edge count exceeds limit {}",
                        limits.dependency_edges
                    ),
                    vec![format!("count={edge_count}")],
                    dlimit,
                )));
            }
            if dep == id {
                return Err(CompositionError::one(CompositionDiagnostic::for_module(
                    DiagnosticCode::DependencyCycle,
                    "module lists itself as a dependency",
                    id,
                    dlimit,
                )));
            }
            if !by_id.contains_key(dep) {
                return Err(CompositionError::one(CompositionDiagnostic::with_subjects(
                    DiagnosticCode::DependencyMissing,
                    "required exact dependency is not present in the inventory",
                    vec![id.display_ref(), dep.display_ref()],
                    dlimit,
                )));
            }
            dfs(
                dep, by_id, limits, cancelled, dlimit, visiting, visited, edge_count, stack_path,
            )?;
        }

        visiting.remove(id);
        stack_path.pop();
        visited.insert(id.clone());
        Ok(())
    }

    for root in roots {
        dfs(
            root,
            by_id,
            limits,
            cancelled,
            dlimit,
            &mut visiting,
            &mut visited,
            &mut edge_count,
            &mut stack_path,
        )?;
    }

    if visited.len() > limits.modules {
        return Err(CompositionError::one(CompositionDiagnostic::with_subjects(
            DiagnosticCode::ResourceModules,
            format!("closure module count exceeds limit {}", limits.modules),
            vec![format!("count={}", visited.len())],
            dlimit,
        )));
    }

    let mut ordered: Vec<OntologyModuleId> = visited.into_iter().collect();
    ordered.sort_by(|a, b| a.sort_key().cmp(&b.sort_key()));
    Ok((ordered, edge_count))
}

fn extract_symbols(module: &OntologyModuleId, doc: &OntologyDoc) -> Vec<QualifiedSymbol> {
    let mut symbols = Vec::new();
    for entity in &doc.entity_types {
        symbols.push(QualifiedSymbol {
            module: module.clone(),
            kind: SymbolKind::Entity,
            local_id: entity.name.clone(),
        });
    }
    for relation in &doc.relation_types {
        symbols.push(QualifiedSymbol {
            module: module.clone(),
            kind: SymbolKind::Relation,
            local_id: relation.name.clone(),
        });
    }
    for property in &doc.properties {
        // Properties are unique per (owner, name) within a module.
        symbols.push(QualifiedSymbol {
            module: module.clone(),
            kind: SymbolKind::Property,
            local_id: format!("{}:{}", property.owner, property.name),
        });
    }
    for (index, _constraint) in doc.constraints.iter().enumerate() {
        symbols.push(QualifiedSymbol {
            module: module.clone(),
            kind: SymbolKind::Constraint,
            local_id: format!("constraint:{index}"),
        });
    }
    for migration in &doc.migrations {
        symbols.push(QualifiedSymbol {
            module: module.clone(),
            kind: SymbolKind::Migration,
            local_id: format!("{}->{}", migration.from_version, migration.to_version),
        });
    }
    symbols
}

fn validate_module_identity(
    id: &OntologyModuleId,
    dlimit: DiagnosticLimit,
) -> Result<(), CompositionError> {
    require_nfc(&id.ontology_id, "ontology_id", dlimit)?;
    require_nfc(&id.authored_version, "authored_version", dlimit)?;
    require_digest(&id.canonical_digest, dlimit)?;
    // Runtime catalog IDs are never semantic: reject obvious numeric-only IDs.
    if id.ontology_id.chars().all(|c| c.is_ascii_digit()) {
        return Err(CompositionError::one(CompositionDiagnostic::with_subjects(
            DiagnosticCode::CollisionMetadata,
            "runtime catalog IDs must not appear in ontology module identity",
            vec![id.ontology_id.clone()],
            dlimit,
        )));
    }
    Ok(())
}

fn validate_bridge_identity(
    id: &BridgeSetId,
    dlimit: DiagnosticLimit,
) -> Result<(), CompositionError> {
    require_nfc(&id.bridge_id, "bridge_id", dlimit)?;
    require_nfc(&id.authored_version, "authored_version", dlimit)?;
    require_digest(&id.canonical_digest, dlimit)?;
    Ok(())
}

fn require_nfc(value: &str, field: &str, dlimit: DiagnosticLimit) -> Result<(), CompositionError> {
    let normalized: String = value.nfc().collect();
    if normalized != value {
        return Err(CompositionError::one(CompositionDiagnostic::with_subjects(
            DiagnosticCode::CollisionMetadata,
            format!("{field} must be NFC-normalized"),
            vec![field.to_owned()],
            dlimit,
        )));
    }
    Ok(())
}

fn require_digest(digest: &str, dlimit: DiagnosticLimit) -> Result<(), CompositionError> {
    if digest.len() != DIGEST_HEX_LEN || !digest.chars().all(|c| matches!(c, '0'..='9' | 'a'..='f'))
    {
        return Err(CompositionError::one(CompositionDiagnostic::with_subjects(
            DiagnosticCode::InterchangeIntegrity,
            "canonical_digest must be lowercase hex SHA-256",
            vec![format!("len={}", digest.len())],
            dlimit,
        )));
    }
    Ok(())
}

fn digests_equal(a: &str, b: &str) -> bool {
    a.as_bytes() == b.as_bytes()
}

fn check_cancelled(
    cancelled: Option<&AtomicBool>,
    dlimit: DiagnosticLimit,
) -> Result<(), CompositionError> {
    if cancelled.is_some_and(|flag| flag.load(Ordering::Relaxed)) {
        return Err(CompositionError::one(CompositionDiagnostic::with_subjects(
            DiagnosticCode::LifecycleCancelled,
            "composition cancelled before publication",
            Vec::new(),
            dlimit,
        )));
    }
    Ok(())
}
