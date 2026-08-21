//! Deterministic symbol binding over a compiled ontology composition.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use graphforge_ontology::{
    ActivationMode, BridgeDocument, CompiledComposition, OntologyModuleId, QualifiedSymbol,
    ResolveRequest, SymbolKind,
};
use serde::{Deserialize, Serialize};

/// Finite output limits for one binding explanation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompositionBindingLimits {
    /// Maximum ambiguity candidates retained.
    pub candidates: usize,
    /// Maximum bridge decisions retained.
    pub bridge_steps: usize,
}

impl Default for CompositionBindingLimits {
    fn default() -> Self {
        Self {
            candidates: 64,
            bridge_steps: 64,
        }
    }
}

/// Stable diagnostic code for composed binding failures and warnings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BindingDiagnosticCode {
    /// A qualified or strict symbol was not found.
    UnknownSymbol,
    /// An unqualified symbol has multiple valid owners.
    AmbiguousSymbol,
    /// A qualifier names no module or more than one module.
    InvalidQualifier,
    /// Multiple bridge paths produce conflicting outcomes.
    ConflictingBridgePaths,
    /// A declared property is used with an owner that does not declare it.
    WrongOwnerProperty,
    /// A bridge requires semantics this binder does not support.
    UnsupportedRequiredSemantics,
    /// Resolution continued through the runtime catalog.
    RuntimeFallback,
}

/// Stable, attributable composed-binding diagnostic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BindingDiagnostic {
    /// Machine-readable code.
    pub code: BindingDiagnosticCode,
    /// Exact module, bridge, or input subject.
    pub subject: String,
    /// Deterministically ordered bounded candidates.
    pub candidates: Vec<String>,
    /// Human remediation.
    pub remediation: String,
}

/// One deterministic decision in a binding explanation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "decision")]
pub enum BindingDecision {
    /// Exact qualified lookup.
    Qualified {
        /// Resolved exact symbol.
        symbol: QualifiedSymbol,
    },
    /// Unique unqualified lookup.
    Unique {
        /// Sole valid candidate.
        symbol: QualifiedSymbol,
    },
    /// Explicit bridge traversal.
    Bridge {
        /// Adopted bridge authority.
        bridge_id: String,
        /// Applied bounded predicate.
        predicate: String,
        /// Traversal source.
        source: QualifiedSymbol,
        /// Traversal target.
        target: QualifiedSymbol,
    },
    /// Progressive runtime-catalog fallback.
    RuntimeFallback {
        /// Runtime symbol kind.
        kind: SymbolKind,
        /// Observed local identifier.
        local_id: String,
    },
}

/// Bounded deterministic explanation returned with a successful binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BindingExplainReceipt {
    /// Exact composition identity carried into the plan.
    pub composition_fingerprint: String,
    /// Effective progressive policy.
    pub effective_mode: ActivationMode,
    /// Ordered decisions.
    pub decisions: Vec<BindingDecision>,
    /// Advisory diagnostics; empty for clean/strict success.
    pub diagnostics: Vec<BindingDiagnostic>,
}

/// Resolved semantic symbol, or a permitted runtime-catalog observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SymbolBinding {
    /// Exact composition-owned identity.
    Qualified(QualifiedSymbol),
    /// Runtime-local fallback; never confused with ontology identity.
    Runtime {
        /// Runtime symbol kind.
        kind: SymbolKind,
        /// Runtime-local identifier.
        local_id: String,
    },
}

/// Immutable consumer-neutral binding context.
#[derive(Clone)]
pub struct CompositionBindingContext {
    composition: Arc<CompiledComposition>,
    bridges: Vec<BridgeDocument>,
    limits: CompositionBindingLimits,
    storage_ids: HashMap<QualifiedSymbol, u32>,
}

impl CompositionBindingContext {
    /// Construct from compiled module authority and adopted bridge documents.
    #[must_use]
    pub fn new(
        composition: Arc<CompiledComposition>,
        mut bridges: Vec<BridgeDocument>,
        limits: CompositionBindingLimits,
    ) -> Self {
        bridges.sort_by(|left, right| {
            (&left.bridge_id, &left.authored_version)
                .cmp(&(&right.bridge_id, &right.authored_version))
        });
        Self {
            composition,
            bridges,
            limits,
            storage_ids: HashMap::new(),
        }
    }

    /// Attach generation-pinned storage IDs. The caller must authenticate the
    /// mapping against this context's exact composition before construction.
    #[must_use]
    pub fn with_storage_ids(
        mut self,
        storage_ids: impl IntoIterator<Item = (QualifiedSymbol, u32)>,
    ) -> Self {
        self.storage_ids = storage_ids.into_iter().collect();
        self
    }

    /// Exact composition fingerprint.
    #[must_use]
    pub fn fingerprint(&self) -> &str {
        &self.composition.fingerprint
    }

    /// Exact compiled authority used for storage-binding authentication.
    #[must_use]
    pub fn composition(&self) -> &Arc<CompiledComposition> {
        &self.composition
    }

    /// Return a copy carrying authenticated generation storage IDs.
    #[must_use]
    pub fn with_generation_storage_ids(
        &self,
        storage_ids: impl IntoIterator<Item = (QualifiedSymbol, u32)>,
    ) -> Self {
        let mut value = self.clone();
        value.storage_ids = storage_ids.into_iter().collect();
        value
    }

    /// Deterministic composition-local semantic ID for a qualified symbol.
    ///
    /// IDs are plan-local projections only; exact semantic identity remains in
    /// the fingerprint and binding receipt and is never confused with a runtime
    /// catalog ID.
    #[must_use]
    pub fn semantic_id(&self, symbol: &QualifiedSymbol) -> u32 {
        if let Some(id) = self.storage_ids.get(symbol) {
            return *id;
        }
        let mut symbols = self
            .composition
            .modules
            .iter()
            .flat_map(|module| module.symbols.iter())
            .collect::<Vec<_>>();
        symbols.sort_by_key(|candidate| {
            (
                candidate.module.sort_key(),
                candidate.kind.as_str().as_bytes().to_vec(),
                candidate.local_id.as_bytes().to_vec(),
            )
        });
        symbols
            .iter()
            .position(|candidate| *candidate == symbol)
            .and_then(|index| u32::try_from(index).ok())
            .expect("compiled composition symbol count is bounded to u32")
    }

    /// Resolve `local_id`, accepting `module-short:local` as an explicit qualifier.
    pub fn resolve(
        &self,
        kind: SymbolKind,
        input: &str,
    ) -> Result<(SymbolBinding, BindingExplainReceipt), BindingDiagnostic> {
        let (module, local_id) = self.parse_qualifier(input)?;
        match self.composition.resolve(&ResolveRequest {
            module: module.as_ref(),
            kind,
            local_id,
            max_candidates: self.limits.candidates.max(1),
        }) {
            Ok(outcome) => {
                let mode = self
                    .composition
                    .effective_module_mode(&outcome.symbol.module);
                let decision = if outcome.via_unqualified {
                    BindingDecision::Unique {
                        symbol: outcome.symbol.clone(),
                    }
                } else {
                    BindingDecision::Qualified {
                        symbol: outcome.symbol.clone(),
                    }
                };
                Ok((
                    SymbolBinding::Qualified(outcome.symbol),
                    BindingExplainReceipt {
                        composition_fingerprint: self.fingerprint().to_owned(),
                        effective_mode: mode,
                        decisions: vec![decision],
                        diagnostics: Vec::new(),
                    },
                ))
            }
            Err(error) => {
                self.resolve_via_bridge_or_policy(kind, local_id, module.as_ref(), &error)
            }
        }
    }

    /// Select an adopted bridge assertion between two exact endpoints.
    ///
    /// Directional assertions are considered only source-to-target. Symmetric
    /// predicates may be traversed in either direction. Required disjoint
    /// semantics are rejected because they cannot produce a positive binding.
    pub fn select_bridge(
        &self,
        source: &QualifiedSymbol,
        target: &QualifiedSymbol,
    ) -> Result<BindingExplainReceipt, BindingDiagnostic> {
        self.require_endpoint(source)?;
        self.require_endpoint(target)?;
        let mut matches = self
            .bridges
            .iter()
            .flat_map(|bridge| {
                bridge.assertions.iter().filter_map(move |assertion| {
                    let forward = assertion.source == *source && assertion.target == *target;
                    let reverse = assertion.predicate.is_symmetric()
                        && assertion.source == *target
                        && assertion.target == *source;
                    (forward || reverse).then_some((bridge, assertion))
                })
            })
            .collect::<Vec<_>>();
        matches.sort_by_key(|(bridge, assertion)| {
            (bridge.bridge_id.as_str(), assertion.predicate.as_str())
        });
        if matches.is_empty() {
            return Err(BindingDiagnostic {
                code: BindingDiagnosticCode::UnknownSymbol,
                subject: format!("{} -> {}", source.display(), target.display()),
                candidates: Vec::new(),
                remediation: "adopt an explicit bridge for these exact endpoints".to_owned(),
            });
        }
        let predicates = matches
            .iter()
            .map(|(_, assertion)| assertion.predicate.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        if predicates.len() > 1 {
            return Err(BindingDiagnostic {
                code: BindingDiagnosticCode::ConflictingBridgePaths,
                subject: format!("{} -> {}", source.display(), target.display()),
                candidates: predicates.into_iter().map(str::to_owned).collect(),
                remediation: "remove conflicting adopted bridge assertions".to_owned(),
            });
        }
        let (bridge, assertion) = matches[0];
        if assertion.predicate.as_str() == "disjoint" {
            return Err(BindingDiagnostic {
                code: BindingDiagnosticCode::UnsupportedRequiredSemantics,
                subject: bridge.bridge_id.clone(),
                candidates: vec![assertion.predicate.as_str().to_owned()],
                remediation: "use disjointness for validation, not positive symbol binding"
                    .to_owned(),
            });
        }
        Ok(BindingExplainReceipt {
            composition_fingerprint: self.fingerprint().to_owned(),
            effective_mode: bridge
                .enforcement
                .unwrap_or_else(|| self.composition.effective_module_mode(&source.module)),
            decisions: vec![BindingDecision::Bridge {
                bridge_id: bridge.bridge_id.clone(),
                predicate: assertion.predicate.as_str().to_owned(),
                source: source.clone(),
                target: target.clone(),
            }],
            diagnostics: Vec::new(),
        })
    }

    /// Verify that a resolved property belongs to the bound entity or relation.
    pub fn validate_property_owner(
        &self,
        property: &QualifiedSymbol,
        owner: &str,
    ) -> Result<(), BindingDiagnostic> {
        let owner_local = owner.rsplit_once(':').map_or(owner, |(_, local)| local);
        let Some(module) = self
            .composition
            .modules
            .iter()
            .find(|module| module.id == property.module)
        else {
            return self.require_endpoint(property);
        };
        if module.doc.properties.iter().any(|candidate| {
            format!("{}:{}", candidate.owner, candidate.name) == property.local_id
                && candidate.owner == owner_local
        }) {
            return Ok(());
        }
        Err(BindingDiagnostic {
            code: BindingDiagnosticCode::WrongOwnerProperty,
            subject: format!("{}.{}", owner, property.local_id),
            candidates: module
                .doc
                .properties
                .iter()
                .filter(|candidate| {
                    format!("{}:{}", candidate.owner, candidate.name) == property.local_id
                })
                .map(|candidate| candidate.owner.clone())
                .take(self.limits.candidates.max(1))
                .collect(),
            remediation: "use the property only on an owner that declares it".to_owned(),
        })
    }

    /// Resolve a property against the exact owner bound by the query pattern.
    pub fn resolve_owned_property(
        &self,
        owner_kind: SymbolKind,
        owner: &str,
        property: &str,
    ) -> Result<(SymbolBinding, BindingExplainReceipt), BindingDiagnostic> {
        let (owner_binding, mut receipt) = self.resolve(owner_kind, owner)?;
        let SymbolBinding::Qualified(owner_symbol) = owner_binding else {
            return self.resolve(SymbolKind::Property, property);
        };
        let local_id = format!("{}:{property}", owner_symbol.local_id);
        let outcome = self
            .composition
            .resolve(&ResolveRequest {
                module: Some(&owner_symbol.module),
                kind: SymbolKind::Property,
                local_id: &local_id,
                max_candidates: self.limits.candidates.max(1),
            })
            .map_err(|_| BindingDiagnostic {
                code: BindingDiagnosticCode::WrongOwnerProperty,
                subject: format!("{owner}.{property}"),
                candidates: self
                    .composition
                    .modules
                    .iter()
                    .flat_map(|module| module.doc.properties.iter())
                    .filter(|candidate| candidate.name == property)
                    .map(|candidate| candidate.owner.clone())
                    .take(self.limits.candidates.max(1))
                    .collect(),
                remediation: "use the property only on an owner that declares it".to_owned(),
            })?;
        receipt.decisions.push(BindingDecision::Qualified {
            symbol: outcome.symbol.clone(),
        });
        Ok((SymbolBinding::Qualified(outcome.symbol), receipt))
    }

    fn require_endpoint(&self, endpoint: &QualifiedSymbol) -> Result<(), BindingDiagnostic> {
        let found = self.composition.modules.iter().any(|module| {
            module.id == endpoint.module && module.symbols.iter().any(|symbol| symbol == endpoint)
        });
        if found {
            Ok(())
        } else {
            Err(BindingDiagnostic {
                code: BindingDiagnosticCode::UnknownSymbol,
                subject: endpoint.display(),
                candidates: Vec::new(),
                remediation: "use an endpoint declared by an activated module".to_owned(),
            })
        }
    }

    fn parse_qualifier<'a>(
        &'a self,
        input: &'a str,
    ) -> Result<(Option<OntologyModuleId>, &'a str), BindingDiagnostic> {
        let Some((qualifier, local_id)) = input.split_once(':') else {
            return Ok((None, input));
        };
        let mut matches = self
            .composition
            .modules
            .iter()
            .filter(|module| {
                module.id.short_name() == qualifier || module.id.display_ref() == qualifier
            })
            .map(|module| module.id.clone())
            .collect::<Vec<_>>();
        matches.sort_by_key(OntologyModuleId::sort_key);
        if matches.len() == 1 && !local_id.is_empty() {
            return Ok((matches.pop(), local_id));
        }
        Err(BindingDiagnostic {
            code: BindingDiagnosticCode::InvalidQualifier,
            subject: input.to_owned(),
            candidates: matches
                .into_iter()
                .take(self.limits.candidates.max(1))
                .map(|module| module.display_ref())
                .collect(),
            remediation: "use one exact module short name or display_ref".to_owned(),
        })
    }

    #[allow(clippy::too_many_lines)]
    fn resolve_via_bridge_or_policy(
        &self,
        kind: SymbolKind,
        local_id: &str,
        qualified_module: Option<&OntologyModuleId>,
        error: &graphforge_ontology::CompositionError,
    ) -> Result<(SymbolBinding, BindingExplainReceipt), BindingDiagnostic> {
        if error
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code.as_str() == "resolution.ambiguous")
        {
            return Err(BindingDiagnostic {
                code: BindingDiagnosticCode::AmbiguousSymbol,
                subject: local_id.to_owned(),
                candidates: error
                    .diagnostics
                    .iter()
                    .flat_map(|diagnostic| diagnostic.candidates.iter().cloned())
                    .take(self.limits.candidates.max(1))
                    .collect(),
                remediation: "qualify the symbol with its exact module".to_owned(),
            });
        }
        let mut bridge_matches = Vec::new();
        for bridge in &self.bridges {
            for assertion in &bridge.assertions {
                let source_matches = assertion.source.kind == kind
                    && assertion.source.local_id == local_id
                    && qualified_module.is_none_or(|module| module == &assertion.source.module);
                let reverse_matches = assertion.predicate.is_symmetric()
                    && assertion.target.kind == kind
                    && assertion.target.local_id == local_id
                    && qualified_module.is_none_or(|module| module == &assertion.target.module);
                if source_matches {
                    bridge_matches.push((bridge, assertion, false));
                } else if reverse_matches {
                    bridge_matches.push((bridge, assertion, true));
                }
            }
        }
        bridge_matches.sort_by(|(left, assertion_left, _), (right, assertion_right, _)| {
            (&left.bridge_id, assertion_left.target.display())
                .cmp(&(&right.bridge_id, assertion_right.target.display()))
        });
        let targets = bridge_matches
            .iter()
            .map(|(_, assertion, reverse)| {
                let target = if *reverse {
                    assertion.source.clone()
                } else {
                    assertion.target.clone()
                };
                (target.display(), target)
            })
            .collect::<BTreeMap<_, _>>();
        if targets.len() == 1 {
            let target = targets.into_values().next().expect("one target");
            let (bridge, assertion, reverse) = bridge_matches[0];
            let source = if reverse {
                assertion.target.clone()
            } else {
                assertion.source.clone()
            };
            let mode = bridge
                .enforcement
                .unwrap_or_else(|| self.composition.effective_module_mode(&target.module));
            return Ok((
                SymbolBinding::Qualified(target.clone()),
                BindingExplainReceipt {
                    composition_fingerprint: self.fingerprint().to_owned(),
                    effective_mode: mode,
                    decisions: vec![BindingDecision::Bridge {
                        bridge_id: bridge.bridge_id.clone(),
                        predicate: assertion.predicate.as_str().to_owned(),
                        source,
                        target,
                    }],
                    diagnostics: Vec::new(),
                },
            ));
        }
        if targets.len() > 1 {
            return Err(BindingDiagnostic {
                code: BindingDiagnosticCode::ConflictingBridgePaths,
                subject: local_id.to_owned(),
                candidates: targets
                    .into_iter()
                    .take(self.limits.candidates.max(1))
                    .map(|(_, target)| target.display())
                    .collect(),
                remediation: "qualify the symbol or remove conflicting bridge paths".to_owned(),
            });
        }

        let mode = qualified_module.map_or(self.composition.profile_default, |module| {
            self.composition.effective_module_mode(module)
        });
        if mode == ActivationMode::Strict {
            return Err(BindingDiagnostic {
                code: BindingDiagnosticCode::UnknownSymbol,
                subject: local_id.to_owned(),
                candidates: error
                    .diagnostics
                    .iter()
                    .flat_map(|diagnostic| diagnostic.candidates.iter().cloned())
                    .take(self.limits.candidates.max(1))
                    .collect(),
                remediation: "qualify or activate a declaring module/bridge".to_owned(),
            });
        }
        let diagnostic = BindingDiagnostic {
            code: BindingDiagnosticCode::RuntimeFallback,
            subject: local_id.to_owned(),
            candidates: Vec::new(),
            remediation: "adopt an ontology declaration before enabling strict mode".to_owned(),
        };
        Ok((
            SymbolBinding::Runtime {
                kind,
                local_id: local_id.to_owned(),
            },
            BindingExplainReceipt {
                composition_fingerprint: self.fingerprint().to_owned(),
                effective_mode: mode,
                decisions: vec![BindingDecision::RuntimeFallback {
                    kind,
                    local_id: local_id.to_owned(),
                }],
                diagnostics: (mode == ActivationMode::Advisory)
                    .then_some(diagnostic)
                    .into_iter()
                    .collect(),
            },
        ))
    }
}
