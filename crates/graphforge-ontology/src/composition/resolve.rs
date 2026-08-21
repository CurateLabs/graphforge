//! Qualified and unqualified symbol resolution over a compiled composition.

use super::compile::CompiledComposition;
use super::diagnostic::{CompositionDiagnostic, CompositionError, DiagnosticCode, DiagnosticLimit};
use super::identity::{OntologyModuleId, QualifiedSymbol, SymbolKind};
use serde::{Deserialize, Serialize};

/// Request to resolve a symbol against a compiled composition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolveRequest<'a> {
    /// Optional exact module identity. When set, resolution is qualified.
    pub module: Option<&'a OntologyModuleId>,
    /// Symbol kind.
    pub kind: SymbolKind,
    /// Local identifier (NFC expected; checked by compiler on ingest).
    pub local_id: &'a str,
    /// Maximum candidates returned on ambiguity.
    pub max_candidates: usize,
}

/// Successful resolution outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolutionOutcome {
    /// Exact qualified symbol.
    pub symbol: QualifiedSymbol,
    /// Whether resolution used an unqualified unique match.
    pub via_unqualified: bool,
}

impl CompiledComposition {
    /// Resolve a qualified or uniquely resolvable unqualified symbol.
    ///
    /// Inventory order never confers precedence. Ambiguous unqualified names
    /// fail with a sorted, bounded candidate list.
    pub fn resolve(
        &self,
        request: &ResolveRequest<'_>,
    ) -> Result<ResolutionOutcome, CompositionError> {
        let dlimit = DiagnosticLimit {
            max_candidates: request.max_candidates.max(1),
        };

        if let Some(module) = request.module {
            let key = (
                module.display_ref(),
                request.kind,
                request.local_id.to_owned(),
            );
            return match self.qualified_index.get(&key) {
                Some(symbol) => Ok(ResolutionOutcome {
                    symbol: symbol.clone(),
                    via_unqualified: false,
                }),
                None => Err(CompositionError::one(CompositionDiagnostic::with_subjects(
                    DiagnosticCode::ResolutionNotFound,
                    format!(
                        "qualified symbol {}:{} not found in module",
                        request.kind.as_str(),
                        request.local_id
                    ),
                    vec![module.display_ref()],
                    dlimit,
                ))),
            };
        }

        let key = (request.kind, request.local_id.to_owned());
        let Some(modules) = self.unqualified_index.get(&key) else {
            return Err(CompositionError::one(CompositionDiagnostic::with_subjects(
                DiagnosticCode::ResolutionNotFound,
                format!(
                    "no module declares {}:{}",
                    request.kind.as_str(),
                    request.local_id
                ),
                Vec::new(),
                dlimit,
            )));
        };

        match modules.as_slice() {
            [] => Err(CompositionError::one(CompositionDiagnostic::with_subjects(
                DiagnosticCode::ResolutionNotFound,
                format!(
                    "no module declares {}:{}",
                    request.kind.as_str(),
                    request.local_id
                ),
                Vec::new(),
                dlimit,
            ))),
            [only] => {
                let qkey = (
                    only.display_ref(),
                    request.kind,
                    request.local_id.to_owned(),
                );
                let symbol = self.qualified_index.get(&qkey).cloned().ok_or_else(|| {
                    CompositionError::one(CompositionDiagnostic::with_subjects(
                        DiagnosticCode::ResolutionNotFound,
                        "internal qualified index missing unique candidate",
                        vec![only.display_ref()],
                        dlimit,
                    ))
                })?;
                Ok(ResolutionOutcome {
                    symbol,
                    via_unqualified: true,
                })
            }
            many => {
                let candidates: Vec<String> = many
                    .iter()
                    .map(|module| format!("{}:{}", module.short_name(), request.local_id))
                    .collect();
                Err(CompositionError::one(CompositionDiagnostic::new(
                    DiagnosticCode::ResolutionAmbiguous,
                    format!(
                        "unqualified {}:{} has {} candidates",
                        request.kind.as_str(),
                        request.local_id,
                        many.len()
                    ),
                    Vec::new(),
                    candidates,
                    dlimit,
                )))
            }
        }
    }

    /// Look up the compiled module retaining source authority for `id`.
    #[must_use]
    pub fn module(&self, id: &OntologyModuleId) -> Option<&super::compile::CompiledModule> {
        self.modules.iter().find(|m| &m.id == id)
    }

    /// Effective activation mode for a module subject (exact override or default).
    #[must_use]
    pub fn effective_module_mode(
        &self,
        module: &OntologyModuleId,
    ) -> super::identity::ActivationMode {
        let subject = module.display_ref();
        self.activation
            .iter()
            .find(|r| r.scope == super::identity::ActivationScope::Module && r.subject == subject)
            .map_or(self.profile_default, |r| r.mode)
    }
}
