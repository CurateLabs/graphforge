//! Bounded, stable composition diagnostics matching the M9 contract codes.

use std::fmt;

use super::identity::OntologyModuleId;

/// Contract phase for a composition failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompositionPhase {
    /// Inventory membership / identity problems.
    Inventory,
    /// Dependency closure problems.
    Dependency,
    /// Qualified symbol collisions across modules.
    Collision,
    /// Resource / limit exhaustion.
    Resource,
    /// Lifecycle cancellation.
    Lifecycle,
    /// Unqualified / qualified resolution.
    Resolution,
}

impl CompositionPhase {
    /// Stable wire token.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Inventory => "inventory",
            Self::Dependency => "dependency",
            Self::Collision => "collision",
            Self::Resource => "resource",
            Self::Lifecycle => "lifecycle",
            Self::Resolution => "resolution",
        }
    }
}

/// Stable diagnostic code family (subset needed by #836).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiagnosticCode {
    /// Duplicate module identity with conflicting digests / membership.
    InventoryDuplicate,
    /// Requested module is absent from the inventory.
    InventoryNotFound,
    /// Required dependency identity is not present.
    DependencyMissing,
    /// Module dependency graph contains a forbidden cycle.
    DependencyCycle,
    /// Same qualified identity declared twice with conflicting content.
    CollisionQualifiedDuplicate,
    /// Module count exceeded the caller limit.
    ResourceModules,
    /// Bridge count exceeded the caller limit.
    ResourceBridges,
    /// Symbol count exceeded the caller limit.
    ResourceSymbols,
    /// Dependency-edge count exceeded the caller limit.
    ResourceDiagnostics,
    /// Caller cancelled before publication.
    LifecycleCancelled,
    /// Unqualified name has more than one candidate.
    ResolutionAmbiguous,
    /// No candidate for the requested symbol.
    ResolutionNotFound,
    /// Kind does not match any candidate for the local id.
    ResolutionKindMismatch,
    /// Module document failed independent validation.
    InventoryMalformed,
    /// Declared digest does not match the canonical module document.
    InterchangeIntegrity,
    /// Identifier is not NFC-normalized or digest is malformed.
    CollisionMetadata,
}

impl DiagnosticCode {
    /// Stable `phase.code` wire token.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InventoryDuplicate => "inventory.duplicate",
            Self::InventoryNotFound => "inventory.not_found",
            Self::DependencyMissing => "dependency.missing",
            Self::DependencyCycle => "dependency.cycle",
            Self::CollisionQualifiedDuplicate => "collision.qualified_duplicate",
            Self::ResourceModules => "resource.modules",
            Self::ResourceBridges => "resource.bridges",
            Self::ResourceSymbols => "resource.symbols",
            Self::ResourceDiagnostics => "resource.diagnostics",
            Self::LifecycleCancelled => "lifecycle.cancelled",
            Self::ResolutionAmbiguous => "resolution.ambiguous",
            Self::ResolutionNotFound => "resolution.not_found",
            Self::ResolutionKindMismatch => "resolution.kind_mismatch",
            Self::InventoryMalformed => "inventory.not_found",
            Self::InterchangeIntegrity => "interchange.integrity",
            Self::CollisionMetadata => "collision.metadata",
        }
    }

    /// Phase for this code.
    pub fn phase(self) -> CompositionPhase {
        match self {
            Self::InventoryDuplicate
            | Self::InventoryNotFound
            | Self::InventoryMalformed
            | Self::InterchangeIntegrity
            | Self::CollisionMetadata => CompositionPhase::Inventory,
            Self::DependencyMissing | Self::DependencyCycle => CompositionPhase::Dependency,
            Self::CollisionQualifiedDuplicate => CompositionPhase::Collision,
            Self::ResourceModules
            | Self::ResourceBridges
            | Self::ResourceSymbols
            | Self::ResourceDiagnostics => CompositionPhase::Resource,
            Self::LifecycleCancelled => CompositionPhase::Lifecycle,
            Self::ResolutionAmbiguous | Self::ResolutionNotFound | Self::ResolutionKindMismatch => {
                CompositionPhase::Resolution
            }
        }
    }
}

impl fmt::Display for DiagnosticCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Default and caller-supplied caps for diagnostic candidate lists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiagnosticLimit {
    /// Maximum subjects / candidates retained in one diagnostic.
    pub max_candidates: usize,
}

impl Default for DiagnosticLimit {
    fn default() -> Self {
        Self { max_candidates: 64 }
    }
}

/// One bounded composition diagnostic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompositionDiagnostic {
    /// Stable code.
    pub code: DiagnosticCode,
    /// Contract phase.
    pub phase: CompositionPhase,
    /// Human-readable explanation (clients branch only on `code`).
    pub message: String,
    /// Identity-sorted, path-free subjects.
    pub subjects: Vec<String>,
    /// Identity-sorted, bounded candidates (e.g. ambiguity).
    pub candidates: Vec<String>,
    /// Caller-visible candidate/subject cap applied.
    pub limit: usize,
}

impl CompositionDiagnostic {
    /// Build a diagnostic, sorting/deduplicating/capping subjects and candidates.
    pub fn new(
        code: DiagnosticCode,
        message: impl Into<String>,
        subjects: Vec<String>,
        candidates: Vec<String>,
        limit: DiagnosticLimit,
    ) -> Self {
        let cap = limit.max_candidates.max(1);
        Self {
            code,
            phase: code.phase(),
            message: message.into(),
            subjects: bound_list(subjects, cap),
            candidates: bound_list(candidates, cap),
            limit: cap,
        }
    }

    /// Convenience for a subject-only diagnostic.
    pub fn with_subjects(
        code: DiagnosticCode,
        message: impl Into<String>,
        subjects: Vec<String>,
        limit: DiagnosticLimit,
    ) -> Self {
        Self::new(code, message, subjects, Vec::new(), limit)
    }

    /// Convenience for a module-id subject.
    pub fn for_module(
        code: DiagnosticCode,
        message: impl Into<String>,
        module: &OntologyModuleId,
        limit: DiagnosticLimit,
    ) -> Self {
        Self::with_subjects(code, message, vec![module.display_ref()], limit)
    }
}

fn bound_list(mut items: Vec<String>, cap: usize) -> Vec<String> {
    items.sort();
    items.dedup();
    if items.len() > cap {
        items.truncate(cap);
    }
    items
}

/// Composition API error: one or more diagnostics, never host paths or payloads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompositionError {
    /// Bounded diagnostics collected for this failure.
    pub diagnostics: Vec<CompositionDiagnostic>,
}

impl CompositionError {
    /// Single-diagnostic error.
    pub fn one(diagnostic: CompositionDiagnostic) -> Self {
        Self {
            diagnostics: vec![diagnostic],
        }
    }

    /// Primary (first) stable code, when present.
    pub fn code(&self) -> Option<DiagnosticCode> {
        self.diagnostics.first().map(|d| d.code)
    }
}

impl fmt::Display for CompositionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(first) = self.diagnostics.first() {
            write!(f, "{}: {}", first.code, first.message)
        } else {
            write!(f, "composition error")
        }
    }
}

impl std::error::Error for CompositionError {}
