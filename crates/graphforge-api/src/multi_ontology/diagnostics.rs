//! Bounded, deterministic multi-ontology diagnostic projection.

use super::{
    GfError, MAX_ERROR_DIAGNOSTICS, MAX_ERROR_TEXT_BYTES, MultiOntologyDiagnostic,
    MultiOntologyError, MultiOntologyValidationReceipt,
};
use graphforge_ontology::{BridgeSetId, OntologyModuleId};

pub(super) fn composition_error(error: graphforge_ontology::CompositionError) -> GfError {
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

pub(super) fn validation_receipt(
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

pub(super) fn composition_change_error(
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

pub(super) fn dependency_blocked_error(preview: &graphforge_ontology::DeletePreview) -> GfError {
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

pub(super) fn portable_error(error: graphforge_storage::PortableV2Error) -> GfError {
    use graphforge_storage::PortableV2ErrorCode;
    // PortableV2Error's Display is deliberately limited to its static,
    // path-free detail (the optional entry is never rendered). Preserve that
    // typed producer diagnosis instead of collapsing every storage failure to
    // the same generic interchange message.
    let diagnostic_message = bounded_text(&error.to_string());
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
            message: diagnostic_message,
            subjects: bounded_items(subjects, MAX_ERROR_DIAGNOSTICS),
            candidates: Vec::new(),
            remediation: remediation.into(),
            limit: MAX_ERROR_DIAGNOSTICS,
        }],
    }
}

pub(super) fn bounded_text(value: &str) -> String {
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

#[cfg(test)]
mod tests;
