use super::super::{OntologyAuthorityState, tests::parity_document};
use super::*;
use graphforge_ontology::{CompositionDiagnostic, DiagnosticCode, DiagnosticLimit};
use uuid::Uuid;

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
        assert_eq!(
            error.diagnostics[0].message,
            format!("portable-v2 {source:?}: test")
        );
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
