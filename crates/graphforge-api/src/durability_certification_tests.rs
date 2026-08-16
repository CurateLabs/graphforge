//! Integrated durability/isolation certification surface (#756).
//!
//! Required CI exercises the bounded seeded state machine. Native POSIX and
//! Windows subprocess failpoint matrices remain authoritative for real
//! API/handle behavior and are cross-checked against the shared oracle
//! boundaries here.

#![cfg(test)]

use graphforge_storage::project_certification::{
    CERT_CONTRACT, CERT_SEED, WRITE_SKEW_CLASSIFICATION, evidence_summary, run_certification_suite,
};
use graphforge_storage::project_fault_oracle::{
    AuthorityClass, PublicationPhase, native_shared_boundary_authority,
};

#[test]
fn seeded_certification_suite_is_clean_at_required_budget() {
    let evidence = run_certification_suite();
    assert_eq!(evidence.contract, CERT_CONTRACT);
    assert_eq!(evidence.seed, CERT_SEED);
    assert_eq!(evidence.issue, 756);
    assert_eq!(
        evidence.untriaged_failures,
        0,
        "{}",
        evidence_summary(&evidence)
    );
    assert!(!evidence.artifact_digest.is_empty());
    assert!(!evidence.commands.is_empty());
    let rendered = serde_json::to_string(&evidence)
        .unwrap()
        .to_ascii_lowercase();
    assert!(!rendered.contains("provides ssi"));
    assert!(!rendered.contains("serializable isolation"));
    assert!(!rendered.contains("distributed durability"));
    assert!(!rendered.contains("universal filesystem"));
}

#[test]
fn native_shared_boundaries_agree_with_oracle_authority() {
    for phase in [
        PublicationPhase::BeforeCurrentReplace,
        PublicationPhase::AfterCurrentReplace,
        PublicationPhase::AfterRootFsync,
    ] {
        let native = native_shared_boundary_authority(phase);
        let expected = match phase {
            PublicationPhase::BeforeCurrentReplace => AuthorityClass::PriorGeneration,
            PublicationPhase::AfterCurrentReplace | PublicationPhase::AfterRootFsync => {
                AuthorityClass::NewGeneration
            }
            _ => unreachable!(),
        };
        assert_eq!(native, expected, "phase={phase:?}");
    }
}

#[test]
fn write_skew_classification_remains_honest() {
    assert_eq!(WRITE_SKEW_CLASSIFICATION, "allowed_documented_not_ssi");
    assert!(!WRITE_SKEW_CLASSIFICATION.contains("serializable"));
}
