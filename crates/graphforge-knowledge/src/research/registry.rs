//! Registered storage families owned by immutable knowledge.
use super::{
    CLAIM_RELATION_SCHEMA, CLAIM_SUPPRESSION_SCHEMA, MAX_RESEARCH_ROWS, RESEARCH_CLAIM_SCHEMA,
    RESEARCH_DECISION_SCHEMA, RESEARCH_RECORD_VERSION,
};
use crate::{EPISTEMIC_CAPABILITY_VERSION, SchemaRegistryEntry};
use graphforge_core::canonical::{CANONICAL_CONTRACT_VERSION, CanonicalDomain, fingerprint};
use std::sync::{Arc, LazyLock};

static CLAIM_SCHEMA_FINGERPRINT: LazyLock<[u8; 32]> = LazyLock::new(|| {
    fingerprint(
    CanonicalDomain::Schema, CANONICAL_CONTRACT_VERSION, b"research_claim/1|assertion_uuid:fixed[16]:required|conceptual_uuid:fixed[16]:required|category:utf8:required|creator_uuid:fixed[16]:required|run_uuid:fixed[16]:nullable|origin_branch_uuid:fixed[16]:nullable|origin_version_uuid:fixed[16]:nullable|provenance_uuid:fixed[16]:required|recorded_at:timestamp_us_utc:required|contract_version:u32:required").expect("bounded registered research schema")
});

static RELATION_SCHEMA_FINGERPRINT: LazyLock<[u8; 32]> = LazyLock::new(|| {
    fingerprint(
    CanonicalDomain::Schema, CANONICAL_CONTRACT_VERSION, b"research_relation/1|relation_uuid:fixed[16]:required|source_assertion_uuid:fixed[16]:required|target_assertion_uuid:fixed[16]:required|kind:utf8:required|creator_uuid:fixed[16]:required|provenance_uuid:fixed[16]:required|recorded_at:timestamp_us_utc:required|contract_version:u32:required").expect("bounded registered research schema")
});

static DECISION_SCHEMA_FINGERPRINT: LazyLock<[u8; 32]> = LazyLock::new(|| {
    fingerprint(
    CanonicalDomain::Schema, CANONICAL_CONTRACT_VERSION, b"research_decision/1|sequence:u64:required|decision_uuid:fixed[16]:required|operation_uuid:fixed[16]:required|request_sha256:fixed[32]:required|project_uuid:fixed[16]:required|community_uuid:fixed[16]:nullable|context_uuid:fixed[16]:required|subject_kind:utf8:required|subject_uuid:fixed[16]:required|kind:utf8:required|creator_uuid:fixed[16]:required|source_version_uuid:fixed[16]:nullable|recorded_at:timestamp_us_utc:required|contract_version:u32:required").expect("bounded registered research schema")
});

static SUPPRESSION_SCHEMA_FINGERPRINT: LazyLock<[u8; 32]> = LazyLock::new(|| {
    fingerprint(
    CanonicalDomain::Schema, CANONICAL_CONTRACT_VERSION, b"research_suppression/1|suppression_uuid:fixed[16]:required|assertion_uuid:fixed[16]:required|context_uuid:fixed[16]:required|creator_uuid:fixed[16]:required|provenance_uuid:fixed[16]:required|recorded_at:timestamp_us_utc:required|contract_version:u32:required").expect("bounded registered research schema")
});

pub(crate) fn schema_registry_entries() -> Vec<SchemaRegistryEntry> {
    vec![
        SchemaRegistryEntry {
            capability_id: "epistemic",
            capability_version: EPISTEMIC_CAPABILITY_VERSION,
            record_family: "research_claims",
            record_version: RESEARCH_RECORD_VERSION,
            schema: Arc::clone(&RESEARCH_CLAIM_SCHEMA),
            schema_fingerprint: *CLAIM_SCHEMA_FINGERPRINT,
            enum_registry_versions: &[("research_category", 1)],
            sort_key: &["assertion_uuid"],
            diff_identity_fields: &["assertion_uuid"],
            diff_record_uuid_field: Some("assertion_uuid"),
            fingerprint_domain: CanonicalDomain::ResearchClaim,
            owner: "graphforge-knowledge",
            implementation_issue: 1353,
            max_rows: MAX_RESEARCH_ROWS,
        },
        SchemaRegistryEntry {
            capability_id: "epistemic",
            capability_version: EPISTEMIC_CAPABILITY_VERSION,
            record_family: "claim_relations",
            record_version: RESEARCH_RECORD_VERSION,
            schema: Arc::clone(&CLAIM_RELATION_SCHEMA),
            schema_fingerprint: *RELATION_SCHEMA_FINGERPRINT,
            enum_registry_versions: &[("claim_relation_kind", 1)],
            sort_key: &["relation_uuid"],
            diff_identity_fields: &["relation_uuid"],
            diff_record_uuid_field: Some("relation_uuid"),
            fingerprint_domain: CanonicalDomain::ResearchClaimRelation,
            owner: "graphforge-knowledge",
            implementation_issue: 1353,
            max_rows: MAX_RESEARCH_ROWS,
        },
        SchemaRegistryEntry {
            capability_id: "research",
            capability_version: 6,
            record_family: "canonical_decisions",
            record_version: RESEARCH_RECORD_VERSION,
            schema: Arc::clone(&RESEARCH_DECISION_SCHEMA),
            schema_fingerprint: *DECISION_SCHEMA_FINGERPRINT,
            enum_registry_versions: &[("research_subject_kind", 1), ("research_decision_kind", 1)],
            sort_key: &["sequence"],
            diff_identity_fields: &["decision_uuid"],
            diff_record_uuid_field: Some("decision_uuid"),
            fingerprint_domain: CanonicalDomain::ResearchDecision,
            owner: "graphforge-knowledge",
            implementation_issue: 1353,
            max_rows: MAX_RESEARCH_ROWS,
        },
        SchemaRegistryEntry {
            capability_id: "epistemic",
            capability_version: EPISTEMIC_CAPABILITY_VERSION,
            record_family: "claim_suppressions",
            record_version: RESEARCH_RECORD_VERSION,
            schema: Arc::clone(&CLAIM_SUPPRESSION_SCHEMA),
            schema_fingerprint: *SUPPRESSION_SCHEMA_FINGERPRINT,
            enum_registry_versions: &[],
            sort_key: &["suppression_uuid"],
            diff_identity_fields: &["suppression_uuid"],
            diff_record_uuid_field: Some("suppression_uuid"),
            fingerprint_domain: CanonicalDomain::ResearchClaimSuppression,
            owner: "graphforge-knowledge",
            implementation_issue: 1353,
            max_rows: MAX_RESEARCH_ROWS,
        },
    ]
}
