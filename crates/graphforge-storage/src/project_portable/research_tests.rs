//! A research verification read failure keeps the storage class, not the
//! package's: `Io` from the reader survives `check_version`, every other
//! reader failure stays `Incompatible`.
use super::*;
use crate::research_versions::{
    ResearchInterchangeManifest, ResearchInterchangeSelection, ResearchParticipantCommitment,
    ResearchParticipantKey, ResearchVersionContent, ResearchVersionRecord, RESEARCH_VERSION,
};
use sha2::Digest;
use std::collections::BTreeSet;
use uuid::Uuid;

/// The logical Version commitment (physical locators excluded), as the
/// interchange identity rule defines it.
fn identity(version: &ResearchVersionRecord) -> [u8; 32] {
    let mut logical = version.clone();
    logical.content.generation_uuid = Uuid::nil();
    logical.content.manifest_sha256 = [0; 32];
    Sha256::digest(serde_json::to_vec(&logical).unwrap()).into()
}

fn version(participants: Vec<ResearchParticipantCommitment>) -> ResearchVersionRecord {
    ResearchVersionRecord {
        parents: Vec::new(),
        author: None,
        committer: None,
        provenance: None,
        version_uuid: Uuid::new_v4(),
        context_uuid: Uuid::new_v4(),
        label: None,
        description: None,
        created_at: 1,
        content: ResearchVersionContent {
            generation_uuid: Uuid::new_v4(),
            manifest_sha256: [7; 32],
            source_version: None,
            graph_projection: None,
            participants,
            required_versions: BTreeSet::new(),
            producer: concat!(
                "graphforge-storage/",
                env!("CARGO_PKG_VERSION"),
                ";research/6"
            )
            .into(),
            evidence: Vec::new(),
        },
    }
}

/// A one-Version research package whose selected Version archives one saved
/// query: the registry bytes and the archived participant bytes a real export
/// carries.
struct Package {
    registry_path: String,
    registry_bytes: Vec<u8>,
    object_path: String,
    object_bytes: Vec<u8>,
    object_digest: String,
}

fn package() -> Package {
    let query = crate::SavedQuery {
        query_uuid: Uuid::new_v4(),
        name: "Archived analysis".into(),
        description: None,
        query: "MATCH (n) RETURN n.name".into(),
        parameters: BTreeMap::new(),
    };
    let definitions = crate::WorkspaceSavedQueries {
        queries: BTreeMap::from([(query.query_uuid, query)]),
        ..crate::WorkspaceSavedQueries::default()
    };
    let participant = definitions.to_project_participant().unwrap();
    let selected = version(vec![ResearchParticipantCommitment {
        key: ResearchParticipantKey {
            capability: participant.capability_id.clone(),
            family: participant.record_family_id.clone(),
        },
        capability_version: participant.capability_version,
        record_version: participant.record_version,
        encoding: "json".into(),
        schema_sha256: participant.schema_fingerprint,
        row_count: participant.row_count,
        content_sha256: Sha256::digest(&participant.bytes).into(),
    }]);
    let project_uuid = Uuid::new_v4();
    let manifest = ResearchInterchangeManifest {
        history: BTreeMap::new(),
        contract_version: 1,
        research_capability_version: RESEARCH_VERSION,
        producer: concat!(
            "graphforge-storage/",
            env!("CARGO_PKG_VERSION"),
            ";research-interchange/1"
        )
        .into(),
        source_project_uuid: project_uuid,
        fork_project_uuid: None,
        fork: None,
        selected_version_uuid: selected.version_uuid,
        selection: ResearchInterchangeSelection::Complete {
            source_version_uuid: selected.version_uuid,
        },
        versions: BTreeMap::from([(selected.version_uuid, selected.clone())]),
        version_projects: BTreeMap::from([(selected.version_uuid, project_uuid)]),
        identities: BTreeMap::from([(selected.version_uuid, identity(&selected))]),
        ancestry: BTreeMap::new(),
        genealogy: BTreeMap::new(),
        accepted: BTreeMap::new(),
        proof_exports: BTreeMap::new(),
    };
    let registry = manifest.registry().unwrap();
    let registry_id =
        crate::project_portable_export::planning::portable_participant_id("research", "registry");
    let digest = hex(&selected.content.participants[0].content_sha256);
    Package {
        registry_path: format!("{}{registry_id}/participant.json", super::research::NAMESPACE),
        registry_bytes: registry.participant().unwrap().bytes,
        object_path: super::research::object_path(&selected.version_uuid, &digest, false),
        object_bytes: participant.bytes,
        object_digest: digest,
    }
}

fn report(package: &Package) -> PortableReport {
    PortableReport {
        research_interchange: true,
        research_entries: vec![
            PortableResearchEntry {
                component_id: crate::project_portable_export::planning::portable_participant_id(
                    "research", "registry",
                ),
                path: package.registry_path.clone(),
                length: package.registry_bytes.len() as u64,
                sha256: hex(&Sha256::digest(&package.registry_bytes)),
            },
            PortableResearchEntry {
                component_id: "research-content".into(),
                path: package.object_path.clone(),
                length: package.object_bytes.len() as u64,
                sha256: package.object_digest.clone(),
            },
        ],
        contract: "graphforge-portable-verify/1",
        representation: PortableRepresentation::Bundle,
        source: PortableSource::File,
        package_digest: String::new(),
        package_class: PortablePackageClass::Complete,
        producing_core_version: env!("CARGO_PKG_VERSION").into(),
        component_count: 2,
        entry_count: 2,
        payload_bytes: (package.registry_bytes.len() + package.object_bytes.len()) as u64,
        bytes_by_kind: BTreeMap::new(),
        plausibility: PortablePlausibility {
            graph_data_bytes: 0,
            graph_data_ratio_ppm: 0,
            parquet: None,
        },
        integrity: PortableIntegrity::Verified,
        compatibility: PortableCompatibility::Supported,
        authenticity: PortableAuthenticity::Unsigned,
        transport_digest: None,
        ontology_composition: None,
        ontology_composition_entries: Vec::new(),
    }
}

/// `None` serves the real core object; `Some(code)` refuses it with that
/// verdict, the way a reader's failure classes reach `check_version`.
fn verify_with(
    package: &Package,
    object_failure: Option<PortableErrorCode>,
) -> Result<
    Option<(
        crate::research_versions::ResearchRegistry,
        super::research::Objects,
    )>,
    PortableError,
> {
    let object_bytes = package.object_bytes.clone();
    super::research::validate_with_reader(
        &report(package),
        PortableLimits::default(),
        None,
        move |path, _| {
            if path == package.registry_path {
                return Ok(package.registry_bytes.clone());
            }
            assert_eq!(
                path, package.object_path,
                "only the registry and the core object are read"
            );
            match object_failure {
                Some(code) => Err(PortableError::new(code, "injected research read failure")),
                None => Ok(object_bytes.clone()),
            }
        },
    )
}

#[test]
fn the_real_package_verifies_from_a_reader() {
    verify_with(&package(), None).unwrap();
}

#[test]
fn a_read_io_failure_of_a_core_object_stays_in_the_storage_class() {
    let error = verify_with(&package(), Some(PortableErrorCode::Io)).unwrap_err();
    assert_eq!(error.code, PortableErrorCode::Io);
}

#[test]
fn a_truncated_core_object_stays_incompatible() {
    let error = verify_with(&package(), Some(PortableErrorCode::InvalidStructure)).unwrap_err();
    assert_eq!(error.code, PortableErrorCode::Incompatible);
}
