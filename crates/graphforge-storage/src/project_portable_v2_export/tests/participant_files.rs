//! Bounded authenticated participant reads over real exported packages.

use super::*;
use crate::project_portable_v2::{PortableV2FileRef, PortableV2PackageIndex};
use crate::{PortableV2ParticipantId, WorkspaceConfiguration, WorkspaceResearchMetadata};

fn participant(capability: &str, family: &str) -> PortableV2ParticipantId {
    PortableV2ParticipantId {
        capability_id: capability.into(),
        record_family_id: family.into(),
    }
}

fn configuration() -> PortableV2ParticipantId {
    participant(
        crate::WORKSPACE_CAPABILITY_ID,
        crate::WORKSPACE_CONFIGURATION_FAMILY,
    )
}

fn research_metadata() -> PortableV2ParticipantId {
    participant(
        crate::WORKSPACE_CAPABILITY_ID,
        crate::WORKSPACE_RESEARCH_METADATA_FAMILY,
    )
}

/// Export one real project in both representations and return `(expanded, bundle, digest)`.
fn exported() -> (tempfile::TempDir, PathBuf, PathBuf, String) {
    let (_project, generation) = graph_generation_with_composition(true);
    let plan = plan_complete_portable_v2(&generation, PortableV2Limits::default()).unwrap();
    let outputs = tempfile::tempdir().unwrap();
    let (expanded, bundle) = write_test_representations(&plan, outputs.path());
    let report = verify_portable_v2(
        &bundle,
        PortableV2Mode::Full,
        PortableV2Limits::default(),
        None,
    )
    .unwrap();
    (outputs, expanded, bundle, report.package_digest)
}

fn open(path: &Path, digest: &str) -> Result<PortableV2PackageIndex, PortableV2Error> {
    PortableV2PackageIndex::open(path, digest, PortableV2Limits::default(), None)
}

#[test]
fn participants_are_located_through_the_runtime_map_and_read_verified_in_both_forms() {
    let (_outputs, expanded, bundle, digest) = exported();
    let mut reads = Vec::new();
    for path in [&expanded, &bundle] {
        let index = open(path, &digest).unwrap();
        let config_file = index.participant_file(&configuration()).unwrap().unwrap();
        let config_bytes = index.read(&config_file, 1 << 20, None).unwrap();
        assert_eq!(config_bytes.len() as u64, config_file.length);
        let configuration = WorkspaceConfiguration::from_canonical_json(&config_bytes).unwrap();
        assert_eq!(
            configuration.ontology_mode,
            crate::WorkspaceOntologyMode::None
        );
        let metadata_file = index
            .participant_file(&research_metadata())
            .unwrap()
            .unwrap();
        let metadata_bytes = index.read(&metadata_file, 1 << 20, None).unwrap();
        assert_eq!(
            WorkspaceResearchMetadata::from_canonical_json(&metadata_bytes).unwrap(),
            WorkspaceResearchMetadata::empty()
        );
        reads.push((config_file, config_bytes, metadata_file, metadata_bytes));
    }
    // The semantic package identity, file listing, and bytes are representation-independent.
    assert_eq!(reads[0], reads[1]);
}

#[test]
fn absent_participant_and_stale_package_digest_are_refused() {
    let (_outputs, expanded, bundle, digest) = exported();
    for path in [&expanded, &bundle] {
        let index = open(path, &digest).unwrap();
        assert_eq!(
            index
                .participant_file(&participant("workspace", "no_such_family"))
                .unwrap(),
            None
        );
        assert_eq!(
            index
                .participant_file(&participant("no_such_capability", "configuration"))
                .unwrap(),
            None
        );
        let stale = format!("sha256:{}", "0".repeat(64));
        let error = open(path, &stale).unwrap_err();
        assert_eq!(error.code, PortableV2ErrorCode::DigestMismatch);
    }
}

#[test]
fn a_file_the_manifest_does_not_list_exactly_is_refused() {
    let (_outputs, expanded, _bundle, digest) = exported();
    let index = open(&expanded, &digest).unwrap();
    let listed = index.participant_file(&configuration()).unwrap().unwrap();
    for forged in [
        PortableV2FileRef {
            sha256: "0".repeat(64),
            ..listed.clone()
        },
        PortableV2FileRef {
            length: listed.length + 1,
            ..listed.clone()
        },
        PortableV2FileRef {
            path: "data/components/settings/unlisted/file.json".into(),
            ..listed.clone()
        },
    ] {
        let error = index.read(&forged, 1 << 20, None).unwrap_err();
        assert_eq!(error.code, PortableV2ErrorCode::InvalidStructure);
    }
}

#[test]
fn tampered_payload_byte_is_a_digest_mismatch_in_both_forms() {
    let (_outputs, expanded, bundle, digest) = exported();
    let listed = open(&expanded, &digest)
        .unwrap()
        .participant_file(&configuration())
        .unwrap()
        .unwrap();
    let target = expanded.join(&listed.path);
    let authentic = fs::read(&target).unwrap();

    let mut tampered = authentic.clone();
    tampered[0] ^= 1;
    fs::write(&target, &tampered).unwrap();
    let error = open(&expanded, &digest).unwrap_err();
    assert_eq!(error.code, PortableV2ErrorCode::DigestMismatch);
    assert_eq!(error.entry.as_deref(), Some(listed.path.as_str()));

    let mut package = fs::read(&bundle).unwrap();
    let offset = package
        .windows(authentic.len())
        .position(|window| window == &authentic[..])
        .expect("the bundle carries the authentic payload bytes");
    package[offset] ^= 1;
    fs::write(&bundle, &package).unwrap();
    let error = open(&bundle, &digest).unwrap_err();
    assert_eq!(error.code, PortableV2ErrorCode::DigestMismatch);
    assert_eq!(error.entry.as_deref(), Some(listed.path.as_str()));
}

#[test]
fn change_after_the_scan_is_refused_rather_than_served() {
    let (_outputs, expanded, _bundle, digest) = exported();
    let index = open(&expanded, &digest).unwrap();
    let listed = index.participant_file(&configuration()).unwrap().unwrap();
    let target = expanded.join(&listed.path);
    let mut bytes = fs::read(&target).unwrap();
    bytes[0] ^= 1;
    fs::write(&target, bytes).unwrap();
    let error = index.read(&listed, 1 << 20, None).unwrap_err();
    assert_eq!(error.code, PortableV2ErrorCode::ConcurrentMutation);
}

#[test]
fn reads_and_scans_are_bounded() {
    let (_outputs, expanded, bundle, digest) = exported();
    for path in [&expanded, &bundle] {
        let index = open(path, &digest).unwrap();
        let listed = index.participant_file(&configuration()).unwrap().unwrap();
        let error = index.read(&listed, listed.length - 1, None).unwrap_err();
        assert_eq!(error.code, PortableV2ErrorCode::LimitExceeded);
        assert_eq!(error.entry.as_deref(), Some(listed.path.as_str()));
        index.read(&listed, listed.length, None).unwrap();

        let error = PortableV2PackageIndex::open(
            path,
            &digest,
            PortableV2Limits {
                max_entry_bytes: 8,
                ..PortableV2Limits::default()
            },
            None,
        )
        .unwrap_err();
        assert_eq!(error.code, PortableV2ErrorCode::LimitExceeded);
    }
}

#[cfg(unix)]
#[test]
fn symlinked_payload_is_never_followed() {
    let (_outputs, expanded, _bundle, digest) = exported();
    let listed = open(&expanded, &digest)
        .unwrap()
        .participant_file(&configuration())
        .unwrap()
        .unwrap();
    let target = expanded.join(&listed.path);
    let copy = expanded.parent().unwrap().join("outside-copy.json");
    fs::rename(&target, &copy).unwrap();
    std::os::unix::fs::symlink(&copy, &target).unwrap();
    let error = open(&expanded, &digest).unwrap_err();
    assert_eq!(error.code, PortableV2ErrorCode::InvalidStructure);
}
