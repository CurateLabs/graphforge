//! A participant's payload is found only where verification requires it to be.

use super::*;

#[test]
fn a_participant_payload_is_found_only_in_its_component_kind() {
    let participant = crate::project_portable::RuntimeParticipant {
        participant_id: "workspace-configuration-0b4c69ad58c5".into(),
        capability_id: "workspace".into(),
        capability_version: 1,
        record_family_id: "configuration".into(),
        record_version: 1,
        encoding: "json".into(),
        schema_fingerprint: "0".repeat(64),
        row_count: 0,
    };
    let stage = tempfile::tempdir().unwrap();
    let place = |kind: &str| {
        let directory = stage
            .path()
            .join("data/components")
            .join(kind)
            .join("workspace-configuration-0b4c69ad58c5");
        fs::create_dir_all(&directory).unwrap();
        fs::write(directory.join("participant.json"), b"{}").unwrap();
    };
    place("evidence");
    let error =
        find_participant_file(stage.path(), &participant, PortableLimits::default()).unwrap_err();
    assert_eq!(error.code, PortableErrorCode::Incompatible);
    place("settings");
    let found =
        find_participant_file(stage.path(), &participant, PortableLimits::default()).unwrap();
    assert!(found.starts_with(stage.path().join("data/components/settings")));
}
