use super::super::tests::{journal_path, participant, project, publish, request};
use super::super::*;
use super::*;
use std::fs;

#[test]
fn request_fingerprint_is_independent_of_participant_input_order() {
    let mut request = request(vec![
        participant("provenance", "events", b"provenance"),
        participant("graph", "nodes", b"graph"),
    ]);
    let (_, first_metadata, first_fingerprint) = request_metadata(&request).unwrap();
    request.participants.reverse();
    let (_, second_metadata, second_fingerprint) = request_metadata(&request).unwrap();

    assert_eq!(first_metadata, second_metadata);
    assert_eq!(first_fingerprint, second_fingerprint);
    assert_eq!(first_metadata[0].capability_id, "graph");
    assert_eq!(first_metadata[1].capability_id, "provenance");
}

#[test]
fn machine_ids_match_the_committed_generation_reader_contract() {
    let root = project();
    let valid = request(vec![participant("graph", "node-properties", b"properties")]);
    let generation_uuid = valid.generation_uuid;
    publish(root.path(), valid);
    let resolved = resolve_project_generation(root.path()).unwrap();
    assert_eq!(resolved.generation_uuid(), generation_uuid);
    assert!(
        resolved
            .participant_path("graph", "node-properties")
            .unwrap()
            .is_file()
    );

    let underscore = request(vec![participant(
        "graph_data",
        "node_properties",
        b"properties",
    )]);
    publish(root.path(), underscore);
    let resolved = resolve_project_generation(root.path()).unwrap();
    assert!(
        resolved
            .participant_path("graph_data", "node_properties")
            .unwrap()
            .is_file()
    );

    let invalid = request(vec![participant("graph", "NodeProperties", b"properties")]);
    let error = stage_project_generation(root.path(), &invalid)
        .err()
        .expect("reader-incompatible machine ID must be rejected");
    assert_eq!(error.code(), "GF_PUBLICATION_FAILED");
}

#[test]
fn tampered_staged_bytes_fail_before_publication() {
    let root = project();
    let parent = resolve_project_generation(root.path())
        .unwrap()
        .generation_uuid();
    let initial_request = request(vec![participant("graph", "nodes", b"original")]);
    let ProjectStageOutcome::Staged(staged) =
        stage_project_generation(root.path(), &initial_request).unwrap()
    else {
        panic!("new request unexpectedly replayed");
    };
    std::fs::write(
        staged.generation_root.join(PARTICIPANTS_DIR).join(
            staged
                .participants
                .first()
                .expect("participant")
                .relative_path
                .as_str(),
        ),
        b"tampered",
    )
    .unwrap();

    let error = staged
        .validate(|_| Ok(()), |_, _| Ok(()))
        .err()
        .expect("tampered bytes must fail validation");

    assert_eq!(error.code(), "GF_PUBLICATION_FAILED");
    assert_eq!(
        resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid(),
        parent
    );

    let request = request(vec![participant("graph", "nodes", b"original")]);
    let ProjectStageOutcome::Staged(staged) =
        stage_project_generation(root.path(), &request).unwrap()
    else {
        panic!("new request unexpectedly replayed");
    };
    let path = staged
        .generation_root
        .join(PARTICIPANTS_DIR)
        .join(&staged.participants[0].relative_path);
    std::fs::write(path, b"short").unwrap();
    assert_eq!(
        staged
            .validate(|_| Ok(()), |_, _| Ok(()))
            .err()
            .expect("truncated staged bytes must fail")
            .code(),
        "GF_PUBLICATION_FAILED"
    );
    assert_eq!(
        resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid(),
        parent
    );
}

#[cfg(unix)]
#[test]
fn staged_participant_hard_link_fails_before_current_mutation() {
    let root = project();
    let parent = resolve_project_generation(root.path())
        .unwrap()
        .generation_uuid();
    let request = request(vec![participant("graph", "nodes", b"stable")]);
    let ProjectStageOutcome::Staged(staged) =
        stage_project_generation(root.path(), &request).unwrap()
    else {
        panic!("unexpected replay")
    };
    let path = staged
        .generation_root
        .join(PARTICIPANTS_DIR)
        .join(&staged.participants[0].relative_path);
    let external = root.path().join("external-participant");
    fs::rename(&path, &external).unwrap();
    fs::hard_link(&external, &path).unwrap();

    assert_eq!(
        staged
            .validate(|_| Ok(()), |_, _| Ok(()))
            .err()
            .expect("hard-linked staged bytes must fail")
            .code(),
        "GF_PUBLICATION_FAILED"
    );
    assert_eq!(
        resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid(),
        parent
    );
}

#[test]
fn malformed_generation_contracts_fail_before_staging_or_current_change() {
    let root = project();
    let before = fs::read(root.path().join(CURRENT_FILE)).unwrap();

    let mut cases = Vec::new();
    let mut no_capability = request(vec![]);
    no_capability.capabilities.clear();
    cases.push((no_capability, "at least one capability"));

    let mut zero_capability = request(vec![]);
    zero_capability.capabilities[0].capability_version = 0;
    cases.push((zero_capability, "capability contract versions"));

    let mut missing_graph = request(vec![]);
    missing_graph.capabilities[0].capability_id = "knowledge".into();
    cases.push((missing_graph, "graph capability version 1"));

    let mut duplicate_capability = request(vec![]);
    duplicate_capability
        .capabilities
        .push(duplicate_capability.capabilities[0].clone());
    cases.push((duplicate_capability, "duplicate capability identity"));

    let mut undeclared = request(vec![participant("knowledge", "events", b"event")]);
    undeclared
        .capabilities
        .retain(|entry| entry.capability_id == "graph");
    cases.push((undeclared, "participant capability is not declared"));

    let mut version_mismatch = request(vec![participant("knowledge", "events", b"event")]);
    version_mismatch.participants[0].capability_version = 2;
    cases.push((version_mismatch, "version conflicts with declaration"));

    let duplicate = participant("graph", "nodes", b"same");
    cases.push((
        request(vec![duplicate.clone(), duplicate]),
        "duplicate participant identity",
    ));

    let mut zero_record = request(vec![participant("graph", "nodes", b"node")]);
    zero_record.participants[0].record_version = 0;
    cases.push((zero_record, "participant contract versions"));

    let mut invalid_id = request(vec![participant("graph", "nodes", b"node")]);
    invalid_id.participants[0].record_family_id = "../nodes".into();
    cases.push((invalid_id, "machine ID"));

    for (candidate, expected) in cases {
        let error = stage_project_generation(root.path(), &candidate)
            .err()
            .expect("malformed request must fail");
        assert_eq!(error.code(), "GF_PUBLICATION_FAILED");
        assert!(error.to_string().contains(expected), "{error}");
        assert_eq!(fs::read(root.path().join(CURRENT_FILE)).unwrap(), before);
        assert!(!journal_path(root.path(), candidate.transaction_uuid).exists());
    }
}

#[test]
fn staged_participant_file_kind_matrix_fails_before_current_mutation() {
    for kind in ["missing", "directory", "symlink"] {
        let root = project();
        let parent = resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid();
        let request = request(vec![participant("graph", "nodes", b"stable")]);
        let ProjectStageOutcome::Staged(staged) =
            stage_project_generation(root.path(), &request).unwrap()
        else {
            panic!("unexpected replay")
        };
        let path = staged
            .generation_root
            .join(PARTICIPANTS_DIR)
            .join(&staged.participants.first().unwrap().relative_path);
        std::fs::remove_file(&path).unwrap();
        match kind {
            "missing" => {}
            "directory" => std::fs::create_dir(&path).unwrap(),
            "symlink" => {
                #[cfg(unix)]
                std::os::unix::fs::symlink(root.path().join(CURRENT_FILE), &path).unwrap();
                #[cfg(not(unix))]
                std::fs::create_dir(&path).unwrap();
            }
            _ => unreachable!(),
        }
        let error = match staged.validate(|_| Ok(()), |_, _| Ok(())) {
            Ok(_) => panic!("hostile staged participant must fail"),
            Err(error) => error,
        };
        let expected_code = if kind == "missing" {
            "GF_IO"
        } else {
            "GF_PUBLICATION_FAILED"
        };
        assert_eq!(error.code(), expected_code);
        assert_eq!(
            resolve_project_generation(root.path())
                .unwrap()
                .generation_uuid(),
            parent
        );
    }
}
