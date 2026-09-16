use super::super::tests::{node_batch, nonempty_project_generation_two, open};
use super::super::*;
use super::*;

use tempfile::TempDir;

#[test]
fn unsupported_or_incomplete_checkpoint_is_refused_before_recovery_mutation() {
    for case in [0, 6, 7, 8, 9, 11, FORMAT_VERSION] {
        let root = TempDir::new().unwrap();
        let operation = Uuid::new_v4();
        let mut session = open(&root, operation.as_u128());
        session
            .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 2))
            .unwrap();
        session.checkpoint.format_version = case;
        if case == FORMAT_VERSION {
            session.checkpoint.evidence.immutable_artifacts = 0;
        }
        replace_checkpoint_control(&session.root, &session.checkpoint).unwrap();
        let private = session.root.path().to_owned();
        let temporary = private.join(control_temp(CHECKPOINT));
        std::fs::write(&temporary, b"unfinished authority").unwrap();
        let before = std::fs::read(private.join(CHECKPOINT)).unwrap();
        let current = std::fs::read(root.path().join("CURRENT")).unwrap();
        drop(session);
        assert!(
            GraphConstructionSession::open(
                root.path(),
                operation,
                0,
                GraphConstructionBudgets::default()
            )
            .is_err()
        );
        assert_eq!(std::fs::read(private.join(CHECKPOINT)).unwrap(), before);
        assert_eq!(std::fs::read(temporary).unwrap(), b"unfinished authority");
        assert_eq!(std::fs::read(root.path().join("CURRENT")).unwrap(), current);
    }
}

#[test]
fn shape_control_encoding_fails_closed_above_its_separate_cap() {
    let oversized = "x".repeat((MAX_SHAPE_CONTROL_BYTES + 1) as usize);
    let error = encode_control(&oversized, SHAPE_INTENT).unwrap_err();
    assert!(error.to_string().contains("control record exceeds bound"));
    assert_eq!(control_limit(CHECKPOINT), MAX_CONTROL_BYTES);
}

#[test]
fn current_parent_phase_checkpoint_refuses_omission_without_rewrite() {
    let project = nonempty_project_generation_two();
    let operation = Uuid::new_v4();
    let mut session = GraphConstructionSession::open(
        project.path(),
        operation,
        2,
        GraphConstructionBudgets::default(),
    )
    .unwrap();
    assert!(session.evidence().authentication_read_bytes > 0);
    for shaped in [false, true] {
        let mut malformed = session.checkpoint.clone();
        malformed.evidence.seal_application_read_bytes = 0;
        if shaped {
            malformed.shape_authority_sha256 = Some("0".repeat(64));
        }
        assert!(validate_parent_phase_bytes(&malformed).is_err());
        assert_eq!(malformed.evidence.seal_application_read_bytes, 0);
    }
    session.checkpoint.evidence.seal_application_read_bytes = 0;
    replace_checkpoint_control(&session.root, &session.checkpoint).unwrap();
    let path = session.root.path().join(CHECKPOINT);
    let before = std::fs::read(&path).unwrap();
    drop(session);
    assert!(
        GraphConstructionSession::open(
            project.path(),
            operation,
            2,
            GraphConstructionBudgets::default()
        )
        .is_err()
    );
    assert_eq!(std::fs::read(path).unwrap(), before);
}
