// Sessions an earlier build recorded in a retired format are refused with a
// typed error before anything of them runs, and are discarded without being
// opened (#1964).
mod retired_formats {
    use super::*;

    fn checkpoint_path(project: &Path, operation: Uuid) -> PathBuf {
        project
            .join(PRIVATE_ROOT)
            .join(operation.simple().to_string())
            .join(CHECKPOINT)
    }

    /// Rewrite the checkpoint of a closed session, as another build recorded it.
    fn record_as_an_earlier_build(
        project: &Path,
        operation: Uuid,
        edit: impl FnOnce(&mut serde_json::Value),
    ) {
        let path = checkpoint_path(project, operation);
        let mut value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        edit(&mut value);
        std::fs::write(path, serde_json::to_vec(&value).unwrap()).unwrap();
    }

    fn assert_refused_then_discarded(project: &Path, operation: Uuid, parent: u64) {
        let refused = GraphConstructionSession::open(
            project,
            operation,
            parent,
            GraphConstructionBudgets::default(),
        )
        .err()
        .expect("a retired session must not open");
        assert!(matches!(refused, GfError::Validation(_)), "{refused:?}");
        assert!(refused.to_string().contains("restart the import"), "{refused}");
        // Refusing it again and discarding it need no open: the tree is removed.
        assert!(crate::discard_retired_construction(project, operation).unwrap());
        assert!(!checkpoint_path(project, operation).exists());
        assert!(
            !project
                .join(PRIVATE_ROOT)
                .join(operation.simple().to_string())
                .exists()
        );
        // Nothing is left to discard.
        assert!(!crate::discard_retired_construction(project, operation).unwrap());
    }

    /// An append recorded by a build whose budgets named the node-index bound
    /// hashed that field into its finish-stage controls; this build serializes
    /// the budgets without it, so such a session could only fail with a digest
    /// error. It is refused with a typed error and stays discardable, and an
    /// append recorded without the field is unaffected.
    #[test]
    fn an_append_recorded_with_the_retired_node_index_bound_is_refused_and_discarded() {
        let project = nonempty_project_with_nodes(2);
        let operation = Uuid::from_u128(0x7e7_1964);
        let mut session = GraphConstructionSession::open(
            project.path(),
            operation,
            1,
            GraphConstructionBudgets::default(),
        )
        .unwrap();
        session
            .append(ConstructionChunkKind::Node, "node", &node_batch(3, 1))
            .unwrap();
        drop(session);

        // Unmarked, the session reopens and is not retired.
        let reopened = GraphConstructionSession::open(
            project.path(),
            operation,
            1,
            GraphConstructionBudgets::default(),
        )
        .unwrap();
        assert_eq!(reopened.accepted_chunks(), 1);
        drop(reopened);
        assert!(!crate::discard_retired_construction(project.path(), operation).unwrap());

        record_as_an_earlier_build(project.path(), operation, |checkpoint| {
            checkpoint["budgets"]["max_node_index_bytes"] = serde_json::json!(1_073_741_824_u64);
        });
        assert_refused_then_discarded(project.path(), operation, 1);
    }

    #[test]
    fn routes_only_an_earlier_build_recorded_are_decoded_to_be_refused_and_discarded() {
        for (index, (field, value)) in [("chunk_route", "staged"), ("seal_route", "replay_staged")]
            .into_iter()
            .enumerate()
        {
            let project = TempDir::new().unwrap();
            crate::open_or_initialize_project(project.path()).unwrap();
            let operation = Uuid::from_u128(0x7e7_2964 + index as u128);
            let mut session = GraphConstructionSession::open(
                project.path(),
                operation,
                0,
                GraphConstructionBudgets::default(),
            )
            .unwrap();
            session
                .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 3))
                .unwrap();
            drop(session);
            record_as_an_earlier_build(project.path(), operation, |checkpoint| {
                checkpoint[field] = serde_json::json!(value);
            });
            assert_refused_then_discarded(project.path(), operation, 0);
        }
    }

    fn staged_retired_session(operation: Uuid) -> TempDir {
        let project = TempDir::new().unwrap();
        crate::open_or_initialize_project(project.path()).unwrap();
        let mut session = GraphConstructionSession::open(
            project.path(),
            operation,
            0,
            GraphConstructionBudgets::default(),
        )
        .unwrap();
        session
            .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 3))
            .unwrap();
        drop(session);
        record_as_an_earlier_build(project.path(), operation, |checkpoint| {
            checkpoint["budgets"]["max_node_index_bytes"] = serde_json::json!(1_073_741_824_u64);
        });
        project
    }

    /// Discarding a retired session never gives up exclusion early: a process
    /// that holds the session's lock (a live build from the earlier binary)
    /// keeps its session, and the discard proceeds once the lock is free.
    #[test]
    fn a_retired_session_another_process_holds_is_not_discarded() {
        let operation = Uuid::from_u128(0x7e7_3964);
        let project = staged_retired_session(operation);
        let lock_path = project
            .path()
            .join(PRIVATE_ROOT)
            .join(operation.simple().to_string())
            .join(SESSION_LOCK);
        let holder = std::fs::File::open(&lock_path).unwrap();
        assert!(crate::file_lock::try_lock_exclusive(&holder).unwrap());
        let refused = crate::discard_retired_construction(project.path(), operation)
            .unwrap_err()
            .to_string();
        assert!(refused.contains("locked by another process"), "{refused}");
        // Nothing of the held session was touched.
        let checkpoint: serde_json::Value =
            serde_json::from_slice(&std::fs::read(checkpoint_path(project.path(), operation)).unwrap())
                .unwrap();
        assert_eq!(checkpoint["state"], "staging");
        crate::file_lock::unlock(&holder).unwrap();
        drop(holder);
        assert!(crate::discard_retired_construction(project.path(), operation).unwrap());
    }

    /// The record says aborted, durably, before anything is unlinked: a discard
    /// that cannot finish leaves a session that can only be discarded.
    #[cfg(unix)]
    #[test]
    fn a_retired_discard_that_cannot_finish_leaves_an_aborted_record() {
        use std::os::unix::fs::PermissionsExt;
        let operation = Uuid::from_u128(0x7e7_4964);
        let project = staged_retired_session(operation);
        let session_dir = project
            .path()
            .join(PRIVATE_ROOT)
            .join(operation.simple().to_string());
        // A directory inside the session that cannot be emptied.
        let stuck = session_dir.join("stuck");
        std::fs::create_dir(&stuck).unwrap();
        std::fs::write(stuck.join("entry"), b"x").unwrap();
        std::fs::set_permissions(&stuck, std::fs::Permissions::from_mode(0o555)).unwrap();
        // Root is not stopped by permissions; there the interruption cannot be made.
        let enforced = std::fs::remove_file(stuck.join("entry")).is_err();
        if enforced {
            assert!(crate::discard_retired_construction(project.path(), operation).is_err());
            let checkpoint: serde_json::Value = serde_json::from_slice(
                &std::fs::read(checkpoint_path(project.path(), operation)).unwrap(),
            )
            .unwrap();
            assert_eq!(checkpoint["state"], "aborted");
            assert!(checkpoint.get("publication_state").is_none());
        }
        std::fs::set_permissions(&stuck, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(crate::discard_retired_construction(project.path(), operation).unwrap());
        assert!(!session_dir.exists());
    }

    /// A checkpoint that is not JSON is corrupt: refused, not taken for current.
    #[test]
    fn a_checkpoint_that_is_not_json_is_not_taken_for_a_current_record() {
        let operation = Uuid::from_u128(0x7e7_5964);
        let project = staged_retired_session(operation);
        let path = checkpoint_path(project.path(), operation);
        std::fs::write(&path, b"not json").unwrap();
        assert!(crate::discard_retired_construction(project.path(), operation).is_err());
        assert!(path.exists());
    }
}
