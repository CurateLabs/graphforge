#[cfg(unix)]
use super::super::tests::while_writer_lock_is_held;
use super::super::tests::{create_request, preserve_checkpoint_intent_after_lock_handoff};
use super::super::*;
use super::*;
use std::process::Command;
use tempfile::tempdir;

fn install_registry_intent(
    checkpoint_root: &Path,
    previous: Option<&Registry>,
    next: &Registry,
) -> RegistryIntent {
    fs::create_dir_all(checkpoint_root).unwrap();
    let transaction_uuid = Uuid::now_v7();
    let next_bytes = next.canonical_bytes().unwrap();
    let next_sha256 = hex(&Sha256::digest(&next_bytes).into());
    let registry_temp = format!(".registry.{transaction_uuid}.json.next");
    let checksum_temp = format!(".registry.{transaction_uuid}.sha256.next");
    fs::write(checkpoint_root.join(&registry_temp), &next_bytes).unwrap();
    fs::write(
        checkpoint_root.join(&checksum_temp),
        format!("{next_sha256}\n"),
    )
    .unwrap();
    let intent = RegistryIntent {
        transaction_uuid,
        previous_revision: previous.map(|registry| registry.revision),
        previous_sha256: previous
            .map(|registry| hex(&Sha256::digest(registry.canonical_bytes().unwrap()).into())),
        next_revision: next.revision,
        next_sha256,
        registry_temp,
        checksum_temp,
    };
    let mut intent_bytes = serde_json::to_vec(&intent).unwrap();
    intent_bytes.push(b'\n');
    fs::write(checkpoint_root.join(INTENT_FILE), intent_bytes).unwrap();
    intent
}

fn write_raw_registry(root: &Path, registry: &Registry) {
    let checkpoint_root = root.join(CHECKPOINTS_DIR);
    let mut bytes = serde_json::to_vec(registry).unwrap();
    bytes.push(b'\n');
    fs::write(checkpoint_root.join(REGISTRY_FILE), &bytes).unwrap();
    fs::write(
        checkpoint_root.join(CHECKSUM_FILE),
        format!("{}\n", hex(&Sha256::digest(&bytes).into())),
    )
    .unwrap();
}

#[test]
fn checksummed_but_impossible_registry_identities_fail_closed() {
    let directory = tempdir().unwrap();
    crate::open_or_initialize_project(directory.path()).unwrap();
    create_checkpoint(
        directory.path(),
        &create_request(Uuid::now_v7(), "Tampered"),
    )
    .unwrap();
    let checkpoint_root = directory.path().join(CHECKPOINTS_DIR);
    let mut registry = read_registry(&checkpoint_root).unwrap();
    registry.active[0].checkpoint_uuid = Uuid::now_v7();
    write_raw_registry(directory.path(), &registry);
    assert_eq!(
        list_checkpoints(directory.path()).unwrap_err().code(),
        "GF_CHECKPOINT_REGISTRY_CORRUPT"
    );

    let directory = tempdir().unwrap();
    crate::open_or_initialize_project(directory.path()).unwrap();
    create_checkpoint(directory.path(), &create_request(Uuid::now_v7(), "Overlap")).unwrap();
    delete_checkpoint(
        directory.path(),
        &CheckpointDeleteRequest {
            operation_uuid: Uuid::now_v7(),
            name: "Overlap".into(),
            actor_uuid: None,
        },
    )
    .unwrap();
    let checkpoint_root = directory.path().join(CHECKPOINTS_DIR);
    let mut registry = read_registry(&checkpoint_root).unwrap();
    let row = &mut registry.tombstones[0];
    row.delete_operation_uuid = row.create_operation_uuid;
    row.delete_request_sha256 = hex(&delete_request_digest_values(
        row.delete_operation_uuid,
        &row.name,
        row.deleted_by,
    ));
    write_raw_registry(directory.path(), &registry);
    assert_eq!(
        list_checkpoints(directory.path()).unwrap_err().code(),
        "GF_CHECKPOINT_REGISTRY_CORRUPT"
    );
}

#[cfg(unix)]
#[test]
fn linked_registry_surfaces_fail_closed_without_following_targets() {
    use std::os::unix::fs::symlink;

    for hard in [false, true] {
        let directory = tempdir().unwrap();
        crate::open_or_initialize_project(directory.path()).unwrap();
        create_checkpoint(directory.path(), &create_request(Uuid::now_v7(), "Linked")).unwrap();
        let checkpoint_root = directory.path().join(CHECKPOINTS_DIR);
        let checksum = checkpoint_root.join(CHECKSUM_FILE);
        let external = directory.path().join("external-checksum");
        fs::rename(&checksum, &external).unwrap();
        if hard {
            fs::hard_link(&external, &checksum).unwrap();
        } else {
            symlink(&external, &checksum).unwrap();
        }
        let external_before = fs::read(&external).unwrap();
        let error = while_writer_lock_is_held(directory.path(), || {
            list_checkpoints(directory.path()).unwrap_err()
        });
        assert_eq!(error.code(), "GF_CHECKPOINT_REGISTRY_CORRUPT");
        assert_eq!(fs::read(&external).unwrap(), external_before);
    }
}

#[test]
fn wave9_durable_registry_intent_recovers_every_atomic_pair_boundary() {
    for boundary in [
        "first-staged",
        "previous-staged",
        "registry-replaced",
        "next-complete",
    ] {
        let directory = tempdir().unwrap();
        crate::open_or_initialize_project(directory.path()).unwrap();
        let checkpoint_root = directory.path().join(CHECKPOINTS_DIR);
        let previous = Registry::empty();
        let mut next = Registry::empty();
        next.revision = 1;

        let has_previous = boundary != "first-staged";
        fs::create_dir_all(&checkpoint_root).unwrap();
        if has_previous {
            write_raw_registry(directory.path(), &previous);
        }
        let intent =
            install_registry_intent(&checkpoint_root, has_previous.then_some(&previous), &next);
        match boundary {
            "first-staged" | "previous-staged" => {}
            "registry-replaced" => {
                fs::rename(
                    checkpoint_root.join(&intent.registry_temp),
                    checkpoint_root.join(REGISTRY_FILE),
                )
                .unwrap();
            }
            "next-complete" => {
                fs::rename(
                    checkpoint_root.join(&intent.registry_temp),
                    checkpoint_root.join(REGISTRY_FILE),
                )
                .unwrap();
                fs::rename(
                    checkpoint_root.join(&intent.checksum_temp),
                    checkpoint_root.join(CHECKSUM_FILE),
                )
                .unwrap();
            }
            _ => unreachable!(),
        }

        recover_pair(&checkpoint_root).unwrap();

        assert!(!checkpoint_root.join(INTENT_FILE).exists(), "{boundary}");
        assert!(
            !checkpoint_root.join(&intent.registry_temp).exists(),
            "{boundary}"
        );
        assert!(
            !checkpoint_root.join(&intent.checksum_temp).exists(),
            "{boundary}"
        );
        let recovered = read_registry(&checkpoint_root).unwrap();
        let expected_revision =
            usize::from(matches!(boundary, "registry-replaced" | "next-complete"));
        assert_eq!(recovered.revision, expected_revision as u64, "{boundary}");
    }
}

#[test]
fn wave9_registry_intent_rejects_unsafe_names_and_wrong_staged_revision() {
    let directory = tempdir().unwrap();
    crate::open_or_initialize_project(directory.path()).unwrap();
    let checkpoint_root = directory.path().join(CHECKPOINTS_DIR);
    let mut next = Registry::empty();
    next.revision = 1;
    let mut intent = install_registry_intent(&checkpoint_root, None, &next);
    intent.registry_temp = "../registry.json".into();
    let mut bytes = serde_json::to_vec(&intent).unwrap();
    bytes.push(b'\n');
    fs::write(checkpoint_root.join(INTENT_FILE), bytes).unwrap();
    assert_eq!(
        recover_pair(&checkpoint_root).unwrap_err().code(),
        "GF_CHECKPOINT_REGISTRY_CORRUPT"
    );

    fs::remove_dir_all(&checkpoint_root).unwrap();
    fs::create_dir(&checkpoint_root).unwrap();
    let mut intent = install_registry_intent(&checkpoint_root, None, &next);
    intent.next_revision = 2;
    let mut bytes = serde_json::to_vec(&intent).unwrap();
    bytes.push(b'\n');
    fs::write(checkpoint_root.join(INTENT_FILE), bytes).unwrap();
    assert_eq!(
        recover_pair(&checkpoint_root).unwrap_err().code(),
        "GF_CHECKPOINT_REGISTRY_CORRUPT"
    );
}

#[test]
fn recovery_rejects_missing_or_tampered_staged_pair() {
    for tamper_checksum in [false, true] {
        let directory = tempdir().unwrap();
        crate::open_or_initialize_project(directory.path()).unwrap();
        create_checkpoint(directory.path(), &create_request(Uuid::now_v7(), "Base")).unwrap();
        let status = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "project_checkpoints::tests::checkpoint_failpoint_helper",
                "--ignored",
            ])
            .env(
                "GRAPHFORGE_PROJECT_FAILPOINTS",
                "graphforge-internal-subprocess-v1",
            )
            .env(
                "GRAPHFORGE_PROJECT_FAILPOINT",
                "checkpoint.registry.before_replace",
            )
            .env("GRAPHFORGE_CHECKPOINT_TEST_ROOT", directory.path())
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(crate::project_failpoint::exit_code()));
        preserve_checkpoint_intent_after_lock_handoff(
            directory.path(),
            &format!(
                "action=create-tamper failpoint=checkpoint.registry.before_replace \
                     committed=false tamper_checksum={tamper_checksum}"
            ),
        );
        let checkpoint_root = directory.path().join(CHECKPOINTS_DIR);
        let intent_bytes = fs::read(checkpoint_root.join(INTENT_FILE)).unwrap();
        let intent: RegistryIntent = serde_json::from_slice(&intent_bytes).unwrap();
        if tamper_checksum {
            fs::write(
                checkpoint_root.join(intent.checksum_temp),
                b"0000000000000000000000000000000000000000000000000000000000000000\n",
            )
            .unwrap();
        } else {
            fs::remove_file(checkpoint_root.join(intent.registry_temp)).unwrap();
        }
        assert_eq!(
            list_checkpoints(directory.path()).unwrap_err().code(),
            "GF_CHECKPOINT_REGISTRY_CORRUPT"
        );
    }
}

#[test]
fn checkpoint_text_and_identity_boundaries_are_canonical() {
    for invalid in [
        "",
        " leading",
        "trailing ",
        ".",
        "..",
        "two  spaces",
        "bad/name",
    ] {
        assert_eq!(validate_name(invalid).unwrap_err().code(), "GF_VALIDATION");
    }
    assert_eq!(validate_name("Résumé.v1").unwrap(), "Résumé.v1");
    let combining_acute = char::from_u32(0x301).unwrap();
    let decomposed = format!("Re{combining_acute}sume{combining_acute}");
    assert_eq!(
        validate_name(&decomposed).unwrap_err().code(),
        "GF_VALIDATION"
    );
    assert!(validate_description(None).is_ok());
    assert!(validate_description(Some("bounded description")).is_ok());
    assert_eq!(
        validate_description(Some("contains\ncontrol"))
            .unwrap_err()
            .code(),
        "GF_VALIDATION"
    );
    assert_eq!(
        validate_description(Some(&"x".repeat(MAX_DESCRIPTION_BYTES + 1)))
            .unwrap_err()
            .code(),
        "GF_VALIDATION"
    );
    assert_eq!(
        validate_reason("  restored after audit  ").unwrap(),
        "restored after audit"
    );
    assert_eq!(validate_reason("   ").unwrap_err().code(), "GF_VALIDATION");

    let operation = Uuid::now_v7();
    let actor = Uuid::now_v7();
    let digest = create_request_digest_values(operation, "baseline", Some("desc"), Some(actor));
    let checkpoint = checkpoint_uuid(operation, digest);
    let encoded = hex(&digest);
    assert_eq!(decode_digest(&encoded).unwrap(), digest);
    assert!(
        validate_record_identity(
            checkpoint,
            operation,
            "baseline",
            Some("desc"),
            Some(actor),
            &encoded,
        )
        .is_ok()
    );
    assert_eq!(
        validate_record_identity(
            Uuid::nil(),
            operation,
            "baseline",
            Some("desc"),
            Some(actor),
            &encoded,
        )
        .unwrap_err()
        .code(),
        "GF_CHECKPOINT_REGISTRY_CORRUPT"
    );
    for malformed in ["0", &"A".repeat(64), &"g".repeat(64)] {
        assert_eq!(
            decode_digest(malformed).unwrap_err().code(),
            "GF_CHECKPOINT_REGISTRY_CORRUPT"
        );
    }
    assert!(valid_private_name(
        &format!(".registry.{operation}.json.next"),
        operation,
        "json"
    ));
    assert!(!valid_private_name(
        ".registry.other.json.next",
        operation,
        "json"
    ));
    assert_eq!(
        parse_uuid("not-a-uuid").unwrap_err().code(),
        "GF_CHECKPOINT_REGISTRY_CORRUPT"
    );
}

#[test]
fn empty_checkpoint_registry_has_stable_canonical_bytes() {
    let registry = Registry::empty();
    let first = registry.canonical_bytes().unwrap();
    let second = registry.canonical_bytes().unwrap();
    assert_eq!(first, second);
    assert!(first.ends_with(b"\n"));
    let decoded: Registry = serde_json::from_slice(&first).unwrap();
    assert_eq!(decoded, registry);
}

#[test]
fn checkpoint_operation_identities_remain_disjoint_across_tombstones() {
    let root = tempdir().unwrap();
    crate::open_or_initialize_project(root.path()).unwrap();
    let create_operation = Uuid::now_v7();
    let create = create_request(create_operation, "Baseline");
    let created = create_checkpoint(root.path(), &create).unwrap();
    let exact_create_replay = create_checkpoint(root.path(), &create).unwrap();
    assert_eq!(exact_create_replay, created);

    let changed_create = CheckpointCreateRequest {
        name: "Changed".into(),
        ..create.clone()
    };
    assert_eq!(
        create_checkpoint(root.path(), &changed_create)
            .unwrap_err()
            .code(),
        "GF_IDEMPOTENCY_CONFLICT"
    );
    assert_eq!(list_checkpoints(root.path()).unwrap().len(), 1);

    let delete_operation = Uuid::now_v7();
    let delete = CheckpointDeleteRequest {
        operation_uuid: delete_operation,
        name: "Baseline".into(),
        actor_uuid: create.actor_uuid,
    };
    let deleted = delete_checkpoint(root.path(), &delete).unwrap();
    let exact_delete_replay = delete_checkpoint(root.path(), &delete).unwrap();
    assert_eq!(exact_delete_replay, deleted);
    assert!(list_checkpoints(root.path()).unwrap().is_empty());

    let tombstone_create_replay = create_checkpoint(root.path(), &create).unwrap();
    assert_eq!(
        tombstone_create_replay.checkpoint_uuid,
        created.checkpoint_uuid
    );
    assert_eq!(tombstone_create_replay, created);
    assert!(list_checkpoints(root.path()).unwrap().is_empty());

    let changed_delete = CheckpointDeleteRequest {
        name: "Other".into(),
        ..delete.clone()
    };
    assert_eq!(
        delete_checkpoint(root.path(), &changed_delete)
            .unwrap_err()
            .code(),
        "GF_IDEMPOTENCY_CONFLICT"
    );
    assert_eq!(
        create_checkpoint(root.path(), &create_request(delete_operation, "Other"))
            .unwrap_err()
            .code(),
        "GF_IDEMPOTENCY_CONFLICT"
    );
    assert_eq!(
        delete_checkpoint(
            root.path(),
            &CheckpointDeleteRequest {
                operation_uuid: create_operation,
                name: "Missing".into(),
                actor_uuid: None,
            },
        )
        .unwrap_err()
        .code(),
        "GF_IDEMPOTENCY_CONFLICT"
    );
    assert_eq!(
        delete_checkpoint(
            root.path(),
            &CheckpointDeleteRequest {
                operation_uuid: Uuid::now_v7(),
                name: "Missing".into(),
                actor_uuid: None,
            },
        )
        .unwrap_err()
        .code(),
        "GF_CHECKPOINT_NOT_FOUND"
    );
    assert!(list_checkpoints(root.path()).unwrap().is_empty());
}

#[test]
fn registry_header_sort_and_revision_validation_matrix_uses_durable_records() {
    let directory = tempdir().unwrap();
    crate::open_or_initialize_project(directory.path()).unwrap();
    create_checkpoint(
        directory.path(),
        &create_request(Uuid::from_u128(701), "Alpha"),
    )
    .unwrap();
    create_checkpoint(
        directory.path(),
        &create_request(Uuid::from_u128(702), "Beta"),
    )
    .unwrap();
    let root = directory.path().join(CHECKPOINTS_DIR);
    let stable = read_registry(&root).unwrap();
    assert!(validate_registry(&stable).is_ok());

    let mutations: Vec<Box<dyn Fn(&mut Registry)>> = vec![
        Box::new(|registry| registry.format = "future".into()),
        Box::new(|registry| registry.format_version = 2),
        Box::new(|registry| registry.active.reverse()),
        Box::new(|registry| registry.active[0].created_revision = 0),
        Box::new(|registry| registry.active[1].name = registry.active[0].name.clone()),
        Box::new(|registry| {
            registry.active[1].checkpoint_uuid = registry.active[0].checkpoint_uuid
        }),
        Box::new(|registry| {
            registry.active[1].create_operation_uuid = registry.active[0].create_operation_uuid
        }),
    ];
    for mutate in mutations {
        let mut candidate = stable.clone();
        mutate(&mut candidate);
        assert_eq!(
            validate_registry(&candidate).unwrap_err().code(),
            "GF_CHECKPOINT_REGISTRY_CORRUPT"
        );
    }
    assert_eq!(read_registry(&root).unwrap(), stable);
}

#[test]
fn tombstone_identity_revision_and_operation_disjointness_matrix_is_total() {
    let directory = tempdir().unwrap();
    crate::open_or_initialize_project(directory.path()).unwrap();
    create_checkpoint(
        directory.path(),
        &create_request(Uuid::from_u128(801), "Deleted"),
    )
    .unwrap();
    delete_checkpoint(
        directory.path(),
        &CheckpointDeleteRequest {
            operation_uuid: Uuid::from_u128(802),
            name: "Deleted".into(),
            actor_uuid: None,
        },
    )
    .unwrap();
    let root = directory.path().join(CHECKPOINTS_DIR);
    let stable = read_registry(&root).unwrap();
    assert_eq!(stable.active.len(), 0);
    assert_eq!(stable.tombstones.len(), 1);
    assert!(validate_registry(&stable).is_ok());

    let mutations: Vec<Box<dyn Fn(&mut Registry)>> = vec![
        Box::new(|registry| registry.tombstones[0].name = " bad".into()),
        Box::new(|registry| registry.tombstones[0].description = Some("bad\nvalue".into())),
        Box::new(|registry| registry.tombstones[0].generation_manifest_sha256 = "bad".into()),
        Box::new(|registry| registry.tombstones[0].create_request_sha256 = "bad".into()),
        Box::new(|registry| registry.tombstones[0].delete_request_sha256 = "bad".into()),
        Box::new(|registry| registry.tombstones[0].checkpoint_uuid = Uuid::nil()),
        Box::new(|registry| registry.tombstones[0].created_revision = 0),
        Box::new(|registry| {
            registry.tombstones[0].deleted_revision = registry.tombstones[0].created_revision
        }),
        Box::new(|registry| registry.tombstones[0].deleted_revision = registry.revision + 1),
        Box::new(|registry| {
            registry.tombstones[0].delete_operation_uuid =
                registry.tombstones[0].create_operation_uuid
        }),
        Box::new(|registry| registry.revision = 0),
    ];
    for mutate in mutations {
        let mut candidate = stable.clone();
        mutate(&mut candidate);
        assert_eq!(
            validate_registry(&candidate).unwrap_err().code(),
            "GF_CHECKPOINT_REGISTRY_CORRUPT"
        );
    }

    let mut duplicate = stable.clone();
    let mut second = duplicate.tombstones[0].clone();
    second.deleted_revision += 1;
    duplicate.revision = second.deleted_revision;
    duplicate.tombstones.push(second);
    assert_eq!(
        validate_registry(&duplicate).unwrap_err().code(),
        "GF_CHECKPOINT_REGISTRY_CORRUPT"
    );
    assert_eq!(read_registry(&root).unwrap(), stable);
}
