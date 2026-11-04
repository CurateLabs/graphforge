//! Repeated import and durable staging of verified ontology composition.

use super::*;

#[test]
fn composed_project_reexport_reimports_with_current_package_staging() {
    let (_package_parent, package) = composition_package();
    let directory = tempfile::tempdir().unwrap();
    let first_target = directory.path().join("first");
    let limits = PortableLimits::default();
    let first = import_complete_portable(
        &package,
        &first_target,
        Uuid::new_v4(),
        Uuid::new_v4(),
        &supported(),
        limits,
        None,
    )
    .unwrap();
    let first_generation = crate::resolve_project_generation(&first_target).unwrap();
    let previous_staging = load_portable_ontology_staging(&first_generation, limits)
        .unwrap()
        .unwrap();
    assert_eq!(previous_staging.package_digest, first.package_digest);
    let plan = crate::plan_complete_portable(&first_generation, limits).unwrap();

    for (name, representation) in [
        ("expanded", crate::PortableOutput::Expanded),
        ("bundle", crate::PortableOutput::Bundle),
    ] {
        let exported = directory.path().join(name);
        crate::export_complete_portable(
            &plan,
            &exported,
            representation,
            limits,
            &AtomicBool::new(false),
            |_| {},
        )
        .unwrap();
        let target = directory.path().join(format!("{name}-imported"));
        let transaction = Uuid::new_v4();
        let generation = Uuid::new_v4();
        let imported = import_complete_portable(
            &exported,
            &target,
            transaction,
            generation,
            &supported(),
            limits,
            None,
        )
        .unwrap();
        let reopened = crate::resolve_project_generation(&target).unwrap();
        let staging = load_portable_ontology_staging(&reopened, limits)
            .unwrap()
            .unwrap();
        assert_eq!(staging.package_digest, imported.package_digest);
        assert_ne!(staging.package_digest, previous_staging.package_digest);
        assert_eq!(staging.composition, previous_staging.composition);
        let bindings = crate::semantic_storage_bindings(&reopened)
            .unwrap()
            .unwrap();
        assert_eq!(
            bindings.composition_fingerprint,
            staging.composition.composition_fingerprint
        );
        let descriptors = reopened.participant_descriptors().unwrap();
        assert_eq!(
            descriptors
                .iter()
                .filter(|descriptor| {
                    descriptor.capability_id == crate::WORKSPACE_CAPABILITY_ID
                        && descriptor.record_family_id
                            == crate::WORKSPACE_PORTABLE_ONTOLOGY_STAGING_FAMILY
                })
                .count(),
            1
        );
        let replay = import_complete_portable(
            &exported,
            &target,
            transaction,
            generation,
            &supported(),
            limits,
            None,
        )
        .unwrap();
        assert!(replay.publication.idempotent_replay);
        assert_eq!(replay.publication.generation_uuid, generation);
    }
}

#[test]
fn durable_composition_staging_reopens_replays_and_fails_without_partial_authority() {
    let (_package_parent, package) = composition_package();
    let parent = tempfile::tempdir().unwrap();
    let target = parent.path().join("imported");
    let transaction = Uuid::new_v4();
    let generation = Uuid::new_v4();
    let paths = crate::StorageAllocationOperation::project_paths(&target).unwrap();
    let allocation = crate::StorageAllocationOperation::from_paths(&paths).unwrap();
    let first = import_complete_portable_with_allocation(
        &package,
        &target,
        transaction,
        generation,
        &supported(),
        PortableLimits::default(),
        None,
        |_| {},
        Some(&allocation),
    )
    .unwrap();
    let expected = crate::StorageAllocationOperation::from_paths(&paths).unwrap();
    let observed = serde_json::to_value(allocation.snapshot().unwrap()).unwrap();
    let expected = serde_json::to_value(expected.snapshot().unwrap()).unwrap();
    assert_eq!(observed["active"], expected["active"]);
    assert_eq!(observed["owners"], expected["owners"]);
    assert_eq!(
        first.transient_peak_allocated_bytes,
        allocation.totals().unwrap().1
    );
    assert!(first.staged_composition.is_some());
    assert!(first.materialized_cleanup.parent_sync_confirmed);
    assert_eq!(
        first.materialized_cleanup.removed_identity_allocated_bytes,
        first.materialized_identity_allocated_bytes
    );
    let reopened = crate::resolve_project_generation(&target).unwrap();
    let staged = load_portable_ontology_staging(&reopened, PortableLimits::default())
        .unwrap()
        .expect("verified candidate must survive reopen");
    assert_eq!(staged.package_digest, first.package_digest);
    assert!(
        reopened
            .participant_snapshots()
            .unwrap()
            .iter()
            .any(|entry| {
                entry.record_family_id == crate::WORKSPACE_ONTOLOGY_COMPOSITION_FAMILY
            })
    );
    assert!(
        crate::semantic_storage_bindings(&reopened)
            .unwrap()
            .is_some()
    );

    let replay = import_complete_portable(
        &package,
        &target,
        transaction,
        generation,
        &supported(),
        PortableLimits::default(),
        None,
    )
    .unwrap();
    assert_eq!(replay.publication.generation_uuid, generation);
    assert!(replay.materialized_cleanup.parent_sync_confirmed);
    assert_eq!(
        replay.materialized_cleanup.removed_identity_allocated_bytes,
        replay.materialized_identity_allocated_bytes
    );
    let conflict = import_complete_portable(
        &package,
        &target,
        transaction,
        Uuid::new_v4(),
        &supported(),
        PortableLimits::default(),
        None,
    )
    .unwrap_err();
    assert_eq!(conflict.code, PortableErrorCode::ConcurrentMutation);
    assert_eq!(
        crate::resolve_project_generation(&target)
            .unwrap()
            .generation_uuid(),
        generation
    );

    for (name, capabilities, cancelled, expected) in [
        (
            "cancelled",
            supported(),
            AtomicBool::new(true),
            PortableErrorCode::Cancelled,
        ),
        (
            "unsupported",
            Vec::new(),
            AtomicBool::new(false),
            PortableErrorCode::Incompatible,
        ),
    ] {
        let failed = parent.path().join(name);
        let transaction = Uuid::new_v4();
        let error = import_complete_portable(
            &package,
            &failed,
            transaction,
            Uuid::new_v4(),
            &capabilities,
            PortableLimits::default(),
            Some(&cancelled),
        )
        .unwrap_err();
        assert_eq!(error.code, expected, "{name}");
        assert!(!failed.exists(), "{name} published a target");
        let residue = parent
            .path()
            .join(format!(".{name}.portable-{}", transaction.hyphenated()));
        assert!(!residue.exists(), "{name} retained private staging");
        let owner = residue.with_file_name(format!(
            ".{name}.portable-{}.owner",
            transaction.hyphenated()
        ));
        assert!(!owner.exists(), "{name} retained its ownership marker");
    }
}
