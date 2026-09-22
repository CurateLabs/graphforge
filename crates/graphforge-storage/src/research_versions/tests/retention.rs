//! Real storage/graph retention with explicitly labeled future-consumer roots.
use super::*;

fn publish_graph(root: &Path, nodes: u128) -> Uuid {
    crate::open_or_initialize_project(root).unwrap();
    let graph = tempfile::tempdir().unwrap();
    let mut writer = crate::GraphWriter::open_at(
        graph.path(),
        graphforge_core::OntologyMode::Exploratory,
        1_700_000_000_000_000,
    )
    .unwrap();
    for id in 1..=nodes {
        let uuid = Uuid::from_u128(id);
        writer
            .create_node(
                uuid,
                graphforge_value::EntityTypeId::ontology(graphforge_core::TypeId(7)).unwrap(),
            )
            .unwrap();
        writer
            .set_properties(
                &uuid,
                None,
                std::collections::HashMap::from([(
                    "value".into(),
                    graphforge_ir::IrLiteral::Str(format!("fixed value {id:08}")),
                )]),
            )
            .unwrap();
    }
    writer
        .create_edge(
            Uuid::from_u128(1_000_000),
            "LINK",
            &Uuid::from_u128(1),
            &Uuid::from_u128(2),
        )
        .unwrap();
    writer.flush().unwrap();
    drop(writer);
    let (inventory, _) = crate::capture_graph_files(graph.path()).unwrap();
    let lease = crate::begin_graph_object_publication(root).unwrap();
    let graph_root = retained_content::install_graph(&lease, graph.path(), &inventory).unwrap();
    let mut participants = vec![
        crate::graph_files::graph_files_root_participant(&graph_root).unwrap(),
        ProjectParticipant {
            capability_id: "workspace".into(),
            capability_version: 1,
            record_family_id: "ontology_fixture".into(),
            record_version: 1,
            encoding: ProjectParticipantEncoding::Json,
            schema_fingerprint: Sha256::digest(b"ontology-retention-fixture/1").into(),
            row_count: 1,
            bytes: br#"{"ontology_fixture":"fixed immutable ontology content"}"#.to_vec(),
        },
    ];
    let mut capabilities = vec![
        ProjectCapability {
            capability_id: "graph".into(),
            capability_version: 1,
        },
        ProjectCapability {
            capability_id: "workspace".into(),
            capability_version: 1,
        },
    ];
    let registry = state(root);
    if !registry.receipts.is_empty() {
        participants.push(registry.participant().unwrap());
        capabilities.push(ProjectCapability {
            capability_id: RESEARCH_CAPABILITY.into(),
            capability_version: RESEARCH_VERSION,
        });
    }
    let request = ProjectGenerationRequest {
        transaction_uuid: Uuid::now_v7(),
        generation_uuid: Uuid::now_v7(),
        capabilities,
        participants,
    };
    let ProjectStageOutcome::Staged(staged) =
        crate::stage_project_generation(root, &request).unwrap()
    else {
        panic!("unexpected replay")
    };
    staged
        .validate(|_| Ok(()), |_, _| Ok(()))
        .unwrap()
        .publish_with_graph_objects(&lease)
        .unwrap();
    request.generation_uuid
}

fn cleanup(root: &Path) {
    crate::project_retention::execute_project_cleanup(
        root,
        crate::project_retention::ProjectRetentionPolicy {
            retained_ancestors: 0,
        },
        crate::project_retention::ProjectRetentionLimits::default(),
    )
    .unwrap();
    crate::project_recovery::recover_project_on_open(root).unwrap();
}

fn mutate(root: &Path, mutation: ResearchMutation) -> ResearchOperationReceipt {
    execute(
        root,
        &ResearchOperation {
            operation_uuid: Uuid::now_v7(),
            expected_generation_uuid: current(root),
            mutation,
        },
    )
}

#[test]
fn fixed_projection_reclaims_growing_parent_and_reopens_exact_content() {
    let mut measurements = Vec::new();
    let mut retained_totals = Vec::new();
    let mut control_copies = Vec::new();
    for count in [32, 512] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let original_generation = publish_graph(root, count);
        let context = Uuid::now_v7();
        let request = register(root, context);
        let receipt = execute(root, &request);
        let origin_id = receipt.version_uuid.unwrap();
        let origin = state(root).versions[&origin_id].clone();
        let selected_id = Uuid::now_v7();
        mutate(
            root,
            ResearchMutation::RegisterGraphProjection {
                spec: RegisterResearchVersion {
                    version_uuid: selected_id,
                    context_uuid: context,
                    source_generation_uuid: original_generation,
                    source_version: Some(origin_id),
                    selection: Some(
                        origin
                            .content
                            .participants
                            .iter()
                            .map(|p| p.key.clone())
                            .collect(),
                    ),
                    required_versions: BTreeSet::new(),
                    label: Some("selected fixture".into()),
                    description: None,
                    created_at: 2,
                    evidence: Vec::new(),
                },
                selection: ResearchGraphSelection {
                    nodes: BTreeSet::new(),
                    edges: BTreeSet::from([Uuid::from_u128(1_000_000)]),
                    induced_edges: false,
                    exclude_properties: BTreeSet::new(),
                },
            },
        );
        let selected = state(root).versions[&selected_id].clone();
        let projection = selected.content.graph_projection.as_ref().unwrap();
        measurements.push((
            projection.source_payload_bytes,
            projection.selected_payload_bytes,
        ));
        let source_inventory = crate::resolve_generation_by_uuid(root, original_generation)
            .unwrap()
            .graph_files_inventory()
            .unwrap()
            .unwrap();
        let route_control_bytes = source_inventory
            .files
            .iter()
            .find(|entry| entry.relative_path == crate::route_component::TABLE_FILE)
            .unwrap()
            .byte_length;
        assert_eq!(
            projection.source_materialization_bytes_copied,
            route_control_bytes
        );
        control_copies.push(route_control_bytes);
        assert!(projection.source_materialization_bytes_reused > 0);
        let before = inspect_research_version(root, &selected).unwrap();
        mutate(
            root,
            ResearchMutation::DeleteVersion {
                version_uuid: origin_id,
            },
        );
        publish_graph(root, 2);
        let before_cleanup = cas_bytes(root);
        cleanup(root);
        let retained = cas_bytes(root);
        assert!(retained < before_cleanup);
        retained_totals.push(retained);
        assert!(crate::resolve_generation_by_uuid(root, original_generation).is_err());
        assert_eq!(inspect_research_version(root, &selected).unwrap(), before);
        assert_eq!(execute(root, &request), receipt);
        let output = tempfile::tempdir().unwrap();
        materialize_research_graph(root, &selected, output.path()).unwrap();
        assert_eq!(
            crate::graph_projection::projected_graph_fingerprint(output.path()).unwrap(),
            projection.fingerprint
        );
        let outside = tempfile::tempdir().unwrap();
        assert!(
            crate::graph_projection::materialize_portable_graph_tree_projection(
                output.path(),
                outside.path(),
                &crate::GraphProjectionSelection {
                    node_uuids: BTreeSet::from([*Uuid::from_u128(3).as_bytes()]),
                    ..Default::default()
                }
            )
            .is_err()
        );
    }
    assert!(measurements[1].0 > measurements[0].0);
    assert_eq!(measurements[1].1, measurements[0].1);
    assert_eq!(retained_totals[0], retained_totals[1]);
    assert_eq!(control_copies[0], control_copies[1]);
    eprintln!(
        "fixed selection total retained CAS bytes: {retained_totals:?}; only route controls copied: {control_copies:?}"
    );
    eprintln!("fixed selected graph: parent/retained bytes {measurements:?}");
}

#[test]
fn complete_materialization_releases_generation_without_releasing_graph_or_receipts() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let source = publish_graph(root, 32);
    let request = register(root, Uuid::now_v7());
    let receipt = execute(root, &request);
    let id = receipt.version_uuid.unwrap();
    let before = state(root).versions[&id].clone();
    let snapshots = inspect_research_version(root, &before).unwrap();
    mutate(
        root,
        ResearchMutation::Compact {
            versions: BTreeSet::from([id]),
        },
    );
    assert_eq!(state(root).versions[&id], before);
    publish_graph(root, 2);
    cleanup(root);
    assert!(crate::resolve_generation_by_uuid(root, source).is_err());
    assert_eq!(inspect_research_version(root, &before).unwrap(), snapshots);
    assert_eq!(execute(root, &request), receipt);
    let output = tempfile::tempdir().unwrap();
    materialize_research_graph(root, &before, output.path()).unwrap();
    let outside = tempfile::tempdir().unwrap();
    crate::graph_projection::materialize_portable_graph_tree_projection(
        output.path(),
        outside.path(),
        &crate::GraphProjectionSelection {
            node_uuids: BTreeSet::from([*Uuid::from_u128(32).as_bytes()]),
            ..Default::default()
        },
    )
    .unwrap();
}

fn cas_bytes(root: &Path) -> u64 {
    fn size(path: &Path) -> u64 {
        std::fs::read_dir(path)
            .unwrap()
            .map(|e| {
                let e = e.unwrap();
                if e.path().is_dir() {
                    size(&e.path())
                } else {
                    e.metadata().unwrap().len()
                }
            })
            .sum()
    }
    size(&root.join("graph-objects/sha256"))
}

#[test]
fn fixed_parent_growing_frozen_roots_release_payloads_and_keep_accepted_dependencies() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    publish_graph(root, 32);
    let context = Uuid::now_v7();
    let mut operations = Vec::new();
    let mut versions = Vec::new();
    let mut frozen_roots = Vec::new();
    for index in 0_u8..6 {
        let mut request = register(root, context);
        let (sha256, byte_length) =
            crate::install_project_object_bytes(root, &vec![index; 1024]).unwrap();
        if let ResearchMutation::Register(spec) = &mut request.mutation {
            spec.evidence.push(ResearchEvidenceReference::Local {
                artifact_uuid: Uuid::now_v7(),
                sha256,
                byte_length,
            });
        }
        let receipt = execute(root, &request);
        let id = receipt.version_uuid.unwrap();
        operations.push((request, receipt));
        versions.push(id);
        let root_uuid = Uuid::now_v7();
        mutate(
            root,
            ResearchMutation::RetainRoot {
                root: ResearchRetentionRoot {
                    root_uuid,
                    kind: ResearchRootKind::FrozenProposal,
                    versions: BTreeSet::from([id]),
                },
            },
        );
        frozen_roots.push(root_uuid);
    }
    mutate(
        root,
        ResearchMutation::Compact {
            versions: versions.iter().copied().collect(),
        },
    );
    cleanup(root);
    let before_bytes = cas_bytes(root);
    let accepted = state(root).versions[&versions[1]].clone();
    let accepted_bytes = inspect_research_version(root, &accepted).unwrap();
    let accepted_root = Uuid::now_v7();
    mutate(
        root,
        ResearchMutation::RetainRoot {
            root: ResearchRetentionRoot {
                root_uuid: accepted_root,
                kind: ResearchRootKind::AcceptedProvenance,
                versions: BTreeSet::from([versions[1]]),
            },
        },
    );
    for (index, root_uuid) in frozen_roots.iter().enumerate() {
        assert!(
            state(root)
                .deletion_blockers(versions[index])
                .contains(&format!("root:{root_uuid}"))
        );
        mutate(
            root,
            ResearchMutation::ReleaseRoot {
                root_uuid: *root_uuid,
            },
        );
        if index != 1 && index != versions.len() - 1 {
            mutate(
                root,
                ResearchMutation::DeleteVersion {
                    version_uuid: versions[index],
                },
            );
        }
    }
    cleanup(root);
    let after_bytes = cas_bytes(root);
    assert_eq!(state(root).versions.len(), 2);
    assert!(
        after_bytes + 4 * 1024 == before_bytes,
        "released distinct Artifact bytes must be reclaimed while graph bytes remain shared"
    );
    assert_eq!(
        inspect_research_version(root, &accepted).unwrap(),
        accepted_bytes
    );
    assert_eq!(
        state(root).deletion_blockers(versions[1]),
        vec![format!("root:{accepted_root}")]
    );
    for (request, receipt) in operations {
        assert_eq!(execute(root, &request), receipt);
    }
    eprintln!("fixed-parent frozen-root history CAS bytes: {before_bytes} -> {after_bytes}");
}

#[test]
fn corrupt_materialized_closure_refuses_cleanup_before_releasing_unrelated_generations() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let old = publish_graph(root, 32);
    let id = execute(root, &register(root, Uuid::now_v7()))
        .version_uuid
        .unwrap();
    mutate(
        root,
        ResearchMutation::Compact {
            versions: BTreeSet::from([id]),
        },
    );
    publish_graph(root, 2);
    let version = state(root).versions[&id].clone();
    let p = &version.content.participants[0];
    let path = crate::graph_object_path(root, &hex(&p.content_sha256)).unwrap();
    crate::graph_object_store::corrupt_sealed_graph_object_for_test(
        &path,
        b"corrupt CAS registry payload",
    );
    assert!(
        crate::project_retention::execute_project_cleanup(
            root,
            crate::project_retention::ProjectRetentionPolicy {
                retained_ancestors: 0
            },
            crate::project_retention::ProjectRetentionLimits::default()
        )
        .is_err()
    );
    assert!(crate::resolve_generation_by_uuid(root, old).is_ok());
}

#[test]
fn compact_legacy_backslash_paths_materialize_through_native_routes() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let digest = compact_fixture_path(root, "legacy retained bytes", "properties\\Person.parquet");
    let id = execute(root, &register(root, Uuid::now_v7()))
        .version_uuid
        .unwrap();
    mutate(
        root,
        ResearchMutation::Compact {
            versions: BTreeSet::from([id]),
        },
    );
    let version = state(root).versions[&id].clone();
    compact_fixture(root, "new current fixture");
    cleanup(root);
    let output = tempfile::tempdir().unwrap();
    materialize_research_graph(root, &version, output.path()).unwrap();
    let inventory = crate::capture_graph_files(output.path()).unwrap().0;
    let table = crate::graph_files::authenticate_route_table(output.path(), &inventory).unwrap();
    let entry = inventory
        .files
        .iter()
        .find(|e| e.content_sha256 == digest)
        .unwrap();
    assert_eq!(
        table.semantic_relative_path(&entry.relative_path).unwrap(),
        "properties/Person.parquet"
    );
    assert!(!entry.relative_path.contains('\\'));
    let file = std::fs::File::open(output.path().join(&entry.relative_path)).unwrap();
    let rows = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap()
        .map(|batch| batch.unwrap().num_rows())
        .sum::<usize>();
    assert_eq!(rows, 1);
}

#[test]
fn compaction_faults_recover_before_cleanup_and_replay_the_original_receipt() {
    for after in [false, true] {
        for returned_error in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let root = temp.path();
            compact_fixture(root, "compaction fault graph");
            let id = execute(root, &register(root, Uuid::now_v7()))
                .version_uuid
                .unwrap();
            let request = ResearchOperation {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: current(root),
                mutation: ResearchMutation::Compact {
                    versions: BTreeSet::from([id]),
                },
            };
            let cancelled_head = current(root);
            assert!(publish_research_operation(root, &request, &AtomicBool::new(true)).is_err());
            assert_eq!(current(root), cancelled_head);
            let boundary = if after {
                "project.after_current_replace"
            } else {
                "project.before_current_replace"
            };
            let failpoint = if returned_error {
                format!("{boundary}.error")
            } else {
                boundary.into()
            };
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "research_versions::tests::research_operation_subprocess",
                    "--nocapture",
                ])
                .env("GRAPHFORGE_RESEARCH_TEST_ROOT", root)
                .env(
                    "GRAPHFORGE_RESEARCH_TEST_REQUEST",
                    serde_json::to_string(&request).unwrap(),
                )
                .env("GRAPHFORGE_RESEARCH_TEST_COMMITTED", after.to_string())
                .env(
                    "GRAPHFORGE_PROJECT_FAILPOINTS",
                    "graphforge-internal-subprocess-v1",
                )
                .env("GRAPHFORGE_PROJECT_FAILPOINT", failpoint)
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(if returned_error { 0 } else { 86 }));
            crate::project_recovery::recover_project_on_open(root).unwrap();
            assert_eq!(state(root).materialized.contains(&id), after);
            assert_eq!(
                state(root).receipts.contains_key(&request.operation_uuid),
                after
            );
            let receipt = execute(root, &request);
            cleanup(root);
            assert_eq!(execute(root, &request), receipt);
            inspect_research_version(root, &state(root).versions[&id]).unwrap();
        }
    }
}

#[test]
fn research_cleanup_with_busy_cas_guard_returns_without_mutation_or_waiting() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    compact_fixture(root, "busy research GC fixture");
    let id = execute(root, &register(root, Uuid::now_v7()))
        .version_uuid
        .unwrap();
    mutate(
        root,
        ResearchMutation::Compact {
            versions: BTreeSet::from([id]),
        },
    );
    let before = current(root);
    let guard = crate::graph_object_store::begin_graph_object_gc(root).unwrap();
    let error = crate::project_retention::execute_project_cleanup(
        root,
        crate::project_retention::ProjectRetentionPolicy {
            retained_ancestors: 0,
        },
        crate::project_retention::ProjectRetentionLimits::default(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("CAS lifecycle guard"));
    assert_eq!(current(root), before);
    drop(guard);
    cleanup(root);
    inspect_research_version(root, &state(root).versions[&id]).unwrap();
}

#[test]
fn object_root_project_restore_and_private_materialization_do_not_attach_a_graph_tree() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    publish_graph(root, 2);
    let context = Uuid::now_v7();
    let version = execute(root, &register(root, context))
        .version_uuid
        .unwrap();
    let original = state(root).versions[&version].clone();
    let private = tempfile::tempdir().unwrap();
    let hydrated = super::super::project_restore::materialize_research_project(
        root,
        &original,
        private.path(),
    )
    .unwrap();
    assert_eq!(
        hydrated.graph_files_inventory().unwrap(),
        crate::resolve_project_generation(root)
            .unwrap()
            .graph_files_inventory()
            .unwrap()
    );
    publish_graph(root, 4);
    mutate(
        root,
        ResearchMutation::RestoreProject {
            context_uuid: context,
            source_version: version,
            version_uuid: Uuid::now_v7(),
            created_at: 3,
        },
    );
    let restored = crate::resolve_project_generation(root).unwrap();
    assert_eq!(
        restored.graph_files_inventory().unwrap(),
        hydrated.graph_files_inventory().unwrap()
    );
    assert!(!restored.graph_tree_root().exists());
}

#[test]
fn branch_selection_is_prepared_without_an_intermediate_authoritative_head() {
    let project = tempfile::tempdir().unwrap();
    let root = project.path();
    let generation = publish_graph(root, 32);
    let origin_id = execute(root, &register(root, Uuid::now_v7()))
        .version_uuid
        .unwrap();
    let origin = state(root).versions[&origin_id].clone();
    let before = current(root);
    let branch_uuid = Uuid::now_v7();
    let version_uuid = Uuid::now_v7();
    let spec = RegisterResearchVersion {
        version_uuid,
        context_uuid: branch_uuid,
        source_generation_uuid: generation,
        source_version: Some(origin_id),
        selection: Some(
            origin
                .content
                .participants
                .iter()
                .map(|p| p.key.clone())
                .collect(),
        ),
        required_versions: BTreeSet::new(),
        label: Some("prepared selected Branch".into()),
        description: None,
        created_at: 11,
        evidence: Vec::new(),
    };
    let selection = ResearchGraphSelection {
        nodes: BTreeSet::from([Uuid::from_u128(1)]),
        edges: BTreeSet::new(),
        induced_edges: false,
        exclude_properties: BTreeSet::new(),
    };
    let prepared = prepare_branch_selection(
        root,
        &spec,
        None,
        Some(&selection),
        &[],
        &AtomicBool::new(false),
    )
    .unwrap();
    assert_eq!(current(root), before);
    assert!(!state(root).heads.contains_key(&branch_uuid));
    assert!(!state(root).versions.contains_key(&version_uuid));
    let projection = prepared.version.content.graph_projection.as_ref().unwrap();
    assert!(projection.selected_payload_bytes < projection.source_payload_bytes);
    assert_eq!(projection.selection, selection);
    let private = tempfile::tempdir().unwrap();
    let selected = materialize_prepared_branch(root, &prepared, private.path()).unwrap();
    assert!(
        selected
            .graph_files_inventory()
            .unwrap()
            .unwrap()
            .total_byte_length
            < projection.source_payload_bytes
    );
    assert_eq!(current(root), before);
    let creation = ResearchBranchRecord {
        branch_uuid,
        project_uuid: Uuid::now_v7(),
        parent_branch_uuid: None,
        origin_version_uuid: origin_id,
        base_version_uuid: version_uuid,
        creator_uuid: Uuid::now_v7(),
        created_at: 11,
        label: "selected".into(),
        selection_sha256: [7; 32],
    };
    mutate(
        root,
        ResearchMutation::PublishBranch {
            intent_sha256: [0; 32],
            origin_capture: None,
            creation: Some(creation),
            version: Box::new(prepared.version.clone()),
        },
    );
    assert_eq!(state(root).heads[&branch_uuid], version_uuid);
    assert_eq!(state(root).versions[&origin_id], origin);
}
