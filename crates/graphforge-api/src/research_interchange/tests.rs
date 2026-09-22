use super::*;
use crate::{
    BranchSource, CancellationToken, CreateResearchBranchRequest, GraphForge, OperationId,
};
use uuid::Uuid;

#[test]
fn complete_research_roundtrip_preserves_version_and_historical_genealogy() {
    let cancellation = CancellationToken::new();
    let mut source = GraphForge::new(None).unwrap();
    source.execute("CREATE (:Item {x:7})").unwrap();
    let branch_uuid = Uuid::now_v7();
    let version_uuid = Uuid::now_v7();
    source
        .create_research_branch(
            &CreateResearchBranchRequest {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: source.generation_for_read().unwrap().generation_uuid(),
                branch_uuid,
                version_uuid,
                source: BranchSource::Current {
                    origin_version_uuid: Uuid::now_v7(),
                    context_uuid: Uuid::now_v7(),
                },
                creator_uuid: Uuid::now_v7(),
                created_at: 1,
                label: "exported study".into(),
            },
            &cancellation,
        )
        .unwrap();
    let before = source
        .research_reference(
            &ResearchReferenceTarget::Version { version_uuid },
            &cancellation,
        )
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let package = directory.path().join("study");
    source
        .export_research(
            &ExportResearchRequest {
                version_uuid,
                output: package.clone(),
                bundled: false,
                projection: None,
            },
            &cancellation,
        )
        .unwrap();
    let report = crate::verify_portable_v2(
        &crate::PortableVerifyRequest {
            input: package.clone(),
            mode: graphforge_core::portable::PortableV2Mode::Full,
            limits: graphforge_core::portable::PortableV2Limits::default(),
        },
        Some(cancellation.flag()),
    )
    .unwrap();
    assert!(report.research_interchange);
    let target = directory.path().join("imported");
    let import = crate::PortableV2ImportRequest {
        input: package,
        operation_id: OperationId(Uuid::now_v7()),
        limits: Default::default(),
    };
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let error =
        GraphForge::import_portable_v2(&target, &import, Some(cancelled.flag())).unwrap_err();
    assert_eq!(
        error.code,
        graphforge_core::portable::PortableV2ErrorCode::Cancelled
    );
    assert!(!target.exists());
    let first = GraphForge::import_portable_v2(&target, &import, None).unwrap();
    let replay = GraphForge::import_portable_v2(&target, &import, None).unwrap();
    assert!(replay.idempotent_replay);
    assert_eq!(first.generation_uuid, replay.generation_uuid);
    let imported = GraphForge::new(Some(target.to_str().unwrap())).unwrap();
    let after = imported
        .research_reference(
            &ResearchReferenceTarget::Version { version_uuid },
            &cancellation,
        )
        .unwrap();
    assert_eq!(after.version, before.version);
    assert_eq!(after.identity_sha256, before.identity_sha256);
    let registry = imported.research_version_retention().unwrap();
    let archive = registry.interchange.values().next().unwrap();
    let mut forged = archive.clone();
    forged
        .versions
        .get_mut(&version_uuid)
        .unwrap()
        .content
        .participants[0]
        .content_sha256 = [91; 32];
    assert!(
        forged.validate().is_err(),
        "same immutable identity cannot claim changed content"
    );
    let mut future = archive.clone();
    future.research_capability_version += 1;
    assert!(future.validate().is_err());
    // Integrity and closure rejection happen before a new destination is admitted.
    let cas = import
        .input
        .join("data/components/research/research-content");
    let entry = std::fs::read_dir(cas)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    std::fs::remove_file(entry).unwrap();
    let refused = directory.path().join("incomplete-import");
    assert!(GraphForge::import_portable_v2(&refused, &import, None).is_err());
    assert!(!refused.exists());

    assert_eq!(after.genealogy, before.genealogy);
    assert_eq!(
        source.generation_for_read().unwrap().generation_uuid(),
        before.resolved_generation_uuid
    );

    assert!(
        imported
            .research_reference(
                &ResearchReferenceTarget::Branch { branch_uuid },
                &cancellation
            )
            .is_err()
    );
}

#[test]
fn fork_has_independent_governance_metadata_and_exact_retry() {
    let cancel = CancellationToken::new();
    let mut source = GraphForge::new(None).unwrap();
    source.execute("CREATE (:Item {x:7})").unwrap();
    let version_uuid = Uuid::now_v7();
    source
        .create_research_branch(
            &CreateResearchBranchRequest {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: source.generation_for_read().unwrap().generation_uuid(),
                branch_uuid: Uuid::now_v7(),
                version_uuid,
                source: BranchSource::Current {
                    origin_version_uuid: Uuid::now_v7(),
                    context_uuid: Uuid::now_v7(),
                },
                creator_uuid: Uuid::now_v7(),
                created_at: 1,
                label: "origin".into(),
            },
            &cancel,
        )
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let mut metadata = graphforge_storage::WorkspaceResearchMetadata::empty();
    metadata.title = Some("Independent study".into());
    metadata.access.visibility = Some("private".into());
    metadata.access.access_policy = Some("Local team only".into());
    let request = ForkResearchRequest {
        operation_uuid: Uuid::now_v7(),
        project_uuid: Uuid::now_v7(),
        version_uuid,
        projection: None,
        target: directory.path().join("fork"),
        actor_uuid: Uuid::now_v7(),
        governance: "Independent review by local team".into(),
        adopt_selected_ontology: true,
        metadata: metadata.clone(),
    };
    let first = source.fork_research(&request, &cancel).unwrap();
    let again = source.fork_research(&request, &cancel).unwrap();
    assert!(again.idempotent_replay);
    assert_eq!(first.generation_uuid, again.generation_uuid);
    let fork = GraphForge::new(Some(request.target.to_str().unwrap())).unwrap();
    assert_eq!(fork.research_project_metadata().unwrap(), metadata);
    let origin = source.research_reference(&ResearchReferenceTarget::Version { version_uuid }, &cancel).unwrap();
    let citation = fork.research_reference(&ResearchReferenceTarget::Version { version_uuid }, &cancel).unwrap();
    assert_eq!(citation.origin_project_uuid, origin.origin_project_uuid);
    assert_eq!(citation.project_uuid, request.project_uuid);
    assert_ne!(citation.project_uuid, citation.origin_project_uuid);

    assert_eq!(
        crate::research_claims::authority::project_uuid(&fork.generation_for_read().unwrap())
            .unwrap(),
        request.project_uuid
    );
    assert_ne!(
        crate::research_claims::authority::project_uuid(&source.generation_for_read().unwrap())
            .unwrap(),
        request.project_uuid
    );
    check_projection_from_fork(&fork, version_uuid, directory.path());
    let mut duplicate = request.clone();
    duplicate.operation_uuid = Uuid::now_v7();
    duplicate.target = directory.path().join("duplicate-project");
    assert!(fork.fork_research(&duplicate, &cancel).is_err());
    assert!(!duplicate.target.exists());
    fork.execute("MATCH (n:Item) SET n.x=9").unwrap();
    let mut changed = request.clone();
    changed.governance = "Changed policy".into();
    assert!(source.fork_research(&changed, &cancel).is_err());
    let after = fork
        .research_reference(&ResearchReferenceTarget::Version { version_uuid }, &cancel)
        .unwrap();
    let before = source
        .research_reference(&ResearchReferenceTarget::Version { version_uuid }, &cancel)
        .unwrap();
    assert_eq!(after.version, before.version);
    assert_eq!(after.genealogy, before.genealogy);
}

#[test]
fn disjoint_and_redacted_exports_have_distinct_content_and_stable_transport_identity() {
    let cancel = CancellationToken::new();
    let mut source = GraphForge::new(None).unwrap();
    source
        .execute("CREATE (:Item {x:1, secret:'private-one'}), (:Item {x:2, secret:'private-two'})")
        .unwrap();
    let source_version = Uuid::now_v7();
    let branch = Uuid::now_v7();
    source
        .create_research_branch(
            &CreateResearchBranchRequest {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: source.generation_for_read().unwrap().generation_uuid(),
                branch_uuid: branch,
                version_uuid: source_version,
                source: BranchSource::Current {
                    origin_version_uuid: Uuid::now_v7(),
                    context_uuid: Uuid::now_v7(),
                },
                creator_uuid: Uuid::now_v7(),
                created_at: 1,
                label: "selected study".into(),
            },
            &cancel,
        )
        .unwrap();
    let version = source.research_version(source_version).unwrap();
    let view = crate::research_versions::materialize_version(&source, &version).unwrap();
    let all = crate::branches::fields::read(&view, &cancel).unwrap();
    let nodes: std::collections::BTreeSet<_> =
        all.keys().filter(|k| k.0 == "node").map(|k| k.1).collect();
    let directory = tempfile::tempdir().unwrap();
    let mut identities = std::collections::BTreeSet::new();
    for (index, id) in nodes.into_iter().enumerate() {
        let frozen = source
            .freeze_slice(
                &crate::SliceRequest {
                    request_uuid: Uuid::now_v7(),
                    source: crate::SliceSource::Version {
                        version_uuid: source_version,
                    },
                    selector: crate::SliceSelector::Direct {
                        members: crate::SliceMembers {
                            nodes: [id].into(),
                            ..Default::default()
                        },
                    },
                    include: Default::default(),
                    exclude: Default::default(),
                    limits: Default::default(),
                },
                &cancel,
            )
            .unwrap();
        let mut ipc = Vec::new();
        {
            let mut writer =
                arrow::ipc::writer::StreamWriter::try_new(&mut ipc, &frozen.schema).unwrap();
            for batch in frozen.batches {
                writer.write(&batch).unwrap();
            }
            writer.finish().unwrap();
        }
        let projection = ResearchExportProjection {
            version_uuid: Uuid::now_v7(),
            frozen_ipc: ipc,
            fields: all
                .keys()
                .filter(|k| k.0 == "node" && k.1 == id && k.2 != "property:secret")
                .map(|k| crate::ResearchFieldIdentity {
                    object_kind: k.0.clone(),
                    object_uuid: k.1,
                    field: k.2.clone(),
                })
                .collect(),
            created_at: 2,
        };
        let request = ExportResearchRequest {
            version_uuid: source_version,
            output: directory.path().join(format!("selected-{index}")),
            bundled: false,
            projection: Some(projection.clone()),
        };
        let expanded = source.export_research(&request, &cancel).unwrap();
        let mut bundled = request.clone();
        bundled.output = directory.path().join(format!("selected-{index}.gfpb"));
        bundled.bundled = true;
        let bundle = source.export_research(&bundled, &cancel).unwrap();
        assert_eq!(expanded.package_digest, bundle.package_digest);
        assert_eq!(expanded.selection_fingerprint, bundle.selection_fingerprint);
        let target = directory.path().join(format!("import-{index}"));
        GraphForge::import_portable_v2(
            &target,
            &crate::PortableV2ImportRequest {
                input: request.output,
                operation_id: OperationId(Uuid::now_v7()),
                limits: Default::default(),
            },
            Some(cancel.flag()),
        )
        .unwrap();
        let imported = GraphForge::new(Some(target.to_str().unwrap())).unwrap();
        let reference = imported
            .research_reference(
                &ResearchReferenceTarget::Version {
                    version_uuid: projection.version_uuid,
                },
                &cancel,
            )
            .unwrap();
        assert_eq!(
            reference.version.content.source_version,
            Some(source_version)
        );
        assert!(identities.insert(reference.identity_sha256));
        assert!(
            imported
                .research_reference(
                    &ResearchReferenceTarget::Version {
                        version_uuid: source_version
                    },
                    &cancel
                )
                .is_err()
        );
        let fields = crate::branches::fields::read(&imported, &cancel).unwrap();
        assert!(!fields.keys().any(|k| k.2 == "property:secret"));
        assert_eq!(
            fields
                .keys()
                .filter(|k| k.0 == "node")
                .map(|k| k.1)
                .collect::<std::collections::BTreeSet<_>>(),
            [id].into()
        );
    }
    assert_eq!(identities.len(), 2);
}

fn check_projection_from_fork(fork: &GraphForge, version_uuid: Uuid, directory: &std::path::Path) {
    let cancel = CancellationToken::new();
    let fields = crate::branches::fields::read(fork, &cancel).unwrap();
    let node = fields.keys().find(|key| key.0 == "node").unwrap().1;
    let frozen = fork
        .freeze_slice(
            &crate::SliceRequest {
                request_uuid: Uuid::now_v7(),
                source: crate::SliceSource::Version { version_uuid },
                selector: crate::SliceSelector::Direct {
                    members: crate::SliceMembers {
                        nodes: [node].into(),
                        ..Default::default()
                    },
                },
                include: Default::default(),
                exclude: Default::default(),
                limits: Default::default(),
            },
            &cancel,
        )
        .unwrap();
    let mut ipc = Vec::new();
    {
        let mut writer =
            arrow::ipc::writer::StreamWriter::try_new(&mut ipc, &frozen.schema).unwrap();
        for batch in frozen.batches {
            writer.write(&batch).unwrap();
        }
        writer.finish().unwrap();
    }
    let output = directory.join("fork-projection");
    fork.export_research(
        &ExportResearchRequest {
            version_uuid,
            output: output.clone(),
            bundled: false,
            projection: Some(ResearchExportProjection {
                version_uuid: Uuid::now_v7(),
                frozen_ipc: ipc,
                fields: fields
                    .keys()
                    .filter(|key| key.0 == "node" && key.1 == node && key.2 != "property:x")
                    .map(|key| crate::ResearchFieldIdentity {
                        object_kind: key.0.clone(),
                        object_uuid: key.1,
                        field: key.2.clone(),
                    })
                    .collect(),
                created_at: 3,
            }),
        },
        &cancel,
    )
    .unwrap();
    let report = crate::verify_portable_v2(
        &crate::PortableVerifyRequest {
            input: output,
            mode: graphforge_core::portable::PortableV2Mode::Full,
            limits: Default::default(),
        },
        None,
    )
    .unwrap();
    assert!(report.research_interchange);
}
