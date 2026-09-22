//! Selected export carries accepted history without promoting it to local authority.
use super::*;
use graphforge_storage::research_versions::ResearchProposalDecision::{Accept, Defer};

#[test]
fn selected_accepted_lineage_roundtrips_without_private_ancestor_or_live_acceptance() {
    let directory = tempfile::tempdir().unwrap();
    let (graph, proposal) = fixture(&directory.path().join("source"));
    let node_uuid = proposal.fields[0].object_uuid;
    let branch_uuid = proposal.source_branch_uuid;
    let version_uuid = proposal.source_version_uuid;
    let before = graph.research_version_retention().unwrap();
    assert_eq!(before.proposals.accepted.len(), 2);
    let mapping = before
        .proposals
        .accepted
        .values()
        .find(|m| m.unit.field == "property:score")
        .unwrap()
        .clone();
    let omitted = before
        .proposals
        .accepted
        .values()
        .find(|m| m.unit.field == "property:other")
        .unwrap();
    assert_eq!(mapping.proof_version_uuid, omitted.proof_version_uuid);
    let original_proof = crate::research_versions::materialize_version(
        &graph,
        &before.versions[&mapping.proof_version_uuid],
    )
    .unwrap();
    assert!(
        crate::branches::fields::read(&original_proof, &CancellationToken::new())
            .unwrap()
            .contains_key(&("node".into(), node_uuid, "property:other".into()))
    );
    let projection_uuid = Uuid::now_v7();
    let package = directory.path().join("selected");
    export_selected(&graph, &proposal, node_uuid, projection_uuid, &package);
    assert_no_private_parquet_values(&package);
    let target = directory.path().join("imported");
    GraphForge::import_portable_v2(
        &target,
        &PortableV2ImportRequest {
            input: package,
            operation_id: OperationId(Uuid::now_v7()),
            limits: Default::default(),
        },
        None,
    )
    .unwrap();
    graphforge_storage::execute_project_cleanup(
        &target,
        graphforge_storage::ProjectRetentionPolicy {
            retained_ancestors: 0,
        },
        graphforge_storage::ProjectRetentionLimits::default(),
    )
    .unwrap();
    let imported = GraphForge::new(target.to_str()).unwrap();
    let registry = imported.research_version_retention().unwrap();
    assert!(registry.proposals.accepted.is_empty());
    assert!(registry.branches.is_empty());
    assert!(registry.heads.is_empty());
    let archive = &registry.interchange[&projection_uuid];
    assert_eq!(archive.accepted.len(), 1);
    assert_eq!(archive.accepted[&mapping.mapping_uuid], mapping);
    assert_eq!(
        serde_json::to_vec(&archive.accepted[&mapping.mapping_uuid]).unwrap(),
        serde_json::to_vec(&mapping).unwrap()
    );
    assert!(!archive.accepted.contains_key(&omitted.mapping_uuid));
    let proof_uuid = archive.proof_exports[&mapping.proof_version_uuid];
    assert_ne!(proof_uuid, mapping.proof_version_uuid);
    assert_eq!(registry.versions.len(), 2);
    for id in [
        version_uuid,
        before.branches[&branch_uuid].base_version_uuid,
        before.branches[&branch_uuid].origin_version_uuid,
        mapping.proof_version_uuid,
    ] {
        assert!(!registry.versions.contains_key(&id));
        assert!(imported.open_research_version(id).is_err());
    }
    verify_baselines(&graph, version_uuid, &imported, projection_uuid);
    verify_historical_self_comparison(&imported, projection_uuid);
    verify_selected_content(&imported, projection_uuid, node_uuid, true);
    verify_selected_content(&imported, proof_uuid, node_uuid, false);
    assert_eq!(
        registry.versions[&proof_uuid].content.source_version,
        Some(mapping.proof_version_uuid)
    );
    let reference = imported
        .research_reference(
            &ResearchReferenceTarget::Version {
                version_uuid: projection_uuid,
            },
            &CancellationToken::new(),
        )
        .unwrap();
    assert_eq!(reference.version.content.source_version, Some(version_uuid));
    assert_eq!(reference.genealogy[0], before.branches[&branch_uuid]);
    verify_reexport(
        &imported,
        projection_uuid,
        &directory.path().join("reexport"),
    );
    assert!(
        imported
            .research_reference(
                &ResearchReferenceTarget::Branch { branch_uuid },
                &CancellationToken::new()
            )
            .is_err()
    );
}

fn verify_selected_content(graph: &GraphForge, version: Uuid, node: Uuid, deferred: bool) {
    let record = graph.research_version(version).unwrap();
    let view = crate::research_versions::materialize_version(graph, &record).unwrap();
    let fields = crate::branches::fields::read(&view, &CancellationToken::new()).unwrap();
    let nodes: std::collections::BTreeSet<_> = fields
        .keys()
        .filter(|k| k.0 == "node")
        .map(|k| k.1)
        .collect();
    assert_eq!(nodes, [node].into());
    for field in ["property:other", "property:private_note", "property:secret"] {
        assert!(!fields.contains_key(&("node".into(), node, field.into())));
    }
    assert_eq!(
        fields.contains_key(&("node".into(), node, "property:deferred".into())),
        deferred
    );
    let result = view.execute("MATCH(n:Character) RETURN n.score").unwrap();
    assert_eq!(
        result.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        1
    );
}

fn fixture(root: &std::path::Path) -> (GraphForge, SubmitResearchProposalRequest) {
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    graph.execute("CREATE (:Character {score:0, other:0, deferred:0, private_note:'private-selected-note'})").unwrap();
    let private_nodes = (0..32)
        .map(|i| format!("(:Private {{secret:'ancestor-private-{i}'}})"))
        .collect::<Vec<_>>()
        .join(",");
    graph.execute(&format!("CREATE {private_nodes}")).unwrap();
    let node_uuid = node(&mut graph);
    let branch_uuid = branch(&mut graph);
    let version_uuid = edit(
        &mut graph,
        branch_uuid,
        "MATCH(n:Character) SET n.score=1, n.other=3, n.deferred=2",
    );
    let proposal = submit(
        &mut graph,
        branch_uuid,
        version_uuid,
        node_uuid,
        &["property:score", "property:other", "property:deferred"],
    );
    let review = decision(&graph, proposal.proposal_uuid, |field| {
        if field == "property:deferred" {
            Defer
        } else {
            Accept
        }
    });
    graph
        .review_research_proposal(&review, &CancellationToken::new())
        .unwrap();
    (graph, proposal)
}

fn export_selected(
    graph: &GraphForge,
    proposal: &SubmitResearchProposalRequest,
    node_uuid: Uuid,
    projection_uuid: Uuid,
    package: &std::path::Path,
) {
    graph
        .export_research(
            &ExportResearchRequest {
                version_uuid: proposal.source_version_uuid,
                output: package.to_path_buf(),
                bundled: false,
                projection: Some(ResearchExportProjection {
                    version_uuid: projection_uuid,
                    frozen_ipc: proposal.frozen_ipc.clone(),
                    fields: ["$object", "$labels", "property:score", "property:deferred"]
                        .into_iter()
                        .map(|field| ResearchFieldIdentity {
                            object_kind: "node".into(),
                            object_uuid: node_uuid,
                            field: field.into(),
                        })
                        .collect(),
                    created_at: 9,
                }),
            },
            &CancellationToken::new(),
        )
        .unwrap();
}

fn verify_baselines(source: &GraphForge, original: Uuid, imported: &GraphForge, selected: Uuid) {
    let source_view = crate::research_versions::materialize_version(
        source,
        &source.research_version(original).unwrap(),
    )
    .unwrap();
    let imported_view = crate::research_versions::materialize_version(
        imported,
        &imported.research_version(selected).unwrap(),
    )
    .unwrap();
    let original = crate::branches::baseline::read(&source_view).unwrap();
    let selected = crate::branches::baseline::read(&imported_view).unwrap();
    assert_eq!(selected.len(), 4);
    for (key, actual) in selected {
        let expected = &original[&key];
        assert_eq!(
            (
                actual.origin,
                actual.incorporated,
                actual.contribution,
                &actual.role
            ),
            (
                expected.origin,
                expected.incorporated,
                expected.contribution,
                &expected.role
            )
        );
        assert_eq!(
            (&actual.original, &actual.baseline, &actual.current),
            (&expected.original, &expected.baseline, &expected.current)
        );
    }
}

fn assert_no_private_parquet_values(root: &std::path::Path) {
    let mut pending = vec![root.to_path_buf()];
    let mut parquet_files = 0;
    while let Some(path) = pending.pop() {
        if path.is_dir() {
            pending.extend(
                std::fs::read_dir(&path)
                    .unwrap()
                    .map(|entry| entry.unwrap().path()),
            );
            continue;
        }
        let bytes = std::fs::read(&path).unwrap();
        if !bytes.starts_with(b"PAR1") {
            continue;
        }
        parquet_files += 1;
        let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            std::fs::File::open(&path).unwrap(),
        )
        .unwrap();
        assert_private_absent(&format!("{:?}", builder.metadata()), &path);
        assert_private_absent(&format!("{:?}", builder.schema()), &path);
        for batch in builder.build().unwrap() {
            let batch = batch.unwrap();
            for column in batch.columns() {
                for row in 0..column.len() {
                    let value =
                        arrow::util::display::array_value_to_string(column.as_ref(), row).unwrap();
                    assert_private_absent(&value, &path);
                }
            }
        }
    }
    assert!(
        parquet_files > 0,
        "fixture must inspect actual exported Parquet bytes"
    );
}

fn assert_private_absent(value: &str, path: &std::path::Path) {
    for secret in ["private-selected-note", "ancestor-private-"] {
        assert!(
            !value.contains(secret),
            "exported physical Parquet retains private value {secret}: {}",
            path.display()
        );
    }
}

fn verify_reexport(imported: &GraphForge, selected: Uuid, root: &std::path::Path) {
    std::fs::create_dir(root).unwrap();
    let package = root.join("package");
    imported
        .export_research(
            &ExportResearchRequest {
                version_uuid: selected,
                output: package.clone(),
                bundled: false,
                projection: None,
            },
            &CancellationToken::new(),
        )
        .unwrap();
    assert_no_private_parquet_values(&package);
    crate::verify_portable_v2(
        &PortableVerifyRequest {
            input: package.clone(),
            mode: graphforge_core::portable::PortableV2Mode::Full,
            limits: Default::default(),
        },
        None,
    )
    .unwrap();
    let target = root.join("imported");
    GraphForge::import_portable_v2(
        &target,
        &PortableV2ImportRequest {
            input: package,
            operation_id: OperationId(Uuid::now_v7()),
            limits: Default::default(),
        },
        None,
    )
    .unwrap();
    let again = GraphForge::new(target.to_str()).unwrap();
    let original = imported.research_version_retention().unwrap();
    let actual = again.research_version_retention().unwrap();
    assert_eq!(
        actual.interchange[&selected].accepted,
        original.interchange[&selected].accepted
    );
    assert_eq!(
        actual.interchange[&selected].proof_exports,
        original.interchange[&selected].proof_exports
    );
    for id in original.interchange[&selected].proof_exports.values() {
        assert_eq!(actual.versions[id], original.versions[id]);
    }
    assert!(actual.proposals.accepted.is_empty());
}

fn verify_historical_self_comparison(graph: &GraphForge, version: Uuid) {
    let endpoint = crate::ResearchComparisonEndpoint::Version {
        version_uuid: version,
    };
    let result = graph
        .compare_research(
            &crate::ResearchComparisonRequest {
                left: endpoint.clone(),
                right: endpoint,
                left_authority: None,
                right_authority: None,
                detail: crate::ResearchComparisonDetail::Changes,
                accepted: vec![],
                max_fields: 40_000,
                max_bytes: 64 * 1024 * 1024,
                page_size: 1000,
                after: None,
            },
            &CancellationToken::new(),
        )
        .unwrap();
    // Imported genealogy and preserved incorporated rows do not need live Branch
    // heads or ancestor payloads to compare an immutable selected Version.
    let changes = result.batches[0]
        .column_by_name("change")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert!(changes.iter().flatten().all(|value| value == "equivalent"));
}
