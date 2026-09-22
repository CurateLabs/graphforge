//! Native legacy fixture: remove only fields absent from the pre-comparison contract.
use crate::branches::{baseline, edit, publication};
use crate::*;
use graphforge_storage::research_versions::{ResearchMutation, prepare_branch_content};
use uuid::Uuid;
fn current(g: &GraphForge) -> Uuid {
    g.generation_for_read().unwrap().generation_uuid()
}
#[test]
fn legacy_local_reference_remains_local_after_bring() {
    let mut g = GraphForge::new(None).unwrap();
    g.execute("CREATE (:Item)").unwrap();
    let token = CancellationToken::new();
    let b = CreateResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&g),
        branch_uuid: Uuid::now_v7(),
        version_uuid: Uuid::now_v7(),
        source: BranchSource::Current {
            origin_version_uuid: Uuid::now_v7(),
            context_uuid: Uuid::now_v7(),
        },
        creator_uuid: Uuid::now_v7(),
        created_at: 1,
        label: "Legacy".into(),
    };
    g.create_research_branch(&b, &token).unwrap();
    g.execute("CREATE (:NewItem)").unwrap();
    let op = g
        .prepare_research_version(PrepareResearchVersionRequest {
            operation_uuid: Uuid::now_v7(),
            version_uuid: Uuid::now_v7(),
            context_uuid: Uuid::now_v7(),
            label: None,
            description: None,
            created_at: 2,
            required_versions: Default::default(),
        })
        .unwrap();
    let source = g
        .commit_research_version_operation(op, &token)
        .unwrap()
        .version_uuid
        .unwrap();
    let reference = ReferenceResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&g),
        branch_uuid: b.branch_uuid,
        version_uuid: Uuid::now_v7(),
        reference_uuid: Uuid::now_v7(),
        source_version_uuid: source,
        label: "Local note".into(),
        created_at: 3,
    };
    g.reference_research_branch(&reference, &token).unwrap();
    let command =
        publication::begin(&g, Uuid::now_v7(), current(&g), &"legacy-fixture", &token).unwrap();
    let (graph, mut version) = edit::prepare(&g, &command, b.branch_uuid).unwrap();
    version.version_uuid = Uuid::now_v7();
    let mut prepared = prepare_branch_content(
        &command.root,
        &graph.generation_for_read().unwrap(),
        version,
        token.flag(),
    )
    .unwrap();
    let mut rows = baseline::read(&graph).unwrap();
    rows.retain(|key, _| !key.0.starts_with("ontology") && key.0 != "reference");
    baseline::install(&command.root, &mut prepared, &rows, &token).unwrap();
    let mutation = ResearchMutation::PublishBranch {
        intent_sha256: command.intent,
        origin_capture: None,
        creation: None,
        version: Box::new(prepared.version.clone()),
    };
    command.publish(&mut g, mutation, &token).unwrap();
    drop(prepared);
    drop(graph);
    let frozen = g
        .freeze_slice(
            &SliceRequest {
                request_uuid: Uuid::now_v7(),
                source: SliceSource::Version {
                    version_uuid: source,
                },
                selector: SliceSelector::Query {
                    query: "MATCH (n:NewItem) RETURN n.node_uuid AS node_uuid".into(),
                },
                include: Default::default(),
                exclude: Default::default(),
                limits: Default::default(),
            },
            &token,
        )
        .unwrap();
    let mut bytes = Vec::new();
    let mut writer =
        arrow::ipc::writer::StreamWriter::try_new(&mut bytes, frozen.schema.as_ref()).unwrap();
    for batch in &frozen.batches {
        writer.write(batch).unwrap();
    }
    writer.finish().unwrap();
    drop(writer);
    g.bring_research_branch(
        &BringResearchBranchRequest {
            operation_uuid: Uuid::now_v7(),
            expected_generation_uuid: current(&g),
            branch_uuid: b.branch_uuid,
            version_uuid: Uuid::now_v7(),
            frozen_ipc: bytes,
            created_at: 4,
        },
        &token,
    )
    .unwrap();
    let view = g.open_research_branch(b.branch_uuid).unwrap();
    let rows = baseline::read(view.graph()).unwrap();
    let references: Vec<_> = rows.values().filter(|r| r.key.0 == "reference").collect();
    assert_eq!(references.len(), 2);
    for row in references {
        assert!(row.baseline.is_empty());
        assert!(row.incorporated.is_none());
        assert!(!row.current.is_empty());
    }
    assert_eq!(
        view.graph()
            .execute("MATCH (n:NewItem) RETURN n")
            .unwrap()
            .batches[0]
            .num_rows(),
        1
    );
}
