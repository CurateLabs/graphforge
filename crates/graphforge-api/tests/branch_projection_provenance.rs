//! Disjoint selections may share immutable provenance without sharing payloads.
use arrow::array::FixedSizeBinaryArray;
use graphforge_api::*;
use graphforge_provenance::ProvenanceLedger;
use std::{collections::BTreeSet, path::Path};
use uuid::Uuid;

fn current(graph: &GraphForge) -> Uuid {
    graph
        .research_project_summary()
        .unwrap()
        .identity
        .generation_uuid
}

fn node_ids(graph: &GraphForge) -> BTreeSet<Uuid> {
    graph
        .execute("MATCH (n) RETURN n.node_uuid AS id")
        .unwrap()
        .batches
        .iter()
        .flat_map(|batch| {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            (0..batch.num_rows())
                .map(|row| Uuid::from_slice(ids.value(row)).unwrap())
                .collect::<Vec<_>>()
        })
        .collect()
}

fn frozen(graph: &GraphForge, version_uuid: Uuid, name: &str) -> Vec<u8> {
    let result = graph
        .freeze_slice(
            &SliceRequest {
                request_uuid: Uuid::now_v7(),
                source: SliceSource::Version { version_uuid },
                selector: SliceSelector::Filter {
                    label: "Story".into(),
                    property: "name".into(),
                    equals: serde_json::json!(name),
                },
                include: Default::default(),
                exclude: Default::default(),
                limits: Default::default(),
            },
            &CancellationToken::new(),
        )
        .unwrap();
    let mut bytes = Vec::new();
    let mut writer =
        arrow::ipc::writer::StreamWriter::try_new(&mut bytes, result.schema.as_ref()).unwrap();
    for batch in &result.batches {
        writer.write(batch).unwrap();
    }
    writer.finish().unwrap();
    bytes
}

fn provenance(root: &Path, graph: &GraphForge, version: Uuid) -> ProvenanceLedger {
    let version = graph.research_version(version).unwrap();
    let snapshots =
        graphforge_storage::research_versions::inspect_research_version(root, &version).unwrap();
    let read = |family: &str| {
        let snapshot = snapshots
            .iter()
            .find(|p| p.capability_id == "provenance" && p.record_family_id == family)
            .unwrap();
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(
            snapshot.bytes.clone(),
        ))
        .unwrap()
        .build()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
    };
    ProvenanceLedger::from_batches(&read("events"), &read("lineage")).unwrap()
}

#[test]
fn bring_unions_disjoint_selected_lineage_from_one_operation_without_unselected_subjects() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("project");
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    graph
        .enable_capability(EnableCapabilityRequest {
            context: WriteContext {
                operation_uuid: OperationId(Uuid::now_v7()),
                actor_uuid: None,
            },
            capability_id: CapabilityId::Provenance,
            capability_version: 1,
        })
        .unwrap();
    graph
        .execute("CREATE (:Story {name:'A'}), (:Story {name:'B'}), (:Story {name:'Unselected'})")
        .unwrap();
    let all_ids = node_ids(&graph);
    assert_eq!(all_ids.len(), 3);
    let source = Uuid::now_v7();
    let operation = graph
        .prepare_research_version(PrepareResearchVersionRequest {
            operation_uuid: Uuid::now_v7(),
            version_uuid: source,
            context_uuid: Uuid::now_v7(),
            label: None,
            description: None,
            created_at: 1,
            required_versions: Default::default(),
        })
        .unwrap();
    graph
        .commit_research_version_operation(operation, &CancellationToken::new())
        .unwrap();
    let original = provenance(&root, &graph, source);
    let subject_rows: Vec<_> = original
        .lineage
        .iter()
        .filter(|row| all_ids.contains(&row.subject_uuid))
        .collect();
    assert_eq!(
        subject_rows
            .iter()
            .map(|row| row.subject_uuid)
            .collect::<BTreeSet<_>>(),
        all_ids
    );
    let event_ids: BTreeSet<_> = subject_rows.iter().map(|row| row.provenance_uuid).collect();
    assert_eq!(
        event_ids.len(),
        1,
        "the regression must share one provenance event"
    );
    let event = original
        .events
        .iter()
        .find(|row| event_ids.contains(&row.provenance_uuid))
        .unwrap();

    let a = frozen(&graph, source, "A");
    let b = frozen(&graph, source, "B");
    let create = CreateResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&graph),
        branch_uuid: Uuid::now_v7(),
        version_uuid: Uuid::now_v7(),
        source: BranchSource::Slice { frozen_ipc: a },
        creator_uuid: Uuid::now_v7(),
        created_at: 2,
        label: "Selected A then B".into(),
    };
    graph
        .create_research_branch(&create, &CancellationToken::new())
        .unwrap();
    let initial = provenance(&root, &graph, create.version_uuid);
    let initial_ids = node_ids(
        graph
            .open_research_branch(create.branch_uuid)
            .unwrap()
            .graph(),
    );
    assert_eq!(initial_ids.len(), 1);
    assert!(
        initial
            .lineage
            .iter()
            .all(|row| initial_ids.contains(&row.subject_uuid))
    );

    let bring = BringResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&graph),
        branch_uuid: create.branch_uuid,
        version_uuid: Uuid::now_v7(),
        frozen_ipc: b,
        created_at: 3,
    };
    let receipt = graph
        .bring_research_branch(&bring, &CancellationToken::new())
        .unwrap();
    assert_eq!(
        graph
            .bring_research_branch(&bring, &CancellationToken::new())
            .unwrap(),
        receipt
    );
    drop(graph);
    let graph = GraphForge::new(root.to_str()).unwrap();
    let view = graph.open_research_branch(create.branch_uuid).unwrap();
    let selected_ids = node_ids(view.graph());
    assert_eq!(selected_ids.len(), 2);
    assert!(initial_ids.is_subset(&selected_ids));
    assert!(selected_ids.is_subset(&all_ids));
    assert_eq!(
        node_ids(&graph),
        all_ids,
        "parent membership must remain unchanged"
    );
    let combined = provenance(&root, &graph, bring.version_uuid);
    let expected: Vec<_> = original
        .lineage
        .iter()
        .filter(|row| selected_ids.contains(&row.subject_uuid))
        .cloned()
        .collect();
    assert_eq!(
        combined.lineage, expected,
        "retain exact original rows, not unrelated operation subjects"
    );
    assert_eq!(combined.events, vec![event.clone()]);
    assert!(
        initial
            .lineage
            .iter()
            .all(|row| combined.lineage.contains(row))
    );
}
