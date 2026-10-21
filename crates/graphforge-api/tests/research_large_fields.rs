//! Issue #1770: a research Version whose typed fields exceed the 64 MiB
//! whole-map budget verifies, imports and reopens through the streaming
//! comparison, while Branch write paths keep their resource bound.
use graphforge_api::{
    BranchSource, CancellationToken, CreateResearchBranchRequest, ExportResearchRequest,
    GraphForge, OperationId, PortableImportRequest, PortableVerifyRequest,
    PrepareResearchVersionRequest,
};
use uuid::Uuid;

const NODES: usize = 24_000;
const CHUNK: usize = 1_000;
const PADDING: usize = 3 * 1024;

fn ingest_large_workspace(graph: &mut GraphForge) {
    let padding = "x".repeat(PADDING);
    for start in (1..=NODES).step_by(CHUNK) {
        graph
            .execute(&format!(
                "UNWIND range({start}, {}) AS i CREATE (:Row {{a: i, b: '{padding}'}})",
                (start + CHUNK - 1).min(NODES)
            ))
            .unwrap();
    }
}

fn current_generation(graph: &GraphForge) -> Uuid {
    graph
        .research_project_summary()
        .unwrap()
        .identity
        .generation_uuid
}

#[test]
fn research_version_fields_over_the_whole_map_budget_verify_import_and_reopen() {
    let cancellation = CancellationToken::new();
    let directory = tempfile::tempdir().unwrap();
    let mut source = GraphForge::new(None).unwrap();
    ingest_large_workspace(&mut source);

    // 4 field keys per node put the whole-graph map past its 64 MiB reserve
    // (96,000 keys) and the 3 KiB strings put the scanned Arrow bytes past
    // 64 MiB. The 1,000,000-row slice cap and the 1,000,000 knowledge-domain
    // key ceiling are not reached here; the streaming path no longer applies
    // them (unit tests in slices/graph.rs and branches/fields.rs).
    let version_uuid = Uuid::now_v7();
    let prepared = source
        .prepare_research_version(PrepareResearchVersionRequest {
            operation_uuid: Uuid::now_v7(),
            version_uuid,
            context_uuid: Uuid::now_v7(),
            label: Some("large fields".into()),
            description: None,
            created_at: 1,
            required_versions: Default::default(),
            author: None,
            committer: None,
        })
        .unwrap();
    source
        .commit_research_version_operation(prepared, &cancellation)
        .unwrap();

    let bundle = directory.path().join("large.gfbundle");
    source
        .export_research(
            &ExportResearchRequest {
                version_uuid,
                output: bundle.clone(),
                bundled: true,
                projection: None,
            },
            &cancellation,
        )
        .unwrap();

    let report = graphforge_api::verify_portable(
        &PortableVerifyRequest {
            input: bundle.clone(),
            mode: graphforge_core::portable::PortableMode::Full,
            limits: Default::default(),
        },
        Some(cancellation.flag()),
    )
    .unwrap();
    assert!(report.research_interchange);

    let target = directory.path().join("destination");
    GraphForge::import_portable(
        &target,
        &PortableImportRequest {
            input: bundle,
            operation_id: OperationId(Uuid::now_v7()),
            limits: Default::default(),
        },
        None,
    )
    .unwrap();

    let imported = GraphForge::new(Some(target.to_str().unwrap())).unwrap();
    let view = imported.open_research_version(version_uuid).unwrap();
    let listed = view.execute("MATCH (n) RETURN n.node_uuid").unwrap();
    let total: usize = listed.batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(total, NODES);
}

#[test]
fn branch_write_paths_keep_the_whole_map_field_bound() {
    let cancellation = CancellationToken::new();
    let mut source = GraphForge::new(None).unwrap();
    ingest_large_workspace(&mut source);
    let error = source
        .create_research_branch(&large_branch_request(&source), &cancellation)
        .unwrap_err();
    assert_eq!(error.code(), "GF_RESOURCE_LIMIT");

    // With narrow rows the whole-graph map reserve is the operative bound:
    // 15,000 nodes with one property are 45,000 keys at 1,536 B each while
    // their scanned Arrow bytes stay far below 64 MiB.
    let mut narrow = GraphForge::new(None).unwrap();
    narrow
        .execute("UNWIND range(1, 15000) AS i CREATE (:Row {a: i})")
        .unwrap();
    let error = narrow
        .create_research_branch(&large_branch_request(&narrow), &cancellation)
        .unwrap_err();
    assert_eq!(error.code(), "GF_RESOURCE_LIMIT");
    assert!(
        error
            .to_string()
            .contains("semantic fields exceed creation"),
        "the field map reserve, not another bound, must refuse: {error}"
    );
}

fn large_branch_request(graph: &GraphForge) -> CreateResearchBranchRequest {
    CreateResearchBranchRequest {
        author: None,
        committer: None,
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current_generation(graph),
        branch_uuid: Uuid::now_v7(),
        version_uuid: Uuid::now_v7(),
        source: BranchSource::Current {
            origin_version_uuid: Uuid::now_v7(),
            context_uuid: Uuid::now_v7(),
        },
        creator_uuid: Uuid::now_v7(),
        created_at: 1,
        label: "refused".into(),
    }
}
