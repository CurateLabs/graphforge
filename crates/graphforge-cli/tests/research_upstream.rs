//! CLI preview, publication and immutable review history use the real native facade.
use graphforge_api::*;
use std::{io::Cursor, path::Path, process::Command};
use uuid::Uuid;
fn run(root: &Path, file: &Path, verb: &str, request: &impl serde::Serialize) -> Vec<u8> {
    std::fs::write(file, serde_json::to_vec(request).unwrap()).unwrap();
    let result = Command::new(env!("CARGO_BIN_EXE_gf"))
        .arg("--project")
        .arg(root)
        .args(["research", "upstream", verb, "--file"])
        .arg(file)
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    result.stdout
}
#[test]
fn cli_upstream_review_reopens_and_replays_native_publication() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("project");
    let file = directory.path().join("request.json");
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    graph.execute("CREATE (:Item {x:0,y:0})").unwrap();
    let current = |graph: &GraphForge| {
        graph
            .research_project_summary()
            .unwrap()
            .identity
            .generation_uuid
    };
    let branch = Uuid::now_v7();
    graph
        .create_research_branch(
            &CreateResearchBranchRequest {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: current(&graph),
                branch_uuid: branch,
                version_uuid: Uuid::now_v7(),
                source: BranchSource::Current {
                    origin_version_uuid: Uuid::now_v7(),
                    context_uuid: Uuid::now_v7(),
                },
                creator_uuid: Uuid::now_v7(),
                created_at: 1,
                label: "CLI upstream".into(),
            },
            &CancellationToken::new(),
        )
        .unwrap();
    graph.execute("MATCH (n:Item) SET n.x=1,n.y=2").unwrap();
    let generation = current(&graph);
    drop(graph);
    let preview = PreviewResearchUpstreamRequest {
        branch_uuid: branch,
        scope: ResearchUpstreamScope::Branch,
    };
    let bytes = run(&root, &file, "preview", &preview);
    let mut reader = arrow::ipc::reader::StreamReader::try_new(Cursor::new(bytes), None).unwrap();
    let text = reader.schema().metadata()["graphforge.upstream.preview_sha256"].clone();
    let mut digest = [0; 32];
    for (i, byte) in digest.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[i * 2..i * 2 + 2], 16).unwrap();
    }
    let batch = reader.next().unwrap().unwrap();
    let names = batch
        .column_by_name("field")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    let row = (0..batch.num_rows())
        .find(|row| names.value(*row) == "property:x")
        .unwrap();
    let ids = batch
        .column_by_name("object_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
        .unwrap();
    let request = UpdateResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: generation,
        version_uuid: Uuid::now_v7(),
        preview,
        preview_sha256: digest,
        selection: ResearchUpstreamSelection::Selected {
            decisions: vec![ResearchUpstreamDecision {
                unit: ResearchFieldIdentity {
                    object_kind: "node".into(),
                    object_uuid: Uuid::from_slice(ids.value(row)).unwrap(),
                    field: "property:x".into(),
                },
                resolution: ResearchUpstreamResolution::AdoptUpstream,
            }],
        },
        acknowledge_evidence: Default::default(),
        actor_uuid: Uuid::now_v7(),
        created_at: 2,
        explanation: "Adopt reviewed x only".into(),
    };
    let receipt: serde_json::Value =
        serde_json::from_slice(&run(&root, &file, "update", &request)).unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&run(&root, &file, "update", &request))
            .unwrap(),
        receipt
    );
    let history = run(
        &root,
        &file,
        "history",
        &ResearchUpstreamHistoryRequest {
            branch_uuid: branch,
            page_size: 10,
            after: None,
        },
    );
    let batches = arrow::ipc::reader::StreamReader::try_new(Cursor::new(history), None)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        1
    );
    let reopened = GraphForge::new(root.to_str()).unwrap();
    let view = reopened.open_research_branch(branch).unwrap();
    let result = view
        .graph()
        .execute("MATCH (n:Item) RETURN n.x AS x,n.y AS y")
        .unwrap();
    for (name, expected) in [("x", 1), ("y", 0)] {
        assert_eq!(
            result.batches[0]
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .value(0),
            expected
        );
    }
    assert_eq!(
        reopened
            .research_version_retention()
            .unwrap()
            .upstream
            .reviews[&request.operation_uuid]
            .version_uuid,
        request.version_uuid
    );
}
