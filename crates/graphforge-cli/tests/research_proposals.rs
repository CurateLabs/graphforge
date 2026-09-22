//! Actual CLI proposal publication and native Arrow review over durable research.
use graphforge_api::*;
use std::{io::Cursor, process::Command};
use uuid::Uuid;
fn current(g: &GraphForge) -> Uuid {
    g.research_project_summary()
        .unwrap()
        .identity
        .generation_uuid
}
fn cli(
    root: &std::path::Path,
    file: &std::path::Path,
    verb: &str,
    request: serde_json::Value,
) -> Vec<u8> {
    std::fs::write(file, serde_json::to_vec(&request).unwrap()).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_gf"))
        .arg("--project")
        .arg(root)
        .args(["research", "proposal", verb, "--file"])
        .arg(file)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout
}
#[test]
fn cli_proposal_submit_review_history_and_release_use_native_contract() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("project");
    let file = directory.path().join("request.json");
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    graph.execute("CREATE (:Character {score:0})").unwrap();
    let result = graph
        .execute("MATCH (n:Character) RETURN n.node_uuid AS id")
        .unwrap();
    let node = Uuid::from_slice(
        result.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
            .unwrap()
            .value(0),
    )
    .unwrap();
    let branch = Uuid::now_v7();
    let version = Uuid::now_v7();
    let proposal = Uuid::now_v7();
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
                label: "Story".into(),
            },
            &CancellationToken::new(),
        )
        .unwrap();
    graph
        .execute_research_branch(
            &ExecuteResearchBranchRequest {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: current(&graph),
                branch_uuid: branch,
                version_uuid: version,
                created_at: 2,
                query: "MATCH (n:Character) SET n.score=1".into(),
            },
            &CancellationToken::new(),
        )
        .unwrap();
    let frozen = graph
        .freeze_slice(
            &SliceRequest {
                request_uuid: Uuid::now_v7(),
                source: SliceSource::Version {
                    version_uuid: version,
                },
                selector: SliceSelector::Direct {
                    members: SliceMembers {
                        nodes: [node].into(),
                        ..Default::default()
                    },
                },
                include: Default::default(),
                exclude: Default::default(),
                limits: Default::default(),
            },
            &CancellationToken::new(),
        )
        .unwrap();
    let mut ipc = Vec::new();
    let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut ipc, &frozen.schema).unwrap();
    for batch in &frozen.batches {
        writer.write(batch).unwrap();
    }
    writer.finish().unwrap();
    drop(writer);
    let generation = current(&graph);
    drop(graph);
    cli(
        &root,
        &file,
        "submit",
        serde_json::json!({"operation_uuid":Uuid::now_v7(),"expected_generation_uuid":generation,"proposal_uuid":proposal,"source_branch_uuid":branch,"source_version_uuid":version,"frozen_ipc":ipc,"fields":[{"object_kind":"node","object_uuid":node,"field":"property:score"}],"actor_uuid":Uuid::now_v7(),"created_at":3,"motivation":"Selected score","policy":""}),
    );
    let bytes = cli(
        &root,
        &file,
        "preview",
        serde_json::json!({"proposal_uuid":proposal}),
    );
    let batch = arrow::ipc::reader::StreamReader::try_new(Cursor::new(bytes), None)
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    let text = |name| {
        batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap()
            .value(0)
            .to_string()
    };
    let hash = text("preview_sha256");
    let digest: Vec<u8> = (0..32)
        .map(|i| u8::from_str_radix(&hash[i * 2..i * 2 + 2], 16).unwrap())
        .collect();
    let review = serde_json::json!({"operation_uuid":Uuid::now_v7(),"expected_generation_uuid":text("generation_uuid"),"proposal_uuid":proposal,"preview_sha256":digest,"decisions":{text("item_uuid"):"accept"},"resolve_conflicts":[],"acknowledge_evidence":[],"promotions":[],"actor_uuid":Uuid::now_v7(),"created_at":4,"explanation":"Reviewed","policy":""});
    let receipt = cli(&root, &file, "review", review.clone());
    assert_eq!(cli(&root, &file, "review", review), receipt);
    let bytes = cli(
        &root,
        &file,
        "history",
        serde_json::json!({"proposal_uuid":proposal,"detail":"accepted","page_size":100}),
    );
    assert_eq!(
        arrow::ipc::reader::StreamReader::try_new(Cursor::new(bytes), None)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .num_rows(),
        1
    );
    let graph = GraphForge::new(root.to_str()).unwrap();
    let generation = current(&graph);
    let result = graph
        .execute("MATCH (n:Character) RETURN n.score AS score")
        .unwrap();
    assert_eq!(
        result.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0),
        1
    );
    drop(graph);
    cli(
        &root,
        &file,
        "release",
        serde_json::json!({"operation_uuid":Uuid::now_v7(),"expected_generation_uuid":generation,"proposal_uuid":proposal}),
    );
}
