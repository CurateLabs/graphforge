//! CLI contextual authority round-trips native Arrow across process reopen.
use graphforge_api::*;
use std::{io::Cursor, path::Path, process::Command};
use uuid::Uuid;
fn run(root: &Path, file: &Path, command: &str) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_gf"))
        .arg("--project")
        .arg(root)
        .args(["research", "claim", command, "--file"])
        .arg(file)
        .output()
        .unwrap()
}
fn rows(output: std::process::Output) -> usize {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    arrow::ipc::reader::StreamReader::try_new(Cursor::new(output.stdout), None)
        .unwrap()
        .map(|batch| batch.unwrap().num_rows())
        .sum()
}
#[test]
fn cli_claim_creation_promotion_and_history_are_native_and_durable() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("project");
    let file = directory.path().join("request.json");
    let graph = GraphForge::new(root.to_str()).unwrap();
    for capability_id in [
        CapabilityId::Provenance,
        CapabilityId::Knowledge,
        CapabilityId::Epistemic,
    ] {
        graph
            .enable_capability(EnableCapabilityRequest {
                context: WriteContext {
                    operation_uuid: OperationId(Uuid::now_v7()),
                    actor_uuid: None,
                },
                capability_id,
                capability_version: 1,
            })
            .unwrap();
    }
    graph.execute("CREATE (:Subject)").unwrap();
    let nodes = graph.execute("MATCH (n) RETURN n.node_uuid").unwrap();
    let node = Uuid::from_slice(
        nodes.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
            .unwrap()
            .value(0),
    )
    .unwrap();
    let assertion = Uuid::now_v7();
    let generation = graph
        .research_project_summary()
        .unwrap()
        .identity
        .generation_uuid;
    drop(graph);
    let create = serde_json::json!({"operation_uuid":Uuid::now_v7(),"expected_generation_uuid":generation,"assertion_uuid":assertion,"claim":"A contextual interpretation","graph_refs":[{"graph_uuid":node,"graph_kind":"node","role":"subject","ordinal":0}],"category":"interpretation","creator_uuid":Uuid::now_v7(),"run_uuid":null,"created_at":1});
    std::fs::write(&file, serde_json::to_vec(&create).unwrap()).unwrap();
    assert_eq!(rows(run(&root, &file, "create")), 1);
    assert_eq!(rows(run(&root, &file, "create")), 1);
    let graph = GraphForge::new(root.to_str()).unwrap();
    let generation = graph
        .research_project_summary()
        .unwrap()
        .identity
        .generation_uuid;
    drop(graph);
    let decision = serde_json::json!({"operation_uuid":Uuid::now_v7(),"expected_generation_uuid":generation,"context":{"kind":"project"},"community_uuid":null,"creator_uuid":Uuid::now_v7(),"recorded_at":2,"decisions":[{"decision_uuid":Uuid::now_v7(),"subject_kind":"assertion","subject_uuid":assertion,"kind":"promote","source_version_uuid":null}]});
    std::fs::write(&file, serde_json::to_vec(&decision).unwrap()).unwrap();
    assert_eq!(rows(run(&root, &file, "decide")), 1);
    std::fs::write(
        &file,
        br#"{"context":{"kind":"project"},"community_uuid":null}"#,
    )
    .unwrap();
    assert_eq!(rows(run(&root, &file, "canonical")), 1);
    assert_eq!(rows(run(&root, &file, "decisions")), 1);
    std::fs::write(
        &file,
        br#"{"context":{"kind":"project"},"community_uuid":null,"include_suppressed":false}"#,
    )
    .unwrap();
    assert_eq!(rows(run(&root, &file, "inspect")), 1);
    std::fs::write(
        &file,
        br#"{"context":{"kind":"project"},"family":"classification","assertion_uuid":null}"#,
    )
    .unwrap();
    assert_eq!(rows(run(&root, &file, "history")), 1);
    std::fs::write(
        &file,
        br#"{"context":{"kind":"project"},"unexpected":true}"#,
    )
    .unwrap();
    assert!(!run(&root, &file, "canonical").status.success());
}
