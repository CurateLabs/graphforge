//! Real CLI Arrow preview/freeze/revision over a durable Project.
use graphforge_api::*;
use std::{io::Cursor, path::Path, process::Command};
use uuid::Uuid;
fn run(root: &Path, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_gf"))
        .arg("--project")
        .arg(root)
        .args(["research", "slice"])
        .args(args)
        .output()
        .unwrap()
}
fn count(output: std::process::Output) -> usize {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    arrow::ipc::reader::StreamReader::try_new(Cursor::new(output.stdout), None)
        .unwrap()
        .map(|b| b.unwrap().num_rows())
        .sum()
}
#[test]
fn cli_frozen_membership_survives_current_edits_and_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("project");
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    graph
        .execute("CREATE (:Character {name:'Shared'})")
        .unwrap();
    let version = Uuid::now_v7();
    let operation = graph
        .prepare_research_version(PrepareResearchVersionRequest {
            operation_uuid: Uuid::now_v7(),
            version_uuid: version,
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
    drop(graph);
    let request = directory.path().join("request.json");
    let capsule = directory.path().join("slice.arrow");
    std::fs::write(&request, serde_json::to_vec(&serde_json::json!({"request_uuid":Uuid::now_v7(),"source":{"kind":"version","version_uuid":version},"selector":{"kind":"query","query":"MATCH (n:Character) RETURN n.node_uuid AS node_uuid"}})).unwrap()).unwrap();
    let file = request.to_str().unwrap();
    assert_eq!(count(run(&root, &["preview", "--file", file])), 1);
    let frozen = run(&root, &["freeze", "--file", file]);
    assert!(
        frozen.status.success(),
        "{}",
        String::from_utf8_lossy(&frozen.stderr)
    );
    std::fs::write(&capsule, frozen.stdout).unwrap();
    let graph = GraphForge::new(root.to_str()).unwrap();
    graph.execute("CREATE (:Character {name:'Later'})").unwrap();
    drop(graph);
    assert_eq!(
        count(run(
            &root,
            &["inspect", "--capsule", capsule.to_str().unwrap()]
        )),
        1
    );
    std::fs::write(
        &request,
        serde_json::to_vec(
            &serde_json::json!({"request_uuid":Uuid::now_v7(),"source_version":null}),
        )
        .unwrap(),
    )
    .unwrap();
    let revised = run(
        &root,
        &[
            "revise",
            "--capsule",
            capsule.to_str().unwrap(),
            "--file",
            file,
        ],
    );
    assert!(
        revised.status.success(),
        "{}",
        String::from_utf8_lossy(&revised.stderr)
    );
    std::fs::write(&capsule, revised.stdout).unwrap();
    assert_eq!(
        count(run(
            &root,
            &["inspect", "--capsule", capsule.to_str().unwrap()]
        )),
        1
    );
    std::fs::write(&request, br#"{"private_sentinel":"private-secret-value"}"#).unwrap();
    let invalid = run(&root, &["preview", "--file", file]);
    assert!(!invalid.status.success());
    let diagnostic = String::from_utf8_lossy(&invalid.stderr);
    assert!(!diagnostic.contains("private_sentinel"));
    assert!(!diagnostic.contains("private-secret-value"));
}
