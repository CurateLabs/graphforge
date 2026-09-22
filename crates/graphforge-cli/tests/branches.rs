//! Real CLI Branch publication and immutable Arrow reads across process reopen.
use graphforge_api::*;
use std::{io::Cursor, path::Path, process::Command};
use uuid::Uuid;
fn run(root: &Path, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_gf"))
        .arg("--project")
        .arg(root)
        .args(["research", "branch"])
        .args(args)
        .output()
        .unwrap()
}
fn success(output: std::process::Output) -> Vec<u8> {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout
}
#[test]
fn cli_branch_edits_restore_without_changing_parent() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("project");
    let graph = GraphForge::new(root.to_str()).unwrap();
    graph
        .execute("CREATE (:Character {name:'Original'})")
        .unwrap();
    let branch = Uuid::now_v7();
    let base = Uuid::now_v7();
    let generation = graph
        .research_project_summary()
        .unwrap()
        .identity
        .generation_uuid;
    drop(graph);
    let path = directory.path().join("request.json");
    let file = path.to_str().unwrap();
    let create = serde_json::json!({"operation_uuid":Uuid::now_v7(), "expected_generation_uuid":generation, "branch_uuid":branch, "version_uuid":base, "source":{"kind":"current","origin_version_uuid":Uuid::now_v7(),"context_uuid":Uuid::now_v7()},"creator_uuid":Uuid::now_v7(),"created_at":1,"label":"Story"});
    std::fs::write(&path, serde_json::to_vec(&create).unwrap()).unwrap();
    let receipt = success(run(&root, &["create", "--file", file]));
    assert_eq!(success(run(&root, &["create", "--file", file])), receipt);
    let id = branch.to_string();
    let selected = success(run(&root, &["selection", "--branch-uuid", &id]));
    let count: usize = arrow::ipc::reader::StreamReader::try_new(Cursor::new(&selected), None)
        .unwrap()
        .map(|b| b.unwrap().num_rows())
        .sum();
    assert_eq!(count, 1);
    let receipt: serde_json::Value = serde_json::from_slice(&receipt).unwrap();
    let edit = serde_json::json!({"operation_uuid":Uuid::now_v7(),"expected_generation_uuid":receipt["generation_uuid"],"branch_uuid":branch,"version_uuid":Uuid::now_v7(),"created_at":2,"query":"CREATE (:Character {name:'Local'})"});
    std::fs::write(&path, serde_json::to_vec(&edit).unwrap()).unwrap();
    let edited: serde_json::Value =
        serde_json::from_slice(&success(run(&root, &["execute", "--file", file]))).unwrap();
    let result = success(run(
        &root,
        &[
            "query",
            "--branch-uuid",
            &id,
            "--query",
            "MATCH (n) RETURN n",
        ],
    ));
    assert_eq!(
        arrow::ipc::reader::StreamReader::try_new(Cursor::new(result), None)
            .unwrap()
            .map(|b| b.unwrap().num_rows())
            .sum::<usize>(),
        2
    );
    assert_eq!(
        success(run(&root, &["selection", "--branch-uuid", &id])),
        selected
    );
    let restore = serde_json::json!({"operation_uuid":Uuid::now_v7(),"expected_generation_uuid":edited["generation_uuid"],"branch_uuid":branch,"source_version_uuid":base,"version_uuid":Uuid::now_v7(),"created_at":3});
    std::fs::write(&path, serde_json::to_vec(&restore).unwrap()).unwrap();
    success(run(&root, &["restore", "--file", file]));
    let graph = GraphForge::new(root.to_str()).unwrap();
    assert_eq!(
        graph
            .execute("MATCH (n) RETURN n")
            .unwrap()
            .batches
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>(),
        1
    );
    assert_eq!(
        graph
            .open_research_branch(branch)
            .unwrap()
            .graph()
            .execute("MATCH (n) RETURN n")
            .unwrap()
            .batches
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>(),
        1
    );
}
