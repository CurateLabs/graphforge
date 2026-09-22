//! CLI comparison uses the native engine and durable endpoint identities.
use graphforge_api::*;
use std::{io::Cursor, process::Command};
use uuid::Uuid;
#[test]
fn cli_comparison_returns_native_arrow_after_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("project");
    let mut g = GraphForge::new(root.to_str()).unwrap();
    g.execute("CREATE (:Item {x:0})").unwrap();
    let capture = g
        .prepare_research_version(PrepareResearchVersionRequest {
            operation_uuid: Uuid::now_v7(),
            version_uuid: Uuid::now_v7(),
            context_uuid: Uuid::now_v7(),
            label: None,
            description: None,
            created_at: 1,
            required_versions: Default::default(),
        })
        .unwrap();
    let receipt = g
        .commit_research_version_operation(capture, &CancellationToken::new())
        .unwrap();
    g.execute("MATCH (n:Item) SET n.x = 1").unwrap();
    let before = g
        .research_project_summary()
        .unwrap()
        .identity
        .generation_uuid;
    drop(g);
    let file = directory.path().join("comparison.json");
    let request = serde_json::json!({"left":{"kind":"version","version_uuid":receipt.version_uuid.unwrap()},"right":{"kind":"project"},"detail":"changes","max_fields":40000,"max_bytes":67108864,"page_size":100});
    std::fs::write(&file, serde_json::to_vec(&request).unwrap()).unwrap();
    let result = Command::new(env!("CARGO_BIN_EXE_gf"))
        .arg("--project")
        .arg(&root)
        .args(["research", "compare", "--file"])
        .arg(&file)
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let batches = arrow::ipc::reader::StreamReader::try_new(Cursor::new(result.stdout), None)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    assert_eq!(
        batches[0]
            .column_by_name("change")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap()
            .value(0),
        "changed"
    );
    assert_eq!(
        GraphForge::new(root.to_str())
            .unwrap()
            .research_project_summary()
            .unwrap()
            .identity
            .generation_uuid,
        before
    );
}
