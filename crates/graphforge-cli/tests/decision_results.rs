//! Same-build CLI validation of externally produced decision batches.
use arrow::ipc::reader::StreamReader;
use graphforge_api::GraphForge;
use std::{io::Cursor, path::Path, process::Command};
use uuid::Uuid;

fn run(root: &Path, input: &Path, output: &Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_gf"))
        .args(["--project"])
        .arg(root)
        .args(["research", "decision", "validate", "--file"])
        .arg(input)
        .arg("--output")
        .arg(output)
        .output()
        .unwrap()
}

#[test]
fn cli_validates_external_decisions_and_writes_arrow_ipc() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("project");
    let graph = GraphForge::new(root.to_str()).unwrap();
    let before = graph
        .research_project_summary()
        .unwrap()
        .identity
        .generation_uuid;
    drop(graph);

    let question = Uuid::now_v7();
    let item = Uuid::now_v7();
    let request = serde_json::json!({
        "input": {
            "generation_uuid": Uuid::now_v7(),
            "version_uuid": null,
            "projection_sha256": vec![7; 32],
            "selection_sha256": vec![9; 32],
            "selected_item_uuids": [item],
        },
        "producer": { "name": "offline CLI fixture" },
        "questions": [{
            "question_uuid": question,
            "text": "Where should this item go?",
            "item_uuids": [item],
            "kind": { "kind": "choice", "allowed_choices": ["research", "human_review"] },
        }],
        "results": [{
            "question_uuid": question,
            "item_uuid": item,
            "status": "answered",
            "value": { "kind": "choice", "value": "research" },
        }],
    });
    serde_json::from_value::<graphforge_api::DecisionBatchV1>(request.clone())
        .unwrap_or_else(|error| panic!("{error}; request={request}"))
        .validate()
        .unwrap();
    let input = directory.path().join("decision.json");
    let arrow = directory.path().join("decision.arrow");
    std::fs::write(&input, serde_json::to_vec(&request).unwrap()).unwrap();

    let result = run(&root, &input, &arrow);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let mut reader =
        StreamReader::try_new(Cursor::new(std::fs::read(arrow).unwrap()), None).unwrap();
    let batch = reader.next().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch
            .schema()
            .metadata()
            .get("graphforge.contract")
            .unwrap(),
        "decision_result/1"
    );
    let graph = GraphForge::new(root.to_str()).unwrap();
    assert_eq!(
        graph
            .research_project_summary()
            .unwrap()
            .identity
            .generation_uuid,
        before
    );
}

#[test]
fn cli_rejects_decision_input_over_the_byte_bound() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("project");
    let graph = GraphForge::new(root.to_str()).unwrap();
    drop(graph);

    let input = directory.path().join("oversized-decision.json");
    let output = directory.path().join("decision.arrow");
    std::fs::write(&input, vec![b' '; 1024 * 1024 + 1]).unwrap();
    let result = run(&root, &input, &output);
    assert!(!result.status.success());
    assert!(
        String::from_utf8_lossy(&result.stderr)
            .contains("decision batch input exceeds its byte bound")
    );
    assert!(!output.exists());
}
