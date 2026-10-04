//! Real CLI saved query CRUD, aggregate execution and exact historical reads.
use graphforge_api::*;
use std::{io::Cursor, path::Path, process::Command};
use uuid::Uuid;
fn run(root: &Path, args: &[&str], json: bool) -> std::process::Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_gf"));
    command.arg("--project").arg(root);
    if json {
        command.arg("--json");
    }
    command.arg("saved-query").args(args).output().unwrap()
}
fn success(output: std::process::Output) -> Vec<u8> {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout
}
fn json(output: std::process::Output) -> serde_json::Value {
    serde_json::from_slice(&success(output)).unwrap()
}
#[test]
fn cli_saved_queries_survive_reopen_and_preserve_historical_aggregate() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("project");
    let graph = GraphForge::new(root.to_str()).unwrap();
    graph
        .execute("CREATE (:Item {score:1}), (:Item {score:3})")
        .unwrap();
    drop(graph);
    let id = Uuid::now_v7().to_string();
    let saved = serde_json::json!({"query_uuid":id,"name":"Threshold","description":null,"query":"MATCH (n:Item) WHERE n.score >= $minimum RETURN count(n) AS total","parameters":{"minimum":"integer"}});
    let file = directory.path().join("definition.json");
    let params = directory.path().join("params.json");
    std::fs::write(&file, serde_json::to_vec(&saved).unwrap()).unwrap();
    std::fs::write(&params, r#"{"minimum":2}"#).unwrap();
    let file = file.to_str().unwrap();
    let params = params.to_str().unwrap();
    assert_eq!(json(run(&root, &["save", "--file", file], true)), saved);
    assert_eq!(json(run(&root, &["show", &id], true)), saved);
    assert_eq!(
        json(run(&root, &["list"], true)),
        serde_json::json!([saved])
    );
    assert_eq!(
        json(run(&root, &["run", &id, "--params", params], true))["rows"],
        serde_json::json!([[1]])
    );
    let bytes = success(run(&root, &["run", &id, "--params", params], false));
    let mut reader = arrow::ipc::reader::StreamReader::try_new(Cursor::new(bytes), None).unwrap();
    let batch = reader.next().unwrap().unwrap();
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0),
        1
    );
    assert!(!run(&root, &["run", &id], true).status.success());
    assert!(!run(&root, &["save", "--file", file], true).status.success());
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    let version = Uuid::now_v7();
    let operation = graph
        .prepare_research_version(PrepareResearchVersionRequest {
            author: None,
            committer: None,
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
    graph.execute("CREATE (:Item {score:4})").unwrap();
    drop(graph);
    let mut updated = saved.clone();
    updated["name"] = serde_json::json!("Updated");
    std::fs::write(file, serde_json::to_vec(&updated).unwrap()).unwrap();
    assert_eq!(json(run(&root, &["update", "--file", file], true)), updated);
    let version = version.to_string();
    assert_eq!(
        json(run(&root, &["show", &id, "--version-uuid", &version], true)),
        saved
    );
    assert_eq!(
        json(run(
            &root,
            &["run", &id, "--params", params, "--version-uuid", &version],
            true
        ))["rows"],
        serde_json::json!([[1]])
    );
    assert_eq!(
        json(run(&root, &["run", &id, "--params", params], true))["rows"],
        serde_json::json!([[2]])
    );
    success(run(&root, &["delete", &id], true));
    assert_eq!(json(run(&root, &["list"], true)), serde_json::json!([]));
    assert!(!run(&root, &["show", &id], true).status.success());
    assert_eq!(
        json(run(&root, &["show", &id, "--version-uuid", &version], true)),
        saved
    );
}

#[test]
fn cli_json_numbers_follow_saved_parameter_declarations_and_uuid_tags() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("project");
    drop(GraphForge::new(root.to_str()).unwrap());
    let id = Uuid::now_v7().to_string();
    let file = directory.path().join("definition.json");
    let params = directory.path().join("params.json");
    let mut saved = serde_json::json!({"query_uuid":id,"name":"Scalar parameter","description":null,"query":"RETURN $x AS value","parameters":{"x":"float"}});
    std::fs::write(&file, serde_json::to_vec(&saved).unwrap()).unwrap();
    let file = file.to_str().unwrap();
    let params = params.to_str().unwrap();
    success(run(&root, &["save", "--file", file], true));
    std::fs::write(params, r#"{"x":1}"#).unwrap();
    let result = json(run(&root, &["run", &id, "--params", params], true));
    assert_eq!(result["columns"][0]["data_type"], "Float64");
    assert_eq!(result["rows"], serde_json::json!([[1.0]]));
    saved["parameters"]["x"] = serde_json::json!("integer");
    std::fs::write(file, serde_json::to_vec(&saved).unwrap()).unwrap();
    success(run(&root, &["update", "--file", file], true));
    for (input, expected) in [
        (r#"{"x":1.0}"#, 1_i64),
        (r#"{"x":4294967296}"#, 4_294_967_296),
    ] {
        std::fs::write(params, input).unwrap();
        let result = json(run(&root, &["run", &id, "--params", params], true));
        assert_eq!(result["columns"][0]["data_type"], "Int64");
        assert_eq!(result["rows"], serde_json::json!([[expected]]));
    }
    for input in [
        r#"{"x":1.5}"#,
        r#"{"x":9007199254740992.0}"#,
        r#"{"x":"1"}"#,
        r#"{"x":true}"#,
    ] {
        std::fs::write(params, input).unwrap();
        let output = run(&root, &["run", &id, "--params", params], true);
        assert!(!output.status.success());
        let error: serde_json::Value = serde_json::from_slice(&output.stderr).unwrap();
        assert_eq!(error["error"]["code"], "GF_VALIDATION");
    }
    saved["parameters"]["x"] = serde_json::json!("uuid");
    saved["query"] =
        serde_json::json!("MATCH (n:Item) WHERE n.node_uuid = $x RETURN count(n) AS value");
    std::fs::write(file, serde_json::to_vec(&saved).unwrap()).unwrap();
    success(run(&root, &["update", "--file", file], true));
    let graph = GraphForge::new(root.to_str()).unwrap();
    let value = graph
        .add_node("Item", &std::collections::HashMap::new())
        .unwrap()
        .uuid;
    drop(graph);
    std::fs::write(
        params,
        serde_json::to_vec(&serde_json::json!({"x":{"$uuid":value}})).unwrap(),
    )
    .unwrap();
    let result = json(run(&root, &["run", &id, "--params", params], true));
    assert_eq!(result["rows"], serde_json::json!([[1]]));
    std::fs::write(
        params,
        serde_json::to_vec(&serde_json::json!({"x":{"$uuid":Uuid::now_v7()}})).unwrap(),
    )
    .unwrap();
    let result = json(run(&root, &["run", &id, "--params", params], true));
    assert_eq!(result["rows"], serde_json::json!([[0]]));
}
