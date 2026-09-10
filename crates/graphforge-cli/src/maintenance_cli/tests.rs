//! Public CLI maintenance contracts with durable facade oracles.

use std::path::Path;

use arrow::array::{
    Array, BooleanArray, FixedSizeBinaryArray, Float64Array, Int8Array, Int64Array, StringArray,
    StructArray,
};
use graphforge_api::{CheckpointRequest, GraphForge, IrLiteral, OperationId};
use serde_json::{Value, json};
use uuid::Uuid;

fn run(path: &Path, args: &[&str], json: bool) -> crate::CliExecution {
    let mut argv = vec![
        "gf".to_owned(),
        "--project".into(),
        path.to_str().unwrap().into(),
    ];
    if json {
        argv.push("--json".into());
    }
    argv.extend(args.iter().map(|arg| (*arg).to_owned()));
    crate::execute(argv)
}

fn success(path: &Path, args: &[&str], json: bool) -> Value {
    let result = run(path, args, json);
    assert_eq!(
        result.exit_code,
        0,
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(result.stderr.is_empty());
    assert!(result.stdout.ends_with(b"\n"));
    serde_json::from_slice(&result.stdout).unwrap()
}

fn failure(path: &Path, args: &[&str], message: &str) {
    let result = run(path, args, true);
    assert_eq!(result.exit_code, 2);
    assert!(result.stdout.is_empty());
    let error: Value = serde_json::from_slice(&result.stderr).unwrap();
    assert_eq!(error["error"]["code"], "GF_VALIDATION");
    assert!(
        error["error"]["message"]
            .as_str()
            .unwrap()
            .contains(message),
        "{error}"
    );
}

fn generation(path: &Path) -> Uuid {
    graphforge_storage::resolve_project_generation(path)
        .unwrap()
        .generation_uuid()
}

fn count(graph: &GraphForge) -> i64 {
    let result = graph.execute("MATCH (n) RETURN count(n)").unwrap();
    let values = result.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(values.null_count(), 0);
    assert_eq!(values.len(), 1);
    values.value(0)
}

#[test]
fn transactions_commit_rollback_and_late_invalid_input_preserve_authority() {
    let root = tempfile::tempdir().unwrap();
    drop(GraphForge::new(root.path().to_str()).unwrap());
    let original = generation(root.path());
    let operation = Uuid::now_v7().to_string();
    let actor = Uuid::now_v7().to_string();
    let node = Uuid::now_v7();
    let spec = format!(
        r#"{node}:Person:{{"name":"Ada","active":true,"rank":7,"ratio":1.5,"missing":null}}"#
    );
    let committed = success(
        root.path(),
        &[
            "transaction",
            "commit",
            "--operation-uuid",
            &operation,
            "--actor-uuid",
            &actor,
            "--add-node",
            &spec,
        ],
        true,
    );
    assert_eq!(committed["phase"], "committed");
    assert_eq!(committed["operation_uuid"], operation);
    assert_eq!(
        committed["generation_uuid"],
        generation(root.path()).to_string()
    );
    assert_ne!(generation(root.path()), original);
    let graph = GraphForge::new(root.path().to_str()).unwrap();
    assert_eq!(count(&graph), 1);
    let rows = graph
        .execute(
            "MATCH (n:Person) RETURN n.node_uuid, n.name, n.active, n.rank, n.ratio, n.missing",
        )
        .unwrap();
    let row = &rows.batches[0];
    assert_eq!(row.num_rows(), 1);
    let id = row
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(id.null_count(), 0);
    assert_eq!(id.value(0), node.as_bytes());
    let names = row
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.iter().collect::<Vec<_>>(), vec![Some("Ada")]);
    let active = row
        .column(2)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert_eq!(active.iter().collect::<Vec<_>>(), vec![Some(true)]);
    let ranks = row.column(3).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(ranks.iter().collect::<Vec<_>>(), vec![Some(7)]);
    let ratios = row
        .column(4)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(ratios.null_count(), 0);
    assert_eq!(ratios.len(), 1);
    assert_eq!(ratios.value(0).to_bits(), 1.5_f64.to_bits());
    // The public scalar heterogeneous layout encodes logical null as tag4;
    // graphforge-value::heterogeneous::select_row maps that tag to Decoded::Null.
    let missing = row
        .column(5)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(
        missing
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        vec![
            "__het_tag",
            "__het_int",
            "__het_float",
            "__het_str",
            "__het_bool"
        ]
    );
    assert_eq!(
        missing
            .column(0)
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(4)]
    );
    assert!(
        missing.columns()[1..]
            .iter()
            .all(|column| column.is_null(0))
    );
    drop(graph);
    let cypher = success(
        root.path(),
        &[
            "transaction",
            "commit",
            "--operation-uuid",
            &Uuid::now_v7().to_string(),
            "--cypher",
            "MATCH (n:Person) SET n.rank = 9",
        ],
        false,
    );
    assert_eq!(cypher["phase"], "committed");
    assert_eq!(
        cypher["generation_uuid"],
        generation(root.path()).to_string()
    );
    let selected = generation(root.path());
    let rollback_operation = Uuid::now_v7().to_string();
    let rolled = success(
        root.path(),
        &[
            "transaction",
            "rollback",
            "--operation-uuid",
            &rollback_operation,
            "--cypher",
            "CREATE (:Discarded)",
        ],
        true,
    );
    assert_eq!(
        rolled,
        json!({"phase":"rolled_back", "operation_uuid":rollback_operation})
    );
    assert_eq!(generation(root.path()), selected);
    let valid = format!("{}:Transient", Uuid::now_v7());
    failure(
        root.path(),
        &[
            "transaction",
            "commit",
            "--operation-uuid",
            &Uuid::now_v7().to_string(),
            "--add-node",
            &valid,
            "--add-node",
            "not-a-uuid:Person",
        ],
        "UUID",
    );
    assert_eq!(generation(root.path()), selected);
    let graph = GraphForge::new(root.path().to_str()).unwrap();
    assert_eq!(count(&graph), 1);
    let rank = graph.execute("MATCH (n:Person) RETURN n.rank").unwrap();
    assert_eq!(
        rank.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(9)]
    );
}

#[test]
fn transaction_validation_errors_never_publish() {
    let root = tempfile::tempdir().unwrap();
    drop(GraphForge::new(root.path().to_str()).unwrap());
    let selected = generation(root.path());
    let op = Uuid::now_v7().to_string();
    let node = Uuid::now_v7().to_string();
    let cases = vec![
        (
            vec!["--operation-uuid".into(), op.clone()],
            "requires at least one",
        ),
        (
            vec![
                "--operation-uuid".into(),
                "bad".into(),
                "--cypher".into(),
                "CREATE (:X)".into(),
            ],
            "UUID",
        ),
        (
            vec![
                "--operation-uuid".into(),
                op.clone(),
                "--actor-uuid".into(),
                "bad".into(),
                "--cypher".into(),
                "CREATE (:X)".into(),
            ],
            "UUID",
        ),
    ];
    for (args, message) in cases {
        let mut command = vec!["transaction", "commit"];
        command.extend(args.iter().map(String::as_str));
        failure(root.path(), &command, message);
    }
    for (suffix, message) in [
        ("", "uuid:Label"),
        (":X:{", "invalid --add-node JSON"),
        (":X:[]", "JSON object"),
        (":X:{\"bad\":[]}", "null/bool/number/string"),
        (":X:{\"bad\":{}}", "null/bool/number/string"),
    ] {
        let spec = format!("{node}{suffix}");
        failure(
            root.path(),
            &[
                "transaction",
                "commit",
                "--operation-uuid",
                &op,
                "--add-node",
                &spec,
            ],
            message,
        );
        assert_eq!(generation(root.path()), selected);
        assert_eq!(count(&GraphForge::new(root.path().to_str()).unwrap()), 0);
    }
}

#[test]
fn cleanup_and_recovery_preserve_checkpoint_reachable_data() {
    let root = tempfile::tempdir().unwrap();
    let graph = GraphForge::new(root.path().to_str()).unwrap();
    graph.execute("CREATE (:Kept {rank:1})").unwrap();
    let retained = generation(root.path());
    graph
        .checkpoint(CheckpointRequest {
            name: "Kept".into(),
            description: None,
            idempotency_key: OperationId(Uuid::now_v7()),
            actor_uuid: None,
        })
        .unwrap();
    graph.execute("CREATE (:Later)").unwrap();
    graph.execute("CREATE (:Latest)").unwrap();
    drop(graph);
    let selected = generation(root.path());
    let recovery = success(root.path(), &["recovery"], true);
    assert_eq!(recovery["kind"], "project_open");
    assert_eq!(recovery["selected_generation_uuid"], selected.to_string());
    assert_eq!(recovery["work_detected"], false);
    let reach = success(
        root.path(),
        &["maintenance", "reachability", "--retained-ancestors", "0"],
        false,
    );
    assert_eq!(reach["selected_generation_uuid"], selected.to_string());
    assert!(
        reach["checkpoint_roots"]
            .as_array()
            .unwrap()
            .contains(&json!(retained.to_string()))
    );
    assert!(
        reach["reachable"]
            .as_array()
            .unwrap()
            .contains(&json!(selected.to_string()))
    );
    let generation_paths = || {
        let mut paths = std::fs::read_dir(root.path().join("generations"))
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        paths.sort();
        paths
    };
    let before_paths = generation_paths();
    let before_current = std::fs::read(root.path().join("CURRENT")).unwrap();
    let preview = success(
        root.path(),
        &[
            "maintenance",
            "cleanup-preview",
            "--retained-ancestors",
            "0",
        ],
        true,
    );
    assert_eq!(generation_paths(), before_paths);
    assert_eq!(
        std::fs::read(root.path().join("CURRENT")).unwrap(),
        before_current
    );
    assert_eq!(preview["dry_run"], true);
    assert_eq!(preview["removed"], 0);
    assert_eq!(preview["selected_generation_uuid"], selected.to_string());
    assert!(preview["candidates"].as_u64().unwrap() > 0);
    assert_eq!(generation(root.path()), selected);
    failure(
        root.path(),
        &["maintenance", "cleanup-execute"],
        "requires --yes",
    );
    let removed = success(
        root.path(),
        &[
            "maintenance",
            "cleanup-execute",
            "--retained-ancestors",
            "0",
            "--yes",
        ],
        true,
    );
    assert_eq!(removed["dry_run"], false);
    assert!(removed["removed"].as_u64().unwrap() > 0);
    assert_eq!(generation(root.path()), selected);
    let graph = GraphForge::new(root.path().to_str()).unwrap();
    assert_eq!(count(&graph), 3);
    let view = graph.open_checkpoint("Kept").unwrap();
    let rows = view.execute("MATCH (n:Kept) RETURN n.rank").unwrap();
    assert_eq!(
        rows.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(1)]
    );
    // ProjectRetentionLimits::validate documents GF_RESOURCE_LIMIT for zero bounds;
    // CLI Project errors retain their code and use exit1 (not validation exit2).
    let limited = run(
        root.path(),
        &["maintenance", "reachability", "--max-entries", "0"],
        true,
    );
    assert_eq!(limited.exit_code, 1);
    assert!(limited.stdout.is_empty());
    let error: Value = serde_json::from_slice(&limited.stderr).unwrap();
    assert_eq!(error["error"]["code"], "GF_RESOURCE_LIMIT");
    assert!(
        error["error"]["message"]
            .as_str()
            .unwrap()
            .contains("resource=max_entries limit=0")
    );
}

#[test]
fn compaction_cli_preserves_typed_delta_graph_and_reports_work() {
    use graphforge_storage::{
        GraphDeltaJournalLimits, GraphDeltaOp, GraphDeltaOpKind, GraphDeltaPayload,
        GraphDeltaPublishRequest, encode_graph_delta_value,
    };
    let root = tempfile::tempdir().unwrap();
    let graph = GraphForge::new(root.path().to_str()).unwrap();
    graph.execute("CREATE (:Base)").unwrap();
    let created = graph.execute("CREATE (n) RETURN n.node_uuid").unwrap();
    let ids = created.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
        .unwrap();
    let node = Uuid::from_slice(ids.value(0)).unwrap();
    drop(graph);
    graphforge_storage::publish_graph_delta(
        root.path(),
        &GraphDeltaPublishRequest {
            transaction_uuid: Uuid::now_v7(),
            generation_uuid: Uuid::now_v7(),
            run_uuid: Uuid::now_v7(),
            limits: GraphDeltaJournalLimits::default(),
            operations: vec![GraphDeltaOp {
                operation_uuid: Uuid::now_v7(),
                kind: GraphDeltaOpKind::SetNodeProperty,
                payload: GraphDeltaPayload::SetNodeProperty {
                    node_uuid: node.to_string(),
                    property_stem: "_untyped".into(),
                    key: "rank".into(),
                    value: encode_graph_delta_value(&IrLiteral::Int(7)).unwrap(),
                },
            }],
        },
    )
    .unwrap();
    let node_ids = |graph: &GraphForge| {
        let rows = graph.execute("MATCH (n) RETURN n.node_uuid").unwrap();
        let mut ids = Vec::new();
        for batch in &rows.batches {
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            assert_eq!(values.null_count(), 0);
            ids.extend(
                values
                    .iter()
                    .map(|value| Uuid::from_slice(value.unwrap()).unwrap()),
            );
        }
        ids.sort_unstable();
        ids
    };
    let before = GraphForge::new(root.path().to_str()).unwrap();
    let before_ids = node_ids(&before);
    assert_eq!(before_ids.len(), 2);
    assert!(before_ids.contains(&node));
    drop(before);
    let selected = generation(root.path());
    let status = success(
        root.path(),
        &[
            "maintenance",
            "compaction-status",
            "--compact-when-runs",
            "1",
            "--compact-when-run-bytes",
            "1",
            "--compact-when-replay-memory-bytes",
            "1",
        ],
        true,
    );
    assert_eq!(status["generation_uuid"], selected.to_string());
    assert_eq!(status["run_count"], 1);
    assert_eq!(status["should_compact"], true);
    assert!(status["run_bytes"].as_u64().unwrap() > 0);
    assert!(status["trigger_reasons"].as_array().unwrap().len() >= 1);
    let tx = Uuid::now_v7().to_string();
    let output = Uuid::now_v7().to_string();
    let flags = [
        "--transaction-uuid",
        &tx,
        "--generation-uuid",
        &output,
        "--through-run-sequence",
        "1",
    ];
    let command = |verb| {
        let mut command = vec!["maintenance", verb];
        command.extend(flags);
        command
    };
    let preview = success(root.path(), &command("compaction-preview"), true);
    assert_eq!(preview["dry_run"], true);
    assert_eq!(preview["input_generation_uuid"], selected.to_string());
    assert_eq!(preview["compacted_runs"], 1);
    assert_eq!(generation(root.path()), selected);
    failure(root.path(), &command("compaction-run"), "requires --yes");
    let mut run_args = command("compaction-run");
    run_args.extend([
        "--yes",
        "--cleanup-after-commit",
        "--retained-ancestors",
        "0",
    ]);
    let report = success(root.path(), &run_args, false);
    assert_eq!(report["dry_run"], false);
    assert_eq!(report["output_generation_uuid"], output);
    assert_eq!(report["compacted_runs"], 1);
    assert_eq!(report["retained_suffix_runs"], 0);
    assert_eq!(report["state_fingerprint"], status["state_fingerprint"]);
    assert!(report["output_bytes"].as_u64().unwrap() > 0);
    assert!(report["cleanup"].is_object());
    assert_eq!(generation(root.path()).to_string(), output);
    let graph = GraphForge::new(root.path().to_str()).unwrap();
    assert_eq!(count(&graph), 2);
    assert_eq!(node_ids(&graph), before_ids);
    let rows = graph
        .execute("MATCH (n) WHERE n.rank = 7 RETURN n.node_uuid, n.rank")
        .unwrap();
    assert_eq!(
        rows.batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>(),
        1
    );
    let row = &rows.batches[0];
    let ids = row
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(ids.null_count(), 0);
    assert_eq!(ids.value(0), node.as_bytes());
    assert_eq!(
        row.column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(7)]
    );
    drop(graph);
    let after = success(root.path(), &["maintenance", "compaction-status"], true);
    assert_eq!(after["run_count"], 0);
    assert_eq!(after["should_compact"], false);
    failure(
        root.path(),
        &[
            "maintenance",
            "compaction-preview",
            "--transaction-uuid",
            "bad",
            "--generation-uuid",
            &output,
        ],
        "UUID",
    );
    failure(
        root.path(),
        &[
            "maintenance",
            "compaction-preview",
            "--transaction-uuid",
            &tx,
            "--generation-uuid",
            "bad",
        ],
        "UUID",
    );
}
