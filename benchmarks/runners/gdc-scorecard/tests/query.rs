//! The query driver against real durable projects: reconciliation after
//! reopen, typed count mismatches, the measured pass and the result digest.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use gdc_scorecard::query::{
    Evidence, LATENCY_CLOCK, Measured, QueryCause, QueryError, Rendered, ResultsDir, Sample,
    nearest_rank, result_digest, run, run_with_results,
};
use graphforge_api::GraphForge;
use serde_json::{Value, json};

/// Four people, two cities; three KNOWS and three LIVES_IN edges.
fn durable_project(root: &Path) -> std::path::PathBuf {
    let path = root.join("project");
    let forge = GraphForge::new(Some(path.to_str().unwrap())).unwrap();
    forge
        .execute(
            "CREATE (a:Person {id: 1, name: 'Ada'}), (b:Person {id: 2, name: 'Bo'}), \
             (c:Person {id: 3, name: 'Cy'}), (d:Person {id: 4, name: 'Di'}), \
             (x:City {id: 10}), (y:City {id: 11}), \
             (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), (c)-[:KNOWS]->(d), \
             (a)-[:LIVES_IN]->(x), (b)-[:LIVES_IN]->(x), (c)-[:LIVES_IN]->(y)",
        )
        .unwrap();
    drop(forge);
    path
}

fn expected(person: u64, knows: u64) -> Value {
    json!({
        "schema": "graphforge-gdc-expected-counts/1",
        "source": "tests/query.rs fixture",
        "nodes": 6,
        "edges": 6,
        "labels": {"Person": person, "City": 2},
        "types": {"KNOWS": knows, "LIVES_IN": 6 - knows},
    })
}

fn int(value: i64) -> Value {
    json!({"type": "Int", "value": value})
}

fn workload(variants: Value) -> Value {
    json!({"schema": "graphforge-gdc-query-workload/1", "suite": "fixture", "variants": variants})
}

fn measured(sample: &Sample) -> &Measured {
    sample.measured().expect("a measured sample")
}

fn people_by_min_id() -> Value {
    json!({
        "id": "people-by-min-id",
        "ordered": true,
        "operation": {"kind": "cypher",
            "text": "MATCH (p:Person) WHERE p.id >= $min RETURN p.id AS id, p.name AS name ORDER BY id"},
        "bindings": [
            {"id": "min-1", "params": {"min": int(1)}},
            {"id": "min-2", "params": {"min": int(2)}},
            {"id": "min-4", "params": {"min": int(4)}},
        ],
    })
}

fn drive(project: &Path, workload: &Value, expected: &Value) -> Result<Evidence, QueryError> {
    run(
        project,
        &serde_json::to_vec(workload).unwrap(),
        &serde_json::to_vec(expected).unwrap(),
        "0".repeat(64),
    )
}

fn cause(result: Result<Evidence, QueryError>) -> (QueryCause, String) {
    let error = result.expect_err("the driver must refuse");
    (error.cause(), error.message().to_owned())
}

#[test]
fn durable_project_reconciles_after_reopen_and_every_binding_is_measured() {
    let root = tempfile::tempdir().unwrap();
    let project = durable_project(root.path());
    let evidence = drive(
        &project,
        &workload(json!([people_by_min_id()])),
        &expected(4, 3),
    )
    .unwrap();
    let document = serde_json::to_value(&evidence).unwrap();

    assert_eq!(document["schema"], "graphforge-gdc-query-evidence/1");
    assert_eq!(document["certification"], false);
    assert_eq!(
        document["project"]["opened_with"],
        "graphforge_api::GraphForge::new(Some(path))"
    );
    let reconciliation = &document["reconciliation"];
    assert_eq!(reconciliation["status"], "reconciled");
    assert_eq!(
        reconciliation["nodes"],
        json!({"expected": 6, "observed": 6})
    );
    assert_eq!(
        reconciliation["edges"],
        json!({"expected": 6, "observed": 6})
    );
    assert_eq!(reconciliation["labels"]["Person"]["observed"], 4);
    assert_eq!(reconciliation["types"]["LIVES_IN"]["observed"], 3);
    assert_eq!(document["latency_clock"]["id"], LATENCY_CLOCK);

    let [variant] = evidence.variants.as_slice() else {
        panic!("one variant")
    };
    assert_eq!(variant.query_id, "people-by-min-id");
    assert_eq!(variant.warmup.binding_id, "min-1");
    assert!(variant.warmup.excluded);
    let bindings: Vec<_> = variant
        .samples
        .iter()
        .map(|s| s.binding_id.as_str())
        .collect();
    assert_eq!(bindings, ["min-1", "min-2", "min-4"]);
    let rows: Vec<_> = variant.samples.iter().map(|s| measured(s).rows).collect();
    assert_eq!(rows, [4, 3, 1]);
    assert!(variant.samples.iter().all(|s| measured(s).latency_ns > 0));
    let mut latencies: Vec<_> = variant
        .samples
        .iter()
        .map(|s| measured(s).latency_ns)
        .collect();
    latencies.sort_unstable();
    let summary = variant.summary.as_ref().unwrap();
    assert_eq!(summary.count, 3);
    assert_eq!(summary.p50_ns, latencies[1]);
    assert_eq!(summary.p95_ns, latencies[2]);
    assert_eq!(document["status"], "passed");
    assert_eq!(document["failures"], json!([]));
    assert_eq!(document["variants"][0]["status"], "measured");
    // The warm-up carries no latency at all.
    assert_eq!(
        document["variants"][0]["warmup"],
        json!({"binding_id": "min-1", "excluded": true, "completed": true})
    );

    // The digest names the result: different answers differ, the same answer
    // from a second run over the reopened project is identical.
    let digests: Vec<_> = variant
        .samples
        .iter()
        .map(|s| measured(s).result_sha256.clone())
        .collect();
    assert_ne!(digests[0], digests[1]);
    assert_ne!(digests[1], digests[2]);
    let again = drive(
        &project,
        &workload(json!([people_by_min_id()])),
        &expected(4, 3),
    )
    .unwrap();
    let repeated: Vec<_> = again.variants[0]
        .samples
        .iter()
        .map(|s| measured(s).result_sha256.clone())
        .collect();
    assert_eq!(digests, repeated);
}

#[test]
fn deliberate_count_mismatches_fail_typed() {
    let root = tempfile::tempdir().unwrap();
    let project = durable_project(root.path());
    let workload = workload(json!([people_by_min_id()]));

    let (code, message) = cause(drive(&project, &workload, &expected(5, 3)));
    assert_eq!(code, QueryCause::CountMismatch);
    assert_eq!(code.as_str(), "count_mismatch");
    assert!(
        message.contains("label Person: expected 5, read 4"),
        "{message}"
    );

    let (code, message) = cause(drive(&project, &workload, &expected(4, 2)));
    assert_eq!(code, QueryCause::CountMismatch);
    assert!(
        message.contains("type KNOWS: expected 2, read 3"),
        "{message}"
    );
    assert!(
        message.contains("type LIVES_IN: expected 4, read 3"),
        "{message}"
    );

    let mut wrong_total = expected(4, 3);
    wrong_total["nodes"] = json!(7);
    let (code, message) = cause(drive(&project, &workload, &wrong_total));
    assert_eq!(code, QueryCause::CountMismatch);
    assert_eq!(message, "nodes: expected 7, read 6");

    let mut undeclared = expected(4, 3);
    undeclared["labels"].as_object_mut().unwrap().remove("City");
    undeclared["nodes"] = json!(6);
    let (code, message) = cause(drive(&project, &workload, &undeclared));
    assert_eq!(code, QueryCause::CountMismatch);
    assert_eq!(message, "label City: present but not declared");
}

#[test]
fn a_missing_project_is_refused_not_created() {
    let root = tempfile::tempdir().unwrap();
    let absent = root.path().join("absent");
    let (code, _) = cause(drive(
        &absent,
        &workload(json!([people_by_min_id()])),
        &expected(4, 3),
    ));
    assert_eq!(code, QueryCause::ProjectMissing);
    assert!(!absent.exists());
    // An empty directory would be initialized as a new project; it is refused too.
    std::fs::create_dir(&absent).unwrap();
    let (code, _) = cause(drive(
        &absent,
        &workload(json!([people_by_min_id()])),
        &expected(4, 3),
    ));
    assert_eq!(code, QueryCause::ProjectMissing);
    assert_eq!(std::fs::read_dir(&absent).unwrap().count(), 0);
    // A populated directory that is not a project is refused by the product.
    std::fs::write(absent.join("notes.txt"), "not a project").unwrap();
    let (code, _) = cause(drive(
        &absent,
        &workload(json!([people_by_min_id()])),
        &expected(4, 3),
    ));
    assert_eq!(code, QueryCause::ProjectOpenFailed);
}

#[test]
fn analyst_verbs_run_through_the_same_clock() {
    let root = tempfile::tempdir().unwrap();
    let project = durable_project(root.path());
    let variants = json!([
        {"id": "person-components", "ordered": false, "bindings": [{"id": "all"}],
         "operation": {"kind": "cluster", "label": "Person", "by": "components", "directed": false}},
        {"id": "person-degree", "ordered": false, "bindings": [{"id": "all"}],
         "operation": {"kind": "rank", "label": "Person", "by": "degree", "directed": true}},
        {"id": "bfs-from-person", "ordered": false, "bindings": [
            {"id": "from-1", "params": {"source": int(1)}},
            {"id": "from-3", "params": {"source": int(3)}}],
         "operation": {"kind": "paths", "by": "bfs", "directed": true, "via": "KNOWS",
            "source": {"label": "Person", "property": "id", "param": "source"}}},
    ]);
    let evidence = drive(&project, &workload(variants), &expected(4, 3)).unwrap();
    let interfaces: Vec<_> = evidence.variants.iter().map(|v| v.interface).collect();
    assert_eq!(
        interfaces,
        [
            "graphforge_api::GraphForge::cluster",
            "graphforge_api::GraphForge::rank",
            "graphforge_api::GraphForge::paths",
        ]
    );
    assert_eq!(evidence.status, "passed");
    assert_eq!(measured(&evidence.variants[0].samples[0]).rows, 4);
    assert_eq!(measured(&evidence.variants[1].samples[0]).rows, 4);
    let bfs = &evidence.variants[2].samples;
    let (from_1, from_3) = (measured(&bfs[0]).rows, measured(&bfs[1]).rows);
    assert!(from_1 > from_3, "{from_1} vs {from_3}");
}

#[test]
fn every_measured_result_is_written_with_the_cells_its_digest_covers() {
    let root = tempfile::tempdir().unwrap();
    let project = durable_project(root.path());
    let results_path = root.path().join("results");
    std::fs::create_dir(&results_path).unwrap();
    let results = ResultsDir::new(&results_path).unwrap();
    let broken = json!({"id": "broken", "ordered": true, "bindings": [{"id": "only"}],
        "operation": {"kind": "cypher", "text": "MATCH (p:Person RETURN p"}});
    let evidence = run_with_results(
        &project,
        &serde_json::to_vec(&workload(json!([people_by_min_id(), broken]))).unwrap(),
        &serde_json::to_vec(&expected(4, 3)).unwrap(),
        "0".repeat(64),
        Some(&results),
    )
    .unwrap();

    // One file per measured sample, in run order; the failed sample writes none.
    let mut names: Vec<_> = std::fs::read_dir(&results_path)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    assert_eq!(names, ["00000000.json", "00000001.json", "00000002.json"]);
    let read = |name: &str| -> Value {
        serde_json::from_slice(&std::fs::read(results_path.join(name)).unwrap()).unwrap()
    };
    let last = read("00000002.json");
    assert_eq!(last["schema"], "graphforge-gdc-query-result/1");
    assert_eq!(
        (&last["query_id"], &last["binding_id"], &last["ordered"]),
        (&json!("people-by-min-id"), &json!("min-4"), &json!(true))
    );
    assert_eq!(
        last["columns"],
        json!([{"name": "id", "type": "Int64"}, {"name": "name", "type": "Utf8"}])
    );
    assert_eq!(last["rows"], json!([["4", "Di"]]));
    for (index, sample) in evidence.variants[0].samples.iter().enumerate() {
        let written = read(&format!("{index:08}.json"));
        assert_eq!(written["binding_id"], sample.binding_id.as_str());
        assert_eq!(
            written["result_sha256"],
            measured(sample).result_sha256.as_str()
        );
        // The written cells are the digest's own input: re-rendering them
        // reproduces the measured digest.
        let rendered = Rendered {
            columns: written["columns"]
                .as_array()
                .unwrap()
                .iter()
                .map(|c| {
                    (
                        c["name"].as_str().unwrap().into(),
                        c["type"].as_str().unwrap().into(),
                    )
                })
                .collect(),
            rows: serde_json::from_value(written["rows"].clone()).unwrap(),
        };
        assert_eq!(rendered.digest(true), measured(sample).result_sha256);
    }
    assert_eq!(evidence.status, "failed");

    // A directory that already holds results is refused, so two runs never mix.
    let (code, message) = (
        ResultsDir::new(&results_path).unwrap_err().cause(),
        ResultsDir::new(&results_path)
            .unwrap_err()
            .message()
            .to_owned(),
    );
    assert_eq!(code, QueryCause::Io);
    assert!(message.contains("is not empty"), "{message}");
}

#[test]
fn malformed_workloads_and_counts_are_refused_before_opening() {
    // No project exists: each document is refused before the driver opens one.
    let root = tempfile::tempdir().unwrap();
    let project = root.path().join("absent");
    let counts = expected(4, 3);
    let refused = |variants: Value| cause(drive(&project, &workload(variants), &counts)).0;

    let mut repeated = people_by_min_id();
    repeated["bindings"][1]["id"] = json!("min-1");
    assert_eq!(refused(json!([repeated])), QueryCause::InvalidWorkload);
    assert_eq!(
        refused(json!([people_by_min_id(), people_by_min_id()])),
        QueryCause::InvalidWorkload
    );
    let mut unbound = people_by_min_id();
    unbound["bindings"] = json!([]);
    assert_eq!(refused(json!([unbound])), QueryCause::InvalidWorkload);
    let mut undeclared_order = people_by_min_id();
    undeclared_order.as_object_mut().unwrap().remove("ordered");
    assert_eq!(
        refused(json!([undeclared_order])),
        QueryCause::InvalidWorkload
    );
    assert_eq!(
        refused(json!([{"id": "bfs", "ordered": false,
            "bindings": [{"id": "list", "params": {"source": {"type": "List", "value": []}}}],
            "operation": {"kind": "paths", "by": "bfs", "directed": true,
                "source": {"label": "Person", "property": "id", "param": "source"}}}])),
        QueryCause::InvalidWorkload
    );
    let mut untyped = people_by_min_id();
    untyped["bindings"][0]["params"]["min"] = json!(1);
    assert_eq!(refused(json!([untyped])), QueryCause::InvalidWorkload);
    assert_eq!(
        refused(
            json!([{"id": "bfs", "ordered": false, "bindings": [{"id": "none"}],
            "operation": {"kind": "paths", "by": "bfs", "directed": true,
                "source": {"label": "Person", "property": "id", "param": "source"}}}])
        ),
        QueryCause::InvalidWorkload
    );
    assert_eq!(
        refused(
            json!([{"id": "nope", "ordered": false, "bindings": [{"id": "all"}],
            "operation": {"kind": "rank", "label": "Person", "by": "no_such_rank", "directed": true}}])
        ),
        QueryCause::InvalidWorkload
    );

    let mut unbalanced = expected(4, 3);
    unbalanced["edges"] = json!(7);
    let (code, _) = cause(drive(
        &project,
        &workload(json!([people_by_min_id()])),
        &unbalanced,
    ));
    assert_eq!(code, QueryCause::InvalidExpectedCounts);
}

#[test]
fn a_failing_variant_between_two_good_ones_is_recorded_and_the_run_continues() {
    let root = tempfile::tempdir().unwrap();
    let project = durable_project(root.path());
    let variants = json!([
        people_by_min_id(),
        {"id": "broken", "ordered": true,
         "bindings": [{"id": "first"}, {"id": "second"}],
         "operation": {"kind": "cypher", "text": "MATCH (p:Person RETURN p"}},
        {"id": "person-components", "ordered": false, "bindings": [{"id": "all"}],
         "operation": {"kind": "cluster", "label": "Person", "by": "components", "directed": false}},
    ]);
    let evidence = drive(&project, &workload(variants), &expected(4, 3)).unwrap();
    let document = serde_json::to_value(&evidence).unwrap();

    assert_eq!(document["status"], "failed");
    assert_eq!(
        document["failures"],
        json!([
            {"query_id": "broken", "binding_id": "first", "cause": "query_failed", "error_code": "GF_PARSE"},
            {"query_id": "broken", "binding_id": "second", "cause": "query_failed", "error_code": "GF_PARSE"},
        ])
    );
    let statuses: Vec<_> = evidence.variants.iter().map(|v| v.status).collect();
    assert_eq!(statuses, ["measured", "failed", "measured"]);

    // The variants either side were measured in full.
    assert_eq!(evidence.variants[0].samples.len(), 3);
    assert_eq!(evidence.variants[0].summary.as_ref().unwrap().count, 3);
    assert_eq!(measured(&evidence.variants[2].samples[0]).rows, 4);

    // The failed variant carries no latency anywhere and no summary.
    let broken = &document["variants"][1];
    assert_eq!(
        broken["warmup"],
        json!({"binding_id": "first", "excluded": true, "completed": false})
    );
    assert_eq!(broken["summary"], Value::Null);
    for sample in broken["samples"].as_array().unwrap() {
        assert_eq!(sample["status"], "failed");
        assert!(sample.get("latency_ns").is_none(), "{sample}");
        let error = sample["error"].as_str().unwrap();
        assert!(!error.is_empty() && error.len() <= 1024 + 3, "{error}");
    }
}

#[test]
fn a_failing_binding_is_excluded_from_its_variant_percentiles() {
    let root = tempfile::tempdir().unwrap();
    let project = durable_project(root.path());
    let mut variant = people_by_min_id();
    // The middle binding omits the parameter the query needs.
    variant["bindings"][1] = json!({"id": "unbound"});
    let evidence = drive(&project, &workload(json!([variant])), &expected(4, 3)).unwrap();
    let variant = &evidence.variants[0];
    assert_eq!(evidence.status, "failed");
    assert_eq!(variant.status, "failed");
    assert!(variant.warmup.completed);
    let outcomes: Vec<_> = variant
        .samples
        .iter()
        .map(|s| (s.binding_id.as_str(), s.measured().is_some()))
        .collect();
    assert_eq!(
        outcomes,
        [("min-1", true), ("unbound", false), ("min-4", true)]
    );
    let mut latencies: Vec<_> = variant
        .samples
        .iter()
        .filter_map(Sample::measured)
        .map(|m| m.latency_ns)
        .collect();
    latencies.sort_unstable();
    let summary = variant.summary.as_ref().unwrap();
    assert_eq!(summary.count, 2);
    assert_eq!(summary.p50_ns, latencies[0]);
    assert_eq!(summary.p95_ns, latencies[1]);
}

#[test]
fn nearest_rank_percentiles() {
    let twenty: Vec<u64> = (1..=20).collect();
    assert_eq!(nearest_rank(&twenty, 50), 10);
    assert_eq!(nearest_rank(&twenty, 95), 19);
    assert_eq!(nearest_rank(&twenty, 100), 20);
    assert_eq!(nearest_rank(&[7], 50), 7);
    assert_eq!(nearest_rank(&[7], 95), 7);
    assert_eq!(nearest_rank(&[1, 2, 3], 50), 2);
    assert_eq!(nearest_rank(&[1, 2, 3], 95), 3);
    assert_eq!(nearest_rank(&[1, 2, 3, 4], 50), 2);
}

#[test]
fn result_digest_separates_nulls_order_and_types_but_not_batch_boundaries() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch = |ids: &[i64], names: &[Option<&str>]| {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef,
                Arc::new(StringArray::from(names.to_vec())) as ArrayRef,
            ],
        )
        .unwrap()
    };
    let digest = |batches: &[RecordBatch]| result_digest(&schema, batches, true).unwrap();
    let whole = digest(&[batch(&[1, 2], &[Some("a"), None])]);
    assert_eq!(
        whole,
        digest(&[batch(&[1], &[Some("a")]), batch(&[2], &[None])])
    );
    assert_ne!(whole, digest(&[batch(&[1, 2], &[Some("a"), Some("")])]));
    assert_ne!(whole, digest(&[batch(&[2, 1], &[None, Some("a")])]));
    assert_ne!(whole, digest(&[batch(&[1, 2], &[Some("a"), Some("N")])]));
    let renamed = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let relabelled = RecordBatch::try_new(
        renamed.clone(),
        batch(&[1, 2], &[Some("a"), None]).columns().to_vec(),
    )
    .unwrap();
    assert_ne!(whole, result_digest(&renamed, &[relabelled], true).unwrap());
}

#[test]
fn unordered_digests_ignore_row_order_and_ordered_digests_do_not() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch = |ids: &[i64], names: &[Option<&str>]| {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef,
                Arc::new(StringArray::from(names.to_vec())) as ArrayRef,
            ],
        )
        .unwrap()
    };
    let forward = [batch(&[1, 2, 3], &[Some("a"), None, Some("c")])];
    let shuffled = [
        batch(&[3], &[Some("c")]),
        batch(&[1, 2], &[Some("a"), None]),
    ];
    let digest =
        |batches: &[RecordBatch], ordered| result_digest(&schema, batches, ordered).unwrap();

    assert_eq!(digest(&forward, false), digest(&shuffled, false));
    assert_ne!(digest(&forward, true), digest(&shuffled, true));
    // Unordered still distinguishes different rows, and the mode is part of the digest.
    let other = [batch(&[1, 2, 3], &[Some("a"), Some(""), Some("c")])];
    assert_ne!(digest(&forward, false), digest(&other, false));
    assert_ne!(digest(&forward, false), digest(&forward, true));
}
