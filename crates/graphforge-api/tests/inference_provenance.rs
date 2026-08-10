//! Inference planning remains observable through `explain()`, but a read-only
//! match does not publish mutation provenance. Persisted inference
//! materialization, when implemented, must emit the knowledge event explicitly.

use arrow::array::{Array, StringArray};
use graphforge_api::GraphForge;
use tempfile::TempDir;

const INFER_ONTOLOGY: &str = "\
ontology_id: infer_test
version: \"v1\"
entity_types:
  - name: P
    abstract: false
relation_types:
  - name: KNOWS
    src: P
    dst: P
    semantic:
      transitive: true
      symmetric: false
properties:
  - owner: P
    name: name
    type: utf8
    nullable: false
constraints: []
migrations: []
";

fn read_parquet(path: &std::path::Path) -> Vec<arrow::record_batch::RecordBatch> {
    let file = std::fs::File::open(path).expect("open parquet");
    parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("reader builder")
        .build()
        .expect("reader")
        .map(|b| b.expect("batch"))
        .collect()
}

/// `(kind, rule_id, confidence_model)` of every persisted provenance event.
fn events(dir: &std::path::Path) -> Vec<(String, Option<String>, Option<String>)> {
    let path = graphforge_storage::resolve_project_generation(dir)
        .expect("resolve committed generation")
        .participants_root()
        .join("provenance/events.parquet");
    if !path.exists() {
        return Vec::new();
    }
    let mut out = Vec::new();
    for b in read_parquet(&path) {
        let s = |name: &str| {
            b.column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .clone()
        };
        let (kind, rule, cm) = (s("kind"), s("rule_id"), s("confidence_model"));
        for i in 0..b.num_rows() {
            out.push((
                kind.value(i).to_owned(),
                (!rule.is_null(i)).then(|| rule.value(i).to_owned()),
                (!cm.is_null(i)).then(|| cm.value(i).to_owned()),
            ));
        }
    }
    out
}

fn inference_count(dir: &std::path::Path) -> usize {
    events(dir)
        .into_iter()
        .filter(|(k, _, _)| k == "inference")
        .count()
}

/// A dir-backed instance with the transitive-KNOWS ontology loaded and an
/// A-KNOWS->B-KNOWS->C chain created.
fn seed(dir: &std::path::Path) -> GraphForge {
    graphforge_storage::open_or_initialize_project(dir).expect("initialize project");
    let onto = dir.join("infer.yaml");
    std::fs::write(&onto, INFER_ONTOLOGY).unwrap();
    let mut gf = GraphForge::new(Some(dir.to_str().unwrap())).expect("dir-backed instance");
    gf.load_ontology(onto.to_str().unwrap())
        .expect("load ontology");
    gf.execute("CREATE (:P {name: 'A'})-[:KNOWS]->(:P {name: 'B'})-[:KNOWS]->(:P {name: 'C'})")
        .expect("create chain");
    gf
}

#[test]
fn transitive_match_does_not_publish_mutation_provenance() {
    let dir = TempDir::new().unwrap();
    let gf = seed(dir.path());
    let before = graphforge_storage::resolve_project_generation(dir.path())
        .unwrap()
        .generation_uuid();

    gf.execute("MATCH (a:P)-[:KNOWS*]->(c:P) RETURN a.name AS an, c.name AS cn")
        .expect("variable-length match");

    let after = graphforge_storage::resolve_project_generation(dir.path())
        .unwrap()
        .generation_uuid();
    assert_eq!(after, before, "read-only inference must not publish");
    assert!(events(dir.path()).is_empty());
}

#[test]
fn explain_surfaces_inference_rule_id() {
    let dir = TempDir::new().unwrap();
    let gf = seed(dir.path());
    let plan = gf
        .explain("MATCH (a:P)-[:KNOWS*]->(c:P) RETURN a.name AS an")
        .expect("explain");
    assert!(
        plan.contains("rule_id=transitive:KNOWS"),
        "explain() should surface the inference rule_id; got:\n{plan}"
    );
}

#[test]
fn explain_is_side_effect_free() {
    let dir = TempDir::new().unwrap();
    let gf = seed(dir.path());
    let before = inference_count(dir.path());
    gf.explain("MATCH (a:P)-[:KNOWS*]->(c:P) RETURN a.name AS an")
        .expect("explain");
    assert_eq!(
        before,
        inference_count(dir.path()),
        "explain() must not write inference provenance"
    );
}

#[test]
fn no_ontology_records_no_inference() {
    // Exploratory (no ontology) → no inference rules → no inference events.
    // This is the TCK-safety property: the no-ontology plan never wraps in
    // OntologyInferNode.
    let dir = TempDir::new().unwrap();
    let gf = GraphForge::new(Some(dir.path().to_str().unwrap())).expect("dir-backed");
    gf.execute("CREATE (:P {name: 'A'})-[:KNOWS]->(:P {name: 'B'})")
        .expect("create");
    gf.execute("MATCH (a:P)-[:KNOWS*]->(b:P) RETURN a.name AS an")
        .expect("match");
    assert_eq!(
        inference_count(dir.path()),
        0,
        "no ontology → no inference provenance"
    );
}
