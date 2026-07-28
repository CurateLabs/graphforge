//! Regression coverage for the v0.5 graph/knowledge ownership boundary (#2410).

use arrow::array::StringArray;
use gf_api::GraphForge;

#[test]
fn confidence_is_an_ordinary_domain_property() {
    let graph = GraphForge::new(None).expect("open graph");
    graph
        .execute(
            "CREATE (:Person {confidence: 'reviewed'})\
             -[:KNOWS {confidence: 'tentative'}]->\
             (:Person {confidence: 'confirmed'})",
        )
        .expect("write ordinary confidence properties");

    let result = graph
        .execute(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) \
             RETURN a.confidence AS source, r.confidence AS edge, b.confidence AS target",
        )
        .expect("read ordinary confidence properties");
    let batch = &result.batches[0];
    for (name, expected) in [
        ("source", "reviewed"),
        ("edge", "tentative"),
        ("target", "confirmed"),
    ] {
        let values = batch
            .column_by_name(name)
            .expect("result column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("ordinary string property");
        assert_eq!(values.value(0), expected);
    }
}

#[test]
fn graph_write_creates_no_graph_embedded_provenance_store() {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let graph = GraphForge::new(dir.path().to_str()).expect("open graph");
    graph
        .execute("CREATE (:Person)-[:KNOWS]->(:Person)")
        .expect("write graph");
    let participants = gf_storage::resolve_project_generation(dir.path())
        .expect("resolve committed generation")
        .participants_root();

    assert!(
        !participants.join("provenance").exists(),
        "graph writes must not create the knowledge-owned provenance directory"
    );

    let names = gf_storage::TYPED_EDGE_SCHEMA
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect::<Vec<_>>();
    assert!(!names.contains(&"confidence"));
    assert!(!names.contains(&"provenance_uuid"));
}
