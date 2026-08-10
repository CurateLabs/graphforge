//! Direct release-conformance evidence for the public search Rust facade.

use std::collections::{BTreeMap, HashMap};

use arrow::array::{FixedSizeBinaryArray, Float64Array, StringArray};
use arrow::datatypes::DataType;
use graphforge_api::{
    CallerEmbeddingBatchRequest, CallerEmbeddingBatchRow, CallerEmbeddingDistance,
    CallerEmbeddingNormalization, FindOptions, GraphForge, NodeHandle, NodeSelector, PropValue,
    SearchIndexOptions,
};
use tempfile::TempDir;

fn add_paper(graph: &GraphForge, title: &str) -> NodeHandle {
    graph
        .add_node(
            "Paper",
            &HashMap::from([("title".into(), PropValue::Str(title.into()))]),
        )
        .unwrap()
}

fn find_options() -> FindOptions {
    FindOptions {
        label: Some("Paper".into()),
        limit: 3,
        ..FindOptions::default()
    }
}

fn publish_search_artifacts(
    graph: &GraphForge,
    alpha: &NodeHandle,
    beta: NodeHandle,
    graph_search: &NodeHandle,
) {
    graph
        .index_search(
            "Paper",
            SearchIndexOptions::Text {
                properties: Some(vec!["title".into()]),
                rebuild: false,
            },
        )
        .unwrap();
    graph
        .publish_caller_embeddings(CallerEmbeddingBatchRequest {
            display_name: "semantic".into(),
            contract_version: "public-surface-v1".into(),
            dimensions: 2,
            normalization: CallerEmbeddingNormalization::None,
            distance: CallerEmbeddingDistance::Cosine,
            source_projection_recipe: BTreeMap::from([("label".into(), "Paper".into())]),
            rows: vec![
                CallerEmbeddingBatchRow {
                    node: NodeSelector::Handle(alpha.clone()),
                    vector: vec![1.0, 0.0],
                },
                CallerEmbeddingBatchRow {
                    node: NodeSelector::Handle(beta),
                    vector: vec![0.0, 1.0],
                },
                CallerEmbeddingBatchRow {
                    node: NodeSelector::Handle(graph_search.clone()),
                    vector: vec![1.0, 1.0],
                },
            ],
            replace_alias: false,
        })
        .unwrap();
}

#[test]
fn public_text_vector_hybrid_find_is_exact_repeatable_and_persistent() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().to_str().unwrap();
    let graph = GraphForge::new(Some(path)).unwrap();
    let alpha = add_paper(&graph, "alpha systems");
    let beta = add_paper(&graph, "beta systems");
    let graph_search = add_paper(&graph, "graph search");
    publish_search_artifacts(&graph, &alpha, beta, &graph_search);

    let hybrid_options = FindOptions {
        query: Some("graph".into()),
        vector: Some(vec![1.0, 0.0]),
        space: Some("semantic".into()),
        ..find_options()
    };
    let first = graph.find(hybrid_options.clone()).unwrap();
    assert_eq!(graph.find(hybrid_options.clone()).unwrap(), first);
    assert_eq!(
        first
            .schema()
            .fields()
            .iter()
            .map(|f| (f.name().as_str(), f.data_type()))
            .collect::<Vec<_>>(),
        [
            ("node_uuid", &DataType::FixedSizeBinary(16)),
            ("title", &DataType::Utf8),
            ("score", &DataType::Float64),
            ("matched_on", &DataType::Utf8),
        ]
    );
    let ids = first
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(ids.value(0), graph_search.uuid.as_bytes());
    let channels = first
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(channels.value(0), "text+vector");
    let scores = first
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!(scores.value(0).is_finite());

    let text = graph
        .find(FindOptions {
            query: Some("alpha".into()),
            ..find_options()
        })
        .unwrap();
    assert_eq!(
        text.column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(0),
        alpha.uuid.as_bytes()
    );
    let by_node = graph
        .find(FindOptions {
            similar_to: Some(NodeSelector::Handle(alpha)),
            space: Some("semantic".into()),
            ..find_options()
        })
        .unwrap();
    assert_eq!(
        by_node
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "vector"
    );

    drop(graph);
    let reopened = GraphForge::new(Some(path)).unwrap();
    assert_eq!(reopened.find(hybrid_options).unwrap(), first);
}

#[test]
fn public_find_option_and_legacy_index_errors_are_exact_and_repeatable() {
    let graph = GraphForge::new(None).unwrap();
    let find_error = || graph.find(FindOptions::default()).unwrap_err().to_string();
    assert_eq!(find_error(), "validation error: find requires label");
    assert_eq!(find_error(), find_error());
    let index_error = graph.index("Paper").unwrap_err();
    assert_eq!(index_error.to_string(), "not implemented: index");
}
