use crate::*;
use arrow::array::{
    Array, BooleanArray, FixedSizeBinaryArray, FixedSizeListArray, Float32Array, Float64Array,
    Int64Array, ListArray, StringArray, StructArray, UInt64Array,
};
use arrow::datatypes::DataType;
use std::collections::HashSet;

mod analysis_directed;
mod analysis_structural;
mod cluster;
mod communities;
mod descriptors;
mod embedding;
mod paths_flow;
mod paths_shortest;
mod paths_walk;
mod rank;
mod similarity;

fn degree_options(directed: bool, via: Option<&str>) -> RankOptions {
    RankOptions {
        by: RankAlgorithm::Degree,
        via: via.map(str::to_owned),
        directed,
        write_property: None,
    }
}

fn degree_scores(batch: &arrow::record_batch::RecordBatch) -> Vec<f64> {
    batch
        .column_by_name("score")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .values()
        .to_vec()
}

fn components_options(directed: bool, via: Option<&str>) -> ClusterOptions {
    ClusterOptions {
        by: ClusterAlgorithm::Components,
        vector_property: None,
        via: via.map(str::to_owned),
        directed,
        write_property: None,
    }
}

fn community_ids(batch: &arrow::record_batch::RecordBatch) -> Vec<i64> {
    batch
        .column_by_name("community_id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec()
}

fn node_similarity_options(k: usize, via: Option<&str>) -> SimilarOptions {
    SimilarOptions {
        by: SimilarAlgorithm::NodeSimilarity,
        k,
        vector_property: None,
        via: via.map(str::to_owned),
    }
}

fn bfs_options(directed: bool, via: Option<&str>) -> PathsOptions {
    PathsOptions {
        by: PathAlgorithm::Bfs,
        directed,
        k: 1,
        via: via.map(str::to_owned),
        weight: None,
        capacity_property: None,
        cost_property: None,
        heuristic: None,
        walk_length: None,
        seed: None,
        terminal_uuids: Vec::new(),
        prize_property: None,
    }
}

fn uuid_column<'a>(
    batch: &'a arrow::record_batch::RecordBatch,
    name: &str,
) -> &'a FixedSizeBinaryArray {
    batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref()
        .unwrap()
}

fn is_dag_options(directed: bool, via: Option<&str>) -> AnalyzeOptions {
    AnalyzeOptions {
        by: AnalyzeAlgorithm::IsDag,
        via: via.map(str::to_owned),
        directed,
        weight: None,
        k: None,
        partition_property: None,
    }
}

fn relationship_rows(graph: &GraphForge, rel_type: &str) -> Vec<([u8; 16], [u8; 16], [u8; 16])> {
    let result = graph
        .execute(&format!(
            "MATCH (source)-[edge:{rel_type}]->(target) RETURN source, edge, target"
        ))
        .unwrap();
    result
        .batches
        .iter()
        .flat_map(|batch| {
            let uuid_column = |column: &str, field: &str| {
                batch
                    .column_by_name(column)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .unwrap()
                    .column_by_name(field)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .unwrap()
            };
            let edges = uuid_column("edge", "edge_uuid");
            let sources = uuid_column("source", "node_uuid");
            let targets = uuid_column("target", "node_uuid");
            (0..batch.num_rows())
                .map(|row| {
                    (
                        edges.value(row).try_into().unwrap(),
                        sources.value(row).try_into().unwrap(),
                        targets.value(row).try_into().unwrap(),
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

fn uuid_path(batch: &arrow::record_batch::RecordBatch, row: usize) -> Vec<[u8; 16]> {
    let paths = batch
        .column_by_name("path")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let values = paths.value(row);
    let values = values
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    (0..values.len())
        .map(|index| values.value(index).try_into().unwrap())
        .collect()
}

fn add_person(graph: &GraphForge, name: &str) -> NodeHandle {
    graph
        .add_node(
            "Person",
            &HashMap::from([("name".to_owned(), PropValue::Str(name.to_owned()))]),
        )
        .unwrap()
}

fn add_person_with_heuristic_value(
    graph: &GraphForge,
    name: &str,
    heuristic: PropValue,
) -> NodeHandle {
    graph
        .add_node(
            "Person",
            &HashMap::from([
                ("name".to_owned(), PropValue::Str(name.to_owned())),
                ("heuristic".to_owned(), heuristic),
            ]),
        )
        .unwrap()
}
