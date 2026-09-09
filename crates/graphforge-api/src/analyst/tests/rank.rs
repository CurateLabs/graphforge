use super::*;

fn betweenness_options(
    directed: bool,
    via: Option<&str>,
    write_property: Option<&str>,
) -> RankOptions {
    RankOptions {
        by: RankAlgorithm::Betweenness,
        via: via.map(str::to_owned),
        directed,
        write_property: write_property.map(str::to_owned),
    }
}

fn closeness_options(
    directed: bool,
    via: Option<&str>,
    write_property: Option<&str>,
) -> RankOptions {
    RankOptions {
        by: RankAlgorithm::Closeness,
        via: via.map(str::to_owned),
        directed,
        write_property: write_property.map(str::to_owned),
    }
}

fn harmonic_closeness_options(
    directed: bool,
    via: Option<&str>,
    write_property: Option<&str>,
) -> RankOptions {
    RankOptions {
        by: RankAlgorithm::HarmonicCloseness,
        via: via.map(str::to_owned),
        directed,
        write_property: write_property.map(str::to_owned),
    }
}

fn eigenvector_options(
    directed: bool,
    via: Option<&str>,
    write_property: Option<&str>,
) -> RankOptions {
    RankOptions {
        by: RankAlgorithm::Eigenvector,
        via: via.map(str::to_owned),
        directed,
        write_property: write_property.map(str::to_owned),
    }
}

fn article_rank_options(
    directed: bool,
    via: Option<&str>,
    write_property: Option<&str>,
) -> RankOptions {
    RankOptions {
        by: RankAlgorithm::ArticleRank,
        via: via.map(str::to_owned),
        directed,
        write_property: write_property.map(str::to_owned),
    }
}

fn hits_hub_options(
    directed: bool,
    via: Option<&str>,
    write_property: Option<&str>,
) -> RankOptions {
    RankOptions {
        by: RankAlgorithm::HitsHub,
        via: via.map(str::to_owned),
        directed,
        write_property: write_property.map(str::to_owned),
    }
}

fn hits_authority_options(
    directed: bool,
    via: Option<&str>,
    write_property: Option<&str>,
) -> RankOptions {
    RankOptions {
        by: RankAlgorithm::HitsAuthority,
        via: via.map(str::to_owned),
        directed,
        write_property: write_property.map(str::to_owned),
    }
}

fn celf_options(directed: bool, via: Option<&str>, write_property: Option<&str>) -> RankOptions {
    RankOptions {
        by: RankAlgorithm::Celf,
        via: via.map(str::to_owned),
        directed,
        write_property: write_property.map(str::to_owned),
    }
}

fn clustering_coefficient_options(
    directed: bool,
    via: Option<&str>,
    write_property: Option<&str>,
) -> RankOptions {
    RankOptions {
        by: RankAlgorithm::ClusteringCoefficient,
        via: via.map(str::to_owned),
        directed,
        write_property: write_property.map(str::to_owned),
    }
}

fn triangles_options(
    directed: bool,
    via: Option<&str>,
    write_property: Option<&str>,
) -> RankOptions {
    RankOptions {
        by: RankAlgorithm::Triangles,
        via: via.map(str::to_owned),
        directed,
        write_property: write_property.map(str::to_owned),
    }
}

fn k_core_options(directed: bool, via: Option<&str>, write_property: Option<&str>) -> RankOptions {
    RankOptions {
        by: RankAlgorithm::KCore,
        via: via.map(str::to_owned),
        directed,
        write_property: write_property.map(str::to_owned),
    }
}

fn preferential_attachment_options(
    directed: bool,
    via: Option<&str>,
    write_property: Option<&str>,
) -> RankOptions {
    RankOptions {
        by: RankAlgorithm::PreferentialAttachment,
        via: via.map(str::to_owned),
        directed,
        write_property: write_property.map(str::to_owned),
    }
}

fn adamic_adar_options(
    directed: bool,
    via: Option<&str>,
    write_property: Option<&str>,
) -> RankOptions {
    RankOptions {
        by: RankAlgorithm::AdamicAdar,
        via: via.map(str::to_owned),
        directed,
        write_property: write_property.map(str::to_owned),
    }
}

fn common_neighbors_options(
    directed: bool,
    via: Option<&str>,
    write_property: Option<&str>,
) -> RankOptions {
    RankOptions {
        by: RankAlgorithm::CommonNeighbors,
        via: via.map(str::to_owned),
        directed,
        write_property: write_property.map(str::to_owned),
    }
}

fn resource_allocation_options(
    directed: bool,
    via: Option<&str>,
    write_property: Option<&str>,
) -> RankOptions {
    RankOptions {
        by: RankAlgorithm::ResourceAllocation,
        via: via.map(str::to_owned),
        directed,
        write_property: write_property.map(str::to_owned),
    }
}

fn total_neighbors_options(
    directed: bool,
    via: Option<&str>,
    write_property: Option<&str>,
) -> RankOptions {
    RankOptions {
        by: RankAlgorithm::TotalNeighbors,
        via: via.map(str::to_owned),
        directed,
        write_property: write_property.map(str::to_owned),
    }
}

fn assert_rank_scores_close(batch: &arrow::record_batch::RecordBatch, expected: &[f64]) {
    let actual = degree_scores(batch);
    assert_eq!(actual.len(), expected.len());
    assert!(
        actual
            .iter()
            .zip(expected)
            .all(|(actual, expected)| (actual - expected).abs() <= 1.0e-12)
    );
}

#[test]
fn degree_obeys_uuid_schema_direction_via_and_multigraph_contracts() {
    let graph = GraphForge::new(None).unwrap();
    graph
        .execute(
            "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person), (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), \
                 (a)-[:KNOWS]->(a), (a)-[:OTHER]->(c)",
        )
        .unwrap();

    let directed = graph
        .rank("Person", degree_options(true, Some("KNOWS")))
        .unwrap();
    assert_eq!(degree_scores(&directed), [1.5, 0.0, 0.0]);
    assert_eq!(
        directed
            .schema()
            .field_with_name("node_uuid")
            .unwrap()
            .data_type(),
        &DataType::FixedSizeBinary(16)
    );
    assert!(directed.column_by_name("node_id").is_none());
    assert_eq!(
        directed
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        [Some("Alice"), Some("Bob"), None]
    );
    let uuids = directed
        .column_by_name("node_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(uuids.value_length(), 16);
    assert_eq!(uuids.null_count(), 0);
    assert_eq!(
        directed,
        graph
            .rank("Person", degree_options(true, Some("KNOWS")))
            .unwrap()
    );
    assert_eq!(
        degree_scores(
            &graph
                .rank("Person", degree_options(false, Some("KNOWS")))
                .unwrap()
        ),
        [2.0, 1.0, 0.0]
    );
    assert_eq!(
        degree_scores(&graph.rank("Person", degree_options(true, None)).unwrap()),
        [2.0, 0.0, 0.0]
    );
}

#[test]
fn degree_empty_and_invalid_inputs_are_structured() {
    let graph = GraphForge::new(None).unwrap();
    let empty = graph.rank("Person", degree_options(true, None)).unwrap();
    assert_eq!(empty.num_rows(), 0);
    assert_eq!(
        empty.schema().field_with_name("score").unwrap().data_type(),
        &DataType::Float64
    );
    for result in [
        graph.rank("", degree_options(true, None)),
        graph.rank("Person", degree_options(true, Some(" "))),
    ] {
        assert!(matches!(result, Err(GfError::Validation(_))));
    }
}

#[test]
fn degree_public_writeback_is_opt_in_exact_and_persistent() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap();
    let graph = GraphForge::new(Some(path)).unwrap();
    graph
        .execute(
            "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (a)-[:KNOWS]->(b)",
        )
        .unwrap();

    let expected = graph
        .rank("Person", degree_options(false, Some("KNOWS")))
        .unwrap();
    assert_eq!(
        graph
            .execute(
                "MATCH (n:Person) WHERE n.degree_score IS NOT NULL \
                     RETURN n.degree_score"
            )
            .unwrap()
            .stats
            .rows_produced,
        0
    );

    let mut options = degree_options(false, Some("KNOWS"));
    options.write_property = Some("degree_score".into());
    assert_eq!(graph.rank("Person", options).unwrap(), expected);
    let immediate = graph
        .execute(
            "MATCH (n:Person) RETURN n.name AS name, \
                 n.degree_score AS degree_score ORDER BY name",
        )
        .unwrap();
    assert_eq!(
        immediate.batches[0]
            .column_by_name("degree_score")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values(),
        &[0.5, 0.5, 0.0]
    );
    drop(graph);

    let reopened = GraphForge::new(Some(path)).unwrap();
    let persisted = reopened
        .execute("MATCH (n:Person) RETURN n.degree_score AS degree_score ORDER BY n.name")
        .unwrap();
    assert_eq!(
        persisted.batches[0]
            .column_by_name("degree_score")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values(),
        &[0.5, 0.5, 0.0]
    );
}

#[test]
fn betweenness_obeys_public_topology_arrow_and_writeback_contracts() {
    let graph = GraphForge::new(None).unwrap();
    graph
        .execute(
            "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), \
                 (a)-[:KNOWS]->(d), (d)-[:KNOWS]->(c), (b)-[:KNOWS]->(b), \
                 (a)-[:OTHER]->(c)",
        )
        .unwrap();

    let options = betweenness_options(true, Some("KNOWS"), None);
    let directed = graph.rank("Person", options.clone()).unwrap();
    assert_eq!(degree_scores(&directed), [0.0, 1.0 / 9.0, 0.0, 1.0 / 18.0]);
    assert_eq!(directed, graph.rank("Person", options).unwrap());
    assert_eq!(
        directed.schema().metadata()["graphforge.algorithm"],
        "betweenness"
    );
    assert_eq!(
        directed
            .schema()
            .field_with_name("node_uuid")
            .unwrap()
            .data_type(),
        &DataType::FixedSizeBinary(16)
    );
    assert_eq!(
        directed
            .schema()
            .field_with_name("score")
            .unwrap()
            .data_type(),
        &DataType::Float64
    );
    assert_eq!(
        directed
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        ["node_uuid", "score", "name"]
    );
    assert_eq!(
        directed
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        [Some("Alice"), Some("Bob"), Some("Carol"), Some("Dan")]
    );
    assert_ne!(
        degree_scores(&directed),
        degree_scores(
            &graph
                .rank("Person", betweenness_options(false, Some("KNOWS"), None))
                .unwrap()
        )
    );
    assert_ne!(
        degree_scores(&directed),
        degree_scores(
            &graph
                .rank("Person", betweenness_options(true, None, None))
                .unwrap()
        )
    );
    assert_eq!(
        graph
            .execute("MATCH (n:Person) WHERE n.between IS NOT NULL RETURN n.node_uuid")
            .unwrap()
            .batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>(),
        0
    );
    graph
        .rank(
            "Person",
            betweenness_options(true, Some("KNOWS"), Some("between")),
        )
        .unwrap();
    assert_eq!(
        graph
            .execute("MATCH (n:Person) WHERE n.between IS NOT NULL RETURN n.node_uuid")
            .unwrap()
            .batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>(),
        4
    );

    let disconnected = GraphForge::new(None).unwrap();
    disconnected
        .execute("CREATE (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person), (d:Person)")
        .unwrap();
    assert_eq!(
        degree_scores(
            &disconnected
                .rank("Person", betweenness_options(true, None, None))
                .unwrap()
        ),
        [0.0, 1.0 / 6.0, 0.0, 0.0]
    );
    assert_eq!(
        GraphForge::new(None)
            .unwrap()
            .rank("Person", betweenness_options(true, None, None))
            .unwrap()
            .num_rows(),
        0
    );
}

#[test]
fn closeness_obeys_public_topology_arrow_and_writeback_contracts() {
    let graph = GraphForge::new(None).unwrap();
    graph
        .execute(
            "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), \
                 (b)-[:KNOWS]->(b), (a)-[:OTHER]->(c)",
        )
        .unwrap();

    let options = closeness_options(true, Some("KNOWS"), None);
    let directed = graph.rank("Person", options.clone()).unwrap();
    assert_eq!(degree_scores(&directed), [4.0 / 9.0, 1.0 / 3.0, 0.0, 0.0]);
    assert_eq!(directed, graph.rank("Person", options).unwrap());
    assert_eq!(
        directed.schema().metadata()["graphforge.algorithm"],
        "closeness"
    );
    assert_eq!(
        directed
            .schema()
            .field_with_name("node_uuid")
            .unwrap()
            .data_type(),
        &DataType::FixedSizeBinary(16)
    );
    assert_eq!(
        directed
            .schema()
            .field_with_name("score")
            .unwrap()
            .data_type(),
        &DataType::Float64
    );
    assert!(directed.column_by_name("node_id").is_none());
    assert_eq!(
        directed
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        [Some("Alice"), Some("Bob"), Some("Carol"), Some("Dan")]
    );
    assert_eq!(
        degree_scores(
            &graph
                .rank("Person", closeness_options(false, Some("KNOWS"), None))
                .unwrap()
        ),
        [4.0 / 9.0, 2.0 / 3.0, 4.0 / 9.0, 0.0]
    );
    assert_eq!(
        degree_scores(
            &graph
                .rank("Person", closeness_options(true, None, None))
                .unwrap()
        ),
        [2.0 / 3.0, 1.0 / 3.0, 0.0, 0.0]
    );
    assert_eq!(
        graph
            .execute("MATCH (n:Person) WHERE n.close_score IS NOT NULL RETURN n.node_uuid")
            .unwrap()
            .batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>(),
        0
    );
    graph
        .rank(
            "Person",
            closeness_options(true, Some("KNOWS"), Some("close_score")),
        )
        .unwrap();
    assert_eq!(
        graph
            .execute("MATCH (n:Person) WHERE n.close_score IS NOT NULL RETURN n.node_uuid")
            .unwrap()
            .batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>(),
        4
    );
    assert_eq!(
        GraphForge::new(None)
            .unwrap()
            .rank("Person", closeness_options(true, None, None))
            .unwrap()
            .num_rows(),
        0
    );
}

#[test]
fn harmonic_closeness_obeys_public_topology_arrow_and_writeback_contracts() {
    let graph = GraphForge::new(None).unwrap();
    graph
        .execute(
            "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), \
                 (b)-[:KNOWS]->(b), (a)-[:OTHER]->(c)",
        )
        .unwrap();

    let options = harmonic_closeness_options(true, Some("KNOWS"), None);
    let directed = graph.rank("Person", options.clone()).unwrap();
    assert_eq!(degree_scores(&directed), [0.5, 1.0 / 3.0, 0.0, 0.0]);
    assert_eq!(directed, graph.rank("Person", options).unwrap());
    assert_eq!(
        directed.schema().metadata()["graphforge.algorithm"],
        "harmonic_closeness"
    );
    assert_eq!(
        directed
            .schema()
            .field_with_name("node_uuid")
            .unwrap()
            .data_type(),
        &DataType::FixedSizeBinary(16)
    );
    assert_eq!(
        directed
            .schema()
            .field_with_name("score")
            .unwrap()
            .data_type(),
        &DataType::Float64
    );
    assert!(directed.column_by_name("node_id").is_none());
    assert_eq!(
        directed
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        [Some("Alice"), Some("Bob"), Some("Carol"), Some("Dan")]
    );
    assert_eq!(
        degree_scores(
            &graph
                .rank(
                    "Person",
                    harmonic_closeness_options(false, Some("KNOWS"), None),
                )
                .unwrap()
        ),
        [0.5, 2.0 / 3.0, 0.5, 0.0]
    );
    assert_eq!(
        degree_scores(
            &graph
                .rank("Person", harmonic_closeness_options(true, None, None),)
                .unwrap()
        ),
        [2.0 / 3.0, 1.0 / 3.0, 0.0, 0.0]
    );
    assert_eq!(
        graph
            .execute("MATCH (n:Person) WHERE n.harmonic IS NOT NULL RETURN n.node_uuid")
            .unwrap()
            .batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>(),
        0
    );
    graph
        .rank(
            "Person",
            harmonic_closeness_options(true, Some("KNOWS"), Some("harmonic")),
        )
        .unwrap();
    assert_eq!(
        graph
            .execute("MATCH (n:Person) WHERE n.harmonic IS NOT NULL RETURN n.node_uuid")
            .unwrap()
            .batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>(),
        4
    );
    assert_eq!(
        GraphForge::new(None)
            .unwrap()
            .rank("Person", harmonic_closeness_options(true, None, None),)
            .unwrap()
            .num_rows(),
        0
    );
}

#[test]
fn eigenvector_obeys_public_topology_arrow_and_writeback_contracts() {
    let graph = GraphForge::new(None).unwrap();
    graph
        .execute(
            "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(b), \
                 (a)-[:OTHER]->(c)",
        )
        .unwrap();

    let options = eigenvector_options(true, Some("KNOWS"), None);
    let directed = graph.rank("Person", options.clone()).unwrap();
    let ratio = 3.0 * 2.0_f64.powi(20) - 2.0;
    let denominator = (ratio * ratio + 3.0).sqrt();
    let expected = [
        1.0 / denominator,
        ratio / denominator,
        1.0 / denominator,
        1.0 / denominator,
    ];
    assert!(
        degree_scores(&directed)
            .iter()
            .zip(expected)
            .all(|(actual, expected)| (actual - expected).abs() <= 1.0e-15)
    );
    assert_eq!(directed, graph.rank("Person", options).unwrap());
    assert_eq!(
        directed.schema().metadata()["graphforge.algorithm"],
        "eigenvector"
    );
    assert_eq!(
        directed
            .schema()
            .field_with_name("node_uuid")
            .unwrap()
            .data_type(),
        &DataType::FixedSizeBinary(16)
    );
    assert_eq!(
        directed
            .schema()
            .field_with_name("score")
            .unwrap()
            .data_type(),
        &DataType::Float64
    );
    assert!(directed.column_by_name("node_id").is_none());
    assert_eq!(
        directed
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        [Some("Alice"), Some("Bob"), Some("Carol"), Some("Dan")]
    );

    let undirected = degree_scores(
        &graph
            .rank("Person", eigenvector_options(false, Some("KNOWS"), None))
            .unwrap(),
    );
    let phi = (1.0 + 5.0_f64.sqrt()) / 2.0;
    let norm = (1.0 + phi * phi).sqrt();
    assert!((undirected[0] - 1.0 / norm).abs() <= 1.0e-7);
    assert!((undirected[1] - phi / norm).abs() <= 1.0e-7);
    assert!(undirected[2] <= 1.0e-7 && undirected[3] <= 1.0e-7);
    let all_edges = degree_scores(
        &graph
            .rank("Person", eigenvector_options(true, None, None))
            .unwrap(),
    );
    assert!(all_edges[2] > all_edges[0]);

    assert_eq!(
        graph
            .execute("MATCH (n:Person) WHERE n.eigen_score IS NOT NULL RETURN n.node_uuid")
            .unwrap()
            .batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>(),
        0
    );
    graph
        .rank(
            "Person",
            eigenvector_options(true, Some("KNOWS"), Some("eigen_score")),
        )
        .unwrap();
    assert_eq!(
        graph
            .execute("MATCH (n:Person) WHERE n.eigen_score IS NOT NULL RETURN n.node_uuid")
            .unwrap()
            .batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>(),
        4
    );

    let edgeless = GraphForge::new(None).unwrap();
    edgeless
        .execute("CREATE (:Person), (:Person), (:Person)")
        .unwrap();
    assert!(
        degree_scores(
            &edgeless
                .rank("Person", eigenvector_options(true, None, None))
                .unwrap()
        )
        .iter()
        .all(|score| (score - 1.0 / 3.0_f64.sqrt()).abs() <= 1.0e-15)
    );
    assert_eq!(
        GraphForge::new(None)
            .unwrap()
            .rank("Person", eigenvector_options(true, None, None))
            .unwrap()
            .num_rows(),
        0
    );
}

#[test]
fn article_rank_obeys_public_topology_arrow_and_writeback_contracts() {
    let graph = GraphForge::new(None).unwrap();
    graph
        .execute(
            "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (a)-[:KNOWS]->(b), (a)-[:OTHER]->(c), \
                 (a)-[:OTHER]->(c), (c)-[:OTHER]->(c)",
        )
        .unwrap();

    let options = article_rank_options(true, Some("KNOWS"), None);
    let directed = graph.rank("Person", options.clone()).unwrap();
    assert!(
        degree_scores(&directed)
            .iter()
            .zip([0.15, 0.252, 0.15, 0.15])
            .all(|(actual, expected)| (actual - expected).abs() <= 1.0e-15)
    );
    assert_eq!(directed, graph.rank("Person", options).unwrap());
    assert_eq!(
        directed.schema().metadata()["graphforge.algorithm"],
        "article_rank"
    );
    assert_eq!(
        directed
            .schema()
            .field_with_name("node_uuid")
            .unwrap()
            .data_type(),
        &DataType::FixedSizeBinary(16)
    );
    assert_eq!(
        directed
            .schema()
            .field_with_name("score")
            .unwrap()
            .data_type(),
        &DataType::Float64
    );
    assert!(directed.column_by_name("node_id").is_none());
    assert_eq!(
        directed
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        [Some("Alice"), Some("Bob"), Some("Carol"), Some("Dan")]
    );

    let undirected = degree_scores(
        &graph
            .rank("Person", article_rank_options(false, Some("KNOWS"), None))
            .unwrap(),
    );
    assert_ne!(undirected, degree_scores(&directed));
    let all_edges = degree_scores(
        &graph
            .rank("Person", article_rank_options(true, None, None))
            .unwrap(),
    );
    assert!(all_edges[2] > all_edges[1]);

    assert_eq!(
        graph
            .execute("MATCH (n:Person) WHERE n.article_score IS NOT NULL RETURN n.node_uuid")
            .unwrap()
            .batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>(),
        0
    );
    graph
        .rank(
            "Person",
            article_rank_options(true, Some("KNOWS"), Some("article_score")),
        )
        .unwrap();
    assert_eq!(
        graph
            .execute("MATCH (n:Person) WHERE n.article_score IS NOT NULL RETURN n.node_uuid")
            .unwrap()
            .batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>(),
        4
    );

    let edgeless = GraphForge::new(None).unwrap();
    edgeless.execute("CREATE (:Person), (:Person)").unwrap();
    assert!(
        degree_scores(
            &edgeless
                .rank("Person", article_rank_options(true, None, None))
                .unwrap(),
        )
        .iter()
        .all(|score| (score - 0.15).abs() <= 1.0e-15)
    );
    assert_eq!(
        GraphForge::new(None)
            .unwrap()
            .rank("Person", article_rank_options(true, None, None))
            .unwrap()
            .num_rows(),
        0
    );
}

#[test]
fn hits_hub_obeys_public_topology_arrow_and_writeback_contracts() {
    let graph = GraphForge::new(None).unwrap();
    graph
        .execute(
            "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), \
                 (a)-[:OTHER]->(c), (a)-[:OTHER]->(c), (c)-[:OTHER]->(c)",
        )
        .unwrap();

    let options = hits_hub_options(true, Some("KNOWS"), None);
    let directed = graph.rank("Person", options.clone()).unwrap();
    let expected = [1.0 / 2.0_f64.sqrt(), 1.0 / 2.0_f64.sqrt(), 0.0, 0.0];
    assert!(
        degree_scores(&directed)
            .iter()
            .zip(expected)
            .all(|(actual, expected)| (actual - expected).abs() <= 1.0e-15)
    );
    assert_eq!(directed, graph.rank("Person", options).unwrap());
    assert_eq!(
        directed.schema().metadata()["graphforge.algorithm"],
        "hits_hub"
    );
    assert_eq!(
        directed
            .schema()
            .field_with_name("node_uuid")
            .unwrap()
            .data_type(),
        &DataType::FixedSizeBinary(16)
    );
    assert_eq!(
        directed
            .schema()
            .field_with_name("score")
            .unwrap()
            .data_type(),
        &DataType::Float64
    );
    assert!(directed.column_by_name("node_id").is_none());
    assert_eq!(
        directed
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        [Some("Alice"), Some("Bob"), Some("Carol"), Some("Dan")]
    );

    let undirected = degree_scores(
        &graph
            .rank("Person", hits_hub_options(false, Some("KNOWS"), None))
            .unwrap(),
    );
    assert!(
        undirected[..3]
            .iter()
            .all(|score| (score - 1.0 / 3.0_f64.sqrt()).abs() <= 1.0e-12)
    );
    assert_eq!(undirected[3], 0.0);
    let all_edges = degree_scores(
        &graph
            .rank("Person", hits_hub_options(true, None, None))
            .unwrap(),
    );
    assert!(all_edges[2] > 0.0);

    assert_eq!(
        graph
            .execute("MATCH (n:Person) WHERE n.hub_score IS NOT NULL RETURN n.node_uuid")
            .unwrap()
            .batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>(),
        0
    );
    graph
        .rank(
            "Person",
            hits_hub_options(true, Some("KNOWS"), Some("hub_score")),
        )
        .unwrap();
    assert_eq!(
        graph
            .execute("MATCH (n:Person) WHERE n.hub_score IS NOT NULL RETURN n.node_uuid")
            .unwrap()
            .batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>(),
        4
    );

    let edgeless = GraphForge::new(None).unwrap();
    edgeless.execute("CREATE (:Person), (:Person)").unwrap();
    assert_eq!(
        degree_scores(
            &edgeless
                .rank("Person", hits_hub_options(true, None, None))
                .unwrap()
        ),
        [0.0, 0.0]
    );
    assert_eq!(
        GraphForge::new(None)
            .unwrap()
            .rank("Person", hits_hub_options(true, None, None))
            .unwrap()
            .num_rows(),
        0
    );
}

#[test]
fn hits_authority_obeys_public_topology_arrow_and_writeback_contracts() {
    let graph = GraphForge::new(None).unwrap();
    graph
        .execute(
            "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), \
                 (a)-[:OTHER]->(c), (a)-[:OTHER]->(c), (c)-[:OTHER]->(c)",
        )
        .unwrap();

    let options = hits_authority_options(true, Some("KNOWS"), None);
    let directed = graph.rank("Person", options.clone()).unwrap();
    let expected = [0.0, 1.0 / 2.0_f64.sqrt(), 1.0 / 2.0_f64.sqrt(), 0.0];
    assert!(
        degree_scores(&directed)
            .iter()
            .zip(expected)
            .all(|(actual, expected)| (actual - expected).abs() <= 1.0e-15)
    );
    assert_eq!(directed, graph.rank("Person", options).unwrap());
    assert_eq!(
        directed.schema().metadata()["graphforge.algorithm"],
        "hits_authority"
    );
    assert_eq!(
        directed
            .schema()
            .field_with_name("node_uuid")
            .unwrap()
            .data_type(),
        &DataType::FixedSizeBinary(16)
    );
    assert_eq!(
        directed
            .schema()
            .field_with_name("score")
            .unwrap()
            .data_type(),
        &DataType::Float64
    );
    assert!(directed.column_by_name("node_id").is_none());
    assert_eq!(
        directed
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        [Some("Alice"), Some("Bob"), Some("Carol"), Some("Dan")]
    );

    let undirected = degree_scores(
        &graph
            .rank("Person", hits_authority_options(false, Some("KNOWS"), None))
            .unwrap(),
    );
    let root_six = 6.0_f64.sqrt();
    assert!(
        undirected
            .iter()
            .zip([1.0 / root_six, 2.0 / root_six, 1.0 / root_six, 0.0])
            .all(|(actual, expected)| (actual - expected).abs() <= 1.0e-12)
    );
    assert_ne!(
        degree_scores(
            &graph
                .rank("Person", hits_authority_options(true, None, None))
                .unwrap()
        ),
        degree_scores(&directed)
    );

    assert_eq!(
        graph
            .execute("MATCH (n:Person) WHERE n.authority_score IS NOT NULL RETURN n.node_uuid")
            .unwrap()
            .batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>(),
        0
    );
    graph
        .rank(
            "Person",
            hits_authority_options(true, Some("KNOWS"), Some("authority_score")),
        )
        .unwrap();
    assert_eq!(
        graph
            .execute("MATCH (n:Person) WHERE n.authority_score IS NOT NULL RETURN n.node_uuid")
            .unwrap()
            .batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>(),
        4
    );

    let edgeless = GraphForge::new(None).unwrap();
    edgeless.execute("CREATE (:Person), (:Person)").unwrap();
    assert_eq!(
        degree_scores(
            &edgeless
                .rank("Person", hits_authority_options(true, None, None))
                .unwrap()
        ),
        [0.0, 0.0]
    );
    assert_eq!(
        GraphForge::new(None)
            .unwrap()
            .rank("Person", hits_authority_options(true, None, None))
            .unwrap()
            .num_rows(),
        0
    );
}

#[test]
fn celf_obeys_public_topology_arrow_and_writeback_contracts() {
    let graph = GraphForge::new(None).unwrap();
    graph
        .execute(
            "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), \
                 (a)-[:OTHER]->(c), (a)-[:OTHER]->(c), (c)-[:OTHER]->(c)",
        )
        .unwrap();

    let options = celf_options(true, Some("KNOWS"), None);
    let directed = graph.rank("Person", options.clone()).unwrap();
    let assert_scores = |actual: &[f64]| {
        assert!(
            actual
                .iter()
                .all(|score| score.is_finite() && *score >= 0.0)
        );
        assert!((actual.iter().sum::<f64>() - 4.0).abs() <= 1.0e-12);
        assert!((actual[3] - 1.0).abs() <= 1.0e-12);
    };
    let directed_scores = degree_scores(&directed);
    assert_scores(&directed_scores);
    assert_eq!(directed, graph.rank("Person", options).unwrap());
    assert_eq!(directed.schema().metadata()["graphforge.algorithm"], "celf");
    assert_eq!(
        directed
            .schema()
            .field_with_name("node_uuid")
            .unwrap()
            .data_type(),
        &DataType::FixedSizeBinary(16)
    );
    assert_eq!(
        directed
            .schema()
            .field_with_name("score")
            .unwrap()
            .data_type(),
        &DataType::Float64
    );
    assert!(directed.column_by_name("node_id").is_none());
    assert_eq!(
        directed
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        [Some("Alice"), Some("Bob"), Some("Carol"), Some("Dan")]
    );
    let undirected_scores = degree_scores(
        &graph
            .rank("Person", celf_options(false, Some("KNOWS"), None))
            .unwrap(),
    );
    assert_scores(&undirected_scores);
    assert_ne!(undirected_scores, directed_scores);
    let all_scores = degree_scores(
        &graph
            .rank("Person", celf_options(true, None, None))
            .unwrap(),
    );
    assert_scores(&all_scores);
    assert_ne!(all_scores, directed_scores);

    assert_eq!(
        graph
            .execute("MATCH (n:Person) WHERE n.celf_score IS NOT NULL RETURN n.node_uuid")
            .unwrap()
            .batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>(),
        0
    );
    graph
        .rank(
            "Person",
            celf_options(true, Some("KNOWS"), Some("celf_score")),
        )
        .unwrap();
    assert_eq!(
        graph
            .execute("MATCH (n:Person) WHERE n.celf_score IS NOT NULL RETURN n.node_uuid")
            .unwrap()
            .batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>(),
        4
    );

    let edgeless = GraphForge::new(None).unwrap();
    edgeless.execute("CREATE (:Person), (:Person)").unwrap();
    assert_eq!(
        degree_scores(
            &edgeless
                .rank("Person", celf_options(true, None, None))
                .unwrap()
        ),
        [1.0, 1.0]
    );
    assert_eq!(
        GraphForge::new(None)
            .unwrap()
            .rank("Person", celf_options(true, None, None))
            .unwrap()
            .num_rows(),
        0
    );
}

#[test]
fn clustering_coefficient_obeys_public_alias_topology_and_writeback_contracts() {
    let graph = GraphForge::new(None).unwrap();
    graph
        .execute(
            "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (e:Person {name:'Eve'}), (a)-[:KNOWS]->(b), \
                 (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), (c)-[:KNOWS]->(a), \
                 (c)-[:KNOWS]->(c), (d)-[:KNOWS]->(e), (a)-[:OTHER]->(d)",
        )
        .unwrap();

    let options = clustering_coefficient_options(true, Some("KNOWS"), None);
    let directed = graph.rank("Person", options.clone()).unwrap();
    assert_eq!(degree_scores(&directed), [0.5, 0.5, 0.5, 0.0, 0.0]);
    assert_eq!(directed, graph.rank("Person", options).unwrap());
    assert_eq!(
        directed.schema().metadata()["graphforge.algorithm"],
        "clustering_coefficient"
    );
    assert_eq!(
        directed
            .schema()
            .field_with_name("node_uuid")
            .unwrap()
            .data_type(),
        &DataType::FixedSizeBinary(16)
    );
    assert_eq!(
        directed
            .schema()
            .field_with_name("score")
            .unwrap()
            .data_type(),
        &DataType::Float64
    );
    assert!(directed.column_by_name("node_id").is_none());
    assert_eq!(
        directed
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        [
            Some("Alice"),
            Some("Bob"),
            Some("Carol"),
            Some("Dan"),
            Some("Eve")
        ]
    );

    let alias: RankAlgorithm = "local_clustering_coefficient".parse().unwrap();
    assert_eq!(alias, RankAlgorithm::ClusteringCoefficient);
    assert_eq!(
        directed,
        graph
            .rank(
                "Person",
                RankOptions {
                    by: alias,
                    via: Some("KNOWS".into()),
                    ..RankOptions::default()
                },
            )
            .unwrap()
    );
    assert_eq!(
        degree_scores(
            &graph
                .rank(
                    "Person",
                    clustering_coefficient_options(false, Some("KNOWS"), None),
                )
                .unwrap(),
        ),
        [1.0, 1.0, 1.0, 0.0, 0.0],
    );
    assert_ne!(
        degree_scores(&directed),
        degree_scores(
            &graph
                .rank("Person", clustering_coefficient_options(true, None, None),)
                .unwrap()
        )
    );

    assert_eq!(
        graph
            .execute("MATCH (n:Person) WHERE n.clustering IS NOT NULL RETURN n.node_uuid")
            .unwrap()
            .batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>(),
        0
    );
    graph
        .rank(
            "Person",
            clustering_coefficient_options(true, Some("KNOWS"), Some("clustering")),
        )
        .unwrap();
    assert_eq!(
        graph
            .execute("MATCH (n:Person) WHERE n.clustering IS NOT NULL RETURN n.node_uuid")
            .unwrap()
            .batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>(),
        5
    );

    let edgeless = GraphForge::new(None).unwrap();
    edgeless.execute("CREATE (:Person), (:Person)").unwrap();
    assert_eq!(
        degree_scores(
            &edgeless
                .rank("Person", clustering_coefficient_options(true, None, None),)
                .unwrap()
        ),
        [0.0, 0.0]
    );
    assert_eq!(
        GraphForge::new(None)
            .unwrap()
            .rank("Person", clustering_coefficient_options(true, None, None),)
            .unwrap()
            .num_rows(),
        0
    );
}

#[test]
fn triangles_obey_uuid_topology_order_and_writeback_contracts() {
    let graph = GraphForge::new(None).unwrap();
    graph
        .execute(
            "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (e:Person {name:'Eve'}), (f:Person {name:'Finn'}), \
                 (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(a), \
                 (b)-[:KNOWS]->(c), (c)-[:KNOWS]->(a), (c)-[:KNOWS]->(d), \
                 (d)-[:KNOWS]->(a), (c)-[:KNOWS]->(c), (e)-[:KNOWS]->(f), \
                 (b)-[:OTHER]->(d)",
        )
        .unwrap();

    let options = triangles_options(true, Some("KNOWS"), None);
    let directed = graph.rank("Person", options.clone()).unwrap();
    assert_eq!(degree_scores(&directed), [2.0, 1.0, 2.0, 1.0, 0.0, 0.0]);
    assert_eq!(directed, graph.rank("Person", options).unwrap());
    assert_eq!(
        directed.schema().metadata()["graphforge.algorithm"],
        "triangles"
    );
    assert_eq!(
        directed
            .schema()
            .field_with_name("node_uuid")
            .unwrap()
            .data_type(),
        &DataType::FixedSizeBinary(16)
    );
    assert_eq!(
        directed
            .schema()
            .field_with_name("score")
            .unwrap()
            .data_type(),
        &DataType::Float64
    );
    assert!(directed.column_by_name("node_id").is_none());
    assert_eq!(
        directed
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        [
            Some("Alice"),
            Some("Bob"),
            Some("Carol"),
            Some("Dan"),
            Some("Eve"),
            Some("Finn")
        ]
    );
    assert_eq!(
        degree_scores(
            &graph
                .rank("Person", triangles_options(false, Some("KNOWS"), None))
                .unwrap()
        ),
        [2.0, 1.0, 2.0, 1.0, 0.0, 0.0]
    );
    assert_eq!(
        degree_scores(
            &graph
                .rank("Person", triangles_options(true, None, None))
                .unwrap()
        ),
        [3.0, 3.0, 3.0, 3.0, 0.0, 0.0]
    );

    assert_eq!(
        graph
            .execute("MATCH (n:Person) WHERE n.triangle_count IS NOT NULL RETURN n.node_uuid")
            .unwrap()
            .batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>(),
        0
    );
    graph
        .rank(
            "Person",
            triangles_options(true, Some("KNOWS"), Some("triangle_count")),
        )
        .unwrap();
    assert_eq!(
        graph
            .execute("MATCH (n:Person) WHERE n.triangle_count IS NOT NULL RETURN n.node_uuid")
            .unwrap()
            .batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>(),
        6
    );

    let edgeless = GraphForge::new(None).unwrap();
    edgeless.execute("CREATE (:Person), (:Person)").unwrap();
    assert_eq!(
        degree_scores(
            &edgeless
                .rank("Person", triangles_options(true, None, None))
                .unwrap()
        ),
        [0.0, 0.0]
    );
    assert_eq!(
        GraphForge::new(None)
            .unwrap()
            .rank("Person", triangles_options(true, None, None))
            .unwrap()
            .num_rows(),
        0
    );
}

#[test]
fn k_core_obeys_uuid_topology_order_and_writeback_contracts() {
    let graph = GraphForge::new(None).unwrap();
    graph
        .execute(
            "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), \
                 (c:Person {name:'C'}), (d:Person {name:'D'}), \
                 (e:Person {name:'E'}), (f:Person {name:'F'}), \
                 (g:Person {name:'G'}), (h:Person {name:'H'}), \
                 (i:Person {name:'I'}), (j:Person {name:'J'}), \
                 (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(a), \
                 (a)-[:KNOWS]->(c), (a)-[:KNOWS]->(d), (b)-[:KNOWS]->(c), \
                 (b)-[:KNOWS]->(d), (c)-[:KNOWS]->(d), (c)-[:KNOWS]->(c), \
                 (a)-[:KNOWS]->(e), (e)-[:KNOWS]->(f), \
                 (h)-[:KNOWS]->(i), (i)-[:KNOWS]->(j), (j)-[:KNOWS]->(h), \
                 (f)-[:OTHER]->(a)",
        )
        .unwrap();

    let options = k_core_options(true, Some("KNOWS"), None);
    let directed = graph.rank("Person", options.clone()).unwrap();
    assert_eq!(
        degree_scores(&directed),
        [3.0, 3.0, 3.0, 3.0, 1.0, 1.0, 0.0, 2.0, 2.0, 2.0]
    );
    assert_eq!(directed, graph.rank("Person", options).unwrap());
    assert_eq!(
        directed.schema().metadata()["graphforge.algorithm"],
        "k_core"
    );
    assert_eq!(
        directed
            .schema()
            .field_with_name("node_uuid")
            .unwrap()
            .data_type(),
        &DataType::FixedSizeBinary(16)
    );
    assert_eq!(
        directed
            .schema()
            .field_with_name("score")
            .unwrap()
            .data_type(),
        &DataType::Float64
    );
    assert!(directed.column_by_name("node_id").is_none());
    assert_eq!(
        directed
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        [
            Some("A"),
            Some("B"),
            Some("C"),
            Some("D"),
            Some("E"),
            Some("F"),
            Some("G"),
            Some("H"),
            Some("I"),
            Some("J")
        ]
    );
    assert_eq!(
        degree_scores(
            &graph
                .rank("Person", k_core_options(false, Some("KNOWS"), None))
                .unwrap()
        ),
        degree_scores(&directed)
    );
    assert_eq!(
        degree_scores(
            &graph
                .rank("Person", k_core_options(true, None, None))
                .unwrap()
        ),
        [3.0, 3.0, 3.0, 3.0, 2.0, 2.0, 0.0, 2.0, 2.0, 2.0]
    );

    assert_eq!(
        graph
            .execute("MATCH (n:Person) WHERE n.core IS NOT NULL RETURN n.node_uuid")
            .unwrap()
            .batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>(),
        0
    );
    graph
        .rank("Person", k_core_options(true, Some("KNOWS"), Some("core")))
        .unwrap();
    assert_eq!(
        graph
            .execute("MATCH (n:Person) WHERE n.core IS NOT NULL RETURN n.node_uuid")
            .unwrap()
            .batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>(),
        10
    );

    let edgeless = GraphForge::new(None).unwrap();
    edgeless.execute("CREATE (:Person), (:Person)").unwrap();
    assert_eq!(
        degree_scores(
            &edgeless
                .rank("Person", k_core_options(true, None, None))
                .unwrap()
        ),
        [0.0, 0.0]
    );
    assert_eq!(
        GraphForge::new(None)
            .unwrap()
            .rank("Person", k_core_options(true, None, None))
            .unwrap()
            .num_rows(),
        0
    );
}

#[test]
fn preferential_attachment_obeys_aggregate_schema_and_writeback_contracts() {
    let graph = GraphForge::new(None).unwrap();
    graph
        .execute(
            "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), \
                 (c:Person {name:'C'}), (d:Person {name:'D'}), \
                 (e:Person {name:'E'}), (f:Person {name:'F'}), \
                 (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(c), \
                 (a)-[:KNOWS]->(a), (b)-[:KNOWS]->(c), (c)-[:KNOWS]->(a), \
                 (d)-[:KNOWS]->(c), (e)-[:OTHER]->(f)",
        )
        .unwrap();

    let options = preferential_attachment_options(true, Some("KNOWS"), None);
    let directed = graph.rank("Person", options.clone()).unwrap();
    assert_eq!(degree_scores(&directed), [2.0, 3.0, 2.0, 3.0, 0.0, 0.0]);
    assert_eq!(directed, graph.rank("Person", options).unwrap());
    assert_eq!(
        directed.schema().metadata()["graphforge.algorithm"],
        "preferential_attachment"
    );
    assert_eq!(
        directed
            .schema()
            .field_with_name("node_uuid")
            .unwrap()
            .data_type(),
        &DataType::FixedSizeBinary(16)
    );
    assert_eq!(
        directed
            .schema()
            .field_with_name("score")
            .unwrap()
            .data_type(),
        &DataType::Float64
    );
    assert!(directed.column_by_name("node_id").is_none());
    assert_eq!(
        directed
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        [
            Some("A"),
            Some("B"),
            Some("C"),
            Some("D"),
            Some("E"),
            Some("F")
        ]
    );
    assert_eq!(
        degree_scores(
            &graph
                .rank(
                    "Person",
                    preferential_attachment_options(false, Some("KNOWS"), None),
                )
                .unwrap()
        ),
        [2.0, 2.0, 0.0, 4.0, 0.0, 0.0]
    );
    assert_eq!(
        degree_scores(
            &graph
                .rank("Person", preferential_attachment_options(true, None, None),)
                .unwrap()
        ),
        [4.0, 4.0, 3.0, 4.0, 5.0, 0.0]
    );

    assert_eq!(
        graph
            .execute("MATCH (n:Person) WHERE n.pa IS NOT NULL RETURN n.node_uuid")
            .unwrap()
            .batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>(),
        0
    );
    graph
        .rank(
            "Person",
            preferential_attachment_options(true, Some("KNOWS"), Some("pa")),
        )
        .unwrap();
    let persisted = graph
        .execute(
            "MATCH (n:Person) WHERE n.pa IS NOT NULL \
                 RETURN n.name AS name, n.pa AS pa ORDER BY name",
        )
        .unwrap();
    assert_eq!(persisted.batches.len(), 1);
    let persisted = &persisted.batches[0];
    assert_eq!(
        persisted
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        [
            Some("A"),
            Some("B"),
            Some("C"),
            Some("D"),
            Some("E"),
            Some("F")
        ]
    );
    assert_eq!(
        persisted
            .column_by_name("pa")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values(),
        &[2.0, 3.0, 2.0, 3.0, 0.0, 0.0]
    );

    let disconnected = GraphForge::new(None).unwrap();
    disconnected
        .execute(
            "CREATE (a:Person)-[:KNOWS]->(b:Person), (b)-[:KNOWS]->(a), \
                 (c:Person)-[:KNOWS]->(d:Person), (d)-[:KNOWS]->(c)",
        )
        .unwrap();
    assert_eq!(
        degree_scores(
            &disconnected
                .rank(
                    "Person",
                    preferential_attachment_options(true, Some("KNOWS"), None),
                )
                .unwrap()
        ),
        [2.0, 2.0, 2.0, 2.0]
    );

    let complete = GraphForge::new(None).unwrap();
    complete
        .execute(
            "CREATE (a:Person), (b:Person), (c:Person), \
                 (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(a), \
                 (a)-[:KNOWS]->(c), (c)-[:KNOWS]->(a), \
                 (b)-[:KNOWS]->(c), (c)-[:KNOWS]->(b)",
        )
        .unwrap();
    assert_eq!(
        degree_scores(
            &complete
                .rank(
                    "Person",
                    preferential_attachment_options(true, Some("KNOWS"), None),
                )
                .unwrap()
        ),
        [0.0, 0.0, 0.0]
    );

    let edgeless = GraphForge::new(None).unwrap();
    edgeless.execute("CREATE (:Person), (:Person)").unwrap();
    assert_eq!(
        degree_scores(
            &edgeless
                .rank("Person", preferential_attachment_options(true, None, None),)
                .unwrap()
        ),
        [0.0, 0.0]
    );
    assert_eq!(
        GraphForge::new(None)
            .unwrap()
            .rank("Person", preferential_attachment_options(true, None, None),)
            .unwrap()
            .num_rows(),
        0
    );
}

#[test]
fn adamic_adar_obeys_aggregate_schema_via_and_writeback_contracts() {
    let graph = GraphForge::new(None).unwrap();
    graph
        .execute(
            "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), \
                 (c:Person {name:'C'}), (d:Person {name:'D'}), \
                 (e:Person {name:'E'}), (f:Person {name:'F'}), \
                 (a)-[:KNOWS]->(c), (a)-[:KNOWS]->(c), (a)-[:KNOWS]->(d), \
                 (a)-[:KNOWS]->(a), (b)-[:KNOWS]->(c), (b)-[:KNOWS]->(d), \
                 (c)-[:KNOWS]->(a), (c)-[:KNOWS]->(e), (d)-[:KNOWS]->(e), \
                 (a)-[:OTHER]->(f), (b)-[:OTHER]->(f)",
        )
        .unwrap();

    let inverse_log_two = 1.0 / 2.0_f64.ln();
    let options = adamic_adar_options(true, Some("KNOWS"), None);
    let directed = graph.rank("Person", options.clone()).unwrap();
    assert_rank_scores_close(
        &directed,
        &[
            2.0 * inverse_log_two,
            2.0 * inverse_log_two,
            inverse_log_two,
            inverse_log_two,
            0.0,
            0.0,
        ],
    );
    assert_eq!(directed, graph.rank("Person", options).unwrap());
    assert_eq!(
        directed.schema().metadata()["graphforge.algorithm"],
        "adamic_adar"
    );
    assert_eq!(
        directed
            .schema()
            .field_with_name("node_uuid")
            .unwrap()
            .data_type(),
        &DataType::FixedSizeBinary(16)
    );
    assert_eq!(
        directed
            .schema()
            .field_with_name("score")
            .unwrap()
            .data_type(),
        &DataType::Float64
    );
    assert!(directed.column_by_name("node_id").is_none());
    assert_eq!(
        directed
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        [
            Some("A"),
            Some("B"),
            Some("C"),
            Some("D"),
            Some("E"),
            Some("F")
        ]
    );

    let inverse_log_three = 1.0 / 3.0_f64.ln();
    assert_rank_scores_close(
        &graph
            .rank("Person", adamic_adar_options(false, Some("KNOWS"), None))
            .unwrap(),
        &[
            4.0 * inverse_log_three,
            4.0 * inverse_log_three,
            3.0 * inverse_log_two,
            3.0 * inverse_log_two,
            4.0 * inverse_log_three,
            0.0,
        ],
    );
    assert_rank_scores_close(
        &graph
            .rank("Person", adamic_adar_options(true, None, None))
            .unwrap(),
        &[
            3.0 * inverse_log_two,
            3.0 * inverse_log_two,
            inverse_log_two,
            inverse_log_two,
            0.0,
            0.0,
        ],
    );

    graph
        .rank(
            "Person",
            adamic_adar_options(true, Some("KNOWS"), Some("adamic")),
        )
        .unwrap();
    let persisted = graph
        .execute(
            "MATCH (n:Person) WHERE n.adamic IS NOT NULL \
                 RETURN n.name AS name, n.adamic AS score ORDER BY name",
        )
        .unwrap();
    assert_eq!(persisted.batches.len(), 1);
    let persisted = &persisted.batches[0];
    assert_eq!(
        persisted
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        [
            Some("A"),
            Some("B"),
            Some("C"),
            Some("D"),
            Some("E"),
            Some("F")
        ]
    );
    assert_rank_scores_close(
        persisted,
        &[
            2.0 * inverse_log_two,
            2.0 * inverse_log_two,
            inverse_log_two,
            inverse_log_two,
            0.0,
            0.0,
        ],
    );

    let edgeless = GraphForge::new(None).unwrap();
    edgeless.execute("CREATE (:Person), (:Person)").unwrap();
    assert_eq!(
        degree_scores(
            &edgeless
                .rank("Person", adamic_adar_options(true, None, None))
                .unwrap()
        ),
        [0.0, 0.0]
    );
    assert_eq!(
        GraphForge::new(None)
            .unwrap()
            .rank("Person", adamic_adar_options(true, None, None))
            .unwrap()
            .num_rows(),
        0
    );
}

#[test]
fn common_neighbors_obeys_aggregate_schema_via_and_writeback_contracts() {
    let graph = GraphForge::new(None).unwrap();
    graph
        .execute(
            "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), \
                 (c:Person {name:'C'}), (d:Person {name:'D'}), \
                 (e:Person {name:'E'}), (f:Person {name:'F'}), \
                 (a)-[:KNOWS]->(c), (a)-[:KNOWS]->(c), (a)-[:KNOWS]->(d), \
                 (a)-[:KNOWS]->(a), (b)-[:KNOWS]->(c), (b)-[:KNOWS]->(d), \
                 (c)-[:KNOWS]->(a), (c)-[:KNOWS]->(e), (d)-[:KNOWS]->(e), \
                 (a)-[:OTHER]->(f), (b)-[:OTHER]->(f)",
        )
        .unwrap();

    let options = common_neighbors_options(true, Some("KNOWS"), None);
    let directed = graph.rank("Person", options.clone()).unwrap();
    assert_eq!(degree_scores(&directed), [2.0, 2.0, 1.0, 1.0, 0.0, 0.0]);
    assert_eq!(directed, graph.rank("Person", options).unwrap());
    assert_eq!(
        directed.schema().metadata()["graphforge.algorithm"],
        "common_neighbors"
    );
    assert_eq!(
        directed
            .schema()
            .field_with_name("node_uuid")
            .unwrap()
            .data_type(),
        &DataType::FixedSizeBinary(16)
    );
    assert_eq!(
        directed
            .schema()
            .field_with_name("score")
            .unwrap()
            .data_type(),
        &DataType::Float64
    );
    assert!(directed.column_by_name("node_id").is_none());
    assert_eq!(
        directed
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        [
            Some("A"),
            Some("B"),
            Some("C"),
            Some("D"),
            Some("E"),
            Some("F")
        ]
    );
    assert_eq!(
        degree_scores(
            &graph
                .rank(
                    "Person",
                    common_neighbors_options(false, Some("KNOWS"), None),
                )
                .unwrap()
        ),
        [4.0, 4.0, 3.0, 3.0, 4.0, 0.0]
    );
    assert_eq!(
        degree_scores(
            &graph
                .rank("Person", common_neighbors_options(true, None, None))
                .unwrap()
        ),
        [3.0, 3.0, 1.0, 1.0, 0.0, 0.0]
    );
    assert_eq!(
        graph
            .execute("MATCH (n:Person) WHERE n.common IS NOT NULL RETURN n.node_uuid")
            .unwrap()
            .batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>(),
        0
    );

    graph
        .rank(
            "Person",
            common_neighbors_options(true, Some("KNOWS"), Some("common")),
        )
        .unwrap();
    let persisted = graph
        .execute(
            "MATCH (n:Person) WHERE n.common IS NOT NULL \
                 RETURN n.name AS name, n.common AS score ORDER BY name",
        )
        .unwrap();
    assert_eq!(persisted.batches.len(), 1);
    assert_eq!(
        degree_scores(&persisted.batches[0]),
        [2.0, 2.0, 1.0, 1.0, 0.0, 0.0]
    );

    let edgeless = GraphForge::new(None).unwrap();
    edgeless.execute("CREATE (:Person), (:Person)").unwrap();
    assert_eq!(
        degree_scores(
            &edgeless
                .rank("Person", common_neighbors_options(true, None, None))
                .unwrap()
        ),
        [0.0, 0.0]
    );
    assert_eq!(
        GraphForge::new(None)
            .unwrap()
            .rank("Person", common_neighbors_options(true, None, None))
            .unwrap()
            .num_rows(),
        0
    );
}

#[test]
fn resource_allocation_obeys_aggregate_schema_via_and_writeback_contracts() {
    let graph = GraphForge::new(None).unwrap();
    graph
        .execute(
            "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), \
                 (c:Person {name:'C'}), (d:Person {name:'D'}), \
                 (e:Person {name:'E'}), (f:Person {name:'F'}), \
                 (a)-[:KNOWS]->(c), (a)-[:KNOWS]->(c), (a)-[:KNOWS]->(d), \
                 (a)-[:KNOWS]->(a), (b)-[:KNOWS]->(c), (b)-[:KNOWS]->(d), \
                 (c)-[:KNOWS]->(a), (c)-[:KNOWS]->(e), (d)-[:KNOWS]->(e), \
                 (a)-[:OTHER]->(f), (b)-[:OTHER]->(f)",
        )
        .unwrap();

    let options = resource_allocation_options(true, Some("KNOWS"), None);
    let directed = graph.rank("Person", options.clone()).unwrap();
    assert_rank_scores_close(&directed, &[1.0, 1.0, 0.5, 0.5, 0.0, 0.0]);
    assert_eq!(directed, graph.rank("Person", options).unwrap());
    assert_eq!(
        directed.schema().metadata()["graphforge.algorithm"],
        "resource_allocation"
    );
    assert_eq!(
        directed
            .schema()
            .field_with_name("node_uuid")
            .unwrap()
            .data_type(),
        &DataType::FixedSizeBinary(16)
    );
    assert_eq!(
        directed
            .schema()
            .field_with_name("score")
            .unwrap()
            .data_type(),
        &DataType::Float64
    );
    assert_eq!(
        directed
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        ["node_uuid", "score", "name"]
    );
    assert_eq!(
        directed
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        [
            Some("A"),
            Some("B"),
            Some("C"),
            Some("D"),
            Some("E"),
            Some("F")
        ]
    );
    assert_rank_scores_close(
        &graph
            .rank(
                "Person",
                resource_allocation_options(false, Some("KNOWS"), None),
            )
            .unwrap(),
        &[4.0 / 3.0, 4.0 / 3.0, 1.5, 1.5, 4.0 / 3.0, 0.0],
    );
    assert_rank_scores_close(
        &graph
            .rank("Person", resource_allocation_options(true, None, None))
            .unwrap(),
        &[1.5, 1.5, 0.5, 0.5, 0.0, 0.0],
    );
    assert_eq!(
        graph
            .execute("MATCH (n:Person) WHERE n.resource IS NOT NULL RETURN n.node_uuid")
            .unwrap()
            .batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>(),
        0
    );

    graph
        .rank(
            "Person",
            resource_allocation_options(true, Some("KNOWS"), Some("resource")),
        )
        .unwrap();
    let persisted = graph
        .execute(
            "MATCH (n:Person) WHERE n.resource IS NOT NULL \
                 RETURN n.name AS name, n.resource AS score ORDER BY name",
        )
        .unwrap();
    assert_eq!(persisted.batches.len(), 1);
    assert_rank_scores_close(&persisted.batches[0], &[1.0, 1.0, 0.5, 0.5, 0.0, 0.0]);

    let edgeless = GraphForge::new(None).unwrap();
    edgeless.execute("CREATE (:Person), (:Person)").unwrap();
    assert_eq!(
        degree_scores(
            &edgeless
                .rank("Person", resource_allocation_options(true, None, None))
                .unwrap()
        ),
        [0.0, 0.0]
    );
    assert_eq!(
        GraphForge::new(None)
            .unwrap()
            .rank("Person", resource_allocation_options(true, None, None))
            .unwrap()
            .num_rows(),
        0
    );
}

#[test]
fn total_neighbors_obeys_aggregate_schema_via_and_writeback_contracts() {
    let graph = GraphForge::new(None).unwrap();
    graph
        .execute(
            "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), \
                 (c:Person {name:'C'}), (d:Person {name:'D'}), \
                 (e:Person {name:'E'}), (f:Person {name:'F'}), \
                 (a)-[:KNOWS]->(c), (a)-[:KNOWS]->(c), (a)-[:KNOWS]->(d), \
                 (a)-[:KNOWS]->(a), (b)-[:KNOWS]->(c), (b)-[:KNOWS]->(d), \
                 (c)-[:KNOWS]->(a), (c)-[:KNOWS]->(e), (d)-[:KNOWS]->(e), \
                 (a)-[:OTHER]->(f), (b)-[:OTHER]->(f)",
        )
        .unwrap();

    let options = total_neighbors_options(true, Some("KNOWS"), None);
    let directed = graph.rank("Person", options.clone()).unwrap();
    assert_rank_scores_close(&directed, &[6.0, 6.0, 8.0, 9.0, 7.0, 7.0]);
    assert_eq!(directed, graph.rank("Person", options).unwrap());
    assert_eq!(
        directed.schema().metadata()["graphforge.algorithm"],
        "total_neighbors"
    );
    assert_eq!(
        directed
            .schema()
            .field_with_name("node_uuid")
            .unwrap()
            .data_type(),
        &DataType::FixedSizeBinary(16)
    );
    assert_eq!(
        directed
            .schema()
            .field_with_name("score")
            .unwrap()
            .data_type(),
        &DataType::Float64
    );
    assert_eq!(
        directed
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        ["node_uuid", "score", "name"]
    );
    assert_eq!(
        directed
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        [
            Some("A"),
            Some("B"),
            Some("C"),
            Some("D"),
            Some("E"),
            Some("F")
        ]
    );
    assert_rank_scores_close(
        &graph
            .rank(
                "Person",
                total_neighbors_options(false, Some("KNOWS"), None),
            )
            .unwrap(),
        &[6.0, 6.0, 6.0, 6.0, 6.0, 12.0],
    );
    assert_rank_scores_close(
        &graph
            .rank("Person", total_neighbors_options(true, None, None))
            .unwrap(),
        &[6.0, 6.0, 9.0, 11.0, 9.0, 9.0],
    );
    assert_eq!(
        graph
            .execute("MATCH (n:Person) WHERE n.total IS NOT NULL RETURN n.node_uuid")
            .unwrap()
            .batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>(),
        0
    );

    graph
        .rank(
            "Person",
            total_neighbors_options(true, Some("KNOWS"), Some("total")),
        )
        .unwrap();
    let persisted = graph
        .execute(
            "MATCH (n:Person) WHERE n.total IS NOT NULL \
                 RETURN n.name AS name, n.total AS score ORDER BY name",
        )
        .unwrap();
    assert_eq!(persisted.batches.len(), 1);
    assert_rank_scores_close(&persisted.batches[0], &[6.0, 6.0, 8.0, 9.0, 7.0, 7.0]);

    let edgeless = GraphForge::new(None).unwrap();
    edgeless.execute("CREATE (:Person), (:Person)").unwrap();
    assert_eq!(
        degree_scores(
            &edgeless
                .rank("Person", total_neighbors_options(true, None, None))
                .unwrap()
        ),
        [0.0, 0.0]
    );
    assert_eq!(
        GraphForge::new(None)
            .unwrap()
            .rank("Person", total_neighbors_options(true, None, None))
            .unwrap()
            .num_rows(),
        0
    );
}
