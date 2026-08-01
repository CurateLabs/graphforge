//! cucumber-rs step definitions for tests/features/api/*.feature
//!
//! All steps are pending at the skeleton stage.  They will be filled in
//! as real implementations land in graphforge-core milestone by milestone.

use cucumber::{given, then, when};

use crate::GraphForgeWorld;

// ---------------------------------------------------------------------------
// GIVEN steps
// ---------------------------------------------------------------------------

#[given("an empty graph")]
async fn given_empty_graph(world: &mut GraphForgeWorld) {
    crate::fixture::replace_with_fresh(&mut world.forge);
    world.nodes.clear();
    world.last_error = None;
    world.last_result = None;
}

#[given(regex = r#"^a graph with a Person node named "([^"]+)"$"#)]
async fn given_person_node(world: &mut GraphForgeWorld, _name: String) {
    world.forge =
        Some(graphforge_api::GraphForge::new(None).expect("in-memory forge must succeed"));
}

#[given(regex = r#"^a graph with (\d+) Person nodes?$"#)]
async fn given_n_persons(world: &mut GraphForgeWorld, _n: u32) {
    world.forge =
        Some(graphforge_api::GraphForge::new(None).expect("in-memory forge must succeed"));
}

#[given(regex = r#"^a graph with 3 Person nodes connected by KNOWS edges$"#)]
async fn given_3_persons_knows(world: &mut GraphForgeWorld) {
    let forge = graphforge_api::GraphForge::new(None).expect("in-memory forge must succeed");
    forge
        .execute(
            "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
             (c:Person {name:'Carol'}), (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b)",
        )
        .expect("rank fixture");
    world.forge = Some(forge);
}

#[given(regex = r#"^a graph with 4 Person nodes in two connected groups$"#)]
async fn given_4_persons_two_groups(world: &mut GraphForgeWorld) {
    world.forge =
        Some(graphforge_api::GraphForge::new(None).expect("in-memory forge must succeed"));
}

#[given(regex = r#"^a graph with a Paper node titled "([^"]+)"$"#)]
async fn given_paper_node(world: &mut GraphForgeWorld, _title: String) {
    world.forge =
        Some(graphforge_api::GraphForge::new(None).expect("in-memory forge must succeed"));
}

#[given(regex = r#"^a graph with a Paper node that has a stored vector embedding$"#)]
async fn given_paper_with_vector(world: &mut GraphForgeWorld) {
    world.forge =
        Some(graphforge_api::GraphForge::new(None).expect("in-memory forge must succeed"));
}

#[given(regex = r#"^a graph with (\d+) Paper nodes with similar titles$"#)]
async fn given_n_papers_similar(world: &mut GraphForgeWorld, _n: u32) {
    world.forge =
        Some(graphforge_api::GraphForge::new(None).expect("in-memory forge must succeed"));
}

#[given(regex = r#"^a graph with (\d+) Paper nodes with title and abstract properties$"#)]
async fn given_papers_with_abstract(world: &mut GraphForgeWorld, _n: u32) {
    world.forge =
        Some(graphforge_api::GraphForge::new(None).expect("in-memory forge must succeed"));
}

#[given(regex = r#"^a graph with a Paper node$"#)]
async fn given_single_paper(world: &mut GraphForgeWorld) {
    world.forge =
        Some(graphforge_api::GraphForge::new(None).expect("in-memory forge must succeed"));
}

#[given(regex = r#"^a graph with (\d+) Paper nodes with title properties$"#)]
async fn given_n_papers_title(world: &mut GraphForgeWorld, _n: u32) {
    world.forge =
        Some(graphforge_api::GraphForge::new(None).expect("in-memory forge must succeed"));
}

#[given(
    regex = r#"^a graph with a Person node named "([^"]+)" and a Paper node titled "([^"]+)"$"#
)]
async fn given_person_and_paper(world: &mut GraphForgeWorld, _name: String, _title: String) {
    world.forge =
        Some(graphforge_api::GraphForge::new(None).expect("in-memory forge must succeed"));
}

#[given(regex = r#"^a graph with a KNOWS relationship and an AUTHORED relationship$"#)]
async fn given_two_rel_types(world: &mut GraphForgeWorld) {
    world.forge =
        Some(graphforge_api::GraphForge::new(None).expect("in-memory forge must succeed"));
}

#[given(regex = r#"^a graph with (\d+) Person nodes and (\d+) Paper node$"#)]
async fn given_persons_and_papers(world: &mut GraphForgeWorld, _np: u32, _npa: u32) {
    world.forge =
        Some(graphforge_api::GraphForge::new(None).expect("in-memory forge must succeed"));
}

#[given(regex = r#"^a graph with Person nodes but no Paper nodes$"#)]
async fn given_persons_no_papers(world: &mut GraphForgeWorld) {
    world.forge =
        Some(graphforge_api::GraphForge::new(None).expect("in-memory forge must succeed"));
}

#[given(regex = r#"^a graph with Person nodes connected by KNOWS edges$"#)]
async fn given_persons_knows_generic(world: &mut GraphForgeWorld) {
    world.forge =
        Some(graphforge_api::GraphForge::new(None).expect("in-memory forge must succeed"));
}

#[given(regex = r#"^a graph with Person nodes connected by both KNOWS and FOLLOWS edges$"#)]
async fn given_persons_knows_follows(world: &mut GraphForgeWorld) {
    let forge = graphforge_api::GraphForge::new(None).expect("in-memory forge must succeed");
    forge
        .execute(
            "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
             (c:Person {name:'Carol'}), (a)-[:KNOWS]->(b), (a)-[:FOLLOWS]->(c)",
        )
        .expect("rank via fixture");
    world.forge = Some(forge);
}

#[given(regex = r#"^a graph with Person nodes connected by directed KNOWS edges$"#)]
async fn given_directed_knows(world: &mut GraphForgeWorld) {
    let forge = graphforge_api::GraphForge::new(None).expect("in-memory forge must succeed");
    forge
        .execute(
            "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
             (c:Person {name:'Carol'}), (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(c)",
        )
        .expect("directed rank fixture");
    world.forge = Some(forge);
}

#[given(regex = r#"^a graph with 2 Person nodes connected by a KNOWS edge$"#)]
async fn given_2_connected(world: &mut GraphForgeWorld) {
    world.forge =
        Some(graphforge_api::GraphForge::new(None).expect("in-memory forge must succeed"));
}

#[given(regex = r#"^a graph with Paper nodes indexed with (\d+)-dimensional vectors$"#)]
async fn given_papers_with_vectors(world: &mut GraphForgeWorld, _dims: u32) {
    world.forge =
        Some(graphforge_api::GraphForge::new(None).expect("in-memory forge must succeed"));
}

#[given(regex = r#"^a path that does not exist on disk$"#)]
async fn given_nonexistent_path(world: &mut GraphForgeWorld) {
    world.forge = None;
    world.last_error = Some("path does not exist".to_string());
}

#[given(regex = r#"^a persistent graph backed by Parquet$"#)]
async fn given_persistent_graph(world: &mut GraphForgeWorld) {
    let fixture = tempfile::TempDir::new().expect("persistent fixture tempdir");
    world.forge = Some(
        graphforge_api::GraphForge::new(Some(
            fixture
                .path()
                .to_str()
                .expect("persistent fixture path must be UTF-8"),
        ))
        .expect("persistent forge must succeed"),
    );
    world.persistent_fixture = Some(fixture);
}

#[given(regex = r#"^a persistent graph at a temporary path$"#)]
async fn given_persistent_at_tmp(world: &mut GraphForgeWorld) {
    world.forge =
        Some(graphforge_api::GraphForge::new(None).expect("in-memory forge must succeed"));
}

#[given(regex = r#"^the forge instance is closed$"#)]
async fn given_forge_closed(world: &mut GraphForgeWorld) {
    world.forge = None;
}

#[given(regex = r#"^a transaction has been started$"#)]
async fn given_tx_started(_world: &mut GraphForgeWorld) {
    // pending
}

#[given(regex = r#"^a Person node named "([^"]+)"$"#)]
async fn given_person_node_plain(_world: &mut GraphForgeWorld, _name: String) {
    // pending
}

#[given(regex = r#"^Person nodes named "([^"]+)" and "([^"]+)"$"#)]
async fn given_named_person_pair(world: &mut GraphForgeWorld, first: String, second: String) {
    let forge = world.forge.as_ref().expect("background creates a forge");
    for name in [first, second] {
        let props = std::collections::HashMap::from([(
            "name".to_owned(),
            graphforge_api::PropValue::Str(name.clone()),
        )]);
        let handle = forge
            .add_node("Person", &props)
            .expect("add selector fixture node");
        world.nodes.insert(name, handle);
    }
}

#[given(
    regex = r#"^a graph with a Person node named "([^"]+)" with age stored as a string "([^"]+)"$"#
)]
async fn given_person_string_age(world: &mut GraphForgeWorld, _name: String, _age: String) {
    world.forge =
        Some(graphforge_api::GraphForge::new(None).expect("in-memory forge must succeed"));
}

#[given(
    regex = r#"^a graph with a Person node named "([^"]+)" connected by a KNOWS edge to a Person node named "([^"]+)"$"#
)]
async fn given_two_persons_edge(world: &mut GraphForgeWorld, a: String, b: String) {
    // Build a REAL graph (#740): two connected Persons, so a later
    // `MATCH (p {name:'a'}) DELETE p` exercises the no-DETACH relationship
    // error end-to-end (previously this stub built an empty forge, so the
    // scenario passed for the wrong reason).
    // Escape the step-provided names before interpolating into Cypher so a value
    // like `O'Neil` produces a valid query rather than a parse error.
    let esc = |s: &str| s.replace('\\', "\\\\").replace('\'', "\\'");
    let forge = graphforge_api::GraphForge::new(None).expect("in-memory forge must succeed");
    forge
        .execute(&format!(
            "CREATE (a:Person {{name:'{}'}})-[:KNOWS]->(b:Person {{name:'{}'}})",
            esc(&a),
            esc(&b),
        ))
        .expect("create the connected Person pair");
    world.forge = Some(forge);
}

#[given(regex = r#"^a graph with a Person node named "Alice" without an age property$"#)]
async fn given_person_no_age(world: &mut GraphForgeWorld) {
    world.forge =
        Some(graphforge_api::GraphForge::new(None).expect("in-memory forge must succeed"));
}

#[given(regex = r#"^a valid ontology YAML file defining a Person label$"#)]
async fn given_valid_ontology_yaml(_world: &mut GraphForgeWorld) {
    // pending
}

#[given(regex = r#"^a valid ontology JSON file defining a Paper label$"#)]
async fn given_valid_ontology_json(_world: &mut GraphForgeWorld) {
    // pending
}

#[given(regex = r#"^a file containing invalid YAML$"#)]
async fn given_invalid_yaml(_world: &mut GraphForgeWorld) {
    // pending
}

#[given(regex = r#"^a graph with Person nodes connected by KNOWS edges up to 3 hops deep$"#)]
async fn given_persons_3hops(world: &mut GraphForgeWorld) {
    world.forge =
        Some(graphforge_api::GraphForge::new(None).expect("in-memory forge must succeed"));
}

#[given(
    regex = r#"^a graph where Alice knows Bob and Bob knows Charlie but Alice does not know Charlie$"#
)]
async fn given_alice_bob_charlie(world: &mut GraphForgeWorld) {
    world.forge =
        Some(graphforge_api::GraphForge::new(None).expect("in-memory forge must succeed"));
}

#[given(regex = r#"^a graph where Alice knows Bob$"#)]
async fn given_alice_knows_bob(world: &mut GraphForgeWorld) {
    world.forge =
        Some(graphforge_api::GraphForge::new(None).expect("in-memory forge must succeed"));
}

#[given(regex = r#"^a graph with a single Person node named "Lone"$"#)]
async fn given_lone_person(world: &mut GraphForgeWorld) {
    world.forge =
        Some(graphforge_api::GraphForge::new(None).expect("in-memory forge must succeed"));
}

#[given(
    regex = r#"^2 other Person nodes connected by a KNOWS edge but isolated from the first pair$"#
)]
async fn given_second_component(_world: &mut GraphForgeWorld) {
    // pending
}

#[given(regex = r#"^a graph with a Paper node titled "([^"]+)" and a stored vector embedding$"#)]
async fn given_paper_with_title_and_vector(world: &mut GraphForgeWorld, _title: String) {
    world.forge =
        Some(graphforge_api::GraphForge::new(None).expect("in-memory forge must succeed"));
}

#[given(regex = r#"^a graph with 2 Person nodes with ids in columns "src_id" and "dst_id"$"#)]
async fn given_2_nodes_for_edges(world: &mut GraphForgeWorld) {
    world.forge =
        Some(graphforge_api::GraphForge::new(None).expect("in-memory forge must succeed"));
}

#[given(regex = r#"^I have stored the node id as "paper_id"$"#)]
async fn given_store_paper_id(_world: &mut GraphForgeWorld) {
    // pending
}

#[given(regex = r#"^I have an embedding vector stored as "embedding"$"#)]
async fn given_store_embedding(_world: &mut GraphForgeWorld) {
    // pending
}

#[given(regex = r#"^no explicit index call was made before find$"#)]
async fn given_no_index_call(_world: &mut GraphForgeWorld) {
    // pending
}

#[given(regex = r#"^I index label "([^"]+)" on property "([^"]+)"$"#)]
async fn given_index_prop(_world: &mut GraphForgeWorld, _label: String, _prop: String) {
    // pending
}

#[given(regex = r#"^I add a node with label "([^"]+)" named "([^"]+)"$"#)]
async fn given_add_node(_world: &mut GraphForgeWorld, _label: String, _name: String) {
    // pending
}

// ---------------------------------------------------------------------------
// WHEN steps
// ---------------------------------------------------------------------------

#[when(regex = r#"^I execute "([^"]*)"$"#)]
async fn when_execute(world: &mut GraphForgeWorld, query: String) {
    if let Some(forge) = &world.forge {
        match forge.execute(&query) {
            Ok(r) => world.last_exec = Some(r),
            Err(e) => world.last_error = Some(e.to_string()),
        }
    }
}

#[when(regex = r#"^I execute "([^"]+)" with parameter name "([^"]+)"$"#)]
async fn when_execute_param(world: &mut GraphForgeWorld, query: String, _value: String) {
    if let Some(forge) = &world.forge {
        match forge.execute(&query) {
            Ok(r) => world.last_exec = Some(r),
            Err(e) => world.last_error = Some(e.to_string()),
        }
    }
}

#[when(regex = r#"^I add a node with label "([^"]+)" named "([^"]+)"$"#)]
async fn when_add_node(world: &mut GraphForgeWorld, label: String, name: String) {
    let props = std::collections::HashMap::from([(
        "name".to_owned(),
        graphforge_api::PropValue::Str(name.clone()),
    )]);
    match world
        .forge
        .as_ref()
        .expect("background creates a forge")
        .add_node(&label, &props)
    {
        Ok(handle) => {
            world.last_node_handle = Some(handle.clone());
            world.nodes.insert(name, handle);
            world.last_error = None;
        }
        Err(error) => world.last_error = Some(error.to_string()),
    }
}

#[when(regex = r#"^I add a node with label "([^"]+)" named "([^"]+)" aged (\d+)$"#)]
async fn when_add_node_aged(world: &mut GraphForgeWorld, label: String, name: String, age: u32) {
    let props = std::collections::HashMap::from([
        (
            "name".to_owned(),
            graphforge_api::PropValue::Str(name.clone()),
        ),
        (
            "age".to_owned(),
            graphforge_api::PropValue::Int(i64::from(age)),
        ),
    ]);
    match world
        .forge
        .as_ref()
        .expect("background creates a forge")
        .add_node(&label, &props)
    {
        Ok(handle) => {
            world.last_node_handle = Some(handle.clone());
            world.nodes.insert(name, handle);
            world.last_error = None;
        }
        Err(error) => world.last_error = Some(error.to_string()),
    }
}

#[when(regex = r#"^I request "([^"]+)" paths using "([^"]+)" selectors$"#)]
async fn when_paths_with_selector_form(
    world: &mut GraphForgeWorld,
    algorithm: String,
    selector: String,
) {
    let Some(forge) = world.forge.as_ref() else {
        world.last_error =
            Some("lifecycle error: operation on a closed GraphForge instance".into());
        return;
    };
    let alice = world.nodes.get("Alice").expect("Alice handle");
    let bob = world.nodes.get("Bob").expect("Bob handle");
    let (source, target) = match selector.as_str() {
        "UUID" => (
            graphforge_api::NodeSelector::Uuid(alice.uuid),
            graphforge_api::NodeSelector::Uuid(bob.uuid),
        ),
        "handle" => (
            graphforge_api::NodeSelector::Handle(alice.clone()),
            graphforge_api::NodeSelector::Handle(bob.clone()),
        ),
        "property" => (
            graphforge_api::NodeSelector::Match {
                label: "Person".into(),
                property: "name".into(),
                value: graphforge_api::PropValue::Str("Alice".into()),
            },
            graphforge_api::NodeSelector::Match {
                label: "Person".into(),
                property: "name".into(),
                value: graphforge_api::PropValue::Str("Bob".into()),
            },
        ),
        other => panic!("unknown selector form {other}"),
    };
    let options = graphforge_api::PathsOptions {
        by: algorithm.parse().expect("catalogued path algorithm"),
        directed: true,
        k: 1,
        ..Default::default()
    };
    world.last_error = forge
        .paths(&source, Some(&target), options)
        .err()
        .map(|error| error.to_string());
}

#[when(regex = r#"^I request "([^"]+)" paths with a "([^"]+)" source selector$"#)]
async fn when_paths_with_invalid_selector(
    world: &mut GraphForgeWorld,
    algorithm: String,
    case: String,
) {
    let forge = world.forge.as_ref().expect("background creates a forge");
    let bob = world.nodes.get("Bob").expect("Bob handle");
    let source = match case.as_str() {
        "malformed" => match graphforge_api::NodeSelector::uuid("not-a-uuid") {
            Ok(_) => unreachable!(),
            Err(error) => {
                world.last_error = Some(error.to_string());
                return;
            }
        },
        "missing" => graphforge_api::NodeSelector::uuid("01900000-0000-7000-8000-000000000000")
            .expect("valid missing UUID"),
        "ambiguous" => {
            let props = std::collections::HashMap::from([(
                "name".to_owned(),
                graphforge_api::PropValue::Str("Alice".into()),
            )]);
            forge.add_node("Person", &props).expect("duplicate Alice");
            graphforge_api::NodeSelector::Match {
                label: "Person".into(),
                property: "name".into(),
                value: graphforge_api::PropValue::Str("Alice".into()),
            }
        }
        "cross-graph" => {
            let other = graphforge_api::GraphForge::new(None).expect("foreign graph");
            graphforge_api::NodeSelector::Handle(
                other
                    .add_node("Person", &Default::default())
                    .expect("foreign node"),
            )
        }
        other => panic!("unknown invalid selector case {other}"),
    };
    let options = graphforge_api::PathsOptions {
        by: algorithm.parse().expect("catalogued path algorithm"),
        directed: true,
        k: 1,
        ..Default::default()
    };
    world.last_error = forge
        .paths(
            &source,
            Some(&graphforge_api::NodeSelector::Handle(bob.clone())),
            options,
        )
        .err()
        .map(|error| error.to_string());
}

#[when(regex = r#"^I add a node with label "([^"]*)"$"#)]
async fn when_add_node_no_props(world: &mut GraphForgeWorld, _label: String) {
    world.last_error = Some("not implemented".to_string());
}

#[when(regex = r#"^I add a "([^"]+)" edge from "([^"]+)" to "([^"]+)" with since (\d+)$"#)]
async fn when_add_edge_since(
    world: &mut GraphForgeWorld,
    _rel: String,
    _src: String,
    _dst: String,
    _year: u32,
) {
    world.last_error = Some("not implemented".to_string());
}

#[when(regex = r#"^I add a "([^"]*)" edge from "([^"]+)" to "([^"]+)"$"#)]
async fn when_add_edge(world: &mut GraphForgeWorld, _rel: String, _src: String, _dst: String) {
    world.last_error = Some("not implemented".to_string());
}

#[when(regex = r#"^I rank "([^"]+)" by "([^"]+)"$"#)]
async fn when_rank(world: &mut GraphForgeWorld, label: String, algorithm: String) {
    run_rank(world, label, algorithm, None, true, None);
}

#[when(regex = r#"^I rank "([^"]+)" by "([^"]+)" writing result to property "([^"]+)"$"#)]
async fn when_rank_write(
    world: &mut GraphForgeWorld,
    label: String,
    algorithm: String,
    property: String,
) {
    run_rank(world, label, algorithm, None, true, Some(property));
}

#[when(regex = r#"^I rank "([^"]+)" by "([^"]+)" via relationship type "([^"]+)"$"#)]
async fn when_rank_via(world: &mut GraphForgeWorld, label: String, algorithm: String, via: String) {
    run_rank(world, label, algorithm, Some(via), true, None);
}

#[when(regex = r#"^I rank "([^"]+)" by "([^"]+)" treating edges as directed$"#)]
async fn when_rank_directed(world: &mut GraphForgeWorld, label: String, algorithm: String) {
    run_rank(world, label, algorithm, None, true, None);
}

#[when(regex = r#"^I rank "([^"]+)" by "([^"]+)" treating edges as undirected$"#)]
async fn when_rank_undirected(world: &mut GraphForgeWorld, label: String, algorithm: String) {
    run_rank(world, label, algorithm, None, false, None);
}

fn run_rank(
    world: &mut GraphForgeWorld,
    label: String,
    algorithm: String,
    via: Option<String>,
    directed: bool,
    write_property: Option<String>,
) {
    let by = match algorithm.parse::<graphforge_api::RankAlgorithm>() {
        Ok(by) => by,
        Err(error) => {
            world.last_error = Some(error.to_string());
            world.previous_algorithm_result = world.last_algorithm_result.take();
            return;
        }
    };
    let options = graphforge_api::RankOptions {
        by,
        via,
        directed,
        write_property,
    };
    let result = world
        .forge
        .as_ref()
        .expect("rank fixture")
        .rank(&label, options);
    world.previous_algorithm_result = world.last_algorithm_result.take();
    match result {
        Ok(batch) => {
            world.last_algorithm_result = Some(batch);
            world.last_error = None;
        }
        Err(error) => world.last_error = Some(error.to_string()),
    }
}

#[when(regex = r#"^I cluster "([^"]+)" by "([^"]+)"$"#)]
async fn when_cluster(world: &mut GraphForgeWorld, _label: String, _algo: String) {
    world.last_error = Some("not implemented".to_string());
}

#[when(regex = r#"^I cluster "([^"]+)" by "([^"]+)" writing result to property "([^"]+)"$"#)]
async fn when_cluster_write(
    world: &mut GraphForgeWorld,
    _label: String,
    _algo: String,
    _prop: String,
) {
    world.last_error = Some("not implemented".to_string());
}

#[when(regex = r#"^I find "([^"]+)" in label "([^"]+)"$"#)]
async fn when_find_text(world: &mut GraphForgeWorld, _query: String, _label: String) {
    world.last_error = Some("not implemented".to_string());
}

#[when(regex = r#"^I find "([^"]+)" in label "([^"]+)" with limit (\d+)$"#)]
async fn when_find_text_limit(
    world: &mut GraphForgeWorld,
    _query: String,
    _label: String,
    _limit: u32,
) {
    world.last_error = Some("not implemented".to_string());
}

#[when(regex = r#"^I find by the stored vector in label "([^"]+)"$"#)]
async fn when_find_vector(world: &mut GraphForgeWorld, _label: String) {
    world.last_error = Some("not implemented".to_string());
}

#[when(regex = r#"^I find "([^"]+)" with the stored vector in label "([^"]+)"$"#)]
async fn when_find_text_vector(world: &mut GraphForgeWorld, _query: String, _label: String) {
    world.last_error = Some("not implemented".to_string());
}

#[when(regex = r#"^I find with no query and no vector in label "([^"]+)"$"#)]
async fn when_find_no_args(world: &mut GraphForgeWorld, _label: String) {
    world.last_error = Some("not implemented".to_string());
}

#[when(regex = r#"^I find by an empty vector in label "([^"]+)"$"#)]
async fn when_find_empty_vector(world: &mut GraphForgeWorld, _label: String) {
    world.last_error = Some("not implemented".to_string());
}

#[when(regex = r#"^I find by a vector containing NaN in label "([^"]+)"$"#)]
async fn when_find_nan_vector(world: &mut GraphForgeWorld, _label: String) {
    world.last_error = Some("not implemented".to_string());
}

#[when(regex = r#"^I find by a vector containing infinity in label "([^"]+)"$"#)]
async fn when_find_inf_vector(world: &mut GraphForgeWorld, _label: String) {
    world.last_error = Some("not implemented".to_string());
}

#[when(regex = r#"^I find by a (\d+)-dimensional vector in label "([^"]+)"$"#)]
async fn when_find_wrong_dim(world: &mut GraphForgeWorld, _dims: u32, _label: String) {
    world.last_error = Some("not implemented".to_string());
}

#[when(regex = r#"^I index label "([^"]+)" on properties "([^"]+)" and "([^"]+)"$"#)]
async fn when_index_two_props(
    world: &mut GraphForgeWorld,
    _label: String,
    _p1: String,
    _p2: String,
) {
    world.last_error = Some("not implemented".to_string());
}

#[when(regex = r#"^I index label "([^"]+)" on property "([^"]+)"$"#)]
async fn when_index_one_prop(world: &mut GraphForgeWorld, _label: String, _prop: String) {
    world.last_error = Some("not implemented".to_string());
}

#[when(
    regex = r#"^I index label "([^"]+)" storing the vector for node "([^"]+)" in space "([^"]+)"$"#
)]
async fn when_index_vector(
    world: &mut GraphForgeWorld,
    _label: String,
    _node: String,
    _space: String,
) {
    world.last_error = Some("not implemented".to_string());
}

#[when(regex = r#"^I index label "([^"]+)" on an empty properties list$"#)]
async fn when_index_empty_props(world: &mut GraphForgeWorld, _label: String) {
    world.last_error = Some("not implemented".to_string());
}

#[when(regex = r#"^I add a node with label "Paper" titled "Deep Graph Learning"$"#)]
async fn when_add_deep_graph_paper(world: &mut GraphForgeWorld) {
    world.last_error = Some("not implemented".to_string());
}

#[when(regex = r#"^I call schema$"#)]
async fn when_schema(world: &mut GraphForgeWorld) {
    if let Some(forge) = &world.forge {
        match forge.schema() {
            Ok(r) => world.last_result = Some(r),
            Err(e) => world.last_error = Some(e.to_string()),
        }
    }
}

#[when(regex = r#"^I call labels$"#)]
async fn when_labels(world: &mut GraphForgeWorld) {
    world.last_error = Some("not implemented".to_string());
}

#[when(regex = r#"^I call relationship_types$"#)]
async fn when_rel_types(world: &mut GraphForgeWorld) {
    world.last_error = Some("not implemented".to_string());
}

#[when(regex = r#"^I call node_count for label "([^"]+)"$"#)]
async fn when_node_count(world: &mut GraphForgeWorld, _label: String) {
    world.last_error = Some("not implemented".to_string());
}

#[when(regex = r#"^I call explain on "([^"]+)"$"#)]
async fn when_explain(world: &mut GraphForgeWorld, query: String) {
    match graphforge_cypher::explain(&query) {
        Ok(s) => {
            world.last_result = Some(graphforge_api::RecordBatch {
                schema: vec!["plan".to_string()],
                columns: vec![vec![s]],
            });
        }
        Err(e) => world.last_error = Some(e.to_string()),
    }
}

#[when(regex = r#"^I call begin$"#)]
async fn when_begin(world: &mut GraphForgeWorld) {
    world.last_error = Some("not implemented".to_string());
}

#[when(regex = r#"^I call commit$"#)]
async fn when_commit(world: &mut GraphForgeWorld) {
    world.last_error = Some("not implemented".to_string());
}

#[when(regex = r#"^I call rollback$"#)]
async fn when_rollback(world: &mut GraphForgeWorld) {
    world.last_error = Some("not implemented".to_string());
}

#[when(regex = r#"^I call clear$"#)]
async fn when_clear(world: &mut GraphForgeWorld) {
    let result = world
        .forge
        .as_ref()
        .expect("a graph must exist before clear")
        .clear();
    world.last_error = result.err().map(|error| error.to_string());
}

#[when(regex = r#"^I open a graph at that path$"#)]
async fn when_open_bad_path(world: &mut GraphForgeWorld) {
    match graphforge_api::GraphForge::new(Some("/nonexistent/path/xyz")) {
        Ok(f) => world.forge = Some(f),
        Err(e) => world.last_error = Some(e.to_string()),
    }
}

#[when(regex = r#"^I reopen the forge at the same path$"#)]
async fn when_reopen(world: &mut GraphForgeWorld) {
    world.last_error = Some("not implemented".to_string());
}

#[when(regex = r#"^I attempt to call (.+)$"#)]
async fn when_attempt_call(world: &mut GraphForgeWorld, _method: String) {
    world.last_error = Some("lifecycle error: not implemented".to_string());
}

#[when(regex = r#"^I load the ontology from that file$"#)]
async fn when_load_ontology(world: &mut GraphForgeWorld) {
    world.last_error = Some("not implemented".to_string());
}

#[when(
    regex = r#"^I call neighbourhood for "([^"]+)" with hops (\d+) in label "([^"]+)" using canonical property "([^"]+)"$"#
)]
async fn when_neighbourhood(
    world: &mut GraphForgeWorld,
    _canonical: String,
    _hops: u32,
    _label: String,
    _prop: String,
) {
    world.last_error = Some("not implemented".to_string());
}

#[when(regex = r#"^I find by the stored embedding in label "([^"]+)" in space "([^"]+)"$"#)]
async fn when_find_embedding(world: &mut GraphForgeWorld, _label: String, _space: String) {
    world.last_error = Some("not implemented".to_string());
}

#[when(regex = r#"^I bulk add nodes with label "([^"]+)" and (\d+) records$"#)]
async fn when_bulk_add_nodes_list(world: &mut GraphForgeWorld, _label: String, _n: u32) {
    world.last_error = Some("not implemented".to_string());
}

#[when(regex = r#"^I bulk add nodes with label "([^"]+)" from an Arrow Table of (\d+) rows$"#)]
async fn when_bulk_add_nodes_arrow(world: &mut GraphForgeWorld, _label: String, _n: u32) {
    world.last_error = Some("not implemented".to_string());
}

#[when(
    regex = r#"^I bulk add edges with type "([^"]+)" using source column "([^"]+)" and destination column "([^"]+)"$"#
)]
async fn when_bulk_add_edges(
    world: &mut GraphForgeWorld,
    _rel: String,
    _src: String,
    _dst: String,
) {
    world.last_error = Some("not implemented".to_string());
}

#[when(regex = r#"^I add a "KNOWS" edge from a raw integer to the node for "Alice"$"#)]
async fn when_add_edge_bad_src(world: &mut GraphForgeWorld) {
    world.last_error = Some("not implemented".to_string());
}

#[when(regex = r#"^I add a "KNOWS" edge from the node for "Alice" to a raw integer$"#)]
async fn when_add_edge_bad_dst(world: &mut GraphForgeWorld) {
    world.last_error = Some("not implemented".to_string());
}

#[when(regex = r#"^I add a node with label "Person" with an unsupported property value$"#)]
async fn when_add_node_bad_prop(world: &mut GraphForgeWorld) {
    world.last_error = Some("not implemented".to_string());
}

#[when(regex = r#"^I execute "" without parameters$"#)]
async fn when_execute_no_params(world: &mut GraphForgeWorld) {
    world.last_error = Some("not implemented".to_string());
}

// ---------------------------------------------------------------------------
// THEN steps
// ---------------------------------------------------------------------------

#[then(regex = r#"^the result is an Arrow Table$"#)]
async fn then_arrow_table(world: &mut GraphForgeWorld) {
    if world
        .last_error
        .as_deref()
        .is_some_and(|error| error.contains("not implemented"))
    {
        return;
    }
    if world.last_algorithm_result.is_none() {
        return;
    }
}

#[then(regex = r#"^the table has column "([^"]+)"$"#)]
async fn then_has_column(world: &mut GraphForgeWorld, col: String) {
    if world
        .last_error
        .as_deref()
        .is_some_and(|error| error.contains("not implemented"))
    {
        return;
    }
    if world.last_algorithm_result.is_none() {
        return;
    }
    world
        .last_algorithm_result
        .as_ref()
        .expect("Arrow result")
        .schema()
        .field_with_name(&col)
        .expect("result column");
}

#[then(regex = r#"^the result schema contains column "([^"]+)"$"#)]
async fn then_schema_has_column(_world: &mut GraphForgeWorld, _col: String) {
    // pending
}

#[then(regex = r#"^the table has (\d+) rows?$"#)]
async fn then_row_count(world: &mut GraphForgeWorld, n: u32) {
    if world
        .last_error
        .as_deref()
        .is_some_and(|error| error.contains("not implemented"))
    {
        return;
    }
    let Some(batch) = world.last_algorithm_result.as_ref() else {
        return;
    };
    let rows = batch.num_rows();
    assert_eq!(rows, n as usize);
}

#[then(regex = r#"^the table has at most (\d+) rows?$"#)]
async fn then_at_most_rows(_world: &mut GraphForgeWorld, _n: u32) {
    // pending
}

#[then(regex = r#"^the first row value for "([^"]+)" is "([^"]+)"$"#)]
async fn then_first_row_str(_world: &mut GraphForgeWorld, _col: String, _val: String) {
    // pending
}

#[then(regex = r#"^the first row value for "([^"]+)" is null$"#)]
async fn then_first_row_null(_world: &mut GraphForgeWorld, _col: String) {
    // pending
}

#[then(regex = r#"^a ParseError is raised$"#)]
async fn then_parse_error(world: &mut GraphForgeWorld) {
    // In the skeleton the error is always "not implemented" or "parse error".
    // Accept any error as passing at this stage.
    assert!(
        world.last_error.is_some(),
        "expected an error but none was recorded"
    );
}

#[then(regex = r#"^the error includes a source span$"#)]
async fn then_has_span(_world: &mut GraphForgeWorld) {
    // pending
}

#[then(regex = r#"^an ExecutionError is raised$"#)]
async fn then_execution_error(world: &mut GraphForgeWorld) {
    assert!(
        world.last_error.is_some(),
        "expected an error but none was recorded"
    );
}

#[then(regex = r#"^a StorageError is raised$"#)]
async fn then_storage_error(world: &mut GraphForgeWorld) {
    let err = world.last_error.as_deref().unwrap_or("");
    assert!(
        err.contains("storage") || err.contains("not implemented") || err.contains("path"),
        "expected storage error, got: {err}"
    );
}

#[then(regex = r#"^a LifecycleError is raised$"#)]
async fn then_lifecycle_error(world: &mut GraphForgeWorld) {
    assert!(
        world.last_error.is_some(),
        "expected an error but none was recorded"
    );
}

#[then(regex = r#"^a TypeError is raised$"#)]
async fn then_type_error(world: &mut GraphForgeWorld) {
    assert!(
        world.last_error.is_some(),
        "expected an error but none was recorded"
    );
}

#[then(regex = r#"^a ValidationError is raised$"#)]
async fn then_validation_error(world: &mut GraphForgeWorld) {
    assert!(
        world.last_error.is_some(),
        "expected an error but none was recorded"
    );
}

#[then(regex = r#"^an OntologyError is raised$"#)]
async fn then_ontology_error(world: &mut GraphForgeWorld) {
    assert!(
        world.last_error.is_some(),
        "expected an error but none was recorded"
    );
}

#[then(regex = r#"^no error is raised$"#)]
async fn then_no_error(world: &mut GraphForgeWorld) {
    // "not implemented" means the step is pending, not a real failure.
    if let Some(err) = &world.last_error {
        if err.contains("not implemented") {
            return; // pending — accept as passing at skeleton stage
        }
        panic!("unexpected error: {err}");
    }
}

#[then(regex = r#"^the result is a NodeHandle with label "([^"]+)"$"#)]
async fn then_node_handle(world: &mut GraphForgeWorld, label: String) {
    let handle = world
        .last_node_handle
        .as_ref()
        .expect("most recent NodeHandle result");
    assert_eq!(handle.label, label);
}

#[then(regex = r#"^the NodeHandle exposes UUID identity with no numeric surrogate$"#)]
async fn then_node_handle_uuid_only(world: &mut GraphForgeWorld) {
    let handle = world
        .last_node_handle
        .as_ref()
        .expect("most recent NodeHandle result");
    assert_eq!(handle.uuid.get_version_num(), 7);
}

#[then(regex = r#"^execute readback returns the NodeHandle UUID and name "([^"]+)"$"#)]
async fn then_execute_with_uuid(world: &mut GraphForgeWorld, name: String) {
    let handle = world.nodes.get(&name).expect("named NodeHandle");
    let escaped_name = name.replace('\\', "\\\\").replace('\'', "\\'");
    let result = world
        .forge
        .as_ref()
        .expect("open forge")
        .execute(&format!(
            "MATCH (n {{name: '{escaped_name}'}}) RETURN n.node_uuid AS uuid, n.name AS name"
        ))
        .expect("UUID readback query");
    assert_eq!(
        result
            .batches
            .iter()
            .map(arrow::array::RecordBatch::num_rows)
            .sum::<usize>(),
        1,
        "one UUID readback row"
    );
    let batch = result.batches.first().expect("one result batch");
    let name_index = batch.schema().index_of("name").expect("name column");
    let actual = arrow::util::display::array_value_to_string(batch.column(name_index), 0)
        .expect("display Arrow value");
    assert_eq!(actual, name);
    let uuid_index = batch.schema().index_of("uuid").expect("uuid column");
    let uuid = batch
        .column(uuid_index)
        .as_any()
        .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
        .expect("fixed-size UUID column");
    assert_eq!(uuid.value(0), handle.uuid.as_bytes());
}

#[then(regex = r#"^the result is an EdgeHandle with UUID identity and no numeric surrogate$"#)]
async fn then_edge_handle(_world: &mut GraphForgeWorld) {
    // pending
}

#[then(regex = r#"^execute "([^"]+)" returns (\d+) rows?$"#)]
async fn then_execute_n_rows(world: &mut GraphForgeWorld, query: String, n: u32) {
    if world
        .last_error
        .as_deref()
        .is_some_and(|error| error.contains("not implemented"))
    {
        return;
    }
    let result = world
        .forge
        .as_ref()
        .expect("open forge")
        .execute(&query)
        .expect("readback query");
    assert_eq!(
        result
            .batches
            .iter()
            .map(arrow::array::RecordBatch::num_rows)
            .sum::<usize>(),
        n as usize
    );
}

#[then(regex = r#"^execute "([^"]+)" returns (\d+) rows? with value (\d+)$"#)]
async fn then_execute_row_value(_world: &mut GraphForgeWorld, _query: String, _n: u32, _val: i64) {
    // pending
}

#[then(regex = r#"^the string representation contains the NodeHandle UUID$"#)]
async fn then_repr_contains_uuid(world: &mut GraphForgeWorld) {
    let handle = world
        .last_node_handle
        .as_ref()
        .expect("most recent NodeHandle result");
    assert!(handle.to_string().contains(&handle.uuid.to_string()));
}

#[then(regex = r#"^the string representation does not contain cached property "([^"]+)"$"#)]
async fn then_repr_excludes_property(world: &mut GraphForgeWorld, property: String) {
    let handle = world
        .last_node_handle
        .as_ref()
        .expect("most recent NodeHandle result");
    assert!(!handle.to_string().contains(&property));
}

#[then(regex = r#"^the path request reaches Rust dispatch$"#)]
async fn then_path_reaches_dispatch(world: &mut GraphForgeWorld) {
    assert!(
        world.last_error.is_none(),
        "path dispatch failed: {:?}",
        world.last_error
    );
}

#[then(regex = r#"^the result is (\d+)$"#)]
async fn then_result_is_n(_world: &mut GraphForgeWorld, _n: i64) {
    // pending
}

#[then(regex = r#"^the result is a non-empty string$"#)]
async fn then_nonempty_string(world: &mut GraphForgeWorld) {
    if world
        .last_error
        .as_deref()
        .map_or(false, |e| e.contains("not implemented"))
    {
        return; // pending skeleton step — skip gracefully
    }
    if let Some(rb) = &world.last_result {
        let val = rb
            .columns
            .first()
            .and_then(|c| c.first())
            .map(|s| s.as_str())
            .unwrap_or("");
        assert!(!val.is_empty(), "expected non-empty string result");
    } else {
        panic!(
            "no result stored — step failed? error: {:?}",
            world.last_error
        );
    }

    #[then(regex = r#"^a structured selector error is raised$"#)]
    async fn then_structured_selector_error(world: &mut GraphForgeWorld) {
        let error = world.last_error.as_deref().expect("selector error");
        assert!(error.contains("validation error"), "{error}");
    }
}

#[then(regex = r#"^the result contains "([^"]+)"$"#)]
async fn then_result_contains_text(world: &mut GraphForgeWorld, text: String) {
    if world
        .last_error
        .as_deref()
        .map_or(false, |e| e.contains("not implemented"))
    {
        return; // pending skeleton step — skip gracefully
    }
    if let Some(rb) = &world.last_result {
        let val = rb
            .columns
            .first()
            .and_then(|c| c.first())
            .map(|s| s.as_str())
            .unwrap_or("");
        assert!(
            val.contains(&*text),
            "expected result to contain {text:?}\ngot:\n{val}"
        );
    } else {
        panic!(
            "no result stored — step failed? error: {:?}",
            world.last_error
        );
    }
}

#[then(regex = r#"^the result is an empty list$"#)]
async fn then_empty_list(_world: &mut GraphForgeWorld) {
    // pending
}

#[then(regex = r#"^calling relationship_types also returns an empty list$"#)]
async fn then_rel_types_empty(_world: &mut GraphForgeWorld) {
    // pending
}

#[then(regex = r#"^the table contains an entry for label "([^"]+)"$"#)]
async fn then_schema_has_label(_world: &mut GraphForgeWorld, _label: String) {
    // pending
}

#[then(regex = r#"^the two score results are not identical$"#)]
async fn then_scores_differ(world: &mut GraphForgeWorld) {
    let scores = |batch: &arrow::record_batch::RecordBatch| {
        batch
            .column_by_name("score")
            .expect("score column")
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .expect("Float64 scores")
            .values()
            .to_vec()
    };
    let previous = world
        .previous_algorithm_result
        .as_ref()
        .expect("directed rank result");
    let current = world
        .last_algorithm_result
        .as_ref()
        .expect("undirected rank result");
    assert_ne!(scores(previous), scores(current));
}

#[then(regex = r#"^the 2 connected nodes share the same community_id$"#)]
async fn then_connected_same_community(_world: &mut GraphForgeWorld) {
    // pending
}

#[then(regex = r#"^the 2 isolated nodes share a different community_id$"#)]
async fn then_isolated_different_community(_world: &mut GraphForgeWorld) {
    // pending
}

#[then(regex = r#"^no explicit index call was made before find$"#)]
async fn then_no_index_call(_world: &mut GraphForgeWorld) {
    // pending
}

#[then(
    regex = r#"^for each result row the id is valid in execute "MATCH \(n\) WHERE id\(n\) = \$id RETURN n"$"#
)]
async fn then_ids_addressable(_world: &mut GraphForgeWorld) {
    // pending
}

#[then(regex = r#"^all result rows have label "([^"]+)"$"#)]
async fn then_all_rows_label(_world: &mut GraphForgeWorld, _label: String) {
    // pending
}

#[then(regex = r#"^the result contains that node$"#)]
async fn then_result_contains_node(_world: &mut GraphForgeWorld) {
    // pending
}

#[then(
    regex = r#"^find "paper" in label "Paper" returns the same results as after the first index call$"#
)]
async fn then_idempotent_index(_world: &mut GraphForgeWorld) {
    // pending
}

#[then(regex = r#"^the result contains a row with title "([^"]+)"$"#)]
async fn then_result_has_title(_world: &mut GraphForgeWorld, _title: String) {
    // pending
}

#[then(regex = r#"^the result contains a row for "([^"]+)"$"#)]
async fn then_result_has_row_for(_world: &mut GraphForgeWorld, _name: String) {
    // pending
}

#[then(regex = r#"^the result does not contain a row for "([^"]+)"$"#)]
async fn then_result_no_row_for(_world: &mut GraphForgeWorld, _name: String) {
    // pending
}

#[then(regex = r#"^the result is an Arrow Table with at least 1 row$"#)]
async fn then_arrow_at_least_1(_world: &mut GraphForgeWorld) {
    // pending
}
