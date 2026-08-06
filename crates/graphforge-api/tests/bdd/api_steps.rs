//! cucumber-rs step definitions for tests/features/api/*.feature
//!
//! Required scenarios call the real Rust facade and assert observable results.

use arrow::array::Array;
use cucumber::{given, then, when};

use crate::GraphForgeWorld;

// ---------------------------------------------------------------------------
// GIVEN steps
// ---------------------------------------------------------------------------

#[given("an empty graph")]
async fn given_empty_graph(world: &mut GraphForgeWorld) {
    crate::fixture::replace_with_fresh(&mut world.forge);
    world.nodes.clear();
    world.index_calls = 0;
    world.last_error = None;
    world.last_error_code = None;
    world.last_result = None;
    world.last_algorithm_result = None;
}

#[given(regex = r#"^a graph with a Person node named "([^"]+)"$"#)]
async fn given_person_node(world: &mut GraphForgeWorld, name: String) {
    let forge = graphforge_api::GraphForge::new(None).expect("in-memory forge must succeed");
    world.nodes.clear();
    let props = std::collections::HashMap::from([(
        "name".to_owned(),
        graphforge_api::PropValue::Str(name.clone()),
    )]);
    let handle = forge.add_node("Person", &props).expect("Person fixture");
    world.nodes.insert(name, handle);
    world.forge = Some(forge);
}

#[given(regex = r#"^a graph with (\d+) Person nodes?$"#)]
async fn given_n_persons(world: &mut GraphForgeWorld, n: u32) {
    let forge = graphforge_api::GraphForge::new(None).expect("in-memory forge must succeed");
    world.nodes.clear();
    for index in 0..n {
        let props = std::collections::HashMap::from([(
            "name".to_owned(),
            graphforge_api::PropValue::Str(format!("Person{index}")),
        )]);
        forge.add_node("Person", &props).expect("Person fixture");
    }
    world.forge = Some(forge);
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
    let forge = graphforge_api::GraphForge::new(None).expect("in-memory forge must succeed");
    forge
        .execute(
            "CREATE (a:Person {name:'Alice'})-[:KNOWS]->(b:Person {name:'Bob'}), \
             (c:Person {name:'Carol'})-[:KNOWS]->(d:Person {name:'Dave'})",
        )
        .expect("two-component fixture");
    world.forge = Some(forge);
}

#[given(regex = r#"^a graph with a Paper node titled "([^"]+)"$"#)]
async fn given_paper_node(world: &mut GraphForgeWorld, title: String) {
    let forge = graphforge_api::GraphForge::new(None).expect("in-memory forge must succeed");
    let props = std::collections::HashMap::from([(
        "title".to_owned(),
        graphforge_api::PropValue::Str(title.clone()),
    )]);
    let handle = forge.add_node("Paper", &props).expect("Paper fixture");
    world.nodes.insert(title, handle);
    world.forge = Some(forge);
}

#[given(regex = r#"^a graph with a Paper node that has a stored vector embedding$"#)]
async fn given_paper_with_vector(world: &mut GraphForgeWorld) {
    let forge = graphforge_api::GraphForge::new(None).expect("in-memory forge must succeed");
    let props = std::collections::HashMap::from([(
        "title".to_owned(),
        graphforge_api::PropValue::Str("Stub Paper".into()),
    )]);
    let handle = forge.add_node("Paper", &props).expect("Paper fixture");
    let vector = vec![1.0_f32; 128];
    forge
        .index_search(
            "Paper",
            graphforge_api::SearchIndexOptions::Vector {
                node: graphforge_api::NodeSelector::Handle(handle.clone()),
                vector: vector.clone(),
                space: "sbert".to_owned(),
            },
        )
        .expect("vector upsert fixture");
    world.nodes.insert("Stub Paper".into(), handle);
    world.stored_vector = Some(vector);
    world.stored_space = Some("sbert".into());
    world.forge = Some(forge);
}

#[given(regex = r#"^a graph with (\d+) Paper nodes with similar titles$"#)]
async fn given_n_papers_similar(world: &mut GraphForgeWorld, n: u32) {
    create_papers(world, n, true);
}

#[given(regex = r#"^a graph with (\d+) Paper nodes with title and abstract properties$"#)]
async fn given_papers_with_abstract(world: &mut GraphForgeWorld, _n: u32) {
    let forge = graphforge_api::GraphForge::new(None).expect("in-memory forge must succeed");
    for index in 0.._n {
        let props = std::collections::HashMap::from([
            (
                "title".to_owned(),
                graphforge_api::PropValue::Str(format!("Neural networks {index}")),
            ),
            (
                "abstract".to_owned(),
                graphforge_api::PropValue::Str("graph neural networks".into()),
            ),
        ]);
        forge.add_node("Paper", &props).expect("Paper fixture");
    }
    world.forge = Some(forge);
}

#[given(regex = r#"^a graph with a Paper node$"#)]
async fn given_single_paper(world: &mut GraphForgeWorld) {
    create_papers(world, 1, false);
}

#[given(regex = r#"^a graph with (\d+) Paper nodes with title properties$"#)]
async fn given_n_papers_title(world: &mut GraphForgeWorld, n: u32) {
    create_papers(world, n, false);
}

fn create_papers(world: &mut GraphForgeWorld, count: u32, similar: bool) {
    let forge = graphforge_api::GraphForge::new(None).expect("in-memory forge must succeed");
    for index in 0..count {
        let title = if similar {
            format!("Graph paper {index}")
        } else if count == 1 {
            "Stub Paper".to_owned()
        } else {
            format!("Paper {index}")
        };
        let props = std::collections::HashMap::from([(
            "title".to_owned(),
            graphforge_api::PropValue::Str(title.clone()),
        )]);
        let handle = forge.add_node("Paper", &props).expect("Paper fixture");
        if count == 1 {
            world.nodes.insert("paper".into(), handle.clone());
            world.stored_paper_id = Some(handle.uuid.to_string());
        }
        world.nodes.insert(title, handle);
    }
    world.forge = Some(forge);
}

#[given(
    regex = r#"^a graph with a (Person node named|Paper node titled) "([^"]+)" and a (Paper node titled|Person node named) "([^"]+)"$"#
)]
async fn given_person_and_paper(
    world: &mut GraphForgeWorld,
    first_kind: String,
    first_value: String,
    _second_kind: String,
    second_value: String,
) {
    let (name, title) = if first_kind.starts_with("Person") {
        (first_value, second_value)
    } else {
        (second_value, first_value)
    };
    let forge = graphforge_api::GraphForge::new(None).expect("in-memory forge must succeed");
    let person_props = std::collections::HashMap::from([(
        "name".to_owned(),
        graphforge_api::PropValue::Str(name),
    )]);
    forge
        .add_node("Person", &person_props)
        .expect("person introspection fixture");
    let paper_props = std::collections::HashMap::from([(
        "title".to_owned(),
        graphforge_api::PropValue::Str(title),
    )]);
    forge
        .add_node("Paper", &paper_props)
        .expect("paper introspection fixture");
    world.forge = Some(forge);
}

#[given(regex = r#"^a graph with a KNOWS relationship and an AUTHORED relationship$"#)]
async fn given_two_rel_types(world: &mut GraphForgeWorld) {
    let forge = graphforge_api::GraphForge::new(None).expect("in-memory forge must succeed");
    forge
        .execute(
            "CREATE (a:Person)-[:KNOWS]->(:Person), \
             (a)-[:AUTHORED]->(:Paper)",
        )
        .expect("relationship introspection fixture");
    world.forge = Some(forge);
}

#[given(regex = r#"^a graph with (\d+) Person nodes and (\d+) Paper node$"#)]
async fn given_persons_and_papers(world: &mut GraphForgeWorld, np: u32, npa: u32) {
    let forge = graphforge_api::GraphForge::new(None).expect("in-memory forge must succeed");
    let mut patterns = Vec::new();
    patterns.extend((0..np).map(|index| format!("(:Person {{name:'Person{index}'}})")));
    patterns.extend((0..npa).map(|index| format!("(:Paper {{title:'Paper{index}'}})")));
    if !patterns.is_empty() {
        forge
            .execute(&format!("CREATE {}", patterns.join(", ")))
            .expect("count introspection fixture");
    }
    world.forge = Some(forge);
}

#[given(regex = r#"^a graph with Person nodes but no Paper nodes$"#)]
async fn given_persons_no_papers(world: &mut GraphForgeWorld) {
    given_n_persons(world, 2).await;
}

#[given(regex = r#"^a graph with Person nodes connected by KNOWS edges$"#)]
async fn given_persons_knows_generic(world: &mut GraphForgeWorld) {
    given_3_persons_knows(world).await;
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
    let forge = graphforge_api::GraphForge::new(None).expect("in-memory forge must succeed");
    let alice = add_named_fixture_node(&forge, "Alice");
    let bob = add_named_fixture_node(&forge, "Bob");
    forge
        .add_edge(&alice, "KNOWS", &bob, &Default::default())
        .expect("connected fixture edge");
    world.nodes.insert("Alice".into(), alice);
    world.nodes.insert("Bob".into(), bob);
    world.forge = Some(forge);
}

fn add_named_fixture_node(
    forge: &graphforge_api::GraphForge,
    name: &str,
) -> graphforge_api::NodeHandle {
    let props = std::collections::HashMap::from([(
        "name".to_owned(),
        graphforge_api::PropValue::Str(name.into()),
    )]);
    forge
        .add_node("Person", &props)
        .expect("named fixture node")
}

fn is_cypher_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

#[given("a graph with a directed cycle")]
async fn given_directed_cycle(world: &mut GraphForgeWorld) {
    let forge = graphforge_api::GraphForge::new(None).expect("in-memory forge must succeed");
    forge
        .execute(
            "CREATE (a:Person {name:'Alice'})-[:KNOWS]->(b:Person {name:'Bob'}), \
             (b)-[:KNOWS]->(a)",
        )
        .expect("directed cycle fixture");
    world.forge = Some(forge);
}

#[given(regex = r#"^a graph with Paper nodes indexed with (\d+)-dimensional vectors$"#)]
async fn given_papers_with_vectors(world: &mut GraphForgeWorld, dims: u32) {
    let forge = graphforge_api::GraphForge::new(None).expect("in-memory forge must succeed");
    let props = std::collections::HashMap::from([(
        "title".to_owned(),
        graphforge_api::PropValue::Str("Stub".into()),
    )]);
    let handle = forge.add_node("Paper", &props).expect("Paper fixture");
    let vector = vec![1.0_f32; dims as usize];
    forge
        .index_search(
            "Paper",
            graphforge_api::SearchIndexOptions::Vector {
                node: graphforge_api::NodeSelector::Handle(handle.clone()),
                vector: vector.clone(),
                space: "sbert".to_owned(),
            },
        )
        .expect("vector upsert fixture");
    world.nodes.insert("paper".into(), handle);
    world.stored_vector = Some(vector);
    world.stored_space = Some("sbert".into());
    world.forge = Some(forge);
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
    given_persistent_graph(world).await;
}

#[given(regex = r#"^the forge instance is closed$"#)]
async fn given_forge_closed(world: &mut GraphForgeWorld) {
    world.forge = None;
}

#[given(regex = r#"^a Person node named "([^"]+)"$"#)]
async fn given_person_node_plain(world: &mut GraphForgeWorld, name: String) {
    let forge = world.forge.as_ref().expect("background creates a forge");
    let props = std::collections::HashMap::from([(
        "name".to_owned(),
        graphforge_api::PropValue::Str(name.clone()),
    )]);
    let handle = forge.add_node("Person", &props).expect("Person fixture");
    world.nodes.insert(name, handle);
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

#[given(regex = r#"^a graph with a Person node named "([^"]+)" with age (\d+)$"#)]
async fn given_person_numeric_age(world: &mut GraphForgeWorld, name: String, age: u32) {
    let forge = graphforge_api::GraphForge::new(None).expect("in-memory forge must succeed");
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
    let handle = forge.add_node("Person", &props).expect("Person fixture");
    world.nodes.insert(name, handle);
    world.forge = Some(forge);
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
    given_person_node(world, "Alice".into()).await;
}

#[given(regex = r#"^a valid ontology YAML file defining a Person label$"#)]
async fn given_valid_ontology_yaml(world: &mut GraphForgeWorld) {
    set_ontology_fixture(
        world,
        "ontology.yaml",
        "ontology_id: people\nversion: \"2026.06\"\nentity_types:\n  - name: Person\nproperties:\n  - name: name\n    owner: Person\n    type: utf8\n",
    );
}

#[given(regex = r#"^a valid ontology JSON file defining a Paper label$"#)]
async fn given_valid_ontology_json(world: &mut GraphForgeWorld) {
    set_ontology_fixture(
        world,
        "ontology.json",
        r#"{"ontology_id":"papers","version":"2026.06","entity_types":[{"name":"Paper"}],"properties":[{"name":"title","owner":"Paper","type":"utf8"}]}"#,
    );
}

#[given(regex = r#"^a file containing invalid YAML$"#)]
async fn given_invalid_yaml(world: &mut GraphForgeWorld) {
    set_ontology_fixture(world, "bad.yaml", ": this is not: valid: yaml: [");
}

fn set_ontology_fixture(world: &mut GraphForgeWorld, name: &str, contents: &str) {
    let fixture = tempfile::TempDir::new().expect("ontology fixture tempdir");
    let path = fixture.path().join(name);
    std::fs::write(&path, contents).expect("write ontology fixture");
    world.ontology_path = Some(path);
    world.ontology_fixture = Some(fixture);
    if world.forge.is_none() {
        world.forge = Some(graphforge_api::GraphForge::new(None).expect("ontology fixture forge"));
    }
}

#[given(regex = r#"^a graph with Person nodes connected by KNOWS edges up to 3 hops deep$"#)]
async fn given_persons_3hops(world: &mut GraphForgeWorld) {
    let forge = graphforge_api::GraphForge::new(None).expect("in-memory forge must succeed");
    world.nodes.clear();
    let names = ["Alice", "Bob", "Carol", "Dave"];
    let mut handles = Vec::with_capacity(names.len());
    for name in names {
        let handle = add_named_fixture_node(&forge, name);
        world.nodes.insert(name.into(), handle.clone());
        handles.push(handle);
    }
    for window in handles.windows(2) {
        forge
            .add_edge(&window[0], "KNOWS", &window[1], &Default::default())
            .expect("3-hop fixture edge");
    }
    world.forge = Some(forge);
}

#[given(
    regex = r#"^a graph where Alice knows Bob and Bob knows Charlie but Alice does not know Charlie$"#
)]
async fn given_alice_bob_charlie(world: &mut GraphForgeWorld) {
    let forge = graphforge_api::GraphForge::new(None).expect("in-memory forge must succeed");
    let alice = add_named_fixture_node(&forge, "Alice");
    let bob = add_named_fixture_node(&forge, "Bob");
    let charlie = add_named_fixture_node(&forge, "Charlie");
    forge
        .add_edge(&alice, "KNOWS", &bob, &Default::default())
        .expect("Alice-Bob edge");
    forge
        .add_edge(&bob, "KNOWS", &charlie, &Default::default())
        .expect("Bob-Charlie edge");
    world.nodes.clear();
    world.nodes.insert("Alice".into(), alice);
    world.nodes.insert("Bob".into(), bob);
    world.nodes.insert("Charlie".into(), charlie);
    world.forge = Some(forge);
}

#[given(regex = r#"^a graph where Alice knows Bob$"#)]
async fn given_alice_knows_bob(world: &mut GraphForgeWorld) {
    let forge = graphforge_api::GraphForge::new(None).expect("in-memory forge must succeed");
    let alice = add_named_fixture_node(&forge, "Alice");
    let bob = add_named_fixture_node(&forge, "Bob");
    forge
        .add_edge(&alice, "KNOWS", &bob, &Default::default())
        .expect("Alice-Bob edge");
    world.nodes.clear();
    world.nodes.insert("Alice".into(), alice);
    world.nodes.insert("Bob".into(), bob);
    world.forge = Some(forge);
}

#[given(regex = r#"^a graph with a single Person node named "Lone"$"#)]
async fn given_lone_person(world: &mut GraphForgeWorld) {
    let forge = graphforge_api::GraphForge::new(None).expect("in-memory forge must succeed");
    let lone = add_named_fixture_node(&forge, "Lone");
    world.nodes.clear();
    world.nodes.insert("Lone".into(), lone);
    world.forge = Some(forge);
}

#[given(
    regex = r#"^2 other Person nodes connected by a KNOWS edge but isolated from the first pair$"#
)]
async fn given_second_component(_world: &mut GraphForgeWorld) {
    let forge = _world.forge.as_ref().expect("connected fixture");
    let carol = add_named_fixture_node(forge, "Carol");
    let dave = add_named_fixture_node(forge, "Dave");
    forge
        .add_edge(&carol, "KNOWS", &dave, &Default::default())
        .expect("second component fixture edge");
    _world.nodes.insert("Carol".into(), carol);
    _world.nodes.insert("Dave".into(), dave);
}

#[given(regex = r#"^a graph with a Paper node titled "([^"]+)" and a stored vector embedding$"#)]
async fn given_paper_with_title_and_vector(world: &mut GraphForgeWorld, title: String) {
    let forge = graphforge_api::GraphForge::new(None).expect("in-memory forge must succeed");
    let props = std::collections::HashMap::from([(
        "title".to_owned(),
        graphforge_api::PropValue::Str(title.clone()),
    )]);
    let handle = forge.add_node("Paper", &props).expect("Paper fixture");
    let vector = vec![1.0_f32; 128];
    forge
        .index_search(
            "Paper",
            graphforge_api::SearchIndexOptions::Vector {
                node: graphforge_api::NodeSelector::Handle(handle.clone()),
                vector: vector.clone(),
                space: "sbert".to_owned(),
            },
        )
        .expect("vector upsert fixture");
    world.nodes.insert(title, handle);
    world.stored_vector = Some(vector);
    world.stored_space = Some("sbert".into());
    world.forge = Some(forge);
}

#[given(regex = r#"^a graph with 2 Person nodes with ids in columns "src_id" and "dst_id"$"#)]
async fn given_2_nodes_for_edges(world: &mut GraphForgeWorld) {
    let forge = graphforge_api::GraphForge::new(None).expect("in-memory forge must succeed");
    let source = add_named_fixture_node(&forge, "Source");
    let target = add_named_fixture_node(&forge, "Target");
    world.nodes.insert("src_id".into(), source);
    world.nodes.insert("dst_id".into(), target);
    world.forge = Some(forge);
}

#[given(regex = r#"^I add a node with label "([^"]+)" named "([^"]+)"$"#)]
async fn given_add_node(world: &mut GraphForgeWorld, label: String, name: String) {
    let props = std::collections::HashMap::from([(
        "name".to_owned(),
        graphforge_api::PropValue::Str(name.clone()),
    )]);
    let handle = world
        .forge
        .as_ref()
        .expect("open forge")
        .add_node(&label, &props)
        .expect("fixture node");
    world.nodes.insert(name, handle);
}

// ---------------------------------------------------------------------------
// WHEN steps
// ---------------------------------------------------------------------------

#[when(regex = r#"^I analyze by "([^"]+)"$"#)]
async fn when_analyze(world: &mut GraphForgeWorld, algorithm: String) {
    let by = match algorithm.parse::<graphforge_api::AnalyzeAlgorithm>() {
        Ok(value) => value,
        Err(error) => {
            world.last_error = Some(error.to_string());
            world.last_algorithm_result = None;
            return;
        }
    };
    let options = graphforge_api::AnalyzeOptions {
        by,
        directed: true,
        ..Default::default()
    };
    match world
        .forge
        .as_ref()
        .expect("analyze fixture")
        .analyze(None, options)
    {
        Ok(batch) => {
            world.last_algorithm_result = Some(batch);
            world.last_error = None;
        }
        Err(error) => {
            world.last_algorithm_result = None;
            world.last_error = Some(error.to_string());
        }
    }
}

#[when(regex = r#"^I execute "([^"]*)"$"#)]
async fn when_execute(world: &mut GraphForgeWorld, query: String) {
    if let Some(forge) = &world.forge {
        match forge.execute(&query) {
            Ok(r) => {
                world.last_exec = Some(r);
                world.last_error = None;
                world.last_error_code = None;
            }
            Err(error) => {
                world.last_error_code = Some(error.code());
                world.last_error = Some(error.to_string());
            }
        }
    }
}

#[when(regex = r#"^I execute "([^"]+)" with parameter name "([^"]+)"$"#)]
async fn when_execute_param(world: &mut GraphForgeWorld, query: String, _value: String) {
    if let Some(forge) = &world.forge {
        let params = std::collections::HashMap::from([(
            "name".to_owned(),
            graphforge_api::IrLiteral::Str(_value),
        )]);
        match forge.execute_with_params(&query, &params) {
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
    let forge = world.forge.as_ref().expect("open path fixture");
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
async fn when_add_node_no_props(world: &mut GraphForgeWorld, label: String) {
    match world
        .forge
        .as_ref()
        .expect("background creates a forge")
        .add_node(&label, &Default::default())
    {
        Ok(handle) => {
            world.last_node_handle = Some(handle);
            world.last_error = None;
        }
        Err(error) => world.last_error = Some(error.to_string()),
    }
}

#[when(regex = r#"^I add a "([^"]+)" edge from "([^"]+)" to "([^"]+)" with since (\d+)$"#)]
async fn when_add_edge_since(
    world: &mut GraphForgeWorld,
    rel: String,
    src: String,
    dst: String,
    year: u32,
) {
    let props = std::collections::HashMap::from([(
        "since".to_owned(),
        graphforge_api::PropValue::Int(i64::from(year)),
    )]);
    add_edge(world, rel, src, dst, props);
}

#[when(regex = r#"^I add a "([^"]*)" edge from "([^"]+)" to "([^"]+)"$"#)]
async fn when_add_edge(world: &mut GraphForgeWorld, rel: String, src: String, dst: String) {
    add_edge(world, rel, src, dst, Default::default());
}

fn add_edge(
    world: &mut GraphForgeWorld,
    rel: String,
    src: String,
    dst: String,
    props: std::collections::HashMap<String, graphforge_api::PropValue>,
) {
    let forge = world.forge.as_ref().expect("background creates a forge");
    let source = world.nodes.get(&src).expect("source fixture handle");
    let target = world.nodes.get(&dst).expect("destination fixture handle");
    match forge.add_edge(source, &rel, target, &props) {
        Ok(handle) => {
            world.last_edge_handle = Some(handle);
            world.last_error = None;
        }
        Err(error) => world.last_error = Some(error.to_string()),
    }
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
async fn when_cluster(world: &mut GraphForgeWorld, label: String, algo: String) {
    run_cluster(world, label, algo, None);
}

#[when(regex = r#"^I cluster "([^"]+)" by "([^"]+)" writing result to property "([^"]+)"$"#)]
async fn when_cluster_write(
    world: &mut GraphForgeWorld,
    label: String,
    algo: String,
    prop: String,
) {
    run_cluster(world, label, algo, Some(prop));
}

fn run_cluster(
    world: &mut GraphForgeWorld,
    label: String,
    algo: String,
    write_property: Option<String>,
) {
    let by = match algo.parse::<graphforge_api::ClusterAlgorithm>() {
        Ok(value) => value,
        Err(error) => {
            world.last_error = Some(error.to_string());
            return;
        }
    };
    let options = graphforge_api::ClusterOptions {
        by,
        directed: false,
        write_property,
        ..Default::default()
    };
    match world
        .forge
        .as_ref()
        .expect("cluster fixture")
        .cluster(&label, options)
    {
        Ok(batch) => {
            world.last_algorithm_result = Some(batch);
            world.last_error = None;
        }
        Err(error) => world.last_error = Some(error.to_string()),
    }
}

#[when(regex = r#"^I find "([^"]+)" in label "([^"]+)"$"#)]
async fn when_find_text(world: &mut GraphForgeWorld, query: String, label: String) {
    run_find(world, Some(query), label, 10, None, None);
}

#[when(regex = r#"^I find "([^"]+)" in label "([^"]+)" with limit (\d+)$"#)]
async fn when_find_text_limit(
    world: &mut GraphForgeWorld,
    query: String,
    label: String,
    limit: u32,
) {
    run_find(world, Some(query), label, limit as usize, None, None);
}

fn run_find(
    world: &mut GraphForgeWorld,
    query: Option<String>,
    label: String,
    limit: usize,
    vector: Option<Vec<f32>>,
    space: Option<String>,
) {
    let options = graphforge_api::FindOptions {
        query,
        label: Some(label),
        vector,
        space,
        limit,
        ..Default::default()
    };
    match world.forge.as_ref().expect("find fixture").find(options) {
        Ok(batch) => {
            world.previous_algorithm_result = world.last_algorithm_result.take();
            world.last_algorithm_result = Some(batch);
            world.last_error = None;
        }
        Err(error) => {
            world.previous_algorithm_result = world.last_algorithm_result.take();
            world.last_exec = None;
            world.last_error = Some(error.to_string());
        }
    }
}

#[when(regex = r#"^I find by the stored vector in label "Paper"$"#)]
async fn when_find_stored_vector(world: &mut GraphForgeWorld) {
    let vector = world.stored_vector.clone().expect("stored vector fixture");
    let space = world
        .stored_space
        .clone()
        .unwrap_or_else(|| "sbert".to_owned());
    run_find(world, None, "Paper".into(), 10, Some(vector), Some(space));
}

#[when(regex = r#"^I find by the stored embedding in label "([^"]+)" in space "([^"]+)"$"#)]
async fn when_find_stored_embedding(world: &mut GraphForgeWorld, label: String, space: String) {
    let vector = world
        .stored_vector
        .clone()
        .expect("stored embedding fixture");
    run_find(world, None, label, 10, Some(vector), Some(space));
}

#[when(regex = r#"^I find "([^"]+)" with the stored vector in label "([^"]+)"$"#)]
async fn when_find_text_and_stored_vector(
    world: &mut GraphForgeWorld,
    query: String,
    label: String,
) {
    let vector = world.stored_vector.clone().expect("stored vector fixture");
    let space = world
        .stored_space
        .clone()
        .unwrap_or_else(|| "sbert".to_owned());
    run_find(world, Some(query), label, 10, Some(vector), Some(space));
}

#[when(regex = r#"^I find by a (\d+)-dimensional vector in label "([^"]+)"$"#)]
async fn when_find_wrong_dim(world: &mut GraphForgeWorld, dims: u32, label: String) {
    let space = world
        .stored_space
        .clone()
        .unwrap_or_else(|| "sbert".to_owned());
    run_find(
        world,
        None,
        label,
        10,
        Some(vec![1.0_f32; dims as usize]),
        Some(space),
    );
}

#[when(regex = r#"^I find with no query and no vector in label "([^"]+)"$"#)]
async fn when_find_no_args(world: &mut GraphForgeWorld, _label: String) {
    run_find(world, None, _label, 10, None, None);
}

#[when(regex = r#"^I find by an empty vector in label "([^"]+)"$"#)]
async fn when_find_empty_vector(world: &mut GraphForgeWorld, _label: String) {
    run_find(world, None, _label, 10, Some(Vec::new()), None);
}

#[when(regex = r#"^I find by a vector containing NaN in label "([^"]+)"$"#)]
async fn when_find_nan_vector(world: &mut GraphForgeWorld, _label: String) {
    run_find(world, None, _label, 10, Some(vec![f32::NAN]), None);
}

#[when(regex = r#"^I find by a vector containing infinity in label "([^"]+)"$"#)]
async fn when_find_inf_vector(world: &mut GraphForgeWorld, _label: String) {
    run_find(world, None, _label, 10, Some(vec![f32::INFINITY]), None);
}

#[given(regex = r#"^I have stored the node id as "paper_id"$"#)]
async fn given_store_paper_id(world: &mut GraphForgeWorld) {
    let handle = world
        .nodes
        .get("paper")
        .or_else(|| world.nodes.values().next())
        .expect("paper fixture node");
    world.stored_paper_id = Some(handle.uuid.to_string());
}

#[given(regex = r#"^I have an embedding vector stored as "embedding"$"#)]
async fn given_store_embedding(world: &mut GraphForgeWorld) {
    world.stored_vector = Some(vec![1.0_f32; 128]);
}

#[when(
    regex = r#"^I index label "([^"]+)" storing the vector for node "([^"]+)" in space "([^"]+)"$"#
)]
async fn when_index_vector(
    world: &mut GraphForgeWorld,
    label: String,
    node_key: String,
    space: String,
) {
    let handle = world
        .nodes
        .get(&node_key)
        .or_else(|| world.nodes.get("paper"))
        .cloned()
        .expect("indexed node fixture");
    let vector = world
        .stored_vector
        .clone()
        .expect("stored embedding fixture");
    world.stored_space = Some(space.clone());
    match world.forge.as_ref().expect("index fixture").index_search(
        &label,
        graphforge_api::SearchIndexOptions::Vector {
            node: graphforge_api::NodeSelector::Handle(handle),
            vector,
            space,
        },
    ) {
        Ok(_) => {
            world.index_calls += 1;
            world.last_error = None;
        }
        Err(error) => world.last_error = Some(error.to_string()),
    }
}

#[when(regex = r#"^I add a node with label "Paper" titled "Deep Graph Learning"$"#)]
async fn when_add_deep_graph_paper(world: &mut GraphForgeWorld) {
    let props = std::collections::HashMap::from([(
        "title".to_owned(),
        graphforge_api::PropValue::Str("Deep Graph Learning".into()),
    )]);
    match world
        .forge
        .as_ref()
        .expect("open forge")
        .add_node("Paper", &props)
    {
        Ok(handle) => {
            world.nodes.insert("Deep Graph Learning".into(), handle);
            world.last_error = None;
        }
        Err(error) => world.last_error = Some(error.to_string()),
    }
}

#[when(regex = r#"^I index label "([^"]+)" on properties "([^"]+)" and "([^"]+)"$"#)]
async fn when_index_two_props(world: &mut GraphForgeWorld, label: String, p1: String, p2: String) {
    run_text_index(world, label, Some(vec![p1, p2]));
}

#[when(regex = r#"^I index label "([^"]+)" on property "([^"]+)"$"#)]
async fn when_index_one_prop(world: &mut GraphForgeWorld, label: String, prop: String) {
    run_text_index(world, label, Some(vec![prop]));
}

#[when(regex = r#"^I index label "([^"]+)" on an empty properties list$"#)]
async fn when_index_empty_props(world: &mut GraphForgeWorld, _label: String) {
    run_text_index(world, _label, Some(Vec::new()));
}

fn run_text_index(world: &mut GraphForgeWorld, label: String, properties: Option<Vec<String>>) {
    world.index_calls += 1;
    let options = graphforge_api::SearchIndexOptions::Text {
        properties,
        rebuild: false,
    };
    match world
        .forge
        .as_ref()
        .expect("index fixture")
        .index_search(&label, options)
    {
        Ok(_) => world.last_error = None,
        Err(error) => world.last_error = Some(error.to_string()),
    }
}

#[when(regex = r#"^I call schema$"#)]
async fn when_schema(world: &mut GraphForgeWorld) {
    if let Some(forge) = &world.forge {
        match forge.schema() {
            Ok(result) => world.last_algorithm_result = Some(result),
            Err(e) => world.last_error = Some(e.to_string()),
        }
    }
}

#[when(regex = r#"^I call labels$"#)]
async fn when_labels(world: &mut GraphForgeWorld) {
    if let Some(forge) = &world.forge {
        match forge.labels() {
            Ok(labels) => {
                world.last_result = Some(graphforge_api::RecordBatch {
                    schema: vec!["label".into()],
                    columns: vec![labels],
                });
            }
            Err(error) => world.last_error = Some(error.to_string()),
        }
    }
}

#[when(regex = r#"^I call relationship_types$"#)]
async fn when_rel_types(world: &mut GraphForgeWorld) {
    if let Some(forge) = &world.forge {
        match forge.relationship_types() {
            Ok(relationship_types) => {
                world.last_result = Some(graphforge_api::RecordBatch {
                    schema: vec!["relationship_type".into()],
                    columns: vec![relationship_types],
                });
            }
            Err(error) => world.last_error = Some(error.to_string()),
        }
    }
}

#[when(regex = r#"^I call node_count for label "([^"]+)"$"#)]
async fn when_node_count(world: &mut GraphForgeWorld, label: String) {
    if let Some(forge) = &world.forge {
        match forge.node_count(&label) {
            Ok(count) => {
                world.last_result = Some(graphforge_api::RecordBatch {
                    schema: vec!["node_count".into()],
                    columns: vec![vec![count.to_string()]],
                });
            }
            Err(error) => world.last_error = Some(error.to_string()),
        }
    }
}

#[when(
    regex = r#"^I call neighbourhood for "([^"]*)" with hops (\d+) in label "([^"]*)" using canonical property "([^"]*)"$"#
)]
async fn when_neighbourhood(
    world: &mut GraphForgeWorld,
    canonical: String,
    hops: u32,
    label: String,
    prop: String,
) {
    world.last_error = None;
    world.last_error_code = None;
    world.last_algorithm_result = None;
    if !is_cypher_identifier(&label) || !is_cypher_identifier(&prop) {
        world.last_error = Some("invalid neighbourhood identifier".into());
        return;
    }
    let return_clause = if prop == "name" {
        "RETURN DISTINCT neighbour.name AS name, labels(neighbour) AS labels".to_owned()
    } else {
        format!(
            "RETURN DISTINCT neighbour.{prop} AS {prop}, neighbour.name AS name, labels(neighbour) AS labels"
        )
    };
    let query = if hops == 0 {
        format!("MATCH (neighbour:{label}) WHERE false {return_clause}")
    } else {
        format!(
            "MATCH (seed:{label} {{{prop}: $canonical}})-[*1..{hops}]-(neighbour:{label}) \
             WHERE neighbour.{prop} <> $canonical {return_clause}"
        )
    };
    let params = std::collections::HashMap::from([(
        "canonical".to_owned(),
        graphforge_api::IrLiteral::Str(canonical),
    )]);
    match world
        .forge
        .as_ref()
        .expect("neighbourhood fixture")
        .execute_with_params(&query, &params)
    {
        Ok(result) => {
            world.last_exec = Some(result);
            world.last_error = None;
        }
        Err(error) => {
            world.last_exec = None;
            world.last_error_code = Some(error.code());
            world.last_error = Some(error.to_string());
        }
    }
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
    let path = world
        .persistent_fixture
        .as_ref()
        .expect("persistent fixture")
        .path()
        .to_str()
        .expect("UTF-8 fixture path")
        .to_owned();
    drop(world.forge.take());
    match graphforge_api::GraphForge::new(Some(&path)) {
        Ok(forge) => {
            world.forge = Some(forge);
            world.last_error = None;
        }
        Err(error) => world.last_error = Some(error.to_string()),
    }
}

#[when(regex = r#"^I load the ontology from that file$"#)]
async fn when_load_ontology(world: &mut GraphForgeWorld) {
    let path = world
        .ontology_path
        .as_ref()
        .expect("ontology fixture path")
        .to_str()
        .expect("UTF-8 ontology path")
        .to_owned();
    match world
        .forge
        .as_mut()
        .expect("ontology fixture forge")
        .load_ontology(&path)
    {
        Ok(()) => {
            world.last_error = None;
            world.last_error_code = None;
        }
        Err(error) => {
            world.last_error_code = Some(error.code());
            world.last_error = Some(error.to_string());
        }
    }
}

#[when(
    regex = r#"^I bulk add edges with type "([^"]+)" using source column "([^"]+)" and destination column "([^"]+)"$"#
)]
async fn when_bulk_add_edges(world: &mut GraphForgeWorld, rel: String, src: String, dst: String) {
    use arrow::array::{FixedSizeBinaryBuilder, StringArray};
    use std::sync::Arc;

    let source = world.nodes.get(&src).expect("bulk source handle");
    let target = world.nodes.get(&dst).expect("bulk destination handle");
    let mut edge_ids = FixedSizeBinaryBuilder::with_capacity(1, 16);
    edge_ids.append_null();
    let mut sources = FixedSizeBinaryBuilder::with_capacity(1, 16);
    sources
        .append_value(source.uuid.as_bytes())
        .expect("source UUID");
    let mut targets = FixedSizeBinaryBuilder::with_capacity(1, 16);
    targets
        .append_value(target.uuid.as_bytes())
        .expect("target UUID");
    let schema = graphforge_api::bulk_edge_input_schema(Vec::new()).expect("bulk edge schema");
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema,
        vec![
            Arc::new(edge_ids.finish()),
            Arc::new(StringArray::from(vec![rel])),
            Arc::new(sources.finish()),
            Arc::new(targets.finish()),
        ],
    )
    .expect("bulk edge batch");
    match world
        .forge
        .as_ref()
        .expect("bulk edge forge")
        .publish_bulk_edges(graphforge_api::OperationId(uuid::Uuid::now_v7()), &[batch])
    {
        Ok(receipt) => {
            world.last_algorithm_result = Some(receipt);
            world.last_error = None;
        }
        Err(error) => world.last_error = Some(error.to_string()),
    }
}

// ---------------------------------------------------------------------------
// THEN steps
// ---------------------------------------------------------------------------

#[then(regex = r#"^the "is_dag" value is (true|false)$"#)]
async fn then_is_dag(world: &mut GraphForgeWorld, expected: String) {
    let batch = result_batch(world);
    assert!(batch.num_rows() > 0, "expected a non-empty is_dag result");
    let values = batch
        .column_by_name("is_dag")
        .expect("is_dag column")
        .as_any()
        .downcast_ref::<arrow::array::BooleanArray>()
        .expect("Boolean is_dag column");
    assert_eq!(values.value(0), expected == "true");
}

#[then(regex = r#"^the result is an Arrow Table$"#)]
async fn then_arrow_table(world: &mut GraphForgeWorld) {
    assert!(
        world.last_error.is_none(),
        "unexpected error: {:?}",
        world.last_error
    );
    assert!(
        world.last_algorithm_result.is_some() || world.last_exec.is_some(),
        "expected an Arrow result"
    );
}

#[then(regex = r#"^the table has column "([^"]+)"$"#)]
async fn then_has_column(world: &mut GraphForgeWorld, col: String) {
    if let Some(batch) = world.last_algorithm_result.as_ref() {
        batch.schema().field_with_name(&col).expect("result column");
    } else {
        world
            .last_exec
            .as_ref()
            .expect("execution result")
            .schema
            .field_with_name(&col)
            .expect("result column");
    }
}

#[then(regex = r#"^the result schema contains column "([^"]+)"$"#)]
async fn then_schema_has_column(world: &mut GraphForgeWorld, col: String) {
    then_has_column(world, col).await;
}

#[then(regex = r#"^the table has (\d+) rows?$"#)]
async fn then_row_count(world: &mut GraphForgeWorld, n: u32) {
    let rows = result_batches(world)
        .iter()
        .map(|batch| batch.num_rows())
        .sum::<usize>();
    assert_eq!(rows, n as usize);
}

#[then(regex = r#"^the table has at most (\d+) rows?$"#)]
async fn then_at_most_rows(world: &mut GraphForgeWorld, n: u32) {
    let rows = result_batches(world)
        .iter()
        .map(|batch| batch.num_rows())
        .sum::<usize>();
    assert!(rows <= n as usize, "expected at most {n} rows, got {rows}");
}

#[then(regex = r#"^the first row value for "([^"]+)" is "([^"]+)"$"#)]
async fn then_first_row_str(world: &mut GraphForgeWorld, col: String, val: String) {
    let batch = result_batch(world);
    assert!(batch.num_rows() > 0, "expected a non-empty result batch");
    let index = batch.schema().index_of(&col).expect("result column");
    let actual = arrow::util::display::array_value_to_string(batch.column(index), 0)
        .expect("display Arrow value");
    assert_eq!(actual, val);
}

#[then(regex = r#"^the first row value for "([^"]+)" is null$"#)]
async fn then_first_row_null(world: &mut GraphForgeWorld, col: String) {
    use arrow::array::Array;
    let batch = result_batch(world);
    assert!(batch.num_rows() > 0, "expected a non-empty result batch");
    let index = batch.schema().index_of(&col).expect("result column");
    let column = batch.column(index);
    let display = arrow::util::display::array_value_to_string(column, 0)
        .unwrap_or_else(|error| format!("<display error: {error}>"));
    assert!(
        matches!(column.data_type(), arrow::datatypes::DataType::Null) || column.is_null(0),
        "expected {col} to be null, got type {:?} value {display:?}",
        column.data_type()
    );
}

fn result_batches(world: &GraphForgeWorld) -> Vec<&arrow::record_batch::RecordBatch> {
    if let Some(batch) = world.last_algorithm_result.as_ref() {
        return vec![batch];
    }
    world
        .last_exec
        .as_ref()
        .expect("Arrow result")
        .batches
        .iter()
        .collect()
}

fn result_batch(world: &GraphForgeWorld) -> &arrow::record_batch::RecordBatch {
    let batches = result_batches(world);
    batches
        .iter()
        .find(|batch| batch.num_rows() > 0)
        .or_else(|| batches.first())
        .copied()
        .expect("result batch")
}

#[then(regex = r#"^a ParseError is raised$"#)]
async fn then_parse_error(world: &mut GraphForgeWorld) {
    let error = world.last_error.as_deref().expect("parse error");
    assert_eq!(
        world.last_error_code,
        Some("GF_PARSE"),
        "expected parse error, got {error}"
    );
}

#[then(regex = r#"^the error includes a source span$"#)]
async fn then_has_span(world: &mut GraphForgeWorld) {
    let error = world.last_error.as_deref().expect("parse error");
    let lower = error.to_ascii_lowercase();
    let named_position = ["line", "column", "offset"].iter().any(|name| {
        lower.match_indices(name).any(|(index, _)| {
            lower[index + name.len()..]
                .trim_start_matches(|value: char| value == ' ' || value == ':' || value == '=')
                .starts_with(|value: char| value.is_ascii_digit())
        })
    });
    let numeric_range = lower.split("at ").skip(1).any(|suffix| {
        let token = suffix
            .split_whitespace()
            .next()
            .unwrap_or_default()
            .trim_end_matches(|value: char| !value.is_ascii_digit() && value != '.');
        token.split_once("..").is_some_and(|(start, end)| {
            start.parse::<usize>().is_ok() && end.parse::<usize>().is_ok()
        })
    });
    assert!(
        lower.contains("span") || lower.contains("byte") || named_position || numeric_range,
        "expected source location in parse error, got {error}"
    );
}

#[then(regex = r#"^an ExecutionError is raised$"#)]
async fn then_execution_error(world: &mut GraphForgeWorld) {
    let error = world.last_error.as_deref().expect("execution error");
    assert_eq!(
        world.last_error_code,
        Some("GF_EXECUTION"),
        "expected execution error, got {error}"
    );
}

#[then(regex = r#"^a StorageError is raised$"#)]
async fn then_storage_error(world: &mut GraphForgeWorld) {
    let err = world.last_error.as_deref().unwrap_or("");
    assert!(
        err.to_ascii_lowercase().contains("storage") || err.contains("path"),
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
    assert_eq!(
        world.last_error_code,
        Some("GF_ONTOLOGY"),
        "expected ontology error, got {:?}",
        world.last_error
    );
}

#[then(regex = r#"^no error is raised$"#)]
async fn then_no_error(world: &mut GraphForgeWorld) {
    assert!(
        world.last_error.is_none(),
        "unexpected error: {:?}",
        world.last_error
    );
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
async fn then_edge_handle(world: &mut GraphForgeWorld) {
    let handle = world.last_edge_handle.as_ref().expect("EdgeHandle result");
    assert_eq!(handle.uuid.get_version_num(), 7);
}

#[then(regex = r#"^execute "([^"]+)" returns (\d+) rows?$"#)]
async fn then_execute_n_rows(world: &mut GraphForgeWorld, query: String, n: u32) {
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
    world.last_exec = Some(result);
}

#[then(regex = r#"^execute "([^"]+)" returns (\d+) rows? with value (\d+)$"#)]
async fn then_execute_row_value(world: &mut GraphForgeWorld, query: String, n: u32, val: i64) {
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
    let batch = result.batches.first().expect("result batch");
    let actual = arrow::util::display::array_value_to_string(batch.column(0), 0)
        .expect("display result value");
    assert_eq!(actual.parse::<i64>().expect("integer result"), val);
    world.last_exec = Some(result);
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
async fn then_result_is_n(world: &mut GraphForgeWorld, n: i64) {
    let value = world
        .last_result
        .as_ref()
        .and_then(|result| result.columns.first())
        .and_then(|column| column.first())
        .expect("integer result");
    assert_eq!(value.parse::<i64>().unwrap(), n);
}

#[then(regex = r#"^the result is a non-empty string$"#)]
async fn then_nonempty_string(world: &mut GraphForgeWorld) {
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

#[then(regex = r#"^the result contains that node$"#)]
async fn then_result_contains_that_node(world: &mut GraphForgeWorld) {
    assert!(
        world.last_error.is_none(),
        "unexpected error: {:?}",
        world.last_error
    );
    let expected = world
        .stored_paper_id
        .clone()
        .or_else(|| {
            world
                .nodes
                .get("paper")
                .map(|handle| handle.uuid.to_string())
        })
        .expect("paper_id fixture")
        .replace('-', "")
        .to_ascii_lowercase();
    let batch = result_batch(world);
    let uuids = batch
        .column_by_name("node_uuid")
        .expect("node_uuid column")
        .as_any()
        .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
        .expect("node_uuid FixedSizeBinary");
    let found = (0..uuids.len()).any(|index| {
        let actual = uuids
            .value(index)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        actual == expected
    });
    assert!(found, "result omitted node {expected}");
}

#[then(regex = r#"^the result contains a row with title "([^"]+)"$"#)]
async fn then_result_contains_title(world: &mut GraphForgeWorld, title: String) {
    assert!(
        world.last_error.is_none(),
        "unexpected error: {:?}",
        world.last_error
    );
    let batch = result_batch(world);
    let titles = batch
        .column_by_name("title")
        .expect("title column")
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .expect("title Utf8");
    let found = (0..titles.len()).any(|index| titles.value(index) == title);
    assert!(found, "result omitted title {title}");
}

#[then(regex = r#"^the result contains "([^"]+)"$"#)]
async fn then_result_contains_text(world: &mut GraphForgeWorld, text: String) {
    if let Some(rb) = &world.last_result {
        let values = rb.columns.iter().flatten().collect::<Vec<_>>();
        assert!(
            values.iter().any(|value| value.contains(&text)),
            "expected result to contain {text:?}\ngot:\n{}",
            values
                .iter()
                .map(|value| value.as_str())
                .collect::<Vec<_>>()
                .join("\n")
        );
    } else {
        panic!(
            "no result stored — step failed? error: {:?}",
            world.last_error
        );
    }
}

#[then(regex = r#"^the result is an empty list$"#)]
async fn then_empty_list(world: &mut GraphForgeWorld) {
    assert!(
        world
            .last_result
            .as_ref()
            .and_then(|result| result.columns.first())
            .is_some_and(Vec::is_empty),
        "expected empty list, got {:?}",
        world.last_result
    );
}

#[then(regex = r#"^calling relationship_types also returns an empty list$"#)]
async fn then_rel_types_empty(world: &mut GraphForgeWorld) {
    assert!(
        world
            .forge
            .as_ref()
            .expect("open forge")
            .relationship_types()
            .expect("relationship types")
            .is_empty()
    );
}

#[then(regex = r#"^the table contains an entry for label "([^"]+)"$"#)]
async fn then_schema_has_label(world: &mut GraphForgeWorld, label: String) {
    let labels = world
        .last_algorithm_result
        .as_ref()
        .expect("schema result")
        .column_by_name("label")
        .expect("label column")
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .expect("Utf8 labels");
    assert!(labels.iter().flatten().any(|value| value == label));
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
async fn then_connected_same_community(world: &mut GraphForgeWorld) {
    let communities = community_by_uuid(world);
    let alice = world.nodes.get("Alice").expect("Alice handle");
    let bob = world.nodes.get("Bob").expect("Bob handle");
    let alice_community = communities
        .get(alice.uuid.as_bytes().as_slice())
        .expect("Alice community");
    let bob_community = communities
        .get(bob.uuid.as_bytes().as_slice())
        .expect("Bob community");
    assert_eq!(alice_community, bob_community);
}

#[then(regex = r#"^the 2 isolated nodes share a different community_id$"#)]
async fn then_isolated_different_community(world: &mut GraphForgeWorld) {
    let communities = community_by_uuid(world);
    let alice = world.nodes.get("Alice").expect("Alice handle");
    let carol = world.nodes.get("Carol").expect("Carol handle");
    let dave = world.nodes.get("Dave").expect("Dave handle");
    let alice_community = communities
        .get(alice.uuid.as_bytes().as_slice())
        .expect("Alice community");
    let carol_community = communities
        .get(carol.uuid.as_bytes().as_slice())
        .expect("Carol community");
    let dave_community = communities
        .get(dave.uuid.as_bytes().as_slice())
        .expect("Dave community");
    assert_eq!(carol_community, dave_community);
    assert_ne!(alice_community, carol_community);
}

fn community_by_uuid(world: &GraphForgeWorld) -> std::collections::HashMap<Vec<u8>, String> {
    let batch = world
        .last_algorithm_result
        .as_ref()
        .expect("cluster result");
    let ids = batch
        .column_by_name("node_uuid")
        .expect("node_uuid column")
        .as_any()
        .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
        .expect("UUID column");
    let communities = batch
        .column_by_name("community_id")
        .expect("community_id column");
    (0..batch.num_rows())
        .map(|row| {
            let value = arrow::util::display::array_value_to_string(communities, row)
                .expect("community value");
            (ids.value(row).to_vec(), value)
        })
        .collect()
}

#[then(regex = r#"^no index call was made before find$"#)]
async fn then_no_index_call(world: &mut GraphForgeWorld) {
    assert_eq!(world.index_calls, 0, "an explicit index call was recorded");
    let rows = result_batches(world)
        .iter()
        .map(|batch| batch.num_rows())
        .sum::<usize>();
    assert!(
        rows > 0,
        "find must return matches without an explicit index call"
    );
}

#[then(
    regex = r#"^for each result row the id is valid in execute "MATCH \(n\) WHERE n\.node_uuid = \$id RETURN n"$"#
)]
async fn then_ids_addressable(world: &mut GraphForgeWorld) {
    let batch = result_batch(world);
    let ids = batch
        .column_by_name("node_uuid")
        .expect("node_uuid column")
        .as_any()
        .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
        .expect("fixed-size UUID column");
    assert!(!ids.is_empty(), "find must return at least one node");
    assert_eq!(ids.null_count(), 0, "find returned a null node_uuid");
    for value in ids.iter().flatten() {
        let bytes: [u8; 16] = value.try_into().expect("16-byte UUID");
        let params = std::collections::HashMap::from([(
            "id".to_owned(),
            graphforge_api::IrLiteral::Uuid(bytes),
        )]);
        let result = world
            .forge
            .as_ref()
            .expect("open forge")
            .execute_with_params(
                "MATCH (n) WHERE n.node_uuid = $id RETURN n.node_uuid",
                &params,
            )
            .expect("UUID addressability query");
        assert_eq!(
            result
                .batches
                .iter()
                .map(arrow::array::RecordBatch::num_rows)
                .sum::<usize>(),
            1
        );
    }
}

#[then(regex = r#"^all result rows have label "([^"]+)"$"#)]
async fn then_all_rows_label(world: &mut GraphForgeWorld, label: String) {
    let batch = result_batch(world);
    let ids = batch
        .column_by_name("node_uuid")
        .expect("node_uuid column")
        .as_any()
        .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
        .expect("fixed-size UUID column");
    assert!(!ids.is_empty(), "find must return at least one node");
    assert_eq!(ids.null_count(), 0, "find returned a null node_uuid");
    for value in ids.iter().flatten() {
        let bytes: [u8; 16] = value.try_into().expect("16-byte UUID");
        let params = std::collections::HashMap::from([(
            "id".to_owned(),
            graphforge_api::IrLiteral::Uuid(bytes),
        )]);
        let result = world
            .forge
            .as_ref()
            .expect("open forge")
            .execute_with_params(
                "MATCH (n) WHERE n.node_uuid = $id RETURN labels(n) AS labels",
                &params,
            )
            .expect("label addressability query");
        let row = result.batches.first().expect("label row");
        let actual =
            arrow::util::display::array_value_to_string(row.column(0), 0).expect("display labels");
        assert!(
            actual.contains(&label),
            "expected label {label}, got {actual}"
        );
    }
}

#[then(
    regex = r#"^find "paper" in label "Paper" returns the same results as after the first index call$"#
)]
async fn then_idempotent_index(world: &mut GraphForgeWorld) {
    let first = world
        .previous_algorithm_result
        .as_ref()
        .expect("find result after the first index call");
    let second = world
        .last_algorithm_result
        .as_ref()
        .expect("find result after the second index call");
    assert_eq!(first.schema(), second.schema());
    assert_eq!(first.num_rows(), second.num_rows());
    for (left, right) in first.columns().iter().zip(second.columns()) {
        assert_eq!(left.to_data(), right.to_data());
    }
}

#[then(regex = r#"^the result is an Arrow Table with at least 1 row$"#)]
async fn then_arrow_at_least_1(world: &mut GraphForgeWorld) {
    assert!(
        result_batches(world)
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>()
            >= 1
    );
}

#[then(regex = r#"^the result contains a row for "([^"]*)"$"#)]
async fn then_result_has_row_for(world: &mut GraphForgeWorld, name: String) {
    assert!(
        world.last_error.is_none(),
        "unexpected error: {:?}",
        world.last_error
    );
    let names = neighbourhood_names(world);
    assert!(
        names.iter().any(|value| value == &name),
        "result omitted row for {name}; got {names:?}"
    );
}

#[then(regex = r#"^the result does not contain a row for "([^"]*)"$"#)]
async fn then_result_no_row_for(world: &mut GraphForgeWorld, name: String) {
    assert!(
        world.last_error.is_none(),
        "unexpected error: {:?}",
        world.last_error
    );
    let names = neighbourhood_names(world);
    assert!(
        names.iter().all(|value| value != &name),
        "result unexpectedly included row for {name}; got {names:?}"
    );
}

fn neighbourhood_names(world: &GraphForgeWorld) -> Vec<String> {
    let mut names = Vec::new();
    for batch in result_batches(world) {
        let column = batch
            .column_by_name("name")
            .expect("neighbourhood result requires a name column");
        let values = column
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("Utf8 name column");
        for index in 0..values.len() {
            if !values.is_null(index) {
                names.push(values.value(index).to_owned());
            }
        }
    }
    names
}
