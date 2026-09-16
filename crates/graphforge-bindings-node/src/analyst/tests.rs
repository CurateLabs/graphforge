use super::*;
use crate::GraphForge;
use crate::NodeSelector;

#[test]
fn steiner_terminals_are_checked_and_preserve_input_order() {
    let first = "018f0f4e-7b8c-7000-8000-000000000002".to_owned();
    let second = "018f0f4e-7b8c-7000-8000-000000000001".to_owned();
    let terminals = parse_terminal_uuids(&[first.clone(), second.clone()]).unwrap();

    let NodeSelector::Uuid(first_uuid) = NodeSelector::uuid(&first).unwrap() else {
        unreachable!()
    };
    let NodeSelector::Uuid(second_uuid) = NodeSelector::uuid(&second).unwrap() else {
        unreachable!()
    };
    assert_eq!(
        terminals,
        vec![*first_uuid.as_bytes(), *second_uuid.as_bytes()]
    );

    assert_eq!(
        parse_terminal_uuids(&["not-a-uuid".to_owned()])
            .unwrap_err()
            .status,
        "ValidationError"
    );
    assert_eq!(
        parse_terminal_uuids(&[first.to_uppercase()])
            .unwrap_err()
            .status,
        "ValidationError"
    );
}

#[test]
fn source_free_minimum_steiner_reaches_active_rust_handler() {
    let graph = GraphForge::new(None, None).unwrap();
    // Construction methods require a napi Env for TypeError coercion; unit
    // tests seed fixtures through the Rust facade instead.
    let (first, second) = {
        let engine = graph.open_guard().unwrap();
        let first = engine
            .add_node("Person", &Default::default())
            .expect("fixture node");
        let second = engine
            .add_node("Person", &Default::default())
            .expect("fixture node");
        (first.uuid.to_string(), second.uuid.to_string())
    };

    let result = graph.paths(
        None,
        None,
        "min_steiner_tree".into(),
        None,
        Some(false),
        Some(1),
        None,
        None,
        None,
        None,
        Some(vec![first, second]),
        None,
        None,
        None,
    );
    let Err(error) = result else {
        panic!("disconnected minimum Steiner input must fail")
    };

    assert_eq!(error.status, "ExecutionError");
    assert!(
        error
            .reason
            .contains("minimum Steiner tree is undefined: mandatory terminals are disconnected")
    );
}
