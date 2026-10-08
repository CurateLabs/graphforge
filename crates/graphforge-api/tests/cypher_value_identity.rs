//! Node, relationship and path identity in equality and list membership
//! (#1887 D1/D12): an entity compared with the same entity read as a list
//! element, a collected value or an unwound value is equal by identity.

use arrow::array::Array;
use arrow::util::display::array_value_to_string;
use graphforge_api::GraphForge;

/// Every row of `column`, rendered; `None` for a null cell.
fn column(gf: &GraphForge, query: &str, column: &str) -> Vec<Option<String>> {
    let result = gf
        .execute(query)
        .unwrap_or_else(|error| panic!("{query}: {error}"));
    let mut values = Vec::new();
    for batch in &result.batches {
        let array = batch
            .column_by_name(column)
            .unwrap_or_else(|| panic!("{query}: no column {column}"));
        for row in 0..array.len() {
            values.push(
                (!array.is_null(row)).then(|| array_value_to_string(array, row).expect("render")),
            );
        }
    }
    values
}

fn one(gf: &GraphForge, query: &str, name: &str) -> Option<String> {
    let values = column(gf, query, name);
    assert_eq!(values.len(), 1, "{query}: {values:?}");
    values.into_iter().next().expect("one row")
}

fn chain() -> GraphForge {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (a:P {id: 1})-[:K {w: 1}]->(b:P {id: 2}), (b)-[:K {w: 2}]->(c:P {id: 3})")
        .expect("create chain");
    gf
}

/// A rendered boolean.
fn b(value: bool) -> String {
    value.to_string()
}

#[test]
fn node_is_a_member_of_a_list_literal_holding_it() {
    let gf = chain();
    assert_eq!(
        one(&gf, "MATCH (a:P {id: 1}) RETURN a IN [a] AS x", "x"),
        Some(b(true))
    );
    assert_eq!(
        one(
            &gf,
            "MATCH (a:P {id: 1}), (b:P {id: 2}) WITH a, b, [a, b] AS xs RETURN a IN xs AS x",
            "x"
        ),
        Some(b(true))
    );
    assert_eq!(
        one(
            &gf,
            "MATCH (a:P {id: 1}), (b:P {id: 2}) RETURN a IN [b] AS x",
            "x"
        ),
        Some(b(false))
    );
    assert_eq!(
        one(
            &gf,
            "MATCH (a:P {id: 1}), (b:P {id: 2}) RETURN a NOT IN [b] AS x",
            "x"
        ),
        Some(b(true))
    );
}

#[test]
fn node_is_a_member_of_a_collected_list() {
    let gf = chain();
    assert_eq!(
        column(
            &gf,
            "MATCH (a:P) WHERE a.id < 3 WITH collect(a) AS xs \
             MATCH (b:P) RETURN b IN xs AS x ORDER BY b.id",
            "x"
        ),
        vec![Some(b(true)), Some(b(true)), Some(b(false))]
    );
    // The filter form a query uses to keep collected entities.
    assert_eq!(
        column(
            &gf,
            "MATCH (a:P) WHERE a.id <> 2 WITH collect(a) AS xs \
             MATCH (b:P) WHERE b IN xs RETURN b.id AS id ORDER BY id",
            "id"
        ),
        vec![Some("1".into()), Some("3".into())]
    );
}

#[test]
fn relationship_is_a_member_of_list_literals_and_collected_lists() {
    let gf = chain();
    assert_eq!(
        column(&gf, "MATCH ()-[r:K]->() RETURN r IN [r] AS x", "x"),
        vec![Some(b(true)), Some(b(true))]
    );
    assert_eq!(
        column(
            &gf,
            "MATCH ()-[r:K {w: 1}]->() WITH collect(r) AS rs \
             MATCH ()-[s:K]->() RETURN s IN rs AS x ORDER BY s.w",
            "x"
        ),
        vec![Some(b(true)), Some(b(false))]
    );
}

#[test]
fn path_is_a_member_of_list_literals_and_collected_lists() {
    let gf = chain();
    assert_eq!(
        one(&gf, "MATCH p = (:P {id: 1})-->() RETURN p IN [p] AS x", "x"),
        Some(b(true))
    );
    assert_eq!(
        column(
            &gf,
            "MATCH p = (:P {id: 1})-->() WITH collect(p) AS ps \
             MATCH q = (s:P)-->() RETURN q IN ps AS x ORDER BY s.id",
            "x"
        ),
        vec![Some(b(true)), Some(b(false))]
    );
}

#[test]
fn entity_equality_holds_across_value_shapes() {
    let gf = chain();
    // An unwound list element against the bound variable.
    assert_eq!(
        column(
            &gf,
            "MATCH (a:P) WITH a, [a] AS xs UNWIND xs AS x \
             RETURN x = a AS eq, x <> a AS ne ORDER BY a.id",
            "eq"
        ),
        vec![Some(b(true)), Some(b(true)), Some(b(true))]
    );
    assert_eq!(
        column(
            &gf,
            "MATCH (a:P), (b:P) WITH a, b, [b] AS xs UNWIND xs AS x \
             WITH a, b, x WHERE x <> a RETURN count(*) AS n",
            "n"
        ),
        vec![Some("6".into())]
    );
    assert_eq!(
        one(
            &gf,
            "MATCH ()-[r:K {w: 1}]->() WITH r, [r] AS rs UNWIND rs AS x RETURN x = r AS eq",
            "eq"
        ),
        Some(b(true))
    );
    // An unwound entity carried through WITH, and the reverse operand order.
    assert_eq!(
        column(
            &gf,
            "MATCH (a:P) UNWIND [a] AS x WITH a, x RETURN a = x AS eq, x IN [a] AS m ORDER BY a.id",
            "eq"
        ),
        vec![Some(b(true)), Some(b(true)), Some(b(true))]
    );
    assert_eq!(
        column(
            &gf,
            "MATCH (a:P) UNWIND [a] AS x WITH a, x RETURN x IN [a] AS m ORDER BY a.id",
            "m"
        ),
        vec![Some(b(true)), Some(b(true)), Some(b(true))]
    );
    assert_eq!(
        one(
            &gf,
            "MATCH (a:P {id: 1}), (b:P {id: 2}) UNWIND [b] AS x RETURN x = a AS eq, x <> a AS ne",
            "ne"
        ),
        Some(b(true))
    );
    // Lists and maps holding entities compare their elements by identity.
    assert_eq!(
        one(
            &gf,
            "MATCH (a:P {id: 1}) MATCH (b:P {id: 1}) RETURN [a] = [b] AS eq",
            "eq"
        ),
        Some(b(true))
    );
    assert_eq!(
        one(
            &gf,
            "MATCH (a:P {id: 1}), (b:P {id: 2}) RETURN {k: a} = {k: b} AS eq",
            "eq"
        ),
        Some(b(false))
    );
    // A node never equals a relationship.
    assert_eq!(
        one(
            &gf,
            "MATCH (a:P {id: 1})-[r:K]->() RETURN a IN [r] AS x",
            "x"
        ),
        Some(b(false))
    );
}

#[test]
fn identity_ignores_the_shape_an_entity_was_read_with() {
    let gf = chain();
    gf.execute("CREATE (:Q {name: 'q'})-[:L {note: 'n'}]->(:Q {name: 'r'})")
        .expect("create a second label with other properties");
    // `a` is read through its label, `b` through an unlabelled scan whose value
    // carries every label's properties: one node, two value shapes.
    assert_eq!(
        one(
            &gf,
            "MATCH (a:P {id: 1}) MATCH (b) WHERE b.id = 1 \
             RETURN [a] = [b] AS lists, {k: a} = {k: b} AS maps, a IN [b] AS member",
            "lists"
        ),
        Some(b(true))
    );
    assert_eq!(
        one(
            &gf,
            "MATCH (a:P {id: 1}) MATCH (b) WHERE b.id = 1 WITH collect(a) AS xs, b \
             RETURN b IN xs AS member",
            "member"
        ),
        Some(b(true))
    );
    assert_eq!(
        one(
            &gf,
            "MATCH ()-[r:K {w: 1}]->() MATCH ()-[s]->() WHERE s.w = 1 RETURN [r] = [s] AS eq",
            "eq"
        ),
        Some(b(true))
    );
}

#[test]
fn distinct_entities_collapse_by_identity() {
    let gf = chain();
    assert_eq!(
        one(
            &gf,
            "MATCH (a:P) WITH a, [a, a] AS xs UNWIND xs AS x RETURN count(DISTINCT x) AS n",
            "n"
        ),
        Some("3".into())
    );
    assert_eq!(
        one(
            &gf,
            "MATCH (a:P), (b:P) WITH collect(DISTINCT a) AS xs RETURN size(xs) AS n",
            "n"
        ),
        Some("3".into())
    );
    assert_eq!(
        column(
            &gf,
            "MATCH (a:P), (b:P) RETURN DISTINCT a.id AS id ORDER BY id",
            "id"
        ),
        vec![Some("1".into()), Some("2".into()), Some("3".into())]
    );
}

#[test]
fn an_unwound_entity_is_returned_as_the_entity() {
    let gf = chain();
    let node = one(&gf, "MATCH (a:P {id: 1}) UNWIND [a] AS x RETURN x", "x").expect("a node value");
    let direct = one(&gf, "MATCH (a:P {id: 1}) RETURN a", "a").expect("a node value");
    assert_eq!(
        node, direct,
        "UNWIND [a] AS x RETURN x returns the node, not its UUID"
    );
    assert_eq!(
        one(
            &gf,
            "MATCH (a:P {id: 1}) UNWIND [a] AS x WITH x RETURN x",
            "x"
        ),
        Some(direct)
    );
    assert_eq!(
        column(
            &gf,
            "MATCH (a:P) WITH collect(a) AS xs UNWIND xs AS x \
             RETURN labels(x) AS l, x.id AS id ORDER BY id",
            "l"
        ),
        vec![Some("[P]".to_owned()); 3]
    );
    let rel = one(
        &gf,
        "MATCH ()-[r:K {w: 1}]->() UNWIND [r] AS x RETURN x",
        "x",
    );
    let direct_rel = one(&gf, "MATCH ()-[r:K {w: 1}]->() RETURN r", "r");
    assert_eq!(rel, direct_rel);
    assert_eq!(
        one(
            &gf,
            "MATCH ()-[r:K {w: 1}]->() UNWIND [r] AS x RETURN type(x) AS t",
            "t"
        ),
        Some("K".to_owned())
    );
    // An unwound node still anchors a pattern.
    assert_eq!(
        one(
            &gf,
            "MATCH (a:P {id: 1}) UNWIND [a] AS x MATCH (x)-[:K]->(y) RETURN y.id AS id",
            "id"
        ),
        Some("2".to_owned())
    );
}

#[test]
fn unwinding_entities_beside_a_column_named_like_a_property() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (:P {id: 1, l: 'x'}), (:P {id: 2, l: 'y'})")
        .expect("create nodes");
    assert_eq!(
        column(
            &gf,
            "MATCH (a:P) WITH collect(a) AS l UNWIND l AS x \
             RETURN x.l AS v, size(l) AS n ORDER BY v",
            "v"
        ),
        vec![Some("x".to_owned()), Some("y".to_owned())]
    );
    assert_eq!(
        column(
            &gf,
            "MATCH (a:P) WITH collect(a) AS l UNWIND l AS x WITH x, l \
             RETURN size(l) AS n, x.id AS id ORDER BY id",
            "n"
        ),
        vec![Some("2".to_owned()), Some("2".to_owned())]
    );
}
