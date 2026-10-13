//! A hop's relationship-only predicate filters the traversed relationships
//! before any destination node is read (#1984).
//!
//! `MATCH (h)-[r:LINK]->(b) WHERE r.weight > $x` keeps a fraction of the hub's
//! relationships. The expansion evaluates the conjunct on the relationships'
//! properties, which the edge-property index supplies by edge id, and reads the
//! destination rows of the relationships that pass only. The filter above the
//! hop stays, so a statement returns what it returned without the push-down.
//!
//! The fixture is a hub: node 0 links to every other node, and relationship `i`
//! (to node `i`) has weight `i`, except that every eighth weight is `NULL`.
//! The gate counts the destination rows the expansion operator reports having
//! read, never time.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryBuilder, Int64Array, Int64Builder, RecordBatch, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use graphforge_api::{
    CONSTRUCTION_EDGE_SCHEMA, CONSTRUCTION_NODE_SCHEMA, ExecutionResourcePolicy, ExplainStage,
    GraphConstructionBudgets, GraphForge, GraphForgeOptions,
};
use graphforge_core::uuid::Uuid;
use graphforge_ir::IrLiteral;

const WRITE_WINDOW: usize = 4 * 1024;
/// The hub's relationships, and so the destinations an unfiltered hop reads.
const SPOKES: usize = 8_000;
/// Relationships with a weight above this pass the predicate.
const THRESHOLD: i64 = 6_000;

fn node_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x1000_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
}

fn edge_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x2000_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
}

/// The weight of the hub's relationship to spoke `spoke` (1-based).
fn weight(spoke: usize) -> Option<i64> {
    (spoke % 8 != 0).then_some(spoke as i64)
}

fn construct(dir: &Path) {
    let nodes = SPOKES + 1;
    let mut fields = CONSTRUCTION_NODE_SCHEMA.fields().to_vec();
    fields.push(Arc::new(Field::new("ident", DataType::Int64, true)));
    let node_schema = Arc::new(Schema::new(fields));
    let mut edge_fields = CONSTRUCTION_EDGE_SCHEMA.fields().to_vec();
    edge_fields.push(Arc::new(Field::new("weight", DataType::Int64, true)));
    let edge_schema = Arc::new(Schema::new(edge_fields));
    let forge = GraphForge::new(Some(dir.to_str().expect("utf-8 path"))).unwrap();
    let mut session = forge
        .begin_graph_construction(GraphConstructionBudgets {
            max_batch_rows: WRITE_WINDOW,
            max_run_records: 4 * WRITE_WINDOW,
            ..GraphConstructionBudgets::default()
        })
        .unwrap();
    for start in (0..nodes).step_by(WRITE_WINDOW) {
        let end = (start + WRITE_WINDOW).min(nodes);
        let mut ids = FixedSizeBinaryBuilder::with_capacity(end - start, 16);
        let mut idents = Int64Builder::with_capacity(end - start);
        for node in start..end {
            ids.append_value(node_uuid(node).as_bytes()).unwrap();
            idents.append_value(node as i64);
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&node_schema),
            vec![
                Arc::new(ids.finish()) as ArrayRef,
                Arc::new(StringArray::from(vec!["Entity"; end - start])),
                Arc::new(idents.finish()),
            ],
        )
        .unwrap();
        session
            .append_nodes(&format!("nodes-{start}"), &batch)
            .unwrap();
    }
    for start in (1..=SPOKES).step_by(WRITE_WINDOW) {
        let end = (start + WRITE_WINDOW).min(SPOKES + 1);
        let rows = end - start;
        let mut ids = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut sources = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut targets = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut weights = Int64Builder::with_capacity(rows);
        for spoke in start..end {
            ids.append_value(edge_uuid(spoke).as_bytes()).unwrap();
            sources.append_value(node_uuid(0).as_bytes()).unwrap();
            targets.append_value(node_uuid(spoke).as_bytes()).unwrap();
            weights.append_option(weight(spoke));
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&edge_schema),
            vec![
                Arc::new(ids.finish()) as ArrayRef,
                Arc::new(StringArray::from(vec!["LINK"; rows])),
                Arc::new(sources.finish()),
                Arc::new(targets.finish()),
                Arc::new(weights.finish()),
            ],
        )
        .unwrap();
        session
            .append_edges(&format!("edges-{start}"), &batch)
            .unwrap();
    }
    session.seal_and_publish().unwrap();
}

fn open(project: &Path, target_partitions: usize) -> GraphForge {
    GraphForge::new_with_options(
        Some(project.to_str().expect("utf-8 path")),
        GraphForgeOptions {
            resource: ExecutionResourcePolicy {
                target_partitions: Some(target_partitions),
                ..ExecutionResourcePolicy::default()
            },
            ..GraphForgeOptions::default()
        },
    )
    .unwrap()
}

static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

struct Built {
    _root: tempfile::TempDir,
    project: std::path::PathBuf,
}

fn build() -> Built {
    let root = tempfile::tempdir().expect("project directory");
    let project = root.path().join("project");
    construct(&project);
    Built {
        _root: root,
        project,
    }
}

/// What a statement returned and what its hops read.
struct Run {
    batches: Vec<RecordBatch>,
    /// Destination node rows the expansion operators returned from storage,
    /// summed over the hops.
    node_rows: u64,
    /// Relationship rows the expansion operators returned from the edge file.
    edge_rows: u64,
}

impl Run {
    fn int_column(&self, name: &str) -> Vec<i64> {
        let mut values = Vec::new();
        for batch in &self.batches {
            let column = batch
                .column_by_name(name)
                .unwrap_or_else(|| panic!("column {name}"));
            let column = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap_or_else(|| panic!("{name} is Int64"));
            values.extend((0..column.len()).map(|row| column.value(row)));
        }
        values
    }

    /// `(v, w)` per row, `w` being `NULL` as `None`.
    fn pairs(&self) -> BTreeMap<i64, Option<i64>> {
        let mut pairs = BTreeMap::new();
        for batch in &self.batches {
            let v = batch
                .column_by_name("v")
                .expect("v column")
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("v is Int64");
            let w = batch
                .column_by_name("w")
                .expect("w column")
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("w is Int64");
            for row in 0..v.len() {
                let weight = w.is_valid(row).then(|| w.value(row));
                assert!(
                    pairs.insert(v.value(row), weight).is_none(),
                    "destination {} returned twice",
                    v.value(row)
                );
            }
        }
        pairs
    }
}

fn run(forge: &GraphForge, query: &str, params: &HashMap<String, IrLiteral>) -> Run {
    let (result, demand) =
        graphforge_exec::demand::capture(|| forge.execute_with_params(query, params));
    let result = result.unwrap_or_else(|e| panic!("{query}: {e}"));
    let (node_rows, edge_rows) = demand.hops.values().fold((0, 0), |(nodes, edges), hop| {
        (
            nodes + hop.node_rows_returned,
            edges + hop.edge_rows_returned,
        )
    });
    Run {
        batches: result.batches,
        node_rows,
        edge_rows,
    }
}

fn threshold() -> HashMap<String, IrLiteral> {
    HashMap::from([("x".to_owned(), IrLiteral::Int(THRESHOLD))])
}

const HUB: &str = "MATCH (h:Entity {ident: 0})-[r:LINK]->(b:Entity)";
const HUB_ANY_DIRECTION: &str = "MATCH (h:Entity {ident: 0})-[r:LINK]-(b:Entity)";

fn expected_weights(keep: impl Fn(Option<i64>) -> bool) -> BTreeMap<i64, Option<i64>> {
    (1..=SPOKES)
        .map(|spoke| (spoke as i64, weight(spoke)))
        .filter(|(_, weight)| keep(*weight))
        .collect()
}

fn passing() -> BTreeMap<i64, Option<i64>> {
    expected_weights(|weight| weight.is_some_and(|w| w > THRESHOLD))
}

fn partition_counts() -> [usize; 2] {
    [1, 4]
}

/// The hop reads the destinations of the relationships that pass, not of every
/// relationship it traverses, at one partition and at four.
#[test]
fn a_relationship_predicate_reads_only_the_passing_destinations() {
    let _serial = serial();
    let built = build();
    let expected = passing();
    assert!(!expected.is_empty() && expected.len() < SPOKES / 2);
    for partitions in partition_counts() {
        let forge = open(&built.project, partitions);
        let none = HashMap::new();

        // Without a predicate every destination is read.
        let everything = run(
            &forge,
            &format!("{HUB} RETURN b.ident AS v, r.weight AS w"),
            &none,
        );
        assert_eq!(
            everything.pairs(),
            expected_weights(|_| true),
            "{partitions} partitions"
        );
        assert_eq!(
            everything.node_rows, SPOKES as u64,
            "{partitions} partitions"
        );

        // The same rows, filtered by the test: what the unpushed form returns.
        let filtered_by_test: BTreeMap<i64, Option<i64>> = everything
            .pairs()
            .into_iter()
            .filter(|(_, weight)| weight.is_some_and(|w| w > THRESHOLD))
            .collect();
        assert_eq!(filtered_by_test, expected);

        for (label, query, params) in [
            (
                "parameter",
                format!("{HUB} WHERE r.weight > $x RETURN b.ident AS v, r.weight AS w"),
                threshold(),
            ),
            (
                "literal",
                format!("{HUB} WHERE r.weight > {THRESHOLD} RETURN b.ident AS v, r.weight AS w"),
                none.clone(),
            ),
        ] {
            let pushed = run(&forge, &query, &params);
            assert_eq!(pushed.pairs(), expected, "{label}, {partitions} partitions");
            assert_eq!(
                pushed.node_rows,
                expected.len() as u64,
                "{label}: destination rows read, {partitions} partitions"
            );
        }

        // The destination is read for its property and ordered, not counted.
        let ordered = run(
            &forge,
            &format!("{HUB} WHERE r.weight > $x RETURN b.ident AS v ORDER BY v"),
            &threshold(),
        );
        assert_eq!(
            ordered.int_column("v"),
            expected.keys().copied().collect::<Vec<_>>(),
            "{partitions} partitions"
        );
        assert_eq!(ordered.node_rows, expected.len() as u64);
        // The relationship properties come from the edge-property index by
        // edge id: the edge file is not read.
        assert_eq!(ordered.edge_rows, 0, "{partitions} partitions");
    }
}

/// A `NULL` verdict excludes the relationship exactly as `false` does, and a
/// disjunction that keeps the `NULL`-weight relationships keeps them.
#[test]
fn a_null_property_is_excluded_by_the_pushed_predicate_and_kept_by_is_null() {
    let _serial = serial();
    let built = build();
    for partitions in partition_counts() {
        let forge = open(&built.project, partitions);
        let none = HashMap::new();

        let nulls = run(
            &forge,
            &format!("{HUB} WHERE r.weight IS NULL RETURN b.ident AS v, r.weight AS w"),
            &none,
        );
        assert_eq!(
            nulls.pairs(),
            expected_weights(|weight| weight.is_none()),
            "{partitions} partitions"
        );
        assert_eq!(nulls.node_rows, (SPOKES / 8) as u64);

        let either = run(
            &forge,
            &format!(
                "{HUB} WHERE r.weight > $x OR r.weight IS NULL \
                 RETURN b.ident AS v, r.weight AS w"
            ),
            &threshold(),
        );
        let expected = expected_weights(|weight| weight.is_none_or(|w| w > THRESHOLD));
        assert_eq!(either.pairs(), expected, "{partitions} partitions");
        assert_eq!(either.node_rows, expected.len() as u64);

        // A comparison with a `NULL` parameter is `NULL` for every relationship.
        // A `NULL`-typed operand is not a primitive comparison, so this one is
        // not pushed; the rows are the same.
        let nothing = run(
            &forge,
            &format!("{HUB} WHERE r.weight > $x RETURN b.ident AS v"),
            &HashMap::from([("x".to_owned(), IrLiteral::Null)]),
        );
        assert!(
            nothing.int_column("v").is_empty(),
            "{partitions} partitions"
        );
    }
}

/// An undirected hop visits each relationship once and filters it the same way.
#[test]
fn an_undirected_hop_filters_its_relationships_before_their_destinations() {
    let _serial = serial();
    let built = build();
    let expected = passing();
    for partitions in partition_counts() {
        let forge = open(&built.project, partitions);
        let pushed = run(
            &forge,
            &format!("{HUB_ANY_DIRECTION} WHERE r.weight > $x RETURN b.ident AS v, r.weight AS w"),
            &threshold(),
        );
        assert_eq!(pushed.pairs(), expected, "{partitions} partitions");
        assert_eq!(
            pushed.node_rows,
            expected.len() as u64,
            "{partitions} partitions"
        );

        // From a spoke the only relationship is the hub's, in the other
        // direction; it passes or fails as it did from the hub.
        for spoke in [SPOKES as i64, SPOKES as i64 - 1, 3, 8] {
            let from_spoke = run(
                &forge,
                &format!(
                    "MATCH (b:Entity {{ident: {spoke}}})-[r:LINK]-(h:Entity) \
                     WHERE r.weight > $x RETURN h.ident AS v, r.weight AS w"
                ),
                &threshold(),
            );
            let keeps = weight(spoke as usize).is_some_and(|w| w > THRESHOLD);
            assert_eq!(
                from_spoke.pairs().len(),
                usize::from(keeps),
                "spoke {spoke}, {partitions} partitions"
            );
            assert_eq!(from_spoke.node_rows, u64::from(keeps));
        }
    }
}

/// A filter that is not total stays above the hop: the expansion is given no
/// predicate and reads every destination, and an error is the error it was.
#[test]
fn a_filter_that_can_fail_or_reads_the_destination_is_not_pushed() {
    let _serial = serial();
    let built = build();
    let expected = expected_weights(|weight| weight.is_some_and(|w| w > THRESHOLD && w % 2 == 0));
    for partitions in partition_counts() {
        let forge = open(&built.project, partitions);

        let mixed = run(
            &forge,
            &format!(
                "{HUB} WHERE r.weight > $x AND r.weight % 2 = 0 \
                 RETURN b.ident AS v, r.weight AS w"
            ),
            &threshold(),
        );
        assert_eq!(mixed.pairs(), expected, "{partitions} partitions");
        assert_eq!(mixed.node_rows, SPOKES as u64, "{partitions} partitions");

        let destination = run(
            &forge,
            &format!(
                "{HUB} WHERE r.weight > $x AND b.ident % 2 = 0 \
                 RETURN b.ident AS v, r.weight AS w"
            ),
            &threshold(),
        );
        assert_eq!(destination.pairs(), expected, "{partitions} partitions");

        // Weight 5 divides by zero. The relationship it belongs to does not
        // pass `r.weight > $x`, and the statement still fails, as it does
        // without the push-down, because the predicate is evaluated above the
        // hop on every row.
        let error = forge
            .execute_with_params(
                &format!(
                    "{HUB} WHERE r.weight > $x AND 10 / (r.weight - 5) > 1 RETURN b.ident AS v"
                ),
                &threshold(),
            )
            .err();
        let unpushed = forge
            .execute_with_params(
                &format!("{HUB} WHERE 10 / (r.weight - 5) > 1 RETURN b.ident AS v"),
                &threshold(),
            )
            .err();
        assert!(unpushed.is_some(), "weight 5 divides by zero");
        assert_eq!(
            error.is_some(),
            unpushed.is_some(),
            "a failing conjunct decides the outcome the same way with and without a pushable one"
        );
    }
}

/// The physical plan shows the pushed conjunct on the hop it applies to, and
/// only when it is pushable.
#[test]
fn the_physical_plan_carries_the_pushed_predicate() {
    let _serial = serial();
    let built = build();
    let forge = open(&built.project, 1);
    let pushed = forge
        .explain_stage(
            &format!("{HUB} WHERE r.weight > {THRESHOLD} RETURN b.ident AS v"),
            ExplainStage::PhysicalPlan,
        )
        .expect("physical plan");
    assert!(
        pushed.contains("edge_predicate="),
        "pushable predicate is not on the hop:\n{pushed}"
    );
    let kept = forge
        .explain_stage(
            &format!("{HUB} WHERE r.weight > {THRESHOLD} AND r.weight % 2 = 0 RETURN b.ident AS v"),
            ExplainStage::PhysicalPlan,
        )
        .expect("physical plan");
    assert!(
        !kept.contains("edge_predicate="),
        "a filter that can fail was pushed:\n{kept}"
    );
    let plain = forge
        .explain_stage(
            &format!("{HUB} RETURN b.ident AS v"),
            ExplainStage::PhysicalPlan,
        )
        .expect("physical plan");
    assert!(!plain.contains("edge_predicate="), "{plain}");
}
