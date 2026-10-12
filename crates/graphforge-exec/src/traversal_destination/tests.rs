use super::*;
use crate::session::{ExecutionSession, GraphForgeExtensionPlanner};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::DFSchema;
use datafusion::logical_expr::{EmptyRelation, LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_expr::expressions::{Column, lit};
use datafusion::physical_expr::{Partitioning, PhysicalExpr};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::filter::FilterExecBuilder;
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner};
use graphforge_core::OntologyMode;
use graphforge_ir::{Direction, RuntimeCatalog};
use graphforge_plan::{ExpandNode, VarLenExpandNode};
use graphforge_storage::{GraphCatalog, TOPOLOGY_NODES_SCHEMA};
use std::sync::Arc;
use tempfile::TempDir;

const INPUT_NODE_UUID: usize = 0;
const INPUT_OTHER: usize = 1;
const INPUT_WIDTH: usize = 2;
const EDGE_WIDTH: usize = 2;

fn input_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("other", DataType::Int64, true),
    ]))
}

fn logical_input() -> Arc<LogicalPlan> {
    Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
        produce_one_row: false,
        schema: Arc::new(DFSchema::try_from_qualified_schema("var_1", &input_schema()).unwrap()),
    }))
}

fn topology_fields() -> Vec<Arc<Field>> {
    TOPOLOGY_NODES_SCHEMA.fields().iter().cloned().collect()
}

/// Plan each of `nodes` over a two-column input that already holds a
/// `node_uuid` of its own, with or without a row fetch bounding that input.
async fn plan(nodes: &[&dyn UserDefinedLogicalNode], bounded: bool) -> Vec<Arc<dyn ExecutionPlan>> {
    let dir = TempDir::new().unwrap();
    let catalog = GraphCatalog::open(dir.path(), None, &RuntimeCatalog::new()).unwrap();
    let session = ExecutionSession::new_with_target(
        catalog,
        None,
        dir.path().to_path_buf(),
        OntologyMode::Strict,
    )
    .unwrap();
    let state = session.context().state();
    let planner = DefaultPhysicalPlanner::default();
    let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(input_schema()));
    let physical: Arc<dyn ExecutionPlan> = if bounded {
        Arc::new(GlobalLimitExec::new(input, 0, Some(8)))
    } else {
        input
    };
    let mut extensions = Vec::new();
    for node in nodes {
        extensions.push(
            GraphForgeExtensionPlanner
                .plan_extension(
                    &planner,
                    *node,
                    &[],
                    std::slice::from_ref(&physical),
                    &state,
                )
                .await
                .unwrap()
                .unwrap(),
        );
    }
    extensions
}

fn expansion() -> ExpandNode {
    ExpandNode::new(
        logical_input(),
        "KNOWS",
        1,
        2,
        3,
        Direction::Out,
        None,
        vec![
            Arc::new(Field::new(
                "edge_uuid",
                DataType::FixedSizeBinary(16),
                false,
            )),
            Arc::new(Field::new("edge_id", DataType::UInt64, false)),
        ],
        vec![],
        topology_fields(),
    )
}

fn variable_length_expansion() -> VarLenExpandNode {
    VarLenExpandNode::new(
        logical_input(),
        "KNOWS",
        1,
        Some(2),
        1,
        2,
        3,
        Direction::Out,
        None,
        topology_fields(),
        graphforge_plan::var_len_edge_list_field(&[]),
    )
}

/// The physical expansion and variable-length expansion over that input.
async fn traversals(bounded: bool) -> (Arc<dyn ExecutionPlan>, Arc<dyn ExecutionPlan>) {
    let mut planned = plan(&[&expansion(), &variable_length_expansion()], bounded).await;
    let var_len = planned.pop().unwrap();
    let expand = planned.pop().unwrap();
    (expand, var_len)
}

fn column(plan: &Arc<dyn ExecutionPlan>, index: usize) -> Arc<dyn PhysicalExpr> {
    Arc::new(Column::new(
        plan.schema().field(index).name().as_str(),
        index,
    ))
}

fn only_destination(plan: &dyn ExecutionPlan, destination: usize) {
    for index in 0..plan.schema().fields().len() {
        assert_eq!(
            is_traversal_destination_uuid(plan, index),
            index == destination,
            "column {index} ({})",
            plan.schema().field(index).name()
        );
    }
}

#[tokio::test]
async fn an_expansion_names_its_destination_uuid_and_not_its_inputs() {
    let (expand, _) = traversals(true).await;
    let destination =
        INPUT_WIDTH + EDGE_WIDTH + TOPOLOGY_NODES_SCHEMA.index_of("node_uuid").unwrap();
    assert_eq!(expand.schema().field(destination).name(), "node_uuid");
    only_destination(expand.as_ref(), destination);
    assert!(!is_traversal_destination_uuid(
        expand.as_ref(),
        INPUT_NODE_UUID
    ));
    assert!(is_frontier_endpoint_uuid(expand.as_ref(), destination));
    assert!(!is_frontier_endpoint_uuid(expand.as_ref(), INPUT_NODE_UUID));
}

#[tokio::test]
async fn a_variable_length_expansion_names_its_destination_uuid_and_not_its_inputs() {
    let (_, var_len) = traversals(true).await;
    let destination = INPUT_WIDTH + TOPOLOGY_NODES_SCHEMA.index_of("node_uuid").unwrap();
    assert_eq!(var_len.schema().field(destination).name(), "node_uuid");
    only_destination(var_len.as_ref(), destination);
    assert!(is_frontier_endpoint_uuid(var_len.as_ref(), destination));
    assert!(!is_frontier_endpoint_uuid(
        var_len.as_ref(),
        INPUT_NODE_UUID
    ));
}

#[tokio::test]
async fn a_reachability_expansion_names_its_destination_uuid_and_not_its_hop_count() {
    // Reachability mode replaces the relationship list with the hop count and
    // leaves the destination's columns directly after the input.
    let reachability = variable_length_expansion().into_reachability();
    let planned = plan(&[&reachability], true).await.pop().unwrap();
    let destination = INPUT_WIDTH + TOPOLOGY_NODES_SCHEMA.index_of("node_uuid").unwrap();
    assert_eq!(planned.schema().field(destination).name(), "node_uuid");
    assert_eq!(
        planned
            .schema()
            .fields()
            .last()
            .map(|field| field.name().as_str()),
        Some("hops")
    );
    only_destination(planned.as_ref(), destination);
    assert!(is_frontier_endpoint_uuid(planned.as_ref(), destination));
    let unbounded = plan(&[&reachability], false).await.pop().unwrap();
    assert!(!is_frontier_endpoint_uuid(unbounded.as_ref(), destination));
}

#[tokio::test]
async fn the_destination_is_followed_through_row_preserving_operators() {
    let (expand, var_len) = traversals(true).await;
    for (traversal, destination) in [
        (
            expand,
            INPUT_WIDTH + EDGE_WIDTH + TOPOLOGY_NODES_SCHEMA.index_of("node_uuid").unwrap(),
        ),
        (
            var_len,
            INPUT_WIDTH + TOPOLOGY_NODES_SCHEMA.index_of("node_uuid").unwrap(),
        ),
    ] {
        // A projection that reorders: the destination moves to column 1.
        let projected: Arc<dyn ExecutionPlan> = Arc::new(
            ProjectionExec::try_new(
                vec![
                    (column(&traversal, INPUT_OTHER), "other".to_owned()),
                    (column(&traversal, destination), "node_uuid".to_owned()),
                    (column(&traversal, INPUT_NODE_UUID), "node_uuid".to_owned()),
                ],
                Arc::clone(&traversal),
            )
            .unwrap(),
        );
        only_destination(projected.as_ref(), 1);

        // A computed expression is not a pass-through of the destination.
        let computed: Arc<dyn ExecutionPlan> = Arc::new(
            ProjectionExec::try_new(
                vec![(lit(1_i64), "node_uuid".to_owned())],
                Arc::clone(&traversal),
            )
            .unwrap(),
        );
        assert!(!is_traversal_destination_uuid(computed.as_ref(), 0));

        // A filter that narrows its columns passes the destination through.
        let filtered: Arc<dyn ExecutionPlan> = Arc::new(
            FilterExecBuilder::new(lit(true), Arc::clone(&projected))
                .apply_projection(Some(vec![2, 1]))
                .unwrap()
                .build()
                .unwrap(),
        );
        only_destination(filtered.as_ref(), 1);

        let exchanged: Arc<dyn ExecutionPlan> = Arc::new(
            RepartitionExec::try_new(Arc::clone(&filtered), Partitioning::RoundRobinBatch(4))
                .unwrap(),
        );
        only_destination(exchanged.as_ref(), 1);
        let coalesced: Arc<dyn ExecutionPlan> =
            Arc::new(CoalescePartitionsExec::new(Arc::clone(&exchanged)));
        only_destination(coalesced.as_ref(), 1);
    }
}

#[test]
fn a_topology_scan_is_never_a_traversal_destination() {
    // The scan behind a whole-graph `count(*)` has a `node_uuid` and no
    // frontier; that join must keep reading its whole route.
    let scan: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::clone(&TOPOLOGY_NODES_SCHEMA)));
    assert!(!is_traversal_destination_uuid(scan.as_ref(), 0));
    assert!(!is_frontier_endpoint_uuid(scan.as_ref(), 0));
}

#[tokio::test]
async fn only_a_bounded_traversal_may_prune_its_enrichment() {
    // The pruned join collects its frontier into one Arrow batch, so a
    // traversal that no anchor or fetch bounds keeps its partitioned join.
    let (bounded_expand, bounded_var_len) = traversals(true).await;
    let (unbounded_expand, unbounded_var_len) = traversals(false).await;
    let offset = TOPOLOGY_NODES_SCHEMA.index_of("node_uuid").unwrap();
    for (bounded, unbounded, destination) in [
        (
            bounded_expand,
            unbounded_expand,
            INPUT_WIDTH + EDGE_WIDTH + offset,
        ),
        (bounded_var_len, unbounded_var_len, INPUT_WIDTH + offset),
    ] {
        assert!(is_frontier_endpoint_uuid(bounded.as_ref(), destination));
        assert!(!is_frontier_endpoint_uuid(unbounded.as_ref(), destination));
        // Lineage alone does not depend on the bound.
        assert!(is_traversal_destination_uuid(
            unbounded.as_ref(),
            destination
        ));
    }
}
