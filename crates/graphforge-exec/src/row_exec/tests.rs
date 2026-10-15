pub(crate) async fn join_rows(
    cfg: &OptionalConfig,
    outer: &[RecordBatch],
    inner: &[RecordBatch],
) -> Result<RecordBatch, DataFusionError> {
    use datafusion::datasource::memory::MemorySourceConfig;
    let context = datafusion::prelude::SessionContext::new();
    let budget = optional_memory::Budget::new(&context.task_ctx());
    let outer =
        MemorySourceConfig::try_new_exec(&[outer.to_vec()], cfg.outer_schema.clone(), None)?;
    let inner =
        MemorySourceConfig::try_new_exec(&[inner.to_vec()], cfg.inner_schema.clone(), None)?;
    let outer = budget.collect(outer, context.task_ctx()).await?;
    let inner = budget.collect(inner, context.task_ctx()).await?;
    let output = optional_memory::join(cfg, outer, inner, &budget)?;
    budget.output(&output)?;
    Ok(output)
}

use super::*;
use arrow::array::RecordBatch;
use arrow::array::UInt64Array;
use arrow::datatypes::DataType;
use arrow::datatypes::Schema;
use std::sync::Arc;

#[tokio::test]
async fn optional_join_preserves_left_rows_matches_duplicate_keys_and_null_shapes_misses() {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::Field;

    let outer_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::UInt64, true),
        Field::new("outer", DataType::Utf8, false),
    ]));
    let inner_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::UInt64, true),
        Field::new("inner", DataType::Int64, false),
    ]));
    let out_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::UInt64, true),
        Field::new("outer", DataType::Utf8, false),
        Field::new("inner", DataType::Int64, true),
    ]));
    let outer = RecordBatch::try_new(
        outer_schema.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![Some(1), Some(2), None])),
            Arc::new(StringArray::from(vec!["one", "two", "null"])),
        ],
    )
    .unwrap();
    let inner = RecordBatch::try_new(
        inner_schema.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![Some(1), Some(1), None])),
            Arc::new(Int64Array::from(vec![10, 11, 99])),
        ],
    )
    .unwrap();
    let cfg = OptionalConfig {
        join_keys: vec![(0, 0)],
        inner_keep_idx: vec![1],
        null_safe_keys: false,
        out_schema,
        outer_schema,
        inner_schema,
    };

    let joined = join_rows(&cfg, &[outer], &[inner]).await.unwrap();
    assert_eq!(
        joined.num_rows(),
        4,
        "duplicate matches must duplicate the left row"
    );
    let keys = joined
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(
        keys.iter().collect::<Vec<_>>(),
        vec![Some(1), Some(1), Some(2), None]
    );
    let values = joined
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(
        values.iter().collect::<Vec<_>>(),
        vec![Some(10), Some(11), None, None]
    );
}

#[tokio::test]
async fn optional_join_handles_cartesian_empty_uuid_and_invalid_key_contracts() {
    use arrow::array::{FixedSizeBinaryArray, Int64Array, StringArray};
    use arrow::datatypes::Field;

    let outer_schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let inner_schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
    let out_schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let outer = RecordBatch::try_new(
        outer_schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2]))],
    )
    .unwrap();
    let cfg = OptionalConfig {
        join_keys: vec![],
        inner_keep_idx: vec![0],
        null_safe_keys: false,
        out_schema: out_schema.clone(),
        outer_schema: outer_schema.clone(),
        inner_schema: inner_schema.clone(),
    };
    let empty = join_rows(&cfg, &[outer.clone()], &[]).await.unwrap();
    assert_eq!(empty.num_rows(), 2);
    assert!(empty.column(1).is_null(0) && empty.column(1).is_null(1));

    let inner = RecordBatch::try_new(
        inner_schema,
        vec![Arc::new(StringArray::from(vec!["a", "b"]))],
    )
    .unwrap();
    let product = join_rows(&cfg, &[outer], &[inner]).await.unwrap();
    assert_eq!(product.num_rows(), 4);

    let uuid_schema = Arc::new(Schema::new(vec![Field::new(
        "uuid",
        DataType::FixedSizeBinary(16),
        false,
    )]));
    let uuids = FixedSizeBinaryArray::try_from_iter([&[1_u8; 16][..]].into_iter()).unwrap();
    let uuid_batch = RecordBatch::try_new(uuid_schema.clone(), vec![Arc::new(uuids)]).unwrap();
    let uuid_cfg = OptionalConfig {
        join_keys: vec![(0, 0)],
        inner_keep_idx: vec![],
        null_safe_keys: false,
        out_schema: uuid_schema.clone(),
        outer_schema: uuid_schema.clone(),
        inner_schema: uuid_schema,
    };
    assert_eq!(
        join_rows(&uuid_cfg, &[uuid_batch.clone()], &[uuid_batch])
            .await
            .unwrap()
            .num_rows(),
        1
    );

    let bad_key_cfg = OptionalConfig {
        join_keys: vec![(0, 0)],
        inner_keep_idx: vec![],
        null_safe_keys: false,
        out_schema: outer_schema.clone(),
        outer_schema: outer_schema.clone(),
        inner_schema: outer_schema.clone(),
    };
    let ints =
        RecordBatch::try_new(outer_schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
    assert!(
        join_rows(&bad_key_cfg, &[ints.clone()], &[ints])
            .await
            .unwrap_err()
            .to_string()
            .contains("unsupported type")
    );

    let bad_keep_cfg = OptionalConfig {
        inner_keep_idx: vec![9],
        null_safe_keys: false,
        ..cfg
    };
    let outer = RecordBatch::try_new(
        bad_keep_cfg.outer_schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .unwrap();
    let inner = RecordBatch::try_new(
        bad_keep_cfg.inner_schema.clone(),
        vec![Arc::new(StringArray::from(vec!["value"]))],
    )
    .unwrap();
    assert!(
        join_rows(&bad_keep_cfg, &[outer], &[inner])
            .await
            .unwrap_err()
            .to_string()
            .contains("inner_keep_idx 9 out of range")
    );
}

#[test]
fn wave11_unwind_explode_preserves_order_and_enforces_list_contract() {
    use arrow::array::{Int64Array, ListArray, StringArray};
    use arrow::datatypes::Field;
    use datafusion::common::DFSchema;
    use datafusion::logical_expr::col;

    let list_field = Arc::new(Field::new("item", DataType::Int64, true));
    let input_schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("items", DataType::List(list_field), true),
    ]));
    let out_schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("items", input_schema.field(1).data_type().clone(), true),
        Field::new("item", DataType::Int64, true),
    ]));
    let lists = ListArray::from_iter_primitive::<arrow::datatypes::Int64Type, _, _>([
        Some(vec![Some(3), Some(4)]),
        None,
        Some(vec![]),
        Some(vec![Some(8)]),
    ]);
    let batch = RecordBatch::try_new(
        input_schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            Arc::new(lists),
        ],
    )
    .unwrap();
    let cfg = UnwindConfig {
        list_expr: col("items"),
        input_schema: input_schema.clone(),
        input_dfschema: Arc::new(DFSchema::try_from(input_schema.as_ref().clone()).unwrap()),
        out_schema,
        required_output: None,
    };
    let exploded = unwind_explode(&cfg, &[batch]).unwrap();
    assert_eq!(exploded.num_rows(), 3);
    assert_eq!(
        exploded
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[3, 4, 8]
    );

    let scalar_schema = Arc::new(Schema::new(vec![Field::new(
        "items",
        DataType::Int64,
        false,
    )]));
    let scalar_cfg = UnwindConfig {
        list_expr: col("items"),
        input_schema: scalar_schema.clone(),
        input_dfschema: Arc::new(DFSchema::try_from(scalar_schema.as_ref().clone()).unwrap()),
        out_schema: Arc::new(Schema::new(vec![
            Field::new("items", DataType::Int64, false),
            Field::new("item", DataType::Int64, true),
        ])),

        required_output: None,
    };
    let scalar =
        RecordBatch::try_new(scalar_schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
    assert!(
        unwind_explode(&scalar_cfg, &[scalar])
            .unwrap_err()
            .to_string()
            .contains("must evaluate to a list")
    );

    let entity_cfg = UnwindConfig {
        list_expr: col("items"),
        input_schema: input_schema.clone(),
        input_dfschema: Arc::new(DFSchema::try_from(input_schema.as_ref().clone()).unwrap()),
        out_schema: Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("items", input_schema.field(1).data_type().clone(), true),
            Field::new("node_uuid", DataType::FixedSizeBinary(16), true),
            Field::new("node_id", DataType::UInt64, true),
        ])),

        required_output: None,
    };
    let primitive_lists =
        ListArray::from_iter_primitive::<arrow::datatypes::Int64Type, _, _>([Some(vec![Some(1)])]);
    let entity_input = RecordBatch::try_new(
        input_schema,
        vec![
            Arc::new(StringArray::from(vec!["a"])),
            Arc::new(primitive_lists),
        ],
    )
    .unwrap();
    assert!(
        unwind_explode(&entity_cfg, &[entity_input])
            .unwrap_err()
            .to_string()
            .contains("requires a struct element")
    );
}

#[test]
fn unwind_explode_copies_only_the_columns_the_parent_reads() {
    use arrow::array::{Array, Int64Array, ListArray, StringArray};
    use arrow::datatypes::Field;
    use datafusion::common::DFSchema;
    use datafusion::logical_expr::col;

    let list_field = Arc::new(Field::new("item", DataType::Int64, true));
    let list_type = DataType::List(list_field);
    let input_schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("wide", DataType::Utf8, true),
        Field::new("items", list_type.clone(), true),
        Field::new("tag", DataType::Int64, false),
    ]));
    let out_schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("wide", DataType::Utf8, true),
        Field::new("items", list_type, true),
        Field::new("tag", DataType::Int64, false),
        Field::new("item", DataType::Int64, true),
    ]));
    let lists = ListArray::from_iter_primitive::<arrow::datatypes::Int64Type, _, _>([
        Some(vec![Some(3), Some(4)]),
        None,
        Some(vec![Some(8)]),
    ]);
    let batch = RecordBatch::try_new(
        input_schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
            Arc::new(StringArray::from(vec!["wa", "wb", "wc"])),
            Arc::new(lists),
            Arc::new(Int64Array::from(vec![10, 20, 30])),
        ],
    )
    .unwrap();
    let explode = |required_output: Option<std::collections::BTreeSet<usize>>| {
        let cfg = UnwindConfig {
            list_expr: col("items"),
            input_schema: input_schema.clone(),
            input_dfschema: Arc::new(DFSchema::try_from(input_schema.as_ref().clone()).unwrap()),
            out_schema: out_schema.clone(),
            required_output,
        };
        unwind_explode(&cfg, std::slice::from_ref(&batch)).unwrap()
    };

    // The parent reads `name` (0) and the element (4). `wide` and `items` are
    // nullable and unread, so they are not copied. `tag` is not nullable, so a
    // null array cannot stand in for it and it is copied.
    let narrow = explode(Some([0, 4].into()));
    let full = explode(None);
    assert_eq!(narrow.schema(), out_schema);
    assert_eq!(narrow.num_rows(), 3);
    for demanded in [0, 3, 4] {
        assert_eq!(
            narrow.column(demanded).as_ref(),
            full.column(demanded).as_ref(),
            "column {demanded} must be unchanged"
        );
    }
    for skipped in [1, 2] {
        assert_eq!(narrow.column(skipped).null_count(), 3, "column {skipped}");
    }
    assert_eq!(full.column(1).null_count(), 0);
    assert_eq!(full.column(2).null_count(), 0);
    let names = narrow
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.iter().flatten().collect::<Vec<_>>(), ["a", "a", "c"]);
    let items = narrow
        .column(4)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(items.values(), &[3, 4, 8]);

    // Demanding every column copies every column.
    let every = explode(Some([0, 1, 2, 3, 4].into()));
    assert_eq!(every, full);
}

#[test]
fn unwind_exec_demands_the_list_column_even_when_its_parent_does_not_read_it() {
    use arrow::datatypes::Field;
    use datafusion::logical_expr::logical_plan::LogicalTableSource;
    use datafusion::logical_expr::{LogicalPlanBuilder, col};
    use datafusion::physical_plan::empty::EmptyExec;

    let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let input_schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("wide", DataType::Utf8, true),
        Field::new("items", list_type, true),
        Field::new("tag", DataType::Int64, true),
    ]));
    let scan = LogicalPlanBuilder::scan(
        "var_0",
        Arc::new(LogicalTableSource::new(input_schema.clone())),
        None,
    )
    .unwrap()
    .build()
    .unwrap();
    let node = UnwindNode::new(
        Arc::new(scan),
        col("items"),
        "var_1",
        &Field::new("elem", DataType::Int64, true),
    );
    let exec = UnwindExec::new(&node, Arc::new(EmptyExec::new(input_schema)));
    let element = 4;

    // The parent reads the element and `name`; the list column is demanded of
    // the input because the explosion reads it, and nothing else is.
    assert_eq!(
        exec.input_demand(&[0, element].into()).unwrap(),
        [0, 2].into()
    );
    // A parent that reads only the element still demands the list.
    assert_eq!(exec.input_demand(&[element].into()).unwrap(), [2].into());
    // A parent that reads the list keeps it once.
    assert_eq!(exec.input_demand(&[2, element].into()).unwrap(), [2].into());

    // The rewritten node prints how many input columns it still copies.
    assert!(!format!("{}", display_plan(&exec)).contains("copied="));
    let narrowed = exec.with_required_output([0, 2, element].into());
    assert!(
        display_plan(narrowed.as_ref()).contains("copied=2"),
        "{}",
        display_plan(narrowed.as_ref())
    );
}

fn display_plan(plan: &dyn ExecutionPlan) -> String {
    datafusion::physical_plan::displayable(plan)
        .indent(false)
        .to_string()
}

fn seed_exec(seed: u64, schema: &Arc<Schema>) -> Arc<dyn ExecutionPlan> {
    Arc::new(CorrelatedSeedExec::with_rows(seed, schema.clone(), None))
}

fn seed_rows(plan: &Arc<dyn ExecutionPlan>) -> Result<Vec<RecordBatch>, DataFusionError> {
    let context = datafusion::prelude::SessionContext::new().task_ctx();
    futures::executor::block_on(collect(plan.clone(), context))
}

#[test]
fn correlated_seed_fails_closed_until_the_outer_rows_are_bound() {
    use arrow::datatypes::Field;

    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::UInt64, false)]));
    let error = seed_rows(&seed_exec(7, &schema)).unwrap_err().to_string();
    assert!(
        error.contains("before its outer rows were bound"),
        "{error}"
    );

    let rows = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(UInt64Array::from(vec![4, 5]))],
    )
    .unwrap();
    let bound = bind_correlated_seed(seed_exec(7, &schema), 7, &[rows]).unwrap();
    let values: Vec<u64> = seed_rows(&bound)
        .unwrap()
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(values, vec![4, 5]);
}

#[test]
fn a_bound_seed_stops_holding_its_rows_once_released() {
    use arrow::datatypes::Field;

    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::UInt64, false)]));
    let rows = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(UInt64Array::from(vec![4, 5]))],
    )
    .unwrap();
    let (bound, handle) = bind_seed_rows(seed_exec(7, &schema), 7, &[rows]).unwrap();
    assert_eq!(seed_rows(&bound).unwrap()[0].num_rows(), 2);

    // The executed plan stays reachable for EXPLAIN ANALYZE, so the rows must
    // not stay alive with it, and a late execution must not see an empty seed.
    handle.release();
    assert!(handle.get().is_none());
    let error = seed_rows(&bound).unwrap_err().to_string();
    assert!(
        error.contains("after its outer rows were released"),
        "{error}"
    );
}

#[test]
fn binding_a_correlated_seed_leaves_other_seeds_and_requires_its_own_leaf() {
    use arrow::datatypes::Field;

    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::UInt64, false)]));
    let rows =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(UInt64Array::from(vec![1]))]).unwrap();

    let error = bind_correlated_seed(seed_exec(2, &schema), 1, std::slice::from_ref(&rows))
        .err()
        .expect("no leaf carries the owner's id");
    assert!(error.to_string().contains("no seed leaf 1"), "{error}");

    // A nested correlated sub-plan's leaf keeps waiting for its own rows.
    let union = datafusion::physical_plan::union::UnionExec::try_new(vec![
        seed_exec(1, &schema),
        seed_exec(2, &schema),
    ])
    .unwrap();
    let bound = bind_correlated_seed(union, 1, &[rows]).unwrap();
    let bound_leaves: Vec<bool> = bound
        .children()
        .iter()
        .map(|child| {
            child
                .downcast_ref::<CorrelatedSeedExec>()
                .expect("seed leaf")
                .rows
                .is_some()
        })
        .collect();
    assert_eq!(bound_leaves, vec![true, false]);
}

fn qualified_scan(name: &str, schema: &Arc<Schema>) -> Arc<datafusion::logical_expr::LogicalPlan> {
    use datafusion::logical_expr::LogicalPlanBuilder;
    use datafusion::logical_expr::logical_plan::LogicalTableSource;

    let source = Arc::new(LogicalTableSource::new(schema.clone()));
    Arc::new(
        LogicalPlanBuilder::scan(name, source, None)
            .unwrap()
            .build()
            .unwrap(),
    )
}

#[test]
fn optional_match_demand_is_the_keys_plus_the_columns_it_outputs() {
    use arrow::datatypes::Field;
    use datafusion::physical_plan::empty::EmptyExec;

    let outer_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::UInt64, false),
        Field::new("a_extra", DataType::UInt64, false),
    ]));
    let inner_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::UInt64, false),
        Field::new("b", DataType::UInt64, false),
        Field::new("r", DataType::UInt64, false),
    ]));
    // Outer column 0 joins inner column 0; the output appends inner `b` then `r`,
    // so output columns 0 and 1 are the outer's and 2 and 3 are `b` and `r`.
    let node = OptionalMatchNode::new(
        qualified_scan("var_0", &outer_schema),
        qualified_scan("var_1", &inner_schema),
        vec![(0, 0)],
        vec![1, 2],
    );
    let exec = |node: &OptionalMatchNode| {
        OptionalMatchExec::new(
            node,
            Arc::new(EmptyExec::new(outer_schema.clone())),
            Arc::new(EmptyExec::new(inner_schema.clone())),
        )
    };
    let demand = |exec: &OptionalMatchExec, required: &[usize]| {
        let required = required.iter().copied().collect::<BTreeSet<_>>();
        let (outer, inner) = exec.input_demand(&required);
        (
            outer.into_iter().collect::<Vec<_>>(),
            inner.into_iter().collect::<Vec<_>>(),
        )
    };

    let plain = exec(&node);
    // Nothing demanded still needs the join keys of both sides.
    assert_eq!(demand(&plain, &[]), (vec![0], vec![0]));
    assert_eq!(demand(&plain, &[1]), (vec![0, 1], vec![0]));
    // Output column 3 is the inner's column 2, not column 3.
    assert_eq!(demand(&plain, &[3]), (vec![0], vec![0, 2]));
    assert_eq!(demand(&plain, &[2, 3]), (vec![0], vec![0, 1, 2]));

    // A correlated sub-plan is seeded with the outer rows, but the columns it
    // reads from them are known only after the sub-plan is rewritten, so the
    // split itself asks the outer input for no more than the join and the
    // output need.
    let correlated = exec(&node.with_correlated_seed(5));
    assert_eq!(correlated.correlated_seed_id(), Some(5));
    assert_eq!(plain.correlated_seed_id(), None);
    assert_eq!(demand(&correlated, &[]), (vec![0], vec![0]));
    assert_eq!(demand(&correlated, &[3]), (vec![0], vec![0, 2]));
}

#[test]
fn correlated_seed_demand_is_what_the_sub_plan_reads_through_its_leaves() {
    use arrow::datatypes::Field;
    use datafusion::physical_plan::union::UnionExec;

    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::UInt64, false),
        Field::new("b", DataType::UInt64, false),
        Field::new("c", DataType::UInt64, false),
    ]));
    let leaf = |seed: u64, demand: Option<&[usize]>| -> Arc<dyn ExecutionPlan> {
        let leaf = CorrelatedSeedExec::with_rows(seed, schema.clone(), None);
        Arc::new(match demand {
            Some(demand) => leaf.with_demand(demand.iter().copied().collect()),
            None => leaf,
        })
    };
    let demand_of = |plans: Vec<Arc<dyn ExecutionPlan>>, seed: u64| {
        let plan: Arc<dyn ExecutionPlan> = UnionExec::try_new(plans).unwrap();
        correlated_seed_demand(&plan, seed)
            .into_iter()
            .collect::<Vec<_>>()
    };

    // The leaves of one seed contribute the union of what each was asked for.
    assert_eq!(
        demand_of(vec![leaf(3, Some(&[0])), leaf(3, Some(&[2]))], 3),
        vec![0, 2]
    );
    // A leaf no demand reached stands for every column.
    assert_eq!(
        demand_of(vec![leaf(3, Some(&[1])), leaf(3, None)], 3),
        vec![0, 1, 2]
    );
    // A leaf of another seed is not this sub-plan's concern.
    assert_eq!(
        demand_of(vec![leaf(3, Some(&[1])), leaf(4, Some(&[0, 2]))], 3),
        vec![1]
    );
    // An empty demand stays empty rather than widening to the whole row.
    assert_eq!(
        demand_of(vec![leaf(3, Some(&[])), leaf(3, Some(&[]))], 3),
        Vec::<usize>::new()
    );
}

/// An inner plan with a real correlated-seed child, stopped at its first poll.
/// Binding must retain the real outer rows before this plan stalls or panics.
#[derive(Debug)]
struct InterruptedSeedExec {
    input: Arc<dyn ExecutionPlan>,
    panic_on_poll: bool,
}

impl DisplayAs for InterruptedSeedExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "InterruptedSeedExec")
    }
}

impl ExecutionPlan for InterruptedSeedExec {
    fn name(&self) -> &str {
        "InterruptedSeedExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        assert_eq!(children.len(), 1);
        Ok(Arc::new(Self {
            input: Arc::clone(&children[0]),
            panic_on_poll: self.panic_on_poll,
        }))
    }

    fn execute(
        &self,
        partition: usize,
        _: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        assert_eq!(partition, 0);
        let panic_on_poll = self.panic_on_poll;
        let stream = futures::stream::poll_fn(move |_| {
            assert!(!panic_on_poll, "interrupted optional inner poll");
            std::task::Poll::<Option<Result<RecordBatch, DataFusionError>>>::Pending
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

pub(super) fn interrupted_optional(
    panic_on_poll: bool,
) -> (
    Arc<OptionalMatchExec>,
    std::sync::Weak<dyn arrow::array::Array>,
) {
    use arrow::datatypes::Field;
    use datafusion::datasource::memory::MemorySourceConfig;

    let schema = Arc::new(Schema::new(vec![Field::new(
        "key",
        DataType::UInt64,
        false,
    )]));
    let column: arrow::array::ArrayRef = Arc::new(UInt64Array::from(vec![1, 2, 3]));
    let weak = Arc::downgrade(&column);
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![column]).unwrap();
    let outer =
        MemorySourceConfig::try_new_exec(&[vec![batch]], Arc::clone(&schema), None).unwrap();
    let inner: Arc<dyn ExecutionPlan> = Arc::new(InterruptedSeedExec {
        input: seed_exec(7, &schema),
        panic_on_poll,
    });
    let node = OptionalMatchNode::new(
        qualified_scan("var_0", &schema),
        qualified_scan("var_1", &schema),
        vec![(0, 0)],
        vec![],
    )
    .with_correlated_seed(7);
    (Arc::new(OptionalMatchExec::new(&node, outer, inner)), weak)
}

fn displayed_seed_rows(optional: &OptionalMatchExec) -> SeedRows {
    optional
        .executed_inner
        .get()
        .expect("the bound inner must remain for display")
        .children()[0]
        .downcast_ref::<CorrelatedSeedExec>()
        .expect("bound seed leaf")
        .rows
        .clone()
        .expect("the leaf was bound")
}

#[tokio::test]
async fn cancelled_optional_collection_releases_displayed_seed_buffers() {
    use futures::StreamExt;
    let (optional, weak) = interrupted_optional(false);
    // The MemorySource itself legitimately owns the array throughout this
    // test. Execution must return to that baseline while the Optional plan,
    // including its stored bound inner, remains alive.
    let baseline = weak.strong_count();
    assert_eq!(baseline, 1);
    let context = datafusion::prelude::SessionContext::new();
    let mut stream = optional.execute(0, context.task_ctx()).unwrap();
    assert!(futures::poll!(stream.next()).is_pending());
    let rows = displayed_seed_rows(&optional);
    assert!(rows.get().is_some());
    assert!(weak.strong_count() > baseline);
    drop(stream);
    assert!(
        rows.get().is_none(),
        "cancelled execution retained the bound seed rows"
    );
    assert_eq!(
        weak.strong_count(),
        baseline,
        "execution leaked outer buffers into the display plan"
    );
    assert!(optional.executed_inner.get().is_some());
}

#[tokio::test]
async fn panicking_optional_collection_releases_displayed_seed_buffers() {
    use futures::FutureExt;
    let (optional, weak) = interrupted_optional(true);
    let baseline = weak.strong_count();
    assert_eq!(baseline, 1);
    let context = datafusion::prelude::SessionContext::new();
    let plan: Arc<dyn ExecutionPlan> = optional.clone();
    let outcome = std::panic::AssertUnwindSafe(collect(plan, context.task_ctx()))
        .catch_unwind()
        .await;
    assert!(
        outcome.is_err(),
        "the inner poll must panic after the seed was bound"
    );
    let rows = displayed_seed_rows(&optional);
    assert!(
        rows.get().is_none(),
        "unwound execution retained the bound seed rows"
    );
    assert_eq!(
        weak.strong_count(),
        baseline,
        "execution leaked outer buffers into the display plan"
    );
    assert!(optional.executed_inner.get().is_some());
}
