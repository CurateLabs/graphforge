use super::*;
use arrow::array::{StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::execution::memory_pool::MemoryPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::logical_expr::LogicalPlanBuilder;
use datafusion::logical_expr::logical_plan::LogicalTableSource;
use datafusion::prelude::{SessionConfig, SessionContext};
use std::sync::atomic::Ordering;

const MIB: usize = 1024 * 1024;

fn batch(rows: usize, width: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::UInt64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let text = "x".repeat(width);
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(vec![1; rows])),
            Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                text, rows,
            ))),
        ],
    )
    .unwrap()
}

fn optional(outer: Vec<RecordBatch>, inner: Vec<RecordBatch>) -> Arc<dyn ExecutionPlan> {
    optional_with_seed(outer, inner, false)
}

fn optional_with_seed(
    outer: Vec<RecordBatch>,
    inner: Vec<RecordBatch>,
    correlated: bool,
) -> Arc<dyn ExecutionPlan> {
    use datafusion_datasource::memory::MemorySourceConfig;
    let logical = |name, schema| {
        Arc::new(
            LogicalPlanBuilder::scan(name, Arc::new(LogicalTableSource::new(schema)), None)
                .unwrap()
                .build()
                .unwrap(),
        )
    };
    let outer_schema = outer[0].schema();
    let inner_schema = inner[0].schema();
    let mut node = OptionalMatchNode::new(
        logical("outer", Arc::clone(&outer_schema)),
        logical("inner", Arc::clone(&inner_schema)),
        vec![(0, 0)],
        vec![1],
    );
    let inner = if correlated {
        node = node.with_correlated_seed(7);
        Arc::new(CorrelatedSeedExec::with_rows(7, inner_schema, None)) as Arc<dyn ExecutionPlan>
    } else {
        MemorySourceConfig::try_new_exec(&[inner], inner_schema, None).unwrap()
    };
    Arc::new(OptionalMatchExec::new(
        &node,
        MemorySourceConfig::try_new_exec(&[outer], outer_schema, None).unwrap(),
        inner,
    ))
}

fn context(budget: usize) -> (SessionContext, Arc<crate::query_pool::QueryMemoryPool>) {
    let pool = Arc::new(crate::query_pool::QueryMemoryPool::new(budget));
    let shared: Arc<dyn MemoryPool> = pool.clone();
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(shared)
        .build_arc()
        .unwrap();
    (
        SessionContext::new_with_config_rt(SessionConfig::new(), runtime),
        pool,
    )
}

fn resource_error(error: DataFusionError) {
    assert!(
        matches!(error.find_root(), DataFusionError::ResourcesExhausted(_)),
        "{error}"
    );
    let text = error.to_string();
    assert!(
        text.contains("Resources exhausted") && text.contains("OptionalMatch"),
        "{text}"
    );
}

#[tokio::test]
async fn optional_collection_respects_the_pool_and_fitting_rows_are_exact() {
    for inner_side in [false, true] {
        let source = vec![batch(256, 128); 4];
        let empty = vec![RecordBatch::new_empty(source[0].schema())];
        let plan = if inner_side {
            optional(empty, source)
        } else {
            optional(source, empty)
        };
        let (tight, pool) = context(64 * 1024);
        let error = collect(Arc::clone(&plan), tight.task_ctx())
            .await
            .err()
            .expect("retained input exceeds the pool");
        assert!(error.to_string().contains("OptionalMatch collection"));
        resource_error(error);
        assert_eq!(pool.reserved(), 0);
        let (roomy, pool) = context(32 * MIB);
        let rows = collect(plan, roomy.task_ctx()).await.unwrap();
        assert_eq!(
            rows.iter().map(RecordBatch::num_rows).sum::<usize>(),
            if inner_side { 0 } else { 1024 }
        );
        if !inner_side {
            for row in &rows {
                let values = row
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                assert!(
                    values
                        .iter()
                        .all(|value| value == Some("x".repeat(128).as_str()))
                );
                assert_eq!(row.column(2).null_count(), row.num_rows());
            }
        }
        assert!(pool.peak_gauge().load(Ordering::Relaxed) <= 32 * MIB);
        assert_eq!(pool.reserved(), 0);
    }
}

#[tokio::test]
async fn optional_many_to_many_wide_output_is_admitted_before_copying() {
    let plan = optional(vec![batch(100, 1024)], vec![batch(100, 1024)]);
    let (tight, pool) = context(4 * MIB);
    resource_error(
        collect(Arc::clone(&plan), tight.task_ctx())
            .await
            .err()
            .expect("wide cross product exceeds the pool"),
    );
    assert_eq!(pool.reserved(), 0);
    let (roomy, pool) = context(128 * MIB);
    let rows = collect(plan, roomy.task_ctx()).await.unwrap();
    assert_eq!(
        rows.iter().map(RecordBatch::num_rows).sum::<usize>(),
        10_000
    );
    let expected = "x".repeat(1024);
    for row in rows {
        for index in [1, 2] {
            let values = row
                .column(index)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert!(values.iter().all(|value| value == Some(expected.as_str())));
        }
    }
    assert!(pool.peak_gauge().load(Ordering::Relaxed) <= 128 * MIB);
    assert_eq!(pool.reserved(), 0);
}

#[tokio::test]
async fn optional_output_charge_survives_yield_until_next_poll_or_drop() {
    use futures::StreamExt;
    for drop_after_output in [false, true] {
        let plan = optional(vec![batch(8, 32)], vec![batch(3, 16)]);
        let (context, pool) = context(MIB);
        let mut stream = plan.execute(0, context.task_ctx()).unwrap();
        let output = stream.next().await.unwrap().unwrap();
        assert_eq!(output.num_rows(), 24);
        assert!(pool.reserved() >= output.get_array_memory_size());
        if !drop_after_output {
            assert!(stream.next().await.is_none());
            assert_eq!(pool.reserved(), 0);
        }
        drop(stream);
        assert_eq!(pool.reserved(), 0);
    }
}

#[tokio::test]
async fn optional_skewed_concat_uses_actual_spans_not_take_multiplicity() {
    let schema = batch(0, 0).schema();
    let make = |values: Vec<&str>| {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![1; values.len()])),
                Arc::new(StringArray::from(values)),
            ],
        )
        .unwrap()
    };
    let wide = "w".repeat(32 * 1024);
    let outer = vec![make(vec![wide.as_str()]), make(vec![""; 100_000])];
    let (context, pool) = context(128 * MIB);
    let budget = optional_memory::Budget::new(&context.task_ctx());
    let complete = optional(outer.clone(), vec![RecordBatch::new_empty(schema.clone())]);
    let plan = datafusion_datasource::memory::MemorySourceConfig::try_new_exec(
        &[outer],
        schema.clone(),
        None,
    )
    .unwrap();
    let collected = budget.collect(plan, context.task_ctx()).await.unwrap();
    let output = budget.concat(collected, &schema).unwrap();
    assert_eq!(output.num_rows(), 100_001);
    let values = output
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(values.value(0), wide);
    assert!(values.iter().skip(1).all(|value| value == Some("")));
    assert!(pool.reserved() >= output.get_array_memory_size());
    drop(output);
    drop(budget);
    assert_eq!(pool.reserved(), 0);
    let output = collect(complete, context.task_ctx()).await.unwrap();
    assert_eq!(
        output.iter().map(RecordBatch::num_rows).sum::<usize>(),
        100_001
    );
    assert_eq!(
        output[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        wide
    );
    assert!(
        output
            .iter()
            .all(|batch| batch.column(2).null_count() == batch.num_rows())
    );
    assert!(output.iter().skip(1).all(|batch| {
        batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .all(|value| value == Some(""))
    }));
    assert_eq!(pool.reserved(), 0);
}

#[tokio::test]
async fn optional_correlated_refusal_and_cancel_release_pool_and_displayed_seed() {
    use futures::StreamExt;
    let plan = optional_with_seed(vec![batch(100, 1024)], vec![batch(100, 1024)], true);
    let (context, pool) = context(4 * MIB);
    resource_error(
        collect(Arc::clone(&plan), context.task_ctx())
            .await
            .err()
            .unwrap(),
    );
    let optional = plan.downcast_ref::<OptionalMatchExec>().unwrap();
    let seed = optional
        .executed_inner
        .get()
        .unwrap()
        .downcast_ref::<CorrelatedSeedExec>()
        .unwrap();
    assert!(seed.rows.as_ref().unwrap().get().is_none());
    assert_eq!(pool.reserved(), 0);
    let (plan, _) = super::tests::interrupted_optional(false);
    let mut stream = plan.execute(0, context.task_ctx()).unwrap();
    assert!(futures::poll!(stream.next()).is_pending());
    assert!(pool.reserved() > 0);
    drop(stream);
    assert_eq!(pool.reserved(), 0);
}

#[tokio::test]
async fn concurrent_seed_streams_admit_handles_before_cloning_and_release_at_eos_or_drop() {
    use futures::StreamExt;
    let batches = vec![batch(1, 8); 128];
    let schema = batches[0].schema();
    let seed = CorrelatedSeedExec {
        rows: Some(SeedRows::new(batches)),
        ..CorrelatedSeedExec::with_rows(7, schema, None)
    };
    let (tight, pool) = context(32 * 1024);
    let first = seed.execute(0, tight.task_ctx()).unwrap();
    let error = seed
        .execute(0, tight.task_ctx())
        .err()
        .expect("two metadata collections exceed pool");
    resource_error(error);
    drop(first);
    assert_eq!(pool.reserved(), 0);
    let (roomy, pool) = context(MIB);
    let mut first = seed.execute(0, roomy.task_ctx()).unwrap();
    let second = seed.execute(0, roomy.task_ctx()).unwrap();
    assert!(pool.reserved() > 0);
    let mut rows = 0;
    while let Some(batch) = first.next().await {
        rows += batch.unwrap().num_rows();
    }
    assert_eq!(rows, 128);
    assert!(
        pool.reserved() > 0,
        "the second stream still owns its handles"
    );
    drop(second);
    assert_eq!(
        pool.reserved(),
        0,
        "EOS released first even while its stream remains alive"
    );
}

#[tokio::test]
async fn optional_null_safe_key_temporaries_and_index_share_the_input_budget() {
    use datafusion_datasource::memory::MemorySourceConfig;
    let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Utf8, true)]));
    let values: Vec<_> = (0..256)
        .map(|row| format!("{row:04}{}", "s".repeat(4096)))
        .collect();
    let inner = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(values.clone()))],
    )
    .unwrap();
    let outer = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec![values[0].clone()]))],
    )
    .unwrap();
    let cfg = OptionalConfig {
        join_keys: vec![(0, 0)],
        inner_keep_idx: vec![],
        null_safe_keys: true,
        out_schema: schema.clone(),
        outer_schema: schema.clone(),
        inner_schema: schema.clone(),
    };
    // ~1046 KiB of rounded Arrow source capacity fits in 1060 KiB. The
    // distinct-key table plus two temporary canonical keys cannot also fit.
    // The former owned-key 2 MiB refusal is now an exact fitting control.
    for (limit, fits) in [(1060 * 1024, false), (2 * MIB, true), (16 * MIB, true)] {
        let (context, pool) = context(limit);
        let budget = optional_memory::Budget::new(&context.task_ctx());
        let outer =
            MemorySourceConfig::try_new_exec(&[vec![outer.clone()]], schema.clone(), None).unwrap();
        let inner =
            MemorySourceConfig::try_new_exec(&[vec![inner.clone()]], schema.clone(), None).unwrap();
        let outer = budget.collect(outer, context.task_ctx()).await.unwrap();
        let inner = budget.collect(inner, context.task_ctx()).await.unwrap();
        let output = optional_memory::join(&cfg, outer, inner, &budget).and_then(|mut prepared| {
            prepared
                .next(&budget, usize::MAX)
                .map(|output| output.unwrap())
        });
        if fits {
            let output = output.unwrap();
            assert_eq!(output.num_rows(), 1);
            assert_eq!(
                output
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0),
                values[0]
            );
        } else {
            let error = output.unwrap_err();
            assert!(error.to_string().contains("OptionalMatch index"), "{error}");
            resource_error(error);
        }
        drop(budget);
        assert_eq!(pool.reserved(), 0);
    }
}

#[tokio::test]
async fn optional_unaudited_arrow_representation_refuses_before_copy_with_canonical_control() {
    use arrow::array::StringViewArray;
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::UInt64, false),
        Field::new("payload", DataType::Utf8View, false),
    ]));
    let view = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![1])),
            Arc::new(StringViewArray::from(vec!["value"])),
        ],
    )
    .unwrap();
    let plan = optional(vec![view.clone()], vec![view]);
    let (context, pool) = context(MIB);
    let error = collect(plan, context.task_ctx()).await.err().unwrap();
    assert!(
        error
            .to_string()
            .contains("cannot bound Arrow kernel for Utf8View")
    );
    resource_error(error);
    assert_eq!(pool.reserved(), 0);
    let output = collect(
        optional(vec![batch(1, 5)], vec![batch(1, 5)]),
        context.task_ctx(),
    )
    .await
    .unwrap();
    assert_eq!(output[0].num_rows(), 1);
    assert_eq!(
        output[0]
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "xxxxx"
    );
    assert_eq!(pool.reserved(), 0);
}

#[tokio::test]
async fn optional_nested_empty_children_charge_the_inherited_builder_capacity() {
    use arrow::array::{ListArray, UInt8Array};
    use arrow::buffer::OffsetBuffer;
    let byte = Arc::new(Field::new("item", DataType::UInt8, false));
    let list = Arc::new(Field::new("item", DataType::List(byte.clone()), false));
    let empty_children = |count: usize| {
        Arc::new(ListArray::new(
            byte.clone(),
            OffsetBuffer::new(vec![0_i32; count + 1].into()),
            Arc::new(UInt8Array::from(Vec::<u8>::new())),
            None,
        ))
    };
    let payload = Arc::new(ListArray::new(
        list.clone(),
        OffsetBuffer::new(vec![0_i32, 1024].into()),
        empty_children(1024),
        None,
    ));
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::UInt64, false),
        Field::new("payload", payload.data_type().clone(), false),
    ]));
    let outer = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(UInt64Array::from(vec![1])), payload],
    )
    .unwrap();
    let inner = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(vec![1; 100])),
            Arc::new(ListArray::new(
                list,
                OffsetBuffer::new(vec![0_i32; 101].into()),
                empty_children(0),
                None,
            )),
        ],
    )
    .unwrap();
    let plan = optional(vec![outer], vec![inner]);
    let (tight, pool) = context(64 * 1024);
    let error = collect(Arc::clone(&plan), tight.task_ctx())
        .await
        .err()
        .unwrap();
    assert!(
        error.to_string().contains("OptionalMatch output"),
        "{error}"
    );
    resource_error(error);
    assert_eq!(pool.reserved(), 0);
    let (roomy, pool) = context(32 * MIB);
    let mut stream = plan.execute(0, roomy.task_ctx()).unwrap();
    use futures::StreamExt;
    let output = stream.next().await.unwrap().unwrap();
    assert_eq!(output.num_rows(), 100);
    let parent = output
        .column(1)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let children = parent
        .values()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(children.len(), 102_400);
    assert_eq!(children.values().len(), 0);
    assert!(
        children.values().get_buffer_memory_size() >= 102_400,
        "the empty grandchild still retains the inherited slot hint"
    );
    assert!(pool.reserved() >= output.get_array_memory_size());
    drop(stream);
    assert_eq!(pool.reserved(), 0);
}

fn nested_rows(
    keys: Vec<u64>,
    offsets: Vec<i32>,
    ids: Vec<u64>,
    texts: Vec<Option<&str>>,
) -> RecordBatch {
    use arrow::array::{ListArray, StructArray};
    use arrow::buffer::OffsetBuffer;
    let fields = arrow::datatypes::Fields::from(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("text", DataType::Utf8, true),
    ]);
    let values = StructArray::try_new(
        fields.clone(),
        vec![
            Arc::new(UInt64Array::from(ids)),
            Arc::new(StringArray::from(texts)),
        ],
        None,
    )
    .unwrap();
    let payload = Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Struct(fields), false)),
        OffsetBuffer::new(offsets.into()),
        Arc::new(values),
        None,
    ));
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::UInt64, false),
        Field::new("payload", payload.data_type().clone(), false),
    ]));
    RecordBatch::try_new(schema, vec![Arc::new(UInt64Array::from(keys)), payload]).unwrap()
}

#[tokio::test]
async fn optional_nested_multi_batch_concat_admits_copies_and_preserves_every_cell() {
    use arrow::array::{ListArray, StructArray};
    let plan = optional(
        vec![
            nested_rows(vec![1], vec![0, 2], vec![10, 11], vec![Some("a"), None]),
            nested_rows(vec![2], vec![0, 1], vec![20], vec![Some("b")]),
        ],
        vec![
            nested_rows(vec![1], vec![0, 1], vec![30], vec![Some("c")]),
            nested_rows(vec![1], vec![0, 2], vec![40, 41], vec![None, Some("d")]),
        ],
    );
    let (tight, pool) = context(16 * 1024);
    let error = collect(Arc::clone(&plan), tight.task_ctx())
        .await
        .err()
        .expect("recursive concat copies exceed remaining query memory");
    assert!(
        error.to_string().contains("OptionalMatch concat"),
        "{error}"
    );
    resource_error(error);
    assert_eq!(pool.reserved(), 0);
    let (roomy, pool) = context(MIB);
    let output = collect(plan, roomy.task_ctx()).await.unwrap();
    assert_eq!(output.len(), 1);
    let output = &output[0];
    assert_eq!(output.num_rows(), 3);
    assert_eq!(
        output
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[1, 1, 2]
    );
    let outer = output
        .column(1)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let inner = output
        .column(2)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    for (list, row, ids, texts) in [
        (outer, 0, vec![10, 11], vec![Some("a"), None]),
        (outer, 1, vec![10, 11], vec![Some("a"), None]),
        (outer, 2, vec![20], vec![Some("b")]),
        (inner, 0, vec![30], vec![Some("c")]),
        (inner, 1, vec![40, 41], vec![None, Some("d")]),
    ] {
        let values = list.value(row);
        let values = values.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(
            values
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .values()
                .as_ref(),
            ids.as_slice()
        );
        assert_eq!(
            values
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            texts
        );
    }
    assert_eq!(inner.null_count(), 1);
    assert!(inner.is_null(2));
    assert!(pool.peak_gauge().load(Ordering::Relaxed) <= MIB);
    assert_eq!(pool.reserved(), 0);
}

#[tokio::test]
async fn optional_concat_charges_full_sources_but_only_copies_visible_slices() {
    use arrow::array::NullArray;
    let rows = 524_288;
    let source = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::UInt64, false),
            Field::new("payload", DataType::Null, true),
        ])),
        vec![
            Arc::new(UInt64Array::from_iter_values(
                0..u64::try_from(rows).unwrap(),
            )),
            Arc::new(NullArray::new(rows)),
        ],
    )
    .unwrap();
    let retained = source.get_array_memory_size();
    let plan = optional(
        vec![source.slice(17, 1), source.slice(511, 1)],
        vec![RecordBatch::new_empty(source.schema())],
    );
    let (context, pool) = context(16 * MIB);
    let output = collect(plan, context.task_ctx()).await.unwrap();
    assert_eq!(output[0].num_rows(), 2);
    assert_eq!(
        output[0]
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[17, 511]
    );
    assert_eq!(output[0].column(1).logical_null_count(), 2);
    assert_eq!(output[0].column(2).logical_null_count(), 2);
    let peak = pool.peak_gauge().load(Ordering::Relaxed);
    assert!(
        peak >= 2 * retained,
        "both full source capacities remain admitted before concat"
    );
    assert!(peak <= 16 * MIB);
    assert_eq!(pool.reserved(), 0);
}

#[tokio::test]
async fn optional_windows_keep_sources_and_yielded_output_charged_until_eos_or_drop() {
    use futures::StreamExt;
    for cancel in [false, true] {
        let outer = batch(3, 4096);
        let inner = batch(3, 32);
        // MemorySource splits at the context batch size, then concat compacts
        // its source slices. Regardless of spare capacity, all original visible
        // source payload and offsets must coexist with the emitted window.
        let source_bytes = 3 * 4096 + 3 * 32 + 3 * 8 + 2 * 4 * 4;
        let plan = optional(vec![outer], vec![inner]);
        let (context, pool) = context(MIB);
        let context = SessionContext::new_with_config_rt(
            SessionConfig::new().with_batch_size(2),
            context.runtime_env(),
        );
        let mut stream = plan.execute(0, context.task_ctx()).unwrap();
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first.num_rows(), 2);
        assert!(
            pool.reserved() >= source_bytes + first.get_array_memory_size(),
            "held {} source {} output {}",
            pool.reserved(),
            source_bytes,
            first.get_array_memory_size()
        );
        if !cancel {
            let mut count = first.num_rows();
            while let Some(output) = stream.next().await {
                let output = output.unwrap();
                assert!(output.num_rows() <= 2);
                for (index, width) in [(1, 4096), (2, 32)] {
                    assert!(
                        output
                            .column(index)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap()
                            .iter()
                            .all(|value| value == Some("x".repeat(width).as_str()))
                    );
                }
                count += output.num_rows();
            }
            assert_eq!(count, 9);
            assert_eq!(pool.reserved(), 0, "EOS drops source authority as well");
        }
        drop(stream);
        assert_eq!(pool.reserved(), 0);
    }
}

#[tokio::test]
async fn optional_tiny_windows_charge_full_pair_bitmap_aliases_in_nested_null_output() {
    use arrow::array::StructArray;
    use futures::StreamExt;
    let rows = 100_003;
    let fields: arrow::datatypes::Fields = vec![
        Field::new("text", DataType::Utf8, false),
        Field::new("number", DataType::UInt64, false),
    ]
    .into();
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::UInt64, false),
        Field::new("payload", DataType::Struct(fields), false),
    ]));
    // Keep the outer payload primitive: this exercises the nullable nested
    // inner output without repeatedly scanning 100K unrelated source strings
    // in the deliberately conservative nested-copy preflight.
    let outer_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::UInt64, false),
        Field::new("payload", DataType::UInt64, false),
    ]));
    let outer = RecordBatch::try_new(
        outer_schema.clone(),
        vec![
            Arc::new(UInt64Array::from_iter_values(
                0..u64::try_from(rows).unwrap(),
            )),
            Arc::new(UInt64Array::from_iter_values(
                0..u64::try_from(rows).unwrap(),
            )),
        ],
    )
    .unwrap();
    let plan = optional(
        vec![outer.clone()],
        vec![RecordBatch::new_empty(schema.clone())],
    );
    // The real seed stream emits an unsplit batch. DataSourceExec otherwise
    // splits this fixture into 50K slices sharing the whole source allocation,
    // correctly exhausting collection before the output-window boundary.
    let outer = Arc::new(CorrelatedSeedExec::with_rows(
        19,
        outer_schema,
        Some(SeedRows::new(vec![outer])),
    ));
    let inner = plan.children()[1].clone();
    let plan = plan.with_new_children(vec![outer, inner]).unwrap();
    let (context, pool) = context(16 * MIB);
    let context = SessionContext::new_with_config_rt(
        SessionConfig::new().with_batch_size(2),
        context.runtime_env(),
    );
    let mut stream = plan.execute(0, context.task_ctx()).unwrap();
    let mut count = 0;
    while let Some(output) = stream.next().await {
        let output = output.unwrap();
        assert!(output.num_rows() <= 2);
        let keys = output
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let numbers = output
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let missing = output
            .column(2)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(missing.null_count(), output.num_rows());
        for child in missing.columns() {
            assert_eq!(child.logical_null_count(), output.num_rows());
        }
        for row in 0..output.num_rows() {
            assert_eq!(keys.value(row), u64::try_from(count + row).unwrap());
            assert_eq!(numbers.value(row), keys.value(row));
        }
        count += output.num_rows();
    }
    assert_eq!(count, rows);
    assert_eq!(pool.reserved(), 0);
    assert!(pool.peak_gauge().load(Ordering::Relaxed) <= 16 * MIB);
}

#[tokio::test]
async fn optional_compound_keys_borrow_outer_values_and_match_the_full_materialized_oracle() {
    use arrow::array::ListArray;
    use arrow::datatypes::UInt64Type;
    use datafusion_datasource::memory::MemorySourceConfig;
    let wide = ["a".repeat(8192), "b".repeat(8192)];
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, true),
        Field::new(
            "nested",
            DataType::List(Arc::new(Field::new_list_field(DataType::UInt64, true))),
            true,
        ),
        Field::new("payload", DataType::UInt64, false),
    ]));
    let make = |rows: Vec<(Option<usize>, Option<Vec<Option<u64>>>, u64)>| {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(
                    rows.iter()
                        .map(|(key, _, _)| key.map(|key| wide[key].as_str()))
                        .collect::<Vec<_>>(),
                )),
                Arc::new(ListArray::from_iter_primitive::<UInt64Type, _, _>(
                    rows.iter().map(|(_, nested, _)| nested.clone()),
                )),
                Arc::new(UInt64Array::from(
                    rows.iter()
                        .map(|(_, _, payload)| *payload)
                        .collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    };
    let outer = make(vec![
        (Some(0), Some(vec![Some(1), None]), 10),
        (None, None, 11),
        (Some(0), Some(vec![Some(1), None]), 12),
        (Some(1), Some(vec![Some(2)]), 13),
        (Some(1), Some(vec![Some(9)]), 14),
    ]);
    let inner: Vec<_> = (0..4)
        .map(|batch| {
            make(
                (0..128)
                    .map(|row| {
                        let payload = batch * 128 + row;
                        match row % 4 {
                            0 => (Some(0), Some(vec![Some(1), None]), payload),
                            1 => (None, None, payload),
                            2 => (Some(0), Some(vec![Some(3)]), payload),
                            _ => (Some(1), Some(vec![Some(2)]), payload),
                        }
                    })
                    .collect(),
            )
        })
        .collect();
    let output_schema = Arc::new(Schema::new(vec![
        schema.field(0).clone(),
        schema.field(1).clone(),
        schema.field(2).clone(),
        Field::new("optional_payload", DataType::UInt64, true),
    ]));
    let mut results = Vec::new();
    for mapped in [false, true] {
        let limit = if mapped { 512 * 1024 } else { 32 * MIB };
        let (context, pool) = context(limit);
        let budget = optional_memory::Budget::new(&context.task_ctx());
        let mut cfg = OptionalConfig {
            join_keys: vec![(0, 0), (1, 1)],
            inner_keep_idx: vec![2],
            null_safe_keys: true,
            out_schema: output_schema.clone(),
            outer_schema: schema.clone(),
            inner_schema: schema.clone(),
        };
        let source =
            MemorySourceConfig::try_new_exec(&[vec![outer.clone()]], schema.clone(), None).unwrap();
        let outer = budget.collect(source, context.task_ctx()).await.unwrap();
        let source =
            MemorySourceConfig::try_new_exec(&[inner.clone()], schema.clone(), None).unwrap();
        let inner = if mapped {
            budget
                .collect_mapped(source, context.task_ctx(), &mut cfg, &outer.batches[0])
                .await
                .unwrap()
        } else {
            budget.collect(source, context.task_ctx()).await.unwrap()
        };
        let mut joined = optional_memory::join(&cfg, outer, inner, &budget).unwrap();
        let mut output = Vec::new();
        while let Some(batch) = joined.next(&budget, 8).unwrap() {
            let bytes = optional_memory::batch_bytes(&batch).unwrap();
            output.push(batch);
            budget.release(bytes);
        }
        assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 513);
        results.push(output);
        drop(joined);
        drop(budget);
        assert_eq!(pool.reserved(), 0);
        assert!(pool.peak_gauge().load(Ordering::Relaxed) <= limit);
    }
    assert_eq!(
        results[0], results[1],
        "same cells, nulls, duplicate multiplicity and source order"
    );
}

#[tokio::test]
async fn optional_compound_key_collection_cancellation_releases_representatives_and_seed() {
    use futures::StreamExt;
    let (plan, weak) = super::tests::interrupted_optional(false);
    let mut plan = Arc::try_unwrap(plan).unwrap();
    // Repeating a key position is a valid compound tuple and reaches the mapped
    // path while retaining the existing deliberately pending correlated source.
    plan.join_keys.push((0, 0));
    let baseline = weak.strong_count();
    let (context, pool) = context(MIB);
    let mut stream = plan.execute(0, context.task_ctx()).unwrap();
    assert!(futures::poll!(stream.next()).is_pending());
    assert!(pool.reserved() > 0);
    drop(stream);
    assert_eq!(pool.reserved(), 0);
    assert_eq!(weak.strong_count(), baseline);
    let executed = plan.executed_inner.get().unwrap();
    let seed = executed.children()[0]
        .downcast_ref::<CorrelatedSeedExec>()
        .unwrap();
    assert!(seed.rows.as_ref().unwrap().get().is_none());
}
