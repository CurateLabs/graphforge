use super::*;
use arrow::array::{AsArray, StringArray, UInt64Array};
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
                .collect_mapped(source, context.task_ctx(), &mut cfg, &outer)
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

#[tokio::test]
async fn optional_repeated_payload_dictionary_preserves_changed_cells_nulls_and_row_order() {
    use arrow::array::ListArray;
    use arrow::datatypes::UInt64Type;
    use datafusion_datasource::memory::MemorySourceConfig;
    let names = ["a".repeat(8192), "b".repeat(8192)];
    let list_type = DataType::List(Arc::new(Field::new_list_field(DataType::UInt64, true)));
    let outer_schema = Arc::new(Schema::new(vec![
        Field::new("node_id", DataType::UInt64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("nested", list_type.clone(), true),
    ]));
    let outer = RecordBatch::try_new(
        outer_schema.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![1, 1, 2, 3])),
            Arc::new(StringArray::from(vec![
                names[0].as_str(),
                names[0].as_str(),
                names[1].as_str(),
                "unmatched",
            ])),
            Arc::new(ListArray::from_iter_primitive::<UInt64Type, _, _>(vec![
                Some(vec![Some(7)]),
                Some(vec![Some(7)]),
                None,
                Some(vec![Some(9)]),
            ])),
        ],
    )
    .unwrap();
    let inner_schema = Arc::new(Schema::new(vec![
        Field::new("anchor_id", DataType::UInt64, false),
        Field::new("anchor_name", DataType::Utf8, false),
        Field::new("node_id", DataType::UInt64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("nested", list_type.clone(), true),
        Field::new("extra", list_type, true),
    ]));
    let inner: Vec<_> = (0..8)
        .map(|batch| {
            RecordBatch::try_new(
                inner_schema.clone(),
                vec![
                    Arc::new(UInt64Array::from(vec![1; 128])),
                    Arc::new(StringArray::from(vec![names[0].as_str(); 128])),
                    Arc::new(UInt64Array::from_iter_values(
                        (0..128).map(|row| if row % 2 == 0 { 1 } else { 2 }),
                    )),
                    Arc::new(StringArray::from(
                        (0..128)
                            .map(|row| {
                                if batch == 1 && row == 0 {
                                    "changed name with the same node id"
                                } else {
                                    names[row % 2].as_str()
                                }
                            })
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(ListArray::from_iter_primitive::<UInt64Type, _, _>(
                        (0..128).map(|row| (row % 2 == 0).then(|| vec![Some(7)])),
                    )),
                    Arc::new(ListArray::from_iter_primitive::<UInt64Type, _, _>(
                        (0..128).map(|row| (batch == 1 && row == 1).then(|| vec![Some(99)])),
                    )),
                ],
            )
            .unwrap()
        })
        .collect();
    let keep = vec![4, 3, 5, 2];
    let mut fields: Vec<_> = outer_schema.fields().iter().cloned().collect();
    fields.extend(
        keep.iter()
            .map(|&index| Arc::new(inner_schema.field(index).clone().with_nullable(true))),
    );
    let output_schema = Arc::new(Schema::new(fields));
    let mut results = Vec::new();
    for mapped in [false, true] {
        let limit = if mapped { 4 * MIB } else { 64 * MIB };
        let (context, pool) = context(limit);
        let budget = optional_memory::Budget::new(&context.task_ctx());
        let mut cfg = OptionalConfig {
            join_keys: vec![(0, 0), (1, 1)],
            inner_keep_idx: keep.clone(),
            null_safe_keys: true,
            out_schema: output_schema.clone(),
            outer_schema: outer_schema.clone(),
            inner_schema: inner_schema.clone(),
        };
        let source =
            MemorySourceConfig::try_new_exec(&[vec![outer.clone()]], outer_schema.clone(), None)
                .unwrap();
        let outer = budget.collect(source, context.task_ctx()).await.unwrap();
        let source =
            MemorySourceConfig::try_new_exec(&[inner.clone()], inner_schema.clone(), None).unwrap();
        let inner = if mapped {
            budget
                .collect_mapped(source, context.task_ctx(), &mut cfg, &outer)
                .await
                .unwrap()
        } else {
            budget.collect(source, context.task_ctx()).await.unwrap()
        };
        assert_eq!(inner.logical_rows(), 1024);
        let retained_outer = outer.batches[0].get_array_memory_size();
        // One authority plus only the mixed batch containing changed cells.
        if mapped {
            assert_eq!(inner.batches.len(), 2);
        }
        let mut joined = optional_memory::join(&cfg, outer, inner, &budget).unwrap();
        assert!(pool.reserved() >= retained_outer);
        let mut output = Vec::new();
        while let Some(batch) = joined.next(&budget, 8).unwrap() {
            assert!(pool.reserved() >= retained_outer + batch.get_array_memory_size());
            budget.release(optional_memory::batch_bytes(&batch).unwrap());
            output.push(batch);
        }
        assert_eq!(
            output.iter().map(RecordBatch::num_rows).sum::<usize>(),
            2050
        );
        results.push(output);
        drop(joined);
        drop(budget);
        assert_eq!(pool.reserved(), 0);
        assert!(pool.peak_gauge().load(Ordering::Relaxed) <= limit);
    }
    assert_eq!(results[0], results[1]);
}

#[tokio::test]
async fn optional_payload_dictionary_cancellation_releases_its_authority() {
    use futures::StreamExt;
    let (original, weak) = super::tests::interrupted_optional(false);
    let logical = |name, schema| {
        Arc::new(
            LogicalPlanBuilder::scan(name, Arc::new(LogicalTableSource::new(schema)), None)
                .unwrap()
                .build()
                .unwrap(),
        )
    };
    let node = OptionalMatchNode::new(
        logical("outer", original.outer.schema()),
        logical("inner", original.inner.schema()),
        vec![(0, 0), (0, 0)],
        vec![0],
    )
    .with_correlated_seed(7);
    let plan = OptionalMatchExec::new(&node, original.outer.clone(), original.inner.clone());
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

#[tokio::test]
async fn optional_payload_dictionary_keeps_distinct_float_bits_in_plain_and_nested_cells() {
    use arrow::array::{ArrayRef, Float64Array, ListArray};
    use arrow::datatypes::Float64Type;
    use datafusion_datasource::memory::MemorySourceConfig;
    let outer_bits = [0_u64, 0x7ff8_0000_0000_0001];
    let inner_bits = [1_u64 << 63, 0x7ff8_0000_0000_0002];
    for nested in [false, true] {
        let floats = |bits: &[u64]| -> ArrayRef {
            if nested {
                Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>(
                    bits.iter()
                        .map(|&bits| Some(vec![Some(f64::from_bits(bits))])),
                ))
            } else {
                Arc::new(Float64Array::from_iter_values(
                    bits.iter().map(|&bits| f64::from_bits(bits)),
                ))
            }
        };
        let outer_values = floats(&outer_bits);
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::UInt64, false),
            Field::new("other_key", DataType::UInt64, false),
            Field::new("value", outer_values.data_type().clone(), false),
        ]));
        let make = |values| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(UInt64Array::from(vec![1, 2])),
                    Arc::new(UInt64Array::from(vec![10, 20])),
                    values,
                ],
            )
            .unwrap()
        };
        let outer = make(outer_values);
        let inner = make(floats(&inner_bits));
        let mut fields: Vec<_> = schema.fields().iter().cloned().collect();
        fields.extend(
            schema
                .fields()
                .iter()
                .map(|field| Arc::new(field.as_ref().clone().with_nullable(true))),
        );
        let mut cfg = OptionalConfig {
            join_keys: vec![(0, 0), (1, 1)],
            inner_keep_idx: vec![0, 1, 2],
            null_safe_keys: true,
            out_schema: Arc::new(Schema::new(fields)),
            outer_schema: schema.clone(),
            inner_schema: schema.clone(),
        };
        let (context, pool) = context(MIB);
        let budget = optional_memory::Budget::new(&context.task_ctx());
        let source =
            MemorySourceConfig::try_new_exec(&[vec![outer]], schema.clone(), None).unwrap();
        let outer = budget.collect(source, context.task_ctx()).await.unwrap();
        let source = MemorySourceConfig::try_new_exec(&[vec![inner]], schema, None).unwrap();
        let inner = budget
            .collect_mapped(source, context.task_ctx(), &mut cfg, &outer)
            .await
            .unwrap();
        assert_eq!(
            inner.batches.len(),
            1,
            "floating-point payloads keep their original cells"
        );
        let mut joined = optional_memory::join(&cfg, outer, inner, &budget).unwrap();
        let output = joined.next(&budget, 8).unwrap().unwrap();
        assert_eq!(output.num_rows(), 2);
        for (row, &bits) in inner_bits.iter().enumerate() {
            let values = if nested {
                output
                    .column(5)
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .unwrap()
                    .value(row)
            } else {
                Arc::clone(output.column(5))
            };
            let values = values.as_any().downcast_ref::<Float64Array>().unwrap();
            assert_eq!(values.value(if nested { 0 } else { row }).to_bits(), bits);
        }
        drop(joined);
        drop(budget);
        assert_eq!(pool.reserved(), 0);
    }
}

#[tokio::test]
async fn optional_payload_dictionary_qualifies_all_types_before_authority_construction() {
    use datafusion::physical_plan::empty::EmptyExec;
    let outer_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::UInt64, false),
        Field::new("other_key", DataType::UInt64, false),
    ]));
    let outer = RecordBatch::try_new(
        outer_schema.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![1])),
            Arc::new(UInt64Array::from(vec![2])),
        ],
    )
    .unwrap();
    let map_type = DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::UInt64, false),
                    Field::new("value", DataType::UInt64, true),
                ]
                .into(),
            ),
            false,
        )),
        false,
    );
    let inner_schema = Arc::new(Schema::new(vec![
        outer_schema.field(0).clone(),
        outer_schema.field(1).clone(),
        Field::new("unsupported", map_type, true),
    ]));
    let mut fields: Vec<_> = outer_schema.fields().iter().cloned().collect();
    fields.extend(
        inner_schema
            .fields()
            .iter()
            .map(|field| Arc::new(field.as_ref().clone().with_nullable(true))),
    );
    let mut cfg = OptionalConfig {
        join_keys: vec![(0, 0), (1, 1)],
        inner_keep_idx: vec![0, 1, 2],
        null_safe_keys: true,
        out_schema: Arc::new(Schema::new(fields)),
        outer_schema,
        inner_schema: inner_schema.clone(),
    };
    let (context, pool) = context(MIB);
    let budget = optional_memory::Budget::new(&context.task_ctx());
    let source = datafusion_datasource::memory::MemorySourceConfig::try_new_exec(
        &[vec![outer]],
        cfg.outer_schema.clone(),
        None,
    )
    .unwrap();
    let outer = budget.collect(source, context.task_ctx()).await.unwrap();
    let reserved = pool.reserved();
    let peak = pool.peak_gauge().load(Ordering::Relaxed);
    let source = Arc::new(EmptyExec::new(inner_schema));
    let error = budget
        .collect_mapped(source, context.task_ctx(), &mut cfg, &outer)
        .await
        .err()
        .unwrap();
    assert!(
        error
            .to_string()
            .contains("cannot bound Arrow kernel for Map"),
        "{error}"
    );
    resource_error(error);
    assert_eq!(
        pool.reserved(),
        reserved,
        "no authority, index or row map was allocated"
    );
    assert_eq!(pool.peak_gauge().load(Ordering::Relaxed), peak);
    drop(budget);
    assert_eq!(pool.reserved(), 0);
}

fn dictionary_alias_batches(slice: bool) -> (RecordBatch, RecordBatch) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::UInt64, false),
        Field::new("other_key", DataType::UInt64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let outer = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from_iter_values((0..256).map(|row| row / 2))),
            Arc::new(UInt64Array::from(vec![7; 256])),
            Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                "x".repeat(8192),
                256,
            ))),
        ],
    )
    .unwrap();
    let outer = if slice { outer.slice(128, 1) } else { outer };
    let mut fields: Vec<_> = schema.fields().iter().cloned().collect();
    let null_type = DataType::List(Arc::new(Field::new_list_field(DataType::UInt64, true)));
    fields.push(Arc::new(Field::new("extra", null_type.clone(), true)));
    // The incoming payload is an independent allocation. Only the materializer
    // proves equality and borrows the already owned outer allocation.
    let inner = RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        vec![
            Arc::new(UInt64Array::from_iter_values(
                outer
                    .column(0)
                    .as_primitive::<arrow::datatypes::UInt64Type>()
                    .values()
                    .iter()
                    .copied(),
            )),
            Arc::new(UInt64Array::from(vec![7; outer.num_rows()])),
            Arc::new(StringArray::from_iter_values(
                outer
                    .column(2)
                    .as_string::<i32>()
                    .iter()
                    .map(Option::unwrap),
            )),
            arrow::array::new_null_array(&null_type, outer.num_rows()),
        ],
    )
    .unwrap();
    (outer, inner)
}

fn dictionary_alias_config(outer: &RecordBatch, inner: &RecordBatch) -> OptionalConfig {
    // Duplicate payload outputs and nontrivial keep order exercise independent
    // aliases of the same outer column, as well as a newly allocated null list.
    let keep = vec![2, 0, 3, 2, 1];
    let mut fields: Vec<_> = outer.schema().fields().iter().cloned().collect();
    fields.extend(
        keep.iter()
            .map(|&index| Arc::new(inner.schema().field(index).clone().with_nullable(true))),
    );
    OptionalConfig {
        join_keys: vec![(0, 0), (1, 1)],
        inner_keep_idx: keep,
        null_safe_keys: true,
        out_schema: Arc::new(Schema::new(fields)),
        outer_schema: outer.schema(),
        inner_schema: inner.schema(),
    }
}

#[tokio::test]
async fn optional_dictionary_aliases_fit_once_and_keep_full_outer_backing_through_output() {
    use datafusion_datasource::memory::MemorySourceConfig;
    for slice in [false, true] {
        let (outer, inner) = dictionary_alias_batches(slice);
        let source_bytes = outer.get_array_memory_size();
        assert!(
            source_bytes >= 2 * MIB,
            "slices retain full source capacity"
        );
        let mut results = Vec::new();
        for mapped in [false, true] {
            let limit = if mapped { 3 * MIB } else { 32 * MIB };
            let (context, pool) = context(limit);
            let budget = optional_memory::Budget::new(&context.task_ctx());
            let mut cfg = dictionary_alias_config(&outer, &inner);
            let source = MemorySourceConfig::try_new_exec(
                &[vec![outer.clone()]],
                cfg.outer_schema.clone(),
                None,
            )
            .unwrap();
            let outer = budget.collect(source, context.task_ctx()).await.unwrap();
            let source = MemorySourceConfig::try_new_exec(
                &[vec![inner.clone()]],
                cfg.inner_schema.clone(),
                None,
            )
            .unwrap();
            let inner = if mapped {
                let inner = budget
                    .collect_mapped(source, context.task_ctx(), &mut cfg, &outer)
                    .await
                    .unwrap();
                assert_eq!(inner.batches.len(), 1, "all payload rows reuse authority");
                for index in [0, 3] {
                    assert!(Arc::ptr_eq(
                        inner.batches[0].column(index),
                        outer.batches[0].column(2)
                    ));
                }
                assert_eq!(
                    inner.batches[0].column(2).null_count(),
                    outer.batches[0].num_rows()
                );
                inner
            } else {
                budget.collect(source, context.task_ctx()).await.unwrap()
            };
            let mut joined = optional_memory::join(&cfg, outer, inner, &budget).unwrap();
            let mut output = Vec::new();
            while let Some(batch) = joined.next(&budget, 8).unwrap() {
                assert!(pool.reserved() >= source_bytes + batch.get_array_memory_size());
                assert_eq!(batch.column(5).null_count(), batch.num_rows());
                budget.release(optional_memory::batch_bytes(&batch).unwrap());
                output.push(batch);
            }
            assert_eq!(
                output.iter().map(RecordBatch::num_rows).sum::<usize>(),
                if slice { 1 } else { 512 }
            );
            results.push(output);
            drop(joined);
            drop(budget);
            assert_eq!(pool.reserved(), 0);
            assert!(pool.peak_gauge().load(Ordering::Relaxed) <= limit);
        }
        assert_eq!(results[0], results[1]);
    }
}

#[tokio::test]
async fn optional_dictionary_unused_aliases_never_release_the_outer_owner() {
    use datafusion_datasource::memory::MemorySourceConfig;
    let (outer, inner) = dictionary_alias_batches(true);
    let source_bytes = outer.get_array_memory_size();
    let (context, pool) = context(3 * MIB);
    let budget = optional_memory::Budget::new(&context.task_ctx());
    let mut cfg = dictionary_alias_config(&outer, &inner);
    let source =
        MemorySourceConfig::try_new_exec(&[vec![outer]], cfg.outer_schema.clone(), None).unwrap();
    let outer = budget.collect(source, context.task_ctx()).await.unwrap();
    let source =
        MemorySourceConfig::try_new_exec(&[vec![inner]], cfg.inner_schema.clone(), None).unwrap();
    let inner = budget
        .collect_mapped(source, context.task_ctx(), &mut cfg, &outer)
        .await
        .unwrap();
    // Prune both wide borrowed aliases and the owned null column after mapping.
    cfg.inner_keep_idx = vec![1];
    let mut fields: Vec<_> = cfg.outer_schema.fields().iter().cloned().collect();
    fields.push(Arc::new(
        cfg.inner_schema.field(1).clone().with_nullable(true),
    ));
    cfg.out_schema = Arc::new(Schema::new(fields));
    let mut joined = optional_memory::join(&cfg, outer, inner, &budget).unwrap();
    assert!(pool.reserved() >= source_bytes);
    let output = joined.next(&budget, 8).unwrap().unwrap();
    assert_eq!(output.num_rows(), 1);
    assert!(pool.reserved() >= source_bytes + output.get_array_memory_size());
    drop(output);
    drop(joined);
    drop(budget);
    assert_eq!(pool.reserved(), 0);
}

#[tokio::test]
async fn optional_dictionary_aliases_preserve_refusal_and_ordinary_concat_ownership() {
    use datafusion_datasource::memory::MemorySourceConfig;
    let (outer, inner) = dictionary_alias_batches(true);
    let source_bytes = outer.get_array_memory_size();
    let (context, pool) = context(3 * MIB);
    let budget = optional_memory::Budget::new(&context.task_ctx());
    let mut cfg = dictionary_alias_config(&outer, &inner);
    let source =
        MemorySourceConfig::try_new_exec(&[vec![outer]], cfg.outer_schema.clone(), None).unwrap();
    let outer = budget.collect(source, context.task_ctx()).await.unwrap();
    let source =
        MemorySourceConfig::try_new_exec(&[vec![inner]], cfg.inner_schema.clone(), None).unwrap();
    let inner = budget
        .collect_mapped(source, context.task_ctx(), &mut cfg, &outer)
        .await
        .unwrap();
    let reserved = pool.reserved();
    resource_error(budget.admit(2 * MIB).unwrap_err());
    assert_eq!(
        pool.reserved(),
        reserved,
        "refusal preserves existing ownership"
    );
    let error = budget.concat(inner, &cfg.inner_schema).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("borrowed concat requires retained outer")
    );
    resource_error(error);
    assert!(pool.reserved() >= source_bytes);
    drop(outer);
    drop(budget);
    assert_eq!(pool.reserved(), 0);
}

#[tokio::test]
async fn optional_dictionary_borrowing_requires_the_compact_outer_owner() {
    use datafusion_datasource::memory::MemorySourceConfig;
    let (outer, inner) = dictionary_alias_batches(true);
    let (context, pool) = context(16 * MIB);
    let budget = optional_memory::Budget::new(&context.task_ctx());
    let mut cfg = dictionary_alias_config(&outer, &inner);
    let source = MemorySourceConfig::try_new_exec(
        &[vec![outer.clone(), outer]],
        cfg.outer_schema.clone(),
        None,
    )
    .unwrap();
    let outer = budget.collect(source, context.task_ctx()).await.unwrap();
    assert_eq!(outer.batches.len(), 2);
    let source =
        MemorySourceConfig::try_new_exec(&[vec![inner]], cfg.inner_schema.clone(), None).unwrap();
    let before = pool.reserved();
    let error = budget
        .collect_mapped(source.clone(), context.task_ctx(), &mut cfg, &outer)
        .await
        .err()
        .unwrap();
    assert!(error.to_string().contains("one fully owned outer batch"));
    resource_error(error);
    assert_eq!(
        pool.reserved(),
        before,
        "no aliases or indexes precede qualification"
    );
    let outer = budget.compact(outer, &cfg.outer_schema).unwrap();
    let inner = budget
        .collect_mapped(source, context.task_ctx(), &mut cfg, &outer)
        .await
        .unwrap();
    let mut joined = optional_memory::join(&cfg, outer, inner, &budget).unwrap();
    assert_eq!(joined.next(&budget, 8).unwrap().unwrap().num_rows(), 2);
    drop(joined);
    drop(budget);
    assert_eq!(pool.reserved(), 0);
}

#[tokio::test]
async fn optional_dictionary_alias_output_cancellation_releases_sources_and_displayed_seed() {
    use datafusion_datasource::memory::MemorySourceConfig;
    use futures::StreamExt;
    let (outer, _) = dictionary_alias_batches(true);
    let source_bytes = outer.get_array_memory_size();
    let weak = Arc::downgrade(outer.column(2));
    let schema = outer.schema();
    let logical = |name| {
        Arc::new(
            LogicalPlanBuilder::scan(
                name,
                Arc::new(LogicalTableSource::new(schema.clone())),
                None,
            )
            .unwrap()
            .build()
            .unwrap(),
        )
    };
    let node = OptionalMatchNode::new(
        logical("outer"),
        logical("inner"),
        vec![(0, 0), (1, 1)],
        vec![2, 0, 1],
    )
    .with_correlated_seed(7);
    let source = MemorySourceConfig::try_new_exec(&[vec![outer]], schema.clone(), None).unwrap();
    let inner = Arc::new(CorrelatedSeedExec::with_rows(7, schema, None));
    let plan = OptionalMatchExec::new(&node, source, inner);
    let baseline = weak.strong_count();
    let (context, pool) = context(3 * MIB);
    let mut stream = plan.execute(0, context.task_ctx()).unwrap();
    let first = stream.next().await.unwrap().unwrap();
    assert_eq!(first.num_rows(), 1);
    assert!(pool.reserved() >= source_bytes + first.get_array_memory_size());
    drop(stream);
    assert_eq!(pool.reserved(), 0);
    assert_eq!(weak.strong_count(), baseline);
    let seed = plan
        .executed_inner
        .get()
        .unwrap()
        .downcast_ref::<CorrelatedSeedExec>()
        .unwrap();
    assert!(seed.rows.as_ref().unwrap().get().is_none());
}
