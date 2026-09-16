//! Public Arrow result shaping and per-query schema metadata.

use super::{ExecutionResult, GfError, OntologyMode};
use arrow::datatypes::SchemaRef;
use graphforge_ontology::OntologyHandle;
use std::sync::Arc;

/// Shape an [`ExecutionResult`] for the public API:
/// - drop internal surrogate identity columns (provenance-marked / UInt64 scan
///   keys — never by final field name alone; see #703 / #719),
/// - attach query metadata (`graphforge.query_id`, `ontology_version`,
///   `ir_version`, `ontology_mode`) to the schema.
pub(super) fn shape_result(
    result: ExecutionResult,
    mode: OntologyMode,
    ontology: Option<&OntologyHandle>,
) -> Result<ExecutionResult, GfError> {
    let ExecutionResult {
        schema,
        batches,
        stats,
        side_effects,
        mutation_receipt,
    } = result;
    let shaper = Shaper::new(&schema, mode, ontology);
    let new_batches = batches
        .iter()
        .map(|batch| {
            shaper
                .apply(batch)
                .map_err(|error| GfError::Execution(error.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ExecutionResult {
        schema: shaper.schema,
        batches: new_batches,
        stats,
        side_effects,
        mutation_receipt,
    })
}

/// Per-batch output shaper: prunes internal surrogate identity columns and
/// re-stamps the public schema (kept fields + query metadata). Built once from
/// the raw result schema, then applied to each batch — shared by the collected
/// ([`shape_result`]) and streaming ([`shape_stream`]) paths.
struct Shaper {
    /// Source-batch column indices to keep, in output order.
    keep: Vec<usize>,
    /// True when at least one internal surrogate column was dropped from the
    /// source schema. Distinguishes surrogate-only projections (#703: preserve
    /// row count) from already-empty schemas such as void `CALL` unit rows
    /// (public result must stay empty for TCK Call1).
    dropped_internal_surrogates: bool,
    /// The pruned, metadata-stamped public schema.
    schema: SchemaRef,
}

impl Shaper {
    fn new(schema: &SchemaRef, mode: OntologyMode, ontology: Option<&OntologyHandle>) -> Self {
        let dropped_internal_surrogates = schema
            .fields()
            .iter()
            .any(|f| graphforge_storage::is_internal_surrogate_field(f));
        let keep: Vec<usize> = schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| !graphforge_storage::is_internal_surrogate_field(f))
            .map(|(i, _)| i)
            .collect();
        let kept_fields: Vec<_> = keep.iter().map(|&i| schema.field(i).clone()).collect();
        let new_schema = Arc::new(arrow::datatypes::Schema::new_with_metadata(
            kept_fields,
            result_metadata(mode, ontology),
        ));
        Self {
            keep,
            dropped_internal_surrogates,
            schema: new_schema,
        }
    }

    fn apply(
        &self,
        batch: &arrow::record_batch::RecordBatch,
    ) -> Result<arrow::record_batch::RecordBatch, arrow::error::ArrowError> {
        if self.keep.iter().any(|index| *index >= batch.num_columns()) {
            return Err(arrow::error::ArrowError::SchemaError(format!(
                "result batch has {} columns but shaper requires source indices {:?}",
                batch.num_columns(),
                self.keep
            )));
        }
        let cols: Vec<_> = self.keep.iter().map(|&i| batch.column(i).clone()).collect();
        // Surrogate-only projections must keep their logical row count (#703).
        // Already-empty schemas (void CALL unit rows) must stay publicly empty
        // so TCK Call1 "yields no results" scenarios do not regress.
        let row_count = if self.keep.is_empty() && !self.dropped_internal_surrogates {
            0
        } else {
            batch.num_rows()
        };
        arrow::record_batch::RecordBatch::try_new_with_options(
            self.schema.clone(),
            cols,
            &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(row_count)),
        )
    }
}

/// Apply the public output shaping (UUID-only columns + schema metadata) to a
/// streaming result, mapping each batch as it flows. The returned stream
/// advertises the shaped schema up front (the `RecordBatchReader` contract that
/// the bindings rely on — #587).
pub(super) fn shape_stream(
    stream: graphforge_exec::SendableRecordBatchStream,
    mode: OntologyMode,
    ontology: Option<&OntologyHandle>,
) -> graphforge_exec::SendableRecordBatchStream {
    use futures::StreamExt;

    let shaper = Shaper::new(&stream.schema(), mode, ontology);
    let out_schema = shaper.schema.clone();
    let mapped = stream.map(move |item| {
        item.and_then(|batch| {
            shaper.apply(&batch).map_err(|error| {
                datafusion::error::DataFusionError::ArrowError(Box::new(error), None)
            })
        })
    });
    Box::pin(datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(out_schema, mapped))
}

/// Build the schema-level metadata attached to every public result.
fn result_metadata(
    mode: OntologyMode,
    ontology: Option<&OntologyHandle>,
) -> std::collections::HashMap<String, String> {
    let mut meta = std::collections::HashMap::new();
    meta.insert(
        "graphforge.query_id".to_owned(),
        graphforge_core::uuid::to_string(&graphforge_core::uuid::new_v7()),
    );
    meta.insert(
        "graphforge.ir_version".to_owned(),
        graphforge_ir::IrVersion::CURRENT.to_string(),
    );
    meta.insert(
        "graphforge.ontology_mode".to_owned(),
        format!("{mode:?}").to_lowercase(),
    );
    if let Some(handle) = ontology {
        meta.insert(
            "graphforge.ontology_version".to_owned(),
            format!("{}:{}", handle.version(), handle.checksum()),
        );
    }
    meta
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;

    #[test]
    fn shaper_schema_mismatch_is_an_error_not_a_panic() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};

        let source_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));
        let batch = arrow::record_batch::RecordBatch::try_from_iter(vec![(
            "a",
            Arc::new(Int64Array::from(vec![1])) as arrow::array::ArrayRef,
        )])
        .unwrap();
        let shaper = Shaper::new(&source_schema, OntologyMode::Exploratory, None);

        let error = shaper.apply(&batch).expect_err("schema mismatch must fail");
        assert!(matches!(error, arrow::error::ArrowError::SchemaError(_)));
    }

    #[test]
    fn shaper_preserves_user_aliases_named_like_surrogates() {
        use arrow::array::{Int64Array, StringArray, UInt64Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use graphforge_storage::{INTERNAL_SURROGATE_META_KEY, is_internal_surrogate_field};

        let marked_node = Field::new("node_id", DataType::UInt64, false).with_metadata(
            [(INTERNAL_SURROGATE_META_KEY.to_owned(), "true".to_owned())]
                .into_iter()
                .collect(),
        );
        let marked_edge = Field::new("edge_id", DataType::UInt64, false).with_metadata(
            [(INTERNAL_SURROGATE_META_KEY.to_owned(), "true".to_owned())]
                .into_iter()
                .collect(),
        );
        assert!(is_internal_surrogate_field(&marked_node));
        assert!(is_internal_surrogate_field(&marked_edge));

        let source_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("node_id", DataType::Int64, false),
            marked_node,
            Field::new("edge_id", DataType::FixedSizeBinary(16), false),
            marked_edge,
        ]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            source_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some("Alice")])) as arrow::array::ArrayRef,
                Arc::new(Int64Array::from(vec![42])),
                Arc::new(UInt64Array::from(vec![7])),
                Arc::new(
                    arrow::array::FixedSizeBinaryArray::try_from_iter(std::iter::once(
                        [0u8; 16].as_slice(),
                    ))
                    .unwrap(),
                ),
                Arc::new(UInt64Array::from(vec![9])),
            ],
        )
        .unwrap();
        let shaper = Shaper::new(&source_schema, OntologyMode::Exploratory, None);
        let shaped = shaper.apply(&batch).expect("shape user aliases");
        assert_eq!(
            shaped
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>(),
            ["name", "node_id", "edge_id"]
        );
        assert_eq!(shaped.num_rows(), 1);
        assert_eq!(
            shaped
                .column_by_name("node_id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            42
        );
    }

    #[test]
    fn shaper_preserves_row_count_when_only_surrogates_remain() {
        use arrow::array::UInt64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use graphforge_storage::INTERNAL_SURROGATE_META_KEY;

        let marked = Field::new("node_id", DataType::UInt64, false).with_metadata(
            [(INTERNAL_SURROGATE_META_KEY.to_owned(), "true".to_owned())]
                .into_iter()
                .collect(),
        );
        let source_schema = Arc::new(Schema::new(vec![marked]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            source_schema.clone(),
            vec![Arc::new(UInt64Array::from(vec![1, 2, 3])) as arrow::array::ArrayRef],
        )
        .unwrap();
        let shaper = Shaper::new(&source_schema, OntologyMode::Exploratory, None);
        let shaped = shaper.apply(&batch).expect("zero-column shape");
        assert_eq!(shaped.num_columns(), 0);
        assert_eq!(shaped.num_rows(), 3);
    }

    #[test]
    fn shaper_collapses_void_unit_row_without_surrogate_drops() {
        use arrow::datatypes::Schema;

        // Empty-plan / void CALL execution yields a zero-column unit row. Public
        // shaping must report an empty result (TCK Call1), not preserve the
        // internal unit row when no surrogate columns were dropped.
        let source_schema = Arc::new(Schema::empty());
        let batch = arrow::record_batch::RecordBatch::try_new_with_options(
            source_schema.clone(),
            vec![],
            &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(1)),
        )
        .unwrap();
        assert_eq!(batch.num_rows(), 1);
        let shaper = Shaper::new(&source_schema, OntologyMode::Exploratory, None);
        let shaped = shaper.apply(&batch).expect("void shape");
        assert_eq!(shaped.num_columns(), 0);
        assert_eq!(shaped.num_rows(), 0);
    }
}
