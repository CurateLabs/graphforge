//! Metadata-derived admission before edge Parquet decoding allocates pages.
//!
//! The estimate covers the complete selected row groups, including the rows a
//! predicate later discards. It deliberately does not equate returned Arrow
//! bytes with peak decoder memory. Readers hold compressed pages, decompressed
//! pages, dictionaries, decoder scratch and Arrow output concurrently.

use std::collections::HashSet;
use std::path::Path;

use datafusion::error::DataFusionError;
use graphforge_core::OntologyMode;
use parquet::basic::Type;
use parquet::file::metadata::{ColumnChunkMetaData, ParquetStatisticsPolicy};
use parquet::file::statistics::Statistics;

use crate::AuthenticatedPropertyInventory;

/// Conservative transient admission for an admitted edge-record read.
///
/// Reserve these bytes in the caller's query pool *before* calling the edge
/// reader, retain that reservation through decoding, then release it after
/// replacing it with the returned batches' reservation. The bound is four
/// times compressed pages + uncompressed pages + reconstructed values and
/// decoder levels, plus 64 KiB per column chunk for reader/buffer overhead.
/// Dictionary byte arrays use their unencoded size statistics; older files
/// use `num_values * uncompressed_size`, which also bounds repeated large
/// dictionary values. Missing or invalid metadata fails typed before decode.
///
/// # Errors
/// Returns storage errors or a named resource exhaustion when sizes cannot be
/// represented. This is a decoder admission bound, not a bound on process RSS,
/// allocator retention, catalog inventory, or later operator allocations.
#[allow(clippy::implicit_hasher)]
pub fn edge_decode_admission_bytes(
    inventory: &AuthenticatedPropertyInventory,
    rel_name: &str,
    mode: OntologyMode,
    edge_ids: &HashSet<u64>,
    projection: &[usize],
) -> Result<usize, DataFusionError> {
    if edge_ids.is_empty() {
        return Ok(0);
    }
    let union = rel_name == "*" && matches!(mode, OntologyMode::Advisory | OntologyMode::Strict);
    let route = match mode {
        OntologyMode::Exploratory => "_exploratory",
        OntologyMode::Advisory | OntologyMode::Strict => rel_name,
    };
    let mut total = 0usize;
    for (_, path) in inventory.edge_files(if union { None } else { Some(route) }) {
        total = total
            .checked_add(file_bytes(
                &path,
                edge_ids,
                if union { None } else { Some(projection) },
            )?)
            .ok_or_else(exhausted)?;
    }
    Ok(total)
}

fn exhausted() -> DataFusionError {
    DataFusionError::ResourcesExhausted(
        "ExpandExec edge Parquet decode: metadata cannot bound decoder allocation".into(),
    )
}

fn nonnegative(value: i64) -> Result<usize, DataFusionError> {
    usize::try_from(value).map_err(|_| exhausted())
}

fn file_bytes(
    path: &Path,
    ids: &HashSet<u64>,
    projection: Option<&[usize]>,
) -> Result<usize, DataFusionError> {
    use parquet::arrow::arrow_reader::ArrowReaderOptions;
    let options =
        ArrowReaderOptions::new().with_size_stats_policy(ParquetStatisticsPolicy::KeepAll);
    // The common reader preflights the encoded footer's 16 MiB ceiling before
    // decoding metadata, then validates row-group and column byte ceilings.
    let reader = crate::catalog::admitted_parquet_with_options(path, options)?;
    let key = reader
        .schema()
        .index_of("edge_id")
        .map_err(|_| exhausted())?;
    let mut columns = projection.map_or_else(
        || (0..reader.schema().fields().len()).collect::<Vec<_>>(),
        <[usize]>::to_vec,
    );
    columns.push(key);
    columns.sort_unstable();
    columns.dedup();
    if columns
        .iter()
        .any(|&index| index >= reader.schema().fields().len())
    {
        return Err(DataFusionError::Execution(
            "edge decode projection index is out of range".into(),
        ));
    }
    // Canonical edge topology consists of one flat primitive leaf per field.
    // Refuse a noncanonical nested field rather than underestimate its buffers.
    if reader.parquet_schema().num_columns() != reader.schema().fields().len() {
        return Err(exhausted());
    }
    let full_read =
        ids.len().saturating_mul(2) > nonnegative(reader.metadata().file_metadata().num_rows())?;
    let mut bytes = 0usize;
    for group in reader.metadata().row_groups() {
        if !full_read
            && let Some(Statistics::Int64(statistics)) = group.column(key).statistics()
            && let (Some(&low), Some(&high)) = (statistics.min_opt(), statistics.max_opt())
        {
            // Canonical UInt64 ids are represented by Parquet INT64;
            // compare their original unsigned bit patterns.
            let low = low.cast_unsigned();
            let high = high.cast_unsigned();
            if low <= high && !ids.iter().any(|id| *id >= low && *id <= high) {
                continue;
            }
        }
        for &column in &columns {
            bytes = bytes
                .checked_add(column_bytes(group.column(column))?)
                .ok_or_else(exhausted)?;
        }
    }
    Ok(bytes)
}

fn column_bytes(column: &ColumnChunkMetaData) -> Result<usize, DataFusionError> {
    let values = nonnegative(column.num_values())?;
    let compressed = nonnegative(column.compressed_size())?;
    let uncompressed = nonnegative(column.uncompressed_size())?;
    let value_bytes = match column.column_type() {
        Type::BOOLEAN => values,
        Type::INT32 | Type::FLOAT => values.checked_mul(4).ok_or_else(exhausted)?,
        Type::INT64 | Type::DOUBLE => values.checked_mul(8).ok_or_else(exhausted)?,
        Type::INT96 => values.checked_mul(16).ok_or_else(exhausted)?,
        Type::FIXED_LEN_BYTE_ARRAY => values
            .checked_mul(nonnegative(i64::from(column.column_descr().type_length()))?)
            .ok_or_else(exhausted)?,
        Type::BYTE_ARRAY => {
            let payload = match column.unencoded_byte_array_data_bytes() {
                Some(bytes) => nonnegative(bytes)?,
                None => values.checked_mul(uncompressed).ok_or_else(exhausted)?,
            };
            payload
                .checked_add(values.checked_mul(8).ok_or_else(exhausted)?)
                .ok_or_else(exhausted)?
        }
    };
    compressed
        .checked_add(uncompressed)
        .and_then(|bytes| bytes.checked_add(value_bytes))
        .and_then(|bytes| bytes.checked_add(values.checked_mul(16)?))
        .and_then(|bytes| bytes.checked_mul(4))
        .and_then(|bytes| bytes.checked_add(64 * 1024))
        .ok_or_else(exhausted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::file::metadata::ColumnChunkMetaData;
    use parquet::schema::types::{ColumnDescriptor, ColumnPath, Type as SchemaType};
    use std::sync::Arc;

    fn dictionary_column(unencoded: Option<i64>) -> ColumnChunkMetaData {
        let field = SchemaType::primitive_type_builder("value", Type::BYTE_ARRAY)
            .build()
            .unwrap();
        let descriptor = Arc::new(ColumnDescriptor::new(
            Arc::new(field),
            0,
            0,
            ColumnPath::from("value"),
        ));
        ColumnChunkMetaData::builder(descriptor)
            .set_num_values(10_000)
            .set_total_compressed_size(2048)
            .set_total_uncompressed_size(4096)
            .set_unencoded_byte_array_data_bytes(unencoded)
            .build()
            .unwrap()
    }

    #[test]
    fn dictionary_repetition_is_bounded_by_unencoded_values_not_encoded_pages() {
        // One 1 KiB dictionary value repeated 10,000 times: encoded pages
        // occupy a few KiB but the Arrow payload occupies about 10 MiB.
        let payload = 1024 * 10_000;
        let bytes = column_bytes(&dictionary_column(Some(payload))).unwrap();
        assert!(bytes > payload as usize * 4);
        let old_footer = column_bytes(&dictionary_column(None)).unwrap();
        assert!(
            old_footer > bytes,
            "missing size statistics must remain conservative"
        );
    }

    #[test]
    fn invalid_decode_sizes_fail_typed() {
        let error = column_bytes(&dictionary_column(Some(-1))).unwrap_err();
        assert!(matches!(error, DataFusionError::ResourcesExhausted(_)));
        assert!(error.to_string().contains("edge Parquet decode"));
    }
}
