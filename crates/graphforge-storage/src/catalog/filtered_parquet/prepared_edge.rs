//! One authenticated source, metadata snapshot and executable selection per file.

use super::{
    EdgeIdRequest, FilteredReadKind, canonical_projection_with_key, decode_budget, edge_ordinal,
    parquet_err,
};
use crate::read_memory::{PrepaidBatch, ReadAdmission, ReadGrant, add, exceeded, mul, token_bytes};
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result};
use graphforge_core::OntologyMode;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use parquet::file::metadata::{PageIndexPolicy, ParquetStatisticsPolicy};
use parquet::file::reader::{ChunkReader, Length};
use std::collections::HashSet;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;

#[derive(Clone)]
struct Source(Arc<crate::lifecycle_io::ReadPathFile>);
impl Length for Source {
    fn len(&self) -> u64 {
        self.0.len()
    }
}
impl ChunkReader for Source {
    type T = <crate::lifecycle_io::ReadPathFile as ChunkReader>::T;
    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        self.0.get_read(start)
    }
    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<bytes::Bytes> {
        self.0.get_bytes(start, length)
    }
}

/// The full fallback remains admitted even when this metadata proves a dense
/// selection. Both execution routes consume this same source and metadata.
struct PreparedEdgeRead<'a> {
    source: Source,
    metadata: ArrowReaderMetadata,
    projection: Vec<usize>,
    schema: SchemaRef,
    request: &'a EdgeIdRequest<'a>,
    metadata_grants: [Option<ReadGrant>; 4],
    retained_metadata: usize,
    structure: ReadGrant,
}
impl<'a> PreparedEdgeRead<'a> {
    fn prepare(
        path: &Path,
        schema: &SchemaRef,
        request: &'a EdgeIdRequest<'a>,
        projection: &[usize],
        admission: &dyn ReadAdmission,
    ) -> Result<Self> {
        admission.check_cancelled()?;
        let structure = admission.reserve(add(
            std::mem::size_of::<Self>() + 2 * std::mem::size_of::<usize>(),
            mul(projection.len() + 1, std::mem::size_of::<usize>())?,
        )?)?;
        let mut options = std::fs::OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC);
        }
        let mut file = options.open(path).map_err(DataFusionError::IoError)?;
        let attributes = file.metadata().map_err(DataFusionError::IoError)?;
        if !attributes.is_file() {
            return Err(exceeded());
        }
        crate::catalog::preflight_parquet_handle(&mut file, attributes.len())?;
        file.seek(SeekFrom::End(-8))
            .map_err(DataFusionError::IoError)?;
        let mut trailer = [0; 8];
        file.read_exact(&mut trailer)
            .map_err(DataFusionError::IoError)?;
        let encoded = u32::from_le_bytes(trailer[..4].try_into().unwrap()) as usize;
        let source = Source(Arc::new(crate::catalog::admitted_path_file(file)?));
        // Fixed stack slots avoid a growing collection of reservation guards.
        let mut grants: [Option<ReadGrant>; 4] = std::array::from_fn(|_| None);
        let mut next = 0;
        let mut admit = |bytes| {
            admission.check_cancelled()?;
            let grant = admission.reserve(bytes)?;
            *grants.get_mut(next).ok_or_else(exceeded)? = Some(grant);
            next += 1;
            Ok(())
        };
        let mut audit = decode_budget::nodes::metadata::Admission::default();
        let (active, footer_start) =
            audit.source(&source, attributes.len(), encoded, Some(schema), &mut admit)?;
        // Load the footer/schema only after its audited allocations are funded.
        let options =
            ArrowReaderOptions::new().with_size_stats_policy(ParquetStatisticsPolicy::KeepAll);
        let initial = parquet::file::metadata::ParquetMetaDataReader::new()
            .with_metadata_options(Some(options.metadata_options().clone()))
            .parse_and_finish(&source)
            .map_err(parquet_err)?;
        audit.source_indexes(&source, &initial, active, footer_start, &mut admit)?;
        let mut reader = parquet::file::metadata::ParquetMetaDataReader::new_with_metadata(initial)
            .with_page_index_policy(PageIndexPolicy::Optional);
        reader.read_page_indexes(&source).map_err(parquet_err)?;
        let metadata =
            ArrowReaderMetadata::try_new(Arc::new(reader.finish().map_err(parquet_err)?), options)
                .map_err(parquet_err)?;
        let builder =
            ParquetRecordBatchReaderBuilder::new_with_metadata(source.clone(), metadata.clone());
        crate::catalog::admit_decoded_parquet(&builder)?;
        if builder.schema().fields() != schema.fields() {
            return Err(DataFusionError::Execution(format!(
                "projected edge read requires canonical schema: {}",
                path.display()
            )));
        }
        let projection = canonical_projection_with_key(schema, projection, "edge_id")?;
        Ok(Self {
            source,
            metadata,
            projection,
            schema: Arc::clone(schema),
            request,
            metadata_grants: grants,
            retained_metadata: audit.retained,
            structure,
        })
    }

    fn builder(&self) -> ParquetRecordBatchReaderBuilder<Source> {
        ParquetRecordBatchReaderBuilder::new_with_metadata(
            self.source.clone(),
            self.metadata.clone(),
        )
    }

    fn decode(
        mut self,
        path: &Path,
        observer: Option<&Arc<dyn crate::io_stats::FilteredReadObserver>>,
        admission: &dyn ReadAdmission,
    ) -> Result<PrepaidBatch> {
        // Exact same projection/request/metadata as execution; all original
        // complete-group, dictionary, codec and batch coefficients are kept.
        let bound = decode_budget::metadata_bytes(
            self.metadata.metadata(),
            self.metadata.schema(),
            self.request.ids(),
            Some(&self.projection),
            None,
        )?;
        let bytes = add(bound.retained, bound.scratch.max(mul(bound.retained, 2)?))?;
        let rows = usize::try_from(self.metadata.metadata().file_metadata().num_rows())
            .map_err(|_| exceeded())?;
        let batches = rows.div_ceil(rows.clamp(1, 1024));
        let tokens = mul(
            mul(mul(batches.max(1), self.projection.len())?, 3)?,
            token_bytes(),
        )?;
        let grant = admission.reserve(add(bytes, add(tokens, 4096)?)?)?;
        admission.check_cancelled()?;
        let batches = if let Some(batches) = edge_ordinal::read_dense_edge_builder(
            self.builder(),
            &self.schema,
            self.request,
            observer,
            Some(&self.projection),
        )? {
            batches
        } else {
            // The already-admitted full envelope covers missing indexes,
            // majority reads and dense validation retries. No new source.
            admission.check_cancelled()?;
            super::read_parquet_filtered_u64_from_builder(
                self.builder(),
                path,
                Arc::clone(&self.schema),
                "edge_id",
                self.request.ids(),
                FilteredReadKind::Edge,
                observer,
                false,
                Some(&self.projection),
                true,
                || self.read_plain(),
            )?
        };
        let batch = concat_raw(&batches)?;
        drop(batches);
        // Parsing/decoder workspace is dead. Retain only schema authority.
        drop(self.metadata);
        drop(self.source);
        drop(self.projection);
        drop(self.structure);
        shrink_grants(&mut self.metadata_grants, self.retained_metadata)?;
        PrepaidBatch::seal_with_metadata(batch, grant, self.metadata_grants)
    }

    fn read_plain(&self) -> Result<Vec<RecordBatch>> {
        let builder = self.builder();
        let projection =
            ProjectionMask::roots(builder.parquet_schema(), self.projection.iter().copied());
        let reader = builder
            .with_projection(projection)
            .build()
            .map_err(parquet_err)?;
        let schema = arrow::record_batch::RecordBatchReader::schema(&reader);
        let batches = reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(parquet_err)?;
        if batches.is_empty() {
            return Ok(vec![RecordBatch::new_empty(schema)]);
        }
        Ok(vec![concat_raw(&batches)?])
    }
}

fn shrink_grants(grants: &mut [Option<ReadGrant>], mut keep: usize) -> Result<()> {
    for grant in grants.iter_mut().flatten() {
        let retained = keep.min(grant.size());
        keep -= retained;
        grant.shrink_to(retained)?;
    }
    if keep != 0 {
        return Err(exceeded());
    }
    Ok(())
}
fn concat_raw(batches: &[RecordBatch]) -> Result<RecordBatch> {
    let first = batches.first().ok_or_else(exceeded)?;
    if batches.len() == 1 {
        return Ok(first.clone());
    }
    arrow::compute::concat_batches(&first.schema(), batches)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

/// Read canonical authenticated edge files with same-pool ownership transfer.
/// Wildcard union normalization remains with its existing separate reader.
///
/// # Errors
/// Returns admission, authenticated source, metadata or decoder failures.
#[allow(clippy::implicit_hasher)]
pub fn read_edges_filtered_prepaid_from_inventory(
    inventory: &crate::AuthenticatedPropertyInventory,
    rel_name: &str,
    mode: OntologyMode,
    edge_ids: &HashSet<u64>,
    projection: &[usize],
    observer: Option<&Arc<dyn crate::io_stats::FilteredReadObserver>>,
    admission: &dyn ReadAdmission,
) -> Result<PrepaidBatch> {
    if rel_name == "*" && matches!(mode, OntologyMode::Strict | OntologyMode::Advisory) {
        return Err(exceeded());
    }
    let (route, schema) = match mode {
        OntologyMode::Exploratory => ("_exploratory", &*crate::EXPLORATORY_EDGE_SCHEMA),
        OntologyMode::Strict | OntologyMode::Advisory => (rel_name, &*crate::TYPED_EDGE_SCHEMA),
    };
    admission.check_cancelled()?;
    let count = if edge_ids.is_empty() {
        0
    } else {
        inventory.borrowed_edge_files(Some(route)).count()
    };
    // Sorted request, predicate membership clone, selection vectors, source
    // Arc, exact result-owner Vec and their temporary copies precede allocation.
    let projection_bytes = mul(add(projection.len(), 1)?, std::mem::size_of::<usize>())?;
    let aux = admission.reserve(add(
        projection_bytes,
        add(
            mul(edge_ids.len(), 64)?,
            add(mul(count, std::mem::size_of::<PrepaidBatch>())?, 16 * 1024)?,
        )?,
    )?)?;
    let request = EdgeIdRequest::new(edge_ids);
    let mut completed = Vec::with_capacity(count);
    for path in inventory.borrowed_edge_files(Some(route)).take(count) {
        let prepared = PreparedEdgeRead::prepare(path, schema, &request, projection, admission)?;
        completed.push(prepared.decode(path, observer, admission)?);
    }
    let result = if completed.len() == 1 {
        let only = completed.pop().unwrap();
        drop(completed); // release its actual Vec backing before aux credit
        only
    } else {
        // Nonempty concatenation keeps the first actual schema, including
        // top-level metadata and zero-row first-file semantics. Only a route
        // with no physical result constructs a canonical empty schema.
        let mut schema_credit = None;
        let output_schema = if let Some(first) = completed.first() {
            first.batch().schema()
        } else {
            let indices = canonical_projection_with_key(schema, projection, "edge_id")?;
            // Canonical FieldRefs are already owned; only Schema/Fields Arc
            // and the temporary FieldRef Vec are newly allocated here.
            let schema_bytes = add(
                std::mem::size_of::<arrow::datatypes::Schema>() + 4 * std::mem::size_of::<usize>(),
                mul(
                    mul(indices.len(), 2)?,
                    std::mem::size_of::<arrow::datatypes::FieldRef>(),
                )?,
            )?;
            schema_credit = Some(admission.reserve(schema_bytes)?);
            Arc::new(arrow::datatypes::Schema::new(
                indices
                    .iter()
                    .map(|&i| Arc::clone(&schema.fields()[i]))
                    .collect::<Vec<_>>(),
            ))
        };
        let bytes = completed
            .iter()
            .try_fold(4096, |n, b| add(n, b.batch().get_array_memory_size()))?;
        let credit = admission.reserve(add(
            mul(bytes, 2)?,
            mul(schema.fields().len() * 3, token_bytes())?,
        )?)?;
        let mut refs = Vec::with_capacity(completed.len());
        refs.extend(completed.iter().map(PrepaidBatch::batch));
        let batch = arrow::compute::concat_batches(&output_schema, refs)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        let mut inputs = completed.into_iter();
        let first_schema = inputs.next().map(PrepaidBatch::into_schema_owner);
        drop(inputs); // all original payloads and other schemas are now dead
        PrepaidBatch::seal_with_schema_credit(batch, credit, schema_credit, first_schema)?
    };
    drop(request);
    drop(aux);
    Ok(result)
}

#[cfg(test)]
mod tests;
