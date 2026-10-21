// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Merge that deals with an arbitrary size of streaming inputs.
//! This is an order-preserving merge.

use crate::metrics::BaselineMetrics;
use crate::sorts::copy_admission::{CopyAdmission, CopyCapability};
use crate::sorts::multi_level_merge::MultiLevelMergeBuilder;
use crate::sorts::{
    merge::SortPreservingMergeStream,
    stream::{FieldCursorStream, RowCursorStream},
};
use crate::{SendableRecordBatchStream, SpillManager};
use arrow::array::*;
use arrow::datatypes::{DataType, SchemaRef};
use datafusion_common::human_readable_size;
use datafusion_common::{Result, assert_or_internal_err, internal_err};
use datafusion_execution::disk_manager::RefCountedTempFile;
use datafusion_execution::memory_pool::{
    MemoryConsumer, MemoryPool, MemoryReservation, UnboundedMemoryPool,
};
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use std::sync::Arc;

macro_rules! primitive_merge_helper {
    ($t:ty, $($v:ident),+) => {
        merge_helper!(PrimitiveArray<$t>, $($v),+)
    };
}

macro_rules! merge_helper {
    ($t:ty, $sort:ident, $streams:ident, $schema:ident, $tracking_metrics:ident, $batch_size:ident, $fetch:ident, $reservation:ident, $enable_round_robin_tie_breaker:ident, $bounded_inputs:ident, $copy:ident) => {{
        if let Some(copy) = $copy.as_ref() {
            // Actual Field dispatch, without qualifying key evaluation/GC for
            // dynamic progress. Its complete original guard stays live.
            let metadata = crate::spill::admission::add(
                FieldCursorStream::<$t>::constructor_bound(
                    $streams.len(),
                    $streams.capacity(),
                )?,
                SortPreservingMergeStream::<
                    super::cursor::ArrayValues<
                        <$t as super::cursor::CursorArray>::Values,
                    >,
                >::constructor_bound($streams.len(), $batch_size)?,
            )?;
            copy.preparing_field(metadata)?;
            let streams = FieldCursorStream::<$t>::try_new_with_copy(
                $sort,
                $streams,
                $reservation.new_empty(),
                copy,
            )?;
            let stream = SortPreservingMergeStream::new_with_copy(
                Box::new(streams),
                $schema,
                $tracking_metrics,
                $batch_size,
                $fetch,
                $reservation,
                $enable_round_robin_tie_breaker,
                $bounded_inputs,
                Some(Arc::clone(copy)),
            )?;
            copy.finish_preparing();
            return Ok(Box::pin(stream));
        }
        let streams =
            FieldCursorStream::<$t>::new($sort, $streams, $reservation.new_empty());
        return Ok(Box::pin(SortPreservingMergeStream::new(
            Box::new(streams),
            $schema,
            $tracking_metrics,
            $batch_size,
            $fetch,
            $reservation,
            $enable_round_robin_tie_breaker,
            $bounded_inputs,
        )));
    }};
}

/// The bank a pass admitted for the readers of its spill files, and each
/// stream's share of it (zero for a stream that is not a spill reader).
/// The merge gives a reader's share back once it is exhausted and the merge
/// holds none of its batches.
#[derive(Debug)]
pub(crate) struct ReaderShares {
    pub(crate) credit: Arc<crate::spill::admission::Credit>,
    pub(crate) shares: Vec<usize>,
}

pub struct SortedSpillFile {
    pub file: RefCountedTempFile,

    /// how much memory the largest memory batch is taking
    pub max_record_batch_memory: usize,
}

impl std::fmt::Debug for SortedSpillFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SortedSpillFile({:?}) takes {}",
            self.file.path(),
            human_readable_size(self.max_record_batch_memory)
        )
    }
}

#[derive(Default)]
pub struct StreamingMergeBuilder<'a> {
    streams: Vec<SendableRecordBatchStream>,
    sorted_spill_files: Vec<SortedSpillFile>,
    spill_manager: Option<SpillManager>,
    schema: Option<SchemaRef>,
    expressions: Option<&'a LexOrdering>,
    metrics: Option<BaselineMetrics>,
    batch_size: Option<usize>,
    fetch: Option<usize>,
    reservation: Option<MemoryReservation>,
    enable_round_robin_tie_breaker: bool,
    copy: Option<Arc<CopyAdmission>>,
    /// The streams read spill files under a bank admitted for the pass.
    bounded_inputs: Option<ReaderShares>,
}

impl<'a> StreamingMergeBuilder<'a> {
    pub fn new() -> Self {
        Self {
            enable_round_robin_tie_breaker: true,
            ..Default::default()
        }
    }

    pub fn with_streams(mut self, streams: Vec<SendableRecordBatchStream>) -> Self {
        self.streams = streams;
        self
    }

    pub fn with_sorted_spill_files(
        mut self,
        sorted_spill_files: Vec<SortedSpillFile>,
    ) -> Self {
        self.sorted_spill_files = sorted_spill_files;
        self
    }

    pub fn with_spill_manager(mut self, spill_manager: SpillManager) -> Self {
        self.spill_manager = Some(spill_manager);
        self
    }

    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }

    pub fn with_expressions(mut self, expressions: &'a LexOrdering) -> Self {
        self.expressions = Some(expressions);
        self
    }

    pub fn with_metrics(mut self, metrics: BaselineMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = Some(batch_size);
        self
    }

    pub fn with_fetch(mut self, fetch: Option<usize>) -> Self {
        self.fetch = fetch;
        self
    }

    pub(super) fn with_copy(mut self, copy: Option<Arc<CopyAdmission>>) -> Self {
        self.copy = copy;
        self
    }

    pub fn with_reservation(mut self, reservation: MemoryReservation) -> Self {
        self.reservation = Some(reservation);
        self
    }

    /// See [SortPreservingMergeExec::with_round_robin_repartition] for more
    /// information.
    ///
    /// [SortPreservingMergeExec::with_round_robin_repartition]: crate::sorts::sort_preserving_merge::SortPreservingMergeExec::with_round_robin_repartition
    pub fn with_round_robin_tie_breaker(
        mut self,
        enable_round_robin_tie_breaker: bool,
    ) -> Self {
        self.enable_round_robin_tie_breaker = enable_round_robin_tie_breaker;
        self
    }

    /// Bypass the mempool and avoid using the memory reservation.
    ///
    /// This is not marked as `pub` because it is not recommended to use this method
    pub(super) fn with_bypass_mempool(mut self, shares: ReaderShares) -> Self {
        let mem_pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        // The readers of the pass draw on its bank, the merge keeps the
        // envelope the bank assumes and gives each reader's share back once
        // it holds nothing of that reader.
        self.bounded_inputs = Some(shares);

        self.with_reservation(
            MemoryConsumer::new("merge stream mock memory").register(&mem_pool),
        )
    }

    /// What the narrowest merge pass of the spill files given so far needs,
    /// which a consumer admits before it builds the merge (see
    /// `multi_level_merge::minimum_pass_bound`).
    pub fn minimum_pass_bound(&self) -> Result<usize> {
        let (Some(schema), Some(expressions), Some(batch_size), Some(manager)) = (
            self.schema.as_ref(),
            self.expressions,
            self.batch_size,
            self.spill_manager.as_ref(),
        ) else {
            return internal_err!(
                "minimum_pass_bound needs schema, expressions, batch size and spill manager"
            );
        };
        crate::sorts::multi_level_merge::minimum_pass_bound(
            schema,
            expressions,
            batch_size,
            manager.compression,
            &self.sorted_spill_files,
            2_usize.saturating_sub(self.streams.len()),
            self.streams.is_empty(),
        )
    }

    pub fn build(self) -> Result<SendableRecordBatchStream> {
        let Self {
            streams,
            sorted_spill_files,
            spill_manager,
            schema,
            metrics,
            batch_size,
            reservation,
            fetch,
            expressions,
            enable_round_robin_tie_breaker,
            copy,
            bounded_inputs,
        } = self;

        // Early return if expressions are empty:
        let Some(expressions) = expressions else {
            return internal_err!("Sort expressions cannot be empty for streaming merge");
        };

        if !sorted_spill_files.is_empty() {
            // Unwrapping mandatory fields
            let schema = schema.expect("Schema cannot be empty for streaming merge");
            let metrics = metrics.expect("Metrics cannot be empty for streaming merge");
            let batch_size =
                batch_size.expect("Batch size cannot be empty for streaming merge");
            let reservation =
                reservation.expect("Reservation cannot be empty for streaming merge");

            return Ok(MultiLevelMergeBuilder::new(
                spill_manager.expect("spill_manager should exist"),
                schema,
                sorted_spill_files,
                streams,
                expressions.clone(),
                metrics,
                batch_size,
                reservation,
                fetch,
                enable_round_robin_tie_breaker,
            )
            .create_spillable_merge_stream());
        }

        // Early return if streams are empty:
        assert_or_internal_err!(
            !streams.is_empty(),
            "Streams/sorted spill files cannot be empty for streaming merge"
        );

        // Unwrapping mandatory fields
        let schema = schema.expect("Schema cannot be empty for streaming merge");
        let metrics = metrics.expect("Metrics cannot be empty for streaming merge");
        let batch_size =
            batch_size.expect("Batch size cannot be empty for streaming merge");
        let reservation =
            reservation.expect("Reservation cannot be empty for streaming merge");

        if copy.is_none() {
            crate::terminal_ownership::gap(
                crate::terminal_ownership::GAP_UNKNOWN_SPONSOR,
            );
        }
        if let Some(copy) = copy
            .as_ref()
            .filter(|copy| copy.capability() == CopyCapability::PrimitiveField)
        {
            let streams = FieldCursorStream::<Int64Array>::try_new_with_copy(
                expressions[0].clone(),
                streams,
                reservation.new_empty(),
                copy,
            )?;
            let stream = SortPreservingMergeStream::new_with_copy(
                Box::new(streams),
                schema,
                metrics,
                batch_size,
                fetch,
                reservation,
                enable_round_robin_tie_breaker,
                bounded_inputs,
                Some(Arc::clone(copy)),
            )?;
            copy.finish_preparing();
            return Ok(Box::pin(stream));
        }

        // Special case single column comparisons with optimized cursor implementations
        if expressions.len() == 1 {
            let sort = expressions[0].clone();
            let data_type = sort.expr.data_type(schema.as_ref())?;
            downcast_primitive! {
                data_type => (primitive_merge_helper, sort, streams, schema, metrics, batch_size, fetch, reservation, enable_round_robin_tie_breaker, bounded_inputs, copy),
                DataType::Utf8 => merge_helper!(StringArray, sort, streams, schema, metrics, batch_size, fetch, reservation, enable_round_robin_tie_breaker, bounded_inputs, copy)
                DataType::Utf8View => merge_helper!(StringViewArray, sort, streams, schema, metrics, batch_size, fetch, reservation, enable_round_robin_tie_breaker, bounded_inputs, copy)
                DataType::LargeUtf8 => merge_helper!(LargeStringArray, sort, streams, schema, metrics, batch_size, fetch, reservation, enable_round_robin_tie_breaker, bounded_inputs, copy)
                DataType::Binary => merge_helper!(BinaryArray, sort, streams, schema, metrics, batch_size, fetch, reservation, enable_round_robin_tie_breaker, bounded_inputs, copy)
                DataType::LargeBinary => merge_helper!(LargeBinaryArray, sort, streams, schema, metrics, batch_size, fetch, reservation, enable_round_robin_tie_breaker, bounded_inputs, copy)
                _ => {}
            }
        }

        let streams = RowCursorStream::try_new_with_copy(
            schema.as_ref(),
            expressions,
            streams,
            reservation.new_empty(),
            copy.clone(),
        )?;
        let stream = SortPreservingMergeStream::new_with_copy(
            Box::new(streams),
            schema,
            metrics,
            batch_size,
            fetch,
            reservation,
            enable_round_robin_tie_breaker,
            bounded_inputs,
            copy.clone(),
        )?;
        if let Some(copy) = &copy {
            copy.finish_preparing();
        }
        Ok(Box::pin(stream))
    }
}
