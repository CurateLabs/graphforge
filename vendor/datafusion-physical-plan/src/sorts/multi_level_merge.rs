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

//! Create a stream that do a multi level merge stream

use crate::metrics::BaselineMetrics;
use crate::terminal_ownership as od;
use crate::{EmptyRecordBatchStream, SpillManager};
use arrow::array::RecordBatch;
use std::fmt::{Debug, Formatter};
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use datafusion_common::Result;
use datafusion_execution::memory_pool::MemoryReservation;

use crate::sorts::builder::try_grow_reservation_to_at_least;
use crate::sorts::copy_admission::{CopyAdmission, CopyCapability};
use crate::sorts::sort::get_reserved_bytes_for_record_batch_size;
use crate::sorts::streaming_merge::{SortedSpillFile, StreamingMergeBuilder};
use crate::stream::RecordBatchStreamAdapter;
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream};
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use futures::TryStreamExt;
use futures::{Stream, StreamExt};

/// Merges a stream of sorted cursors and record batches into a single sorted stream
///
/// This is a wrapper around [`SortPreservingMergeStream`](crate::sorts::merge::SortPreservingMergeStream)
/// that provide it the sorted streams/files to merge while making sure we can merge them in memory.
/// In case we can't merge all of them in a single pass we will spill the intermediate results to disk
/// and repeat the process.
///
/// ## High level Algorithm
/// 1. Get the maximum amount of sorted in-memory streams and spill files we can merge with the available memory
/// 2. Sort them to a sorted stream
/// 3. Do we have more spill files to merge?
///  - Yes: write that sorted stream to a spill file,
///    add that spill file back to the spill files to merge and
///    repeat the process
///
///  - No: return that sorted stream as the final output stream
///
/// ```text
/// Initial State: Multiple sorted streams + spill files
///      ┌───────────┐
///      │  Phase 1  │
///      └───────────┘
/// ┌──Can hold in memory─┐
/// │   ┌──────────────┐  │
/// │   │  In-memory   │
/// │   │sorted stream │──┼────────┐
/// │   │      1       │  │        │
///     └──────────────┘  │        │
/// │   ┌──────────────┐  │        │
/// │   │  In-memory   │           │
/// │   │sorted stream │──┼────────┤
/// │   │      2       │  │        │
///     └──────────────┘  │        │
/// │   ┌──────────────┐  │        │
/// │   │  In-memory   │           │
/// │   │sorted stream │──┼────────┤
/// │   │      3       │  │        │
///     └──────────────┘  │        │
/// │   ┌──────────────┐  │        │            ┌───────────┐
/// │   │ Sorted Spill │           │            │  Phase 2  │
/// │   │    file 1    │──┼────────┤            └───────────┘
/// │   └──────────────┘  │        │
///  ──── ──── ──── ──── ─┘        │       ┌──Can hold in memory─┐
///                                │       │                     │
///     ┌──────────────┐           │       │   ┌──────────────┐
///     │ Sorted Spill │           │       │   │ Sorted Spill │  │
///     │    file 2    │──────────────────────▶│    file 2    │──┼─────┐
///     └──────────────┘           │           └──────────────┘  │     │
///     ┌──────────────┐           │       │   ┌──────────────┐  │     │
///     │ Sorted Spill │           │       │   │ Sorted Spill │        │
///     │    file 3    │──────────────────────▶│    file 3    │──┼─────┤
///     └──────────────┘           │       │   └──────────────┘  │     │
///     ┌──────────────┐           │           ┌──────────────┐  │     │
///     │ Sorted Spill │           │       │   │ Sorted Spill │  │     │
///     │    file 4    │──────────────────────▶│    file 4    │────────┤          ┌───────────┐
///     └──────────────┘           │       │   └──────────────┘  │     │          │  Phase 3  │
///                                │       │                     │     │          └───────────┘
///                                │        ──── ──── ──── ──── ─┘     │     ┌──Can hold in memory─┐
///                                │                                   │     │                     │
///     ┌──────────────┐           │           ┌──────────────┐        │     │  ┌──────────────┐
///     │ Sorted Spill │           │           │ Sorted Spill │        │     │  │ Sorted Spill │   │
///     │    file 5    │──────────────────────▶│    file 5    │────────────────▶│    file 5    │───┼───┐
///     └──────────────┘           │           └──────────────┘        │     │  └──────────────┘   │   │
///                                │                                   │     │                     │   │
///                                │           ┌──────────────┐        │     │  ┌──────────────┐       │
///                                │           │ Sorted Spill │        │     │  │ Sorted Spill │   │   │       ┌── ─── ─── ─── ─── ─── ─── ──┐
///                                └──────────▶│    file 6    │────────────────▶│    file 6    │───┼───┼──────▶         Output Stream
///                                            └──────────────┘        │     │  └──────────────┘   │   │       └── ─── ─── ─── ─── ─── ─── ──┘
///                                                                    │     │                     │   │
///                                                                    │     │  ┌──────────────┐       │
///                                                                    │     │  │ Sorted Spill │   │   │
///                                                                    └───────▶│    file 7    │───┼───┘
///                                                                          │  └──────────────┘   │
///                                                                          │                     │
///                                                                          └─ ──── ──── ──── ────
/// ```
///
/// ## Memory Management Strategy
///
/// This multi-level merge make sure that we can handle any amount of data to sort as long as
/// we have enough memory to merge at least 2 streams at a time.
///
/// 1. **Worst-Case Memory Reservation**: Reserves memory based on the largest
///    batch size encountered in each spill file to merge, ensuring sufficient memory is always
///    available during merge operations.
/// 2. **Adaptive Buffer Sizing**: Reduces buffer sizes when memory is constrained
/// 3. **Spill-to-Disk**: Spill to disk when we cannot merge all files in memory
pub(crate) struct MultiLevelMergeBuilder {
    spill_manager: SpillManager,
    schema: SchemaRef,
    sorted_spill_files: Vec<SortedSpillFile>,
    sorted_streams: Vec<SendableRecordBatchStream>,
    expr: LexOrdering,
    metrics: BaselineMetrics,
    batch_size: usize,
    reservation: MemoryReservation,
    fetch: Option<usize>,
    enable_round_robin_tie_breaker: bool,
    diagnostic_piece: od::Piece,
    diagnostic: od::Life,
}

impl Debug for MultiLevelMergeBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MultiLevelMergeBuilder")
    }
}

impl MultiLevelMergeBuilder {
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn new(
        spill_manager: SpillManager,
        schema: SchemaRef,
        sorted_spill_files: Vec<SortedSpillFile>,
        sorted_streams: Vec<SendableRecordBatchStream>,
        expr: LexOrdering,
        metrics: BaselineMetrics,
        batch_size: usize,
        reservation: MemoryReservation,
        fetch: Option<usize>,
        enable_round_robin_tie_breaker: bool,
    ) -> Self {
        let diagnostic = od::Life::inherited(od::Kind::Source);
        let diagnostic_piece = od::Piece::new(&reservation, diagnostic.id());
        diagnostic.update(|o| {
            o.piece = diagnostic_piece.id();
            o.held = reservation.size() as u64;
            o.flags = 4;
        });
        Self {
            diagnostic,
            diagnostic_piece,
            spill_manager,
            schema,
            sorted_spill_files,
            sorted_streams,
            expr,
            metrics,
            batch_size,
            reservation,
            enable_round_robin_tie_breaker,
            fetch,
        }
    }

    pub(crate) fn create_spillable_merge_stream(self) -> SendableRecordBatchStream {
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            futures::stream::once(self.create_stream()).try_flatten(),
        ))
    }

    async fn create_stream(mut self) -> Result<SendableRecordBatchStream> {
        loop {
            let mut stream = self.merge_sorted_runs_within_mem_limit()?;

            // TODO - add a threshold for number of files to disk even if empty and reading from disk so
            //        we can avoid the memory reservation

            // If no spill files are left, we can return the stream as this is the last sorted run
            // TODO - We can write to disk before reading it back to avoid having multiple streams in memory
            if self.sorted_spill_files.is_empty() {
                assert!(
                    self.sorted_streams.is_empty(),
                    "We should not have any sorted streams left"
                );

                return Ok(stream);
            }

            // Need to sort to a spill file
            let Some((spill_file, max_record_batch_memory)) = self
                .spill_manager
                .spill_record_batch_stream_and_return_max_batch_memory(
                    &mut stream,
                    "MultiLevelMergeBuilder intermediate spill",
                )
                .await?
            else {
                continue;
            };

            // Add the spill file
            self.sorted_spill_files.push(SortedSpillFile {
                file: spill_file,
                max_record_batch_memory,
            });
        }
    }

    /// This tries to create a stream that merges the most sorted streams and sorted spill files
    /// as possible within the memory limit.
    fn merge_sorted_runs_within_mem_limit(
        &mut self,
    ) -> Result<SendableRecordBatchStream> {
        let _transition = od::Transition::begin();
        match (self.sorted_spill_files.len(), self.sorted_streams.len()) {
            // No data so empty batch
            (0, 0) => Ok(Box::pin(EmptyRecordBatchStream::new(Arc::clone(
                &self.schema,
            )))),

            // Only in-memory stream, return that
            (0, 1) => Ok(self.sorted_streams.remove(0)),

            // Only single sorted spill file so return it
            (1, 0) => {
                let spill_file = self.sorted_spill_files.remove(0);

                // Not reserving any memory for this disk as we are not holding it in memory
                self.spill_manager
                    .read_spill_as_stream(spill_file.file, None)
            }

            // Only in memory streams, so merge them all in a single pass
            (0, _) => {
                let sorted_stream = mem::take(&mut self.sorted_streams);
                self.create_new_merge_sort(
                    sorted_stream,
                    // If we have no sorted spill files left, this is the last run
                    true,
                    true,
                    None,
                )
            }

            // Need to merge multiple streams
            (_, _) => {
                // Transfer any pre-reserved bytes (from sort_spill_reservation_bytes)
                // to the merge memory reservation. This prevents starvation when
                // concurrent sort partitions compete for pool memory: the pre-reserved
                // bytes cover spill file buffer reservations without additional pool
                // allocation.
                let mut memory_reservation = self.reservation.take();
                let mut memory_piece = mem::take(&mut self.diagnostic_piece);
                self.diagnostic_piece =
                    memory_piece.empty_successor(&self.reservation, self.diagnostic.id());

                // Don't account for existing streams memory
                // as we are not holding the memory for them
                let mut sorted_streams = mem::take(&mut self.sorted_streams);
                // Only these sources carry IPC buffer authority. Mixed memory
                // inputs continue using the existing fixed-copy reservation.
                let all_ipc_inputs = sorted_streams.is_empty();

                // Decide eligibility before moving any authority. Stock split
                // preserves consumer identity: spilling aggregate sponsors must
                // keep their existing loan/floor and reservation lifecycle.
                let retiring = all_ipc_inputs
                    && !memory_reservation.consumer().can_spill()
                    && crate::spill::resident_metadata::std_mutex_heap_bound().is_some()
                    && crate::spill::admission::ticket_schema(&self.schema);
                // Qualification precedes all authority moves and prefetch.
                let capability = if all_ipc_inputs {
                    if let Some(encoded_width) =
                        super::copy_bounds::qualified_columns(&self.schema, &self.expr)?
                    {
                        CopyCapability::Row { encoded_width }
                    } else if retiring
                        && super::copy_bounds::primitive_field(&self.schema, &self.expr)
                    {
                        CopyCapability::PrimitiveField
                    } else {
                        CopyCapability::Legacy
                    }
                } else {
                    CopyCapability::Legacy
                };
                // A merge that reads with a Field cursor keeps the whole
                // envelope of its copies; its working set lies beside it.
                let field = all_ipc_inputs
                    && capability == CopyCapability::Legacy
                    && super::copy_bounds::single_column_field(&self.schema, &self.expr)?;
                // Streams that are all spill files select at least two of them,
                // so a merge with a copy capability always reads spill files.
                let (sorted_spill_files, buffer_size, working_set) = self
                    .get_sorted_spill_files_to_merge(
                        2,
                        // we must have at least 2 streams to merge
                        2_usize.saturating_sub(sorted_streams.len()),
                        capability,
                        field,
                        sorted_streams.capacity(),
                        &mut memory_reservation,
                    )?;

                let is_only_merging_memory_streams = sorted_spill_files.is_empty();

                // If no spill files were selected (e.g. all too large for
                // available memory but enough in-memory streams exist),
                // return the pre-reserved bytes to self.reservation so
                // create_new_merge_sort can transfer them to the merge
                // stream's BatchBuilder.
                if is_only_merging_memory_streams {
                    mem::swap(&mut self.reservation, &mut memory_reservation);
                    mem::swap(&mut self.diagnostic_piece, &mut memory_piece);
                }

                // Preserve the existing sorted-copy half of the reservation.
                // Only the previously prepaid input-buffer half is transferable
                // to readers; leases protect queues and escaped aliases.
                let input_bytes =
                    sorted_spill_files
                        .iter()
                        .try_fold(0_usize, |total, spill| {
                            crate::spill::admission::add(
                                total,
                                crate::spill::admission::mul(
                                    spill.max_record_batch_memory,
                                    buffer_size,
                                )?,
                            )
                        })?;
                memory_piece.refresh(&memory_reservation, self.diagnostic.id());
                let (input_credit, producers) = if retiring {
                    let input = memory_reservation.split(input_bytes);
                    let piece =
                        memory_piece.child(&input, self.diagnostic.id(), input_bytes);
                    memory_piece.refresh(&memory_reservation, self.diagnostic.id());
                    piece.transfer_to(
                        self.diagnostic.id(),
                        od::Kind::Credit as u64,
                        || {
                            crate::spill::admission::Credit::with_readers(
                                input,
                                sorted_spill_files.len(),
                            )
                        },
                    )?
                } else {
                    (
                        {
                            let input = memory_reservation.split(input_bytes);
                            let piece = memory_piece.child(
                                &input,
                                self.diagnostic.id(),
                                input_bytes,
                            );
                            memory_piece
                                .refresh(&memory_reservation, self.diagnostic.id());
                            piece.transfer_to(
                                self.diagnostic.id(),
                                od::Kind::Credit as u64,
                                || crate::spill::admission::Credit::new(input),
                            )
                        },
                        Vec::new(),
                    )
                };
                // All roots are registered/sealed above, before spawn_buffered
                // can poll the first reader. The iterator owns any unused roots
                // if a later constructor fails.
                let mut producers = producers.into_iter();

                let copy = if all_ipc_inputs && !is_only_merging_memory_streams {
                    let authority = memory_reservation.take();
                    let authority_piece = mem::take(&mut memory_piece);
                    if capability == CopyCapability::Legacy {
                        Some(authority_piece.transfer_to(
                            self.diagnostic.id(),
                            od::Kind::Copy as u64,
                            || CopyAdmission::new(authority, working_set),
                        )?)
                    } else {
                        let metadata = self.copy_metadata(
                            capability,
                            sorted_spill_files.len(),
                            sorted_spill_files.capacity(),
                            sorted_streams.capacity(),
                        )?;
                        Some(authority_piece.transfer_to(
                            self.diagnostic.id(),
                            od::Kind::Copy as u64,
                            || CopyAdmission::preparing(authority, metadata, capability, working_set),
                        )?)
                    }
                } else {
                    None
                };

                for spill in sorted_spill_files {
                    let stream = self
                        .spill_manager
                        .clone()
                        .with_batch_read_buffer_capacity(buffer_size)
                        .read_spill_with_credit(
                            spill.file,
                            spill.max_record_batch_memory,
                            Arc::clone(&input_credit),
                            producers.next(),
                        )?;
                    sorted_streams.push(stream);
                }
                let merge_sort_stream = self.create_new_merge_sort(
                    sorted_streams,
                    // If we have no sorted spill files left, this is the last run
                    self.sorted_spill_files.is_empty(),
                    is_only_merging_memory_streams,
                    copy.clone(),
                )?;

                // If we're only merging memory streams, we don't need to attach the memory reservation
                // as it's empty
                if is_only_merging_memory_streams {
                    assert_eq!(
                        memory_reservation.size(),
                        0,
                        "when only merging memory streams, we should not have any memory reservation and let the merge sort handle the memory"
                    );

                    Ok(merge_sort_stream)
                } else {
                    // Attach the memory reservation to the stream to make sure we have enough memory
                    // throughout the merge process as we bypassed the memory pool for the merge sort stream
                    Ok(memory_piece.transfer(self.diagnostic.id(), || {
                        Box::pin(StreamAttachedReservation::new(
                            merge_sort_stream,
                            memory_reservation,
                            copy,
                        )) as SendableRecordBatchStream
                    }))
                }
            }
        }
    }

    fn create_new_merge_sort(
        &mut self,
        streams: Vec<SendableRecordBatchStream>,
        is_output: bool,
        all_in_memory: bool,
        copy: Option<Arc<CopyAdmission>>,
    ) -> Result<SendableRecordBatchStream> {
        let mut builder = StreamingMergeBuilder::new()
            .with_schema(Arc::clone(&self.schema))
            .with_expressions(&self.expr)
            .with_batch_size(self.batch_size)
            .with_fetch(self.fetch)
            .with_metrics(if is_output {
                // Only add the metrics to the last run
                self.metrics.clone()
            } else {
                self.metrics.intermediate()
            })
            .with_round_robin_tie_breaker(self.enable_round_robin_tie_breaker)
            .with_streams(streams)
            .with_copy(copy);

        if !all_in_memory {
            // Don't track memory used by this stream as we reserve that memory by worst case sceneries
            // (reserving memory for the biggest batch in each stream)
            // TODO - avoid this hack as this can be broken easily when `SortPreservingMergeStream`
            //        changes the implementation to use more/less memory
            builder = builder.with_bypass_mempool();
        } else {
            // If we are only merging in-memory streams, we need to use the memory reservation
            // because we don't know the maximum size of the batches in the streams.
            // Use take() to transfer any pre-reserved bytes so the merge can use them
            // as its initial budget without additional pool allocation.
            builder = builder.with_reservation(self.reservation.take());
        }

        if all_in_memory {
            let piece = mem::take(&mut self.diagnostic_piece);
            self.diagnostic_piece =
                piece.empty_successor(&self.reservation, self.diagnostic.id());
            piece.transfer_to(self.diagnostic.id(), od::Kind::Builder as u64, || {
                builder.build()
            })
        } else {
            od::scoped(self.diagnostic.id(), 0, || builder.build())
        }
    }

    /// Return the sorted spill files to use for the next phase, and the buffer size
    /// This will try to get as many spill files as possible to merge, and if we don't have enough streams
    /// it will try to reduce the buffer size until we have enough streams to merge
    /// otherwise it will return an error
    ///
    /// A merge with a copy capability also admits, with the same loop, what
    /// building its copies and merging with them takes beyond the buffers of
    /// the files it chose: their metadata and their working set (see
    /// [`copy_working_set`]); a merge with a Field cursor, which keeps the
    /// envelope of its copies, admits the working set beside it. The breadth of
    /// the merge adapts to it, and no batch merged later asks the pool for its
    /// workspace. The working set is returned (zero for any other merge).
    fn get_sorted_spill_files_to_merge(
        &mut self,
        buffer_len: usize,
        minimum_number_of_required_streams: usize,
        capability: CopyCapability,
        field: bool,
        backing: usize,
        reservation: &mut MemoryReservation,
    ) -> Result<(Vec<SortedSpillFile>, usize, usize)> {
        assert_ne!(buffer_len, 0, "Buffer length must be greater than 0");
        let mut number_of_spills_to_read_for_current_phase = 0;
        // Track total memory needed for spill file buffers. When the
        // reservation has pre-reserved bytes (from sort_spill_reservation_bytes),
        // those bytes cover the first N spill files without additional pool
        // allocation, preventing starvation under memory pressure.
        let mut total_needed: usize = 0;
        // The buffers the readers take from the reservation; the copies keep
        // what remains of it.
        let mut input_bytes: usize = 0;
        let mut largest: usize = 0;
        let heap = crate::spill::resident_metadata::std_mutex_heap_bound().unwrap_or(0);

        for spill in &self.sorted_spill_files {
            let per_spill = get_reserved_bytes_for_record_batch_size(
                spill.max_record_batch_memory,
                // Size will be the same as the sliced size, bc it is a spilled batch.
                spill.max_record_batch_memory,
            ) * buffer_len;
            total_needed += per_spill;
            let mut needed = total_needed;
            if capability != CopyCapability::Legacy {
                input_bytes += spill.max_record_batch_memory * buffer_len;
                let streams = number_of_spills_to_read_for_current_phase + 1;
                let copies = crate::spill::admission::add(
                    copy_working_set(largest.max(spill.max_record_batch_memory))?,
                    crate::spill::admission::add(
                        self.copy_metadata(capability, streams, streams, backing)?,
                        heap,
                    )?,
                )?;
                needed = needed.max(crate::spill::admission::add(input_bytes, copies)?);
            } else if field {
                needed = crate::spill::admission::add(
                    needed,
                    copy_working_set(largest.max(spill.max_record_batch_memory))?,
                )?;
            }

            // For memory pools that are not shared this is good, for other
            // this is not and there should be some upper limit to memory
            // reservation so we won't starve the system.
            match try_grow_reservation_to_at_least(reservation, needed) {
                Ok(_) => {
                    number_of_spills_to_read_for_current_phase += 1;
                    largest = largest.max(spill.max_record_batch_memory);
                }
                // If we can't grow the reservation, we need to stop
                Err(err) => {
                    // We must have at least 2 streams to merge, so if we don't have enough memory
                    // fail
                    if minimum_number_of_required_streams
                        > number_of_spills_to_read_for_current_phase
                    {
                        // Free the memory we reserved for this merge as we either try again or fail
                        reservation.free();
                        if buffer_len > 1 {
                            // Try again with smaller buffer size, it will be slower but at least we can merge
                            return self.get_sorted_spill_files_to_merge(
                                buffer_len - 1,
                                minimum_number_of_required_streams,
                                capability,
                                field,
                                backing,
                                reservation,
                            );
                        }

                        return Err(err);
                    }

                    // We reached the maximum amount of memory we can use
                    // for this merge
                    break;
                }
            }
        }

        let spills = self
            .sorted_spill_files
            .drain(..number_of_spills_to_read_for_current_phase)
            .collect::<Vec<_>>();

        let working_set = if capability == CopyCapability::Legacy && !field {
            0
        } else {
            copy_working_set(largest)?
        };
        Ok((spills, buffer_len, working_set))
    }

    /// The bytes the cursor and merge constructors, the schema and the list of
    /// the files of a merge of `streams` spill files take, which the merge
    /// admits before it builds them. `files` is the capacity of that list and
    /// `backing` that of the stream list a Field cursor may reuse.
    fn copy_metadata(
        &self,
        capability: CopyCapability,
        streams: usize,
        files: usize,
        backing: usize,
    ) -> Result<usize> {
        let (cursor, merge) = match capability {
            CopyCapability::Row { .. } => (
                super::stream::RowCursorStream::constructor_bound(
                    streams,
                    self.expr.len(),
                )?,
                super::merge::SortPreservingMergeStream::<super::cursor::RowValues>::constructor_bound(
                    streams,
                    self.batch_size,
                )?,
            ),
            CopyCapability::PrimitiveField => {
                // An empty supplied Vec can retain capacity.
                // Fund that backing and possible push growth;
                // Field separately allocates N Fuse slots.
                let capacity = backing
                    .max(crate::spill::admission::mul(streams, 2)?.max(4));
                (
                    super::stream::FieldCursorStream::<arrow::array::Int64Array>::constructor_bound(
                        streams, capacity,
                    )?,
                    super::merge::SortPreservingMergeStream::<
                        super::cursor::ArrayValues<super::cursor::PrimitiveValues<i64>>,
                    >::constructor_bound(streams, self.batch_size)?,
                )
            }
            CopyCapability::Legacy => return Ok(0),
        };
        crate::spill::admission::add(
            cursor,
            crate::spill::admission::add(
                merge,
                crate::spill::admission::add(
                    crate::spill::admission::schema_bound(&self.schema)?,
                    crate::spill::admission::mul(files, size_of::<SortedSpillFile>())?,
                )?,
            )?,
        )
    }
}

/// The memory the copies of a spill merge keep for as long as it runs, so that
/// no merged batch asks the pool for its workspace: one interleave workspace
/// and one live output, each sized as the sort sizes a copy of a batch as large
/// as the largest spill file's. The merge of a Row or Field capability checks
/// every copy against it, and asks the pool only for an output that exceeds it.
fn copy_working_set(largest: usize) -> Result<usize> {
    crate::spill::admission::mul(
        2,
        get_reserved_bytes_for_record_batch_size(largest, largest),
    )
}

struct StreamAttachedReservation {
    stream: Option<SendableRecordBatchStream>,
    schema: SchemaRef,
    reservation: MemoryReservation,
    copy: Option<Arc<CopyAdmission>>,
    #[allow(dead_code)] // Retirement-only sentinel after original fields.
    diagnostic_piece: od::Piece,
    diagnostic: od::Life,
}

impl StreamAttachedReservation {
    fn new(
        stream: SendableRecordBatchStream,
        reservation: MemoryReservation,
        copy: Option<Arc<CopyAdmission>>,
    ) -> Self {
        let diagnostic = od::Life::inherited(od::Kind::Source);
        let diagnostic_piece = od::Piece::new(&reservation, diagnostic.id());
        diagnostic.update(|o| {
            o.piece = diagnostic_piece.id();
            o.held = reservation.size() as u64;
            o.flags = 4;
        });

        let schema = stream.schema();
        Self {
            diagnostic,
            diagnostic_piece,
            stream: Some(stream),
            schema,
            reservation,
            copy,
        }
    }
    fn retire(&mut self) {
        // Cursor values, GC buffers and failed phases must die while the
        // original production floor is still held. No Arrow drop under its lock.
        self.stream.take();
        if let Some(copy) = self.copy.take() {
            copy.retire();
        }
        self.reservation.free();
    }
}
impl Drop for StreamAttachedReservation {
    fn drop(&mut self) {
        self.diagnostic.begin_drop();
        self.retire();
    }
}

impl Stream for StreamAttachedReservation {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let Some(stream) = self.stream.as_mut() else {
            return Poll::Ready(None);
        };
        let res = stream.poll_next_unpin(cx);

        match res {
            Poll::Ready(res) => {
                match res {
                    Some(Ok(batch)) => Poll::Ready(Some(Ok(batch))),
                    Some(Err(err)) => {
                        // Had an error so drop the data
                        self.retire();
                        Poll::Ready(Some(Err(err)))
                    }
                    None => {
                        // Stream is done so free the memory
                        self.retire();

                        Poll::Ready(None)
                    }
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for StreamAttachedReservation {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Drop for MultiLevelMergeBuilder {
    fn drop(&mut self) {
        self.diagnostic.begin_drop();
    }
}
