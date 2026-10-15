//! Exact payload rows may reuse the already retained outer values. Every field
//! participates in equality; a nonmatching row retains its original batch.
use std::collections::hash_map::RandomState;

use arrow::array::{RecordBatchOptions, new_empty_array, new_null_array};
use arrow::datatypes::{DataType, Field, Schema};

use super::join::{KeySource, index::Index};
use super::{Arc, Budget, RecordBatch, Result, SchemaRef, add, arrays, mul, refused, schema_bound};

pub(super) struct Dictionary {
    authority: RecordBatch,
    columns: Vec<usize>,
    incoming: Vec<usize>,
    keys: Vec<(usize, usize)>,
    probe_keys: Vec<usize>,
    handles: usize,
    rows: Vec<u32>,
    stored: usize,
}

// ScalarValue's nested equality need not preserve floating-point bit patterns.
// Keep the ordinary materializer for these valid payloads, including children.
fn exact_type(data_type: &DataType) -> bool {
    match data_type {
        DataType::Float16 | DataType::Float32 | DataType::Float64 => false,
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            exact_type(field.data_type())
        }
        DataType::Struct(fields) => fields.iter().all(|field| exact_type(field.data_type())),
        _ => true,
    }
}

fn matching<'a>(outer: &'a Schema, field: &'a Field) -> impl Iterator<Item = usize> + 'a {
    outer
        .fields()
        .iter()
        .enumerate()
        .filter(move |(_, source)| {
            source.name() == field.name() && source.data_type() == field.data_type()
        })
        .map(|(index, _)| index)
}

impl Dictionary {
    pub(super) fn new(
        outer: &RecordBatch,
        schema: &SchemaRef,
        keep: &[usize],
        outer_keys: &[usize],
        budget: &Budget,
    ) -> Result<Option<Self>> {
        let outer_schema = outer.schema();
        let mut shared = false;
        for field in schema.fields() {
            if !exact_type(field.data_type()) {
                return Ok(None);
            }
            let mut sources = matching(outer_schema.as_ref(), field);
            if let Some(index) = sources.next() {
                if sources.next().is_some()
                    || (!field.is_nullable() && outer.column(index).null_count() != 0)
                {
                    return Ok(None);
                }
                shared = true;
            } else if !field.is_nullable() {
                return Ok(None);
            }
        }
        if !shared {
            return Ok(None);
        }
        for &index in outer_keys {
            let field = outer_schema.field(index);
            let mut matches = schema.fields().iter().filter(|candidate| {
                candidate.name() == field.name() && candidate.data_type() == field.data_type()
            });
            if matches.next().is_none() || matches.next().is_some() {
                return Ok(None);
            }
        }
        let handles = mul(
            add(mul(keep.len(), 4)?, outer_keys.len())?,
            std::mem::size_of::<usize>(),
        )?;
        budget.admit(add(handles, schema_bound(schema, 0)?)?)?;
        let metadata = add(std::mem::size_of::<RecordBatch>(), mul(keep.len(), 128)?)?;
        budget.admit(metadata)?;
        let mut columns = Vec::with_capacity(keep.len());
        for field in schema.fields() {
            if let Some(index) = matching(outer_schema.as_ref(), field).next() {
                let source = outer.column(index);
                // Count complete backing aliases conservatively again.
                budget.admit(source.get_array_memory_size())?;
                columns.push(Arc::clone(source));
            } else {
                let empty = new_empty_array(field.data_type());
                let envelope = arrays::take(empty.as_ref(), outer.num_rows())?;
                budget.admit(envelope)?;
                let nulls = new_null_array(field.data_type(), outer.num_rows());
                let actual = nulls.get_array_memory_size();
                if actual > envelope {
                    return Err(refused("null authority exceeded its admitted envelope"));
                }
                budget.release(envelope - actual);
                columns.push(nulls);
            }
        }
        let authority = RecordBatch::try_new_with_options(
            schema.clone(),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(outer.num_rows())),
        )?;
        let columns: Vec<_> = (0..keep.len()).collect();
        let incoming = keep.to_vec();
        let keys: Vec<_> = columns.iter().copied().zip(keep.iter().copied()).collect();
        let probe_keys: Vec<_> = outer_keys
            .iter()
            .map(|&index| {
                let field = outer_schema.field(index);
                let slot = schema
                    .fields()
                    .iter()
                    .position(|candidate| {
                        candidate.name() == field.name()
                            && candidate.data_type() == field.data_type()
                    })
                    .expect("eligible payload key");
                keep[slot]
            })
            .collect();
        let stored = authority.num_rows();
        Ok(Some(Self {
            authority,
            columns,
            incoming,
            keys,
            probe_keys,
            handles,
            rows: Vec::new(),
            stored,
        }))
    }

    /// Return whether this entire source batch needs normal retained storage.
    pub(super) fn map(
        &mut self,
        batch: &RecordBatch,
        index: &Index<RandomState>,
        outer: KeySource<'_>,
        budget: &Budget,
    ) -> Result<bool> {
        let work = budget.index(batch, &self.authority, &self.keys, true)?;
        let candidate_work =
            budget.probe_index(batch, outer.batch, outer.columns, &self.probe_keys)?;
        let required = add(self.rows.len(), batch.num_rows())?;
        if required > self.rows.capacity() {
            let next = required.max(mul(self.rows.capacity(), 2)?);
            budget.admit(mul(next, std::mem::size_of::<u32>())?)?;
            let old = mul(self.rows.capacity(), std::mem::size_of::<u32>())?;
            self.rows
                .try_reserve_exact(next - self.rows.len())
                .map_err(|_| refused("payload row allocation failed"))?;
            budget.release(old);
        }
        let probe = KeySource {
            batch,
            columns: &self.incoming,
            mapped: None,
        };
        let source = KeySource {
            batch: &self.authority,
            columns: &self.columns,
            mapped: None,
        };
        let candidate = KeySource {
            batch,
            columns: &self.probe_keys,
            mapped: None,
        };
        let mut retained = false;
        for row in 0..batch.num_rows() {
            let matched = index.first(candidate, row, outer, true)?;
            let matched = match matched {
                Some(index) if source.key(index, true)? == probe.key(row, true)? => Some(index),
                _ => None,
            };
            let index = if let Some(index) = matched {
                index
            } else {
                retained = true;
                add(self.stored, row)?
            };
            self.rows
                .push(u32::try_from(index).map_err(|_| refused("payload row index exceeds u32"))?);
        }
        if retained {
            self.stored = add(self.stored, batch.num_rows())?;
            u32::try_from(self.stored.saturating_sub(1))
                .map_err(|_| refused("payload row index exceeds u32"))?;
        }
        budget.release(add(work, candidate_work)?);
        Ok(retained)
    }

    pub(super) fn finish(self, budget: &Budget) -> (RecordBatch, Vec<u32>) {
        let Self {
            authority,
            columns,
            incoming,
            keys,
            probe_keys,
            handles,
            rows,
            ..
        } = self;
        drop(columns);
        drop(incoming);
        drop(keys);
        drop(probe_keys);
        budget.release(handles);
        (authority, rows)
    }
}
