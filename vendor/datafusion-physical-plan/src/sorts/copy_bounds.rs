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

//! Checked allocation bounds for the pinned Arrow 58.4 operation families.
//! Unqualified expressions keep the existing envelope. Controlled Row kernels
//! require a checked payload bound before execution.
use super::builder::BufferedBatch;
use crate::spill::admission::{add, mul};
use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, Schema};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use arrow_buffer::Buffer;
use datafusion_common::{DataFusionError, Result};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use std::sync::Arc;

fn overflow() -> DataFusionError {
    DataFusionError::ResourcesExhausted(
        "Merge operation allocation bound overflow".into(),
    )
}
fn fixed_width(data_type: &DataType) -> Result<Option<usize>> {
    match data_type {
        DataType::Null | DataType::Boolean => Ok(Some(2)),
        DataType::FixedSizeBinary(width) => Ok(Some(add(
            usize::try_from(*width).map_err(|_| overflow())?,
            1,
        )?)),
        _ => data_type
            .primitive_width()
            .map(|width| add(width, 1))
            .transpose(),
    }
}
fn variable(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::Utf8View
            | DataType::BinaryView
    )
}
/// Qualify only direct Columns with stateless Arrow row codecs. The returned
/// width is the fixed component; variable cells are measured before append.
/// Single primitive/offset-byte/Utf8View keys use the separate FieldCursor.
pub(super) fn qualified_columns(
    schema: &Schema,
    expressions: &LexOrdering,
) -> Result<Option<usize>> {
    let mut width = 0;
    for expr in expressions.iter() {
        let Some(column) = expr.expr.downcast_ref::<Column>() else {
            return Ok(None);
        };
        let Some(field) = schema.fields().get(column.index()) else {
            return Ok(None);
        };
        if expressions.len() == 1
            && (field.data_type().primitive_width().is_some()
                || matches!(
                    field.data_type(),
                    DataType::Utf8
                        | DataType::LargeUtf8
                        | DataType::Binary
                        | DataType::LargeBinary
                        | DataType::Utf8View
                ))
        {
            return Ok(None);
        }
        if variable(field.data_type()) {
            continue;
        }
        let Some(encoded) = fixed_width(field.data_type())? else {
            return Ok(None);
        };
        width = add(width, encoded)?;
    }
    Ok(Some(width))
}
/// This Field proof is deliberately narrower than Arrow's dispatch table.
/// Direct Int64 values shallow-clone their ScalarBuffer. Only the already
/// proved flat Int64/Utf8 output kernels enter this new seam.
pub(super) fn primitive_field(schema: &Schema, expressions: &LexOrdering) -> bool {
    expressions.len() == 1
        && expressions[0]
            .expr
            .downcast_ref::<Column>()
            .and_then(|column| schema.fields().get(column.index()))
            .is_some_and(|field| field.data_type() == &DataType::Int64)
        && schema
            .fields()
            .iter()
            .all(|field| matches!(field.data_type(), DataType::Int64 | DataType::Utf8))
}
/// Append reuses two Vecs; reserve/resize needs at most max(2*old, required,
/// minimum). Keep old backing resident and admit complete new capacities.
/// Arrow row variable.rs uses mini-blocks through 32 bytes and 32-byte blocks
/// thereafter; a NULL cell is one byte. One LengthTracker Vec is materialized
/// for all variable columns, with later columns updating it in place.
pub(super) fn row_workspace(
    old_rows_size: usize,
    batch: &RecordBatch,
    expressions: &[Arc<dyn PhysicalExpr>],
    fixed_width: usize,
) -> Result<usize> {
    let rows = batch.num_rows();
    let mut encoded = mul(rows, fixed_width)?;
    let mut has_variable = false;
    for expression in expressions {
        // Constructor qualification proves these are source-array clones.
        let column = expression.downcast_ref::<Column>().ok_or_else(overflow)?;
        let array = batch.column(column.index());
        if !variable(array.data_type()) {
            continue;
        }
        has_variable = true;
        for row in 0..rows {
            let length = if array.is_null(row) {
                1
            } else {
                let len = match array.data_type() {
                    DataType::Utf8View => {
                        array.as_string_view().views()[row] as u32 as usize
                    }
                    DataType::BinaryView => {
                        array.as_binary_view().views()[row] as u32 as usize
                    }
                    _ => {
                        let (start, end) = span(array.as_ref(), row)?;
                        end - start
                    }
                };
                if len <= 32 {
                    add(1, mul(add(len, 7)? / 8, 9)?)?
                } else {
                    add(4, mul(add(len, 31)? / 32, 33)?)?
                }
            };
            encoded = add(encoded, length)?;
        }
    }
    add(
        add(mul(old_rows_size, 2)?, encoded)?,
        add(
            mul(add(rows, 1)?, size_of::<usize>())?,
            add(
                if has_variable {
                    mul(rows, size_of::<usize>())?
                } else {
                    0
                },
                add(mul(expressions.len(), 512)?, 4096)?,
            )?,
        )?,
    )
}
fn aligned(bytes: usize) -> Result<usize> {
    Ok(mul(add(bytes, 63)? / 64, 64)?)
}
fn bitmap(rows: usize) -> Result<usize> {
    aligned(add(rows, 7)? / 8)
}
fn span(array: &dyn Array, row: usize) -> Result<(usize, usize)> {
    let (start, end) = match array.data_type() {
        DataType::Utf8 => {
            let o = array.as_string::<i32>().value_offsets();
            (i64::from(o[row]), i64::from(o[row + 1]))
        }
        DataType::Binary => {
            let o = array.as_binary::<i32>().value_offsets();
            (i64::from(o[row]), i64::from(o[row + 1]))
        }
        DataType::LargeUtf8 => {
            let o = array.as_string::<i64>().value_offsets();
            (o[row], o[row + 1])
        }
        DataType::LargeBinary => {
            let o = array.as_binary::<i64>().value_offsets();
            (o[row], o[row + 1])
        }
        DataType::List(_) => {
            let o = array.as_list::<i32>().value_offsets();
            (i64::from(o[row]), i64::from(o[row + 1]))
        }
        DataType::LargeList(_) => {
            let o = array.as_list::<i64>().value_offsets();
            (o[row], o[row + 1])
        }
        _ => unreachable!("qualified offset array"),
    };
    let start = usize::try_from(start).map_err(|_| overflow())?;
    let end = usize::try_from(end).map_err(|_| overflow())?;
    if end < start {
        return Err(overflow());
    }
    Ok((start, end))
}
/// Stack-borrowed paths select a child without building ArrayRef vectors.
enum Arrays<'a> {
    Column(&'a [BufferedBatch], usize),
    Struct(&'a Arrays<'a>, usize),
    List(&'a Arrays<'a>),
}
impl Arrays<'_> {
    fn sources(&self) -> usize {
        match self {
            Self::Column(batches, _) => batches.len(),
            Self::Struct(parent, _) | Self::List(parent) => parent.sources(),
        }
    }
    fn array(&self, source: usize) -> &dyn Array {
        match self {
            Self::Column(batches, column) => {
                batches[source].batch.column(*column).as_ref()
            }
            Self::Struct(parent, column) => {
                parent.array(source).as_struct().column(*column).as_ref()
            }
            Self::List(parent) => match parent.array(source).data_type() {
                DataType::List(_) => {
                    parent.array(source).as_list::<i32>().values().as_ref()
                }
                DataType::LargeList(_) => {
                    parent.array(source).as_list::<i64>().values().as_ref()
                }
                _ => unreachable!("qualified list path"),
            },
        }
    }
}
enum Selection<'a> {
    Rows(&'a [(usize, usize)]),
    Children {
        parent: &'a Selection<'a>,
        arrays: &'a Arrays<'a>,
        len: usize,
    },
}
impl Selection<'_> {
    fn len(&self) -> usize {
        match self {
            Self::Rows(rows) => rows.len(),
            Self::Children { len, .. } => *len,
        }
    }
    fn each(&self, visit: &mut dyn FnMut(usize, usize) -> Result<()>) -> Result<()> {
        match self {
            Self::Rows(rows) => {
                for (source, row) in *rows {
                    visit(*source, *row)?;
                }
            }
            Self::Children { parent, arrays, .. } => {
                parent.each(&mut |source, row| {
                    let (start, end) = span(arrays.array(source), row)?;
                    for child in start..end {
                        visit(source, child)?;
                    }
                    Ok(())
                })?;
            }
        }
        Ok(())
    }
}
/// interleave.rs: primitives allocate n*width; byte arrays allocate exact
/// selected spans and n+1 offsets. Struct recurses even under a NULL parent.
/// List allocates exact flattened child_indices before recursively interleaving.
/// NULL offset spans are deliberately included: Arrow copies them too.
/// MutableArrayData's Boolean/FixedBinary fallback preallocates aligned buffers
/// at output capacity, so these branches do not grow payload buffers.
fn node(arrays: &Arrays<'_>, selected: &Selection<'_>) -> Result<Option<usize>> {
    let data_type = arrays.array(0).data_type();
    let rows = selected.len();
    let nulls = bitmap(rows)?;
    if let Some(width) = data_type.primitive_width() {
        return Ok(Some(add(nulls, mul(rows, width)?)?));
    }
    match data_type {
        DataType::Null => Ok(Some(0)),
        DataType::Boolean => Ok(Some(mul(nulls, 2)?)),
        DataType::Utf8View | DataType::BinaryView => {
            // Only the flat view family is qualified here; nested view kernels
            // retain the original envelope until separately covered.
            if !matches!(arrays, Arrays::Column(_, _)) {
                return Ok(None);
            }
            // interleave_views maps ALL source data-buffer handles, including
            // unused/empty handles and physical aliases. Each selected raw view
            // (even NULL) can add one logical handle to the output Buffer Vec.
            let mut buffers = 0;
            for source in 0..arrays.sources() {
                let array = arrays.array(source);
                let count = match data_type {
                    DataType::Utf8View => array.as_string_view().data_buffers().len(),
                    _ => array.as_binary_view().data_buffers().len(),
                };
                buffers = add(buffers, count)?;
            }
            let handles = buffers.min(rows);
            let capacity = if handles == 0 { 0 } else { handles.max(4) };
            // The larger of Vec old+new realloc and Vec-to-Arc conversion
            // peaks fits 3*K Buffer headers. Payload buffers remain aliases
            // of source IPC owners and retain their existing lifetime tokens.
            Ok(Some(add(
                add(mul(rows, 16)?, nulls)?,
                add(
                    mul(add(arrays.sources(), 1)?, size_of::<usize>())?,
                    add(
                        mul(buffers, size_of::<Option<u32>>())?,
                        add(
                            mul(mul(3, capacity)?, size_of::<Buffer>())?,
                            2 * size_of::<usize>(),
                        )?,
                    )?,
                )?,
            )?))
        }
        DataType::FixedSizeBinary(width) => Ok(Some(add(
            nulls,
            aligned(mul(rows, usize::try_from(*width).map_err(|_| overflow())?)?)?,
        )?)),
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::List(_)
        | DataType::LargeList(_) => {
            let large = matches!(
                data_type,
                DataType::LargeUtf8 | DataType::LargeBinary | DataType::LargeList(_)
            );
            let mut count = 0;
            selected.each(&mut |source, row| {
                let (start, end) = span(arrays.array(source), row)?;
                count = add(count, end - start)?;
                Ok(())
            })?;
            // Preserve the kernel's existing offset-overflow/partial-output path.
            if !large && count > i32::MAX as usize {
                return Err(ArrowError::OffsetOverflowError(count).into());
            }
            let offsets = mul(add(rows, 1)?, if large { 8 } else { 4 })?;
            let base = add(nulls, offsets)?;
            if matches!(data_type, DataType::List(_) | DataType::LargeList(_)) {
                let children = Selection::Children {
                    parent: selected,
                    arrays,
                    len: count,
                };
                let Some(child) = node(&Arrays::List(arrays), &children)? else {
                    return Ok(None);
                };
                Ok(Some(add(
                    base,
                    add(mul(count, size_of::<(usize, usize)>())?, child)?,
                )?))
            } else {
                Ok(Some(add(base, count)?))
            }
        }
        DataType::Struct(fields) => {
            let mut total = nulls;
            for column in 0..fields.len() {
                let Some(child) = node(&Arrays::Struct(arrays, column), selected)? else {
                    return Ok(None);
                };
                total = add(total, child)?;
            }
            Ok(Some(total))
        }
        _ => Ok(None),
    }
}
/// Payload and child-index workspace only. SourceOwners separately preadmits
/// full ArrayData/reference/closure Vecs and headers before the kernel: its
/// per-source node allowance covers the pinned fixed-binary/null fallback's
/// two boxed 24-byte closures and their fat-pointer Vecs as well as to_data.
/// Input backing remains fully charged independently of these visible spans.
pub(super) fn interleave(
    batches: &[BufferedBatch],
    indices: &[(usize, usize)],
) -> Result<Option<usize>> {
    let selected = Selection::Rows(indices);
    let mut total = 0;
    for column in 0..batches[0].batch.num_columns() {
        let Some(bytes) = node(&Arrays::Column(batches, column), &selected)? else {
            return Ok(None);
        };
        total = add(total, bytes)?;
    }
    Ok(Some(total))
}

/// Payloads survive their sibling kernels; List child-index scratch does not.
/// Metadata is deliberately separate: SourceOwners adds the complete active
/// source-column construction bound and accumulating output headers.
pub(super) struct SerialPayload {
    pub(super) resident: usize,
    pub(super) peak: usize,
}
fn serial_node(
    arrays: &Arrays<'_>,
    selected: &Selection<'_>,
    depth: usize,
) -> Result<Option<SerialPayload>> {
    if depth >= 64 {
        return Ok(None);
    }
    let data_type = arrays.array(0).data_type();
    let rows = selected.len();
    let nulls = bitmap(rows)?;
    let resident = if let Some(width) = data_type.primitive_width() {
        add(nulls, mul(rows, width)?)?
    } else {
        match data_type {
            DataType::Null => 0,
            DataType::Boolean => mul(nulls, 2)?,
            DataType::FixedSizeBinary(width) => add(
                nulls,
                aligned(mul(rows, usize::try_from(*width).map_err(|_| overflow())?)?)?,
            )?,
            DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::List(_)
            | DataType::LargeList(_) => {
                let large = matches!(
                    data_type,
                    DataType::LargeUtf8 | DataType::LargeBinary | DataType::LargeList(_)
                );
                let mut count = 0;
                selected.each(&mut |source, row| {
                    let (start, end) = span(arrays.array(source), row)?;
                    count = add(count, end - start)?;
                    Ok(())
                })?;
                if !large && count > i32::MAX as usize {
                    return Err(ArrowError::OffsetOverflowError(count).into());
                }
                let base = add(nulls, mul(add(rows, 1)?, if large { 8 } else { 4 })?)?;
                if matches!(data_type, DataType::List(_) | DataType::LargeList(_)) {
                    let children = Selection::Children {
                        parent: selected,
                        arrays,
                        len: count,
                    };
                    let Some(child) =
                        serial_node(&Arrays::List(arrays), &children, depth + 1)?
                    else {
                        return Ok(None);
                    };
                    return Ok(Some(SerialPayload {
                        resident: add(base, child.resident)?,
                        // Parent offsets/validity and child_indices remain live
                        // throughout the complete recursive child invocation.
                        peak: add(
                            base,
                            add(mul(count, size_of::<(usize, usize)>())?, child.peak)?,
                        )?,
                    }));
                }
                add(base, count)?
            }
            DataType::Struct(fields) => {
                let mut resident = nulls;
                let mut peak = nulls;
                for column in 0..fields.len() {
                    let Some(child) = serial_node(
                        &Arrays::Struct(arrays, column),
                        selected,
                        depth + 1,
                    )?
                    else {
                        return Ok(None);
                    };
                    peak = peak.max(add(resident, child.peak)?);
                    resident = add(resident, child.resident)?;
                }
                return Ok(Some(SerialPayload { resident, peak }));
            }
            // Views/dictionaries and every unproved kernel keep the old path.
            _ => return Ok(None),
        }
    };
    Ok(Some(SerialPayload {
        resident,
        peak: resident,
    }))
}

pub(super) fn serial_interleave(
    batches: &[BufferedBatch],
    indices: &[(usize, usize)],
) -> Result<Option<SerialPayload>> {
    if batches.is_empty() || indices.is_empty() {
        return Ok(None);
    }
    let selected = Selection::Rows(indices);
    let mut resident = 0;
    let mut peak = 0;
    for column in 0..batches[0].batch.num_columns() {
        let Some(child) = serial_node(&Arrays::Column(batches, column), &selected, 0)?
        else {
            return Ok(None);
        };
        peak = peak.max(add(resident, child.peak)?);
        resident = add(resident, child.resident)?;
    }
    Ok(Some(SerialPayload { resident, peak }))
}
