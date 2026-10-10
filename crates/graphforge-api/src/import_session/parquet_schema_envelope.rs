//! Borrowed validation and allocation envelope for parquet's native schema
//! topology. This component covers schema construction only; footer metadata
//! and Arrow inference/hint allocations are admitted by their own phases.

use std::alloc::Layout;
use std::mem::size_of;
use std::sync::Arc;

use graphforge_core::GfError;
use parquet::schema::types::{ColumnDescriptor, SchemaDescriptor, Type};

use super::inventory_budget::InventoryBudget;
use super::parquet_compact::CompactSlice;
use super::parquet_footer_counts::{FooterCountFacts, SchemaElementScalarFacts, schema_element};
use super::{cancelled, limit, storage};
use crate::CancellationToken;

/// Scalar facts for the selected first schema and the native Type/descriptor
/// heap request envelope. `preflight_peak_bytes` describes only temporary
/// scratch used by this walk; it is released before native decoding.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct SchemaTopologyFacts {
    pub(super) nodes: u64,
    pub(super) physical_leaves: u64,
    pub(super) name_bytes: u64,
    pub(super) group_child_slots: u64,
    pub(super) primitive_crs_clone_bytes: u64,
    pub(super) group_crs_clone_bytes: u64,
    pub(super) path_string_slots: u64,
    pub(super) path_name_bytes: u64,
    pub(super) max_visited_depth: u64,
    pub(super) max_definition_level: i16,
    pub(super) max_repetition_level: i16,
    pub(super) native_request_bytes: u64,
    pub(super) preflight_peak_bytes: u64,
}

#[derive(Clone, Copy)]
struct Frame {
    remaining_children: usize,
    path_name_bytes: u64,
    depth: usize,
    definition: i16,
    repetition: i16,
}

fn malformed() -> GfError {
    storage("Parquet footer schema topology is malformed")
}

fn overflow() -> GfError {
    limit("Parquet native schema allocation exceeds the admitted budget")
}

fn add(total: &mut u64, amount: u64) -> Result<(), GfError> {
    *total = total.checked_add(amount).ok_or_else(overflow)?;
    Ok(())
}

fn bytes(count: u64, width: usize) -> Result<u64, GfError> {
    let count = usize::try_from(count).map_err(|_| overflow())?;
    let requested = count.checked_mul(width).ok_or_else(overflow)?;
    if requested > isize::MAX as usize {
        return Err(overflow());
    }
    u64::try_from(requested).map_err(|_| overflow())
}

fn arc_allocation<T>() -> Result<u64, GfError> {
    let header = Layout::new::<std::sync::atomic::AtomicUsize>()
        .extend(Layout::new::<std::sync::atomic::AtomicUsize>())
        .map_err(|_| overflow())?
        .0;
    let layout = header
        .extend(Layout::new::<T>())
        .map_err(|_| overflow())?
        .0
        .pad_to_align();
    if layout.size() > isize::MAX as usize {
        return Err(overflow());
    }
    u64::try_from(layout.size()).map_err(|_| overflow())
}

fn selected_shape(element: SchemaElementScalarFacts, root: bool) -> Result<(usize, bool), GfError> {
    let children = element.children.unwrap_or(0);
    if root && children == 0 {
        return Ok((0, false));
    }
    if children > 0 {
        return Ok((children, false));
    }
    if element.repetition.is_none() {
        return Err(malformed());
    }
    Ok((0, element.physical_type.is_some()))
}

fn reserve_frame(
    stack: &mut Vec<Frame>,
    budget: &mut InventoryBudget,
    peak: &mut u64,
    cancellation: Option<&CancellationToken>,
) -> Result<(), GfError> {
    let required = stack.len().checked_add(1).ok_or_else(overflow)?;
    if required <= stack.capacity() {
        return Ok(());
    }
    if cancellation.is_some_and(CancellationToken::is_cancelled) {
        return Err(cancelled());
    }
    let request =
        super::parquet_alloc::vector(stack.capacity(), required, size_of::<Frame>(), false)?;
    budget.admit(request.retained_bytes, "the Parquet schema topology stack")?;
    *peak = (*peak).max(request.peak_bytes);
    let old = u64::try_from(stack.capacity())
        .map_err(|_| overflow())?
        .checked_mul(u64::try_from(size_of::<Frame>()).map_err(|_| overflow())?)
        .ok_or_else(overflow)?;
    let new_capacity = request
        .retained_bytes
        .checked_div(u64::try_from(size_of::<Frame>()).map_err(|_| overflow())?)
        .ok_or_else(overflow)?;
    let additional = usize::try_from(new_capacity)
        .map_err(|_| overflow())?
        .checked_sub(stack.len())
        .ok_or_else(overflow)?;
    if stack.try_reserve_exact(additional).is_err() {
        budget.release(request.retained_bytes);
        return Err(overflow());
    }
    budget.release(old);
    Ok(())
}

/// Replay the first SchemaElement list with the same compact grammar used by
/// footer accounting, validate native's flattened preorder tree, and compute
/// the native schema heap request envelope without constructing native types.
pub(super) fn preflight(
    footer: &[u8],
    footer_facts: &FooterCountFacts,
    budget: &mut InventoryBudget,
    cancellation: Option<&CancellationToken>,
) -> Result<SchemaTopologyFacts, GfError> {
    if cancellation.is_some_and(CancellationToken::is_cancelled) {
        return Err(cancelled());
    }
    let offset = footer_facts.schema_offset.ok_or_else(malformed)?;
    let count = usize::try_from(footer_facts.schema_elements).map_err(|_| overflow())?;
    if count == 0 {
        return Err(malformed());
    }
    let mut result = SchemaTopologyFacts::default();
    let mut stack = Vec::<Frame>::new();
    let mut cursor = CompactSlice::new(&footer[offset..], cancellation);
    let mut parser_facts = FooterCountFacts::default();

    for index in 0..count {
        if cancellation.is_some_and(CancellationToken::is_cancelled) {
            return Err(cancelled());
        }
        let element = schema_element(&mut cursor, &mut parser_facts, u64::MAX, index == 0)?;
        add(&mut result.nodes, 1)?;
        add(
            &mut result.name_bytes,
            u64::try_from(element.name_len).map_err(|_| overflow())?,
        )?;

        if index == 0 {
            let (children, _) = selected_shape(element, true)?;
            if children == 0 {
                if count != 1 {
                    return Err(malformed());
                }
                result.max_visited_depth = 0;
                continue;
            }
            if children > count - 1 {
                return Err(malformed());
            }
            add(
                &mut result.group_child_slots,
                u64::try_from(children).map_err(|_| overflow())?,
            )?;
            if let Some(len) = element.crs_len {
                add(
                    &mut result.group_crs_clone_bytes,
                    u64::try_from(len).map_err(|_| overflow())?,
                )?;
            }
            reserve_frame(
                &mut stack,
                budget,
                &mut result.preflight_peak_bytes,
                cancellation,
            )?;
            stack.push(Frame {
                remaining_children: children,
                path_name_bytes: 0,
                depth: 0,
                definition: 0,
                repetition: 0,
            });
            continue;
        }

        while stack
            .last()
            .is_some_and(|frame| frame.remaining_children == 0)
        {
            stack.pop();
        }
        let parent = stack.last_mut().ok_or_else(malformed)?;
        parent.remaining_children -= 1;
        let depth = parent.depth.checked_add(1).ok_or_else(overflow)?;
        let path_name_bytes = parent
            .path_name_bytes
            .checked_add(u64::try_from(element.name_len).map_err(|_| overflow())?)
            .ok_or_else(overflow)?;
        let repetition = element.repetition.ok_or_else(malformed)?;
        if !(0..=2).contains(&repetition) {
            return Err(malformed());
        }
        let def_increment = i16::from(repetition != 0);
        let rep_increment = i16::from(repetition == 2);
        let definition = parent
            .definition
            .checked_add(def_increment)
            .ok_or_else(malformed)?;
        let repetition_level = parent
            .repetition
            .checked_add(rep_increment)
            .ok_or_else(malformed)?;
        result.max_definition_level = result.max_definition_level.max(definition);
        result.max_repetition_level = result.max_repetition_level.max(repetition_level);
        result.max_visited_depth = result
            .max_visited_depth
            .max(u64::try_from(depth).map_err(|_| overflow())?);

        let (children, primitive) = selected_shape(element, false)?;
        if primitive {
            add(&mut result.physical_leaves, 1)?;
            if let Some(len) = element.crs_len {
                add(
                    &mut result.primitive_crs_clone_bytes,
                    u64::try_from(len).map_err(|_| overflow())?,
                )?;
            }
            add(
                &mut result.path_string_slots,
                u64::try_from(depth.max(4)).map_err(|_| overflow())?,
            )?;
            add(&mut result.path_name_bytes, path_name_bytes)?;
        } else if children > 0 {
            if children > count - index - 1 {
                return Err(malformed());
            }
            add(
                &mut result.group_child_slots,
                u64::try_from(children).map_err(|_| overflow())?,
            )?;
            if let Some(len) = element.crs_len {
                add(
                    &mut result.group_crs_clone_bytes,
                    u64::try_from(len).map_err(|_| overflow())?,
                )?;
            }
            reserve_frame(
                &mut stack,
                budget,
                &mut result.preflight_peak_bytes,
                cancellation,
            )?;
            stack.push(Frame {
                remaining_children: children,
                path_name_bytes,
                depth,
                definition,
                repetition: repetition_level,
            });
        } else if let Some(len) = element.crs_len {
            // Native's zero-child, no-physical-type compatibility case is a
            // real empty GroupType and clones its logical annotation once.
            add(
                &mut result.group_crs_clone_bytes,
                u64::try_from(len).map_err(|_| overflow())?,
            )?;
        }
    }
    while stack
        .last()
        .is_some_and(|frame| frame.remaining_children == 0)
    {
        stack.pop();
    }
    if !stack.is_empty()
        || result.physical_leaves
            != u64::try_from(footer_facts.schema_leaves).map_err(|_| overflow())?
    {
        return Err(malformed());
    }

    let mut native = 0u64;
    let type_arc = arc_allocation::<Type>()?;
    let schema_arc = arc_allocation::<SchemaDescriptor>()?;
    let column_arc = arc_allocation::<ColumnDescriptor>()?;
    add(
        &mut native,
        result.nodes.checked_mul(type_arc).ok_or_else(overflow)?,
    )?;
    add(&mut native, result.name_bytes)?;
    add(
        &mut native,
        result
            .primitive_crs_clone_bytes
            .checked_mul(2)
            .ok_or_else(overflow)?,
    )?;
    add(&mut native, result.group_crs_clone_bytes)?;
    add(
        &mut native,
        bytes(result.group_child_slots, size_of::<Arc<Type>>())?,
    )?;
    add(&mut native, schema_arc)?;
    add(
        &mut native,
        bytes(result.physical_leaves, size_of::<Arc<ColumnDescriptor>>())?,
    )?;
    add(
        &mut native,
        bytes(result.physical_leaves, size_of::<usize>())?,
    )?;
    add(
        &mut native,
        result
            .physical_leaves
            .checked_mul(column_arc)
            .ok_or_else(overflow)?,
    )?;
    add(
        &mut native,
        bytes(result.path_string_slots, size_of::<String>())?,
    )?;
    add(&mut native, result.path_name_bytes)?;
    add(&mut native, bytes(1, size_of::<Arc<Type>>())?)?;
    let scratch = super::parquet_alloc::vector_envelope(
        16,
        usize::try_from(result.max_visited_depth).map_err(|_| overflow())?,
        size_of::<&str>(),
    )?;
    add(&mut native, scratch.peak_bytes)?;
    result.native_request_bytes = native;
    let stack_bytes = u64::try_from(stack.capacity())
        .map_err(|_| overflow())?
        .checked_mul(u64::try_from(size_of::<Frame>()).map_err(|_| overflow())?)
        .ok_or_else(overflow)?;
    drop(stack);
    budget.release(stack_bytes);
    Ok(result)
}

#[cfg(test)]
mod tests;
