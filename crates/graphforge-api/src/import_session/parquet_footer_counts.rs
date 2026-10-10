//! Allocation-free accounting for the allocating lists and copied payloads
//! in parquet 58.4's default footer metadata reader.
//!
//! This accounts the footer's native metadata vectors and copied primitive
//! payloads only. It does not include the expanded `Type` tree, schema
//! descriptor/path expansion, Arrow inference, or the Arrow schema hint; those
//! are separate admission phases.

use std::alloc::Layout;
use std::mem::{align_of, size_of};

use graphforge_core::GfError;
use parquet::basic::{ConvertedType, LogicalType, Repetition, Type};
use parquet::file::metadata::{ColumnChunkMetaData, KeyValue, RowGroupMetaData, SortingColumn};

use super::parquet_compact::{CompactSlice, Kind};
use super::{cancelled, limit, storage};
use crate::CancellationToken;

/// Scalar allocation facts from the native footer metadata phase.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct FooterCountFacts {
    /// Sum of backing requests for native vectors (including conservative
    /// duplicate-field coexistence and repeated-column Vec growth).
    pub(super) vector_bytes: u64,
    /// Peak covered by the same conservative coexistence bound.
    pub(super) vector_peak_bytes: u64,
    /// Requested bytes in copied strings and binary statistics payloads.
    pub(super) owned_payload_bytes: u64,
    /// Fixed boxed geospatial-statistics objects (including inline boxes).
    pub(super) fixed_allocation_bytes: u64,
    /// bytes crate Shared boxes accompanying short statistics payload Vecs.
    pub(super) bytes_control_bytes: u64,
    /// Number of elements requested in native schema lists.
    pub(super) schema_elements: u64,
    /// Physical leaves in the first schema, used to bound native row-group
    /// column storage and validate its required list length.
    pub(super) schema_leaves: usize,
    /// Sum of borrowed SchemaElement names copied by the later Type-tree phase.
    pub(super) schema_name_bytes: u64,
    /// Root's declared child count, for the later flattened-topology check.
    pub(super) root_children: Option<u64>,
    /// Start of the selected first schema in the borrowed footer.
    pub(super) schema_offset: Option<usize>,
    /// Number of elements requested in native row-group lists.
    pub(super) row_groups: u64,
    /// Number of copied payload allocation requests.
    pub(super) owned_payloads: u64,
}

impl FooterCountFacts {
    fn account_vector(&mut self, bytes: usize, budget: u64) -> Result<(), GfError> {
        let bytes = u64::try_from(bytes).map_err(|_| allocation_limit())?;
        self.vector_bytes = self
            .vector_bytes
            .checked_add(bytes)
            .ok_or_else(allocation_limit)?;
        self.vector_peak_bytes = self.vector_bytes;
        if self
            .vector_bytes
            .checked_add(self.owned_payload_bytes)
            .and_then(|bytes| bytes.checked_add(self.fixed_allocation_bytes))
            .and_then(|bytes| bytes.checked_add(self.bytes_control_bytes))
            .ok_or_else(allocation_limit)?
            > budget
        {
            return Err(limit(
                "Parquet footer metadata allocations exceed the admitted budget",
            ));
        }
        Ok(())
    }

    fn account_payload(&mut self, bytes: usize, budget: u64) -> Result<(), GfError> {
        // ByteArray::from(&[u8]) grows an empty Vec to at least eight bytes.
        // Strings use the requested length, and empty payloads allocate no
        // backing bytes. The caller passes the native request size.
        let bytes = u64::try_from(bytes).map_err(|_| allocation_limit())?;
        if bytes != 0 {
            self.owned_payloads = self
                .owned_payloads
                .checked_add(1)
                .ok_or_else(allocation_limit)?;
        }
        self.owned_payload_bytes = self
            .owned_payload_bytes
            .checked_add(bytes)
            .ok_or_else(allocation_limit)?;
        if self
            .vector_bytes
            .checked_add(self.owned_payload_bytes)
            .and_then(|bytes| bytes.checked_add(self.fixed_allocation_bytes))
            .and_then(|bytes| bytes.checked_add(self.bytes_control_bytes))
            .ok_or_else(allocation_limit)?
            > budget
        {
            return Err(limit(
                "Parquet footer metadata allocations exceed the admitted budget",
            ));
        }
        Ok(())
    }

    fn account_fixed(&mut self, bytes: usize, budget: u64) -> Result<(), GfError> {
        self.fixed_allocation_bytes = self
            .fixed_allocation_bytes
            .checked_add(u64::try_from(bytes).map_err(|_| allocation_limit())?)
            .ok_or_else(allocation_limit)?;
        if self
            .vector_bytes
            .checked_add(self.owned_payload_bytes)
            .and_then(|value| value.checked_add(self.fixed_allocation_bytes))
            .and_then(|value| value.checked_add(self.bytes_control_bytes))
            .ok_or_else(allocation_limit)?
            > budget
        {
            return Err(limit(
                "Parquet footer metadata allocations exceed the admitted budget",
            ));
        }
        Ok(())
    }

    fn account_bytes_control(&mut self, bytes: usize, budget: u64) -> Result<(), GfError> {
        self.bytes_control_bytes = self
            .bytes_control_bytes
            .checked_add(u64::try_from(bytes).map_err(|_| allocation_limit())?)
            .ok_or_else(allocation_limit)?;
        if self
            .vector_bytes
            .checked_add(self.owned_payload_bytes)
            .and_then(|value| value.checked_add(self.fixed_allocation_bytes))
            .and_then(|value| value.checked_add(self.bytes_control_bytes))
            .ok_or_else(allocation_limit)?
            > budget
        {
            return Err(limit(
                "Parquet footer metadata allocations exceed the admitted budget",
            ));
        }
        Ok(())
    }
}

fn allocation_limit() -> GfError {
    limit("Parquet footer metadata allocation size is not representable")
}

fn malformed() -> GfError {
    storage("Parquet footer compact metadata struct is malformed")
}

/// A finite padding/member-layout envelope for parquet's private SchemaElement
/// repr(Rust) declaration. Each member's actual pinned type size and alignment
/// comes from the same parquet source declaration; this is a bound, not an
/// equal-size local mirror.
fn schema_element_envelope() -> Result<usize, GfError> {
    let members = [
        (size_of::<Option<Type>>(), align_of::<Option<Type>>()),
        (size_of::<Option<i32>>(), align_of::<Option<i32>>()),
        (
            size_of::<Option<Repetition>>(),
            align_of::<Option<Repetition>>(),
        ),
        (size_of::<&str>(), align_of::<&str>()),
        (size_of::<Option<i32>>(), align_of::<Option<i32>>()),
        (
            size_of::<Option<ConvertedType>>(),
            align_of::<Option<ConvertedType>>(),
        ),
        (size_of::<Option<i32>>(), align_of::<Option<i32>>()),
        (size_of::<Option<i32>>(), align_of::<Option<i32>>()),
        (size_of::<Option<i32>>(), align_of::<Option<i32>>()),
        (
            size_of::<Option<LogicalType>>(),
            align_of::<Option<LogicalType>>(),
        ),
    ];
    let max_align = members
        .iter()
        .map(|(_, align)| *align)
        .max()
        .ok_or_else(allocation_limit)?;
    let unpadded = members
        .iter()
        .try_fold(0usize, |sum, (size, align)| {
            sum.checked_add(*size)?.checked_add(align.saturating_sub(1))
        })
        .ok_or_else(allocation_limit)?;
    let layout = Layout::from_size_align(unpadded, max_align).map_err(|_| allocation_limit())?;
    Ok(layout.pad_to_align().size())
}

fn admit_list(
    cursor: &mut CompactSlice<'_>,
    facts: &mut FooterCountFacts,
    width: usize,
    budget: u64,
) -> Result<usize, GfError> {
    let used = facts
        .vector_bytes
        .checked_add(facts.owned_payload_bytes)
        .and_then(|value| value.checked_add(facts.fixed_allocation_bytes))
        .and_then(|value| value.checked_add(facts.bytes_control_bytes))
        .ok_or_else(allocation_limit)?;
    let remaining = budget.checked_sub(used).unwrap_or(0);
    let list = cursor.allocating_list(width, remaining)?;
    let bytes = list.count.checked_mul(width).ok_or_else(allocation_limit)?;
    facts.account_vector(bytes, budget)?;
    Ok(list.count)
}

fn payload(
    facts: &mut FooterCountFacts,
    len: usize,
    bytes_vec: bool,
    budget: u64,
) -> Result<(), GfError> {
    let request = if bytes_vec && len > 0 {
        let capacity = len.max(8);
        if capacity != len {
            facts.account_bytes_control(bytes_shared_envelope()?, budget)?;
        }
        capacity
    } else {
        len
    };
    facts.account_payload(request, budget)
}

fn bytes_shared_envelope() -> Result<usize, GfError> {
    // bytes 1.12.1 Shared fields: pointer, capacity, AtomicUsize.
    let members = [
        (size_of::<*mut u8>(), align_of::<*mut u8>()),
        (size_of::<usize>(), align_of::<usize>()),
        (
            size_of::<std::sync::atomic::AtomicUsize>(),
            align_of::<std::sync::atomic::AtomicUsize>(),
        ),
    ];
    let mut layout = Layout::from_size_align(0, 1).map_err(|_| allocation_limit())?;
    for (size, align) in members {
        (layout, _) = layout
            .extend(Layout::from_size_align(size, align).map_err(|_| allocation_limit())?)
            .map_err(|_| allocation_limit())?;
    }
    Ok(layout.pad_to_align().size())
}

fn field(
    cursor: &mut CompactSlice<'_>,
    previous: &mut i16,
) -> Result<Option<(i16, Kind)>, GfError> {
    let Some(next) = cursor.read_field(*previous)? else {
        return Ok(None);
    };
    *previous = next.id;
    Ok(Some((next.id, next.kind)))
}

fn skip(cursor: &mut CompactSlice<'_>, kind: Kind) -> Result<(), GfError> {
    cursor.skip(kind)
}

fn read_utf8_payload(
    cursor: &mut CompactSlice<'_>,
    facts: &mut FooterCountFacts,
    budget: u64,
) -> Result<(), GfError> {
    let value = cursor.read_utf8()?;
    payload(facts, value.len(), false, budget)
}

fn read_binary_payload(
    cursor: &mut CompactSlice<'_>,
    facts: &mut FooterCountFacts,
    budget: u64,
) -> Result<(), GfError> {
    let value = cursor.read_bytes()?;
    payload(facts, value.len(), true, budget)
}

pub(super) fn schema_element(
    cursor: &mut CompactSlice<'_>,
    facts: &mut FooterCountFacts,
    budget: u64,
    is_root_node: bool,
) -> Result<SchemaElementScalarFacts, GfError> {
    let mut previous = 0;
    let mut name_len = None;
    let mut physical_type = false;
    let mut physical_type_id = None;
    let mut children = None;
    let mut repetition = None;
    let mut selected_crs_len = None;
    while let Some((id, kind)) = field(cursor, &mut previous)? {
        match id {
            1 => {
                physical_type_id = Some(cursor.read_i32()?);
                physical_type = true;
            }
            2 | 6 | 7 | 8 | 9 => {
                let _ = cursor.read_i32()?;
            }
            3 => repetition = Some(cursor.read_i32()?),
            4 => {
                name_len = Some(read_utf8_payload_borrowed(cursor)?);
            }
            5 => {
                let value = cursor.read_i32()?;
                if value < 0 {
                    return Err(malformed());
                }
                children = Some(usize::try_from(value).map_err(|_| malformed())?);
            }
            10 => selected_crs_len = logical_type(cursor, facts, budget)?,
            _ => skip(cursor, kind)?,
        }
    }
    let name_len = name_len.ok_or_else(malformed)?;
    facts.schema_name_bytes = facts
        .schema_name_bytes
        .checked_add(u64::try_from(name_len).map_err(|_| allocation_limit())?)
        .ok_or_else(allocation_limit)?;
    // `schema_from_array_helper` treats an empty root as a group before it
    // examines the physical type, and positive child counts always select a
    // group. Only a non-root primitive contributes a physical leaf.
    let is_leaf = !is_root_node && children.unwrap_or(0) == 0 && physical_type;
    if is_leaf {
        facts.schema_leaves = facts
            .schema_leaves
            .checked_add(1)
            .ok_or_else(allocation_limit)?;
    }
    Ok(SchemaElementScalarFacts {
        name_len,
        physical_type: physical_type.then_some(physical_type_id).flatten(),
        physical_leaf: is_leaf,
        repetition,
        children,
        crs_len: selected_crs_len,
    })
}

/// Borrowed wire facts needed to replay native schema topology. The parser
/// retains no names or CRS payloads; lengths are from each field's final value.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct SchemaElementScalarFacts {
    pub(super) name_len: usize,
    pub(super) physical_type: Option<i32>,
    /// Whether native schema conversion selects this element as a primitive
    /// leaf. A raw physical field on the root or a group is ignored by native.
    pub(super) physical_leaf: bool,
    pub(super) repetition: Option<i32>,
    pub(super) children: Option<usize>,
    pub(super) crs_len: Option<usize>,
}

fn read_utf8_payload_borrowed(cursor: &mut CompactSlice<'_>) -> Result<usize, GfError> {
    Ok(cursor.read_utf8()?.len())
}

fn logical_type(
    cursor: &mut CompactSlice<'_>,
    facts: &mut FooterCountFacts,
    budget: u64,
) -> Result<Option<usize>, GfError> {
    let mut previous = 0;
    let mut variants = 0usize;
    let mut selected_crs_len = None;
    while let Some((id, kind)) = field(cursor, &mut previous)? {
        variants = variants.checked_add(1).ok_or_else(allocation_limit)?;
        match id {
            1..=4 | 6 | 11..=15 => {
                empty_struct(cursor)?;
                selected_crs_len = None;
            }
            5 => {
                decimal_type(cursor)?;
                selected_crs_len = None;
            }
            7 | 8 => {
                timestamp_type(cursor)?;
                selected_crs_len = None;
            }
            10 => {
                int_type(cursor)?;
                selected_crs_len = None;
            }
            16 => {
                variant_type(cursor)?;
                selected_crs_len = None;
            }
            17 | 18 => selected_crs_len = geometry_type(cursor, facts, budget, id == 18)?,
            _ => skip(cursor, kind)?,
        }
    }
    if variants > 1 {
        return Err(malformed());
    }
    Ok(selected_crs_len)
}

fn empty_struct(cursor: &mut CompactSlice<'_>) -> Result<(), GfError> {
    if cursor.read_byte()? == 0 {
        Ok(())
    } else {
        Err(malformed())
    }
}

fn decimal_type(cursor: &mut CompactSlice<'_>) -> Result<(), GfError> {
    let mut previous = 0;
    let mut required = 0u8;
    while let Some((id, kind)) = field(cursor, &mut previous)? {
        if id == 1 || id == 2 {
            let _ = cursor.read_i32()?;
            required |= if id == 1 { 1 } else { 2 };
        } else {
            skip(cursor, kind)?;
        }
    }
    if required != 3 {
        return Err(malformed());
    }
    Ok(())
}

fn time_unit(cursor: &mut CompactSlice<'_>) -> Result<(), GfError> {
    let mut previous = 0;
    let Some((id, kind)) = field(cursor, &mut previous)? else {
        return Err(malformed());
    };
    if (1..=3).contains(&id) {
        empty_struct(cursor)?;
    } else {
        skip(cursor, kind)?;
    }
    if field(cursor, &mut previous)?.is_some() {
        return Err(malformed());
    }
    Ok(())
}

fn timestamp_type(cursor: &mut CompactSlice<'_>) -> Result<(), GfError> {
    let mut previous = 0;
    let mut required = 0u8;
    while let Some((id, kind)) = field(cursor, &mut previous)? {
        match id {
            1 => {
                if !matches!(kind, Kind::BoolTrue | Kind::BoolFalse) {
                    return Err(malformed());
                }
                required |= 1;
            }
            2 => {
                time_unit(cursor)?;
                required |= 2;
            }
            _ => skip(cursor, kind)?,
        }
    }
    if required != 3 {
        return Err(malformed());
    }
    Ok(())
}

fn int_type(cursor: &mut CompactSlice<'_>) -> Result<(), GfError> {
    let mut previous = 0;
    let mut required = 0u8;
    while let Some((id, kind)) = field(cursor, &mut previous)? {
        match id {
            1 => {
                let _ = cursor.read_byte()?;
                required |= 1;
            }
            2 => {
                if !matches!(kind, Kind::BoolTrue | Kind::BoolFalse) {
                    return Err(malformed());
                }
                required |= 2;
            }
            _ => skip(cursor, kind)?,
        }
    }
    if required != 3 {
        return Err(malformed());
    }
    Ok(())
}

fn variant_type(cursor: &mut CompactSlice<'_>) -> Result<(), GfError> {
    let mut previous = 0;
    while let Some((id, kind)) = field(cursor, &mut previous)? {
        if id == 1 {
            let _ = cursor.read_byte()?;
        } else {
            skip(cursor, kind)?;
        }
    }
    Ok(())
}

fn geometry_type(
    cursor: &mut CompactSlice<'_>,
    facts: &mut FooterCountFacts,
    budget: u64,
    geography: bool,
) -> Result<Option<usize>, GfError> {
    let mut previous = 0;
    let mut crs_len = None;
    while let Some((id, kind)) = field(cursor, &mut previous)? {
        match id {
            1 => {
                let len = cursor.read_utf8()?.len();
                payload(facts, len, false, budget)?;
                crs_len = Some(len);
            }
            2 if geography => {
                let _ = cursor.read_i32()?;
            }
            _ => skip(cursor, kind)?,
        }
    }
    Ok(crs_len)
}

fn key_value(
    cursor: &mut CompactSlice<'_>,
    facts: &mut FooterCountFacts,
    budget: u64,
) -> Result<(), GfError> {
    let mut previous = 0;
    let mut key = false;
    while let Some((id, kind)) = field(cursor, &mut previous)? {
        match id {
            1 => {
                read_utf8_payload(cursor, facts, budget)?;
                key = true;
            }
            2 => read_utf8_payload(cursor, facts, budget)?,
            _ => skip(cursor, kind)?,
        }
    }
    if !key {
        return Err(malformed());
    }
    Ok(())
}

fn sorting_column(cursor: &mut CompactSlice<'_>) -> Result<(), GfError> {
    let mut previous = 0;
    let mut required = 0u8;
    while let Some((id, kind)) = field(cursor, &mut previous)? {
        match id {
            1 => {
                let _ = cursor.read_i32()?;
                required |= 1;
            }
            2 | 3 => {
                // Native bool fields carry their value in FieldIdentifier.
                if !matches!(kind, Kind::BoolTrue | Kind::BoolFalse) {
                    return Err(malformed());
                }
                required |= if id == 2 { 2 } else { 4 };
            }
            _ => skip(cursor, kind)?,
        }
    }
    if required != 7 {
        return Err(malformed());
    }
    Ok(())
}

fn statistics(
    cursor: &mut CompactSlice<'_>,
    facts: &mut FooterCountFacts,
    budget: u64,
    byte_array: bool,
) -> Result<(), GfError> {
    let mut previous = 0;
    while let Some((id, kind)) = field(cursor, &mut previous)? {
        match id {
            1 | 2 | 5 | 6 => {
                if byte_array {
                    read_binary_payload(cursor, facts, budget)?;
                } else {
                    let _ = cursor.read_bytes()?;
                }
            }
            3 | 4 => skip(cursor, Kind::I64)?,
            7 | 8 => {
                if !matches!(kind, Kind::BoolTrue | Kind::BoolFalse) {
                    return Err(malformed());
                }
            }
            _ => skip(cursor, kind)?,
        }
    }
    Ok(())
}

fn encoding_mask_list(cursor: &mut CompactSlice<'_>) -> Result<(), GfError> {
    let list = cursor.read_list()?;
    if list.count > cursor.remaining().len() {
        return Err(malformed());
    }
    // EncodingMask::read_thrift dispatches to Encoding::read_thrift for every
    // entry and therefore consumes an i32 irrespective of the advertised
    // element tag. The result is inline and owns no vector.
    for _ in 0..list.count {
        let _ = cursor.read_i32()?;
    }
    Ok(())
}

fn page_encoding_stats(cursor: &mut CompactSlice<'_>) -> Result<(), GfError> {
    let mut previous = 0;
    let mut required = 0u8;
    while let Some((id, kind)) = field(cursor, &mut previous)? {
        match id {
            1 | 2 | 3 => {
                let _ = cursor.read_i32()?;
                required |= 1 << (id - 1);
            }
            _ => skip(cursor, kind)?,
        }
    }
    if required != 7 {
        return Err(malformed());
    }
    Ok(())
}

fn encoding_stats_mask(cursor: &mut CompactSlice<'_>) -> Result<(), GfError> {
    let list = cursor.read_list()?;
    if list.count > cursor.remaining().len() {
        return Err(malformed());
    }
    for _ in 0..list.count {
        page_encoding_stats(cursor)?;
    }
    Ok(())
}

fn size_statistics(
    cursor: &mut CompactSlice<'_>,
    facts: &mut FooterCountFacts,
    budget: u64,
) -> Result<(), GfError> {
    let mut previous = 0;
    while let Some((id, kind)) = field(cursor, &mut previous)? {
        match id {
            1 => skip(cursor, Kind::I64)?,
            2 | 3 => {
                let count = admit_list(cursor, facts, size_of::<i64>(), budget)?;
                for _ in 0..count {
                    skip(cursor, Kind::I64)?;
                }
            }
            _ => skip(cursor, kind)?,
        }
    }
    Ok(())
}

fn geo_statistics(
    cursor: &mut CompactSlice<'_>,
    facts: &mut FooterCountFacts,
    budget: u64,
) -> Result<(), GfError> {
    facts.account_fixed(
        size_of::<parquet::geospatial::statistics::GeospatialStatistics>(),
        budget,
    )?;
    let mut previous = 0;
    while let Some((id, kind)) = field(cursor, &mut previous)? {
        match id {
            1 => {
                let mut bbox_previous = 0;
                let mut required = 0u8;
                while let Some((bbox_id, bbox_kind)) = field(cursor, &mut bbox_previous)? {
                    if (1..=8).contains(&bbox_id) {
                        skip(cursor, Kind::Double)?;
                        if bbox_id <= 4 {
                            required |= 1 << (bbox_id - 1);
                        }
                    } else {
                        skip(cursor, bbox_kind)?;
                    }
                }
                if required != 0b1111 {
                    return Err(malformed());
                }
            }
            2 => {
                let count = admit_list(cursor, facts, size_of::<i32>(), budget)?;
                for _ in 0..count {
                    let _ = cursor.read_i32()?;
                }
            }
            _ => skip(cursor, kind)?,
        }
    }
    Ok(())
}

fn column_chunk(
    cursor: &mut CompactSlice<'_>,
    facts: &mut FooterCountFacts,
    budget: u64,
    physical_type: i32,
) -> Result<(), GfError> {
    let mut previous = 0;
    let mut has_file_offset = false;
    while let Some((id, kind)) = field(cursor, &mut previous)? {
        match id {
            1 => read_utf8_payload(cursor, facts, budget)?,
            2 => {
                skip(cursor, Kind::I64)?;
                has_file_offset = true;
            }
            4 | 6 => skip(cursor, Kind::I64)?,
            5 | 7 => skip(cursor, Kind::I32)?,
            3 => {
                // ColumnMetaData, parsed with default options.
                let mut meta_previous = 0;
                while let Some((meta_id, meta_kind)) = field(cursor, &mut meta_previous)? {
                    match meta_id {
                        1 | 4 => skip(cursor, Kind::I32)?,
                        2 => encoding_mask_list(cursor)?,
                        5 | 6 | 7 | 9 | 10 | 11 | 14 => skip(cursor, Kind::I64)?,
                        15 => skip(cursor, Kind::I32)?,
                        12 => statistics(cursor, facts, budget, physical_type >= 6)?,
                        13 => encoding_stats_mask(cursor)?, // Default options retain only a mask.
                        16 => size_statistics(cursor, facts, budget)?,
                        17 => geo_statistics(cursor, facts, budget)?,
                        _ => skip(cursor, meta_kind)?, // includes path and column KV.
                    }
                }
            }
            _ => skip(cursor, kind)?,
        }
    }
    if !has_file_offset {
        return Err(malformed());
    }
    Ok(())
}

fn row_group(
    cursor: &mut CompactSlice<'_>,
    facts: &mut FooterCountFacts,
    budget: u64,
    schema_bytes: &[u8],
    schema_elements: usize,
    cancellation: Option<&CancellationToken>,
) -> Result<(), GfError> {
    let initial_columns = facts
        .schema_leaves
        .checked_mul(size_of::<ColumnChunkMetaData>())
        .ok_or_else(allocation_limit)?;
    facts.account_vector(initial_columns, budget)?;
    let mut previous = 0;
    let mut required = 0u8;
    let mut columns_seen = false;
    let mut columns_appended = 0usize;
    while let Some((id, kind)) = field(cursor, &mut previous)? {
        match id {
            1 => {
                let list = cursor.read_list()?;
                let count = list.count;
                if count != facts.schema_leaves {
                    return Err(malformed());
                }
                if count > cursor.remaining().len() {
                    return Err(malformed());
                }
                if !columns_seen {
                    columns_seen = true;
                    columns_appended = count;
                } else {
                    columns_appended = columns_appended
                        .checked_add(count)
                        .ok_or_else(allocation_limit)?;
                    let envelope = super::parquet_alloc::vector_envelope(
                        facts.schema_leaves,
                        columns_appended,
                        size_of::<ColumnChunkMetaData>(),
                    )?;
                    // Account the replacement peak conservatively as a
                    // coexistence request, after the initial backing above.
                    let peak =
                        usize::try_from(envelope.peak_bytes).map_err(|_| allocation_limit())?;
                    facts.account_vector(peak, budget)?;
                }
                let mut schema_cursor = CompactSlice::new(schema_bytes, cancellation);
                let mut elements_left = schema_elements;
                let mut schema_index = 0;
                let mut schema_facts = FooterCountFacts::default();
                for _ in 0..count {
                    let physical_type = loop {
                        if elements_left == 0 {
                            return Err(malformed());
                        }
                        let element = schema_element(
                            &mut schema_cursor,
                            &mut schema_facts,
                            u64::MAX,
                            schema_index == 0,
                        )?;
                        elements_left -= 1;
                        schema_index += 1;
                        if element.physical_leaf {
                            break element.physical_type.ok_or_else(malformed)?;
                        }
                    };
                    column_chunk(cursor, facts, budget, physical_type)?;
                }
                required |= 1;
            }
            2 | 3 => {
                skip(cursor, Kind::I64)?;
                required |= if id == 2 { 2 } else { 4 };
            }
            4 => {
                let count = admit_list(cursor, facts, size_of::<SortingColumn>(), budget)?;
                for _ in 0..count {
                    sorting_column(cursor)?;
                }
            }
            5 => {
                let _ = cursor.read_zig_zag()?;
            }
            // total_compressed_size is deliberately skipped by the native
            // decoder, so preserve its wire-kind dispatch.
            6 => skip(cursor, kind)?,
            7 => skip(cursor, Kind::I16)?,
            _ => skip(cursor, kind)?,
        }
    }
    if required != 7 || !columns_seen {
        return Err(malformed());
    }
    Ok(())
}

/// Count allocations requested by the default parquet 58.4 footer reader.
/// The CompactSlice borrows the owned footer and all facts stay in scalars.
pub(super) fn preflight(
    footer: &[u8],
    remaining_budget: u64,
    cancellation: Option<&CancellationToken>,
) -> Result<FooterCountFacts, GfError> {
    if cancellation.is_some_and(CancellationToken::is_cancelled) {
        return Err(cancelled());
    }
    let mut cursor = CompactSlice::new(footer, cancellation);
    let mut facts = FooterCountFacts::default();
    let mut previous = 0;
    let mut version = false;
    let mut num_rows = false;
    let mut schema_seen = false;
    let mut row_groups_seen = false;
    let mut column_orders_len = None;
    let mut schema_source = None;
    while let Some((id, kind)) = field(&mut cursor, &mut previous)? {
        match id {
            1 => {
                let _ = cursor.read_i32()?;
                version = true;
            }
            2 => {
                if schema_seen {
                    skip(&mut cursor, kind)?;
                    continue;
                }
                let count = admit_list(
                    &mut cursor,
                    &mut facts,
                    schema_element_envelope()?,
                    remaining_budget,
                )?;
                let schema_body_offset = cursor.position();
                facts.schema_elements = facts
                    .schema_elements
                    .checked_add(u64::try_from(count).map_err(|_| allocation_limit())?)
                    .ok_or_else(allocation_limit)?;
                let mut root_children = None;
                for index in 0..count {
                    let element =
                        schema_element(&mut cursor, &mut facts, remaining_budget, index == 0)?;
                    if index == 0 {
                        root_children = element.children;
                        facts.root_children = element
                            .children
                            .map(|value| u64::try_from(value).map_err(|_| allocation_limit()))
                            .transpose()?;
                    }
                }
                if count > 0 && root_children.unwrap_or(0) > count - 1 {
                    return Err(malformed());
                }
                schema_seen = true;
                facts.schema_offset = Some(schema_body_offset);
                schema_source = Some((schema_body_offset, count));
            }
            3 => {
                skip(&mut cursor, Kind::I64)?;
                num_rows = true;
            }
            4 => {
                if !schema_seen {
                    return Err(malformed());
                }
                let count = admit_list(
                    &mut cursor,
                    &mut facts,
                    size_of::<RowGroupMetaData>(),
                    remaining_budget,
                )?;
                if count > usize::try_from(i16::MAX).map_err(|_| allocation_limit())? + 1 {
                    return Err(malformed());
                }
                facts.row_groups = facts
                    .row_groups
                    .checked_add(u64::try_from(count).map_err(|_| allocation_limit())?)
                    .ok_or_else(allocation_limit)?;
                for _ in 0..count {
                    let (schema_offset, schema_elements) = schema_source.ok_or_else(malformed)?;
                    row_group(
                        &mut cursor,
                        &mut facts,
                        remaining_budget,
                        &footer[schema_offset..],
                        schema_elements,
                        cancellation,
                    )?;
                }
                row_groups_seen = true;
            }
            5 => {
                let count = admit_list(
                    &mut cursor,
                    &mut facts,
                    size_of::<KeyValue>(),
                    remaining_budget,
                )?;
                for _ in 0..count {
                    key_value(&mut cursor, &mut facts, remaining_budget)?;
                }
            }
            6 => read_utf8_payload(&mut cursor, &mut facts, remaining_budget)?,
            7 => {
                let count = admit_list(
                    &mut cursor,
                    &mut facts,
                    size_of::<parquet::basic::ColumnOrder>(),
                    remaining_budget,
                )?;
                for _ in 0..count {
                    // ColumnOrder is a union: native accepts one struct field
                    // and rejects an empty or multi-field union.
                    let mut union_previous = 0;
                    let Some((variant, union_kind)) = field(&mut cursor, &mut union_previous)?
                    else {
                        return Err(malformed());
                    };
                    if variant == 1 {
                        empty_struct(&mut cursor)?;
                    } else {
                        skip(&mut cursor, union_kind)?;
                    }
                    if field(&mut cursor, &mut union_previous)?.is_some() {
                        return Err(malformed());
                    }
                }
                column_orders_len = Some(count);
            }
            _ => skip(&mut cursor, kind)?,
        }
    }
    if !version || !num_rows || !schema_seen || !row_groups_seen {
        return Err(malformed());
    }
    if column_orders_len.is_some_and(|count| count != facts.schema_leaves) {
        return Err(malformed());
    }
    Ok(facts)
}

#[cfg(test)]
mod tests;
