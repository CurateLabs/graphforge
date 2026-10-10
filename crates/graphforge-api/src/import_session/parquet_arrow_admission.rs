//! Preflight the Arrow schema and ParquetField allocations made by
//! `ArrowReaderMetadata::try_new` (parquet 58.4).
//!
//! The Parquet converter first copies footer key/value metadata, decodes an
//! optional base64 Arrow IPC schema hint, converts that hint with
//! `fb_to_schema`, then builds Arrow fields and the `ParquetField` level tree.
//! This module performs the hint conversion only for measurement, after
//! charging its decoded bytes, and bounds the subsequent Parquet conversion
//! from the already-validated schema topology. It never constructs an owned
//! Arrow schema or ParquetField tree.
//!
//! Bounds are requested payload bytes (including the Rust/Arrow container
//! transitions modeled by `parquet_alloc`), not allocator usable size or RSS.

use std::alloc::Layout;
use std::mem::size_of;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use arrow::datatypes::{DataType, Field};
use graphforge_core::GfError;
use parquet::file::metadata::ParquetMetaData;
use parquet::schema::types::Type;

use crate::CancellationToken;

use super::inventory_budget::InventoryBudget;
use super::ipc_schema_admission::IpcSchemaEnvelope;
use super::parquet_alloc;
use super::parquet_schema_envelope::SchemaTopologyFacts;
use super::{cancelled, limit, storage};

const ARROW_SCHEMA_KEY: &str = "ARROW:schema";
const BASE64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

// Mirrors parquet 58.4's private `ParquetFieldType` and `ParquetField` layout
// from arrow/schema/complex.rs. Keeping the pinned fields and enum variants
// here lets `size_of` compute the inline request instead of guessing a fixed
// number of bytes. The recursive edge is behind Vec, as in the upstream type.
#[allow(dead_code)] // Fields mirror an upstream private type for layout sizing.
enum ParquetFieldTypeRequest {
    Primitive {
        col_idx: usize,
        primitive_type: Arc<Type>,
    },
    Group {
        children: Vec<ParquetFieldRequest>,
    },
    Virtual(VirtualColumnRequest),
}

#[allow(dead_code)] // Variants mirror an upstream private type for layout sizing.
enum VirtualColumnRequest {
    RowNumber,
    RowGroupIndex,
}

#[allow(dead_code)] // Fields mirror an upstream private type for layout sizing.
struct ParquetFieldRequest {
    rep_level: i16,
    def_level: i16,
    nullable: bool,
    arrow_type: DataType,
    field_type: ParquetFieldTypeRequest,
}

/// Payload request envelope for `ArrowReaderMetadata::try_new`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct ArrowSchemaEnvelope {
    /// Requests retained in the resulting Arrow schema and ParquetField tree.
    pub(super) retained_request_bytes: u64,
    /// Maximum simultaneous request payload during hint decode/schema build.
    pub(super) peak_request_bytes: u64,
    /// Temporary decoded IPC schema-hint bytes at the peak.
    pub(super) hint_decoded: u64,
}

fn malformed() -> GfError {
    storage("Parquet Arrow schema metadata is malformed")
}

fn overflow() -> GfError {
    limit("Parquet Arrow schema allocation exceeds the admitted workspace")
}

fn add(total: &mut u64, amount: u64) -> Result<(), GfError> {
    *total = total.checked_add(amount).ok_or_else(overflow)?;
    Ok(())
}

fn product(count: u64, width: usize) -> Result<u64, GfError> {
    let count = usize::try_from(count).map_err(|_| overflow())?;
    let bytes = count.checked_mul(width).ok_or_else(overflow)?;
    if bytes > isize::MAX as usize {
        return Err(overflow());
    }
    u64::try_from(bytes).map_err(|_| overflow())
}

fn arc_bytes<T>() -> Result<u64, GfError> {
    let header = Layout::new::<AtomicUsize>()
        .extend(Layout::new::<AtomicUsize>())
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

fn estimate_inferred_requests(
    topology: SchemaTopologyFacts,
    field_occurrences: u64,
    hint_metadata_occurrences: u64,
    hint_metadata_bytes: u64,
) -> Result<u64, GfError> {
    // Each Parquet schema element maps to at most one Arrow field and one
    // ParquetField. Repeated elements add one list wrapper, so twice the node
    // count bounds both. A valid IPC hint may add independently nested field
    // values; include its measured occurrences as well.
    let native_fields = topology.nodes.checked_mul(2).ok_or_else(overflow)?;
    let fields = native_fields.max(field_occurrences);
    let parquet_fields = native_fields;
    let child_slots = topology
        .group_child_slots
        .checked_mul(4)
        .and_then(|n| n.checked_add(topology.nodes.checked_mul(2)?))
        .ok_or_else(overflow)?;

    let mut bytes = 0_u64;
    // Arrow Field Arc allocations and the ParquetField tree nodes. The local
    // layout mirror above is field-for-field identical to parquet 58.4's
    // private type, so this includes enum padding/alignment from this target.
    let per_parquet_field = size_of::<ParquetFieldRequest>();
    add(
        &mut bytes,
        product(
            fields,
            usize::try_from(arc_bytes::<Field>()?).map_err(|_| overflow())?,
        )?,
    )?;
    add(&mut bytes, product(parquet_fields, per_parquet_field)?)?;

    // Child slots coexist in Arrow Fields storage, ParquetField child vectors,
    // and builder/temporary vectors while the converter finishes a group.
    let child_slots = usize::try_from(child_slots).map_err(|_| overflow())?;
    add(
        &mut bytes,
        parquet_alloc::vector_envelope(0, child_slots, size_of::<std::sync::Arc<Field>>())?
            .peak_bytes,
    )?;
    add(
        &mut bytes,
        parquet_alloc::vector_envelope(0, child_slots, per_parquet_field)?.peak_bytes,
    )?;

    // Names are copied into Arrow Fields; repeated-list wrappers add a small
    // synthetic name. Field IDs and extension annotations add bounded
    // per-element metadata strings/maps.
    let copied_names = topology
        .name_bytes
        .checked_mul(2)
        .and_then(|n| n.checked_add(topology.nodes.checked_mul(64)?))
        .ok_or_else(overflow)?;
    add(&mut bytes, copied_names)?;
    add(
        &mut bytes,
        topology.nodes.checked_mul(256).ok_or_else(overflow)?,
    )?;
    // Geospatial extension conversion copies the CRS into Arrow field
    // metadata. The topology facts already counted source CRS bytes for
    // primitive and group annotations; charge two copies for the temporary
    // field metadata and merged Arrow schema metadata.
    let crs_bytes = topology
        .primitive_crs_clone_bytes
        .checked_add(topology.group_crs_clone_bytes)
        .and_then(|n| n.checked_mul(2))
        .ok_or_else(overflow)?;
    add(&mut bytes, crs_bytes)?;

    // Arrow schema metadata is cloned from file metadata and merged with the
    // hint schema's custom metadata. This includes strings and a conservative
    // HashMap/table request for both source occurrences and hint occurrences.
    let metadata_count = hint_metadata_occurrences;
    add(
        &mut bytes,
        hint_metadata_bytes.checked_mul(2).ok_or_else(overflow)?,
    )?;
    add(&mut bytes, product(metadata_count, 256)?)?;

    // Root Fields slice and the final Arrow Schema/ParquetField Arc headers.
    add(
        &mut bytes,
        product(fields, size_of::<std::sync::Arc<Field>>())?,
    )?;
    add(
        &mut bytes,
        product(
            fields,
            usize::try_from(arc_bytes::<std::sync::Arc<Field>>()?).map_err(|_| overflow())?,
        )?,
    )?;
    add(&mut bytes, arc_bytes::<arrow::datatypes::Schema>()?)?;
    // ArrowReaderMetadata retains the root ParquetField behind an Arc. Account
    // for its control block and one inline ParquetField using the same bound.
    add(&mut bytes, arc_bytes::<ParquetFieldRequest>()?)?;
    Ok(bytes)
}

fn decoded_base64_len(input: &str) -> Result<usize, GfError> {
    let bytes = input.as_bytes();
    if !bytes.len().is_multiple_of(4) {
        return Err(malformed());
    }
    let padding = match bytes.last() {
        Some(b'=') => match bytes.get(bytes.len().saturating_sub(2)) {
            Some(b'=') => 2,
            _ => 1,
        },
        _ => 0,
    };
    let groups = bytes.len() / 4;
    groups
        .checked_mul(3)
        .and_then(|n| n.checked_sub(padding))
        .ok_or_else(malformed)
}

fn decode_base64(
    input: &str,
    output: &mut Vec<u8>,
    cancellation: Option<&CancellationToken>,
) -> Result<(), GfError> {
    let bytes = input.as_bytes();
    let mut accumulator = 0_u32;
    let mut bits = 0_u8;
    for (index, byte) in bytes.iter().copied().enumerate() {
        if index % 4096 == 0 && cancellation.is_some_and(CancellationToken::is_cancelled) {
            return Err(cancelled());
        }
        if byte == b'=' {
            if bytes[index..].iter().any(|tail| *tail != b'=') || bytes.len() - index > 2 {
                return Err(malformed());
            }
            break;
        }
        let value = u32::try_from(
            BASE64
                .iter()
                .position(|candidate| *candidate == byte)
                .ok_or_else(malformed)?,
        )
        .map_err(|_| malformed())?;
        accumulator = (accumulator << 6) | value;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            output.push(u8::try_from(accumulator >> bits).map_err(|_| malformed())?);
            accumulator &= (1_u32 << bits).wrapping_sub(1);
        }
    }
    if output.len() != decoded_base64_len(input)? {
        return Err(malformed());
    }
    let padding = input
        .as_bytes()
        .iter()
        .rev()
        .take_while(|b| **b == b'=')
        .count();
    let data_end = input.len().checked_sub(padding).ok_or_else(malformed)?;
    let trailing_bits_mask = match padding {
        2 => 0b1111,
        1 => 0b11,
        _ => 0,
    };
    if trailing_bits_mask != 0 {
        let sextet = input.as_bytes()[data_end - 1];
        let value = u8::try_from(
            BASE64
                .iter()
                .position(|candidate| *candidate == sextet)
                .ok_or_else(malformed)?,
        )
        .map_err(|_| malformed())?;
        if value & trailing_bits_mask != 0 {
            return Err(malformed());
        }
    }
    Ok(())
}

fn hint_schema_bytes(
    encoded: &str,
    capacity: u64,
    cancellation: Option<&CancellationToken>,
) -> Result<(u64, IpcSchemaEnvelope), GfError> {
    if cancellation.is_some_and(CancellationToken::is_cancelled) {
        return Err(cancelled());
    }
    let decoded_len = decoded_base64_len(encoded)?;
    let decoded_bytes = u64::try_from(decoded_len).map_err(|_| overflow())?;
    let mut temporary_budget = InventoryBudget::new(capacity);
    temporary_budget.admit(decoded_bytes, "the decoded Parquet Arrow schema hint")?;
    let mut decoded = Vec::new();
    decoded
        .try_reserve_exact(decoded_len)
        .map_err(|_| overflow())?;
    decode_base64(encoded, &mut decoded, cancellation)?;

    let slice = if decoded.len() > 8 && decoded[..4] == [u8::MAX; 4] {
        &decoded[8..]
    } else {
        decoded.as_slice()
    };
    let message = arrow::ipc::root_as_message(slice).map_err(|_| malformed())?;
    let schema = message.header_as_schema().ok_or_else(malformed)?;
    let hint_capacity = capacity.saturating_sub(decoded_bytes);
    let envelope = super::ipc_schema_admission::preflight(schema, hint_capacity, cancellation)?;
    temporary_budget.admit(
        envelope.peak_request_bytes,
        "the converted Parquet Arrow schema hint",
    )?;
    Ok((decoded_bytes, envelope))
}

/// Compute a pre-allocation envelope for the Arrow schema and ParquetField
/// inference phase. Call after native footer parsing (so key/value metadata is
/// available), but before `ArrowReaderMetadata::try_new`. `capacity` is the
/// remaining shared inventory/workspace budget.
pub(super) fn preflight(
    metadata: &ParquetMetaData,
    topology: SchemaTopologyFacts,
    capacity: u64,
    cancellation: Option<&CancellationToken>,
) -> Result<ArrowSchemaEnvelope, GfError> {
    if cancellation.is_some_and(CancellationToken::is_cancelled) {
        return Err(cancelled());
    }

    let key_values = metadata.file_metadata().key_value_metadata();
    let mut key_value_bytes = 0_u64;
    let mut key_value_count = 0_u64;
    let mut hint = None;
    if let Some(values) = key_values {
        for entry in values {
            if cancellation.is_some_and(CancellationToken::is_cancelled) {
                return Err(cancelled());
            }
            let key = &entry.key;
            if let Some(value) = &entry.value {
                key_value_count = key_value_count.checked_add(1).ok_or_else(overflow)?;
                add(
                    &mut key_value_bytes,
                    u64::try_from(key.len()).map_err(|_| overflow())?,
                )?;
                add(
                    &mut key_value_bytes,
                    u64::try_from(value.len()).map_err(|_| overflow())?,
                )?;
                if key == ARROW_SCHEMA_KEY {
                    hint = Some(value);
                }
            }
        }
    }

    // parse_key_value_metadata clones every key/value into a HashMap before
    // removing the schema hint. Include all strings and a conservative table
    // request; these allocations coexist with the converter's output.
    let metadata_map_bytes = key_value_bytes
        .checked_add(key_value_count.checked_mul(256).ok_or_else(overflow)?)
        .ok_or_else(overflow)?;
    let (hint_decoded_bytes, hint_facts) = match hint {
        Some(encoded) => {
            let (decoded, facts) = hint_schema_bytes(
                encoded,
                capacity
                    .checked_sub(metadata_map_bytes)
                    .ok_or_else(overflow)?,
                cancellation,
            )?;
            (decoded, Some(facts))
        }
        None => (0, None),
    };

    let inferred = estimate_inferred_requests(
        topology,
        hint_facts.map_or(0, |facts| facts.field_occurrences),
        hint_facts.map_or(0, |facts| facts.metadata_occurrences),
        hint_facts.map_or(0, |facts| facts.metadata_copy_bytes),
    )?;
    let hint_peak = hint_facts.map_or(0, |facts| facts.peak_request_bytes);
    let hint_retained = hint_facts.map_or(0, |facts| facts.retained_request_bytes);

    let mut retained = metadata_map_bytes;
    add(&mut retained, inferred)?;
    // The converted hint stays live while the Parquet schema converter uses
    // its fields and metadata as hints, but is dropped before the final
    // ArrowReaderMetadata is returned.
    let convert_peak = retained.checked_add(hint_retained).ok_or_else(overflow)?;
    let decode_peak = metadata_map_bytes
        .checked_add(hint_decoded_bytes)
        .and_then(|n| n.checked_add(hint_peak))
        .ok_or_else(overflow)?;
    let peak = convert_peak.max(decode_peak);
    if peak > capacity {
        return Err(limit(
            "Parquet Arrow schema conversion exceeds the admitted workspace",
        ));
    }

    Ok(ArrowSchemaEnvelope {
        retained_request_bytes: retained,
        peak_request_bytes: peak,
        hint_decoded: hint_decoded_bytes,
    })
}

#[cfg(test)]
#[path = "parquet_arrow_admission/tests.rs"]
mod tests;
