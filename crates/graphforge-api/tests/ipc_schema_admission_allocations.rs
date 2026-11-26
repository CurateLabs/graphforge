//! Measures the actual pinned Arrow 60.2 native conversion against the
//! source-derived IPC schema admission envelope. This is a separate test
//! binary because the production crate forbids unsafe code.

use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::convert::{IpcSchemaEncoder, try_fb_to_schema};
use arrow::ipc::root_as_schema;
use graphforge_api::CancellationToken;
use graphforge_core::GfError;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

#[derive(Clone, Copy, Default)]
struct Track {
    active: bool,
    live: usize,
    peak: usize,
    requests: usize,
}

thread_local! {
    static TRACK: Cell<Track> = const { Cell::new(Track { active: false, live: 0, peak: 0, requests: 0 }) };
}

struct TrackingSystem;

// SAFETY: all allocation operations are delegated to `System`; the thread-local
// counter only observes requested layouts and remains inactive outside the
// synchronous conversion window.
unsafe impl GlobalAlloc for TrackingSystem {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            TRACK.with(|state| {
                let mut track = state.get();
                if track.active {
                    track.live = track.live.saturating_add(layout.size());
                    track.peak = track.peak.max(track.live);
                    track.requests += 1;
                    state.set(track);
                }
            });
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        TRACK.with(|state| {
            let mut track = state.get();
            if track.active {
                track.live = track.live.saturating_sub(layout.size());
                state.set(track);
            }
        });
        unsafe { System.dealloc(pointer, layout) };
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        TRACK.with(|state| {
            let mut track = state.get();
            if track.active {
                track.peak = track.peak.max(track.live.saturating_add(new_size));
                track.requests += 1;
                state.set(track);
            }
        });
        let moved = unsafe { System.realloc(pointer, layout, new_size) };
        if !moved.is_null() {
            TRACK.with(|state| {
                let mut track = state.get();
                if track.active {
                    track.live = track
                        .live
                        .saturating_sub(layout.size())
                        .saturating_add(new_size);
                    track.peak = track.peak.max(track.live);
                    state.set(track);
                }
            });
        }
        moved
    }
}

#[global_allocator]
static ALLOCATOR: TrackingSystem = TrackingSystem;

fn limit(message: impl Into<String>) -> GfError {
    GfError::Project {
        code: graphforge_core::ProjectErrorCode::ResourceLimit,
        message: message.into(),
    }
}

fn storage(error: impl std::fmt::Display) -> GfError {
    GfError::Storage(error.to_string())
}

fn cancelled() -> GfError {
    GfError::Api {
        code: graphforge_core::ApiErrorCode::Cancelled,
        message: "cancelled".into(),
    }
}

#[path = "../src/import_session/inventory_budget.rs"]
mod inventory_budget;
#[path = "../src/import_session/ipc_schema_admission.rs"]
mod ipc_schema_admission;
#[path = "../src/import_session/parquet_alloc.rs"]
mod parquet_alloc;

fn encoded(schema: &Schema) -> Vec<u8> {
    IpcSchemaEncoder::new()
        .schema_to_fb(schema)
        .finished_data()
        .to_vec()
}

fn measure(schema: arrow::ipc::Schema<'_>) -> (arrow::datatypes::Schema, Track) {
    TRACK.with(|state| {
        state.set(Track {
            active: true,
            ..Track::default()
        })
    });
    let converted = try_fb_to_schema(schema).expect("a valid IPC schema");
    let result = TRACK.with(|state| {
        let track = state.get();
        state.set(Track::default());
        track
    });
    (converted, result)
}

#[test]
fn native_conversion_peak_is_below_preflight_envelope() {
    let nested = Field::new(
        "outer",
        DataType::Struct(
            (0..23)
                .map(|index| Field::new(format!("child-{index}"), DataType::Utf8, true))
                .collect::<Vec<_>>()
                .into(),
        ),
        true,
    );
    let schema = Schema::new(vec![nested; 7]);
    let bytes = encoded(&schema);
    let borrowed = root_as_schema(&bytes).unwrap();
    let envelope = ipc_schema_admission::preflight(borrowed, u64::MAX, None).unwrap();
    let (converted, measured) = measure(root_as_schema(&bytes).unwrap());

    assert_eq!(converted, schema);
    assert!(measured.requests > 0);
    assert!(
        measured.peak as u64 <= envelope.peak_request_bytes,
        "native request peak {} exceeded admitted envelope {}",
        measured.peak,
        envelope.peak_request_bytes
    );
}

/// Arrow 60 converts every metadata `HashMap` into a `BTreeMap`-backed
/// `Metadata`: the entries are collected into a `Vec`, stable-sorted (with
/// heap scratch past 85 entries), bulk-built into tree nodes, and wrapped in
/// an `Arc`. Large schema-level and per-field maps exercise every stage.
#[test]
fn metadata_tree_conversion_peak_and_retention_are_below_preflight_envelope() {
    let entries = |count: usize, prefix: &str| {
        (0..count)
            .map(|index| (format!("{prefix}-key-{index:04}"), format!("value-{index}")))
            .collect::<std::collections::HashMap<_, _>>()
    };
    let fields = (0..7)
        .map(|index| {
            Field::new(format!("field-{index}"), DataType::Utf8, true)
                .with_metadata(entries(40 + index * 17, &format!("field-{index}")))
        })
        .collect::<Vec<_>>();
    let schema = Schema::new_with_metadata(fields, entries(300, "schema"));
    let bytes = encoded(&schema);
    let envelope =
        ipc_schema_admission::preflight(root_as_schema(&bytes).unwrap(), u64::MAX, None).unwrap();
    assert_eq!(
        envelope.metadata_occurrences,
        300 + (0..7).map(|index| 40 + index * 17).sum::<u64>()
    );
    let (converted, measured) = measure(root_as_schema(&bytes).unwrap());

    assert_eq!(converted, schema);
    assert!(
        measured.peak as u64 <= envelope.peak_request_bytes,
        "native request peak {} exceeded admitted envelope {}",
        measured.peak,
        envelope.peak_request_bytes
    );
    assert!(
        measured.live as u64 <= envelope.retained_request_bytes,
        "native retained requests {} exceeded the retained envelope {}",
        measured.live,
        envelope.retained_request_bytes
    );
}

#[test]
fn aliases_are_counted_per_occurrence_and_low_budget_refuses_them() {
    let schema = Schema::new(
        (0..256)
            .map(|_| Field::new("repeated", DataType::Utf8, true))
            .collect::<Vec<_>>(),
    );
    let mut bytes = encoded(&schema);
    let root = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
    let vtable = root - u32::from_le_bytes(bytes[root..root + 4].try_into().unwrap()) as usize;
    let fields_entry = vtable + 6; // Schema vtable slot 1 (fields vector).
    let fields_offset =
        u16::from_le_bytes(bytes[fields_entry..fields_entry + 2].try_into().unwrap()) as usize;
    assert_ne!(fields_offset, 0);
    let vector_field = root + fields_offset;
    let vector = vector_field
        + u32::from_le_bytes(bytes[vector_field..vector_field + 4].try_into().unwrap()) as usize;
    let count = u32::from_le_bytes(bytes[vector..vector + 4].try_into().unwrap()) as usize;
    assert_eq!(count, 256);
    let target = (0..count)
        .map(|index| {
            let slot = vector + 4 + index * 4;
            slot + u32::from_le_bytes(bytes[slot..slot + 4].try_into().unwrap()) as usize
        })
        .max()
        .unwrap();
    for index in 1..count {
        let slot = vector + 4 + index * 4;
        let relative = target.checked_sub(slot).unwrap();
        bytes[slot..slot + 4].copy_from_slice(&u32::try_from(relative).unwrap().to_le_bytes());
    }

    let aliased = root_as_schema(&bytes).expect("aliased but verifier-valid schema");
    let small = ipc_schema_admission::preflight(aliased, 1, None);
    assert!(
        small.is_err(),
        "repeated table aliases must be budgeted per occurrence"
    );
    let envelope =
        ipc_schema_admission::preflight(root_as_schema(&bytes).unwrap(), u64::MAX, None).unwrap();
    assert_eq!(envelope.field_occurrences, 256);
    assert_eq!(envelope.field_name_copy_bytes, 8 * 256);

    let (converted, measured) = measure(root_as_schema(&bytes).unwrap());
    assert_eq!(converted.fields().len(), 256);
    assert!(
        measured.peak as u64 <= envelope.peak_request_bytes,
        "aliased native request peak {} exceeded occurrence-counted envelope {}",
        measured.peak,
        envelope.peak_request_bytes
    );
}

#[test]
fn cancellation_token_type_remains_linked_to_the_public_facade() {
    let token = CancellationToken::new();
    assert!(!token.is_cancelled());
    token.cancel();
    assert!(token.is_cancelled());
}
