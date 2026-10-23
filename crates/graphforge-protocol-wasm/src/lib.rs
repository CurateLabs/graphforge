//! WebAssembly adapter over the Rust-owned Hub discovery and publish validators.
//!
//! Every export takes only primitives: JSON text, item references, names,
//! media types or UUIDs (`&str`), and image bytes (`&[u8]`). Rust never reads a
//! JavaScript object, so user-supplied objects, proxies and getters cannot run
//! code under a Rust frame. The package's `index.js` wrapper turns a plain-object argument into
//! JSON text with `JSON.stringify` before calling an export, so an object and its
//! JSON text always reach Rust as the same string.
//!
//! Each export calls the matching [`engine`] function and returns a value built
//! only from Rust data: 64-bit integers are `bigint` (exact), `None` is `null`,
//! and maps are `Map`s that the wrapper turns into plain objects with
//! `Object.fromEntries`, which keeps a `__proto__` member as an own property. A
//! failure is thrown as a plain [`engine::ProtocolError`] object.
//!
//! The TypeScript declarations come from the `Tsify` derives on the Rust wire
//! types and on this crate's result types; nothing is described by hand beyond
//! the `string | T` shape of a document argument.

// `wasm-bindgen` cannot take an optional borrowed string, so the optional
// `limits` argument is passed by value.
#![allow(clippy::needless_pass_by_value)]

pub mod engine;

use engine::{Outcome, ProtocolError};
use serde::Serialize;
use serde_wasm_bindgen::Serializer;
use std::panic::{AssertUnwindSafe, catch_unwind};
use wasm_bindgen::prelude::*;

#[wasm_bindgen(typescript_custom_section)]
const JSON_INPUT: &str = r#"
/**
 * A JSON document as text (validated byte for byte) or as a plain object, which
 * is converted with `JSON.stringify`. In an object, an integer above 2^53 must
 * be a `bigint`.
 */
export type JsonInput<T> = string | T;
"#;

/// Build a JavaScript value from Rust data. 64-bit integers are `bigint`
/// (exact; a JavaScript number cannot hold them), `None` is `null`, and maps are
/// `Map`s, never objects: assigning a `__proto__` key to an object would set a
/// prototype instead of a member.
fn to_js<T: Serialize>(value: &T) -> Result<JsValue, serde_wasm_bindgen::Error> {
    value.serialize(
        &Serializer::new()
            .serialize_missing_as_null(true)
            .serialize_large_number_types_as_bigints(true),
    )
}

fn error_value(error: &ProtocolError) -> JsValue {
    to_js(error).unwrap_or_else(|_| JsValue::from_str(&error.message))
}

fn panic_error() -> ProtocolError {
    ProtocolError {
        code: "internal".into(),
        path: None,
        version: None,
        message: "the validator failed unexpectedly".into(),
    }
}

/// Run an engine function, mapping every outcome to a JavaScript value.
///
/// A WebAssembly build aborts on panic, so `catch_unwind` only intercepts a
/// panic where unwinding exists; the engine is written to be panic-free.
fn run<T: Serialize>(call: impl FnOnce() -> Outcome<T>) -> Result<JsValue, JsValue> {
    let outcome = catch_unwind(AssertUnwindSafe(call)).unwrap_or_else(|_| Err(panic_error()));
    let value = outcome.map_err(|error| error_value(&error))?;
    to_js(&value).map_err(|_| error_value(&panic_error()))
}

/// Like [`run`] for functions whose success carries no value.
fn run_unit(call: impl FnOnce() -> Outcome<()>) -> Result<(), JsValue> {
    catch_unwind(AssertUnwindSafe(call))
        .unwrap_or_else(|_| Err(panic_error()))
        .map_err(|error| error_value(&error))
}

fn resolve_limits(value: Option<&str>) -> Outcome<graphforge_discovery::DiscoveryLimits> {
    engine::limits(value.map(str::as_bytes))
}

// ------------------------------------------------------------- permanent IDs

/// Parse a bare permanent ID or canonical URI, folding ASCII case only.
#[wasm_bindgen(unchecked_return_type = "ParsedItemId")]
pub fn parse_item_id(reference: &str) -> Result<JsValue, JsValue> {
    run(|| engine::parse_item_id(reference))
}

/// Whether a name has canonical lowercase ID shape, including unknown prefixes.
#[must_use]
#[wasm_bindgen]
pub fn has_item_id_shape(name: &str) -> bool {
    engine::has_item_id_shape(name)
}

// ------------------------------------------------------------------ discovery

/// Parse and validate a permanent item descriptor without fetching module bytes.
#[wasm_bindgen(unchecked_return_type = "ValidatedItemDescriptor")]
pub fn validate_item_descriptor(
    #[wasm_bindgen(unchecked_param_type = "JsonInput<ItemDescriptor>")] descriptor: &str,
    #[wasm_bindgen(unchecked_optional_param_type = "DiscoveryLimitsOverride | null")]
    limits: Option<String>,
) -> Result<JsValue, JsValue> {
    run(|| {
        engine::validate_item_descriptor(descriptor.as_bytes(), resolve_limits(limits.as_deref())?)
    })
}

/// Parse and validate a discovery manifest; returns its canonical form and digest.
#[wasm_bindgen(unchecked_return_type = "ValidatedManifest")]
pub fn validate_manifest(
    #[wasm_bindgen(unchecked_param_type = "JsonInput<DiscoveryManifest>")] manifest: &str,
    #[wasm_bindgen(unchecked_param_type = "DiscoveryLimitsOverride")] limits: Option<String>,
) -> Result<JsValue, JsValue> {
    run(|| engine::validate_manifest(manifest.as_bytes(), resolve_limits(limits.as_deref())?))
}

/// Parse and validate a refs snapshot; returns its canonical form and revision.
#[wasm_bindgen(unchecked_return_type = "ValidatedRefs")]
pub fn validate_refs(
    #[wasm_bindgen(unchecked_param_type = "JsonInput<RefSet>")] refs: &str,
    #[wasm_bindgen(unchecked_param_type = "DiscoveryLimitsOverride")] limits: Option<String>,
) -> Result<JsValue, JsValue> {
    run(|| engine::validate_refs(refs.as_bytes(), resolve_limits(limits.as_deref())?))
}

/// Parse and validate a Project summary; returns its canonical form and digest.
#[wasm_bindgen(unchecked_return_type = "ValidatedSummary")]
pub fn validate_summary(
    #[wasm_bindgen(unchecked_param_type = "JsonInput<ProjectSummary>")] summary: &str,
    #[wasm_bindgen(unchecked_param_type = "DiscoveryLimitsOverride")] limits: Option<String>,
) -> Result<JsValue, JsValue> {
    run(|| engine::validate_summary(summary.as_bytes(), resolve_limits(limits.as_deref())?))
}

/// Parse and validate a research lineage; returns its canonical form and digest.
#[wasm_bindgen(unchecked_return_type = "ValidatedLineage")]
pub fn validate_lineage(
    #[wasm_bindgen(unchecked_param_type = "JsonInput<ResearchLineage>")] lineage: &str,
    #[wasm_bindgen(unchecked_param_type = "DiscoveryLimitsOverride")] limits: Option<String>,
) -> Result<JsValue, JsValue> {
    run(|| engine::validate_lineage(lineage.as_bytes(), resolve_limits(limits.as_deref())?))
}

/// Parse and validate a repository move marker.
#[wasm_bindgen(unchecked_return_type = "ValidatedRepositoryMoved")]
pub fn validate_repository_moved(
    #[wasm_bindgen(unchecked_param_type = "JsonInput<RepositoryMoved>")] moved: &str,
    #[wasm_bindgen(unchecked_param_type = "DiscoveryLimitsOverride")] limits: Option<String>,
) -> Result<JsValue, JsValue> {
    run(|| engine::validate_repository_moved(moved.as_bytes(), resolve_limits(limits.as_deref())?))
}

/// Check that a summary binds its manifest.
#[wasm_bindgen]
pub fn bind_summary(
    #[wasm_bindgen(unchecked_param_type = "JsonInput<DiscoveryManifest>")] manifest: &str,
    #[wasm_bindgen(unchecked_param_type = "JsonInput<ProjectSummary>")] summary: &str,
    #[wasm_bindgen(unchecked_param_type = "DiscoveryLimitsOverride")] limits: Option<String>,
) -> Result<(), JsValue> {
    run_unit(|| {
        engine::bind_summary(
            manifest.as_bytes(),
            summary.as_bytes(),
            resolve_limits(limits.as_deref())?,
        )
    })
}

/// Check that a lineage binds its manifest and refs snapshot.
#[wasm_bindgen]
pub fn bind_lineage(
    #[wasm_bindgen(unchecked_param_type = "JsonInput<DiscoveryManifest>")] manifest: &str,
    #[wasm_bindgen(unchecked_param_type = "JsonInput<RefSet>")] refs: &str,
    #[wasm_bindgen(unchecked_param_type = "JsonInput<ResearchLineage>")] lineage: &str,
    #[wasm_bindgen(unchecked_param_type = "DiscoveryLimitsOverride")] limits: Option<String>,
) -> Result<(), JsValue> {
    run_unit(|| {
        engine::bind_lineage(
            manifest.as_bytes(),
            refs.as_bytes(),
            lineage.as_bytes(),
            resolve_limits(limits.as_deref())?,
        )
    })
}

/// Check that a summary's people agree with its lineage (`null`: no lineage).
#[wasm_bindgen]
pub fn bind_summary_lineage(
    #[wasm_bindgen(unchecked_param_type = "JsonInput<ProjectSummary>")] summary: &str,
    #[wasm_bindgen(unchecked_param_type = "JsonInput<ResearchLineage> | null")] lineage: Option<
        String,
    >,
    #[wasm_bindgen(unchecked_param_type = "DiscoveryLimitsOverride")] limits: Option<String>,
) -> Result<(), JsValue> {
    run_unit(|| {
        engine::bind_summary_lineage(
            summary.as_bytes(),
            lineage.as_deref().map(str::as_bytes),
            resolve_limits(limits.as_deref())?,
        )
    })
}

/// Check that a manifest is bound to a refs snapshot.
#[wasm_bindgen]
pub fn bind_refs(
    #[wasm_bindgen(unchecked_param_type = "JsonInput<RefSet>")] refs: &str,
    #[wasm_bindgen(unchecked_param_type = "JsonInput<DiscoveryManifest>")] manifest: &str,
    #[wasm_bindgen(unchecked_param_type = "DiscoveryLimitsOverride")] limits: Option<String>,
) -> Result<(), JsValue> {
    run_unit(|| {
        engine::bind_refs(
            refs.as_bytes(),
            manifest.as_bytes(),
            resolve_limits(limits.as_deref())?,
        )
    })
}

/// Select the descriptor and transport object of one exact ontology module.
#[wasm_bindgen(unchecked_return_type = "OntologyModuleObject")]
pub fn ontology_module_object(
    #[wasm_bindgen(unchecked_param_type = "JsonInput<DiscoveryManifest>")] manifest: &str,
    #[wasm_bindgen(unchecked_param_type = "ExactIdentity")] identity: &str,
    #[wasm_bindgen(unchecked_param_type = "DiscoveryLimitsOverride")] limits: Option<String>,
) -> Result<JsValue, JsValue> {
    run(|| {
        engine::ontology_module_object(
            manifest.as_bytes(),
            identity.as_bytes(),
            resolve_limits(limits.as_deref())?,
        )
    })
}

/// Select the package object of one research Version UUID.
#[wasm_bindgen(unchecked_return_type = "ResearchVersionObject")]
pub fn research_version_object(
    #[wasm_bindgen(unchecked_param_type = "JsonInput<DiscoveryManifest>")] manifest: &str,
    #[wasm_bindgen(unchecked_param_type = "JsonInput<ResearchLineage>")] lineage: &str,
    version_uuid: &str,
    #[wasm_bindgen(unchecked_param_type = "DiscoveryLimitsOverride")] limits: Option<String>,
) -> Result<JsValue, JsValue> {
    run(|| {
        engine::research_version_object(
            manifest.as_bytes(),
            lineage.as_bytes(),
            version_uuid,
            resolve_limits(limits.as_deref())?,
        )
    })
}

/// Re-home a repository over its complete current ref closure.
#[wasm_bindgen(unchecked_return_type = "RehomeResult")]
pub fn rehome_repository(
    #[wasm_bindgen(unchecked_param_type = "JsonInput<RefSet>")] refs: &str,
    #[wasm_bindgen(unchecked_param_type = "RehomeSnapshot[]")] snapshots: &str,
    #[wasm_bindgen(unchecked_param_type = "RepositoryIdentity")] new_identity: &str,
    control_locations: &str,
    #[wasm_bindgen(unchecked_param_type = "DiscoveryLimitsOverride")] limits: Option<String>,
) -> Result<JsValue, JsValue> {
    run(|| {
        engine::rehome_repository(
            refs.as_bytes(),
            snapshots.as_bytes(),
            new_identity.as_bytes(),
            control_locations,
            resolve_limits(limits.as_deref())?,
        )
    })
}

// -------------------------------------------------------------------- preview

/// Validate image bytes against the closed PNG/SVG profile.
#[wasm_bindgen(unchecked_return_type = "ImageDimensions")]
pub fn validate_image(media_type: &str, bytes: &[u8]) -> Result<JsValue, JsValue> {
    run(|| engine::validate_image(media_type, bytes))
}

/// Revalidate untrusted bytes against every field of their descriptor.
#[wasm_bindgen]
pub fn validate_preview(
    #[wasm_bindgen(unchecked_param_type = "PreviewDescriptor")] descriptor: &str,
    bytes: &[u8],
) -> Result<(), JsValue> {
    run_unit(|| engine::validate_preview(descriptor.as_bytes(), bytes))
}

/// Validate a descriptor list without fetching image bytes.
#[wasm_bindgen]
pub fn validate_preview_descriptors(
    #[wasm_bindgen(unchecked_param_type = "PreviewDescriptor[]")] descriptors: &str,
) -> Result<(), JsValue> {
    run_unit(|| engine::validate_preview_descriptors(descriptors.as_bytes()))
}

/// The preview descriptors a manifest selects.
#[wasm_bindgen(unchecked_return_type = "PreviewDescriptor[]")]
pub fn manifest_preview_descriptors(
    #[wasm_bindgen(unchecked_param_type = "JsonInput<DiscoveryManifest>")] manifest: &str,
    #[wasm_bindgen(unchecked_param_type = "DiscoveryLimitsOverride")] limits: Option<String>,
) -> Result<JsValue, JsValue> {
    run(|| {
        engine::manifest_preview_descriptors(
            manifest.as_bytes(),
            resolve_limits(limits.as_deref())?,
        )
    })
}

// ----------------------------------------------------------------- hub publish

/// Parse and validate a publish capabilities document.
#[wasm_bindgen(unchecked_return_type = "ValidatedCapabilities")]
pub fn validate_capabilities(
    #[wasm_bindgen(unchecked_param_type = "JsonInput<PublishCapabilities>")] capabilities: &str,
) -> Result<JsValue, JsValue> {
    run(|| engine::validate_capabilities(capabilities.as_bytes()))
}

/// Parse and validate a publish session request.
#[wasm_bindgen(unchecked_return_type = "ValidatedSessionRequest")]
pub fn validate_session_request(
    #[wasm_bindgen(unchecked_param_type = "JsonInput<OpenSessionRequest>")] request: &str,
) -> Result<JsValue, JsValue> {
    run(|| engine::validate_session_request(request.as_bytes()))
}

/// Parse and validate a publish commit request.
#[wasm_bindgen(unchecked_return_type = "ValidatedCommitRequest")]
pub fn validate_commit_request(
    #[wasm_bindgen(unchecked_param_type = "JsonInput<CommitRequest>")] request: &str,
) -> Result<JsValue, JsValue> {
    run(|| engine::validate_commit_request(request.as_bytes()))
}

/// Parse and validate a publish receipt.
#[wasm_bindgen(unchecked_return_type = "ValidatedReceipt")]
pub fn validate_receipt(
    #[wasm_bindgen(unchecked_param_type = "JsonInput<PublishReceipt>")] receipt: &str,
) -> Result<JsValue, JsValue> {
    run(|| engine::validate_receipt(receipt.as_bytes()))
}

/// Parse and validate a publish operation status.
#[wasm_bindgen(unchecked_return_type = "ValidatedOperationStatus")]
pub fn validate_operation_status(
    #[wasm_bindgen(unchecked_param_type = "JsonInput<OperationStatus>")] status: &str,
) -> Result<JsValue, JsValue> {
    run(|| engine::validate_operation_status(status.as_bytes()))
}

/// Compute the `sha256:` request commitment of a publish intent.
#[wasm_bindgen]
pub fn request_commitment(
    #[wasm_bindgen(unchecked_param_type = "PublishIntentInput")] intent: &str,
) -> Result<String, JsValue> {
    catch_unwind(AssertUnwindSafe(|| {
        engine::request_commitment(intent.as_bytes())
    }))
    .unwrap_or_else(|_| Err(panic_error()))
    .map_err(|error| error_value(&error))
}

/// Check the manifest a commit would publish against its session: repository,
/// exact inventory at the Hub's template locations, and the resolved ref.
#[wasm_bindgen]
pub fn check_commit_manifest(
    #[wasm_bindgen(unchecked_param_type = "CommitManifestInput")] input: &str,
) -> Result<(), JsValue> {
    run_unit(|| engine::check_commit_manifest(input.as_bytes()))
}
