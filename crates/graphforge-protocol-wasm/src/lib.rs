//! WebAssembly adapter over the Rust-owned Hub discovery and publish validators.
//!
//! Every export converts its JavaScript arguments to JSON text, calls the
//! matching [`engine`] function, and converts the result back to a plain
//! object. A document argument is a JSON string (validated byte for byte,
//! including duplicate-member rejection) or a plain object (serialized with
//! `JSON.stringify` first). A failure is thrown as a plain
//! [`engine::ProtocolError`] object.
//!
//! The TypeScript declarations come from the `Tsify` derives on the Rust wire
//! types and on this crate's result types; nothing is described by hand beyond
//! the `string | T` shape of a document argument.

// `wasm-bindgen` cannot take an optional borrowed `JsValue`, so the optional
// `limits` argument is passed by value.
#![allow(clippy::needless_pass_by_value)]

pub mod engine;

use engine::{Outcome, ProtocolError};
use js_sys::{JSON, Uint8Array};
use serde::Serialize;
use std::panic::{AssertUnwindSafe, catch_unwind};
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;

#[wasm_bindgen(typescript_custom_section)]
const JSON_INPUT: &str = r#"
/** A JSON document as text (validated byte for byte) or as a plain object. */
export type JsonInput<T> = string | T;
"#;

fn error_value(error: &ProtocolError) -> JsValue {
    serde_json::to_string(error)
        .ok()
        .and_then(|text| JSON::parse(&text).ok())
        .unwrap_or_else(|| JsValue::from_str(&error.message))
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
    let text = serde_json::to_string(&value).map_err(|_| error_value(&panic_error()))?;
    JSON::parse(&text)
}

/// Like [`run`] for functions whose success carries no value.
fn run_unit(call: impl FnOnce() -> Outcome<()>) -> Result<(), JsValue> {
    catch_unwind(AssertUnwindSafe(call))
        .unwrap_or_else(|_| Err(panic_error()))
        .map_err(|error| error_value(&error))
}

/// JSON text of a document or argument: a string as is, anything else
/// serialized with `JSON.stringify`.
fn text(value: &JsValue) -> Outcome<Vec<u8>> {
    if let Some(text) = value.as_string() {
        return Ok(text.into_bytes());
    }
    JSON::stringify(value)
        .ok()
        .and_then(|text| text.as_string())
        .map(String::into_bytes)
        .ok_or_else(|| ProtocolError::argument("argument is not a JSON value"))
}

/// A string argument. Typed `&str` parameters would hand the JavaScript glue's
/// unchecked conversion a wrong-typed value, so strings arrive as `JsValue`.
fn string(value: &JsValue, name: &str) -> Outcome<String> {
    value
        .as_string()
        .ok_or_else(|| ProtocolError::argument(&format!("{name} argument must be a string")))
}

/// A `Uint8Array` argument, copied into Rust memory.
fn bytes_of(value: &JsValue) -> Outcome<Vec<u8>> {
    value
        .dyn_ref::<Uint8Array>()
        .map(Uint8Array::to_vec)
        .ok_or_else(|| ProtocolError::argument("bytes argument must be a Uint8Array"))
}

/// Optional argument: `undefined` and `null` are absent.
fn optional(value: &JsValue) -> Outcome<Option<Vec<u8>>> {
    if value.is_undefined() || value.is_null() {
        Ok(None)
    } else {
        text(value).map(Some)
    }
}

fn resolve_limits(value: Option<&JsValue>) -> Outcome<graphforge_discovery::DiscoveryLimits> {
    engine::limits(value.map(optional).transpose()?.flatten().as_deref())
}

// ------------------------------------------------------------------ discovery

/// Parse and validate a discovery manifest; returns its canonical form and digest.
#[wasm_bindgen(unchecked_return_type = "ValidatedManifest")]
pub fn validate_manifest(
    #[wasm_bindgen(unchecked_param_type = "JsonInput<DiscoveryManifest>")] manifest: &JsValue,
    #[wasm_bindgen(unchecked_param_type = "DiscoveryLimitsOverride")] limits: Option<JsValue>,
) -> Result<JsValue, JsValue> {
    run(|| engine::validate_manifest(&text(manifest)?, resolve_limits(limits.as_ref())?))
}

/// Parse and validate a refs snapshot; returns its canonical form and revision.
#[wasm_bindgen(unchecked_return_type = "ValidatedRefs")]
pub fn validate_refs(
    #[wasm_bindgen(unchecked_param_type = "JsonInput<RefSet>")] refs: &JsValue,
    #[wasm_bindgen(unchecked_param_type = "DiscoveryLimitsOverride")] limits: Option<JsValue>,
) -> Result<JsValue, JsValue> {
    run(|| engine::validate_refs(&text(refs)?, resolve_limits(limits.as_ref())?))
}

/// Parse and validate a Project summary; returns its canonical form and digest.
#[wasm_bindgen(unchecked_return_type = "ValidatedSummary")]
pub fn validate_summary(
    #[wasm_bindgen(unchecked_param_type = "JsonInput<ProjectSummary>")] summary: &JsValue,
    #[wasm_bindgen(unchecked_param_type = "DiscoveryLimitsOverride")] limits: Option<JsValue>,
) -> Result<JsValue, JsValue> {
    run(|| engine::validate_summary(&text(summary)?, resolve_limits(limits.as_ref())?))
}

/// Parse and validate a research lineage; returns its canonical form and digest.
#[wasm_bindgen(unchecked_return_type = "ValidatedLineage")]
pub fn validate_lineage(
    #[wasm_bindgen(unchecked_param_type = "JsonInput<ResearchLineage>")] lineage: &JsValue,
    #[wasm_bindgen(unchecked_param_type = "DiscoveryLimitsOverride")] limits: Option<JsValue>,
) -> Result<JsValue, JsValue> {
    run(|| engine::validate_lineage(&text(lineage)?, resolve_limits(limits.as_ref())?))
}

/// Parse and validate a repository move marker.
#[wasm_bindgen(unchecked_return_type = "ValidatedRepositoryMoved")]
pub fn validate_repository_moved(
    #[wasm_bindgen(unchecked_param_type = "JsonInput<RepositoryMoved>")] moved: &JsValue,
    #[wasm_bindgen(unchecked_param_type = "DiscoveryLimitsOverride")] limits: Option<JsValue>,
) -> Result<JsValue, JsValue> {
    run(|| engine::validate_repository_moved(&text(moved)?, resolve_limits(limits.as_ref())?))
}

/// Check that a summary binds its manifest.
#[wasm_bindgen]
pub fn bind_summary(
    #[wasm_bindgen(unchecked_param_type = "JsonInput<DiscoveryManifest>")] manifest: &JsValue,
    #[wasm_bindgen(unchecked_param_type = "JsonInput<ProjectSummary>")] summary: &JsValue,
    #[wasm_bindgen(unchecked_param_type = "DiscoveryLimitsOverride")] limits: Option<JsValue>,
) -> Result<(), JsValue> {
    run_unit(|| {
        engine::bind_summary(
            &text(manifest)?,
            &text(summary)?,
            resolve_limits(limits.as_ref())?,
        )
    })
}

/// Check that a lineage binds its manifest and refs snapshot.
#[wasm_bindgen]
pub fn bind_lineage(
    #[wasm_bindgen(unchecked_param_type = "JsonInput<DiscoveryManifest>")] manifest: &JsValue,
    #[wasm_bindgen(unchecked_param_type = "JsonInput<RefSet>")] refs: &JsValue,
    #[wasm_bindgen(unchecked_param_type = "JsonInput<ResearchLineage>")] lineage: &JsValue,
    #[wasm_bindgen(unchecked_param_type = "DiscoveryLimitsOverride")] limits: Option<JsValue>,
) -> Result<(), JsValue> {
    run_unit(|| {
        engine::bind_lineage(
            &text(manifest)?,
            &text(refs)?,
            &text(lineage)?,
            resolve_limits(limits.as_ref())?,
        )
    })
}

/// Check that a summary's people agree with its lineage (`null`: no lineage).
#[wasm_bindgen]
pub fn bind_summary_lineage(
    #[wasm_bindgen(unchecked_param_type = "JsonInput<ProjectSummary>")] summary: &JsValue,
    #[wasm_bindgen(unchecked_param_type = "JsonInput<ResearchLineage> | null")] lineage: &JsValue,
    #[wasm_bindgen(unchecked_param_type = "DiscoveryLimitsOverride")] limits: Option<JsValue>,
) -> Result<(), JsValue> {
    run_unit(|| {
        engine::bind_summary_lineage(
            &text(summary)?,
            optional(lineage)?.as_deref(),
            resolve_limits(limits.as_ref())?,
        )
    })
}

/// Check that a manifest is bound to a refs snapshot.
#[wasm_bindgen]
pub fn bind_refs(
    #[wasm_bindgen(unchecked_param_type = "JsonInput<RefSet>")] refs: &JsValue,
    #[wasm_bindgen(unchecked_param_type = "JsonInput<DiscoveryManifest>")] manifest: &JsValue,
    #[wasm_bindgen(unchecked_param_type = "DiscoveryLimitsOverride")] limits: Option<JsValue>,
) -> Result<(), JsValue> {
    run_unit(|| {
        engine::bind_refs(
            &text(refs)?,
            &text(manifest)?,
            resolve_limits(limits.as_ref())?,
        )
    })
}

/// Select the descriptor and transport object of one exact ontology module.
#[wasm_bindgen(unchecked_return_type = "OntologyModuleObject")]
pub fn ontology_module_object(
    #[wasm_bindgen(unchecked_param_type = "JsonInput<DiscoveryManifest>")] manifest: &JsValue,
    #[wasm_bindgen(unchecked_param_type = "ExactIdentity")] identity: &JsValue,
    #[wasm_bindgen(unchecked_param_type = "DiscoveryLimitsOverride")] limits: Option<JsValue>,
) -> Result<JsValue, JsValue> {
    run(|| {
        engine::ontology_module_object(
            &text(manifest)?,
            &text(identity)?,
            resolve_limits(limits.as_ref())?,
        )
    })
}

/// Select the package object of one research Version UUID.
#[wasm_bindgen(unchecked_return_type = "ResearchVersionObject")]
pub fn research_version_object(
    #[wasm_bindgen(unchecked_param_type = "JsonInput<DiscoveryManifest>")] manifest: &JsValue,
    #[wasm_bindgen(unchecked_param_type = "JsonInput<ResearchLineage>")] lineage: &JsValue,
    #[wasm_bindgen(unchecked_param_type = "string")] version_uuid: &JsValue,
    #[wasm_bindgen(unchecked_param_type = "DiscoveryLimitsOverride")] limits: Option<JsValue>,
) -> Result<JsValue, JsValue> {
    run(|| {
        engine::research_version_object(
            &text(manifest)?,
            &text(lineage)?,
            &string(version_uuid, "version_uuid")?,
            resolve_limits(limits.as_ref())?,
        )
    })
}

/// Re-home a repository over its complete current ref closure.
#[wasm_bindgen(unchecked_return_type = "RehomeResult")]
pub fn rehome_repository(
    #[wasm_bindgen(unchecked_param_type = "JsonInput<RefSet>")] refs: &JsValue,
    #[wasm_bindgen(unchecked_param_type = "RehomeSnapshot[]")] snapshots: &JsValue,
    #[wasm_bindgen(unchecked_param_type = "RepositoryIdentity")] new_identity: &JsValue,
    #[wasm_bindgen(unchecked_param_type = "string")] control_locations: &JsValue,
    #[wasm_bindgen(unchecked_param_type = "DiscoveryLimitsOverride")] limits: Option<JsValue>,
) -> Result<JsValue, JsValue> {
    run(|| {
        engine::rehome_repository(
            &text(refs)?,
            &text(snapshots)?,
            &text(new_identity)?,
            &string(control_locations, "control_locations")?,
            resolve_limits(limits.as_ref())?,
        )
    })
}

// -------------------------------------------------------------------- preview

/// Validate image bytes against the closed PNG/SVG profile.
#[wasm_bindgen(unchecked_return_type = "ImageDimensions")]
pub fn validate_image(
    #[wasm_bindgen(unchecked_param_type = "string")] media_type: &JsValue,
    #[wasm_bindgen(unchecked_param_type = "Uint8Array")] bytes: &JsValue,
) -> Result<JsValue, JsValue> {
    run(|| engine::validate_image(&string(media_type, "media_type")?, &bytes_of(bytes)?))
}

/// Revalidate untrusted bytes against every field of their descriptor.
#[wasm_bindgen]
pub fn validate_preview(
    #[wasm_bindgen(unchecked_param_type = "PreviewDescriptor")] descriptor: &JsValue,
    #[wasm_bindgen(unchecked_param_type = "Uint8Array")] bytes: &JsValue,
) -> Result<(), JsValue> {
    run_unit(|| engine::validate_preview(&text(descriptor)?, &bytes_of(bytes)?))
}

/// Validate a descriptor list without fetching image bytes.
#[wasm_bindgen]
pub fn validate_preview_descriptors(
    #[wasm_bindgen(unchecked_param_type = "PreviewDescriptor[]")] descriptors: &JsValue,
) -> Result<(), JsValue> {
    run_unit(|| engine::validate_preview_descriptors(&text(descriptors)?))
}

/// The preview descriptors a manifest selects.
#[wasm_bindgen(unchecked_return_type = "PreviewDescriptor[]")]
pub fn manifest_preview_descriptors(
    #[wasm_bindgen(unchecked_param_type = "JsonInput<DiscoveryManifest>")] manifest: &JsValue,
    #[wasm_bindgen(unchecked_param_type = "DiscoveryLimitsOverride")] limits: Option<JsValue>,
) -> Result<JsValue, JsValue> {
    run(|| engine::manifest_preview_descriptors(&text(manifest)?, resolve_limits(limits.as_ref())?))
}

// ----------------------------------------------------------------- hub publish

/// Parse and validate a publish capabilities document.
#[wasm_bindgen(unchecked_return_type = "ValidatedCapabilities")]
pub fn validate_capabilities(
    #[wasm_bindgen(unchecked_param_type = "JsonInput<PublishCapabilities>")] capabilities: &JsValue,
) -> Result<JsValue, JsValue> {
    run(|| engine::validate_capabilities(&text(capabilities)?))
}

/// Parse and validate a publish session request.
#[wasm_bindgen(unchecked_return_type = "ValidatedSessionRequest")]
pub fn validate_session_request(
    #[wasm_bindgen(unchecked_param_type = "JsonInput<OpenSessionRequest>")] request: &JsValue,
) -> Result<JsValue, JsValue> {
    run(|| engine::validate_session_request(&text(request)?))
}

/// Parse and validate a publish commit request.
#[wasm_bindgen(unchecked_return_type = "ValidatedCommitRequest")]
pub fn validate_commit_request(
    #[wasm_bindgen(unchecked_param_type = "JsonInput<CommitRequest>")] request: &JsValue,
) -> Result<JsValue, JsValue> {
    run(|| engine::validate_commit_request(&text(request)?))
}

/// Parse and validate a publish receipt.
#[wasm_bindgen(unchecked_return_type = "ValidatedReceipt")]
pub fn validate_receipt(
    #[wasm_bindgen(unchecked_param_type = "JsonInput<PublishReceipt>")] receipt: &JsValue,
) -> Result<JsValue, JsValue> {
    run(|| engine::validate_receipt(&text(receipt)?))
}

/// Parse and validate a publish operation status.
#[wasm_bindgen(unchecked_return_type = "ValidatedOperationStatus")]
pub fn validate_operation_status(
    #[wasm_bindgen(unchecked_param_type = "JsonInput<OperationStatus>")] status: &JsValue,
) -> Result<JsValue, JsValue> {
    run(|| engine::validate_operation_status(&text(status)?))
}

/// Compute the `sha256:` request commitment of a publish intent.
#[wasm_bindgen]
pub fn request_commitment(
    #[wasm_bindgen(unchecked_param_type = "PublishIntentInput")] intent: &JsValue,
) -> Result<String, JsValue> {
    let mut commitment = String::new();
    run_unit(|| {
        commitment = engine::request_commitment(&text(intent)?)?;
        Ok(())
    })?;
    Ok(commitment)
}

/// Check the manifest a commit would publish against its session: repository,
/// exact inventory at the Hub's template locations, and the resolved ref.
#[wasm_bindgen]
pub fn check_commit_manifest(
    #[wasm_bindgen(unchecked_param_type = "CommitManifestInput")] input: &JsValue,
) -> Result<(), JsValue> {
    run_unit(|| engine::check_commit_manifest(&text(input)?))
}
