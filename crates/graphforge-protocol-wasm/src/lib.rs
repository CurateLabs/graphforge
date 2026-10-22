//! WebAssembly adapter over the Rust-owned Hub discovery and publish validators.
//!
//! Every export converts its JavaScript arguments to JSON text, calls the
//! matching [`engine`] function, and converts the result back to a plain
//! object. A document argument is a JSON string (validated byte for byte,
//! including duplicate-member rejection) or a plain object, written to JSON
//! exactly: an integer above 2^53 must be a `BigInt` (a number would already be
//! rounded, so it is refused). Results are plain objects whose 64-bit integers
//! are `bigint`. A failure is thrown as a plain [`engine::ProtocolError`] object.
//!
//! The TypeScript declarations come from the `Tsify` derives on the Rust wire
//! types and on this crate's result types; nothing is described by hand beyond
//! the `string | T` shape of a document argument.

// `wasm-bindgen` cannot take an optional borrowed `JsValue`, so the optional
// `limits` argument is passed by value.
#![allow(clippy::needless_pass_by_value)]

pub mod engine;

use engine::{Outcome, ProtocolError};
use js_sys::{Array, BigInt, Object, Reflect, Uint8Array};
use serde::Serialize;
use serde_wasm_bindgen::Serializer;
use std::panic::{AssertUnwindSafe, catch_unwind};
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;

#[wasm_bindgen(typescript_custom_section)]
const JSON_INPUT: &str = r#"
/** A JSON document as text (validated byte for byte) or as a plain object. In an object, an integer above 2^53 must be a `bigint`. */
export type JsonInput<T> = string | T;
"#;

/// Every result is a plain object. 64-bit integers are `bigint` (exact; a
/// JavaScript number cannot hold them), maps are objects, and `None` is `null`.
fn to_js<T: Serialize>(value: &T) -> Result<JsValue, serde_wasm_bindgen::Error> {
    value.serialize(&Serializer::json_compatible().serialize_large_number_types_as_bigints(true))
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

/// Largest integer a JavaScript number holds exactly (2^53 − 1).
const MAX_SAFE_INTEGER: f64 = 9_007_199_254_740_991.0;

/// Deepest nesting an object argument may have (`serde_json` refuses more).
const MAX_DEPTH: usize = 128;

const UNSAFE_INTEGER: &str =
    "an integer outside ±2^53 in an object argument must be a BigInt, or sent in a JSON string";

fn plain_object(value: &JsValue) -> bool {
    let prototype = Object::get_prototype_of(value);
    prototype.is_null() || prototype == Object::get_prototype_of(&Object::new())
}

/// Write `value` as JSON, exactly: a `BigInt` keeps every digit, an integer a
/// JavaScript number cannot hold exactly is refused rather than rounded, and
/// anything that is not plain JSON data (functions, `Date`, `Map`, class
/// instances, `NaN`) is refused rather than silently reshaped.
fn write_json(value: &JsValue, out: &mut String, depth: usize) -> Outcome<()> {
    let refuse = |message: &str| Err(ProtocolError::argument(message));
    if depth > MAX_DEPTH {
        return refuse("object argument is nested too deeply");
    }
    if value.is_null() {
        out.push_str("null");
    } else if let Some(flag) = value.as_bool() {
        out.push_str(if flag { "true" } else { "false" });
    } else if let Some(text) = value.as_string() {
        out.push_str(&serde_json::to_string(&text).map_err(|_| ProtocolError::argument("string"))?);
    } else if value.is_bigint() {
        let digits = BigInt::from(value.clone())
            .to_string(10)
            .map_err(|_| ProtocolError::argument("bigint"))?;
        out.push_str(&String::from(digits));
    } else if let Some(number) = value.as_f64() {
        if !number.is_finite() {
            return refuse("object argument holds a number that is not finite");
        }
        if number.fract() == 0.0 {
            if number.abs() > MAX_SAFE_INTEGER {
                return refuse(UNSAFE_INTEGER);
            }
            // `-0` is `0` in JSON.
            #[allow(clippy::cast_possible_truncation)]
            out.push_str(&(number as i64).to_string());
        } else if let Some(float) = serde_json::Number::from_f64(number) {
            out.push_str(&float.to_string());
        }
    } else if let Some(array) = value.dyn_ref::<Array>() {
        out.push('[');
        for (index, item) in array.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            // `undefined` inside an array is `null`, as in `JSON.stringify`.
            if item.is_undefined() {
                out.push_str("null");
            } else {
                write_json(&item, out, depth + 1)?;
            }
        }
        out.push(']');
    } else if value.is_object() && !value.is_function() && plain_object(value) {
        out.push('{');
        let mut first = true;
        for key in Object::keys(value.unchecked_ref::<Object>()).iter() {
            let member =
                Reflect::get(value, &key).map_err(|_| ProtocolError::argument("member"))?;
            // `undefined` members are omitted, as in `JSON.stringify`.
            if member.is_undefined() {
                continue;
            }
            if !first {
                out.push(',');
            }
            first = false;
            let key = key.as_string().unwrap_or_default();
            out.push_str(&serde_json::to_string(&key).map_err(|_| ProtocolError::argument("key"))?);
            out.push(':');
            write_json(&member, out, depth + 1)?;
        }
        out.push('}');
    } else {
        return refuse("argument is not plain JSON data");
    }
    Ok(())
}

/// JSON text of a document or argument: a string as is, anything else written
/// by [`write_json`].
fn text(value: &JsValue) -> Outcome<Vec<u8>> {
    if let Some(text) = value.as_string() {
        return Ok(text.into_bytes());
    }
    if value.is_undefined() {
        return Err(ProtocolError::argument("argument is not a JSON value"));
    }
    let mut out = String::new();
    write_json(value, &mut out, 0)?;
    Ok(out.into_bytes())
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
