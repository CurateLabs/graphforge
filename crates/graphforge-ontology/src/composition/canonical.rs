//! JCS-style canonical JSON and module digests.

use serde_json::Value;
use sha2::{Digest, Sha256};

use super::identity::{BRIDGE_DIGEST_DOMAIN, MODULE_DIGEST_DOMAIN};

/// Serialize a JSON [`Value`] with object keys sorted (RFC 8785 subset used by GraphForge).
pub fn canonical_json(value: &Value) -> Result<Vec<u8>, String> {
    fn write(value: &Value, output: &mut Vec<u8>) -> Result<(), String> {
        match value {
            Value::Null => output.extend_from_slice(b"null"),
            Value::Bool(v) => output.extend_from_slice(if *v { b"true" } else { b"false" }),
            Value::Number(v) => output.extend(v.to_string().bytes()),
            Value::String(v) => {
                let encoded = serde_json::to_vec(v).map_err(|e| e.to_string())?;
                output.extend(encoded);
            }
            Value::Array(values) => {
                output.push(b'[');
                for (index, value) in values.iter().enumerate() {
                    if index != 0 {
                        output.push(b',');
                    }
                    write(value, output)?;
                }
                output.push(b']');
            }
            Value::Object(map) => {
                output.push(b'{');
                let mut keys: Vec<&String> = map.keys().collect();
                keys.sort();
                for (index, key) in keys.into_iter().enumerate() {
                    if index != 0 {
                        output.push(b',');
                    }
                    let encoded_key = serde_json::to_vec(key).map_err(|e| e.to_string())?;
                    output.extend(encoded_key);
                    output.push(b':');
                    write(&map[key], output)?;
                }
                output.push(b'}');
            }
        }
        Ok(())
    }
    let mut output = Vec::new();
    write(value, &mut output)?;
    Ok(output)
}

/// Lowercase hex SHA-256 of `domain || canonical_json(value)`.
pub fn domain_digest(domain: &[u8], value: &Value) -> Result<String, String> {
    let canonical = canonical_json(value)?;
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(&canonical);
    Ok(hex_lower(hasher.finalize().as_slice()))
}

/// Digest a serde-serializable module document under the module domain.
pub fn module_document_digest(doc: &impl serde::Serialize) -> Result<String, String> {
    let value = serde_json::to_value(doc).map_err(|e| e.to_string())?;
    domain_digest(MODULE_DIGEST_DOMAIN, &value)
}

/// Digest a serde-serializable bridge-set document under the bridge domain.
pub fn bridge_document_digest(doc: &impl serde::Serialize) -> Result<String, String> {
    let value = serde_json::to_value(doc).map_err(|e| e.to_string())?;
    domain_digest(BRIDGE_DIGEST_DOMAIN, &value)
}

pub(crate) fn hex_lower(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut out, byte| {
            write!(&mut out, "{byte:02x}").expect("writing to String cannot fail");
            out
        })
}
