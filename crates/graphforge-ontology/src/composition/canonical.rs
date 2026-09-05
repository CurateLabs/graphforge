//! Frozen ontology `/1` sorted-JSON grammar and domain-separated digests.
//!
//! This is intentionally distinct from `graphforge_core::canonical`'s binary
//! `graphforge-canonical/1` envelope: replacing either grammar changes persisted
//! identities. See `docs/book/architecture/canonical-fingerprints-v1.md`,
//! "Ontology document compatibility boundary", for the byte rules and rationale.

use serde_json::Value;
use sha2::{Digest, Sha256};

use super::identity::{BRIDGE_DIGEST_DOMAIN, MODULE_DIGEST_DOMAIN};

/// Serialize a JSON [`Value`] using the frozen ontology `/1` sorted-JSON grammar.
///
/// Object keys sort by Rust string order (UTF-8 bytes), arrays retain order,
/// strings use `serde_json` escaping, and numbers use `serde_json::Number`'s
/// spelling. No Unicode or floating-point normalization is performed. This is
/// not full RFC 8785 JCS (which has different key ordering and number rules).
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::composition::identity::COMPOSITION_DOMAIN;

    // Frozen before any encoder change for #1014. Expected hashes were computed
    // independently with Python hashlib over the literal UTF-8 bytes and the
    // NUL-terminated domain, not derived from the encoder under test.
    #[test]
    fn ontology_json_bytes_and_all_three_domains_match_frozen_vectors() {
        let input = r#"{
            "𐀀": 2, "\uE000": 1, "é": "e\u0301",
            "n": [-0.0, 0.0, 1.0, 1e30, 1e-7, 18446744073709551615],
            "a": [null, true, false, {"z": "é", "a": "quote\" slash/ newline\n"}]
        }"#;
        let canonical = concat!(
            r#"{"a":[null,true,false,{"a":"quote\" slash/ newline\n","z":"é"}],"n":[-0.0,0.0,1.0,1e+30,1e-7,18446744073709551615],"é":"e"#,
            "\u{301}",
            "\",\"\u{e000}\":1,\"𐀀\":2}",
        );
        for (input, expected_bytes, expected_digests) in [
            (
                "{}",
                "{}",
                [
                    "0ac9473099dfa8458a10862284a5f3d8cc8074352b1381532801787207e27acf",
                    "adbf86344fa0d9b236b1922a37ff03e111c8646ac159f20874644f4105dadbc2",
                    "c8dd8266bea2f0630c5990f5ca20f7b2cf953498fded20ea626cd3d9c6caf09f",
                ],
            ),
            (
                input,
                canonical,
                [
                    "e71e871057cbf44aa5314d1df06312eb5cb99caa84e858676c0f4c78ee49d478",
                    "d297a7cfeaadf9de85f0b00d2eea1882688dcc5a498a91ccacf077ce6b585568",
                    "9caedae437e332f96314b220eee94773bc3b2e14b14c3a6483dac017de683c01",
                ],
            ),
        ] {
            let value: Value = serde_json::from_str(input).unwrap();
            assert_eq!(canonical_json(&value).unwrap(), expected_bytes.as_bytes());
            for (domain, expected) in [
                MODULE_DIGEST_DOMAIN,
                BRIDGE_DIGEST_DOMAIN,
                COMPOSITION_DOMAIN,
            ]
            .into_iter()
            .zip(expected_digests)
            {
                assert_eq!(domain_digest(domain, &value).unwrap(), expected);
            }
        }
    }
}
