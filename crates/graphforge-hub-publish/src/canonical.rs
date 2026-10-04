//! Canonical JSON, duplicate-member rejection, and SHA-256 digests.

use crate::error::{HubPublishError, invalid};
use serde::Deserializer;
use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Value};
use sha2::{Digest as _, Sha256};
use std::fmt;

/// Lowercase `sha256:<64 hex>` digest of `bytes`.
#[must_use]
pub fn sha256_digest(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(71);
    out.push_str("sha256:");
    for byte in digest {
        out.push(HEX[usize::from(byte >> 4)] as char);
        out.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    out
}

/// Require the canonical digest spelling `sha256:` followed by 64 lowercase hex digits.
///
/// Uppercase hex is rejected: it names the same bytes but never compares equal
/// to a computed digest, so admitting it would make an object unverifiable.
pub fn validate_digest(digest: &str) -> Result<(), HubPublishError> {
    let Some(hex) = digest.strip_prefix("sha256:") else {
        return Err(invalid("digest must be sha256:<64 lowercase hex>"));
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Err(invalid("digest must be sha256:<64 lowercase hex>"));
    }
    Ok(())
}

/// Compact JSON with object members sorted by key, recursively.
///
/// Independent of `serde_json`'s map ordering features, so the bytes are a
/// stable commitment input.
#[must_use]
pub fn canonical_json(value: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    write_canonical(value, &mut out);
    out
}

fn write_canonical(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Object(map) => {
            let mut entries: Vec<(&String, &Value)> = map.iter().collect();
            entries.sort_by(|left, right| left.0.cmp(right.0));
            out.push(b'{');
            for (index, (key, member)) in entries.into_iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                out.extend_from_slice(
                    serde_json::to_string(key)
                        .expect("string keys serialize")
                        .as_bytes(),
                );
                out.push(b':');
                write_canonical(member, out);
            }
            out.push(b'}');
        }
        Value::Array(items) => {
            out.push(b'[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                write_canonical(item, out);
            }
            out.push(b']');
        }
        scalar => out.extend_from_slice(
            serde_json::to_string(scalar)
                .expect("scalars serialize")
                .as_bytes(),
        ),
    }
}

/// Parse untrusted JSON, rejecting duplicate object members at any depth.
pub fn parse_unique_json(bytes: &[u8]) -> Result<Value, HubPublishError> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let value = UniqueValue
        .deserialize(&mut deserializer)
        .map_err(|_| invalid("request body is not valid JSON without duplicate members"))?;
    deserializer
        .end()
        .map_err(|_| invalid("request body has trailing content"))?;
    Ok(value)
}

struct UniqueValue;

impl<'de> DeserializeSeed<'de> for UniqueValue {
    type Value = Value;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Value, D::Error> {
        deserializer.deserialize_any(UniqueVisitor)
    }
}

struct UniqueVisitor;

impl<'de> Visitor<'de> for UniqueVisitor {
    type Value = Value;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("JSON value")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Value, E> {
        Ok(Value::Bool(value))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Value, E> {
        Ok(Value::from(value))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Value, E> {
        Ok(Value::from(value))
    }

    fn visit_f64<E: de::Error>(self, value: f64) -> Result<Value, E> {
        serde_json::Number::from_f64(value)
            .map(Value::Number)
            .ok_or_else(|| E::custom("non-finite number"))
    }

    fn visit_str<E>(self, value: &str) -> Result<Value, E> {
        Ok(Value::String(value.to_owned()))
    }

    fn visit_string<E>(self, value: String) -> Result<Value, E> {
        Ok(Value::String(value))
    }

    fn visit_unit<E>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut access: A) -> Result<Value, A::Error> {
        let mut items = Vec::new();
        while let Some(item) = access.next_element_seed(UniqueValue)? {
            items.push(item);
        }
        Ok(Value::Array(items))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut access: A) -> Result<Value, A::Error> {
        let mut map = Map::new();
        while let Some(key) = access.next_key::<String>()? {
            if map.contains_key(&key) {
                return Err(de::Error::custom("duplicate JSON member"));
            }
            let value = access.next_value_seed(UniqueValue)?;
            map.insert(key, value);
        }
        Ok(Value::Object(map))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uppercase_hex_digest_is_rejected() {
        let lower = sha256_digest(b"x");
        validate_digest(&lower).unwrap();
        let upper = format!("sha256:{}", lower[7..].to_ascii_uppercase());
        assert!(validate_digest(&upper).is_err());
        assert!(validate_digest("sha256:abc").is_err());
        assert!(validate_digest(&lower[7..]).is_err());
    }

    #[test]
    fn duplicate_members_are_rejected_at_any_depth() {
        assert!(parse_unique_json(br#"{"a":1,"a":2}"#).is_err());
        assert!(parse_unique_json(br#"{"a":[{"b":1,"b":1}]}"#).is_err());
        assert!(parse_unique_json(br#"{"a":1} x"#).is_err());
        assert_eq!(
            parse_unique_json(br#"{"a":[1,{"b":null}]}"#).unwrap(),
            serde_json::json!({"a":[1,{"b":null}]})
        );
    }

    #[test]
    fn canonical_json_sorts_members_recursively() {
        let value = serde_json::json!({"z":1,"a":{"y":[{"d":1,"c":2}],"b":"\u{1}"}});
        assert_eq!(
            String::from_utf8(canonical_json(&value)).unwrap(),
            r#"{"a":{"b":"\u0001","y":[{"c":2,"d":1}]},"z":1}"#
        );
    }
}
