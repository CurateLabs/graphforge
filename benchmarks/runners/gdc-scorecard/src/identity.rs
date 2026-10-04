//! Deterministic entity identity. Import requires UUIDv7-shaped values; the
//! bytes here are a SHA-256 prefix with version and variant bits set, so the
//! same (label, id) always maps to the same UUID in every conversion.

use sha2::{Digest, Sha256};

pub type Uuid = [u8; 16];

fn shaped(mut hasher: Sha256) -> Uuid {
    let digest = hasher.finalize_reset();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x70;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    bytes
}

#[must_use]
pub fn node_uuid(label: &str, id: i64) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(b"graphforge.gdc.node.v1\0");
    hasher.update(label.as_bytes());
    hasher.update([0]);
    hasher.update(id.to_be_bytes());
    shaped(hasher)
}

/// Edge identity is (table id, row ordinal within the table across its files).
#[must_use]
pub fn edge_uuid(table: &str, ordinal: u64) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(b"graphforge.gdc.edge.v1\0");
    hasher.update(table.as_bytes());
    hasher.update([0]);
    hasher.update(ordinal.to_be_bytes());
    shaped(hasher)
}

#[must_use]
pub fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes.iter().fold(String::new(), |mut out, byte| {
        let _ = write!(out, "{byte:02x}");
        out
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_is_stable_versioned_and_distinct() {
        let a = node_uuid("Person", 7);
        assert_eq!(a, node_uuid("Person", 7));
        assert_ne!(a, node_uuid("Place", 7));
        assert_ne!(a, node_uuid("Person", 8));
        assert_eq!(a[6] >> 4, 7);
        assert_eq!(a[8] >> 6, 0b10);
        assert_ne!(edge_uuid("knows", 0), edge_uuid("knows", 1));
        assert_ne!(edge_uuid("knows", 0), edge_uuid("likes", 0));
    }

    #[test]
    fn pinned_vector_guards_the_derivation() {
        assert_eq!(hex(&node_uuid("Person", 1)).len(), 32);
        // A change to this value changes every converted graph's identities.
        assert_eq!(hex(&node_uuid("Person", 1)), PINNED_PERSON_1);
    }

    const PINNED_PERSON_1: &str = "6e89c2863d5d722a95ba53fc0addc07a";
}
