//! UUIDv7 generation and Arrow/Parquet serialisation helpers.
//!
//! All write paths (CREATE, MERGE, batch ingest, provenance) mint UUIDv7
//! identifiers for new first-class objects.  Centralising generation here keeps
//! identity consistent across crates and provides the
//! [`FixedSizeBinary(16)`](to_bytes) conversions used by the storage layer.
//!
//! UUIDv7 is time-ordered (RFC 9562): the 48 most-significant bits are a
//! Unix-millisecond timestamp, so identifiers minted later sort after earlier
//! ones — useful for stable, roughly-chronological row ordering.

pub use uuid::Uuid;

/// Generate a new UUIDv7 stamped with the current time.
#[must_use]
pub fn new_v7() -> Uuid {
    Uuid::now_v7()
}

/// Namespace UUID for GraphForge provenance lineage (#604): the stable root for
/// content-addressed derived-fact handles. A fixed constant — never regenerate
/// it, or existing lineage `child_uuid`s stop resolving.
pub const PROVENANCE_NAMESPACE: Uuid = Uuid::from_bytes([
    0x9f, 0x6c, 0x2a, 0x1e, 0x7b, 0x42, 0x4d, 0x88, 0xa3, 0x10, 0x5e, 0x0c, 0x91, 0x33, 0x77, 0xd2,
]);

/// Mint a name-based (UUIDv5, SHA-1) identifier under `namespace`. The same
/// `(namespace, name)` always yields the same UUID, so it content-addresses a
/// derived fact: re-running a query re-derives the identical `child_uuid`,
/// letting lineage rows dedup/accumulate idempotently rather than growing
/// unboundedly. (#604)
#[must_use]
pub fn new_v5(namespace: &Uuid, name: &[u8]) -> Uuid {
    Uuid::new_v5(namespace, name)
}

/// Convert a [`Uuid`] into its 16-byte big-endian form, suitable for an Arrow
/// `FixedSizeBinary(16)` column.
#[must_use]
pub fn to_bytes(uuid: &Uuid) -> [u8; 16] {
    *uuid.as_bytes()
}

/// Reconstruct a [`Uuid`] from its 16-byte big-endian form.
#[must_use]
pub fn from_bytes(bytes: &[u8; 16]) -> Uuid {
    Uuid::from_bytes(*bytes)
}

/// Render a [`Uuid`] in canonical hyphenated form for display.
#[must_use]
pub fn to_string(uuid: &Uuid) -> String {
    uuid.hyphenated().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uuids_are_unique() {
        let mut seen = std::collections::HashSet::new();
        for _ in 0..1000 {
            assert!(seen.insert(new_v7()), "generated a duplicate UUID");
        }
        assert_eq!(seen.len(), 1000);
    }

    #[test]
    fn uuids_are_time_monotone() {
        // UUIDv7 embeds a millisecond timestamp in its high bits; successive
        // values generated within the same process should be non-decreasing
        // bitwise.  Comparing many in sequence keeps the test robust to
        // multiple UUIDs landing in the same millisecond (the v7 counter then
        // breaks the tie monotonically).
        let mut prev = new_v7();
        for _ in 0..1000 {
            let next = new_v7();
            assert!(next > prev, "UUIDv7 ordering violated: {next} !> {prev}");
            prev = next;
        }
    }

    #[test]
    fn byte_round_trip_preserves_value() {
        let original = new_v7();
        let bytes = to_bytes(&original);
        let restored = from_bytes(&bytes);
        assert_eq!(original, restored);
    }

    #[test]
    fn string_matches_rfc_format() {
        let s = to_string(&new_v7());
        // 8-4-4-4-12 hyphenated form, 36 chars total.
        assert_eq!(s.len(), 36);
        let groups: Vec<&str> = s.split('-').collect();
        assert_eq!(groups.len(), 5);
        assert_eq!(
            groups.iter().map(|g| g.len()).collect::<Vec<_>>(),
            vec![8, 4, 4, 4, 12]
        );
        assert!(s.chars().all(|c| c.is_ascii_hexdigit() || c == '-'));
        // Version nibble (first char of the 3rd group) must be '7'.
        assert_eq!(groups[2].chars().next(), Some('7'));
    }
}
