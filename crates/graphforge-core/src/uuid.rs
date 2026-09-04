//! UUID generation, deterministic derivation, and Arrow/Parquet serialisation helpers.
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

/// Derive the portable-v1 imported generation from its caller-owned operation.
/// The versioned name is a persisted compatibility contract.
#[must_use]
pub fn portable_import_generation(operation: &Uuid) -> Uuid {
    new_v5(operation, b"graphforge-portable-import-generation/1")
}

/// Derive the portable-v2 imported generation in a separate versioned domain.
#[must_use]
pub fn portable_v2_import_generation(operation: &Uuid) -> Uuid {
    new_v5(operation, b"graphforge-portable-v2-import-generation/1")
}

/// Derive one composite delta operation using its original mutation index.
/// The slash separates the versioned domain from the unambiguous decimal index.
#[must_use]
pub fn composite_delta_operation(operation: &Uuid, index: usize) -> Uuid {
    new_v5(
        operation,
        format!("graphforge-composite-delta-operation/1/{index}").as_bytes(),
    )
}

/// Derive the composite delta run from its transaction identity.
#[must_use]
pub fn composite_delta_run(transaction: &Uuid) -> Uuid {
    new_v5(transaction, b"graphforge-composite-delta-run/1")
}

/// Derive a hub clone operation using the historical URL namespace and name.
///
/// `repository` must be the validated canonical `owner/repository` identity
/// (whose slugs cannot contain `:`), and `immutable_version` the verified package
/// version. The colon therefore separates the components unambiguously. Preserve
/// this legacy domain: adding a new prefix would change existing retry identities.
#[must_use]
pub fn hub_clone_operation(repository: &str, immutable_version: &str) -> Uuid {
    new_v5(
        &Uuid::NAMESPACE_URL,
        format!("{repository}:{immutable_version}").as_bytes(),
    )
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
    fn v5_is_namespace_and_name_deterministic() {
        let namespace = Uuid::from_bytes([7; 16]);
        let first = new_v5(&namespace, b"graphforge");
        assert_eq!(first, new_v5(&namespace, b"graphforge"));
        assert_ne!(first, new_v5(&namespace, b"GraphForge"));
        assert_eq!(first.get_version_num(), 5);
    }

    #[test]
    fn persisted_derivation_golden_vectors() {
        // Frozen from the original namespace/name pairs, independently calculated
        // with Python uuid.uuid5. Compare bytes to protect persisted identities.
        let operation = Uuid::parse_str("00112233-4455-6677-8899-aabbccddeeff").unwrap();
        let version_a = format!("sha256:{}", "a".repeat(64));
        let version_b = format!("sha256:{}", "b".repeat(64));
        let vectors = [
            (
                portable_import_generation(&operation),
                "02abd337-e93c-56a3-bf8f-dadbb5b1e8d5",
            ),
            (
                portable_v2_import_generation(&operation),
                "167a58d3-52f2-533e-b3c4-01d743d7537d",
            ),
            (
                composite_delta_operation(&operation, 0),
                "bf613aba-38a4-5ae7-8982-1675caa13e7c",
            ),
            (
                composite_delta_operation(&operation, 1),
                "dcffd77e-a011-5480-8fb3-8493d22ff8be",
            ),
            (
                composite_delta_operation(&operation, 10),
                "de381822-c256-5bf9-904f-25692a6b6c7e",
            ),
            (
                composite_delta_run(&operation),
                "d230eba6-b8a4-549e-b032-888112a9fc62",
            ),
            (
                hub_clone_operation("curatelabs/demo", &version_a),
                "e6caa879-f226-55fc-9d67-41c15b8f9658",
            ),
            (
                hub_clone_operation("curatelabs/demo", &version_b),
                "db3c36b4-97ee-57bb-ac63-995c79fb577b",
            ),
            (
                hub_clone_operation("curatelabs/other", &version_a),
                "29ea96be-d350-5026-a85b-32cbf5b835cc",
            ),
            (
                new_v5(&PROVENANCE_NAMESPACE, b"graphforge"),
                "59a64d29-3fe8-5c2c-ad21-1f285e53d758",
            ),
        ];
        let mut unique = std::collections::HashSet::new();
        for (actual, expected) in vectors {
            assert_eq!(
                actual.as_bytes(),
                Uuid::parse_str(expected).unwrap().as_bytes()
            );
            assert!(unique.insert(actual), "derivation domains collided");
        }
        let other_operation = Uuid::from_bytes([7; 16]);
        assert_ne!(
            portable_import_generation(&operation),
            portable_import_generation(&other_operation)
        );
        assert_ne!(
            portable_v2_import_generation(&operation),
            portable_v2_import_generation(&other_operation)
        );
        assert_ne!(
            composite_delta_operation(&operation, 0),
            composite_delta_operation(&other_operation, 0)
        );
        assert_ne!(
            composite_delta_run(&operation),
            composite_delta_run(&other_operation)
        );
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
