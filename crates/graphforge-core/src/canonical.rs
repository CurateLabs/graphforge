//! Shared `graphforge-canonical/1` envelope and byte primitives.
//!
//! Domain owners define their registered schemas and logical-value encoders.
//! This module owns the common bounded byte writer, closed fingerprint domains,
//! SHA-256 envelope, and UUIDv8 projection.

use sha2::{Digest, Sha256};

/// Frozen canonical contract version.
pub const CANONICAL_CONTRACT_VERSION: u32 = 1;
/// Maximum canonical payload size in version 1.
pub const MAX_CANONICAL_PAYLOAD_BYTES: u64 = 2_147_483_648;
/// Maximum UTF-8 value size in version 1.
pub const MAX_CANONICAL_TEXT_BYTES: u64 = 16_777_216;
/// Maximum binary value size in version 1.
pub const MAX_CANONICAL_BINARY_BYTES: u64 = 67_108_864;

/// Closed fingerprint domains in `graphforge-canonical/1`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CanonicalDomain {
    /// Authoritative schema bytes.
    Schema,
    /// Assertion record/table.
    Assertion,
    /// Evidence-link record/table.
    EvidenceLink,
    /// Confidence-assessment record/table.
    ConfidenceAssessment,
    /// Immutable reasoning record/table.
    Reasoning,
    /// Append-only assertion-status event/table.
    AssertionStatus,
    /// Append-only assertion-validity event/table.
    AssertionValidity,
    /// Immutable assertion-supersession relation/table.
    AssertionSupersession,
    /// Immutable hypothesis-group record/table.
    HypothesisGroup,
    /// Append-only hypothesis-membership event/table.
    HypothesisMembership,
    /// Append-only hypothesis-selection event/table.
    HypothesisSelection,
    /// Ordered composite graph-mutation content.
    CompositeGraphMutationContent,
    /// Composite graph and knowledge request content.
    CompositeRequest,
    /// Provenance event/table.
    ProvenanceEvent,
    /// Lineage record/table.
    Lineage,
    /// Neutral M18 invocation descriptor.
    InvocationDescriptor,
    /// Resolved graph projection.
    GraphProjection,
    /// Resolved-belief policy bytes.
    BeliefProjectionPolicy,
    /// Append-only interpretation attachment.
    BeliefProjectionAttachment,
    /// Public Arrow result table.
    ArrowResult,
}

impl CanonicalDomain {
    /// Exact lowercase domain tag.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Schema => "graphforge/schema",
            Self::Assertion => "graphforge/assertion",
            Self::EvidenceLink => "graphforge/evidence-link",
            Self::ConfidenceAssessment => "graphforge/confidence-assessment",
            Self::Reasoning => "graphforge/reasoning",
            Self::AssertionStatus => "graphforge/assertion-status",
            Self::AssertionValidity => "graphforge/assertion-validity",
            Self::AssertionSupersession => "graphforge/assertion-supersession",
            Self::HypothesisGroup => "graphforge/hypothesis-group",
            Self::HypothesisMembership => "graphforge/hypothesis-membership",
            Self::HypothesisSelection => "graphforge/hypothesis-selection",
            Self::CompositeGraphMutationContent => "graphforge/composite-graph-mutation-content",
            Self::CompositeRequest => "graphforge/composite-request",
            Self::ProvenanceEvent => "graphforge/provenance-event",
            Self::Lineage => "graphforge/lineage",
            Self::InvocationDescriptor => "graphforge/invocation-descriptor",
            Self::GraphProjection => "graphforge/graph-projection",
            Self::BeliefProjectionPolicy => "graphforge/belief-projection-policy",
            Self::BeliefProjectionAttachment => "graphforge/belief-projection-attachment",
            Self::ArrowResult => "graphforge/arrow-result",
        }
    }
}

/// Structured canonicalization failures.
#[derive(thiserror::Error, Clone, Debug, PartialEq, Eq)]
pub enum CanonicalError {
    /// A bounded byte or item limit was exceeded.
    #[error("canonical limit exceeded for {item}: observed {observed}, limit {limit}")]
    Limit {
        /// Safe item category.
        item: &'static str,
        /// Observed byte/item count.
        observed: u64,
        /// Frozen maximum.
        limit: u64,
    },
    /// A caller requested an unsupported canonical contract version.
    #[error("unsupported canonical contract version {version}")]
    UnsupportedVersion {
        /// Unsupported version.
        version: u32,
    },
    /// Canonical bytes are truncated or structurally invalid.
    #[error("invalid canonical payload: {0}")]
    Malformed(&'static str),
}

impl CanonicalError {
    /// Stable public error code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Limit { .. } => "GF_CANONICAL_LIMIT",
            Self::UnsupportedVersion { .. } => "GF_UNSUPPORTED_CONTRACT_VERSION",
            Self::Malformed(_) => "GF_CANONICAL_INVALID",
        }
    }
}

/// Bounded writer for the fixed-width canonical grammar.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CanonicalWriter {
    bytes: Vec<u8>,
}

/// Strict reader for the fixed-width canonical grammar.
#[derive(Clone, Debug)]
pub struct CanonicalReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> CanonicalReader<'a> {
    /// Construct a reader after enforcing the top-level payload bound.
    pub fn new(bytes: &'a [u8]) -> Result<Self, CanonicalError> {
        let observed = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        if observed > MAX_CANONICAL_PAYLOAD_BYTES {
            return Err(CanonicalError::Limit {
                item: "payload",
                observed,
                limit: MAX_CANONICAL_PAYLOAD_BYTES,
            });
        }
        Ok(Self { bytes, offset: 0 })
    }

    /// Read an exact number of bytes.
    pub fn raw(&mut self, length: usize) -> Result<&'a [u8], CanonicalError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(CanonicalError::Malformed("byte range overflow"))?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(CanonicalError::Malformed("truncated payload"))?;
        self.offset = end;
        Ok(value)
    }

    /// Read one unsigned byte.
    pub fn u8(&mut self) -> Result<u8, CanonicalError> {
        Ok(self.raw(1)?[0])
    }

    /// Read one big-endian unsigned 32-bit integer.
    pub fn u32(&mut self) -> Result<u32, CanonicalError> {
        Ok(u32::from_be_bytes(
            self.raw(4)?.try_into().expect("reader returned four bytes"),
        ))
    }

    /// Read one big-endian unsigned 64-bit integer.
    pub fn u64(&mut self) -> Result<u64, CanonicalError> {
        Ok(u64::from_be_bytes(
            self.raw(8)?
                .try_into()
                .expect("reader returned eight bytes"),
        ))
    }

    /// Read one bounded length-prefixed UTF-8 value.
    pub fn text(&mut self) -> Result<&'a str, CanonicalError> {
        let length = self.u64()?;
        if length > MAX_CANONICAL_TEXT_BYTES {
            return Err(CanonicalError::Limit {
                item: "text",
                observed: length,
                limit: MAX_CANONICAL_TEXT_BYTES,
            });
        }
        let length = usize::try_from(length)
            .map_err(|_| CanonicalError::Malformed("text length exceeds usize"))?;
        std::str::from_utf8(self.raw(length)?)
            .map_err(|_| CanonicalError::Malformed("text is not valid UTF-8"))
    }

    /// Reject unconsumed trailing bytes.
    pub fn finish(self) -> Result<(), CanonicalError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(CanonicalError::Malformed("trailing bytes"))
        }
    }
}

impl CanonicalWriter {
    /// Construct an empty writer.
    #[must_use]
    pub const fn new() -> Self {
        Self { bytes: Vec::new() }
    }

    /// Append exact bytes while enforcing the payload bound.
    pub fn raw(&mut self, bytes: &[u8]) -> Result<(), CanonicalError> {
        let next = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .and_then(|value| u64::try_from(value).ok())
            .ok_or(CanonicalError::Limit {
                item: "payload",
                observed: u64::MAX,
                limit: MAX_CANONICAL_PAYLOAD_BYTES,
            })?;
        if next > MAX_CANONICAL_PAYLOAD_BYTES {
            return Err(CanonicalError::Limit {
                item: "payload",
                observed: next,
                limit: MAX_CANONICAL_PAYLOAD_BYTES,
            });
        }
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }

    /// Append one unsigned byte.
    pub fn u8(&mut self, value: u8) -> Result<(), CanonicalError> {
        self.raw(&[value])
    }

    /// Append an unsigned big-endian 16-bit integer.
    pub fn u16(&mut self, value: u16) -> Result<(), CanonicalError> {
        self.raw(&value.to_be_bytes())
    }

    /// Append an unsigned big-endian 32-bit integer.
    pub fn u32(&mut self, value: u32) -> Result<(), CanonicalError> {
        self.raw(&value.to_be_bytes())
    }

    /// Append an unsigned big-endian 64-bit integer.
    pub fn u64(&mut self, value: u64) -> Result<(), CanonicalError> {
        self.raw(&value.to_be_bytes())
    }

    /// Append a signed big-endian 64-bit integer.
    pub fn i64(&mut self, value: i64) -> Result<(), CanonicalError> {
        self.raw(&value.to_be_bytes())
    }

    /// Append a bounded binary item (`u64` length plus bytes).
    pub fn binary(&mut self, value: &[u8]) -> Result<(), CanonicalError> {
        let length = u64::try_from(value.len()).map_err(|_| CanonicalError::Limit {
            item: "binary",
            observed: u64::MAX,
            limit: MAX_CANONICAL_BINARY_BYTES,
        })?;
        if length > MAX_CANONICAL_BINARY_BYTES {
            return Err(CanonicalError::Limit {
                item: "binary",
                observed: length,
                limit: MAX_CANONICAL_BINARY_BYTES,
            });
        }
        self.u64(length)?;
        self.raw(value)
    }

    /// Append a bounded UTF-8 item (`u64` length plus exact UTF-8 bytes).
    pub fn text(&mut self, value: &str) -> Result<(), CanonicalError> {
        let length = u64::try_from(value.len()).map_err(|_| CanonicalError::Limit {
            item: "text",
            observed: u64::MAX,
            limit: MAX_CANONICAL_TEXT_BYTES,
        })?;
        if length > MAX_CANONICAL_TEXT_BYTES {
            return Err(CanonicalError::Limit {
                item: "text",
                observed: length,
                limit: MAX_CANONICAL_TEXT_BYTES,
            });
        }
        self.u64(length)?;
        self.raw(value.as_bytes())
    }

    /// Consume the writer.
    #[must_use]
    pub fn finish(self) -> Vec<u8> {
        self.bytes
    }
}

/// Build the frozen fingerprint preimage for a canonical payload.
pub fn fingerprint_preimage(
    domain: CanonicalDomain,
    contract_version: u32,
    payload: &[u8],
) -> Result<Vec<u8>, CanonicalError> {
    if contract_version != CANONICAL_CONTRACT_VERSION {
        return Err(CanonicalError::UnsupportedVersion {
            version: contract_version,
        });
    }
    let payload_length = u64::try_from(payload.len()).map_err(|_| CanonicalError::Limit {
        item: "payload",
        observed: u64::MAX,
        limit: MAX_CANONICAL_PAYLOAD_BYTES,
    })?;
    if payload_length > MAX_CANONICAL_PAYLOAD_BYTES {
        return Err(CanonicalError::Limit {
            item: "payload",
            observed: payload_length,
            limit: MAX_CANONICAL_PAYLOAD_BYTES,
        });
    }
    let domain_bytes = domain.as_str().as_bytes();
    let domain_length = u16::try_from(domain_bytes.len()).expect("closed domains fit UInt16");
    let mut writer = CanonicalWriter::new();
    writer.raw(b"GFFP")?;
    writer.u8(1)?;
    writer.u16(domain_length)?;
    writer.raw(domain_bytes)?;
    writer.u32(contract_version)?;
    writer.u64(payload_length)?;
    writer.raw(payload)?;
    Ok(writer.finish())
}

/// Compute the domain-separated SHA-256 fingerprint.
pub fn fingerprint(
    domain: CanonicalDomain,
    contract_version: u32,
    payload: &[u8],
) -> Result<[u8; 32], CanonicalError> {
    Ok(Sha256::digest(fingerprint_preimage(domain, contract_version, payload)?).into())
}

/// Project a full canonical fingerprint into an RFC 9562 UUIDv8.
#[must_use]
pub fn uuid_v8(fingerprint: [u8; 32]) -> uuid::Uuid {
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&fingerprint[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    uuid::Uuid::from_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_hex(value: &str) -> Vec<u8> {
        value
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let text = std::str::from_utf8(pair).unwrap();
                u8::from_str_radix(text, 16).unwrap()
            })
            .collect()
    }

    #[test]
    fn envelope_hash_and_uuid_match_the_frozen_vector() {
        let payload = decode_hex(
            "474654310000000000000027474653310000000100000000000000096e6f64655f757569640032000000100000000000000000000000000000000101018f47d2a1b27c3d8e4f001122334455",
        );
        let preimage = fingerprint_preimage(
            CanonicalDomain::ArrowResult,
            CANONICAL_CONTRACT_VERSION,
            &payload,
        )
        .unwrap();
        assert_eq!(
            preimage,
            decode_hex(
                "474646500100176772617068666f7267652f6172726f772d726573756c7400000001000000000000004c474654310000000000000027474653310000000100000000000000096e6f64655f757569640032000000100000000000000000000000000000000101018f47d2a1b27c3d8e4f001122334455",
            )
        );
        let digest = fingerprint(
            CanonicalDomain::ArrowResult,
            CANONICAL_CONTRACT_VERSION,
            &payload,
        )
        .unwrap();
        assert_eq!(
            digest,
            decode_hex("3cc432a310818554c11f6eb272fead2c549bcd108ab059453a641ab379c7bbb9")
                .as_slice()
        );
        assert_eq!(
            uuid_v8(digest).to_string(),
            "3cc432a3-1081-8554-811f-6eb272fead2c"
        );
    }

    #[test]
    fn writer_is_big_endian_exact_and_bounded() {
        let mut writer = CanonicalWriter::new();
        writer.u16(0x0102).unwrap();
        writer.u32(0x0304_0506).unwrap();
        writer.u64(7).unwrap();
        writer.text("é").unwrap();
        assert_eq!(
            writer.finish(),
            [
                1, 2, 3, 4, 5, 6, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 2, 0xc3, 0xa9,
            ]
        );
    }

    #[test]
    fn unknown_versions_fail_before_hashing() {
        let error = fingerprint(CanonicalDomain::Schema, 2, b"").unwrap_err();
        assert_eq!(error.code(), "GF_UNSUPPORTED_CONTRACT_VERSION");
    }

    #[test]
    fn reader_round_trips_and_rejects_truncation_trailing_and_utf8() {
        let mut writer = CanonicalWriter::new();
        writer.u8(7).unwrap();
        writer.u32(9).unwrap();
        writer.u64(11).unwrap();
        writer.text("é").unwrap();
        let bytes = writer.finish();
        let mut reader = CanonicalReader::new(&bytes).unwrap();
        assert_eq!(reader.u8().unwrap(), 7);
        assert_eq!(reader.u32().unwrap(), 9);
        assert_eq!(reader.u64().unwrap(), 11);
        assert_eq!(reader.text().unwrap(), "é");
        reader.finish().unwrap();

        assert!(CanonicalReader::new(&[0, 0]).unwrap().u32().is_err());
        let mut trailing = CanonicalReader::new(&[1, 2]).unwrap();
        assert_eq!(trailing.u8().unwrap(), 1);
        assert!(trailing.finish().is_err());
        let mut invalid = CanonicalReader::new(&[0, 0, 0, 0, 0, 0, 0, 1, 0xff]).unwrap();
        assert!(invalid.text().is_err());
    }

    #[test]
    fn binary_signed_integer_and_declared_text_limit_contracts_are_exact() {
        let mut writer = CanonicalWriter::new();
        writer.i64(-7).unwrap();
        writer.binary(&[1, 2, 3]).unwrap();
        assert_eq!(
            writer.finish(),
            [
                0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xf9, 0, 0, 0, 0, 0, 0, 0, 3, 1, 2, 3,
            ]
        );

        let mut declared = (MAX_CANONICAL_TEXT_BYTES + 1).to_be_bytes().to_vec();
        declared.extend_from_slice(b"unused");
        let error = CanonicalReader::new(&declared).unwrap().text().unwrap_err();
        assert_eq!(error.code(), "GF_CANONICAL_LIMIT");
        assert!(matches!(error, CanonicalError::Limit { item: "text", .. }));
    }

    #[test]
    fn every_domain_and_error_code_has_a_frozen_external_spelling() {
        let domains = [
            (CanonicalDomain::Schema, "graphforge/schema"),
            (CanonicalDomain::Assertion, "graphforge/assertion"),
            (CanonicalDomain::EvidenceLink, "graphforge/evidence-link"),
            (
                CanonicalDomain::ConfidenceAssessment,
                "graphforge/confidence-assessment",
            ),
            (CanonicalDomain::Reasoning, "graphforge/reasoning"),
            (
                CanonicalDomain::AssertionStatus,
                "graphforge/assertion-status",
            ),
            (
                CanonicalDomain::AssertionValidity,
                "graphforge/assertion-validity",
            ),
            (
                CanonicalDomain::AssertionSupersession,
                "graphforge/assertion-supersession",
            ),
            (
                CanonicalDomain::HypothesisGroup,
                "graphforge/hypothesis-group",
            ),
            (
                CanonicalDomain::HypothesisMembership,
                "graphforge/hypothesis-membership",
            ),
            (
                CanonicalDomain::HypothesisSelection,
                "graphforge/hypothesis-selection",
            ),
            (
                CanonicalDomain::CompositeGraphMutationContent,
                "graphforge/composite-graph-mutation-content",
            ),
            (
                CanonicalDomain::CompositeRequest,
                "graphforge/composite-request",
            ),
            (
                CanonicalDomain::ProvenanceEvent,
                "graphforge/provenance-event",
            ),
            (CanonicalDomain::Lineage, "graphforge/lineage"),
            (
                CanonicalDomain::InvocationDescriptor,
                "graphforge/invocation-descriptor",
            ),
            (
                CanonicalDomain::GraphProjection,
                "graphforge/graph-projection",
            ),
            (
                CanonicalDomain::BeliefProjectionPolicy,
                "graphforge/belief-projection-policy",
            ),
            (
                CanonicalDomain::BeliefProjectionAttachment,
                "graphforge/belief-projection-attachment",
            ),
            (CanonicalDomain::ArrowResult, "graphforge/arrow-result"),
        ];
        for (domain, spelling) in domains {
            assert_eq!(domain.as_str(), spelling);
        }

        assert_eq!(
            CanonicalError::Limit {
                item: "x",
                observed: 2,
                limit: 1
            }
            .code(),
            "GF_CANONICAL_LIMIT"
        );
        assert_eq!(
            CanonicalError::UnsupportedVersion { version: 0 }.code(),
            "GF_UNSUPPORTED_CONTRACT_VERSION"
        );
        assert_eq!(
            CanonicalError::Malformed("x").code(),
            "GF_CANONICAL_INVALID"
        );
    }

    #[test]
    fn reader_rejects_declared_text_limit_and_offset_overflow() {
        let bytes = (MAX_CANONICAL_TEXT_BYTES + 1).to_be_bytes();
        let error = CanonicalReader::new(&bytes).unwrap().text().unwrap_err();
        assert!(matches!(error, CanonicalError::Limit { item: "text", .. }));

        let mut reader = CanonicalReader::new(&[0]).unwrap();
        reader.u8().unwrap();
        assert_eq!(
            reader.raw(usize::MAX).unwrap_err(),
            CanonicalError::Malformed("byte range overflow")
        );
    }

    #[test]
    fn writer_covers_signed_binary_and_single_byte_grammar() {
        let mut writer = CanonicalWriter::new();
        writer.u8(0xab).unwrap();
        writer.i64(-2).unwrap();
        writer.binary(&[1, 2, 3]).unwrap();
        assert_eq!(
            writer.finish(),
            [
                0xab, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe, 0, 0, 0, 0, 0, 0, 0, 3, 1, 2,
                3
            ]
        );
    }
}
