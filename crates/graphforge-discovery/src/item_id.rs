//! Permanent IDs for ontology modules and bridge sets (#2215).
//!
//! A permanent ID is a typed, opaque token that never contains an owner:
//! `<prefix>_<16 lowercase Crockford base32 characters>`, for example
//! `ont_7k2m9x4q8r3t5v1w`. The prefix names the kind (`ont` for an ontology
//! module, `brg` for a bridge set) and the 16 characters carry 80 random bits.
//! Data always holds the canonical URI `https://graphforge.sh/<id>`, which is
//! the module's `ontology_id` or the bridge set's `bridge_id`.
//!
//! Input is read ASCII case-insensitively and normalized to lowercase. Only
//! case is folded: the Crockford aliases (`i`/`l` for `1`, `o` for `0`) are
//! rejected, so every accepted spelling names exactly one ID.
//!
//! This module is the single grammar for Core, the bindings and the Hub.
//! Parsing and formatting build for every target; [`ItemId::generate`] needs
//! the default `generate` feature and a non-wasm target.

use std::fmt;
use std::str::FromStr;

/// Canonical URI base of every permanent ID written into data.
pub const ITEM_ID_URI_BASE: &str = "https://graphforge.sh/";

/// Number of Crockford base32 characters after the prefix separator.
pub const ITEM_ID_RANDOM_CHARS: usize = 16;

/// Number of random bytes behind [`ITEM_ID_RANDOM_CHARS`] (80 bits).
pub const ITEM_ID_RANDOM_BYTES: usize = 10;

/// Length of a canonical bare ID: a three-letter prefix, `_`, and the random part.
pub const ITEM_ID_LEN: usize = 4 + ITEM_ID_RANDOM_CHARS;

/// Lowercase Crockford base32 alphabet (no `i`, `l`, `o`, `u`).
const ALPHABET: &[u8; 32] = b"0123456789abcdefghjkmnpqrstvwxyz";

/// Kind of item a permanent ID names. The prefix is part of the ID.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ItemKind {
    /// Ontology module, prefix `ont`, field `ontology_id`.
    OntologyModule,
    /// Bridge set, prefix `brg`, field `bridge_id`.
    BridgeSet,
}

impl ItemKind {
    /// Every kind, in declaration order.
    pub const ALL: [Self; 2] = [Self::OntologyModule, Self::BridgeSet];

    /// Three-letter lowercase prefix, without the `_` separator.
    #[must_use]
    pub const fn prefix(self) -> &'static str {
        match self {
            Self::OntologyModule => "ont",
            Self::BridgeSet => "brg",
        }
    }

    /// Document field that holds the ID's canonical URI.
    #[must_use]
    pub const fn field(self) -> &'static str {
        match self {
            Self::OntologyModule => "ontology_id",
            Self::BridgeSet => "bridge_id",
        }
    }

    /// Kind for a lowercase prefix, if it is a known one.
    #[must_use]
    pub fn from_prefix(prefix: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.prefix() == prefix)
    }
}

impl fmt::Display for ItemKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::OntologyModule => "ontology module",
            Self::BridgeSet => "bridge set",
        })
    }
}

/// Why a string is not a permanent ID of the expected form.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ItemIdError {
    /// The bare ID is not `<three letters>_<16 characters>` long.
    Length,
    /// The ID has no `_` after a three-letter prefix.
    MissingSeparator,
    /// The prefix is three letters but not a known kind.
    UnknownPrefix,
    /// The random part contains a character outside lowercase Crockford base32.
    InvalidCharacter,
    /// The URI is not `https://graphforge.sh/<id>`.
    NotGraphForgeUri,
    /// The ID is well formed but names a different kind.
    KindMismatch {
        /// Kind the caller required.
        expected: ItemKind,
        /// Kind the ID's prefix names.
        found: ItemKind,
    },
}

impl fmt::Display for ItemIdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Length => formatter.write_str(
                "permanent ID must be a three-letter prefix, '_', and 16 Crockford base32 characters",
            ),
            Self::MissingSeparator => {
                formatter.write_str("permanent ID must have '_' after its three-letter prefix")
            }
            Self::UnknownPrefix => {
                formatter.write_str("permanent ID prefix must be 'ont' or 'brg'")
            }
            Self::InvalidCharacter => formatter.write_str(
                "permanent ID must use Crockford base32 characters (0-9, a-z without i, l, o, u)",
            ),
            Self::NotGraphForgeUri => formatter
                .write_str("permanent ID URI must be https://graphforge.sh/ followed by the ID"),
            Self::KindMismatch { expected, found } => write!(
                formatter,
                "permanent ID names a {found}, but a {expected} ID was required"
            ),
        }
    }
}

impl std::error::Error for ItemIdError {}

/// A validated permanent ID in canonical lowercase form.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ItemId {
    kind: ItemKind,
    canonical: String,
}

impl ItemId {
    /// Parse a bare ID (`ont_…`), reading it ASCII case-insensitively.
    ///
    /// # Errors
    ///
    /// Returns [`ItemIdError`] when the input is not a bare permanent ID.
    pub fn parse(input: &str) -> Result<Self, ItemIdError> {
        let bytes = input.as_bytes();
        if bytes.len() != ITEM_ID_LEN {
            return Err(ItemIdError::Length);
        }
        if bytes[3] != b'_' {
            return Err(ItemIdError::MissingSeparator);
        }
        let canonical = input.to_ascii_lowercase();
        let kind = ItemKind::from_prefix(&canonical[..3]).ok_or(ItemIdError::UnknownPrefix)?;
        if !canonical.as_bytes()[4..]
            .iter()
            .all(|byte| ALPHABET.contains(byte))
        {
            return Err(ItemIdError::InvalidCharacter);
        }
        Ok(Self { kind, canonical })
    }

    /// Parse the canonical URI form `https://graphforge.sh/<id>`.
    ///
    /// The scheme and host are matched ASCII case-insensitively, like the ID.
    /// No other path, query, fragment, port or trailing slash is accepted.
    ///
    /// # Errors
    ///
    /// Returns [`ItemIdError::NotGraphForgeUri`] for any other URI, or the
    /// bare-ID error for a malformed ID after the base.
    pub fn parse_uri(input: &str) -> Result<Self, ItemIdError> {
        let base = ITEM_ID_URI_BASE.len();
        if input.len() < base || !input.is_char_boundary(base) {
            return Err(ItemIdError::NotGraphForgeUri);
        }
        let (prefix, id) = input.split_at(base);
        if !prefix.eq_ignore_ascii_case(ITEM_ID_URI_BASE) || id.contains(['/', '?', '#']) {
            return Err(ItemIdError::NotGraphForgeUri);
        }
        Self::parse(id)
    }

    /// Parse either a bare ID or its canonical URI, as a client accepts it.
    ///
    /// # Errors
    ///
    /// Returns [`ItemIdError`] when the input is neither form.
    pub fn parse_reference(input: &str) -> Result<Self, ItemIdError> {
        if input.contains(':') {
            Self::parse_uri(input)
        } else {
            Self::parse(input)
        }
    }

    /// Parse a bare ID or URI and require `kind`.
    ///
    /// # Errors
    ///
    /// Returns [`ItemIdError::KindMismatch`] for a well-formed ID of another
    /// kind, or the parse error.
    pub fn parse_kind(input: &str, kind: ItemKind) -> Result<Self, ItemIdError> {
        let id = Self::parse_reference(input)?;
        if id.kind == kind {
            Ok(id)
        } else {
            Err(ItemIdError::KindMismatch {
                expected: kind,
                found: id.kind,
            })
        }
    }

    /// Build the ID whose random part encodes `random` (80 bits, big-endian).
    #[must_use]
    pub fn from_random_bytes(kind: ItemKind, random: [u8; ITEM_ID_RANDOM_BYTES]) -> Self {
        let mut canonical = String::with_capacity(ITEM_ID_LEN);
        canonical.push_str(kind.prefix());
        canonical.push('_');
        let bits = random
            .iter()
            .fold(0_u128, |bits, byte| (bits << 8) | u128::from(*byte));
        for index in (0..ITEM_ID_RANDOM_CHARS).rev() {
            let symbol = (bits >> (index * 5)) & 0x1f;
            canonical.push(char::from(ALPHABET[symbol as usize]));
        }
        Self { kind, canonical }
    }

    /// Create a fresh ID from the operating system's CSPRNG.
    ///
    /// # Errors
    ///
    /// Returns the platform error when no secure randomness is available;
    /// never falls back to a weaker source.
    #[cfg(all(feature = "generate", not(target_arch = "wasm32")))]
    pub fn generate(kind: ItemKind) -> Result<Self, getrandom::Error> {
        let mut random = [0_u8; ITEM_ID_RANDOM_BYTES];
        getrandom::fill(&mut random)?;
        Ok(Self::from_random_bytes(kind, random))
    }

    /// Kind named by the prefix.
    #[must_use]
    pub const fn kind(&self) -> ItemKind {
        self.kind
    }

    /// Canonical lowercase bare ID, for example `ont_7k2m9x4q8r3t5v1w`.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.canonical
    }

    /// Canonical URI written into data: `https://graphforge.sh/<id>`.
    #[must_use]
    pub fn to_uri(&self) -> String {
        format!("{ITEM_ID_URI_BASE}{}", self.canonical)
    }
}

impl fmt::Display for ItemId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.canonical)
    }
}

impl FromStr for ItemId {
    type Err = ItemIdError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        Self::parse_reference(input)
    }
}

/// Whether `name` has the shape of any permanent ID,
/// `^[a-z]{3}_[0-9a-hjkmnp-tv-z]{16}$`, including prefixes not yet assigned.
///
/// Hub owner handles and repository names must not have this shape, so a
/// path segment never reads as both a name and an ID.
#[must_use]
pub fn has_item_id_shape(name: &str) -> bool {
    let bytes = name.as_bytes();
    bytes.len() == ITEM_ID_LEN
        && bytes[..3].iter().all(u8::is_ascii_lowercase)
        && bytes[3] == b'_'
        && bytes[4..].iter().all(|byte| ALPHABET.contains(byte))
}

#[cfg(test)]
mod tests {
    use super::*;

    const MODULE: &str = "ont_7k2m9x4q8r3t5v1w";

    /// Reference implementation of the published regular expression.
    fn matches_regex(name: &str) -> bool {
        let bytes = name.as_bytes();
        bytes.len() == 20
            && bytes[..3].iter().all(u8::is_ascii_lowercase)
            && bytes[3] == b'_'
            && bytes[4..].iter().all(|b| {
                matches!(b, b'0'..=b'9' | b'a'..=b'h' | b'j' | b'k' | b'm' | b'n' | b'p'..=b't' | b'v'..=b'z')
            })
    }

    #[test]
    fn alphabet_is_lowercase_crockford_base32() {
        assert_eq!(ALPHABET.len(), 32);
        let mut sorted = ALPHABET.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), 32);
        for excluded in [b'i', b'l', b'o', b'u'] {
            assert!(!ALPHABET.contains(&excluded));
        }
    }

    #[test]
    fn parses_both_kinds_and_formats_canonically() {
        let module = ItemId::parse(MODULE).unwrap();
        assert_eq!(module.kind(), ItemKind::OntologyModule);
        assert_eq!(module.as_str(), MODULE);
        assert_eq!(module.to_string(), MODULE);
        assert_eq!(
            module.to_uri(),
            "https://graphforge.sh/ont_7k2m9x4q8r3t5v1w"
        );
        let bridge = ItemId::parse("brg_0000000000000000").unwrap();
        assert_eq!(bridge.kind(), ItemKind::BridgeSet);
        assert_eq!(bridge.kind().field(), "bridge_id");
        assert_eq!(ItemKind::OntologyModule.field(), "ontology_id");
    }

    #[test]
    fn input_is_case_insensitive_and_canonicalized_to_lowercase() {
        for input in [
            "ONT_7K2M9X4Q8R3T5V1W",
            "Ont_7k2M9x4Q8r3T5v1W",
            "HTTPS://GRAPHFORGE.SH/ONT_7K2M9X4Q8R3T5V1W",
            "https://graphforge.sh/ont_7k2m9x4q8r3t5v1w",
        ] {
            let id = ItemId::parse_reference(input).unwrap();
            assert_eq!(id.as_str(), MODULE, "{input}");
            assert_eq!(input.parse::<ItemId>().unwrap(), id);
        }
    }

    #[test]
    fn rejects_malformed_ids_with_a_specific_reason() {
        let cases = [
            ("", ItemIdError::Length),
            ("ont_7k2m9x4q8r3t5v1", ItemIdError::Length),
            ("ont_7k2m9x4q8r3t5v1wx", ItemIdError::Length),
            ("ont-7k2m9x4q8r3t5v1w", ItemIdError::MissingSeparator),
            ("onto7k2m9x4q8r3t5v1w", ItemIdError::MissingSeparator),
            ("prj_7k2m9x4q8r3t5v1w", ItemIdError::UnknownPrefix),
            ("on1_7k2m9x4q8r3t5v1w", ItemIdError::UnknownPrefix),
            ("ont_7k2m9x4q8r3t5v1i", ItemIdError::InvalidCharacter),
            ("ont_7k2m9x4q8r3t5v1l", ItemIdError::InvalidCharacter),
            ("ont_7k2m9x4q8r3t5v1o", ItemIdError::InvalidCharacter),
            ("ont_7k2m9x4q8r3t5v1u", ItemIdError::InvalidCharacter),
            ("ont_7k2m9x4q8r3t5v1_", ItemIdError::InvalidCharacter),
            ("ont_7k2m9x4q8r3t5v1é", ItemIdError::Length),
            ("ont_7k2m9x4q8r3t5vé", ItemIdError::InvalidCharacter),
        ];
        for (input, expected) in cases {
            assert_eq!(ItemId::parse(input), Err(expected), "{input:?}");
        }
    }

    #[test]
    fn uri_form_is_exactly_the_graphforge_base_and_the_id() {
        for input in [
            "http://graphforge.sh/ont_7k2m9x4q8r3t5v1w",
            "https://graphforge.sh:443/ont_7k2m9x4q8r3t5v1w",
            "https://www.graphforge.sh/ont_7k2m9x4q8r3t5v1w",
            "https://graphforge.sh/ontology/ont_7k2m9x4q8r3t5v1w",
            "https://graphforge.sh/owner/ont_7k2m9x4q8r3t5v1w",
            "https://graphforge.sh/ont_7k2m9x4q8r3t5v1w/",
            "https://graphforge.sh/ont_7k2m9x4q8r3t5v1w/2026.01",
            "https://graphforge.sh/ont_7k2m9x4q8r3t5v1w?x=1",
            "https://graphforge.sh/ont_7k2m9x4q8r3t5v1w#frag",
            "https://example.org/ontology/works",
            "graphforge.sh/ont_7k2m9x4q8r3t5v1w",
            "https://graphforge.sé",
            "https:",
        ] {
            assert!(ItemId::parse_reference(input).is_err(), "{input}");
        }
        assert_eq!(
            ItemId::parse_uri("https://graphforge.sh/prj_7k2m9x4q8r3t5v1w"),
            Err(ItemIdError::UnknownPrefix)
        );
        assert_eq!(
            ItemId::parse_uri(MODULE),
            Err(ItemIdError::NotGraphForgeUri)
        );
    }

    #[test]
    fn kind_is_enforced_when_the_caller_requires_one() {
        assert!(ItemId::parse_kind(MODULE, ItemKind::OntologyModule).is_ok());
        assert_eq!(
            ItemId::parse_kind(MODULE, ItemKind::BridgeSet),
            Err(ItemIdError::KindMismatch {
                expected: ItemKind::BridgeSet,
                found: ItemKind::OntologyModule,
            })
        );
        assert!(
            ItemId::parse_kind(
                "https://graphforge.sh/brg_7k2m9x4q8r3t5v1w",
                ItemKind::BridgeSet
            )
            .is_ok()
        );
    }

    #[test]
    fn random_bytes_encode_big_endian_five_bits_per_character() {
        assert_eq!(
            ItemId::from_random_bytes(ItemKind::OntologyModule, [0; 10]).as_str(),
            "ont_0000000000000000"
        );
        assert_eq!(
            ItemId::from_random_bytes(ItemKind::BridgeSet, [0xff; 10]).as_str(),
            "brg_zzzzzzzzzzzzzzzz"
        );
        let mut low = [0; 10];
        low[9] = 1;
        assert_eq!(
            ItemId::from_random_bytes(ItemKind::OntologyModule, low).as_str(),
            "ont_0000000000000001"
        );
        let mut high = [0; 10];
        high[0] = 0x80;
        assert_eq!(
            ItemId::from_random_bytes(ItemKind::OntologyModule, high).as_str(),
            "ont_g000000000000000"
        );
        // Every random value round-trips through the parser.
        let sample = [0x5a, 0x0f, 0xc3, 0x99, 0x01, 0xfe, 0x42, 0x7d, 0xb6, 0x2c];
        let id = ItemId::from_random_bytes(ItemKind::OntologyModule, sample);
        assert_eq!(ItemId::parse(id.as_str()).unwrap(), id);
        assert!(matches_regex(id.as_str()));
    }

    #[test]
    fn id_shape_matches_the_published_expression() {
        for name in [
            MODULE,
            "brg_zzzzzzzzzzzzzzzz",
            "abc_0000000000000000",
            "ONT_7K2M9X4Q8R3T5V1W",
            "ont_7k2m9x4q8r3t5v1",
            "ont_7k2m9x4q8r3t5v1o",
            "on_7k2m9x4q8r3t5v1wx",
            "my_repository_name",
            "opendata",
            "",
        ] {
            assert_eq!(has_item_id_shape(name), matches_regex(name), "{name}");
        }
        assert!(has_item_id_shape("abc_0000000000000000"));
        assert!(!has_item_id_shape("ONT_7K2M9X4Q8R3T5V1W"));
    }

    #[cfg(all(feature = "generate", not(target_arch = "wasm32")))]
    #[test]
    fn generated_ids_are_valid_distinct_and_of_the_requested_kind() {
        let mut seen = std::collections::BTreeSet::new();
        for kind in ItemKind::ALL {
            for _ in 0..256 {
                let id = ItemId::generate(kind).unwrap();
                assert_eq!(id.kind(), kind);
                assert!(matches_regex(id.as_str()), "{id}");
                assert_eq!(ItemId::parse(id.as_str()).unwrap(), id);
                assert!(seen.insert(id));
            }
        }
    }
}
