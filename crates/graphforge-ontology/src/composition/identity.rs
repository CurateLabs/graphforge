//! Stable identity types for composable ontology inventories.

use serde::{Deserialize, Serialize};

/// Domain separation prefix for module document digests.
pub const MODULE_DIGEST_DOMAIN: &[u8] = b"graphforge-ontology-module/1\0";

/// Domain separation prefix for composition fingerprints (ADR 0023).
pub const COMPOSITION_DOMAIN: &[u8] = b"graphforge-ontology-composition/1\0";

/// Domain separation prefix for bridge-set document digests (#838).
pub const BRIDGE_DIGEST_DOMAIN: &[u8] = b"graphforge-ontology-bridge/1\0";

/// Hex-encoded SHA-256 digest length.
pub const DIGEST_HEX_LEN: usize = 64;

/// Exact ontology module identity: `{ ontology_id, authored_version, canonical_digest }`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OntologyModuleId {
    /// Globally unique, NFC-normalized URI.
    pub ontology_id: String,
    /// Opaque NFC version string owned by the ontology's version scheme.
    pub authored_version: String,
    /// Lowercase SHA-256 hex digest of the domain-separated canonical module document.
    pub canonical_digest: String,
}

impl OntologyModuleId {
    /// Lexicographic sort key: UTF-8 bytes of `(ontology_id, authored_version, canonical_digest)`.
    #[must_use]
    pub fn sort_key(&self) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
        (
            self.ontology_id.as_bytes().to_vec(),
            self.authored_version.as_bytes().to_vec(),
            self.canonical_digest.as_bytes().to_vec(),
        )
    }

    /// Compact display form used in activation subjects and candidate lists.
    #[must_use]
    pub fn display_ref(&self) -> String {
        format!(
            "{}@{}#{}",
            self.ontology_id, self.authored_version, self.canonical_digest
        )
    }

    /// Short `local_name:Person`-style candidate label for ambiguity receipts.
    #[must_use]
    pub fn short_name(&self) -> &str {
        self.ontology_id
            .rsplit('/')
            .next()
            .unwrap_or(self.ontology_id.as_str())
    }
}

/// Exact bridge-set identity (lifecycle owned by #838; identity used here for fingerprint).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BridgeSetId {
    /// Globally unique, NFC-normalized URI.
    pub bridge_id: String,
    /// Opaque NFC version string.
    pub authored_version: String,
    /// Lowercase SHA-256 hex digest of the canonical bridge document.
    pub canonical_digest: String,
}

impl BridgeSetId {
    /// Lexicographic sort key over identity fields.
    #[must_use]
    pub fn sort_key(&self) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
        (
            self.bridge_id.as_bytes().to_vec(),
            self.authored_version.as_bytes().to_vec(),
            self.canonical_digest.as_bytes().to_vec(),
        )
    }

    /// Compact display form used in subjects and candidate lists.
    #[must_use]
    pub fn display_ref(&self) -> String {
        format!(
            "{}@{}#{}",
            self.bridge_id, self.authored_version, self.canonical_digest
        )
    }
}

/// Symbol kind within a module.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SymbolKind {
    /// Entity / node type.
    Entity,
    /// Relation / edge type.
    Relation,
    /// Property definition.
    Property,
    /// Constraint definition.
    Constraint,
    /// Migration definition.
    Migration,
}

impl SymbolKind {
    /// Stable wire token.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Entity => "entity",
            Self::Relation => "relation",
            Self::Property => "property",
            Self::Constraint => "constraint",
            Self::Migration => "migration",
        }
    }
}

/// Qualified symbol: `{ module, kind, local_id }`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct QualifiedSymbol {
    /// Exact owning module identity.
    pub module: OntologyModuleId,
    /// Symbol kind.
    pub kind: SymbolKind,
    /// NFC-normalized local identifier unique for its kind within the module.
    pub local_id: String,
}

impl QualifiedSymbol {
    /// Stable display form `short:kind:local` / fixture-style `genealogy:entity:Person`.
    #[must_use]
    pub fn display(&self) -> String {
        format!(
            "{}:{}:{}",
            self.module.short_name(),
            self.kind.as_str(),
            self.local_id
        )
    }

    /// Ambiguity candidate label `short:local` matching the contract fixture oracle.
    #[must_use]
    pub fn ambiguity_candidate(&self) -> String {
        format!("{}:{}", self.module.short_name(), self.local_id)
    }
}

/// Progressive enforcement mode for a scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivationMode {
    /// Accept unknown runtime observations into the disjoint RuntimeCatalog.
    Exploratory,
    /// Preserve the operation and emit bounded structured warnings.
    Advisory,
    /// Reject unresolved or violating operations atomically.
    Strict,
}

impl ActivationMode {
    /// Stable wire token.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Exploratory => "exploratory",
            Self::Advisory => "advisory",
            Self::Strict => "strict",
        }
    }
}

/// Activation override scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivationScope {
    /// Exact module subject.
    Module,
    /// Exact bridge subject.
    Bridge,
}

impl ActivationScope {
    /// Stable wire token.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Module => "module",
            Self::Bridge => "bridge",
        }
    }
}

/// Scoped activation override record contributing to composition identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ActivationRecord {
    /// Module or bridge scope.
    pub scope: ActivationScope,
    /// Exact subject identity string (module `display_ref` or bridge equivalent).
    pub subject: String,
    /// Enforcement mode for this subject.
    pub mode: ActivationMode,
}

impl ActivationRecord {
    /// Sort key: UTF-8 bytes of `(scope, subject, mode)`.
    #[must_use]
    pub fn sort_key(&self) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
        (
            self.scope.as_str().as_bytes().to_vec(),
            self.subject.as_bytes().to_vec(),
            self.mode.as_str().as_bytes().to_vec(),
        )
    }
}
