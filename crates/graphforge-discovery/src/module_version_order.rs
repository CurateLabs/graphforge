//! Declared ontology precedence, independent of opaque authored version labels.

use crate::input_budget::InputBudget;
use crate::{
    DiscoveryError, DiscoveryErrorCode, DiscoveryLimits, ExactIdentity, ItemId, ItemKind,
    check_response_bound, check_string, parse_unique_json,
};
use serde::{Deserialize, Serialize};

/// One exact ontology version and its authenticated author's declaration.
///
/// Parsing proves structure only. Callers must bind the declaration to verified
/// module bytes before using the result as an authoritative comparison.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct DeclaredModuleVersion {
    /// Exact identity with a canonical permanent ontology URI.
    pub module: ExactIdentity,
    /// Optional author-declared precedence; no declaration means unordered.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "typescript", tsify(optional))]
    pub version_order: Option<u32>,
}

impl DeclaredModuleVersion {
    /// Parse bounded, closed JSON without duplicate members.
    pub fn from_json(bytes: &[u8], limits: DiscoveryLimits) -> Result<Self, DiscoveryError> {
        check_response_bound(bytes, limits)?;
        let declaration: Self = parse_unique_json(bytes)?;
        declaration.validate(limits)?;
        Ok(declaration)
    }

    fn validate(&self, limits: DiscoveryLimits) -> Result<(), DiscoveryError> {
        check_string(&self.module.id, "module.id", limits)?;
        check_string(&self.module.version, "module.version", limits)?;
        if !ItemId::parse_uri(&self.module.id)
            .is_ok_and(|id| id.kind() == ItemKind::OntologyModule && id.to_uri() == self.module.id)
        {
            return Err(invalid_identity());
        }
        self.module.content_digest.validate()
    }
}

/// Precedence only: equal ordinals do not establish equal module identities.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(rename_all = "snake_case")]
pub enum ModuleVersionOrdering {
    /// The first declaration precedes the second.
    Less,
    /// Equal precedence, or the same exact identity and declaration.
    Equal,
    /// The first declaration follows the second.
    Greater,
    /// Distinct exact identities with at least one missing declaration.
    Unordered,
}

/// Compare declared precedence within one ontology ID, never authored labels.
///
/// The same exact identity must carry the same declaration, including absence.
/// An equal ordinal on distinct identities means equal precedence only.
pub fn compare_module_versions(
    a: &DeclaredModuleVersion,
    b: &DeclaredModuleVersion,
    limits: DiscoveryLimits,
) -> Result<ModuleVersionOrdering, DiscoveryError> {
    let mut budget = InputBudget::new(limits.max_response_bytes, "module_versions");
    budget.account(a)?;
    budget.account(b)?;
    a.validate(limits)?;
    b.validate(limits)?;
    if a.module.id != b.module.id {
        return Err(invalid_identity());
    }
    if a.module == b.module {
        if a.version_order != b.version_order {
            return Err(DiscoveryError::new(
                DiscoveryErrorCode::IntegrityFailure,
                Some("version_order"),
                "same exact module identity has contradictory version order",
            ));
        }
        return Ok(ModuleVersionOrdering::Equal);
    }
    Ok(match (a.version_order, b.version_order) {
        (Some(a), Some(b)) => match a.cmp(&b) {
            std::cmp::Ordering::Less => ModuleVersionOrdering::Less,
            std::cmp::Ordering::Equal => ModuleVersionOrdering::Equal,
            std::cmp::Ordering::Greater => ModuleVersionOrdering::Greater,
        },
        _ => ModuleVersionOrdering::Unordered,
    })
}

/// Duplicate-aware JSON comparison, bounded across both input documents.
pub fn compare_module_versions_json(
    a: &[u8],
    b: &[u8],
    limits: DiscoveryLimits,
) -> Result<ModuleVersionOrdering, DiscoveryError> {
    let mut budget = InputBudget::new(limits.max_response_bytes, "module_versions");
    budget.account_bytes(a)?;
    budget.account_bytes(b)?;
    let a = DeclaredModuleVersion::from_json(a, limits)?;
    let b = DeclaredModuleVersion::from_json(b, limits)?;
    compare_module_versions(&a, &b, limits)
}

fn invalid_identity() -> DiscoveryError {
    DiscoveryError::new(
        DiscoveryErrorCode::InvalidIdentity,
        Some("module.id"),
        "version comparison requires the same canonical ontology ID",
    )
}
