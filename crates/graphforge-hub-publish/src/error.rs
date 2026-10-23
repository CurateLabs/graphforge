//! Stable publish error classification and its HTTP projection.

use graphforge_discovery::{ItemId, ItemKind, RepositoryIdentity};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Fixed refusal text; item metadata is carried separately from this message.
pub const ID_TAKEN_MESSAGE: &str = "permanent item ID is already owned";
/// The discovery inventory allows 4096 modules and 4096 bridge sets.
pub const MAX_ID_TAKEN_ITEMS: usize = 8192;

fn present<'de, D: serde::Deserializer<'de>, T: Deserialize<'de>>(
    deserializer: D,
) -> Result<Option<T>, D::Error> {
    T::deserialize(deserializer).map(Some)
}

/// GraphForge-wide idempotency conflict code for changed content under a stable operation identity.
pub const GF_IDEMPOTENCY_CONFLICT: &str = "GF_IDEMPOTENCY_CONFLICT";

/// Stable machine-readable publish failure classification.
///
/// The serialized form (`auth_denied`, ...) is the `code` member of every Hub
/// error body.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HubPublishErrorCode {
    /// Publish credential missing, expired, or not scoped to the repository.
    AuthDenied,
    /// Entitlement or quota denied publication.
    EntitlementDenied,
    /// Same operation identity with a conflicting request.
    IdempotencyConflict,
    /// Ref advance rejected by an expected-revision precondition.
    RefConflict,
    /// Existing Branch head is not an ancestor of the proposed head.
    NonFastForward,
    /// New content uses a permanent item ID owned by another publisher.
    IdTaken,
    /// Required future protocol format or capability is unsupported.
    UnsupportedFuture,
    /// Transported or stored bytes disagree with the declared digest or length.
    IntegrityFailure,
    /// Input failed structural validation.
    InvalidInput,
    /// Transport or Hub internal failure.
    Internal,
}

impl HubPublishErrorCode {
    /// Every code, in declaration order.
    pub const ALL: [Self; 10] = [
        Self::AuthDenied,
        Self::EntitlementDenied,
        Self::IdempotencyConflict,
        Self::RefConflict,
        Self::NonFastForward,
        Self::IdTaken,
        Self::UnsupportedFuture,
        Self::IntegrityFailure,
        Self::InvalidInput,
        Self::Internal,
    ];

    /// Snake-case token used as the `code` member of Hub error bodies.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AuthDenied => "auth_denied",
            Self::EntitlementDenied => "entitlement_denied",
            Self::IdempotencyConflict => "idempotency_conflict",
            Self::RefConflict => "ref_conflict",
            Self::NonFastForward => "non_fast_forward",
            Self::IdTaken => "id_taken",
            Self::UnsupportedFuture => "unsupported_future",
            Self::IntegrityFailure => "integrity_failure",
            Self::InvalidInput => "invalid_input",
            Self::Internal => "internal",
        }
    }

    /// Default HTTP status for this code.
    ///
    /// The mapping in ADR 0053 lists the few request-specific overrides
    /// (`404` unknown resource, `413` declared size over a limit, `416` upload
    /// offset mismatch, `403` insufficient token scope).
    #[must_use]
    pub const fn http_status(self) -> u16 {
        match self {
            Self::AuthDenied => 401,
            Self::EntitlementDenied => 403,
            Self::IdempotencyConflict | Self::NonFastForward | Self::IdTaken => 409,
            Self::RefConflict => 412,
            Self::UnsupportedFuture | Self::IntegrityFailure => 422,
            Self::InvalidInput => 400,
            Self::Internal => 500,
        }
    }
}

/// Sanitized publish error suitable for CLI, Hub, and telemetry projection.
///
/// `detail` is always a static string chosen by GraphForge code, never request
/// input, so it can never echo a token or an upload URL.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HubPublishError {
    /// Stable machine-readable code.
    pub code: HubPublishErrorCode,
    detail: &'static str,
    items: Option<Vec<IdTakenItem>>,
}

impl HubPublishError {
    /// Construct a publish error with a stable detail string (never untrusted input).
    #[must_use]
    pub const fn new(code: HubPublishErrorCode, detail: &'static str) -> Self {
        Self {
            code,
            detail,
            items: None,
        }
    }

    /// Construct one privacy-filtered permanent-ID refusal.
    pub fn id_taken(items: Vec<IdTakenItem>) -> Result<Self, Self> {
        let body = HubErrorBody {
            code: HubPublishErrorCode::IdTaken,
            message: ID_TAKEN_MESSAGE.into(),
            items: Some(items.clone()),
        };
        body.validate()?;
        Ok(Self {
            code: body.code,
            detail: ID_TAKEN_MESSAGE,
            items: Some(items),
        })
    }

    /// Sanitized, non-input-bearing diagnostic text.
    #[must_use]
    pub const fn detail(&self) -> &'static str {
        self.detail
    }

    /// Wire-level token used in logs and telemetry (`hub.publish.*`).
    #[must_use]
    pub const fn wire_code(&self) -> &'static str {
        match self.code {
            HubPublishErrorCode::AuthDenied => "hub.publish.auth_denied",
            HubPublishErrorCode::EntitlementDenied => "hub.publish.entitlement_denied",
            HubPublishErrorCode::IdempotencyConflict => "hub.publish.idempotency_conflict",
            HubPublishErrorCode::RefConflict => "hub.publish.ref_conflict",
            HubPublishErrorCode::NonFastForward => "hub.publish.non_fast_forward",
            HubPublishErrorCode::IdTaken => "hub.publish.id_taken",
            HubPublishErrorCode::UnsupportedFuture => "hub.publish.unsupported_future",
            HubPublishErrorCode::IntegrityFailure => "hub.publish.integrity_failure",
            HubPublishErrorCode::InvalidInput => "hub.publish.invalid_input",
            HubPublishErrorCode::Internal => "hub.publish.internal",
        }
    }

    /// Native GraphForge error code projected to callers.
    ///
    /// Idempotency conflicts use the GraphForge-wide [`GF_IDEMPOTENCY_CONFLICT`];
    /// every other code projects its wire token unchanged.
    #[must_use]
    pub const fn gf_code(&self) -> &'static str {
        match self.code {
            HubPublishErrorCode::IdempotencyConflict => GF_IDEMPOTENCY_CONFLICT,
            _ => self.wire_code(),
        }
    }

    /// Hub error body for this error.
    #[must_use]
    pub fn body(&self) -> HubErrorBody {
        HubErrorBody {
            code: self.code,
            message: if self.code == HubPublishErrorCode::IdTaken {
                ID_TAKEN_MESSAGE
            } else {
                self.detail
            }
            .to_owned(),
            items: self.items.clone(),
        }
    }
}

impl fmt::Display for HubPublishError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} ({}): {}",
            self.gf_code(),
            self.wire_code(),
            self.detail
        )
    }
}

impl std::error::Error for HubPublishError {}

/// One refused canonical permanent ID, with only caller-visible ownership facts.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct IdTakenItem {
    /// Item kind, which must match the ID prefix.
    pub kind: ItemKind,
    /// Canonical lowercase bare ID, never an owner URL or own-domain IRI.
    pub id: String,
    /// Owning handle, only when the caller can see this item.
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    #[cfg_attr(feature = "typescript", tsify(optional))]
    pub owner: Option<String>,
    /// Present only as `true` for authorized publication as the owning organization.
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    #[cfg_attr(feature = "typescript", tsify(optional))]
    pub publish_as_owner: Option<bool>,
}

impl IdTakenItem {
    /// Validate the shared permanent-ID grammar and safe, bounded ownership facts.
    pub fn validate(&self) -> Result<(), HubPublishError> {
        let id = ItemId::parse(&self.id).map_err(|_| invalid("refused item ID is invalid"))?;
        if id.as_str() != self.id || id.kind() != self.kind {
            return Err(invalid(
                "refused item ID is not canonical or has the wrong kind",
            ));
        }
        if let Some(owner) = &self.owner {
            RepositoryIdentity {
                owner: owner.clone(),
                repository: "item".into(),
            }
            .validate()
            .map_err(|_| invalid("refused item owner is invalid"))?;
        }
        if self.publish_as_owner == Some(false) {
            return Err(invalid("publish_as_owner must be true when present"));
        }
        Ok(())
    }
}

/// JSON error body `{code, message, items?}` returned by every Hub publish endpoint
/// except the OAuth token endpoint (which uses RFC 6749 error bodies).
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "UncheckedHubErrorBody")]
pub struct HubErrorBody {
    /// Stable machine-readable code.
    pub code: HubPublishErrorCode,
    /// Fixed server text. Clients render their own guidance and never echo it.
    pub message: String,
    /// Sorted unique refused IDs, permitted only for `id_taken`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub items: Option<Vec<IdTakenItem>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UncheckedHubErrorBody {
    code: HubPublishErrorCode,
    message: String,
    #[serde(default, deserialize_with = "present")]
    items: Option<Vec<IdTakenItem>>,
}

impl TryFrom<UncheckedHubErrorBody> for HubErrorBody {
    type Error = HubPublishError;
    fn try_from(raw: UncheckedHubErrorBody) -> Result<Self, Self::Error> {
        let body = Self {
            code: raw.code,
            message: raw.message,
            items: raw.items,
        };
        body.validate()?;
        Ok(body)
    }
}

impl HubErrorBody {
    /// Validate refusal details without trusting server-provided prose.
    pub fn validate(&self) -> Result<(), HubPublishError> {
        if let Some(items) = &self.items {
            if self.code != HubPublishErrorCode::IdTaken
                || items.is_empty()
                || items.len() > MAX_ID_TAKEN_ITEMS
            {
                return Err(invalid("refusal items are not valid for this error"));
            }
            let mut prior: Option<&str> = None;
            for item in items {
                item.validate()?;
                if prior.is_some_and(|prior| prior >= item.id.as_str()) {
                    return Err(invalid("refused items must be sorted and unique"));
                }
                prior = Some(&item.id);
            }
        }
        Ok(())
    }
}

pub(crate) const fn invalid(detail: &'static str) -> HubPublishError {
    HubPublishError::new(HubPublishErrorCode::InvalidInput, detail)
}

pub(crate) const fn unsupported(detail: &'static str) -> HubPublishError {
    HubPublishError::new(HubPublishErrorCode::UnsupportedFuture, detail)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idempotency_conflict_projects_the_graphforge_code() {
        let error = HubPublishError::new(HubPublishErrorCode::IdempotencyConflict, "conflict");
        assert_eq!(error.gf_code(), GF_IDEMPOTENCY_CONFLICT);
        assert_eq!(error.wire_code(), "hub.publish.idempotency_conflict");
    }

    #[test]
    fn serialized_code_equals_the_documented_token() {
        for code in HubPublishErrorCode::ALL {
            assert_eq!(
                serde_json::to_value(code).unwrap(),
                serde_json::Value::String(code.as_str().to_owned())
            );
            let error = HubPublishError::new(code, "x");
            assert_eq!(error.wire_code(), format!("hub.publish.{}", code.as_str()));
        }
    }
}
