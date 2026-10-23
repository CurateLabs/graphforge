//! Stable publish error classification and its HTTP projection.

use graphforge_discovery::{ItemId, ItemKind, RepositoryIdentity};
use serde::{Deserialize, Serialize};
use std::fmt;

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
    /// New content uses a permanent item ID owned by another publisher.
    IdTaken,
    /// Same operation identity with a conflicting request.
    IdempotencyConflict,
    /// Ref advance rejected by an expected-revision precondition.
    RefConflict,
    /// Existing Branch head is not an ancestor of the proposed head.
    NonFastForward,
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
        Self::IdTaken,
        Self::IdempotencyConflict,
        Self::RefConflict,
        Self::NonFastForward,
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
            Self::IdTaken => "id_taken",
            Self::IdempotencyConflict => "idempotency_conflict",
            Self::RefConflict => "ref_conflict",
            Self::NonFastForward => "non_fast_forward",
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
            HubPublishErrorCode::IdTaken => "hub.publish.id_taken",
            HubPublishErrorCode::IdempotencyConflict => "hub.publish.idempotency_conflict",
            HubPublishErrorCode::RefConflict => "hub.publish.ref_conflict",
            HubPublishErrorCode::NonFastForward => "hub.publish.non_fast_forward",
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
            message: self.detail.to_owned(),
            items: self.items.clone(),
        }
    }

    /// Refuse foreign permanent IDs with a fixed message and sorted safe hints.
    pub fn id_taken(mut items: Vec<IdTakenItem>) -> Result<Self, HubPublishError> {
        items.sort_by(|a, b| a.id.cmp(&b.id));
        validate_items(&items)?;
        Ok(Self {
            code: HubPublishErrorCode::IdTaken,
            detail: "permanent item ID is owned by another publisher",
            items: Some(items),
        })
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

/// Privacy-filtered hint for one refused permanent item ID.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdTakenItem {
    /// Kind of the permanent ID.
    pub kind: ItemKind,
    /// Canonical lowercase bare permanent ID.
    pub id: String,
    /// Owning handle, only when the caller can see this item.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    /// Present as `true` only when the caller may publish as the owning organization.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publish_as_owner: Option<bool>,
}

impl IdTakenItem {
    /// Validate a hint before retaining it or interpolating it into local text.
    pub fn validate(&self) -> Result<(), HubPublishError> {
        if !ItemId::parse_kind(&self.id, self.kind).is_ok_and(|id| id.as_str() == self.id)
            || self.publish_as_owner == Some(false)
            || self.owner.as_ref().is_some_and(|owner| {
                RepositoryIdentity {
                    owner: owner.clone(),
                    repository: "item".into(),
                }
                .validate()
                .is_err()
            })
        {
            return Err(invalid("id_taken item is malformed"));
        }
        Ok(())
    }
}

fn validate_items(items: &[IdTakenItem]) -> Result<(), HubPublishError> {
    if items.is_empty()
        || items.len() > graphforge_discovery::DiscoveryLimits::default().max_ontology_entries
        || items.windows(2).any(|pair| pair[0].id >= pair[1].id)
    {
        return Err(invalid(
            "id_taken items are not strictly ordered or bounded",
        ));
    }
    items.iter().try_for_each(IdTakenItem::validate)
}

/// JSON publish failure. OAuth endpoints retain their RFC 6749 error bodies.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, try_from = "UncheckedHubErrorBody")]
pub struct HubErrorBody {
    /// Stable machine-readable code.
    pub code: HubPublishErrorCode,
    /// Fixed Hub diagnostic. Clients render their own text and never branch on it.
    pub message: String,
    /// Optional ordered permanent-ID refusals; present only for `id_taken`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub items: Option<Vec<IdTakenItem>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UncheckedHubErrorBody {
    code: HubPublishErrorCode,
    message: String,
    #[serde(default, deserialize_with = "non_null_items")]
    items: Option<Vec<IdTakenItem>>,
}

fn non_null_items<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Vec<IdTakenItem>>, D::Error> {
    Vec::deserialize(deserializer).map(Some)
}

impl TryFrom<UncheckedHubErrorBody> for HubErrorBody {
    type Error = HubPublishError;
    fn try_from(body: UncheckedHubErrorBody) -> Result<Self, Self::Error> {
        if let Some(items) = &body.items {
            if body.code != HubPublishErrorCode::IdTaken {
                return Err(invalid("items are only valid for id_taken"));
            }
            validate_items(items)?;
        }
        Ok(Self {
            code: body.code,
            message: body.message,
            items: body.items,
        })
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

    #[test]
    fn id_taken_body_is_bounded_ordered_and_safe_to_render() {
        let valid = serde_json::json!({"code":"id_taken","message":"ignored remote text","items":[
            {"kind":"bridge_set","id":"brg_aaaaaaaaaaaaaaaa"},
            {"kind":"ontology_module","id":"ont_aaaaaaaaaaaaaaaa","owner":"org","publish_as_owner":true}
        ]});
        let body: HubErrorBody = serde_json::from_value(valid.clone()).unwrap();
        assert_eq!(body.items.as_ref().unwrap()[0].owner, None);
        assert_eq!(body.code.http_status(), 409);
        assert_eq!(
            HubPublishError::new(body.code, "").gf_code(),
            "hub.publish.id_taken"
        );
        for mutation in [
            "wrong_code",
            "unsorted",
            "duplicate",
            "kind",
            "id",
            "owner",
            "false_hint",
            "empty",
            "null",
        ] {
            let mut bad = valid.clone();
            match mutation {
                "wrong_code" => bad["code"] = serde_json::json!("invalid_input"),
                "unsorted" => bad["items"].as_array_mut().unwrap().reverse(),
                "duplicate" => bad["items"][1] = bad["items"][0].clone(),
                "kind" => bad["items"][0]["kind"] = serde_json::json!("ontology_module"),
                "id" => bad["items"][0]["id"] = serde_json::json!("brg_AAAAAAAAAAAAAAAA"),
                "owner" => bad["items"][1]["owner"] = serde_json::json!("org\nSECRET"),
                "false_hint" => bad["items"][1]["publish_as_owner"] = serde_json::json!(false),
                "empty" => bad["items"] = serde_json::json!([]),
                "null" => bad["items"] = serde_json::Value::Null,
                _ => unreachable!(),
            }
            assert!(
                serde_json::from_value::<HubErrorBody>(bad).is_err(),
                "{mutation}"
            );
        }
        let no_items: HubErrorBody =
            serde_json::from_value(serde_json::json!({"code":"id_taken","message":"fixed"}))
                .unwrap();
        assert!(no_items.items.is_none());
        for code in HubPublishErrorCode::ALL
            .into_iter()
            .filter(|code| *code != HubPublishErrorCode::IdTaken)
        {
            assert!(
                serde_json::to_value(HubPublishError::new(code, "fixed").body())
                    .unwrap()
                    .get("items")
                    .is_none()
            );
        }
    }
}
