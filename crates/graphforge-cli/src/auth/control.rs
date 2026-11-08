//! Fixed, same-origin native control channel. Credentials never leave auth.rs.

use super::{
    GfError, HubMethod, HubRequest, PreviewTransport, SavedCredentialError, Store, Transport, Url,
    Vault, denied, now, origin, session_lookup, validation,
};
use graphforge_discovery::hub::{
    HUB_CONTROL_PATH, HubControlOperation, HubControlRequest, HubControlResponse, HubControlResult,
    MAX_HUB_RESPONSE_BYTES,
};

pub(crate) fn exchange_control(
    hub: &str,
    bypass_env: Option<&str>,
    request: &HubControlRequest,
    anonymous: bool,
) -> Result<HubControlResponse, GfError> {
    let hub = origin(hub)?;
    let transport = PreviewTransport::new(&hub, bypass_env)?;
    exchange_control_from(
        &Vault,
        &transport,
        &hub,
        request,
        anonymous,
        crate::process_attached(),
        now(),
    )
}

pub(super) fn exchange_control_from(
    store: &dyn Store,
    transport: &dyn Transport,
    hub: &Url,
    request: &HubControlRequest,
    anonymous: bool,
    attached: bool,
    now: u64,
) -> Result<HubControlResponse, GfError> {
    // Validate all input and cross-origin/expired review state before touching
    // credentials or making a network request.
    let hub = origin(&hub.origin().ascii_serialization())?;
    let bytes = request.to_json().map_err(|_| invalid_control())?;
    if let HubControlOperation::Apply { plan } = &request.operation {
        if plan.hub_origin != hub.origin().ascii_serialization() {
            return Err(validation(
                "hub.control_origin_mismatch",
                "reviewed operation belongs to another Hub; prepare and review it on the selected Hub",
            ));
        }
        if plan.expires_at_ms <= now.saturating_mul(1000) {
            return Err(validation(
                "hub.control_plan_expired",
                "reviewed operation expired; prepare and review a fresh plan",
            ));
        }
    }
    if request.requires_session() && (anonymous || !attached) {
        return Err(denied(
            "this operation requires a saved human session on this host; run gf auth login in the native CLI process",
        ));
    }
    let saved = if anonymous
        || !attached
        || matches!(request.operation, HubControlOperation::Capabilities {})
    {
        None
    } else {
        match session_lookup(store, transport, &hub, now) {
            Ok(saved) => saved,
            Err(SavedCredentialError::Unavailable) if !request.requires_session() => None,
            Err(error) => return Err(error.into()),
        }
    };
    if saved.is_none() && request.requires_session() {
        return Err(denied(
            "no saved human session on this host; run gf auth login for the selected Hub",
        ));
    }
    let authenticated = saved.is_some();
    let mut wire = HubRequest::new(
        HubMethod::Post,
        hub.join(HUB_CONTROL_PATH)
            .map_err(|_| invalid_control())?
            .as_str(),
    )
    .with_header("content-type", "application/json")
    .with_header("accept", "application/json")
    .with_body(bytes);
    if let Some(saved) = saved {
        wire = wire.with_header("authorization", format!("Bearer {}", saved.access_token));
    }
    // No automatic retries: interruption of apply requires status reconciliation
    // using the same operation UUID and commitment, not a second mutation.
    transport.validate(&Url::parse(&wire.url).map_err(|_| invalid_control())?)?;
    let response = transport
        .exchange(&wire, MAX_HUB_RESPONSE_BYTES)
        .map_err(|_| unavailable(request))?;
    if (300..400).contains(&response.status) {
        return Err(unavailable(request));
    }
    let result = HubControlResponse::from_json(&response.body, request).map_err(|_| {
        if matches!(request.operation, HubControlOperation::Apply { .. }) {
            unavailable(request)
        } else {
            invalid_control()
        }
    })?;
    if !authenticated
        && let HubControlResult::Inspect { record, .. } = &result.result
        && (record.visibility != graphforge_discovery::hub::HubVisibility::Public
            || record.account_id.is_some())
    {
        return Err(invalid_control());
    }
    if response.status != 200 && !matches!(result.result, HubControlResult::Failure { .. }) {
        return Err(unavailable(request));
    }
    if let HubControlResult::Prepared { plan } = &result.result
        && (plan.hub_origin != hub.origin().ascii_serialization()
            || plan.expires_at_ms <= now.saturating_mul(1000))
    {
        return Err(invalid_control());
    }
    Ok(result)
}

fn invalid_control() -> GfError {
    validation(
        "hub.control_invalid",
        "Hub control document is invalid or unsupported; refresh native capabilities and retry",
    )
}
fn unavailable(request: &HubControlRequest) -> GfError {
    if matches!(request.operation, HubControlOperation::Apply { .. }) {
        validation(
            "hub.control_outcome_uncertain",
            "operation outcome is uncertain; request status with the reviewed operation UUID and intent digest before retrying",
        )
    } else {
        validation(
            "hub.control_unavailable",
            "Hub control channel is unavailable; preserve the current selection and retry the same read or preparation",
        )
    }
}
