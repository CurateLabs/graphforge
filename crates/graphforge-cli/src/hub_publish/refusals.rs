//! Typed Hub refusals, safe diagnostics, and permanent-ID recovery.
use super::{
    BTreeSet, GfError, GraphForge, HubErrorBody, HubPublishErrorCode, HubResponse, PublishToken,
    Uuid, network, publish_error,
};
use graphforge_discovery::{ItemId, ItemKind};
use graphforge_hub_publish::IdTakenItem;
use std::cell::{Cell, RefCell};

// Refusal prose has its own display budget, inside the existing control-body limit.
const MAX_MESSAGE_BYTES: usize = 4096;

pub(super) fn body(response: &HubResponse) -> Result<HubErrorBody, GfError> {
    if response.body.len() > super::MAX_CONTROL_RESPONSE_BYTES {
        return Err(network("Hub returned an unsuccessful status"));
    }
    let value = super::parse_unique_json(&response.body)
        .map_err(|_| network("Hub returned an unsuccessful status"))?;
    let body: HubErrorBody = serde_json::from_value(value)
        .map_err(|_| network("Hub returned an unsuccessful status"))?;
    if (matches!(
        body.code,
        HubPublishErrorCode::IdTaken | HubPublishErrorCode::VisibilityMismatch
    ) && response.status != 409)
        || (body.reason.is_some() && response.status != body.code.http_status())
    {
        return Err(network("Hub refusal status is invalid"));
    }
    Ok(body)
}

/// Normalize untrusted prose to one bounded terminal-safe line. URLs are removed
/// in full: even a path without a query can be an upload capability. Request
/// credentials are redacted using only values already acquired for that request.
pub(super) fn message(text: &str, secrets: &[&str]) -> String {
    let mut text = text.to_owned();
    for secret in secrets.iter().filter(|secret| !secret.is_empty()) {
        text = text.replace(secret, "[redacted]");
    }
    let text: String = text
        .chars()
        .map(|ch| match ch {
            '\u{200b}'..='\u{200f}'
            | '\u{2028}'..='\u{202e}'
            | '\u{2060}'..='\u{206f}'
            | '\u{feff}' => ' ',
            ch if ch.is_control() => ' ',
            ch => ch,
        })
        .collect();
    let mut bearer = false;
    let mut output = String::new();
    for word in text.split_whitespace() {
        let lower = word.to_ascii_lowercase();
        let is_bearer = lower
            .trim_start_matches(|ch: char| ch.is_ascii_punctuation())
            .starts_with("bearer");
        let redacted = bearer
            || is_bearer
            || word.contains("//")
            || word.contains('@')
            || lower.contains("token=")
            || lower.contains("secret=")
            || lower.contains("signature=");
        let word = if redacted { "[redacted]" } else { word };
        let extra = usize::from(!output.is_empty());
        if output.len() + extra + word.len() > MAX_MESSAGE_BYTES {
            break;
        }
        if extra != 0 {
            output.push(' ');
        }
        output.push_str(word);
        bearer = is_bearer;
    }
    output
}

// Preserve malformed bodies for the existing failure classifier; only validated
// refusal messages may cross into the diagnostic renderer.
pub(super) fn sanitize_response(
    mut response: HubResponse,
    request: &super::HubRequest,
) -> Result<HubResponse, GfError> {
    if let Ok(mut body) = body(&response) {
        let secrets: Vec<&str> = [
            request
                .header("authorization")
                .and_then(|value| value.strip_prefix("Bearer ")),
            request.header("x-vercel-protection-bypass"),
        ]
        .into_iter()
        .flatten()
        .collect();
        body.message = message(&body.message, &secrets);
        response.body =
            serde_json::to_vec(&body).map_err(|_| network("Hub refusal could not be encoded"))?;
    }
    Ok(response)
}

#[derive(Default)]
pub(super) struct Capture {
    version: Cell<Option<Uuid>>,
    items: RefCell<Option<(Vec<IdTakenItem>, String)>>,
}

impl Capture {
    pub(super) fn select(&self, version: Uuid) {
        self.version.set(Some(version));
    }

    pub(super) fn observe(
        &self,
        response: &HubResponse,
        token: &PublishToken,
        bypass: Option<&str>,
    ) -> Result<(), GfError> {
        if response.status != 409 {
            return Ok(());
        }
        let Ok(body) = body(response) else {
            return Ok(());
        };
        if body.code != HubPublishErrorCode::IdTaken {
            return Ok(());
        }
        if let Some(items) = body.items {
            if [Some(token.expose()), bypass]
                .into_iter()
                .flatten()
                .filter(|secret| !secret.is_empty())
                .any(|secret| {
                    items.iter().any(|item| {
                        item.id.contains(secret)
                            || item
                                .owner
                                .as_ref()
                                .is_some_and(|owner| owner.contains(secret))
                    })
                })
            {
                return Err(network("Hub refusal metadata contains credential material"));
            }
            let message = message(&body.message, &[token.expose(), bypass.unwrap_or("")]);
            self.items.replace(Some((items, message)));
        }
        Ok(())
    }

    pub(super) fn take(&self) -> Option<(Uuid, Vec<IdTakenItem>, String)> {
        let version = self.version.get()?;
        let (items, message) = self.items.take()?;
        Some((version, items, message))
    }
}

/// Add the already sanitized Hub guidance to the local recovery outcome. Server
/// prose remains ephemeral; only verified refusal items are written to disk.
pub(super) fn record(
    graph: &GraphForge,
    version: Uuid,
    items: &[IdTakenItem],
    message: &str,
) -> GfError {
    match record_items(graph, version, items) {
        GfError::Validation(detail) => {
            GfError::Validation(format!("{detail}\nHub message: {message}"))
        }
        error => error,
    }
}

fn record_items(graph: &GraphForge, version: Uuid, items: &[IdTakenItem]) -> GfError {
    let inventory = || -> Result<BTreeSet<(ItemKind, String)>, GfError> {
        let mut ids = BTreeSet::new();
        for entry in graph
            .ontology_modules()
            .map_err(|_| network("local item inventory is unavailable"))?
        {
            if let Ok(id) = ItemId::parse_reference(&entry.id.ontology_id) {
                ids.insert((ItemKind::OntologyModule, id.as_str().into()));
            }
        }
        for entry in graph
            .ontology_bridges()
            .map_err(|_| network("local item inventory is unavailable"))?
        {
            if let Ok(id) = ItemId::parse_reference(&entry.id.bridge_id) {
                ids.insert((ItemKind::BridgeSet, id.as_str().into()));
            }
        }
        Ok(ids)
    };
    let Ok(inventory) = inventory() else {
        return publish_error(
            HubPublishErrorCode::IdTaken,
            "the Hub refused permanent IDs; local item inventory is unavailable",
        );
    };
    if items
        .iter()
        .any(|item| !inventory.contains(&(item.kind, item.id.clone())))
    {
        return publish_error(
            HubPublishErrorCode::InvalidInput,
            "Hub refusal names an item outside this publication",
        );
    }
    let receipts = match graph.record_publish_id_taken(version, items) {
        Ok(receipts) => receipts,
        Err(error) => return receipt_failure(&error),
    };
    let guidance = items
        .iter()
        .zip(receipts)
        .map(|(item, receipt)| {
            if let Some(regenerated) = receipt.regenerated {
                return format!(
                    "{} was replaced by {} in Version {}; publish from that regenerated Version",
                    item.id, regenerated.id, regenerated.version_uuid
                );
            }
            let owner = item.owner.as_deref().unwrap_or("the owning organization");
            if item.publish_as_owner == Some(true) {
                format!(
                    "{} is taken; publish as {owner} instead of regenerating",
                    item.id
                )
            } else if item.owner.is_some() {
                format!(
                    "{} is taken; publish as {owner}, or run gf ontology regenerate-id {}",
                    item.id, item.id
                )
            } else {
                format!(
                    "{} is taken; run gf ontology regenerate-id {}",
                    item.id, item.id
                )
            }
        })
        .collect::<Vec<_>>()
        .join("\n");
    publish_error(HubPublishErrorCode::IdTaken, &guidance)
}

fn receipt_failure(error: &GfError) -> GfError {
    let guidance = match error {
        GfError::Api {
            code: graphforge_api::ApiErrorCode::ResourceLimit,
            ..
        } => {
            "the Hub refused permanent IDs; local refusal receipt capacity is full; keep existing replacement records, retry with fewer conflicting IDs or use a separate local Project before regenerating"
        }
        _ => {
            "the Hub refused permanent IDs; the local refusal receipt could not be saved; repair local storage and publish again before regenerating"
        }
    };
    publish_error(HubPublishErrorCode::IdTaken, guidance)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn receipt_capacity_and_storage_failures_have_distinct_sanitized_guidance() {
        for (failure, capacity) in [
            (
                GfError::Api {
                    code: graphforge_api::ApiErrorCode::ResourceLimit,
                    message: "private-error-text".into(),
                },
                true,
            ),
            (GfError::Storage("private-error-text".into()), false),
            (GfError::Validation("private-error-text".into()), false),
        ] {
            let error = receipt_failure(&failure);
            let text = error.to_string();
            assert!(
                matches!(&error, GfError::Validation(message) if message.starts_with("hub.publish.id_taken:"))
            );
            assert_eq!(crate::error_exit_code(&error.into()), 2);
            assert_eq!(text.contains("capacity is full"), capacity);
            assert_eq!(text.contains("repair local storage"), !capacity);
            assert!(!text.contains("private-error-text"));
            if capacity {
                assert!(text.contains("keep existing replacement records"));
                assert!(text.contains("separate local Project"));
            }
        }
    }
}
