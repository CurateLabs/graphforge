//! Typed refusal capture across immediate commit and asynchronous polling.
use super::{
    BTreeSet, GfError, GraphForge, HubErrorBody, HubPublishErrorCode, HubResponse, PublishToken,
    Uuid, network, publish_error,
};
use graphforge_discovery::{ItemId, ItemKind};
use graphforge_hub_publish::IdTakenItem;
use std::cell::{Cell, RefCell};

#[derive(Default)]
pub(super) struct Capture {
    version: Cell<Option<Uuid>>,
    items: RefCell<Option<Vec<IdTakenItem>>>,
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
        let Ok(body) = serde_json::from_slice::<HubErrorBody>(&response.body) else {
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
            self.items.replace(Some(items));
        }
        Ok(())
    }

    pub(super) fn take(&self) -> Option<(Uuid, Vec<IdTakenItem>)> {
        Some((self.version.get()?, self.items.take()?))
    }
}

pub(super) fn record(graph: &GraphForge, version: Uuid, items: &[IdTakenItem]) -> GfError {
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
