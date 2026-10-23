//! Private, authenticated publish refusal and regeneration tracking.
use super::{GfError, ResearchRegistry, Uuid, invalid};
use graphforge_discovery::{ItemId, ItemKind, RepositoryIdentity};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A permanent ID refused by the Hub, with only the hints it disclosed.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishIdTakenItem {
    /// Permanent item kind.
    pub kind: ItemKind,
    /// Canonical lowercase bare permanent ID.
    pub id: String,
    /// Owning handle disclosed by the Hub; absence is preserved.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    /// Owning organization publish permission, when disclosed as true.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publish_as_owner: Option<bool>,
}

/// Per-item refusals from one publication attempt. This is a caller assertion
/// about a remote response, authenticated locally by the project generation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishIdTakenReceipt {
    /// Hub operation whose commit refused these IDs.
    pub publish_operation_uuid: Uuid,
    /// Canonical HTTPS owner/repository URL of that operation.
    pub repository_url: String,
    /// Immutable Version selected for that publication.
    pub version_uuid: Uuid,
    /// Privacy-filtered per-ID receipts in strictly ascending ID order.
    pub items: Vec<PublishIdTakenItem>,
}

/// Identity replacement installed by the owning regeneration operation alongside
/// its new Version. Old immutable Versions and their identities remain intact.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OntologyIdRegeneration {
    /// Research operation that created the replacement Version.
    pub operation_uuid: Uuid,
    /// Kind shared by both permanent IDs.
    pub kind: ItemKind,
    /// Refused canonical lowercase bare ID.
    pub old_id: String,
    /// Newly generated canonical lowercase bare ID.
    pub new_id: String,
    /// Refused immutable Version selected for regeneration.
    pub source_version_uuid: Uuid,
    /// Immutable Version carrying the replacement ID.
    pub version_uuid: Uuid,
}

/// Project-local state; excluded from publication identity and interchange archives.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishIdState {
    /// Refusal records keyed by their local recording operation UUID.
    pub id_taken: BTreeMap<Uuid, PublishIdTakenReceipt>,
    /// Regeneration relations keyed by their creating operation UUID.
    pub regenerations: BTreeMap<Uuid, OntologyIdRegeneration>,
}

fn canonical_id(id: &str, kind: ItemKind) -> bool {
    ItemId::parse_kind(id, kind).is_ok_and(|parsed| parsed.as_str() == id)
}

impl PublishIdTakenReceipt {
    /// Validate bounded canonical receipt fields before accepting caller data.
    pub fn validate(&self) -> Result<(), GfError> {
        let url = url::Url::parse(&self.repository_url)
            .map_err(|_| invalid("refusal repository URL is invalid"))?;
        if self.publish_operation_uuid.is_nil()
            || self.version_uuid.is_nil()
            || self.repository_url.len() > 4096
            || url.scheme() != "https"
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || url.as_str() != self.repository_url
            || RepositoryIdentity::parse(url.path().trim_start_matches('/')).is_err()
            || self.items.is_empty()
            || self.items.len() > 4096
            || self.items.windows(2).any(|pair| pair[0].id >= pair[1].id)
        {
            return Err(invalid("invalid permanent ID refusal receipt"));
        }
        for item in &self.items {
            if !canonical_id(&item.id, item.kind)
                || item.publish_as_owner == Some(false)
                || item.owner.as_ref().is_some_and(|owner| {
                    RepositoryIdentity {
                        owner: owner.clone(),
                        repository: "item".into(),
                    }
                    .validate()
                    .is_err()
                })
            {
                return Err(invalid("invalid permanent ID refusal item"));
            }
        }
        Ok(())
    }
}

impl PublishIdState {
    /// Whether no private tracking state is stored.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.id_taken.is_empty() && self.regenerations.is_empty()
    }

    pub(super) fn validate(&self, registry: &ResearchRegistry) -> Result<(), GfError> {
        if self.id_taken.len() > super::MAX_RECEIPTS
            || self.regenerations.len() > super::MAX_RECEIPTS
        {
            return Err(invalid("permanent ID tracking capacity exceeded"));
        }
        for (operation, refusal) in &self.id_taken {
            refusal.validate()?;
            if operation.is_nil()
                || !registry.receipts.contains_key(operation)
                || !registry.identities.contains_key(&refusal.version_uuid)
            {
                return Err(invalid("permanent ID refusal authority is unavailable"));
            }
        }

        for (operation, relation) in &self.regenerations {
            if operation.is_nil()
                || *operation != relation.operation_uuid
                || !canonical_id(&relation.old_id, relation.kind)
                || !canonical_id(&relation.new_id, relation.kind)
                || relation.old_id == relation.new_id
                || relation.source_version_uuid == relation.version_uuid
                || !registry
                    .identities
                    .contains_key(&relation.source_version_uuid)
                || !registry.identities.contains_key(&relation.version_uuid)
                || registry
                    .receipts
                    .get(operation)
                    .is_none_or(|receipt| receipt.version_uuid != Some(relation.version_uuid))
            {
                return Err(invalid("invalid permanent ID regeneration relation"));
            }
        }
        Ok(())
    }
}

pub(super) fn preserve(before: &PublishIdState, after: &PublishIdState) -> Result<(), GfError> {
    if before
        .id_taken
        .iter()
        .any(|(key, value)| after.id_taken.get(key) != Some(value))
        || before
            .regenerations
            .iter()
            .any(|(key, value)| after.regenerations.get(key) != Some(value))
    {
        return Err(invalid(
            "publication cannot erase or rewrite permanent ID tracking",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::research_versions::ResearchOperationReceipt;

    #[test]
    fn regeneration_relations_require_canonical_ids_known_versions_and_the_creating_receipt() {
        let operation = Uuid::now_v7();
        let source = Uuid::now_v7();
        let target = Uuid::now_v7();
        let relation = OntologyIdRegeneration {
            operation_uuid: operation,
            kind: ItemKind::OntologyModule,
            old_id: "ont_aaaaaaaaaaaaaaaa".into(),
            new_id: "ont_bbbbbbbbbbbbbbbb".into(),
            source_version_uuid: source,
            version_uuid: target,
        };
        let mut registry = ResearchRegistry::default();
        registry
            .identities
            .extend([(source, [1; 32]), (target, [2; 32])]);
        registry.receipts.insert(
            operation,
            ResearchOperationReceipt {
                intent_sha256: None,
                operation_uuid: operation,
                request_sha256: [3; 32],
                generation_uuid: Uuid::now_v7(),
                version_uuid: Some(target),
            },
        );
        let state = PublishIdState {
            regenerations: BTreeMap::from([(operation, relation)]),
            ..Default::default()
        };
        state.validate(&registry).unwrap();
        for case in [
            "wrong_operation",
            "same_ids",
            "noncanonical",
            "wrong_kind",
            "unknown_source",
            "unknown_target",
            "wrong_receipt",
            "missing_receipt",
        ] {
            let mut bad = state.clone();
            let relation = bad.regenerations.get_mut(&operation).unwrap();
            let mut authority = registry.clone();
            match case {
                "wrong_operation" => relation.operation_uuid = Uuid::now_v7(),
                "same_ids" => relation.new_id.clone_from(&relation.old_id),
                "noncanonical" => relation.old_id.make_ascii_uppercase(),
                "wrong_kind" => relation.kind = ItemKind::BridgeSet,
                "unknown_source" => relation.source_version_uuid = Uuid::now_v7(),
                "unknown_target" => relation.version_uuid = Uuid::now_v7(),
                "wrong_receipt" => {
                    authority.receipts.get_mut(&operation).unwrap().version_uuid = Some(source)
                }
                "missing_receipt" => {
                    authority.receipts.clear();
                }
                _ => unreachable!(),
            }
            assert!(bad.validate(&authority).is_err(), "{case}");
        }
        assert!(preserve(&state, &PublishIdState::default()).is_err());
        preserve(&state, &state).unwrap();
    }
}
