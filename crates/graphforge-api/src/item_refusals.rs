//! Private local publish refusals, separate from portable Project authority.
//!
//! Regeneration binds private authorization before native CURRENT publication;
//! authenticated native receipts recover acknowledgement after a crash.
use crate::{GfError, GraphForge};
use graphforge_discovery::{ItemId, ItemKind};
use graphforge_filesystem::StableDirectory;
use graphforge_hub_publish::{HubPublishError, IdTakenItem, MAX_ID_TAKEN_ITEMS};
use graphforge_storage::durable_commit::{AtomicHooks, publish_atomic_in};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::fs::File;
use std::io::Read as _;
use uuid::Uuid;

const DIRECTORY: &str = ".publish-refusals";
const DOCUMENT: &str = "id-taken.json";
const LOCK: &str = "lock";
const FORMAT: &str = "graphforge-item-refusals/1";
const MAX_BYTES: u64 = 8 * 1024 * 1024;

mod regeneration;
pub(crate) use regeneration::{RegenerationBinding, RegenerationGuard};
#[cfg(test)]
mod tests;

/// A completed identity replacement, recorded after its native Version exists.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ItemIdRegeneration {
    /// New canonical bare permanent ID, of the refused item's kind.
    pub id: String,
    /// Retained Version containing the replacement identity.
    pub version_uuid: Uuid,
}

/// Exact-ID local refusal used by native publication and future regeneration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ItemIdRefusalReceipt {
    /// Validated, visibility-filtered refusal metadata; never credentials or prose.
    pub item: IdTakenItem,
    /// Version whose publication received this refusal.
    pub refused_version_uuid: Uuid,
    /// An acknowledged replacement. The original refusal remains available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub regenerated: Option<ItemIdRegeneration>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Document {
    format: String,
    receipts: BTreeMap<String, ItemIdRefusalReceipt>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    regenerations: BTreeMap<Uuid, RegenerationBinding>,
}

struct Store {
    directory: StableDirectory,
    lock: File,
}

fn storage(_: impl std::fmt::Display) -> GfError {
    GfError::Storage("local publish refusal storage is unavailable".into())
}

fn invalid(message: &str) -> GfError {
    GfError::Validation(message.into())
}

fn parse_id(value: &str) -> Result<ItemId, GfError> {
    ItemId::parse_reference(value)
        .map_err(|_| invalid("a permanent GraphForge item ID is required"))
}

impl Store {
    fn open(graph: &GraphForge, create: bool) -> Result<Option<Self>, GfError> {
        if graph.path().is_none() {
            return Err(invalid(
                "publish refusal receipts require a persistent Project",
            ));
        }
        let root =
            StableDirectory::open(graph.resolved_generation.container_root()).map_err(storage)?;
        let directory = match root.open_child_directory(OsStr::new(DIRECTORY)) {
            Ok(directory) => directory,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                if !create {
                    return Ok(None);
                }
                match root.create_child_directory(OsStr::new(DIRECTORY)) {
                    Ok(directory) => {
                        root.sync().map_err(storage)?;
                        directory
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => root
                        .open_child_directory(OsStr::new(DIRECTORY))
                        .map_err(storage)?,
                    Err(error) => return Err(storage(error)),
                }
            }
            Err(error) => return Err(storage(error)),
        };
        let lock = directory
            .open_or_create_child_file(OsStr::new(LOCK))
            .map_err(storage)?;
        fs4::FileExt::lock(&lock).map_err(storage)?;
        directory
            .revalidate_child_file(OsStr::new(LOCK), &lock)
            .map_err(storage)?;
        Ok(Some(Self { directory, lock }))
    }

    fn read(&self) -> Result<Document, GfError> {
        let file = match self.directory.open_child_file(OsStr::new(DOCUMENT)) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Document {
                    format: FORMAT.into(),
                    receipts: BTreeMap::new(),
                    regenerations: BTreeMap::new(),
                });
            }
            Err(error) => return Err(storage(error)),
        };
        if file.metadata().map_err(storage)?.len() > MAX_BYTES {
            return Err(invalid("local publish refusal storage exceeds its bound"));
        }
        let mut bytes = Vec::new();
        file.take(MAX_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(storage)?;
        if bytes.len() as u64 > MAX_BYTES {
            return Err(invalid("local publish refusal storage exceeds its bound"));
        }
        let document: Document = serde_json::from_slice(&bytes).map_err(storage)?;
        validate_document(&document)?;
        Ok(document)
    }

    fn write(&self, document: &mut Document, current: &BTreeSet<&str>) -> Result<(), GfError> {
        let bytes = retain_receipts(
            document,
            current,
            MAX_ID_TAKEN_ITEMS,
            usize::try_from(MAX_BYTES).map_err(storage)?,
        )?;
        validate_document(document)?;
        self.directory
            .revalidate_child_file(OsStr::new(LOCK), &self.lock)
            .map_err(storage)?;
        // Reopen through the retained directory capability, not a user-controlled
        // sidecar path, and use the storage owner's seal/replace/acknowledge path.
        let parent = StableDirectory::open(self.directory.path()).map_err(storage)?;
        publish_atomic_in(
            parent,
            OsStr::new(DOCUMENT),
            &bytes,
            AtomicHooks {
                after_write: || Ok(()),
                after_seal: || Ok(()),
                before_visible: || {
                    self.directory
                        .revalidate_child_file(OsStr::new(LOCK), &self.lock)
                },
            },
            None,
        )
        .map_err(storage)?
        .acknowledge(None)
        .map_err(storage)
    }
}

fn retain_receipts(
    document: &mut Document,
    current: &BTreeSet<&str>,
    max_receipts: usize,
    max_bytes: usize,
) -> Result<Vec<u8>, GfError> {
    let mut byte_count = serde_json::to_vec(document).map_err(storage)?.len();
    let removable = document
        .receipts
        .iter()
        .filter(|(id, receipt)| {
            receipt.regenerated.is_none()
                && !current.contains(id.as_str())
                && !document
                    .regenerations
                    .values()
                    .any(|binding| binding.old_id == **id)
        })
        .map(|(id, _)| id.clone())
        .collect::<Vec<_>>();
    // Canonical ID order is deterministic, without adding clocks or changing
    // the private format. Current refusals and replacement replay evidence
    // always win over unresolved receipts from earlier responses.
    for id in removable {
        if document.receipts.len() <= max_receipts && byte_count <= max_bytes {
            break;
        }
        let entry = BTreeMap::from([(&id, &document.receipts[&id])]);
        let entry_bytes = serde_json::to_vec(&entry).map_err(storage)?.len() - 2;
        byte_count -= entry_bytes + usize::from(document.receipts.len() > 1);
        document.receipts.remove(&id);
    }
    if document.receipts.len() > max_receipts || byte_count > max_bytes {
        return Err(GfError::Api {
            code: graphforge_core::ApiErrorCode::ResourceLimit,
            message: "local publish refusal receipt capacity is full; preserve recorded replacements and retry with fewer conflicting IDs or a separate local Project".into(),
        });
    }
    serde_json::to_vec(document).map_err(storage)
}

fn validate_document(document: &Document) -> Result<(), GfError> {
    if document.format != FORMAT
        || document.receipts.len() > MAX_ID_TAKEN_ITEMS
        || document.regenerations.len() > MAX_ID_TAKEN_ITEMS
    {
        return Err(invalid("local publish refusal document is invalid"));
    }
    let mut bound_ids = BTreeSet::new();
    for (operation, binding) in &document.regenerations {
        binding.refusal.item.validate().map_err(storage)?;
        let old = parse_id(&binding.old_id)?;
        let new = parse_id(&binding.replacement.id)?;
        if operation.is_nil()
            || *operation != binding.operation_uuid
            || binding.source_generation.is_nil()
            || binding.branch_uuid.is_nil()
            || binding.source_version.is_nil()
            || binding.refusal.refused_version_uuid.is_nil()
            || binding.refusal.item.id != binding.old_id
            || binding.refusal.item.kind != old.kind()
            || binding.refusal.item.publish_as_owner == Some(true)
            || binding.refusal.regenerated.is_some()
            || binding.replacement.version_uuid.is_nil()
            || old.kind() != new.kind()
            || old == new
            || old.as_str() != binding.old_id
            || new.as_str() != binding.replacement.id
            || !document.receipts.contains_key(old.as_str())
            || !bound_ids.insert((&binding.old_id, binding.source_generation))
        {
            return Err(invalid("local ID regeneration binding is invalid"));
        }
    }
    for (id, receipt) in &document.receipts {
        receipt.item.validate().map_err(storage)?;
        if id != &receipt.item.id {
            return Err(invalid("local refusal ID does not match its key"));
        }
        if let Some(regenerated) = &receipt.regenerated {
            let new = parse_id(&regenerated.id)?;
            if new.kind() != receipt.item.kind
                || new.as_str() != regenerated.id
                || new.as_str() == id
            {
                return Err(invalid("local replacement ID is invalid"));
            }
        }
    }
    Ok(())
}

impl GraphForge {
    /// Durably record validated per-item publication refusals without storing
    /// credentials, server text, URLs, or portable Project participant state.
    /// Evict unresolved receipts outside this response in canonical ID order
    /// as needed. Every current ID and recorded replacement stays protected;
    /// capacity exhaustion leaves the previous durable document unchanged.
    pub fn record_publish_id_taken(
        &self,
        version_uuid: Uuid,
        items: &[IdTakenItem],
    ) -> Result<Vec<ItemIdRefusalReceipt>, GfError> {
        HubPublishError::id_taken(items.to_vec()).map_err(storage)?;
        self.research_version(version_uuid)?;
        let store = Store::open(self, true)?.expect("create requires a store");
        let mut document = store.read_recovered(self)?;
        for item in items {
            if document
                .receipts
                .get(&item.id)
                .is_some_and(|receipt| receipt.regenerated.is_some())
            {
                continue;
            }
            document.receipts.insert(
                item.id.clone(),
                ItemIdRefusalReceipt {
                    item: item.clone(),
                    refused_version_uuid: version_uuid,
                    regenerated: None,
                },
            );
        }
        let current = items.iter().map(|item| item.id.as_str()).collect();
        store.write(&mut document, &current)?;
        items
            .iter()
            .map(|item| {
                document
                    .receipts
                    .get(&item.id)
                    .cloned()
                    .ok_or_else(|| invalid("local refusal receipt is missing"))
            })
            .collect()
    }

    /// Read the receipt for this exact permanent ID, accepting its bare or URI
    /// spelling. No receipt for another ID authorizes a future rewrite.
    pub fn id_taken_receipt(&self, id: &str) -> Result<Option<ItemIdRefusalReceipt>, GfError> {
        let id = parse_id(id)?;
        let Some(store) = Store::open(self, false)? else {
            return Ok(None);
        };
        Ok(store.read_recovered(self)?.receipts.remove(id.as_str()))
    }

    /// Record an already-committed replacement for future stale-Version
    /// guidance. This does not regenerate an ID or change Project history.
    pub fn record_item_id_regeneration(
        &self,
        old_id: &str,
        new_id: &str,
        version_uuid: Uuid,
    ) -> Result<ItemIdRefusalReceipt, GfError> {
        let old_id = parse_id(old_id)?;
        let new_id = parse_id(new_id)?;
        if old_id.kind() != new_id.kind() || old_id == new_id {
            return Err(invalid(
                "replacement ID must be different and have the same kind",
            ));
        }
        let Some(store) = Store::open(self, false)? else {
            return Err(invalid(
                "regeneration requires an id_taken receipt for that exact ID",
            ));
        };
        let mut document = store.read_recovered(self)?;
        if document
            .regenerations
            .values()
            .any(|binding| binding.old_id == old_id.as_str())
        {
            let receipt = document
                .receipts
                .get(old_id.as_str())
                .ok_or_else(|| invalid("local refusal receipt is missing"))?;
            if receipt.regenerated.as_ref().is_some_and(|relation| {
                relation.id == new_id.as_str() && relation.version_uuid == version_uuid
            }) {
                return Ok(receipt.clone());
            }
            return Err(invalid("the refused ID has a bound regeneration operation"));
        }
        let receipt = document.receipts.get_mut(old_id.as_str()).ok_or_else(|| {
            invalid("regeneration requires an id_taken receipt for that exact ID")
        })?;
        let relation = ItemIdRegeneration {
            id: new_id.as_str().into(),
            version_uuid,
        };
        if let Some(prior) = &receipt.regenerated {
            if prior == &relation {
                return Ok(receipt.clone());
            }
            return Err(invalid(
                "the refused ID already has a different recorded replacement",
            ));
        }
        if receipt.item.publish_as_owner == Some(true) {
            return Err(invalid(
                "publish as the owning organization instead of regenerating this ID",
            ));
        }
        self.validate_item_replacement(&old_id, &new_id, version_uuid)?;
        receipt.regenerated = Some(relation);
        let result = receipt.clone();
        store.write(&mut document, &BTreeSet::from([old_id.as_str()]))?;
        Ok(result)
    }

    fn validate_item_replacement(
        &self,
        old: &ItemId,
        new: &ItemId,
        version_uuid: Uuid,
    ) -> Result<(), GfError> {
        let version = self.research_version(version_uuid)?;
        let retained = version
            .content
            .participants
            .iter()
            .find(|entry| {
                entry.key.capability == graphforge_storage::WORKSPACE_CAPABILITY_ID
                    && entry.key.family == graphforge_storage::WORKSPACE_ONTOLOGY_COMPOSITION_FAMILY
            })
            .ok_or_else(|| invalid("replacement Version has no ontology composition"))?;
        let current = self
            .resolved_generation
            .participant_descriptors()?
            .into_iter()
            .find(|entry| {
                entry.capability_id == graphforge_storage::WORKSPACE_CAPABILITY_ID
                    && entry.record_family_id
                        == graphforge_storage::WORKSPACE_ONTOLOGY_COMPOSITION_FAMILY
            })
            .ok_or_else(|| invalid("current Project has no ontology composition"))?;
        if current.content_sha256 != retained.content_sha256 {
            return Err(invalid(
                "replacement Version does not contain the current item composition",
            ));
        }
        let ids = match new.kind() {
            ItemKind::OntologyModule => self
                .ontology_modules()
                .map_err(|_| storage("ontology inventory"))?
                .into_iter()
                .map(|entry| entry.id.ontology_id)
                .collect::<Vec<_>>(),
            ItemKind::BridgeSet => self
                .ontology_bridges()
                .map_err(|_| storage("bridge inventory"))?
                .into_iter()
                .map(|entry| entry.id.bridge_id)
                .collect::<Vec<_>>(),
        };
        let contains = |expected: &ItemId| {
            ids.iter()
                .any(|id| ItemId::parse_reference(id).is_ok_and(|id| &id == expected))
        };
        if !contains(new) || contains(old) {
            return Err(invalid(
                "replacement Version must contain the new identity and omit the refused identity",
            ));
        }
        Ok(())
    }
}
