//! Durable private execution bindings, authenticated by permanent native receipts.
use super::*;
use graphforge_storage::research_versions::{ResearchOperationReceipt, read_research_registry};
use sha2::Digest as _;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RegenerationBinding {
    pub operation_uuid: Uuid,
    pub old_id: String,
    pub replacement: ItemIdRegeneration,
    pub source_generation: Uuid,
    pub branch_uuid: Uuid,
    pub source_version: Uuid,
    pub created_at: i64,
    // Persist the exact prepared native request before attempting publication.
    // It may change after an aborted private preparation, never after a commit.
    pub request_sha256: Option<[u8; 32]>,
}
impl RegenerationBinding {
    pub(crate) fn digest(&self) -> Result<[u8; 32], GfError> {
        let mut logical = self.clone();
        logical.request_sha256 = None;
        let mut hash = sha2::Sha256::new();
        hash.update(b"graphforge-item-id-regeneration/1");
        hash.update(serde_json::to_vec(&logical).map_err(storage)?);
        Ok(hash.finalize().into())
    }
}

pub(crate) struct RegenerationGuard {
    store: Store,
    document: Document,
    pub binding: RegenerationBinding,
}

fn conflict() -> GfError {
    GfError::Project {
        code: graphforge_core::ProjectErrorCode::TransactionConflict,
        message: "ID regeneration operation has conflicting intent or receipt".into(),
    }
}

fn authenticate(
    binding: &RegenerationBinding,
    receipt: &ResearchOperationReceipt,
) -> Result<(), GfError> {
    if receipt.operation_uuid != binding.operation_uuid
        || receipt.version_uuid != Some(binding.replacement.version_uuid)
        || receipt.intent_sha256 != Some(binding.digest()?)
        || Some(receipt.request_sha256) != binding.request_sha256
    {
        return Err(conflict());
    }
    Ok(())
}

// CURRENT history survives content restore. No lookup of today's composition or
// of a potentially released historical payload is needed to authenticate replay.
fn recover(graph: &GraphForge, store: &Store, document: &mut Document) -> Result<(), GfError> {
    let current =
        graphforge_storage::resolve_project_generation(graph.resolved_generation.container_root())?;
    let registry = read_research_registry(&current)?;
    let mut changed = false;
    for binding in document.regenerations.values() {
        let Some(native) = registry.receipts.get(&binding.operation_uuid) else {
            continue;
        };
        authenticate(binding, native)?;
        let receipt = document
            .receipts
            .get_mut(&binding.old_id)
            .ok_or_else(conflict)?;
        if let Some(existing) = &receipt.regenerated {
            if existing != &binding.replacement {
                return Err(conflict());
            }
        } else {
            receipt.regenerated = Some(binding.replacement.clone());
            changed = true;
        }
    }
    if changed {
        store.write(document, &BTreeSet::new())?;
    }
    Ok(())
}

impl Store {
    pub(super) fn read_recovered(&self, graph: &GraphForge) -> Result<Document, GfError> {
        let mut document = self.read()?;
        recover(graph, self, &mut document)?;
        Ok(document)
    }
}

impl RegenerationGuard {
    pub(crate) fn begin(
        graph: &GraphForge,
        id: &ItemId,
        operation_uuid: Uuid,
        prepare: impl FnOnce() -> Result<RegenerationBinding, GfError>,
    ) -> Result<Self, GfError> {
        let store = Store::open(graph, false)?.ok_or_else(|| {
            invalid("regeneration requires an id_taken receipt for that exact ID")
        })?;
        let mut document = store.read_recovered(graph)?;
        if let Some(binding) = document.regenerations.get(&operation_uuid) {
            if binding.old_id != id.as_str() {
                return Err(conflict());
            }
            return Ok(Self {
                binding: binding.clone(),
                store,
                document,
            });
        }
        let current = graphforge_storage::resolve_project_generation(
            graph.resolved_generation.container_root(),
        )?;
        if read_research_registry(&current)?
            .receipts
            .contains_key(&operation_uuid)
        {
            return Err(conflict());
        }
        let receipt = document.receipts.get(id.as_str()).ok_or_else(|| {
            invalid("regeneration requires an id_taken receipt for that exact ID")
        })?;
        if receipt.item.publish_as_owner == Some(true) {
            return Err(invalid(
                "publish as the owning organization instead of regenerating this ID",
            ));
        }
        if receipt.regenerated.is_some()
            || document
                .regenerations
                .values()
                .any(|binding| binding.old_id == id.as_str())
        {
            return Err(conflict());
        }
        let binding = prepare()?;
        document
            .regenerations
            .insert(operation_uuid, binding.clone());
        store.write(&mut document, &BTreeSet::from([id.as_str()]))?;
        Ok(Self {
            store,
            document,
            binding,
        })
    }

    pub(crate) fn replay(
        &mut self,
        graph: &mut GraphForge,
    ) -> Result<Option<ItemIdRegeneration>, GfError> {
        let root = graph.resolved_generation.container_root().to_path_buf();
        let current = graphforge_storage::resolve_project_generation(&root)?;
        let registry = read_research_registry(&current)?;
        let Some(native) = registry.receipts.get(&self.binding.operation_uuid) else {
            return Ok(None);
        };
        authenticate(&self.binding, native)?;
        graph.refresh_research_authority(
            &root,
            &Ok(native.clone()),
            None,
            self.binding.source_generation,
            false,
        )?;
        Ok(Some(self.binding.replacement.clone()))
    }

    pub(crate) fn bind_request(
        &mut self,
        operation: &graphforge_storage::research_versions::ResearchOperation,
    ) -> Result<(), GfError> {
        self.binding.request_sha256 =
            Some(sha2::Sha256::digest(serde_json::to_vec(operation).map_err(storage)?).into());
        self.document
            .regenerations
            .insert(self.binding.operation_uuid, self.binding.clone());
        self.store.write(
            &mut self.document,
            &BTreeSet::from([self.binding.old_id.as_str()]),
        )
    }

    pub(crate) fn acknowledge(&mut self, graph: &GraphForge) -> Result<(), GfError> {
        recover(graph, &self.store, &mut self.document)
    }
}
