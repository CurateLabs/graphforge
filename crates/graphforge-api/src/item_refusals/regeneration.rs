//! Durable private execution bindings, authenticated by permanent native receipts.
use super::*;
use graphforge_storage::research_versions::{ResearchOperationReceipt, read_research_registry};
use sha2::Digest as _;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RegenerationBinding {
    pub operation_uuid: Uuid,
    pub old_id: String,
    // Legacy receipt bytes stay identical. Explicit authority has a required
    // distinct wire marker: missing, null or malformed authority fails closed.
    #[serde(with = "authorization")]
    pub refusal: Option<ItemIdRefusalReceipt>,
    pub replacement: ItemIdRegeneration,
    pub source_generation: Uuid,
    pub branch_uuid: Uuid,
    pub source_version: Uuid,
    pub created_at: i64,
    // Persist the exact prepared native request before attempting publication.
    // It may change after an aborted private preparation, never after a commit.
    pub request_sha256: Option<[u8; 32]>,
}

mod authorization {
    use super::*;

    #[derive(Serialize, Deserialize)]
    #[serde(untagged)]
    enum Wire {
        Refusal(ItemIdRefusalReceipt),
        Explicit(Explicit),
    }
    #[derive(Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Explicit {
        mode: Mode,
    }
    #[derive(Serialize, Deserialize)]
    enum Mode {
        #[serde(rename = "explicit_ontology_fork")]
        ExplicitOntologyFork,
    }

    #[allow(clippy::ref_option)] // serde field adapters receive a reference to the field
    pub(super) fn serialize<S: serde::Serializer>(
        value: &Option<ItemIdRefusalReceipt>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            Some(receipt) => receipt.serialize(serializer),
            None => Explicit {
                mode: Mode::ExplicitOntologyFork,
            }
            .serialize(serializer),
        }
    }
    pub(super) fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<ItemIdRefusalReceipt>, D::Error> {
        Ok(match Wire::deserialize(deserializer)? {
            Wire::Refusal(receipt) => Some(receipt),
            Wire::Explicit(_) => None,
        })
    }
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
        // A caller-owned UUID may have been committed by another native
        // operation before regeneration reserved its journal. That authenticated
        // different intent cannot acknowledge this binding; only its targeted
        // regeneration replay conflicts. Matching intent still requires every
        // physical request and Version commitment to authenticate exactly.
        if native.intent_sha256 != Some(binding.digest()?) {
            if binding.request_sha256 == Some(native.request_sha256) {
                // The same physical request cannot carry two logical intents.
                // This is inconsistent authentication, not a foreign consumer.
                return Err(conflict());
            }
            continue;
        }
        authenticate(binding, native)?;
        if binding.refusal.is_none() {
            // Explicit forks acknowledge through the permanent native receipt;
            // they never manufacture or alter publication refusal evidence.
            continue;
        }
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

fn regeneration_refusal<'a>(
    document: &'a Document,
    id: &ItemId,
    explicit_fork: bool,
) -> Result<Option<&'a ItemIdRefusalReceipt>, GfError> {
    if explicit_fork {
        return Ok(None);
    }
    let receipt = document
        .receipts
        .get(id.as_str())
        .ok_or_else(|| invalid("regeneration requires an id_taken receipt for that exact ID"))?;
    if receipt.item.publish_as_owner == Some(true) {
        return Err(invalid(
            "publish as the owning organization instead of regenerating this ID",
        ));
    }
    if receipt.regenerated.is_some() {
        return Err(conflict());
    }
    Ok(Some(receipt))
}

impl RegenerationGuard {
    pub(crate) fn begin(
        graph: &GraphForge,
        id: &ItemId,
        operation_uuid: Uuid,
        explicit_fork: bool,
        prepare: impl FnOnce(Option<&ItemIdRefusalReceipt>) -> Result<RegenerationBinding, GfError>,
    ) -> Result<Self, GfError> {
        // Validate a first explicit fork before creating any private state.
        // Existing bindings must replay before inspecting today's composition.
        let mut prepare = Some(prepare);
        let mut prepared = None;
        let store = match Store::open(graph, false)? {
            Some(store) => store,
            None if explicit_fork => {
                prepared = Some(prepare.take().expect("unused preparation")(None)?);
                Store::open(graph, true)?.expect("created store")
            }
            None => {
                return Err(invalid(
                    "regeneration requires an id_taken receipt for that exact ID",
                ));
            }
        };
        let mut document = store.read_recovered(graph)?;
        if let Some(binding) = document.regenerations.get(&operation_uuid) {
            if binding.old_id != id.as_str() || binding.refusal.is_none() != explicit_fork {
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
        if document
            .regenerations
            .values()
            .any(|binding| binding.old_id == id.as_str())
        {
            // Under the same sidecar lock, recover native journals before
            // deciding whether an old preparation can still publish. Retain
            // every immutable binding: its UUID must forever replay or be stale.
            let root = graph.resolved_generation.container_root();
            graphforge_storage::recover_project_transactions_with_mode(root, graph.lifecycle_mode)?;
            recover(graph, &store, &mut document)?;
            let current = graphforge_storage::resolve_project_generation(root)?;
            let registry = read_research_registry(&current)?;
            for binding in document
                .regenerations
                .values()
                .filter(|binding| binding.old_id == id.as_str())
            {
                if binding.source_generation == current.generation_uuid() {
                    return Err(conflict());
                }
                if let Some(published) =
                    graphforge_storage::published_project_transaction(root, binding.operation_uuid)?
                {
                    // A permanent native receipt can positively identify a
                    // foreign UUID consumer. A journal without that matching
                    // receipt remains uncertain and cannot release this ID.
                    let intent = binding.digest()?;
                    let foreign =
                        registry
                            .receipts
                            .get(&binding.operation_uuid)
                            .is_some_and(|receipt| {
                                receipt.intent_sha256 != Some(intent)
                                    && binding.request_sha256 != Some(receipt.request_sha256)
                                    && receipt.generation_uuid == published.generation_uuid
                            });
                    if !foreign {
                        return Err(conflict());
                    }
                }
            }
        }
        let receipt = regeneration_refusal(&document, id, explicit_fork)?;
        if document.regenerations.len() == MAX_ID_TAKEN_ITEMS {
            return Err(GfError::Api {
                code: graphforge_core::ApiErrorCode::ResourceLimit,
                message: "local ID regeneration binding capacity is full".into(),
            });
        }
        let binding = match prepared {
            Some(binding) => binding,
            None => prepare.take().expect("unused preparation")(receipt)?,
        };
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
