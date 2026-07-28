//! Public Rust inspection and alias control for versioned embedding spaces.

use std::collections::{BTreeMap, BTreeSet};

use gf_storage::embedding_catalog::remove_embedding_space_catalog_entry;
use gf_storage::{
    ChunkingIdentity, EmbeddingCompatibilityId, EmbeddingDisplayName, EmbeddingProducerIdentity,
    EmbeddingSpaceCatalogLimits, EmbeddingSpaceCatalogUpdate, EmbeddingSpaceDiscoveryLimits,
    SearchCoordinationLimits, TokenCountClass, TokenizerIdentity,
    bind_existing_embedding_space_catalog_entry, delete_embedding_space_lineage,
    discover_embedding_spaces, read_embedding_space_catalog, update_embedding_space_catalog,
};

use super::{GfError, GraphForge};

/// Stable producer metadata exposed without payloads or credentials.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EmbeddingSpaceProducer {
    /// Canonical M18 structural embedding result.
    M18 {
        /// Stable algorithm token.
        algorithm: String,
        /// Frozen algorithm contract version.
        algorithm_version: String,
    },
    /// Process-local model adapter.
    Local {
        /// Adapter implementation identity.
        implementation: String,
        /// Model identifier.
        model: String,
        /// Immutable model revision or `unavailable`.
        revision: String,
        /// Adapter response contract version.
        contract_version: String,
    },
    /// Caller-registered callback contract.
    Callback {
        /// Stable callback contract identity.
        callback_contract: String,
        /// Callback request/response contract version.
        contract_version: String,
    },
    /// Explicit remote provider selection.
    Remote {
        /// Normalized provider token.
        provider: String,
        /// Provider model identifier.
        model: String,
        /// Immutable revision or `unavailable`.
        revision: String,
        /// Provider response contract version.
        response_contract_version: String,
    },
    /// Complete caller-supplied UUID/vector batch.
    CallerSupplied {
        /// Caller batch contract version.
        contract_version: String,
    },
}

/// Stable classification of tokenizer count evidence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EmbeddingTokenCountClass {
    /// Exact tokenizer implemented locally.
    ExactLocal,
    /// Exact count reported by the provider.
    ProviderReported,
    /// Conservative approximation.
    Approximate,
}

/// Content-free tokenizer and input-limit metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmbeddingTokenizerInfo {
    /// Tokenizer identifier or `unavailable`.
    pub identifier: String,
    /// Immutable tokenizer version or `unavailable`.
    pub version: String,
    /// Evidence class for token counts.
    pub count_class: EmbeddingTokenCountClass,
    /// Maximum supported tokens in one model input.
    pub max_input_tokens: u64,
    /// Versioned text-normalization token.
    pub normalization: String,
}

/// Explicit chunking contract; truncation remains reject-only.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmbeddingChunkingInfo {
    /// Maximum tokens in one chunk.
    pub chunk_size_tokens: u64,
    /// Tokens repeated between adjacent chunks.
    pub overlap_tokens: u64,
    /// Versioned aggregation token.
    pub aggregation: String,
    /// Stable truncation policy, currently `reject`.
    pub truncation_policy: String,
}

/// Content-free active-generation metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActiveEmbeddingGenerationInfo {
    /// Content-idempotent generation digest.
    pub generation_id: String,
    /// Complete UUID/vector row count.
    pub vector_count: u64,
    /// Committed graph generation consumed by the producer.
    pub source_graph_generation: u64,
    /// Exact source-state digest.
    pub source_fingerprint: String,
    /// Producer completion time in UTC microseconds.
    pub generated_at_micros: i64,
    /// Durable publication time in UTC microseconds.
    pub committed_at_micros: i64,
}

/// One verified compatibility lineage joined to its caller-facing aliases.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmbeddingSpaceInfo {
    /// Exact compatibility digest.
    pub compatibility_id: String,
    /// Normalized aliases in deterministic order.
    pub aliases: Vec<String>,
    /// Selected default alias when it resolves to this lineage.
    pub default_alias: Option<String>,
    /// Fixed vector width.
    pub dimensions: u32,
    /// Statically distinct producer identity.
    pub producer: EmbeddingSpaceProducer,
    /// Tokenizer identity for text-derived spaces.
    pub tokenizer: Option<EmbeddingTokenizerInfo>,
    /// Explicit chunking identity when selected.
    pub chunking: Option<EmbeddingChunkingInfo>,
    /// Fully verified active generation, or `None` before first publication.
    pub active: Option<ActiveEmbeddingGenerationInfo>,
}

impl GraphForge {
    /// List every verified embedding compatibility lineage deterministically.
    ///
    /// # Errors
    /// Returns structured storage, corruption, incompatibility, cancellation,
    /// or resource errors. A dangling durable alias fails closed.
    pub fn embedding_spaces(&self) -> Result<Vec<EmbeddingSpaceInfo>, GfError> {
        let catalog = read_embedding_space_catalog(
            &self.dir,
            EmbeddingSpaceCatalogLimits::default(),
            || Ok(()),
        )?;
        let discovered =
            discover_embedding_spaces(&self.dir, EmbeddingSpaceDiscoveryLimits::default(), || {
                Ok(())
            })?;
        join_spaces(&catalog, &discovered)
    }

    /// Bind one alias to an existing verified compatibility lineage.
    ///
    /// # Errors
    /// Rejects malformed or missing identities and implicit rebinding. Storage
    /// failures remain structured.
    pub fn bind_embedding_space_alias(
        &self,
        display_name: &str,
        compatibility_id: &str,
        replace: bool,
    ) -> Result<EmbeddingSpaceInfo, GfError> {
        let compatibility_id = EmbeddingCompatibilityId::from_hex(compatibility_id)?;
        let discovered =
            discover_embedding_spaces(&self.dir, EmbeddingSpaceDiscoveryLimits::default(), || {
                Ok(())
            })?;
        let exists = discovered
            .iter()
            .any(|space| space.compatibility_id() == compatibility_id);
        if !exists {
            return Err(validation(
                "embedding compatibility identity is not published",
            ));
        }
        let catalog = bind_existing_embedding_space_catalog_entry(
            &self.dir,
            display_name,
            compatibility_id,
            replace,
            EmbeddingSpaceCatalogLimits::default(),
            || Ok(()),
        )?;
        let selected = resolve_named_space(join_spaces(&catalog, &discovered)?, display_name)?;
        self.publish_workspace_update()?;
        Ok(selected)
    }

    /// Remove one alias without deleting its primary embedding generation.
    ///
    /// # Errors
    /// Rejects malformed names and preserves structured storage failures.
    pub fn remove_embedding_space_alias(&self, display_name: &str) -> Result<bool, GfError> {
        let removed = remove_embedding_space_catalog_entry(
            &self.dir,
            display_name,
            EmbeddingSpaceCatalogLimits::default(),
            || Ok(()),
        )?;
        if removed {
            self.publish_workspace_update()?;
        }
        Ok(removed)
    }

    /// Explicitly delete one named compatibility lineage and all of its aliases.
    ///
    /// Passing `None` selects the configured default. A missing normalized name
    /// or absent default is idempotent and returns `false`.
    ///
    /// # Errors
    /// Rejects malformed names and preserves structured storage, cancellation,
    /// and lock failures. No partial vector generation is exposed.
    pub fn delete_embedding_space(&self, display_name: Option<&str>) -> Result<bool, GfError> {
        let catalog = read_embedding_space_catalog(
            &self.dir,
            EmbeddingSpaceCatalogLimits::default(),
            || Ok(()),
        )?;
        let compatibility_id = match display_name {
            Some(display_name) => {
                let display_name = EmbeddingDisplayName::new(display_name)?;
                catalog.get(&display_name)
            }
            None => catalog
                .selected_default()
                .map(|entry| entry.compatibility_id()),
        };
        let Some(compatibility_id) = compatibility_id else {
            return Ok(false);
        };
        let deleted = delete_embedding_space_lineage(
            &self.dir,
            compatibility_id,
            EmbeddingSpaceCatalogLimits::default(),
            SearchCoordinationLimits::default(),
            || Ok(()),
        )?;
        if deleted {
            self.publish_workspace_update()?;
        }
        Ok(deleted)
    }

    /// Select an existing alias as default, or clear the default with `None`.
    ///
    /// # Errors
    /// Rejects malformed or missing aliases and preserves structured storage errors.
    pub fn set_default_embedding_space(
        &self,
        display_name: Option<&str>,
    ) -> Result<Option<EmbeddingSpaceInfo>, GfError> {
        let catalog = update_embedding_space_catalog(
            &self.dir,
            EmbeddingSpaceCatalogUpdate::SetDefault { display_name },
            EmbeddingSpaceCatalogLimits::default(),
            || Ok(()),
        )?;
        self.publish_workspace_update()?;
        let Some(display_name) = display_name else {
            return Ok(None);
        };
        let discovered =
            discover_embedding_spaces(&self.dir, EmbeddingSpaceDiscoveryLimits::default(), || {
                Ok(())
            })?;
        resolve_named_space(join_spaces(&catalog, &discovered)?, display_name).map(Some)
    }

    /// Resolve one explicit alias, or the configured default when omitted.
    ///
    /// # Errors
    /// Returns validation for a missing alias/default and structured storage
    /// errors for any invalid durable state.
    pub fn embedding_space(
        &self,
        display_name: Option<&str>,
    ) -> Result<EmbeddingSpaceInfo, GfError> {
        self.resolve_embedding_space_lineage(display_name)
            .map(|(info, _)| info)
    }

    pub(crate) fn resolve_embedding_space_lineage(
        &self,
        display_name: Option<&str>,
    ) -> Result<(EmbeddingSpaceInfo, gf_storage::DiscoveredEmbeddingSpace), GfError> {
        let catalog = read_embedding_space_catalog(
            &self.dir,
            EmbeddingSpaceCatalogLimits::default(),
            || Ok(()),
        )?;
        let discovered =
            discover_embedding_spaces(&self.dir, EmbeddingSpaceDiscoveryLimits::default(), || {
                Ok(())
            })?;
        let spaces = join_spaces(&catalog, &discovered)?;
        let selected = match display_name {
            Some(display_name) => resolve_named_space(spaces, display_name)?,
            None => spaces
                .into_iter()
                .find(|space| space.default_alias.is_some())
                .ok_or_else(|| validation("default embedding space is not configured"))?,
        };
        let compatibility_id = EmbeddingCompatibilityId::from_hex(&selected.compatibility_id)?;
        let lineage = discovered
            .into_iter()
            .find(|space| space.compatibility_id() == compatibility_id)
            .ok_or_else(|| gf_storage::SearchArtifactError::Missing {
                path: self
                    .dir
                    .join("embeddings/spaces")
                    .join(compatibility_id.to_hex())
                    .join("space.json"),
            })?;
        Ok((selected, lineage))
    }
}

fn resolve_named_space(
    spaces: Vec<EmbeddingSpaceInfo>,
    display_name: &str,
) -> Result<EmbeddingSpaceInfo, GfError> {
    let display_name = EmbeddingDisplayName::new(display_name)?;
    spaces
        .into_iter()
        .find(|space| {
            space
                .aliases
                .binary_search_by(|alias| alias.as_str().cmp(display_name.as_str()))
                .is_ok()
        })
        .ok_or_else(|| validation("embedding alias is not configured"))
}

fn join_spaces(
    catalog: &gf_storage::EmbeddingSpaceCatalog,
    discovered: &[gf_storage::DiscoveredEmbeddingSpace],
) -> Result<Vec<EmbeddingSpaceInfo>, GfError> {
    let discovered_ids = discovered
        .iter()
        .map(gf_storage::DiscoveredEmbeddingSpace::compatibility_id)
        .collect::<BTreeSet<_>>();
    let mut aliases = BTreeMap::<EmbeddingCompatibilityId, Vec<String>>::new();
    for entry in catalog.entries() {
        if !discovered_ids.contains(&entry.compatibility_id()) {
            return Err(GfError::Storage(
                "embedding alias targets an undiscoverable compatibility identity".to_owned(),
            ));
        }
        aliases
            .entry(entry.compatibility_id())
            .or_default()
            .push(entry.display_name().as_str().to_owned());
    }
    let selected_default = catalog.selected_default();
    discovered
        .iter()
        .map(|space| {
            let compatibility_id = space.compatibility_id();
            let descriptor = space.descriptor();
            let active = space.active().map(|publication| {
                let manifest = &publication.manifest;
                ActiveEmbeddingGenerationInfo {
                    generation_id: manifest.generation_id().to_hex(),
                    vector_count: manifest.vector_count(),
                    source_graph_generation: manifest.source().graph_generation(),
                    source_fingerprint: manifest.source().fingerprint().to_hex(),
                    generated_at_micros: manifest.generated_at_micros(),
                    committed_at_micros: manifest.committed_at_micros(),
                }
            });
            Ok(EmbeddingSpaceInfo {
                compatibility_id: compatibility_id.to_hex(),
                aliases: aliases.remove(&compatibility_id).unwrap_or_default(),
                default_alias: selected_default.as_ref().and_then(|entry| {
                    (entry.compatibility_id() == compatibility_id)
                        .then(|| entry.display_name().as_str().to_owned())
                }),
                dimensions: descriptor.dimensions(),
                producer: producer_info(descriptor.producer()),
                tokenizer: descriptor.tokenizer().map(tokenizer_info),
                chunking: descriptor.chunking().map(chunking_info),
                active,
            })
        })
        .collect()
}

fn producer_info(identity: &EmbeddingProducerIdentity) -> EmbeddingSpaceProducer {
    match identity {
        EmbeddingProducerIdentity::M18 {
            algorithm,
            algorithm_version,
        } => EmbeddingSpaceProducer::M18 {
            algorithm: algorithm.clone(),
            algorithm_version: algorithm_version.clone(),
        },
        EmbeddingProducerIdentity::Local {
            implementation,
            model,
            revision,
            contract_version,
        } => EmbeddingSpaceProducer::Local {
            implementation: implementation.clone(),
            model: model.clone(),
            revision: revision.clone(),
            contract_version: contract_version.clone(),
        },
        EmbeddingProducerIdentity::Callback {
            callback_contract,
            contract_version,
        } => EmbeddingSpaceProducer::Callback {
            callback_contract: callback_contract.clone(),
            contract_version: contract_version.clone(),
        },
        EmbeddingProducerIdentity::Remote {
            provider,
            model,
            revision,
            response_contract_version,
        } => EmbeddingSpaceProducer::Remote {
            provider: provider.clone(),
            model: model.clone(),
            revision: revision.clone(),
            response_contract_version: response_contract_version.clone(),
        },
        EmbeddingProducerIdentity::CallerSupplied { contract_version } => {
            EmbeddingSpaceProducer::CallerSupplied {
                contract_version: contract_version.clone(),
            }
        }
    }
}

fn tokenizer_info(identity: &TokenizerIdentity) -> EmbeddingTokenizerInfo {
    EmbeddingTokenizerInfo {
        identifier: identity.identifier.clone(),
        version: identity.version.clone(),
        count_class: match identity.count_class {
            TokenCountClass::ExactLocal => EmbeddingTokenCountClass::ExactLocal,
            TokenCountClass::ProviderReported => EmbeddingTokenCountClass::ProviderReported,
            TokenCountClass::Approximate => EmbeddingTokenCountClass::Approximate,
        },
        max_input_tokens: identity.max_input_tokens,
        normalization: identity.normalization.clone(),
    }
}

fn chunking_info(identity: &ChunkingIdentity) -> EmbeddingChunkingInfo {
    EmbeddingChunkingInfo {
        chunk_size_tokens: identity.chunk_size_tokens,
        overlap_tokens: identity.overlap_tokens,
        aggregation: identity.aggregation.clone(),
        truncation_policy: identity.truncation_policy.clone(),
    }
}

fn validation(message: impl Into<String>) -> GfError {
    GfError::Validation(message.into())
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use gf_storage::{
        EmbeddingBatchRow, EmbeddingCompatibilityDescriptor, EmbeddingCompatibilityInput,
        EmbeddingDistance, EmbeddingNormalization, EmbeddingPublicationRequest,
        EmbeddingSourceState, EmbeddingValueType, SearchCoordinationLimits, TokenCountClass,
        TokenizerIdentity, ValidatedEmbeddingBatch, VectorStoreLimits,
        publish_embedding_generation, validate_embedding_batch,
    };

    use super::*;

    fn descriptor(producer: EmbeddingProducerIdentity) -> EmbeddingCompatibilityDescriptor {
        let remote = matches!(&producer, EmbeddingProducerIdentity::Remote { .. });
        EmbeddingCompatibilityDescriptor::new(EmbeddingCompatibilityInput {
            producer,
            dimensions: 2,
            value_type: EmbeddingValueType::Float32,
            normalization: EmbeddingNormalization::None,
            distance: EmbeddingDistance::Cosine,
            tokenizer: remote.then_some(TokenizerIdentity {
                identifier: "provider-reported".to_owned(),
                version: "v1".to_owned(),
                count_class: TokenCountClass::ProviderReported,
                max_input_tokens: 8_192,
                normalization: "unicode-v1".to_owned(),
            }),
            chunking: None,
            hyperparameters: BTreeMap::new(),
            input_recipe: BTreeMap::from([("property".to_owned(), "body".into())]),
            source_projection_recipe: BTreeMap::from([("label".to_owned(), "Document".into())]),
        })
        .unwrap()
    }

    fn batch(marker: u8) -> ValidatedEmbeddingBatch {
        validate_embedding_batch(
            vec![EmbeddingBatchRow {
                node_uuid: [marker; 16],
                vector: vec![1.0, 2.0],
            }],
            &BTreeSet::from([[marker; 16]]),
            2,
            EmbeddingNormalization::None,
            VectorStoreLimits::default(),
            || Ok(()),
        )
        .unwrap()
    }

    fn publish(graph: &GraphForge, descriptor: &EmbeddingCompatibilityDescriptor, marker: u8) {
        publish_embedding_generation(
            &graph.dir,
            EmbeddingPublicationRequest {
                descriptor,
                source: EmbeddingSourceState::new(7, [marker; 32], [marker + 1; 32], 1),
                batch: &batch(marker),
                generated_at_micros: 20,
                committed_at_micros: 21,
            },
            VectorStoreLimits::default(),
            SearchCoordinationLimits::default(),
            || Ok(()),
        )
        .unwrap();
    }

    #[test]
    fn empty_alias_default_and_identity_validation_are_stable() {
        let graph = GraphForge::new(None).unwrap();
        assert!(graph.embedding_spaces().unwrap().is_empty());
        assert!(matches!(
            graph.embedding_space(None),
            Err(GfError::Validation(_))
        ));
        assert!(matches!(
            graph.embedding_space(Some("missing")),
            Err(GfError::Validation(_))
        ));
        assert!(matches!(
            graph.bind_embedding_space_alias("name", &"0".repeat(64), false),
            Err(GfError::Validation(_))
        ));
    }

    #[test]
    fn aliases_defaults_replacement_removal_and_reopen_are_deterministic() {
        let project = tempfile::tempdir().unwrap();
        let path = project.path().to_str().unwrap();
        let graph = GraphForge::new(Some(path)).unwrap();
        let first = descriptor(EmbeddingProducerIdentity::CallerSupplied {
            contract_version: "v1".to_owned(),
        });
        let second = descriptor(EmbeddingProducerIdentity::M18 {
            algorithm: "node2vec".to_owned(),
            algorithm_version: "embedding-v1".to_owned(),
        });
        publish(&graph, &second, 2);
        publish(&graph, &first, 1);
        let first_id = first.compatibility_id().unwrap().to_hex();
        let second_id = second.compatibility_id().unwrap().to_hex();

        graph
            .bind_embedding_space_alias("semantic", &first_id, false)
            .unwrap();
        graph
            .bind_embedding_space_alias("semantic", &first_id, false)
            .unwrap();
        graph
            .bind_embedding_space_alias("also-semantic", &first_id, false)
            .unwrap();
        assert!(matches!(
            graph.bind_embedding_space_alias("semantic", &second_id, false),
            Err(GfError::Validation(_))
        ));
        graph
            .bind_embedding_space_alias("semantic", &second_id, true)
            .unwrap();
        let selected = graph
            .set_default_embedding_space(Some("semantic"))
            .unwrap()
            .unwrap();
        assert_eq!(selected.compatibility_id, second_id);
        assert_eq!(selected.default_alias.as_deref(), Some("semantic"));
        assert_eq!(graph.embedding_space(None).unwrap(), selected);

        let listed = graph.embedding_spaces().unwrap();
        assert_eq!(listed.len(), 2);
        assert!(
            listed
                .windows(2)
                .all(|pair| pair[0].compatibility_id.as_str() < pair[1].compatibility_id.as_str())
        );
        assert_eq!(
            listed
                .iter()
                .find(|space| space.compatibility_id == first_id)
                .unwrap()
                .aliases,
            ["also-semantic"]
        );
        assert_eq!(
            listed
                .iter()
                .find(|space| space.compatibility_id == second_id)
                .unwrap()
                .aliases,
            ["semantic"]
        );
        assert!(graph.remove_embedding_space_alias("also-semantic").unwrap());
        assert!(!graph.remove_embedding_space_alias("also-semantic").unwrap());
        graph.set_default_embedding_space(None).unwrap();
        assert!(matches!(
            graph.embedding_space(None),
            Err(GfError::Validation(_))
        ));

        drop(graph);
        let reopened = GraphForge::new(Some(path)).unwrap();
        assert_eq!(
            reopened
                .embedding_space(Some("semantic"))
                .unwrap()
                .compatibility_id,
            second_id
        );
    }

    #[test]
    fn explicit_deletion_removes_one_lineage_all_aliases_and_default() {
        let project = tempfile::tempdir().unwrap();
        let path = project.path().to_str().unwrap();
        let graph = GraphForge::new(Some(path)).unwrap();
        let deleted = descriptor(EmbeddingProducerIdentity::CallerSupplied {
            contract_version: "delete-v1".to_owned(),
        });
        let retained = descriptor(EmbeddingProducerIdentity::M18 {
            algorithm: "node2vec".to_owned(),
            algorithm_version: "retain-v1".to_owned(),
        });
        publish(&graph, &deleted, 3);
        publish(&graph, &retained, 4);
        let deleted_id = deleted.compatibility_id().unwrap().to_hex();
        let retained_id = retained.compatibility_id().unwrap().to_hex();
        graph
            .bind_embedding_space_alias("obsolete", &deleted_id, false)
            .unwrap();
        graph
            .bind_embedding_space_alias("obsolete-copy", &deleted_id, false)
            .unwrap();
        graph
            .bind_embedding_space_alias("retained", &retained_id, false)
            .unwrap();
        graph
            .set_default_embedding_space(Some("obsolete-copy"))
            .unwrap();

        let interrupted_marker = graph
            .dir
            .join("embeddings")
            .join(format!(".deleting-{deleted_id}"));
        std::fs::write(&interrupted_marker, deleted_id.as_bytes()).unwrap();
        assert!(matches!(
            graph.bind_embedding_space_alias("late-alias", &deleted_id, false),
            Err(GfError::Validation(_))
        ));
        assert!(graph.embedding_spaces().is_err());

        assert!(graph.delete_embedding_space(Some("obsolete")).unwrap());
        assert!(!interrupted_marker.exists());
        assert!(!graph.delete_embedding_space(Some("obsolete")).unwrap());
        assert!(matches!(
            graph.embedding_space(Some("obsolete-copy")),
            Err(GfError::Validation(_))
        ));
        assert!(matches!(
            graph.embedding_space(None),
            Err(GfError::Validation(_))
        ));
        let spaces = graph.embedding_spaces().unwrap();
        assert_eq!(spaces.len(), 1);
        assert_eq!(spaces[0].compatibility_id, retained_id);
        assert!(
            !graph
                .dir
                .join("embeddings/spaces")
                .join(&deleted_id)
                .exists()
        );
        assert!(
            graph
                .dir
                .join("embeddings/spaces")
                .join(&retained_id)
                .exists()
        );
        assert!(matches!(
            graph.delete_embedding_space(Some("\n")),
            Err(GfError::Validation(_))
        ));

        drop(graph);
        let reopened = GraphForge::new(Some(path)).unwrap();
        assert_eq!(
            reopened
                .embedding_space(Some("retained"))
                .unwrap()
                .compatibility_id,
            retained_id
        );
        reopened
            .set_default_embedding_space(Some("retained"))
            .unwrap();
        assert!(reopened.delete_embedding_space(None).unwrap());
        assert!(!reopened.delete_embedding_space(None).unwrap());
        assert!(reopened.embedding_spaces().unwrap().is_empty());
    }

    #[test]
    fn every_producer_is_typed_and_inspection_is_content_free() {
        let graph = GraphForge::new(None).unwrap();
        let producers = [
            EmbeddingProducerIdentity::M18 {
                algorithm: "node2vec".into(),
                algorithm_version: "v1".into(),
            },
            EmbeddingProducerIdentity::Local {
                implementation: "local".into(),
                model: "model".into(),
                revision: "r1".into(),
                contract_version: "v1".into(),
            },
            EmbeddingProducerIdentity::Callback {
                callback_contract: "callback".into(),
                contract_version: "v1".into(),
            },
            EmbeddingProducerIdentity::Remote {
                provider: "openrouter".into(),
                model: "model".into(),
                revision: "r1".into(),
                response_contract_version: "v1".into(),
            },
            EmbeddingProducerIdentity::CallerSupplied {
                contract_version: "v1".into(),
            },
        ];
        for (index, producer) in producers.into_iter().enumerate() {
            publish(&graph, &descriptor(producer), index as u8 + 1);
        }
        let spaces = graph.embedding_spaces().unwrap();
        assert_eq!(spaces.len(), 5);
        assert!(spaces.iter().all(|space| space.active.is_some()));
        let rendered = format!("{spaces:?}");
        for forbidden in [
            "credential",
            "source text",
            "vector: ",
            "confidence",
            "provenance",
            "evidence",
            "valid_time",
        ] {
            assert!(!rendered.contains(forbidden), "leaked {forbidden}");
        }
    }

    #[test]
    fn descriptor_only_lineage_exposes_stable_metadata_without_an_active_generation() {
        let graph = GraphForge::new(None).unwrap();
        let descriptor = descriptor(EmbeddingProducerIdentity::Local {
            implementation: "local-runtime".into(),
            model: "tiny-model".into(),
            revision: "r1".into(),
            contract_version: "v1".into(),
        });
        let compatibility_id = descriptor.compatibility_id().unwrap().to_hex();
        let root = graph
            .dir
            .join("embeddings")
            .join("spaces")
            .join(&compatibility_id);
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(
            root.join("space.json"),
            descriptor.to_canonical_json().unwrap(),
        )
        .unwrap();

        let spaces = graph.embedding_spaces().unwrap();
        assert_eq!(spaces.len(), 1);
        assert_eq!(spaces[0].compatibility_id, compatibility_id);
        assert_eq!(spaces[0].dimensions, 2);
        assert!(spaces[0].active.is_none());
        assert!(spaces[0].aliases.is_empty());
        assert!(matches!(
            spaces[0].producer,
            EmbeddingSpaceProducer::Local { .. }
        ));
    }

    #[test]
    fn dangling_catalog_identity_fails_closed() {
        let graph = GraphForge::new(None).unwrap();
        update_embedding_space_catalog(
            &graph.dir,
            EmbeddingSpaceCatalogUpdate::Bind {
                display_name: "dangling",
                compatibility_id: EmbeddingCompatibilityId::from_hex(&"9".repeat(64)).unwrap(),
                replace: false,
            },
            EmbeddingSpaceCatalogLimits::default(),
            || Ok(()),
        )
        .unwrap();
        assert!(matches!(graph.embedding_spaces(), Err(GfError::Storage(_))));
    }
}
