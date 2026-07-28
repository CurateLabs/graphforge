//! Public content-free freshness inspection for versioned embedding spaces.

use gf_search::{EmbeddingReadLimits, PreparedEmbeddingRead, prepare_embedding_read};
use gf_storage::{
    EmbeddingFreshnessReason, EmbeddingFreshnessState, EmbeddingMutationJournalLimits,
    EmbeddingReadDecision, EmbeddingSourceState, SearchArtifactError,
    generation::read_search_generation, read_embedding_mutation_journal,
};

use super::{GfError, GraphForge};

/// Stable public freshness classification for one complete embedding generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EmbeddingSpaceFreshnessState {
    /// The complete generation exactly matches proven durable source state.
    Fresh,
    /// A bounded relevant mutation occurred below the substantial threshold.
    Stale,
    /// Ordinary search must refresh successfully before serving.
    SubstantiallyStale,
}

/// Exact serving decision produced by the Rust-owned freshness policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EmbeddingSpaceReadDecision {
    /// Serve the verified complete fresh generation.
    ServeFresh,
    /// Serve a mildly stale generation while refresh is queued.
    ServeStale {
        /// Stable content-free freshness reason token.
        reason: String,
    },
    /// Block ordinary reads until refresh succeeds.
    RefreshRequired {
        /// Stable content-free substantial-staleness reason token.
        reason: String,
    },
    /// Serve the last complete substantially stale generation only by explicit request.
    ServeForcedStale {
        /// Stable content-free diagnostic required at the caller boundary.
        diagnostic: String,
    },
}

/// Content-free freshness and serving metadata for one exact active generation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmbeddingSpaceFreshnessInspection {
    /// Compatibility lineage selected by alias or configured default.
    pub compatibility_id: String,
    /// Exact immutable complete generation inspected.
    pub generation_id: String,
    /// Durable freshness classification independent of force selection.
    pub state: EmbeddingSpaceFreshnessState,
    /// Stable reason token, absent only for a fresh generation.
    pub reason: Option<String>,
    /// Rust-owned serving decision for this inspection request.
    pub decision: EmbeddingSpaceReadDecision,
}

impl GraphForge {
    /// Inspect one selected embedding space before indexing or search.
    ///
    /// `display_name=None` resolves the configured default. Setting
    /// `force_stale=true` changes only the serving decision for a substantially
    /// stale, otherwise valid complete generation. It cannot bypass missing,
    /// corrupt, incompatible, or invalid primary state.
    ///
    /// # Errors
    /// Returns structured alias/default, missing-generation, corruption,
    /// incompatibility, source-regression, cancellation, or resource errors.
    pub fn inspect_embedding_space_freshness(
        &self,
        display_name: Option<&str>,
        force_stale: bool,
    ) -> Result<EmbeddingSpaceFreshnessInspection, GfError> {
        let prepared = self.prepare_embedding_space_read(display_name, force_stale)?;
        let compatibility_id = prepared.publication().manifest.compatibility_id();
        let freshness = prepared.freshness();
        Ok(EmbeddingSpaceFreshnessInspection {
            compatibility_id: compatibility_id.to_hex(),
            generation_id: prepared.publication().manifest.generation_id().to_hex(),
            state: public_state(freshness.state()),
            reason: freshness.reason().map(reason_token),
            decision: public_decision(prepared.decision()),
        })
    }

    pub(crate) fn prepare_embedding_space_read(
        &self,
        display_name: Option<&str>,
        force_stale: bool,
    ) -> Result<PreparedEmbeddingRead, GfError> {
        let _visibility = self.embedding_refresh_visibility.lock().map_err(|_| {
            GfError::Execution("embedding refresh visibility lock is poisoned".into())
        })?;
        let (_, lineage) = self.resolve_embedding_space_lineage(display_name)?;
        let compatibility_id = lineage.compatibility_id();
        let publication = lineage
            .active()
            .ok_or_else(|| SearchArtifactError::Missing {
                path: self
                    .dir
                    .join("embeddings/spaces")
                    .join(compatibility_id.to_hex())
                    .join("active.json"),
            })?;
        let current_source = current_source(&self.dir, publication)?;
        prepare_embedding_read(
            &self.dir,
            lineage.descriptor(),
            current_source,
            force_stale,
            EmbeddingReadLimits::default(),
            || Ok(()),
        )?
        .ok_or_else(|| SearchArtifactError::Missing {
            path: self
                .dir
                .join("embeddings/spaces")
                .join(compatibility_id.to_hex())
                .join("active.json"),
        })
        .map_err(Into::into)
    }
}

fn current_source(
    project_dir: &std::path::Path,
    publication: &gf_storage::EmbeddingGenerationPublication,
) -> Result<EmbeddingSourceState, SearchArtifactError> {
    let live_generation = read_search_generation(project_dir).map_err(|error| {
        SearchArtifactError::SourceSnapshot {
            reason: error.to_string(),
        }
    })?;
    let recorded = publication.manifest.source();
    if live_generation < recorded.graph_generation() {
        return Err(SearchArtifactError::SourceSnapshot {
            reason: "current graph generation predates the embedding generation".to_owned(),
        });
    }
    let durable = read_embedding_mutation_journal(
        project_dir,
        &publication.manifest,
        EmbeddingMutationJournalLimits::default(),
    )?
    .map(|journal| journal.observation())
    .transpose()?
    .map(gf_storage::EmbeddingMutationObservation::current_source);
    let base = durable.unwrap_or(recorded);
    if base.graph_generation() > live_generation {
        return Err(SearchArtifactError::SourceSnapshot {
            reason: "embedding mutation evidence is ahead of the current graph generation"
                .to_owned(),
        });
    }
    if base.graph_generation() == live_generation {
        return Ok(base);
    }
    Ok(EmbeddingSourceState::new(
        live_generation,
        base.label_membership_digest(),
        base.dependency_input_digest(),
        base.eligible_uuid_count(),
    ))
}

const fn public_state(state: EmbeddingFreshnessState) -> EmbeddingSpaceFreshnessState {
    match state {
        EmbeddingFreshnessState::Fresh => EmbeddingSpaceFreshnessState::Fresh,
        EmbeddingFreshnessState::Stale => EmbeddingSpaceFreshnessState::Stale,
        EmbeddingFreshnessState::SubstantiallyStale => {
            EmbeddingSpaceFreshnessState::SubstantiallyStale
        }
    }
}

fn public_decision(decision: EmbeddingReadDecision) -> EmbeddingSpaceReadDecision {
    match decision {
        EmbeddingReadDecision::ServeFresh => EmbeddingSpaceReadDecision::ServeFresh,
        EmbeddingReadDecision::ServeStale { reason } => EmbeddingSpaceReadDecision::ServeStale {
            reason: reason_token(reason),
        },
        EmbeddingReadDecision::RefreshRequired { reason } => {
            EmbeddingSpaceReadDecision::RefreshRequired {
                reason: reason_token(reason),
            }
        }
        EmbeddingReadDecision::ServeForcedStale { diagnostic } => {
            EmbeddingSpaceReadDecision::ServeForcedStale {
                diagnostic: diagnostic.stable_message(),
            }
        }
    }
}

fn reason_token(reason: EmbeddingFreshnessReason) -> String {
    reason.as_str().to_owned()
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use gf_storage::{
        EmbeddingBatchRow, EmbeddingCompatibilityDescriptor, EmbeddingCompatibilityInput,
        EmbeddingDistance, EmbeddingMutationBatch, EmbeddingMutationJournalLimits,
        EmbeddingNormalization, EmbeddingProducerIdentity, EmbeddingPublicationRequest,
        EmbeddingSourceState, EmbeddingValueType, SearchCoordinationLimits, VectorStoreLimits,
        generation::{bump_search_generation, read_search_generation},
        merge_embedding_mutation_batch, publish_embedding_generation,
        reset_embedding_mutation_journal, validate_embedding_batch,
    };

    use super::*;

    fn descriptor() -> EmbeddingCompatibilityDescriptor {
        EmbeddingCompatibilityDescriptor::new(EmbeddingCompatibilityInput {
            producer: EmbeddingProducerIdentity::CallerSupplied {
                contract_version: "v1".to_owned(),
            },
            dimensions: 2,
            value_type: EmbeddingValueType::Float32,
            normalization: EmbeddingNormalization::None,
            distance: EmbeddingDistance::Cosine,
            tokenizer: None,
            chunking: None,
            hyperparameters: BTreeMap::new(),
            input_recipe: BTreeMap::from([("kind".to_owned(), "test".into())]),
            source_projection_recipe: BTreeMap::from([("label".to_owned(), "Person".into())]),
        })
        .unwrap()
    }

    fn publish(graph: &GraphForge, count: u8) -> gf_storage::EmbeddingGenerationPublication {
        let descriptor = descriptor();
        let eligible = (1..=count)
            .map(|marker| [marker; 16])
            .collect::<BTreeSet<_>>();
        let rows = (1..=count)
            .map(|marker| EmbeddingBatchRow {
                node_uuid: [marker; 16],
                vector: vec![f32::from(marker), 1.0],
            })
            .collect();
        let batch = validate_embedding_batch(
            rows,
            &eligible,
            2,
            EmbeddingNormalization::None,
            VectorStoreLimits::default(),
            || Ok(()),
        )
        .unwrap();
        let source = EmbeddingSourceState::new(
            read_search_generation(&graph.dir).unwrap(),
            [1; 32],
            [2; 32],
            u64::from(count),
        );
        let publication = publish_embedding_generation(
            &graph.dir,
            EmbeddingPublicationRequest {
                descriptor: &descriptor,
                source,
                batch: &batch,
                generated_at_micros: 10,
                committed_at_micros: 11,
            },
            VectorStoreLimits::default(),
            SearchCoordinationLimits::default(),
            || Ok(()),
        )
        .unwrap()
        .publication()
        .clone();
        reset_embedding_mutation_journal(
            &graph.dir,
            &publication.manifest,
            EmbeddingMutationJournalLimits::default(),
            SearchCoordinationLimits::default(),
            || Ok(()),
        )
        .unwrap();
        graph
            .bind_embedding_space_alias(
                "semantic",
                &descriptor.compatibility_id().unwrap().to_hex(),
                false,
            )
            .unwrap();
        publication
    }

    #[test]
    fn fresh_alias_and_default_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
        let publication = publish(&graph, 2);
        graph.set_default_embedding_space(Some("semantic")).unwrap();

        let fresh = graph
            .inspect_embedding_space_freshness(Some("semantic"), false)
            .unwrap();
        assert_eq!(fresh.state, EmbeddingSpaceFreshnessState::Fresh);
        assert_eq!(fresh.reason, None);
        assert_eq!(fresh.decision, EmbeddingSpaceReadDecision::ServeFresh);
        assert_eq!(
            fresh.generation_id,
            publication.manifest.generation_id().to_hex()
        );

        drop(graph);
        let reopened = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
        assert_eq!(
            reopened
                .inspect_embedding_space_freshness(None, false)
                .unwrap(),
            fresh
        );
    }

    #[test]
    fn unproven_generation_advance_blocks_unless_explicitly_forced() {
        let graph = GraphForge::new(None).unwrap();
        publish(&graph, 2);
        bump_search_generation(&graph.dir).unwrap();

        let blocked = graph
            .inspect_embedding_space_freshness(Some("semantic"), false)
            .unwrap();
        assert_eq!(
            blocked.state,
            EmbeddingSpaceFreshnessState::SubstantiallyStale
        );
        assert_eq!(blocked.reason.as_deref(), Some("unproven_mutation_scope"));
        assert_eq!(
            blocked.decision,
            EmbeddingSpaceReadDecision::RefreshRequired {
                reason: "unproven_mutation_scope".to_owned()
            }
        );

        let forced = graph
            .inspect_embedding_space_freshness(Some("semantic"), true)
            .unwrap();
        let EmbeddingSpaceReadDecision::ServeForcedStale { diagnostic } = forced.decision else {
            panic!("explicit force must produce its stable diagnostic")
        };
        assert!(diagnostic.starts_with("embedding_force_stale:v1 compatibility_id="));
        assert!(diagnostic.ends_with("reason=unproven_mutation_scope"));
    }

    #[test]
    fn proven_small_mutation_is_mildly_stale_and_serveable() {
        let graph = GraphForge::new(None).unwrap();
        let publication = publish(&graph, 100);
        let generation = bump_search_generation(&graph.dir).unwrap();
        let current_source = EmbeddingSourceState::new(generation, [3; 32], [2; 32], 100);
        merge_embedding_mutation_batch(
            &graph.dir,
            &publication.manifest,
            EmbeddingMutationBatch {
                current_source,
                changed_uuids: &[[1; 16]],
                structural_mutation: false,
                scope_proven: true,
            },
            EmbeddingMutationJournalLimits::default(),
            SearchCoordinationLimits::default(),
            || Ok(()),
        )
        .unwrap();

        let stale = graph
            .inspect_embedding_space_freshness(Some("semantic"), false)
            .unwrap();
        assert_eq!(stale.state, EmbeddingSpaceFreshnessState::Stale);
        assert_eq!(stale.reason.as_deref(), Some("relevant_mutation"));
        assert_eq!(
            stale.decision,
            EmbeddingSpaceReadDecision::ServeStale {
                reason: "relevant_mutation".to_owned()
            }
        );
    }

    #[test]
    fn missing_active_generation_stays_structured() {
        let graph = GraphForge::new(None).unwrap();
        let descriptor = descriptor();
        let root = graph
            .dir
            .join("embeddings/spaces")
            .join(descriptor.compatibility_id().unwrap().to_hex());
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(
            root.join("space.json"),
            descriptor.to_canonical_json().unwrap(),
        )
        .unwrap();
        graph
            .bind_embedding_space_alias(
                "semantic",
                &descriptor.compatibility_id().unwrap().to_hex(),
                false,
            )
            .unwrap();

        assert!(matches!(
            graph.inspect_embedding_space_freshness(Some("semantic"), false),
            Err(GfError::Storage(message)) if message.contains("missing")
        ));
    }
}
