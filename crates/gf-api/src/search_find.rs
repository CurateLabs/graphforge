//! Public graph-native text and complete-generation vector retrieval.

use gf_search::{
    EmbeddingGenerationQuery, EmbeddingVectorQuery, FindSearchLimits, FindSearchRequest,
    FusedSearchHit, MatchedOn, SearchChannelHit, VectorLifecycleLimits, reciprocal_rank_fusion,
    search_embedding_generation, search_graph_native,
};
use gf_storage::{SearchArtifactError, generation::read_search_generation};

use super::search_output::shape_search_output;
use super::{FindOptions, GfError, GraphForge};

enum VectorQuery {
    Raw(Vec<f32>),
    Node([u8; 16]),
}

impl GraphForge {
    /// Search one required label by text, complete-generation vector, or RRF hybrid retrieval.
    ///
    /// Raw and existing-node vectors select one explicit complete embedding-space alias.
    /// Substantially stale generations fail unless `force_stale` explicitly selects the last
    /// otherwise-valid complete generation. Semantic queries resolve one exact process-local
    /// provider runtime before outbound work.
    ///
    /// # Errors
    /// Returns structured option, label, selector, freshness, dimension, corruption, lifecycle,
    /// cancellation, resource, storage, or Arrow-shaping errors without partial rows.
    pub fn find(&self, options: FindOptions) -> Result<arrow::record_batch::RecordBatch, GfError> {
        if options.semantic_query.is_some() {
            return self.find_with_configured_provider(options);
        }
        let FindOptions {
            query,
            label,
            vector,
            similar_to,
            semantic_query: _,
            limit,
            space,
            force_stale,
        } = options;
        let label = label.ok_or_else(|| validation("find requires label"))?;
        let label_id = self.search_label_id(&label)?;
        let vector_forms = usize::from(vector.is_some()) + usize::from(similar_to.is_some());
        if vector_forms > 1 {
            return Err(validation("vector and similar_to are mutually exclusive"));
        }
        let vector_query = match (vector, similar_to) {
            (Some(vector), None) => Some(VectorQuery::Raw(vector)),
            (None, Some(selector)) => Some(VectorQuery::Node(
                *self.resolve_node_selector(&selector)?.as_bytes(),
            )),
            (None, None) => None,
            _ => unreachable!("multiple vector forms rejected above"),
        };
        match (&vector_query, space.as_deref()) {
            (Some(_), None) => return Err(validation("vector query form requires space")),
            (None, Some(_)) => return Err(validation("space requires a vector query form")),
            (None, None) if force_stale => {
                return Err(validation("force_stale requires a vector query form"));
            }
            _ => {}
        }
        if query.is_none() && vector_query.is_none() {
            return Err(validation("find requires text or vector retrieval"));
        }
        for attempt in 1_u8..=2 {
            let before = read_search_generation(&self.dir)?;
            let hits = self.retrieve_find_hits(
                &label,
                label_id,
                query.as_deref(),
                vector_query.as_ref(),
                space.as_deref(),
                force_stale,
                limit,
            )?;
            let batch = shape_search_output(&self.dir, label_id, &hits)?;
            if before == read_search_generation(&self.dir)? {
                return Ok(batch);
            }
            if attempt == 2 {
                return Err(SearchArtifactError::ConcurrentMutation.into());
            }
        }
        unreachable!("bounded find retry returns on both terminal paths")
    }

    #[allow(clippy::too_many_arguments)]
    fn retrieve_find_hits(
        &self,
        label: &str,
        label_id: u32,
        query: Option<&str>,
        vector_query: Option<&VectorQuery>,
        space: Option<&str>,
        force_stale: bool,
        limit: usize,
    ) -> Result<Vec<FusedSearchHit>, GfError> {
        let text = query
            .map(|query| {
                search_graph_native(
                    &self.dir,
                    FindSearchRequest {
                        label,
                        label_id,
                        query: Some(query),
                        vector: None,
                        space: None,
                        limit,
                    },
                    FindSearchLimits::default(),
                    || Ok(()),
                )
            })
            .transpose()?;
        let vector = vector_query
            .map(|vector_query| {
                let prepared = self.prepare_embedding_space_read(space, force_stale)?;
                let query = match vector_query {
                    VectorQuery::Raw(vector) => EmbeddingVectorQuery::Raw(vector),
                    VectorQuery::Node(node_uuid) => EmbeddingVectorQuery::Node(*node_uuid),
                };
                search_embedding_generation(
                    &self.dir,
                    EmbeddingGenerationQuery {
                        prepared: &prepared,
                        label_id,
                        query,
                        limit,
                    },
                    VectorLifecycleLimits::default(),
                    || Ok(()),
                )
                .map_err(GfError::from)
            })
            .transpose()?;

        match (text, vector) {
            (Some(text), Some(vector)) => reciprocal_rank_fusion(
                &text
                    .into_iter()
                    .map(|hit| SearchChannelHit {
                        node_uuid: hit.node_uuid,
                        score: hit.score,
                    })
                    .collect::<Vec<_>>(),
                &vector
                    .into_iter()
                    .map(|hit| SearchChannelHit {
                        node_uuid: hit.node_uuid,
                        score: hit.score,
                    })
                    .collect::<Vec<_>>(),
                limit,
            )
            .map_err(Into::into),
            (Some(text), None) => Ok(text),
            (None, Some(vector)) => Ok(vector
                .into_iter()
                .map(|hit| FusedSearchHit {
                    node_uuid: hit.node_uuid,
                    score: hit.score,
                    matched_on: MatchedOn::Vector,
                })
                .collect()),
            (None, None) => unreachable!("at least one retrieval channel validated"),
        }
    }
}

fn validation(message: impl Into<String>) -> GfError {
    GfError::Validation(message.into())
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use arrow::array::{FixedSizeBinaryArray, Float64Array, StringArray};

    use super::*;
    use crate::{
        CallerEmbeddingBatchRequest, CallerEmbeddingBatchRow, CallerEmbeddingDistance,
        CallerEmbeddingNormalization, NodeHandle, NodeSelector, PropValue,
    };

    fn node(graph: &GraphForge, title: &str) -> NodeHandle {
        graph
            .add_node(
                "Paper",
                &HashMap::from([("title".to_owned(), PropValue::Str(title.to_owned()))]),
            )
            .unwrap()
    }

    fn publish(graph: &GraphForge, nodes: &[(&NodeHandle, [f32; 2])]) {
        graph
            .publish_caller_embeddings(CallerEmbeddingBatchRequest {
                display_name: "semantic".to_owned(),
                contract_version: "test-v1".to_owned(),
                dimensions: 2,
                normalization: CallerEmbeddingNormalization::None,
                distance: CallerEmbeddingDistance::Cosine,
                source_projection_recipe: BTreeMap::from([(
                    "label".to_owned(),
                    "Paper".to_owned(),
                )]),
                rows: nodes
                    .iter()
                    .map(|(node, vector)| CallerEmbeddingBatchRow {
                        node: NodeSelector::Handle((*node).clone()),
                        vector: vector.to_vec(),
                    })
                    .collect(),
                replace_alias: false,
            })
            .unwrap();
    }

    fn options() -> FindOptions {
        FindOptions {
            label: Some("Paper".to_owned()),
            limit: 3,
            ..FindOptions::default()
        }
    }

    fn uuids(batch: &arrow::record_batch::RecordBatch) -> Vec<[u8; 16]> {
        batch
            .column_by_name("node_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .iter()
            .map(|value| value.unwrap().try_into().unwrap())
            .collect()
    }

    fn channels(batch: &arrow::record_batch::RecordBatch) -> Vec<&str> {
        batch
            .column_by_name("matched_on")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(Option::unwrap)
            .collect()
    }

    #[test]
    fn executes_text_raw_node_and_hybrid_modes_as_arrow() {
        let graph = GraphForge::new(None).unwrap();
        let alice = node(&graph, "alpha systems");
        let bob = node(&graph, "beta systems");
        let cara = node(&graph, "graph search");
        publish(
            &graph,
            &[
                (&alice, [1.0, 0.0]),
                (&bob, [0.0, 1.0]),
                (&cara, [1.0, 1.0]),
            ],
        );

        let text = graph
            .find(FindOptions {
                query: Some("graph".to_owned()),
                ..options()
            })
            .unwrap();
        assert_eq!(uuids(&text), [*cara.uuid.as_bytes()]);
        assert_eq!(channels(&text), ["text"]);

        let raw = graph
            .find(FindOptions {
                vector: Some(vec![1.0, 0.0]),
                space: Some("semantic".to_owned()),
                ..options()
            })
            .unwrap();
        assert_eq!(uuids(&raw)[0], *alice.uuid.as_bytes());
        assert_eq!(channels(&raw), ["vector", "vector", "vector"]);
        let scores = raw
            .column_by_name("score")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(scores.value(0), 1.0);

        let by_node = graph
            .find(FindOptions {
                similar_to: Some(NodeSelector::Handle(bob.clone())),
                space: Some("semantic".to_owned()),
                ..options()
            })
            .unwrap();
        assert_eq!(uuids(&by_node)[0], *bob.uuid.as_bytes());

        let hybrid = graph
            .find(FindOptions {
                query: Some("graph".to_owned()),
                vector: Some(vec![1.0, 0.0]),
                space: Some("semantic".to_owned()),
                ..options()
            })
            .unwrap();
        assert_eq!(uuids(&hybrid)[0], *cara.uuid.as_bytes());
        assert_eq!(channels(&hybrid)[0], "text+vector");
        assert_eq!(
            hybrid
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            ["node_uuid", "title", "score", "matched_on"]
        );
    }

    #[test]
    fn validates_query_forms_and_blocks_substantial_staleness_unless_forced() {
        let graph = GraphForge::new(None).unwrap();
        let alice = node(&graph, "alpha");
        publish(&graph, &[(&alice, [1.0, 0.0])]);

        for invalid in [
            FindOptions::default(),
            FindOptions {
                label: Some("Paper".to_owned()),
                vector: Some(vec![1.0, 0.0]),
                ..FindOptions::default()
            },
            FindOptions {
                label: Some("Paper".to_owned()),
                query: Some("alpha".to_owned()),
                space: Some("semantic".to_owned()),
                ..FindOptions::default()
            },
            FindOptions {
                label: Some("Paper".to_owned()),
                vector: Some(vec![1.0, 0.0]),
                similar_to: Some(NodeSelector::Handle(alice.clone())),
                space: Some("semantic".to_owned()),
                ..FindOptions::default()
            },
        ] {
            assert!(matches!(graph.find(invalid), Err(GfError::Validation(_))));
        }
        assert!(matches!(
            graph.find(FindOptions {
                label: Some("Paper".to_owned()),
                semantic_query: Some("alpha".to_owned()),
                space: Some("semantic".to_owned()),
                ..FindOptions::default()
            }),
            Err(GfError::Validation(_))
        ));

        node(&graph, "new mutation");
        let ordinary = graph.find(FindOptions {
            vector: Some(vec![1.0, 0.0]),
            space: Some("semantic".to_owned()),
            ..options()
        });
        assert!(matches!(ordinary, Err(GfError::Storage(_))));
        let forced_options = FindOptions {
            vector: Some(vec![1.0, 0.0]),
            space: Some("semantic".to_owned()),
            force_stale: true,
            ..options()
        };
        let forced = graph.find(forced_options.clone()).unwrap();
        assert_eq!(uuids(&forced), [*alice.uuid.as_bytes()]);
        let detailed = graph
            .find_with_diagnostics(
                crate::FindExecutionOptions {
                    find: forced_options,
                    ..crate::FindExecutionOptions::default()
                },
                None,
            )
            .unwrap();
        let (_, diagnostics, _) = detailed.into_parts();
        assert!(matches!(
            diagnostics.as_slice(),
            [crate::FindDiagnostic::ForcedStale { diagnostic }]
                if diagnostic.starts_with("embedding_force_stale:v1 ")
        ));
    }
}
