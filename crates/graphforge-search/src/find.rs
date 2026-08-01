//! Unified graph-native text, vector, and hybrid backend orchestration.

use std::path::Path;

use graphforge_storage::{
    SearchArtifactError, SearchArtifactKey, generation::read_search_generation, validate_vector,
};

use crate::MAX_FUSION_RESULTS;
use crate::analyzer::analyze_query;
use crate::fusion::{FusedSearchHit, MatchedOn, SearchChannelHit, reciprocal_rank_fusion};
use crate::lifecycle::{LazyTextRequest, TextLifecycleLimits, search_default_text};
use crate::vector_lifecycle::{VectorIndexRequest, VectorLifecycleLimits, search_graph_vectors};

/// Complete backend request after the public facade resolves a required label.
#[derive(Clone, Copy, Debug)]
pub struct FindSearchRequest<'a> {
    /// Normalized graph label.
    pub label: &'a str,
    /// Local catalog identity used only for graph membership projection.
    pub label_id: u32,
    /// Optional non-empty plain-text query.
    pub query: Option<&'a str>,
    /// Optional finite, non-zero vector query.
    pub vector: Option<&'a [f32]>,
    /// Required caller-defined space when `vector` is present.
    pub space: Option<&'a str>,
    /// Maximum results and per-channel hybrid candidate depth.
    pub limit: usize,
}

/// Resource bounds for unified backend search.
#[derive(Clone, Copy, Debug, Default)]
pub struct FindSearchLimits {
    /// Lazy text lifecycle and Tantivy bounds.
    pub text: TextLifecycleLimits,
    /// UUID vector lifecycle and exact-cosine bounds.
    pub vector: VectorLifecycleLimits,
}

#[derive(Clone, Copy, Debug)]
enum FindMode<'a> {
    Text(&'a str),
    Vector {
        vector: &'a [f32],
        space: &'a str,
    },
    Hybrid {
        query: &'a str,
        vector: &'a [f32],
        space: &'a str,
    },
}

/// Search one required graph label through the text, vector, or hybrid backend.
///
/// The complete operation is enclosed by a search-generation check. A source
/// mutation between channel reads retries the full request once; a second race
/// fails without returning partial hits.
///
/// # Errors
/// Returns structured option, lifecycle, corruption, cancellation, resource,
/// build, lock, source, or I/O errors from the shared search stack.
pub fn search_graph_native<C>(
    project_dir: &Path,
    request: FindSearchRequest<'_>,
    limits: FindSearchLimits,
    checkpoint: C,
) -> Result<Vec<FusedSearchHit>, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    search_graph_native_with_generation(project_dir, request, limits, checkpoint, |project_dir| {
        read_search_generation(project_dir).map_err(|error| SearchArtifactError::SourceSnapshot {
            reason: error.to_string(),
        })
    })
}

fn search_graph_native_with_generation<C, G>(
    project_dir: &Path,
    request: FindSearchRequest<'_>,
    limits: FindSearchLimits,
    mut checkpoint: C,
    mut generation: G,
) -> Result<Vec<FusedSearchHit>, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
    G: FnMut(&Path) -> Result<u64, SearchArtifactError>,
{
    let mode = validate_request(request, limits)?;
    for attempt in 1_u8..=2 {
        checkpoint()?;
        let before = generation(project_dir)?;
        let hits = search_once(project_dir, request, mode, limits, &mut checkpoint)?;
        checkpoint()?;
        if before == generation(project_dir)? {
            return Ok(hits);
        }
        if attempt == 2 {
            return Err(SearchArtifactError::ConcurrentMutation);
        }
    }
    unreachable!("the bounded unified search loop returns on both terminal paths")
}

fn validate_request(
    request: FindSearchRequest<'_>,
    limits: FindSearchLimits,
) -> Result<FindMode<'_>, SearchArtifactError> {
    SearchArtifactKey::text(request.label, ["_"])?;
    if request.limit == 0 {
        return Err(invalid("limit", "must be greater than zero"));
    }
    if request.limit > MAX_FUSION_RESULTS {
        return Err(SearchArtifactError::ResourceExhausted {
            resource: "search_results",
            limit: MAX_FUSION_RESULTS as u64,
        });
    }
    if request.query.is_some_and(|query| query.trim().is_empty()) {
        return Err(invalid("query", "must not be blank"));
    }

    match (request.query, request.vector, request.space) {
        (Some(query), None, None) => {
            analyze_query(query, limits.text.text)?;
            Ok(FindMode::Text(query))
        }
        (None, Some(vector), Some(space)) => {
            SearchArtifactKey::vector(request.label, space)?;
            validate_vector(vector, limits.vector.vector)?;
            Ok(FindMode::Vector { vector, space })
        }
        (Some(query), Some(vector), Some(space)) => {
            SearchArtifactKey::vector(request.label, space)?;
            analyze_query(query, limits.text.text)?;
            validate_vector(vector, limits.vector.vector)?;
            Ok(FindMode::Hybrid {
                query,
                vector,
                space,
            })
        }
        (None, None, None) => Err(invalid("query", "text query or vector is required")),
        (_, None, Some(_)) => Err(invalid("space", "requires a vector query")),
        (_, Some(_), None) => Err(invalid("space", "is required with a vector query")),
    }
}

fn search_once<C>(
    project_dir: &Path,
    request: FindSearchRequest<'_>,
    mode: FindMode<'_>,
    limits: FindSearchLimits,
    checkpoint: &mut C,
) -> Result<Vec<FusedSearchHit>, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    match mode {
        FindMode::Text(query) => search_text(project_dir, request, query, limits, checkpoint),
        FindMode::Vector { vector, space } => {
            search_vector(project_dir, request, vector, space, limits, checkpoint)
        }
        FindMode::Hybrid {
            query,
            vector,
            space,
        } => {
            let text = search_default_text(
                project_dir,
                LazyTextRequest {
                    label: request.label,
                    label_id: request.label_id,
                },
                query,
                request.limit,
                limits.text,
                &mut *checkpoint,
            )?;
            checkpoint()?;
            let vector = search_graph_vectors(
                project_dir,
                VectorIndexRequest {
                    label: request.label,
                    label_id: request.label_id,
                    space,
                },
                vector,
                request.limit,
                limits.vector,
                &mut *checkpoint,
            )?;
            let text = text
                .into_iter()
                .map(|hit| SearchChannelHit {
                    node_uuid: hit.node_uuid,
                    score: hit.score,
                })
                .collect::<Vec<_>>();
            let vector = vector
                .into_iter()
                .map(|hit| SearchChannelHit {
                    node_uuid: hit.node_uuid,
                    score: hit.score,
                })
                .collect::<Vec<_>>();
            reciprocal_rank_fusion(&text, &vector, request.limit)
        }
    }
}

fn search_text<C>(
    project_dir: &Path,
    request: FindSearchRequest<'_>,
    query: &str,
    limits: FindSearchLimits,
    checkpoint: &mut C,
) -> Result<Vec<FusedSearchHit>, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    search_default_text(
        project_dir,
        LazyTextRequest {
            label: request.label,
            label_id: request.label_id,
        },
        query,
        request.limit,
        limits.text,
        checkpoint,
    )
    .map(|hits| {
        hits.into_iter()
            .map(|hit| FusedSearchHit {
                node_uuid: hit.node_uuid,
                score: hit.score,
                matched_on: MatchedOn::Text,
            })
            .collect()
    })
}

fn search_vector<C>(
    project_dir: &Path,
    request: FindSearchRequest<'_>,
    vector: &[f32],
    space: &str,
    limits: FindSearchLimits,
    checkpoint: &mut C,
) -> Result<Vec<FusedSearchHit>, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    search_graph_vectors(
        project_dir,
        VectorIndexRequest {
            label: request.label,
            label_id: request.label_id,
            space,
        },
        vector,
        request.limit,
        limits.vector,
        checkpoint,
    )
    .map(|hits| {
        hits.into_iter()
            .map(|hit| FusedSearchHit {
                node_uuid: hit.node_uuid,
                score: hit.score,
                matched_on: MatchedOn::Vector,
            })
            .collect()
    })
}

fn invalid(field: &'static str, reason: impl Into<String>) -> SearchArtifactError {
    SearchArtifactError::InvalidSelector {
        field,
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::collections::HashMap;

    use graphforge_core::uuid::Uuid;
    use graphforge_ir::{IrLiteral, OntologyMode, TypeId};
    use graphforge_storage::GraphWriter;
    use tempfile::TempDir;

    use super::*;
    use crate::upsert_graph_vector;

    const LABEL: &str = "Person";
    const LABEL_ID: u32 = 9;

    fn uuid(value: u8) -> Uuid {
        let mut bytes = [0_u8; 16];
        bytes[15] = value;
        Uuid::from_bytes(bytes)
    }

    fn request<'a>(
        query: Option<&'a str>,
        vector: Option<&'a [f32]>,
        space: Option<&'a str>,
        limit: usize,
    ) -> FindSearchRequest<'a> {
        FindSearchRequest {
            label: LABEL,
            label_id: LABEL_ID,
            query,
            vector,
            space,
            limit,
        }
    }

    fn write_people(dir: &TempDir) {
        let mut writer = GraphWriter::open_at(dir.path(), OntologyMode::Strict, 1).unwrap();
        for value in [2_u8, 1] {
            writer.create_node(uuid(value), TypeId(LABEL_ID)).unwrap();
            writer
                .set_properties(
                    &uuid(value),
                    Some(LABEL),
                    HashMap::from([("name".to_owned(), IrLiteral::Str("shared term".to_owned()))]),
                )
                .unwrap();
        }
        writer.flush().unwrap();
        for (value, vector) in [(1_u8, [1.0, 0.0]), (2_u8, [0.0, 1.0])] {
            upsert_graph_vector(
                dir.path(),
                VectorIndexRequest {
                    label: LABEL,
                    label_id: LABEL_ID,
                    space: "semantic",
                },
                *uuid(value).as_bytes(),
                &vector,
                i64::from(value),
                VectorLifecycleLimits::default(),
                || Ok(()),
            )
            .unwrap();
        }
    }

    #[test]
    fn validates_every_mode_before_source_work() {
        let path = Path::new("/path/that/must/not/be/read");
        for invalid_request in [
            request(None, None, None, 10),
            request(Some(""), None, None, 10),
            request(None, Some(&[1.0]), None, 10),
            request(None, None, Some("semantic"), 10),
            request(Some("text"), None, Some("semantic"), 10),
            request(Some("text"), Some(&[1.0]), None, 10),
            request(Some("---"), None, None, 10),
            request(None, Some(&[0.0]), Some("semantic"), 10),
            request(Some("text"), None, None, 0),
        ] {
            assert!(matches!(
                search_graph_native(
                    path,
                    invalid_request,
                    FindSearchLimits::default(),
                    || Ok(())
                ),
                Err(SearchArtifactError::InvalidSelector { .. })
            ));
        }
        assert!(matches!(
            search_graph_native(
                path,
                request(Some("text"), None, None, MAX_FUSION_RESULTS + 1),
                FindSearchLimits::default(),
                || Ok(())
            ),
            Err(SearchArtifactError::ResourceExhausted {
                resource: "search_results",
                ..
            })
        ));
    }

    #[test]
    fn preserves_single_channel_scores_and_fuses_hybrid_ranks() {
        let dir = TempDir::new().unwrap();
        write_people(&dir);

        let text = search_graph_native(
            dir.path(),
            request(Some("shared"), None, None, 2),
            FindSearchLimits::default(),
            || Ok(()),
        )
        .unwrap();
        assert_eq!(text.len(), 2);
        assert_eq!(text[0].node_uuid, *uuid(1).as_bytes());
        assert_eq!(text[0].score, text[1].score);
        assert!(text.iter().all(|hit| hit.matched_on == MatchedOn::Text));

        let vector = search_graph_native(
            dir.path(),
            request(None, Some(&[0.0, 1.0]), Some("semantic"), 2),
            FindSearchLimits::default(),
            || Ok(()),
        )
        .unwrap();
        assert_eq!(vector[0].node_uuid, *uuid(2).as_bytes());
        assert_eq!(vector[0].score, 1.0);
        assert_eq!(vector[1].score, 0.0);
        assert!(vector.iter().all(|hit| hit.matched_on == MatchedOn::Vector));

        let hybrid = search_graph_native(
            dir.path(),
            request(Some("shared"), Some(&[0.0, 1.0]), Some("semantic"), 2),
            FindSearchLimits::default(),
            || Ok(()),
        )
        .unwrap();
        assert_eq!(
            hybrid.iter().map(|hit| hit.node_uuid).collect::<Vec<_>>(),
            [*uuid(1).as_bytes(), *uuid(2).as_bytes()]
        );
        assert_eq!(hybrid[0].score, 1.0 / 61.0 + 1.0 / 62.0);
        assert_eq!(hybrid[0].score, hybrid[1].score);
        assert!(
            hybrid
                .iter()
                .all(|hit| hit.matched_on == MatchedOn::TextAndVector)
        );
    }

    #[test]
    fn empty_channels_and_cancellation_are_stable() {
        let dir = TempDir::new().unwrap();
        write_people(&dir);

        assert!(
            search_graph_native(
                dir.path(),
                request(None, Some(&[1.0, 0.0]), Some("missing"), 10),
                FindSearchLimits::default(),
                || Ok(())
            )
            .unwrap()
            .is_empty()
        );
        let empty_label = FindSearchRequest {
            label: "Empty",
            label_id: 44,
            query: Some("shared"),
            vector: None,
            space: None,
            limit: 10,
        };
        assert!(
            search_graph_native(dir.path(), empty_label, FindSearchLimits::default(), || Ok(
                ()
            ))
            .unwrap()
            .is_empty()
        );
        assert!(matches!(
            search_graph_native(
                dir.path(),
                request(Some("shared"), None, None, 2),
                FindSearchLimits::default(),
                || Err(SearchArtifactError::Cancelled)
            ),
            Err(SearchArtifactError::Cancelled)
        ));
    }

    #[test]
    fn retries_one_combined_generation_change_and_rejects_two() {
        let dir = TempDir::new().unwrap();
        write_people(&dir);
        let calls = Cell::new(0_u8);
        let hits = search_graph_native_with_generation(
            dir.path(),
            request(None, Some(&[1.0, 0.0]), Some("semantic"), 1),
            FindSearchLimits::default(),
            || Ok(()),
            |_| {
                let call = calls.get();
                calls.set(call + 1);
                Ok([1_u64, 2, 2, 2][usize::from(call)])
            },
        )
        .unwrap();
        assert_eq!(hits[0].node_uuid, *uuid(1).as_bytes());
        assert_eq!(calls.get(), 4);

        let calls = Cell::new(0_u64);
        assert!(matches!(
            search_graph_native_with_generation(
                dir.path(),
                request(None, Some(&[1.0, 0.0]), Some("semantic"), 1),
                FindSearchLimits::default(),
                || Ok(()),
                |_| {
                    let next = calls.get();
                    calls.set(next + 1);
                    Ok(next)
                },
            ),
            Err(SearchArtifactError::ConcurrentMutation)
        ));
        assert_eq!(calls.get(), 4);
    }
}
