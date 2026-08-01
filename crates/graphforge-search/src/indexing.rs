//! Typed explicit indexing coordinator over the text and vector lifecycles.

use std::path::Path;

use graphforge_storage::{SearchArtifactError, SearchPublicationMode, SearchPublicationOutcome};

use crate::lifecycle::{
    LazyTextRequest, TextIndexPreparation, TextIndexRequest, TextLifecycleLimits,
    prepare_default_text_index, prepare_explicit_text_index,
};
use crate::vector_lifecycle::{VectorIndexRequest, VectorLifecycleLimits, upsert_graph_vector};

/// Combined limits for typed text and vector indexing.
#[derive(Clone, Copy, Debug, Default)]
pub struct SearchIndexLimits {
    /// Text discovery, build, persistence, and coordination bounds.
    pub text: TextLifecycleLimits,
    /// Vector persistence, membership, and coordination bounds.
    pub vector: VectorLifecycleLimits,
}

/// One caller-resolved, statically distinct search indexing request.
#[derive(Clone, Copy, Debug)]
pub enum SearchIndexRequest<'a> {
    /// Build, reuse, or replace one text artifact.
    Text {
        /// Normalized graph label persisted in the artifact key.
        label: &'a str,
        /// Local catalog identity used only for graph membership projection.
        label_id: u32,
        /// Explicit properties, or `None` for stable default discovery.
        properties: Option<&'a [String]>,
        /// Force atomic replacement even when an exact fresh artifact exists.
        rebuild: bool,
    },
    /// Insert or replace one UUID-keyed vector.
    Vector {
        /// Normalized graph label persisted in the artifact key.
        label: &'a str,
        /// Local catalog identity used only for graph membership projection.
        label_id: u32,
        /// Stable graph UUID resolved by the caller.
        node_uuid: [u8; 16],
        /// Finite, non-zero fixed-dimension vector.
        vector: &'a [f32],
        /// Required normalized vector space.
        space: &'a str,
        /// Diagnostic transaction time in microseconds since Unix epoch.
        updated_at_micros: i64,
    },
}

/// Backend result of one typed search indexing request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SearchIndexOutcome {
    /// Default discovery or explicit text publication.
    Text(TextIndexPreparation),
    /// UUID vector publication or idempotent reuse.
    Vector(SearchPublicationOutcome),
}

/// Coordinate one typed text or vector indexing request.
///
/// Public selector resolution remains outside this backend boundary. Text
/// requests own stable discovery and reuse/rebuild policy; vector requests
/// delegate membership, dimension, idempotence, and replacement semantics to
/// the graph-membership vector lifecycle.
///
/// # Errors
/// Returns structured selector, source, corruption, cancellation, resource,
/// lock, build, I/O, or repeated-concurrent-mutation errors.
pub fn prepare_search_index<C>(
    project_dir: &Path,
    request: SearchIndexRequest<'_>,
    limits: SearchIndexLimits,
    checkpoint: C,
) -> Result<SearchIndexOutcome, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    match request {
        SearchIndexRequest::Text {
            label,
            label_id,
            properties,
            rebuild,
        } => {
            let mode = if rebuild {
                SearchPublicationMode::Replace
            } else {
                SearchPublicationMode::ReuseFresh
            };
            let outcome = match properties {
                Some(properties) => TextIndexPreparation::Published(prepare_explicit_text_index(
                    project_dir,
                    TextIndexRequest {
                        label,
                        label_id,
                        properties,
                    },
                    mode,
                    limits.text,
                    checkpoint,
                )?),
                None => prepare_default_text_index(
                    project_dir,
                    LazyTextRequest { label, label_id },
                    mode,
                    limits.text,
                    checkpoint,
                )?,
            };
            Ok(SearchIndexOutcome::Text(outcome))
        }
        SearchIndexRequest::Vector {
            label,
            label_id,
            node_uuid,
            vector,
            space,
            updated_at_micros,
        } => upsert_graph_vector(
            project_dir,
            VectorIndexRequest {
                label,
                label_id,
                space,
            },
            node_uuid,
            vector,
            updated_at_micros,
            limits.vector,
            checkpoint,
        )
        .map(SearchIndexOutcome::Vector),
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::collections::HashMap;

    use graphforge_core::uuid::Uuid;
    use graphforge_ir::{IrLiteral, OntologyMode, TypeId};
    use graphforge_storage::{
        GraphWriter, SearchPublicationOutcome, generation::bump_search_generation,
    };
    use tempfile::TempDir;

    use super::*;

    fn uuid(value: u8) -> Uuid {
        let mut bytes = [0_u8; 16];
        bytes[15] = value;
        Uuid::from_bytes(bytes)
    }

    fn uuid_bytes(value: u8) -> [u8; 16] {
        *uuid(value).as_bytes()
    }

    fn write_text_graph(dir: &TempDir) {
        let mut writer = GraphWriter::open_at(dir.path(), OntologyMode::Strict, 1).unwrap();
        writer
            .create_node_with_labels(uuid(1), &[TypeId(1), TypeId(9)])
            .unwrap();
        writer
            .set_properties(
                &uuid(1),
                Some("Primary"),
                HashMap::from([
                    (
                        "summary".to_owned(),
                        IrLiteral::Str("Graph search".to_owned()),
                    ),
                    ("age".to_owned(), IrLiteral::Int(30)),
                    ("name".to_owned(), IrLiteral::Str("Alice".to_owned())),
                ]),
            )
            .unwrap();
        writer.flush().unwrap();
    }

    fn text_request<'a>(properties: Option<&'a [String]>, rebuild: bool) -> SearchIndexRequest<'a> {
        SearchIndexRequest::Text {
            label: "Person",
            label_id: 9,
            properties,
            rebuild,
        }
    }

    fn vector_request(vector: &[f32], updated_at_micros: i64) -> SearchIndexRequest<'_> {
        SearchIndexRequest::Vector {
            label: "Person",
            label_id: 9,
            node_uuid: uuid_bytes(1),
            vector,
            space: "semantic",
            updated_at_micros,
        }
    }

    fn published_text(outcome: SearchIndexOutcome) -> crate::PublishedTextIndex {
        match outcome {
            SearchIndexOutcome::Text(TextIndexPreparation::Published(index)) => index,
            other => panic!("expected a published text index, found {other:?}"),
        }
    }

    #[test]
    fn default_text_uses_exact_key_reuses_and_rebuilds() {
        let dir = TempDir::new().unwrap();
        write_text_graph(&dir);
        let first = published_text(
            prepare_search_index(
                dir.path(),
                text_request(None, false),
                SearchIndexLimits::default(),
                || Ok(()),
            )
            .unwrap(),
        );
        assert_eq!(
            first.artifact().manifest.properties.as_deref().unwrap(),
            ["name", "summary"]
        );
        let reused = published_text(
            prepare_search_index(
                dir.path(),
                text_request(None, false),
                SearchIndexLimits::default(),
                || Ok(()),
            )
            .unwrap(),
        );
        assert_eq!(reused.artifact().path, first.artifact().path);
        let rebuilt = published_text(
            prepare_search_index(
                dir.path(),
                text_request(None, true),
                SearchIndexLimits::default(),
                || Ok(()),
            )
            .unwrap(),
        );
        assert_ne!(rebuilt.artifact().path, first.artifact().path);
    }

    #[test]
    fn no_text_and_invalid_explicit_properties_are_structured() {
        let empty = TempDir::new().unwrap();
        let outcome = prepare_search_index(
            empty.path(),
            text_request(None, false),
            SearchIndexLimits::default(),
            || Ok(()),
        )
        .unwrap();
        assert_eq!(
            outcome,
            SearchIndexOutcome::Text(TextIndexPreparation::NoTextProperties)
        );
        assert!(!empty.path().join("indexes/search").exists());

        let dir = TempDir::new().unwrap();
        write_text_graph(&dir);
        for properties in [
            Vec::new(),
            vec!["name".to_owned(), " name ".to_owned()],
            vec!["missing".to_owned()],
            vec!["age".to_owned()],
        ] {
            assert!(matches!(
                prepare_search_index(
                    dir.path(),
                    text_request(Some(&properties), false),
                    SearchIndexLimits::default(),
                    || Ok(()),
                ),
                Err(SearchArtifactError::InvalidSelector { .. })
            ));
        }
        let properties = vec!["summary".to_owned(), "name".to_owned()];
        let index = published_text(
            prepare_search_index(
                dir.path(),
                text_request(Some(&properties), false),
                SearchIndexLimits::default(),
                || Ok(()),
            )
            .unwrap(),
        );
        assert_eq!(
            index.artifact().manifest.properties.as_deref().unwrap(),
            ["name", "summary"]
        );
    }

    #[test]
    fn vector_dispatch_preserves_insert_idempotence_and_replacement() {
        let dir = TempDir::new().unwrap();
        let mut writer = GraphWriter::open_at(dir.path(), OntologyMode::Strict, 1).unwrap();
        writer.create_node(uuid(1), TypeId(9)).unwrap();
        writer.flush().unwrap();
        let first = prepare_search_index(
            dir.path(),
            vector_request(&[1.0, 0.0], 1),
            SearchIndexLimits::default(),
            || Ok(()),
        )
        .unwrap();
        let first_path = match first {
            SearchIndexOutcome::Vector(SearchPublicationOutcome::Published {
                artifact, ..
            }) => artifact.path,
            other => panic!("expected vector publication, found {other:?}"),
        };
        let repeated = prepare_search_index(
            dir.path(),
            vector_request(&[1.0, 0.0], 2),
            SearchIndexLimits::default(),
            || Ok(()),
        )
        .unwrap();
        assert!(matches!(
            repeated,
            SearchIndexOutcome::Vector(SearchPublicationOutcome::Reused(ref artifact))
                if artifact.path == first_path
        ));
        let replaced = prepare_search_index(
            dir.path(),
            vector_request(&[0.0, 1.0], 3),
            SearchIndexLimits::default(),
            || Ok(()),
        )
        .unwrap();
        assert!(matches!(
            replaced,
            SearchIndexOutcome::Vector(SearchPublicationOutcome::Published { artifact, .. })
                if artifact.path != first_path
        ));
    }

    #[test]
    fn cancellation_and_repeated_discovery_mutation_return_no_outcome() {
        let dir = TempDir::new().unwrap();
        assert!(matches!(
            prepare_search_index(
                dir.path(),
                text_request(None, false),
                SearchIndexLimits::default(),
                || Err(SearchArtifactError::Cancelled),
            ),
            Err(SearchArtifactError::Cancelled)
        ));

        write_text_graph(&dir);
        let checks = Cell::new(0_u8);
        let raced = prepare_search_index(
            dir.path(),
            text_request(None, false),
            SearchIndexLimits::default(),
            || {
                checks.set(checks.get().saturating_add(1));
                bump_search_generation(dir.path()).unwrap();
                Ok(())
            },
        );
        assert!(matches!(
            raced,
            Err(SearchArtifactError::ConcurrentMutation)
        ));
    }
}
