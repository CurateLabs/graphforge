//! Canonical committed graph-source capture for embedding generations.

use std::path::Path;

use graphforge_storage::{
    EmbeddingSourceState, SearchArtifactError, SearchSourcePart, canonical_source_fingerprint,
    read_search_generation,
};

const FINGERPRINT_PREFIX: &str = "gf-fnv1a256:";
const FINGERPRINT_HEX_BYTES: usize = 64;

/// Resource limits for one committed embedding-source capture.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EmbeddingSourceCaptureLimits {
    /// Maximum named source parts across label membership and dependencies.
    pub parts: usize,
    /// Maximum combined bytes across source-part names and values.
    pub bytes: u64,
    /// Maximum eligible UUID count represented by the captured source.
    pub eligible_uuids: u64,
}

impl Default for EmbeddingSourceCaptureLimits {
    fn default() -> Self {
        Self {
            parts: 256,
            bytes: 64 * 1024 * 1024,
            eligible_uuids: 1_000_000,
        }
    }
}

/// Capture the exact committed graph state consumed by an embedding producer.
///
/// Label-membership inputs and dependency inputs are fingerprinted separately,
/// preserving the distinction required by freshness classification. Logical
/// source-part order is irrelevant; names and exact committed bytes define the
/// canonical state.
///
/// # Errors
/// Returns structured validation, resource, cancellation, or storage errors.
pub fn capture_embedding_source<C>(
    project_dir: &Path,
    label_membership: &[SearchSourcePart<'_>],
    dependency_inputs: &[SearchSourcePart<'_>],
    eligible_uuid_count: u64,
    limits: EmbeddingSourceCaptureLimits,
    mut checkpoint: C,
) -> Result<EmbeddingSourceState, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    checkpoint()?;
    validate_limits(limits)?;
    if eligible_uuid_count > limits.eligible_uuids {
        return Err(exhausted(
            "embedding_source_eligible_uuids",
            limits.eligible_uuids,
        ));
    }

    let part_count = label_membership
        .len()
        .checked_add(dependency_inputs.len())
        .ok_or_else(|| exhausted("embedding_source_parts", limits.parts as u64))?;
    if part_count > limits.parts {
        return Err(exhausted("embedding_source_parts", limits.parts as u64));
    }

    let mut consumed_bytes = 0_u64;
    for part in label_membership.iter().chain(dependency_inputs) {
        checkpoint()?;
        let part_bytes = part
            .name
            .len()
            .checked_add(part.bytes.len())
            .and_then(|count| u64::try_from(count).ok())
            .ok_or_else(|| exhausted("embedding_source_bytes", limits.bytes))?;
        consumed_bytes = consumed_bytes
            .checked_add(part_bytes)
            .ok_or_else(|| exhausted("embedding_source_bytes", limits.bytes))?;
        if consumed_bytes > limits.bytes {
            return Err(exhausted("embedding_source_bytes", limits.bytes));
        }
    }

    let label_membership_digest = source_digest(label_membership)?;
    checkpoint()?;
    let dependency_input_digest = source_digest(dependency_inputs)?;
    checkpoint()?;
    let graph_generation = read_search_generation(project_dir).map_err(|error| {
        SearchArtifactError::SourceSnapshot {
            reason: error.to_string(),
        }
    })?;
    checkpoint()?;

    Ok(EmbeddingSourceState::new(
        graph_generation,
        label_membership_digest,
        dependency_input_digest,
        eligible_uuid_count,
    ))
}

fn validate_limits(limits: EmbeddingSourceCaptureLimits) -> Result<(), SearchArtifactError> {
    if limits.parts == 0 {
        return Err(invalid("embedding source limits", "parts must be positive"));
    }
    if limits.bytes == 0 {
        return Err(invalid("embedding source limits", "bytes must be positive"));
    }
    if limits.eligible_uuids == 0 {
        return Err(invalid(
            "embedding source limits",
            "eligible_uuids must be positive",
        ));
    }
    Ok(())
}

fn source_digest(parts: &[SearchSourcePart<'_>]) -> Result<[u8; 32], SearchArtifactError> {
    let fingerprint = canonical_source_fingerprint(parts)?;
    fingerprint_bytes(&fingerprint)
}

fn fingerprint_bytes(value: &str) -> Result<[u8; 32], SearchArtifactError> {
    let hex = value.strip_prefix(FINGERPRINT_PREFIX).ok_or_else(|| {
        SearchArtifactError::SourceSnapshot {
            reason: "canonical fingerprint used an unexpected domain".to_owned(),
        }
    })?;
    if hex.len() != FINGERPRINT_HEX_BYTES {
        return Err(SearchArtifactError::SourceSnapshot {
            reason: "canonical fingerprint used an unexpected width".to_owned(),
        });
    }

    let mut digest = [0_u8; 32];
    for (index, byte) in digest.iter_mut().enumerate() {
        let start = index * 2;
        *byte = u8::from_str_radix(&hex[start..start + 2], 16).map_err(|_| {
            SearchArtifactError::SourceSnapshot {
                reason: "canonical fingerprint was not lowercase hexadecimal".to_owned(),
            }
        })?;
    }
    Ok(digest)
}

fn invalid(field: &'static str, reason: impl Into<String>) -> SearchArtifactError {
    SearchArtifactError::InvalidSelector {
        field,
        reason: reason.into(),
    }
}

fn exhausted(resource: &'static str, limit: u64) -> SearchArtifactError {
    SearchArtifactError::ResourceExhausted { resource, limit }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet, HashMap};

    use graphforge_core::uuid::{new_v7, to_bytes};
    use graphforge_core::{OntologyMode, TypeId};
    use graphforge_ir::IrLiteral;
    use graphforge_storage::{
        EmbeddingBatchRow, EmbeddingCompatibilityDescriptor, EmbeddingCompatibilityInput,
        EmbeddingDistance, EmbeddingNormalization, EmbeddingProducerIdentity,
        EmbeddingPublicationRequest, EmbeddingReadDecision, EmbeddingValueType, GraphWriter,
        SearchCoordinationLimits, ValidatedEmbeddingBatch, VectorStoreLimits,
        publish_embedding_generation, reset_embedding_mutation_journal, set_node_properties,
        validate_embedding_batch,
    };

    use crate::{EmbeddingReadLimits, prepare_embedding_read};

    use super::*;

    fn part<'a>(name: &'a str, bytes: &'a [u8]) -> SearchSourcePart<'a> {
        SearchSourcePart { name, bytes }
    }

    fn capture(
        project: &Path,
        labels: &[SearchSourcePart<'_>],
        dependencies: &[SearchSourcePart<'_>],
        eligible: u64,
    ) -> EmbeddingSourceState {
        capture_embedding_source(
            project,
            labels,
            dependencies,
            eligible,
            EmbeddingSourceCaptureLimits::default(),
            || Ok(()),
        )
        .unwrap()
    }

    fn descriptor() -> EmbeddingCompatibilityDescriptor {
        EmbeddingCompatibilityDescriptor::new(EmbeddingCompatibilityInput {
            producer: EmbeddingProducerIdentity::Local {
                implementation: "source-capture-test".to_owned(),
                model: "model-a".to_owned(),
                revision: "r1".to_owned(),
                contract_version: "v1".to_owned(),
            },
            dimensions: 2,
            value_type: EmbeddingValueType::Float32,
            normalization: EmbeddingNormalization::None,
            distance: EmbeddingDistance::Cosine,
            tokenizer: None,
            chunking: None,
            hyperparameters: BTreeMap::new(),
            input_recipe: BTreeMap::from([("property".to_owned(), "body".into())]),
            source_projection_recipe: BTreeMap::from([("label".to_owned(), "Document".into())]),
        })
        .unwrap()
    }

    fn batch(uuid: [u8; 16]) -> ValidatedEmbeddingBatch {
        validate_embedding_batch(
            vec![EmbeddingBatchRow {
                node_uuid: uuid,
                vector: vec![1.0, 2.0],
            }],
            &BTreeSet::from([uuid]),
            2,
            EmbeddingNormalization::None,
            VectorStoreLimits::default(),
            || Ok(()),
        )
        .unwrap()
    }

    #[test]
    fn capture_is_order_independent_and_keeps_source_domains_distinct() {
        let project = tempfile::tempdir().unwrap();
        let labels_a = [part("labels/b", b"b"), part("labels/a", b"a")];
        let labels_b = [part("labels/a", b"a"), part("labels/b", b"b")];
        let dependencies_a = [part("props/body", b"text"), part("props/title", b"title")];
        let dependencies_b = [part("props/title", b"title"), part("props/body", b"text")];

        let first = capture(project.path(), &labels_a, &dependencies_a, 2);
        let reordered = capture(project.path(), &labels_b, &dependencies_b, 2);
        assert_eq!(first, reordered);

        let changed_labels = capture(
            project.path(),
            &[part("labels/a", b"different")],
            &dependencies_a,
            2,
        );
        let changed_dependencies = capture(
            project.path(),
            &labels_a,
            &[part("props/body", b"different")],
            2,
        );
        assert_ne!(
            first.label_membership_digest(),
            changed_labels.label_membership_digest()
        );
        assert_ne!(
            first.dependency_input_digest(),
            changed_dependencies.dependency_input_digest()
        );
    }

    #[test]
    fn committed_node_and_property_changes_advance_but_edges_do_not() {
        let project = tempfile::tempdir().unwrap();
        let node_a = new_v7();
        let node_b = new_v7();
        let mut writer =
            GraphWriter::open_at(project.path(), OntologyMode::Exploratory, 10).unwrap();
        assert_eq!(writer.create_node(node_a, TypeId(1)).unwrap(), 1);
        assert_eq!(writer.create_node(node_b, TypeId(1)).unwrap(), 2);
        writer.flush().unwrap();

        let first = capture(project.path(), &[part("labels", b"a,b")], &[], 2);
        assert_eq!(first.graph_generation(), 1);

        let mut edge_writer =
            GraphWriter::open_at(project.path(), OntologyMode::Exploratory, 11).unwrap();
        edge_writer.register_existing_node(node_a, 1);
        edge_writer.register_existing_node(node_b, 2);
        edge_writer
            .create_edge(new_v7(), "LINKS", &node_a, &node_b)
            .unwrap();
        edge_writer.flush().unwrap();
        let after_edge = capture(project.path(), &[part("labels", b"a,b")], &[], 2);
        assert_eq!(after_edge, first);

        let updates = HashMap::from([(
            to_bytes(&node_a),
            HashMap::from([("body".to_owned(), IrLiteral::Str("changed".to_owned()))]),
        )]);
        assert_eq!(
            set_node_properties(project.path(), "_untyped", &updates).unwrap(),
            1
        );
        let after_property = capture(
            project.path(),
            &[part("labels", b"a,b")],
            &[part("props/body", b"changed")],
            2,
        );
        assert_eq!(after_property.graph_generation(), 2);
        assert_eq!(
            after_property,
            capture(
                project.path(),
                &[part("labels", b"a,b")],
                &[part("props/body", b"changed")],
                2,
            )
        );
    }

    #[test]
    fn older_publication_is_substantially_stale_after_committed_mutation() {
        let project = tempfile::tempdir().unwrap();
        let node = new_v7();
        let node_bytes = to_bytes(&node);
        let mut writer =
            GraphWriter::open_at(project.path(), OntologyMode::Exploratory, 10).unwrap();
        writer.create_node(node, TypeId(1)).unwrap();
        writer.flush().unwrap();
        let recorded = capture(
            project.path(),
            &[part("labels", &node_bytes)],
            &[part("props/body", b"before")],
            1,
        );
        let descriptor = descriptor();
        let vectors = batch(node_bytes);
        let publication = publish_embedding_generation(
            project.path(),
            EmbeddingPublicationRequest {
                descriptor: &descriptor,
                source: recorded,
                batch: &vectors,
                generated_at_micros: 20,
                committed_at_micros: 21,
            },
            VectorStoreLimits::default(),
            SearchCoordinationLimits::default(),
            || Ok(()),
        )
        .unwrap();
        reset_embedding_mutation_journal(
            project.path(),
            &publication.publication().manifest,
            graphforge_storage::EmbeddingMutationJournalLimits::default(),
            SearchCoordinationLimits::default(),
            || Ok(()),
        )
        .unwrap();

        let updates = HashMap::from([(
            node_bytes,
            HashMap::from([("body".to_owned(), IrLiteral::Str("after".to_owned()))]),
        )]);
        set_node_properties(project.path(), "_untyped", &updates).unwrap();
        let current = capture(
            project.path(),
            &[part("labels", &node_bytes)],
            &[part("props/body", b"after")],
            1,
        );
        let prepared = prepare_embedding_read(
            project.path(),
            &descriptor,
            current,
            false,
            EmbeddingReadLimits::default(),
            || Ok(()),
        )
        .unwrap()
        .unwrap();
        assert!(matches!(
            prepared.decision(),
            EmbeddingReadDecision::RefreshRequired { .. }
        ));
    }

    #[test]
    fn limits_cancellation_invalid_parts_and_corrupt_generation_are_structured() {
        let project = tempfile::tempdir().unwrap();
        let labels = [part("labels", b"abc")];
        let error = capture_embedding_source(
            project.path(),
            &labels,
            &[],
            1,
            EmbeddingSourceCaptureLimits {
                bytes: 2,
                ..EmbeddingSourceCaptureLimits::default()
            },
            || Ok(()),
        )
        .unwrap_err();
        assert!(matches!(
            error,
            SearchArtifactError::ResourceExhausted {
                resource: "embedding_source_bytes",
                ..
            }
        ));

        let error = capture_embedding_source(
            project.path(),
            &[part("labels/a", b"a"), part("labels/b", b"b")],
            &[],
            2,
            EmbeddingSourceCaptureLimits {
                parts: 1,
                ..EmbeddingSourceCaptureLimits::default()
            },
            || Ok(()),
        )
        .unwrap_err();
        assert!(matches!(
            error,
            SearchArtifactError::ResourceExhausted {
                resource: "embedding_source_parts",
                ..
            }
        ));

        let error = capture_embedding_source(
            project.path(),
            &labels,
            &[],
            2,
            EmbeddingSourceCaptureLimits {
                eligible_uuids: 1,
                ..EmbeddingSourceCaptureLimits::default()
            },
            || Ok(()),
        )
        .unwrap_err();
        assert!(matches!(
            error,
            SearchArtifactError::ResourceExhausted {
                resource: "embedding_source_eligible_uuids",
                ..
            }
        ));

        let error = capture_embedding_source(
            project.path(),
            &labels,
            &[],
            1,
            EmbeddingSourceCaptureLimits::default(),
            || Err(SearchArtifactError::Cancelled),
        )
        .unwrap_err();
        assert!(matches!(error, SearchArtifactError::Cancelled));

        let duplicate = [part("same", b"a"), part("same", b"b")];
        assert!(matches!(
            capture_embedding_source(
                project.path(),
                &duplicate,
                &[],
                1,
                EmbeddingSourceCaptureLimits::default(),
                || Ok(())
            )
            .unwrap_err(),
            SearchArtifactError::InvalidSelector { .. }
        ));

        std::fs::create_dir_all(project.path().join("topology")).unwrap();
        std::fs::write(project.path().join("topology/generation.json"), b"not-json").unwrap();
        assert!(matches!(
            capture_embedding_source(
                project.path(),
                &labels,
                &[],
                1,
                EmbeddingSourceCaptureLimits::default(),
                || Ok(())
            )
            .unwrap_err(),
            SearchArtifactError::SourceSnapshot { .. }
        ));
    }
}
