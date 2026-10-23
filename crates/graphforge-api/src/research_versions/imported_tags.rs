//! Tags on a Version clone are installed before the staged Project is exposed.
use crate::{CancellationToken, GfError, GraphForge, SetResearchTagRequest};
use graphforge_discovery::{DiscoveryLimits, ResearchLineage};
use sha2::Digest as _;
use std::path::Path;
use uuid::Uuid;

/// Admit only this exact import, or its unbroken chain of tag publications.
/// A retry resumes that chain rather than asking the portable importer to reopen
/// the older generation after tags have advanced CURRENT.
impl GraphForge {
    /// Continue the private staging of a verified Version clone by installing its
    /// applicable lineage tags. Read-only admission preserves foreign or advanced
    /// staging. `None` means the native portable import must finish first.
    pub fn continue_imported_research_tags(
        project: &Path,
        lineage: &ResearchLineage,
        selected: Uuid,
        operation: Uuid,
    ) -> Result<Option<Uuid>, GfError> {
        if graphforge_storage::portable_import_pending(project, operation)? {
            return Ok(None);
        }
        let generation = graphforge_storage::resolve_project_generation(project)?;
        let registry = graphforge_storage::research_versions::read_research_registry(&generation)?;
        lineage
            .validate(DiscoveryLimits::default())
            .map_err(|error| GfError::Validation(error.to_string()))?;
        let advertised = lineage
            .version(&selected.to_string())
            .ok_or_else(conflict)?;
        let archive = registry.interchange.get(&selected).ok_or_else(conflict)?;
        if archive.selected_version_uuid != selected
            || registry
                .identities
                .get(&selected)
                .and_then(|digest| crate::discovery_research_lineage::identity_digest(digest).ok())
                .as_ref()
                != Some(&advertised.identity_digest)
        {
            return Err(conflict());
        }
        let tags: Vec<_> = lineage
            .tags
            .iter()
            .filter_map(|tag| {
                let target = Uuid::parse_str(&tag.version_uuid).ok()?;
                registry.identities.contains_key(&target).then(|| {
                    let id = tag_operation(operation, &tag.name, target);
                    (tag.name.clone(), target, id)
                })
            })
            .collect();
        let mut expected = graphforge_core::uuid::portable_import_generation(&operation);
        let mut missing = false;
        for (name, target, id) in &tags {
            if let Some(receipt) = registry.receipts.get(id) {
                let installed = graphforge_storage::resolve_generation_by_uuid(
                    project,
                    receipt.generation_uuid,
                )?;
                if missing
                    || installed.parent_generation_uuid() != Some(expected)
                    || registry.tags.get(name) != Some(target)
                {
                    return Err(conflict());
                }
                expected = receipt.generation_uuid;
            } else {
                missing = true;
            }
        }
        if generation.generation_uuid() != expected {
            return Err(conflict());
        }
        // The read-only admission above precedes the mutable constructor, just as
        // imported Branch continuation does. Foreign or advanced staging is preserved.
        let mut graph = GraphForge::new(Some(project.to_str().ok_or_else(conflict)?))?;
        if graph.committed_generation_identity()?.generation_uuid != expected {
            return Err(conflict());
        }
        let pending = tags
            .into_iter()
            .filter(|(_, _, id)| !registry.receipts.contains_key(id));
        publish_tags(&mut graph, pending, expected).map(Some)
    }
}

/// Advance only from the admitted import through this continuation's receipts.
/// Reading CURRENT to refresh a precondition would admit an independent writer.
fn publish_tags(
    graph: &mut GraphForge,
    tags: impl IntoIterator<Item = (String, Uuid, Uuid)>,
    mut expected: Uuid,
) -> Result<Uuid, GfError> {
    for (name, target, id) in tags {
        let request = SetResearchTagRequest {
            operation_uuid: id,
            expected_generation_uuid: expected,
            name,
            version_uuid: Some(target),
        };
        expected = graph
            .set_research_tag(&request, &CancellationToken::new())?
            .generation_uuid;
    }
    if graph.committed_generation_identity()?.generation_uuid != expected {
        return Err(conflict());
    }
    Ok(expected)
}

fn tag_operation(import: Uuid, name: &str, target: Uuid) -> Uuid {
    let mut digest = sha2::Sha256::new();
    digest.update(b"graphforge-clone-tag/1");
    digest.update(import.as_bytes());
    digest.update(target.as_bytes());
    digest.update(name.as_bytes());
    graphforge_core::canonical::uuid_v8(digest.finalize().into())
}

fn conflict() -> GfError {
    GfError::Validation("hub.destination_conflict: staging is not the selected Version import or its tag publications".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PrepareResearchVersionRequest;
    use std::collections::BTreeSet;

    #[test]
    fn tag_continuation_refuses_a_writer_after_admission_and_preserves_its_state() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("project");
        let mut graph = GraphForge::new(path.to_str()).unwrap();
        graph.execute("CREATE (:Item {score: 1})").unwrap();
        let version = Uuid::now_v7();
        let operation = graph
            .prepare_research_version(PrepareResearchVersionRequest {
                operation_uuid: Uuid::now_v7(),
                version_uuid: version,
                context_uuid: Uuid::now_v7(),
                label: None,
                description: None,
                author: None,
                committer: None,
                created_at: 1,
                required_versions: BTreeSet::new(),
            })
            .unwrap();
        graph
            .commit_research_version_operation(operation, &CancellationToken::new())
            .unwrap();
        let admitted = graph
            .committed_generation_identity()
            .unwrap()
            .generation_uuid;
        let first = publish_tags(
            &mut graph,
            [("first".into(), version, Uuid::now_v7())],
            admitted,
        )
        .unwrap();

        // An independent handle publishes after our admitted tag receipt. No
        // lifetime writer lock protects the continuation between publications.
        let writer = GraphForge::new(path.to_str()).unwrap();
        writer.execute("CREATE (:Independent)").unwrap();
        let advanced = writer
            .committed_generation_identity()
            .unwrap()
            .generation_uuid;
        assert_ne!(advanced, first);
        drop(writer);

        let refused = publish_tags(
            &mut graph,
            [("second".into(), version, Uuid::now_v7())],
            first,
        )
        .unwrap_err();
        assert!(matches!(
            refused,
            GfError::Project {
                code: graphforge_core::ProjectErrorCode::WriteConflict,
                ..
            }
        ));
        // A completed/replayed continuation with no pending tags must also
        // refuse an independent advance instead of returning it as our receipt.
        assert!(matches!(
            publish_tags(&mut graph, [], first),
            Err(GfError::Validation(message)) if message.starts_with("hub.destination_conflict:")
        ));

        let reopened = GraphForge::new(path.to_str()).unwrap();
        assert_eq!(
            reopened
                .committed_generation_identity()
                .unwrap()
                .generation_uuid,
            advanced
        );
        let registry = reopened.research_version_retention().unwrap();
        assert_eq!(registry.tags.get("first"), Some(&version));
        assert!(!registry.tags.contains_key("second"));
        let result = reopened
            .execute("MATCH (:Independent) RETURN count(*) AS count")
            .unwrap();
        let count = result.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(count, 1);
    }
}
