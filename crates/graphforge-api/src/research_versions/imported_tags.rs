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
        for (name, target, id) in tags {
            let request = SetResearchTagRequest {
                operation_uuid: id,
                expected_generation_uuid: graph.committed_generation_identity()?.generation_uuid,
                name,
                version_uuid: Some(target),
            };
            graph.set_research_tag(&request, &CancellationToken::new())?;
        }
        Ok(Some(graph.committed_generation_identity()?.generation_uuid))
    }
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
