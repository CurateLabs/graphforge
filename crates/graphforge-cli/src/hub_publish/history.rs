//! Explicit history publication selection and established Fork origin observations.
use super::*;

pub(super) struct SnapshotHistory<'a> {
    pub published: Option<&'a Published>,
    pub fork_origin_repository: Option<RepositoryIdentity>,
    pub rewrite: bool,
    pub prune: bool,
}

pub(super) fn bind_flags(identity: String, args: &PublishArgs) -> String {
    if !args.rewrite && !args.prune {
        return identity;
    }
    sha256_digest(&canonical_json(
        &json!({"identity":identity,"rewrite":args.rewrite,"prune":args.prune}),
    ))
}

pub(super) fn verify_established_fork(
    hub: &Hub<'_>,
    graph: &GraphForge,
    published: Option<&Published>,
    next: &ResearchLineage,
    origin_url: Option<&str>,
    notices: &mut dyn Write,
) -> Result<(), GfError> {
    let Some(origin_url) = origin_url else {
        return Ok(());
    };
    let previous = published.ok_or_else(|| {
        publish_error(
            HubPublishErrorCode::InvalidInput,
            "origin check requires an established Fork repository",
        )
    })?;
    let citation = previous.lineage.fork.as_ref().ok_or_else(|| {
        publish_error(
            HubPublishErrorCode::InvalidInput,
            "origin check requires an established Fork citation",
        )
    })?;
    let refused = || {
        publish_error(
            HubPublishErrorCode::IntegrityFailure,
            "established Fork origin does not match retained native authority",
        )
    };
    if next.fork.as_ref() != Some(citation) || next.project_uuid != previous.lineage.project_uuid {
        return Err(refused());
    }
    // The discovery builder binds this citation to the original imported native
    // archive. Authenticate its retained identity again before any origin request.
    let registry = graph.research_version_retention()?;
    if !registry.interchange.values().any(|archive| {
        archive.fork.is_some()
            && archive
                .fork_project_uuid
                .is_some_and(|id| id.to_string() == next.project_uuid)
            && archive.source_project_uuid.to_string() == citation.origin_project_uuid
            && archive.selected_version_uuid.to_string() == citation.origin_version_uuid
            && archive
                .identities
                .get(&archive.selected_version_uuid)
                .is_some_and(|identity| hex_digest(identity) == citation.origin_version_identity)
    }) {
        return Err(refused());
    }
    let (identity, base) = parse_input(origin_url)?;
    if identity != citation.origin_repository {
        return Err(refused());
    }
    // A whole-repository 404 is ambiguous (including privacy), never erased proof.
    let origin = read_published(hub, &base, &citation.origin_repository)?.ok_or_else(refused)?;
    if origin.lineage.project_uuid != citation.origin_project_uuid {
        return Err(refused());
    }
    if let Some(version) = origin.lineage.version(&citation.origin_version_uuid) {
        if version.identity_digest != citation.origin_version_identity {
            return Err(refused());
        }
    } else {
        writeln!(notices, "Fork origin Version was erased from its verified current lineage; this repository retains its independent copy.")
            .map_err(|error| GfError::Execution(error.to_string()))?;
    }
    Ok(())
}
