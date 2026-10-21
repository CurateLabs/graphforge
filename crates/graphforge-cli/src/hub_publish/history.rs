//! Explicit history publication selection and established Fork origin observations.
use super::{
    GfError, GraphForge, Hub, HubPublishErrorCode, PublishArgs, Published, RepositoryIdentity,
    Write, canonical_json, hex_digest, json, parse_input, publish_error, read_published,
    sha256_digest,
};

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
    project_uuid: uuid::Uuid,
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
    if project_uuid.to_string() != previous.lineage.project_uuid {
        return Err(refused());
    }
    // Match the sole retained native Fork creation to the selected Project and
    // exact published citation, including on receipt-only replay.
    let registry = graph.research_version_retention()?;
    let mut forks = registry.interchange.values().filter(|archive| {
        archive.fork.is_some() && archive.fork_project_uuid == Some(project_uuid)
    });
    let archive = forks.next().ok_or_else(refused)?;
    if forks.next().is_some()
        || archive.source_project_uuid.to_string() != citation.origin_project_uuid
        || archive.selected_version_uuid.to_string() != citation.origin_version_uuid
        || archive
            .identities
            .get(&archive.selected_version_uuid)
            .is_none_or(|identity| hex_digest(identity) != citation.origin_version_identity)
    {
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
