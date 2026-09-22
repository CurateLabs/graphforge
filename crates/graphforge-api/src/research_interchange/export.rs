//! Freeze a native research selection into the existing verified portable transport.
use crate::{CancellationToken, GfError, GraphForge};
use graphforge_core::portable::{PortableV2Error, PortableV2Limits, PortableV2Output};
use graphforge_storage::research_versions::ResearchInterchangeSelection;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use uuid::Uuid;

/// Explicit research content and destination; paths are transport, never citation identity.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportResearchRequest {
    /// Exact retained immutable research Version.
    pub version_uuid: Uuid,
    /// New output directory or bundle file.
    pub output: PathBuf,
    /// Emit canonical bundle when true; expanded package otherwise.
    pub bundled: bool,
    /// None preserves the complete selected Version; Some creates a distinct projection.
    #[serde(default)]
    pub projection: Option<super::ResearchExportProjection>,
}

impl GraphForge {
    /// Export exact selected immutable research and its bounded native lineage closure.
    pub fn export_research(
        &self,
        request: &ExportResearchRequest,
        cancellation: &CancellationToken,
    ) -> Result<crate::PortableV2ExportFacadeResult, PortableV2Error> {
        export(self, request, None, cancellation)
    }
}

pub(super) fn export(
    owner: &GraphForge,
    request: &ExportResearchRequest,
    fork: Option<&super::ForkResearchRequest>,
    cancellation: &CancellationToken,
) -> Result<crate::PortableV2ExportFacadeResult, PortableV2Error> {
    cancellation.checkpoint().map_err(resolve_error)?;
    let current = owner.generation_for_read().map_err(resolve_error)?;
    let mut registry = graphforge_storage::research_versions::read_research_registry(&current)
        .map_err(resolve_error)?;
    let prepared = request
        .projection
        .as_ref()
        .map(|selection| {
            super::projection::prepare(owner, request.version_uuid, selection, cancellation)
        })
        .transpose()
        .map_err(resolve_error)?;
    if let Some((content, _)) = &prepared {
        register_prepared(&mut registry, content)?;
    }
    let selected = prepared
        .as_ref()
        .map_or(request.version_uuid, |(content, _)| {
            content.version.version_uuid
        });
    let version = registry
        .versions
        .get(&selected)
        .ok_or_else(|| {
            resolve_error(GfError::Validation(
                "research Version is unavailable".into(),
            ))
        })?
        .clone();
    let selected_view = if let Some((content, _)) = &prepared {
        crate::branches::private_view::open(owner, content)
    } else {
        crate::research_versions::materialize_version(owner, &version)
    }
    .map_err(resolve_error)?;
    let proofs = super::accepted::prepare(owner, &registry, selected, &selected_view, cancellation)
        .map_err(resolve_error)?;
    for content in &proofs.prepared {
        register_prepared(&mut registry, content)?;
    }
    let prepared_views: Vec<_> = prepared
        .as_ref()
        .map(|(content, _)| content)
        .into_iter()
        .chain(proofs.prepared.iter())
        .collect();
    let mut manifest = super::manifest::build(
        owner,
        &registry,
        &version,
        &prepared_views,
        &proofs,
        cancellation,
    )
    .map_err(resolve_error)?;
    if let Some((_, fingerprint)) = &prepared {
        manifest.selection = ResearchInterchangeSelection::Projection {
            source_version_uuid: request.version_uuid,
            selection_sha256: *fingerprint,
        };
    }
    if let Some(request) = fork {
        manifest.fork_project_uuid = Some(request.project_uuid);
        manifest.fork = Some(super::fork::record(request)?);
        manifest.validate().map_err(resolve_error)?;
    }
    publish_package(
        owner,
        request,
        fork,
        cancellation,
        current.container_root(),
        &manifest,
    )
}

fn publish_package(
    owner: &GraphForge,
    request: &ExportResearchRequest,
    fork: Option<&super::ForkResearchRequest>,
    cancellation: &CancellationToken,
    root: &std::path::Path,
    manifest: &graphforge_storage::research_versions::ResearchInterchangeManifest,
) -> Result<crate::PortableV2ExportFacadeResult, PortableV2Error> {
    let private = tempfile::tempdir()
        .map_err(|_| resolve_error(GfError::Storage("cannot prepare research export".into())))?;
    let generation = graphforge_storage::research_versions::materialize_research_interchange(
        root,
        manifest,
        private.path(),
    )
    .map_err(resolve_error)?;
    let generation = if let Some(request) = fork {
        super::fork::configure(&generation, request)?
    } else {
        generation
    };
    let graph = GraphForge::open_resolved_with_options(
        private.path().to_path_buf(),
        generation.clone(),
        true,
        owner.write_options.clone(),
        owner.resource_policy.clone(),
        graphforge_storage::ProjectOpenRecoveryEvidence::checkpoint_view(
            generation.generation_uuid(),
        ),
    )
    .map_err(resolve_error)?;
    graph.export_portable_v2(
        &crate::PortableV2ExportRequest {
            selection: crate::PortableSelection::Current,
            output_path: request.output.clone(),
            representation: if request.bundled {
                PortableV2Output::Bundle
            } else {
                PortableV2Output::Expanded
            },
            profile: graphforge_core::portable::PortableV2SelectionProfile::Complete,
            subset: None,
            limits: PortableV2Limits::default(),
        },
        Some(cancellation.flag()),
        |_| {},
    )
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "map_err consumes the native error while projecting a sanitized portable cause"
)]
fn resolve_error(error: GfError) -> PortableV2Error {
    let code = if matches!(
        error,
        GfError::Api {
            code: graphforge_core::ApiErrorCode::Cancelled,
            ..
        }
    ) {
        graphforge_core::portable::PortableV2ErrorCode::Cancelled
    } else {
        graphforge_core::portable::PortableV2ErrorCode::Incompatible
    };
    PortableV2Error::new(
        code,
        "research export selection or native content is unavailable or invalid",
    )
}

fn register_prepared(
    registry: &mut graphforge_storage::research_versions::ResearchRegistry,
    content: &graphforge_storage::research_versions::PreparedResearchContent,
) -> Result<(), PortableV2Error> {
    let id = content.version.version_uuid;
    let digest = content.version.identity_sha256().map_err(resolve_error)?;
    if registry
        .identities
        .get(&id)
        .is_some_and(|known| *known != digest)
    {
        return Err(resolve_error(GfError::Validation(
            "projection conflicts with known immutable identity".into(),
        )));
    }
    registry.identities.insert(id, digest);
    registry.versions.insert(id, content.version.clone());
    registry.materialized.insert(id);
    Ok(())
}
