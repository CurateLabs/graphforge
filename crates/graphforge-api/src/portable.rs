//! Portable project interchange orchestration.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

use graphforge_core::GfError;
use graphforge_storage::{PortableProjectLimits, ProjectCapability};
use serde::Serialize;
use uuid::Uuid;

use crate::{GraphForge, OperationId};

/// Immutable generation selected for portable export.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PortableSelection {
    /// Resolve the committed generation at call time.
    Current,
    /// Resolve an active named checkpoint.
    Checkpoint(String),
}

/// Portable export request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PortableExportRequest {
    /// Current or named-checkpoint selection.
    pub selection: PortableSelection,
    /// Destination file. Existing paths are rejected.
    pub output: PathBuf,
}

/// Portable import request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PortableImportRequest {
    /// Bounded regular envelope file.
    pub input: PathBuf,
    /// Caller-owned idempotency identity.
    pub operation_id: OperationId,
}

/// Verification-first portable-v2 complete-project import request.
#[derive(Clone, Debug)]
pub struct PortableV2ImportRequest {
    /// Expanded package directory or canonical `.gfpb` bundle.
    pub input: PathBuf,
    /// Caller-owned idempotency identity.
    pub operation_id: OperationId,
    /// Caller-selected finite verifier and streaming limits.
    pub limits: graphforge_storage::PortableV2Limits,
}

/// Stable Rust-owned portable-v2 import result.
#[derive(Clone, Debug)]
pub struct PortableV2ImportResult {
    /// Canonical semantic package identity.
    pub package_digest: String,
    /// Representation-specific transport identity.
    pub transport_digest: Option<String>,
    /// Newly published local generation UUID.
    pub generation_uuid: Uuid,
    /// Whether the operation replayed an identical publication.
    pub idempotent_replay: bool,
}

/// Publish a verified local portable-v2 package to an OCI registry.
#[derive(Clone, Debug)]
pub struct PortableV2OciPublishFacadeRequest {
    /// Verified local package path (bundle).
    pub package_path: PathBuf,
    /// Registry host without scheme or credentials.
    pub registry: String,
    /// Repository path.
    pub repository: String,
    /// Optional mutable tag.
    pub tag: Option<String>,
    /// Verifier limits.
    pub limits: graphforge_storage::PortableV2Limits,
    /// Authenticity policy applied after transport.
    pub authenticity: graphforge_storage::PortableV2OciAuthenticityPolicy,
    /// Optional signature material attached as an OCI referrer.
    pub signature: Option<graphforge_storage::PortableV2OciSignatureMaterial>,
    /// Use plain HTTP (local disposable registries only).
    pub insecure_http: bool,
    /// Caller-owned credential; never logged or persisted.
    pub credential: Option<String>,
}

/// Pull a digest-pinned portable-v2 package from an OCI registry.
#[derive(Clone, Debug)]
pub struct PortableV2OciPullFacadeRequest {
    /// Registry host without scheme or credentials.
    pub registry: String,
    /// Repository path.
    pub repository: String,
    /// Digest or mutable tag reference.
    pub reference: String,
    /// Optional expected digest when `reference` is a tag.
    pub expected_oci_digest: Option<String>,
    /// Destination path for the verified package.
    pub destination: PathBuf,
    /// Verifier limits.
    pub limits: graphforge_storage::PortableV2Limits,
    /// Authenticity policy.
    pub authenticity: graphforge_storage::PortableV2OciAuthenticityPolicy,
    /// Use plain HTTP (local disposable registries only).
    pub insecure_http: bool,
    /// Caller-owned credential; never logged or persisted.
    pub credential: Option<String>,
}

/// Read-only portable-v2 verification request.
#[derive(Clone, Debug)]
pub struct PortableVerifyRequest {
    /// Expanded package directory or canonical `.gfpb` bundle.
    pub input: PathBuf,
    /// Full content verification or honest structure-only inspection.
    pub mode: graphforge_storage::PortableV2Mode,
    /// Caller-selected finite resource limits.
    pub limits: graphforge_storage::PortableV2Limits,
}

/// Stable Rust-owned portable-v2 verification report.
pub type PortableVerifyResult = graphforge_storage::PortableV2Report;

/// Portable-v2 selection preview request against a pinned generation.
#[derive(Clone, Debug)]
pub struct PortableV2SelectionPreviewRequest {
    /// Current or named-checkpoint selection.
    pub selection: PortableSelection,
    /// Selection profile / custom identities.
    pub request: graphforge_storage::PortableV2SelectionRequest,
    /// Caller-selected finite resource limits.
    pub limits: graphforge_storage::PortableV2Limits,
}

/// Portable-v2 graph-subset preview request against a pinned generation.
#[derive(Clone, Debug)]
pub struct PortableV2SubsetPreviewRequest {
    /// Current or named-checkpoint selection.
    pub selection: PortableSelection,
    /// Graph-subset selector and closure.
    pub request: graphforge_storage::PortableV2SubsetRequest,
    /// Caller-selected finite resource limits.
    pub limits: graphforge_storage::PortableV2Limits,
}

/// Portable-v2 export request (expanded directory or canonical bundle).
#[derive(Clone, Debug)]
pub struct PortableV2ExportRequest {
    /// Current or named-checkpoint selection.
    pub selection: PortableSelection,
    /// Destination path (new file or directory). Existing paths are rejected.
    pub output_path: PathBuf,
    /// Expanded directory or canonical `.gfpb` bundle.
    pub representation: graphforge_storage::PortableV2Output,
    /// Component selection profile. Ignored when `subset` is `Some`.
    pub profile: graphforge_storage::PortableV2SelectionProfile,
    /// Optional graph/data subset. When set, exports `graph-data-subset`.
    pub subset: Option<graphforge_storage::PortableV2SubsetRequest>,
    /// Caller-selected finite planner and streaming limits.
    pub limits: graphforge_storage::PortableV2Limits,
}

/// Stable portable-v2 export receipt for bindings and CLI JSON.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PortableV2ExportFacadeResult {
    /// Contract name.
    pub contract: &'static str,
    /// Source selector kind.
    pub source: &'static str,
    /// Named checkpoint, when selected.
    pub checkpoint: Option<String>,
    /// Pinned source generation.
    pub generation_uuid: Uuid,
    /// Semantic package identity (`sha256:…`).
    pub package_digest: String,
    /// Representation-specific transport identity (`sha256:…`).
    pub transport_digest: String,
    /// Verified physical package entry count.
    pub entry_count: usize,
    /// Source payload bytes, excluding tags and manifest.
    pub payload_bytes: u64,
    /// Published representation token.
    pub representation: &'static str,
    /// Immutable content-free selection fingerprint.
    pub selection_fingerprint: String,
    /// Caller-selected output path.
    pub output: PathBuf,
}

/// Stable export result.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PortableExportResult {
    /// Contract name.
    pub contract: &'static str,
    /// Source selector kind.
    pub source: &'static str,
    /// Named checkpoint, when selected.
    pub checkpoint: Option<String>,
    /// Exported generation UUID.
    pub generation_uuid: Uuid,
    /// Complete envelope SHA-256.
    pub envelope_sha256: String,
    /// Complete envelope bytes.
    pub byte_length: u64,
    /// Participant count.
    pub participant_count: usize,
    /// Caller-selected output path.
    pub output: PathBuf,
}

/// Stable import result.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PortableImportResult {
    /// Contract name.
    pub contract: &'static str,
    /// Original exported generation UUID.
    pub source_generation_uuid: Uuid,
    /// Newly published local generation UUID.
    pub generation_uuid: Uuid,
    /// Complete envelope SHA-256.
    pub envelope_sha256: String,
    /// Whether an identical operation was replayed.
    pub idempotent_replay: bool,
}

/// Verify portable-v2 content without opening or mutating a project.
pub fn verify_portable_v2(
    request: &PortableVerifyRequest,
    cancelled: Option<&AtomicBool>,
) -> Result<PortableVerifyResult, graphforge_storage::PortableV2Error> {
    graphforge_storage::verify_portable_v2(&request.input, request.mode, request.limits, cancelled)
}

/// Publish a verified portable-v2 package through an OCI Distribution registry.
pub fn publish_portable_v2_oci(
    request: &PortableV2OciPublishFacadeRequest,
    cancelled: Option<&AtomicBool>,
) -> Result<graphforge_storage::PortableV2OciReference, graphforge_storage::PortableV2Error> {
    let client = graphforge_storage::HttpOciRegistry::new(
        &request.registry,
        request.credential.as_deref(),
        request.insecure_http,
    )?;
    graphforge_storage::publish_portable_v2_oci(
        &client,
        &graphforge_storage::PortableV2OciPublishRequest {
            package_path: &request.package_path,
            registry: &request.registry,
            repository: &request.repository,
            tag: request.tag.as_deref(),
            limits: request.limits,
            authenticity: request.authenticity.clone(),
            signature: request.signature.clone(),
            credential: request.credential.as_deref(),
        },
        cancelled,
    )
}

/// Pull and verify a portable-v2 package from an OCI Distribution registry.
pub fn pull_portable_v2_oci(
    request: &PortableV2OciPullFacadeRequest,
    cancelled: Option<&AtomicBool>,
) -> Result<graphforge_storage::PortableV2OciPullReceipt, graphforge_storage::PortableV2Error> {
    let client = graphforge_storage::HttpOciRegistry::new(
        &request.registry,
        request.credential.as_deref(),
        request.insecure_http,
    )?;
    graphforge_storage::pull_portable_v2_oci(
        &client,
        &graphforge_storage::PortableV2OciPullRequest {
            registry: &request.registry,
            repository: &request.repository,
            reference: &request.reference,
            expected_oci_digest: request.expected_oci_digest.as_deref(),
            destination: &request.destination,
            limits: request.limits,
            authenticity: request.authenticity.clone(),
            credential: request.credential.as_deref(),
        },
        cancelled,
    )
}

/// Publish/pull against an injected registry backend (local conformance / tests).
pub fn publish_portable_v2_oci_with_registry(
    registry: &dyn graphforge_storage::PortableV2OciRegistry,
    request: &graphforge_storage::PortableV2OciPublishRequest<'_>,
    cancelled: Option<&AtomicBool>,
) -> Result<graphforge_storage::PortableV2OciReference, graphforge_storage::PortableV2Error> {
    graphforge_storage::publish_portable_v2_oci(registry, request, cancelled)
}

/// Pull against an injected registry backend (local conformance / tests).
pub fn pull_portable_v2_oci_with_registry(
    registry: &dyn graphforge_storage::PortableV2OciRegistry,
    request: &graphforge_storage::PortableV2OciPullRequest<'_>,
    cancelled: Option<&AtomicBool>,
) -> Result<graphforge_storage::PortableV2OciPullReceipt, graphforge_storage::PortableV2Error> {
    graphforge_storage::pull_portable_v2_oci(registry, request, cancelled)
}

impl GraphForge {
    /// Resolve a pinned generation for portable export/preview helpers.
    fn resolve_portable_generation(
        &self,
        selection: &PortableSelection,
    ) -> Result<
        (
            graphforge_storage::ResolvedProjectGeneration,
            &'static str,
            Option<String>,
        ),
        GfError,
    > {
        let root = self.resolved_generation.container_root();
        match selection {
            PortableSelection::Current => Ok((
                graphforge_storage::resolve_project_generation(root)?,
                "current",
                None,
            )),
            PortableSelection::Checkpoint(name) => {
                let (_, generation) = graphforge_storage::open_checkpoint_generation_with_mode(
                    root,
                    name,
                    self.lifecycle_mode,
                )?;
                Ok((generation, "checkpoint", Some(name.clone())))
            }
        }
    }

    /// Preview one content-free portable-v2 component selection.
    pub fn preview_portable_v2_selection(
        &self,
        request: &PortableV2SelectionPreviewRequest,
    ) -> Result<graphforge_storage::PortableV2SelectionPlan, graphforge_storage::PortableV2Error>
    {
        let (generation, _, _) = self
            .resolve_portable_generation(&request.selection)
            .map_err(portable_resolve_err)?;
        graphforge_storage::preview_portable_v2_selection(
            &generation,
            &request.request,
            request.limits,
        )
    }

    /// Preview one content-free portable-v2 graph-data subset.
    pub fn preview_portable_v2_graph_subset(
        &self,
        request: &PortableV2SubsetPreviewRequest,
    ) -> Result<graphforge_storage::PortableV2SubsetPlan, graphforge_storage::PortableV2Error> {
        let (generation, _, _) = self
            .resolve_portable_generation(&request.selection)
            .map_err(portable_resolve_err)?;
        graphforge_storage::preview_portable_v2_graph_subset(
            &generation,
            &request.request,
            request.limits,
        )
    }

    /// Export one pinned generation as an expanded or bundled portable-v2 package.
    pub fn export_portable_v2(
        &self,
        request: &PortableV2ExportRequest,
        cancelled: Option<&AtomicBool>,
    ) -> Result<PortableV2ExportFacadeResult, graphforge_storage::PortableV2Error> {
        let (generation, source, checkpoint) = self
            .resolve_portable_generation(&request.selection)
            .map_err(portable_resolve_err)?;
        let plan = if let Some(subset) = &request.subset {
            let preview = graphforge_storage::preview_portable_v2_graph_subset(
                &generation,
                subset,
                request.limits,
            )?;
            graphforge_storage::plan_graph_subset_portable_v2(
                &generation,
                &preview,
                request.limits,
            )?
        } else {
            let selection = graphforge_storage::preview_portable_v2_selection(
                &generation,
                &graphforge_storage::PortableV2SelectionRequest {
                    profile: request.profile.clone(),
                    strict: false,
                },
                request.limits,
            )?;
            graphforge_storage::plan_selected_portable_v2(&generation, &selection, request.limits)?
        };
        let default_cancelled = AtomicBool::new(false);
        let cancelled = cancelled.unwrap_or(&default_cancelled);
        let receipt = graphforge_storage::export_complete_portable_v2(
            &plan,
            &request.output_path,
            request.representation,
            request.limits,
            cancelled,
            |_| {},
        )?;
        Ok(PortableV2ExportFacadeResult {
            contract: "graphforge-portable-export/2",
            source,
            checkpoint,
            generation_uuid: receipt.generation_uuid,
            package_digest: format!("sha256:{}", hex(receipt.package_digest)),
            transport_digest: format!("sha256:{}", hex(receipt.transport_digest)),
            entry_count: receipt.entry_count,
            payload_bytes: receipt.payload_bytes,
            representation: match receipt.output {
                graphforge_storage::PortableV2Output::Expanded => "expanded",
                graphforge_storage::PortableV2Output::Bundle => "bundle",
            },
            selection_fingerprint: receipt.selection_fingerprint,
            output: request.output_path.clone(),
        })
    }

    /// Export one pinned current/checkpoint generation without copying live layout metadata.
    pub fn export_portable(
        &self,
        request: PortableExportRequest,
    ) -> Result<PortableExportResult, GfError> {
        let (generation, source, checkpoint) =
            self.resolve_portable_generation(&request.selection)?;
        let receipt = graphforge_storage::export_portable_project(
            &generation,
            &request.output,
            PortableProjectLimits::default(),
        )?;
        Ok(PortableExportResult {
            contract: "graphforge-portable-export/1",
            source,
            checkpoint,
            generation_uuid: receipt.generation_uuid,
            envelope_sha256: hex(receipt.envelope_sha256),
            byte_length: receipt.byte_length,
            participant_count: receipt.participant_count,
            output: request.output,
        })
    }

    /// Validate and import into a new, empty, or pristine initialized project.
    pub fn import_portable(
        project_root: &Path,
        request: &PortableImportRequest,
    ) -> Result<PortableImportResult, GfError> {
        let generation_uuid = Uuid::new_v5(
            &request.operation_id.0,
            b"graphforge-portable-import-generation/1",
        );
        let receipt = graphforge_storage::import_portable_project_file(
            &request.input,
            project_root,
            request.operation_id.0,
            generation_uuid,
            &supported_capabilities(),
            PortableProjectLimits::default(),
        )?;
        // Reopen through the public facade so success also proves normal runtime readability.
        let root = project_root
            .to_str()
            .ok_or_else(|| GfError::Validation("project path must be valid UTF-8".into()))?;
        let reopened = Self::new(Some(root))?;
        if reopened.resolved_generation.generation_uuid() != receipt.publication.generation_uuid {
            return Err(GfError::Lifecycle(
                "imported generation did not reopen as CURRENT".into(),
            ));
        }
        Ok(PortableImportResult {
            contract: "graphforge-portable-import/1",
            source_generation_uuid: receipt.source_generation_uuid,
            generation_uuid: receipt.publication.generation_uuid,
            envelope_sha256: hex(receipt.envelope_sha256),
            idempotent_replay: receipt.publication.idempotent_replay,
        })
    }

    /// Verify and atomically import a complete portable-v2 package.
    pub fn import_portable_v2(
        project_root: &Path,
        request: &PortableV2ImportRequest,
        cancelled: Option<&AtomicBool>,
    ) -> Result<PortableV2ImportResult, graphforge_storage::PortableV2Error> {
        let generation_uuid = Uuid::new_v5(
            &request.operation_id.0,
            b"graphforge-portable-v2-import-generation/1",
        );
        let receipt = graphforge_storage::import_complete_portable_v2(
            &request.input,
            project_root,
            request.operation_id.0,
            generation_uuid,
            &supported_capabilities(),
            request.limits,
            cancelled,
        )?;
        let root = project_root.to_str().ok_or_else(|| {
            graphforge_storage::PortableV2Error::new(
                graphforge_storage::PortableV2ErrorCode::InvalidPath,
                "invalid project path",
            )
        })?;
        let reopened = Self::new(Some(root)).map_err(|_| {
            graphforge_storage::PortableV2Error::new(
                graphforge_storage::PortableV2ErrorCode::Io,
                "imported project did not reopen",
            )
        })?;
        if reopened.resolved_generation.generation_uuid() != receipt.publication.generation_uuid {
            return Err(graphforge_storage::PortableV2Error::new(
                graphforge_storage::PortableV2ErrorCode::Io,
                "imported generation did not reopen",
            ));
        }
        Ok(PortableV2ImportResult {
            package_digest: receipt.package_digest,
            transport_digest: receipt.transport_digest,
            generation_uuid: receipt.publication.generation_uuid,
            idempotent_replay: receipt.publication.idempotent_replay,
        })
    }
}

fn portable_resolve_err(_error: GfError) -> graphforge_storage::PortableV2Error {
    graphforge_storage::PortableV2Error::new(
        graphforge_storage::PortableV2ErrorCode::Io,
        "pinned project generation is not exportable",
    )
}

fn supported_capabilities() -> Vec<ProjectCapability> {
    [
        "epistemic",
        "graph",
        "knowledge",
        "provenance",
        "valid_time",
        "workspace",
    ]
    .into_iter()
    .map(|capability_id| ProjectCapability {
        capability_id: capability_id.into(),
        capability_version: 1,
    })
    .collect()
}

fn hex(digest: [u8; 32]) -> String {
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        write!(encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AdoptOntologyRequest, CheckpointRequest, ClearOntologyRequest, WriteContext};
    use graphforge_core::OntologyMode;

    const ONTOLOGY: &str = "ontology_id: portable-authority\nversion: \"1\"\nentity_types:\n  - name: Person\n    abstract: false\nrelation_types: []\n";

    #[test]
    fn portable_v2_verification_facade_preserves_typed_cancellation() {
        let source = tempfile::tempdir().unwrap();
        let cancelled = AtomicBool::new(true);
        let error = verify_portable_v2(
            &PortableVerifyRequest {
                input: source.path().to_path_buf(),
                mode: graphforge_storage::PortableV2Mode::Full,
                limits: graphforge_storage::PortableV2Limits::default(),
            },
            Some(&cancelled),
        )
        .unwrap_err();
        assert_eq!(
            error.code,
            graphforge_storage::PortableV2ErrorCode::Cancelled
        );
        assert!(source.path().read_dir().unwrap().next().is_none());
    }

    fn write_context(seed: u128) -> WriteContext {
        WriteContext {
            operation_uuid: OperationId(Uuid::from_u128(seed)),
            actor_uuid: None,
        }
    }

    #[test]
    fn public_facade_round_trips_current_generation_and_reopens_import() {
        let source = tempfile::tempdir().unwrap();
        let source_path = source.path().join("source");
        std::fs::create_dir(&source_path).unwrap();
        let graph = GraphForge::new(source_path.to_str()).unwrap();
        let envelope = source.path().join("portable.gfportable");

        let exported = graph
            .export_portable(PortableExportRequest {
                selection: PortableSelection::Current,
                output: envelope.clone(),
            })
            .unwrap();
        assert_eq!(exported.contract, "graphforge-portable-export/1");
        assert_eq!(exported.source, "current");
        assert_eq!(exported.checkpoint, None);
        assert_eq!(exported.output, envelope);

        let target = source.path().join("imported");
        let imported = GraphForge::import_portable(
            &target,
            &PortableImportRequest {
                input: exported.output,
                operation_id: OperationId(Uuid::new_v4()),
            },
        )
        .unwrap();
        assert_eq!(imported.contract, "graphforge-portable-import/1");
        assert_eq!(imported.source_generation_uuid, exported.generation_uuid);
        assert_eq!(imported.envelope_sha256, exported.envelope_sha256);

        GraphForge::new(target.to_str()).expect("imported CURRENT must reopen");
    }

    #[test]
    fn public_v2_export_preview_and_verify_agree_on_package_digest() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        std::fs::create_dir(&source).unwrap();
        let graph = GraphForge::new(source.to_str()).unwrap();
        let limits = graphforge_storage::PortableV2Limits::default();
        let preview = graph
            .preview_portable_v2_selection(&PortableV2SelectionPreviewRequest {
                selection: PortableSelection::Current,
                request: graphforge_storage::PortableV2SelectionRequest {
                    profile: graphforge_storage::PortableV2SelectionProfile::Complete,
                    strict: false,
                },
                limits,
            })
            .unwrap();
        assert_eq!(preview.package_class, "complete");
        let expanded = root.path().join("expanded");
        let bundle = root.path().join("complete.gfpb");
        let expanded_export = graph
            .export_portable_v2(
                &PortableV2ExportRequest {
                    selection: PortableSelection::Current,
                    output_path: expanded.clone(),
                    representation: graphforge_storage::PortableV2Output::Expanded,
                    profile: graphforge_storage::PortableV2SelectionProfile::Complete,
                    subset: None,
                    limits,
                },
                None,
            )
            .unwrap();
        let bundle_export = graph
            .export_portable_v2(
                &PortableV2ExportRequest {
                    selection: PortableSelection::Current,
                    output_path: bundle.clone(),
                    representation: graphforge_storage::PortableV2Output::Bundle,
                    profile: graphforge_storage::PortableV2SelectionProfile::Complete,
                    subset: None,
                    limits,
                },
                None,
            )
            .unwrap();
        assert_eq!(expanded_export.package_digest, bundle_export.package_digest);
        assert_eq!(
            expanded_export.selection_fingerprint,
            preview.selection_fingerprint
        );
        let verified = verify_portable_v2(
            &PortableVerifyRequest {
                input: bundle,
                mode: graphforge_storage::PortableV2Mode::Full,
                limits,
            },
            None,
        )
        .unwrap();
        assert_eq!(verified.package_digest, bundle_export.package_digest);
        let _ = graph.preview_portable_v2_graph_subset(&PortableV2SubsetPreviewRequest {
            selection: PortableSelection::Current,
            request: graphforge_storage::PortableV2SubsetRequest {
                selector: graphforge_storage::PortableV2GraphSelector::default(),
                closure: graphforge_storage::PortableV2SubsetClosure::InducedEdges,
                projection: graphforge_storage::PortableV2PropertyProjection::default(),
            },
            limits,
        });
    }

    #[test]
    fn public_v2_import_facade_verifies_publishes_and_reopens() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        std::fs::create_dir(&source).unwrap();
        GraphForge::new(source.to_str()).unwrap();
        let generation = graphforge_storage::resolve_project_generation(&source).unwrap();
        let limits = graphforge_storage::PortableV2Limits::default();
        let plan = graphforge_storage::plan_complete_portable_v2(&generation, limits).unwrap();
        let package = root.path().join("complete.gfpb");
        graphforge_storage::export_complete_portable_v2(
            &plan,
            &package,
            graphforge_storage::PortableV2Output::Bundle,
            limits,
            &AtomicBool::new(false),
            |_| {},
        )
        .unwrap();

        let target = root.path().join("target");
        let imported = GraphForge::import_portable_v2(
            &target,
            &PortableV2ImportRequest {
                input: package,
                operation_id: OperationId(Uuid::new_v4()),
                limits,
            },
            None,
        )
        .unwrap();
        assert!(!imported.idempotent_replay);
        assert!(imported.package_digest.starts_with("sha256:"));
        assert_eq!(
            GraphForge::new(target.to_str())
                .unwrap()
                .resolved_generation
                .generation_uuid(),
            imported.generation_uuid
        );
    }

    #[test]
    fn in_memory_checkpoint_can_be_exported() {
        let graph = GraphForge::new(None).unwrap();
        graph
            .checkpoint(CheckpointRequest {
                name: "Export".into(),
                description: None,
                idempotency_key: OperationId(Uuid::from_u128(901)),
                actor_uuid: None,
            })
            .unwrap();
        let output_dir = tempfile::tempdir().unwrap();
        let output = output_dir.path().join("checkpoint.gfportable");

        let exported = graph
            .export_portable(PortableExportRequest {
                selection: PortableSelection::Checkpoint("Export".into()),
                output,
            })
            .unwrap();

        assert_eq!(exported.source, "checkpoint");
        assert_eq!(exported.checkpoint.as_deref(), Some("Export"));
    }

    #[test]
    fn import_rejects_nonempty_target_without_changing_it() {
        let source = tempfile::tempdir().unwrap();
        let source_path = source.path().join("source");
        std::fs::create_dir(&source_path).unwrap();
        let graph = GraphForge::new(source_path.to_str()).unwrap();
        let envelope = source.path().join("portable.gfportable");
        graph
            .export_portable(PortableExportRequest {
                selection: PortableSelection::Current,
                output: envelope.clone(),
            })
            .unwrap();

        let target = source.path().join("occupied");
        std::fs::create_dir(&target).unwrap();
        let sentinel = target.join("keep.txt");
        std::fs::write(&sentinel, b"preserve me").unwrap();
        let error = GraphForge::import_portable(
            &target,
            &PortableImportRequest {
                input: envelope,
                operation_id: OperationId(Uuid::new_v4()),
            },
        )
        .unwrap_err();

        assert!(error.to_string().contains("must be empty"));
        assert_eq!(std::fs::read(sentinel).unwrap(), b"preserve me");
    }

    #[test]
    fn portable_interchange_preserves_durable_ontology_authority() {
        let root = tempfile::tempdir().unwrap();
        let source_path = root.path().join("source");
        std::fs::create_dir(&source_path).unwrap();
        let ontology_path = root.path().join("authority.yaml");
        std::fs::write(&ontology_path, ONTOLOGY).unwrap();
        let mut source = GraphForge::new(source_path.to_str()).unwrap();
        source
            .adopt_ontology(AdoptOntologyRequest {
                context: write_context(1),
                path: ontology_path,
                mode: OntologyMode::Strict,
            })
            .unwrap();
        let expected = source.workspace_ontology().unwrap();
        let envelope = root.path().join("adopted.gfportable");
        source
            .export_portable(PortableExportRequest {
                selection: PortableSelection::Current,
                output: envelope.clone(),
            })
            .unwrap();

        let target_path = root.path().join("imported-adopted");
        GraphForge::import_portable(
            &target_path,
            &PortableImportRequest {
                input: envelope,
                operation_id: OperationId(Uuid::from_u128(2)),
            },
        )
        .unwrap();
        let imported = GraphForge::new(target_path.to_str()).unwrap();

        assert_eq!(imported.ontology_mode(), OntologyMode::Strict);
        assert_eq!(imported.workspace_ontology().unwrap(), expected);
    }

    #[test]
    fn portable_interchange_excludes_session_load_and_preserves_durable_clear() {
        let root = tempfile::tempdir().unwrap();
        let source_path = root.path().join("source");
        std::fs::create_dir(&source_path).unwrap();
        let ontology_path = root.path().join("session.yaml");
        std::fs::write(&ontology_path, ONTOLOGY).unwrap();
        let mut source = GraphForge::new(source_path.to_str()).unwrap();
        source
            .load_ontology(ontology_path.to_str().unwrap())
            .unwrap();
        assert_eq!(source.ontology_mode(), OntologyMode::Advisory);
        let session_envelope = root.path().join("session.gfportable");
        source
            .export_portable(PortableExportRequest {
                selection: PortableSelection::Current,
                output: session_envelope.clone(),
            })
            .unwrap();

        let session_target = root.path().join("imported-session");
        GraphForge::import_portable(
            &session_target,
            &PortableImportRequest {
                input: session_envelope,
                operation_id: OperationId(Uuid::from_u128(3)),
            },
        )
        .unwrap();
        let imported_session = GraphForge::new(session_target.to_str()).unwrap();
        assert_eq!(imported_session.ontology_mode(), OntologyMode::Exploratory);
        assert!(
            imported_session
                .workspace_ontology()
                .unwrap()
                .canonical_ontology
                .is_none()
        );

        source
            .adopt_ontology(AdoptOntologyRequest {
                context: write_context(4),
                path: ontology_path,
                mode: OntologyMode::Advisory,
            })
            .unwrap();
        source
            .clear_ontology(ClearOntologyRequest {
                context: write_context(5),
            })
            .unwrap();
        let cleared_envelope = root.path().join("cleared.gfportable");
        source
            .export_portable(PortableExportRequest {
                selection: PortableSelection::Current,
                output: cleared_envelope.clone(),
            })
            .unwrap();

        let cleared_target = root.path().join("imported-cleared");
        GraphForge::import_portable(
            &cleared_target,
            &PortableImportRequest {
                input: cleared_envelope,
                operation_id: OperationId(Uuid::from_u128(6)),
            },
        )
        .unwrap();
        let imported_clear = GraphForge::new(cleared_target.to_str()).unwrap();
        assert_eq!(imported_clear.ontology_mode(), OntologyMode::Exploratory);
        assert_eq!(
            imported_clear.workspace_ontology().unwrap(),
            source.workspace_ontology().unwrap()
        );
    }

    #[test]
    fn oci_facade_round_trips_through_injected_memory_registry() {
        let root = tempfile::tempdir().unwrap();
        let project = root.path().join("project");
        std::fs::create_dir(&project).unwrap();
        let _graph = GraphForge::new(project.to_str()).unwrap();
        let generation = graphforge_storage::resolve_project_generation(&project).unwrap();
        let limits = graphforge_storage::PortableV2Limits::default();
        let plan = graphforge_storage::plan_complete_portable_v2(&generation, limits).unwrap();
        let bundle = root.path().join("pkg.gfpb");
        graphforge_storage::export_complete_portable_v2(
            &plan,
            &bundle,
            graphforge_storage::PortableV2Output::Bundle,
            limits,
            &AtomicBool::new(false),
            |_| {},
        )
        .unwrap();

        let registry = graphforge_storage::MemoryOciRegistry::default();
        let published = publish_portable_v2_oci_with_registry(
            &registry,
            &graphforge_storage::PortableV2OciPublishRequest {
                package_path: &bundle,
                registry: "memory.local",
                repository: "tests/facade",
                tag: Some("latest"),
                limits,
                authenticity: graphforge_storage::PortableV2OciAuthenticityPolicy::default(),
                signature: None,
                credential: None,
            },
            None,
        )
        .unwrap();
        let destination = root.path().join("pulled.gfpb");
        let pulled = pull_portable_v2_oci_with_registry(
            &registry,
            &graphforge_storage::PortableV2OciPullRequest {
                registry: "memory.local",
                repository: "tests/facade",
                reference: &published.oci_manifest_digest,
                expected_oci_digest: None,
                destination: &destination,
                limits,
                authenticity: graphforge_storage::PortableV2OciAuthenticityPolicy::default(),
                credential: None,
            },
            None,
        )
        .unwrap();
        assert_eq!(pulled.report.package_digest, published.package_digest);
        assert_eq!(
            pulled.signature_state,
            graphforge_storage::PortableV2OciSignatureState::Absent
        );
    }
}
