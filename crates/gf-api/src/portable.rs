//! Portable project interchange orchestration.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use gf_core::GfError;
use gf_storage::{PortableProjectLimits, ProjectCapability};
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

impl GraphForge {
    /// Export one pinned current/checkpoint generation without copying live layout metadata.
    pub fn export_portable(
        &self,
        request: PortableExportRequest,
    ) -> Result<PortableExportResult, GfError> {
        let root = self.resolved_generation.container_root();
        let (generation, source, checkpoint) = match request.selection {
            PortableSelection::Current => (
                gf_storage::resolve_project_generation(root)?,
                "current",
                None,
            ),
            PortableSelection::Checkpoint(name) => {
                let (_, generation) = gf_storage::open_checkpoint_generation(root, &name)?;
                (generation, "checkpoint", Some(name))
            }
        };
        let receipt = gf_storage::export_portable_project(
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
        let receipt = gf_storage::import_portable_project_file(
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
    use crate::{AdoptOntologyRequest, ClearOntologyRequest, WriteContext};
    use gf_core::OntologyMode;

    const ONTOLOGY: &str = "ontology_id: portable-authority\nversion: \"1\"\nentity_types:\n  - name: Person\n    abstract: false\nrelation_types: []\n";

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
}
