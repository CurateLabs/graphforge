//! API-owned portable OCI publication and retrieval.
#![allow(missing_docs)]
use graphforge_core::portable::{
    PortableError, PortableErrorCode, PortableIntegrity, PortableLimits, PortableMode,
    PortableOciAuthenticityPolicy, PortableOciPhase, PortableOciProgress, PortableOciPullReceipt,
    PortableOciReference, PortableOciSignatureMaterial, PortableOciSignatureState,
    PortableRepresentation,
};
use graphforge_portable_oci::{
    CONFIG_CONTRACT, DigestingReader, OCI_ARTIFACT_TYPE, OCI_CONFIG_MEDIA_TYPE,
    OCI_LAYER_MEDIA_TYPE, OCI_MANIFEST_MEDIA_TYPE, OciConfig, OciDescriptor, OciManifest,
    PortableOciRegistry, attach_signature_referrer, authenticity_error, check_cancelled,
    digest_reader, digest_sha256, evaluate_signature_state, package_class_str, validate_digest,
    validate_reference, validate_repository,
};
use graphforge_storage::{
    portable_bytes::{PortablePackageStage, open_package_file, portable_bundle_byte_limit},
    verify_portable,
};
use std::fs;
use std::io::Read;
use std::path::Path;
#[cfg(test)]
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};

/// Fails reads once the operation is cancelled, so a long transfer stops
/// promptly. Callers re-check the flag to report `Cancelled` rather than `Io`.
struct CancellableReader<'a, R> {
    inner: R,
    cancelled: Option<&'a AtomicBool>,
}

impl<R: Read> Read for CancellableReader<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self
            .cancelled
            .is_some_and(|flag| flag.load(Ordering::Relaxed))
        {
            return Err(std::io::Error::other("portable OCI operation cancelled"));
        }
        self.inner.read(buf)
    }
}

/// Publish request for a locally verified portable bundle.
#[derive(Clone)]
pub struct PortableOciPublishRequest<'a> {
    /// Verified local package path (bundle or expanded).
    pub package_path: &'a Path,
    /// Registry host, e.g. `127.0.0.1:5000` or `ghcr.io`.
    pub registry: &'a str,
    /// Repository name, e.g. `curatelabs/graphforge-packages`.
    pub repository: &'a str,
    /// Optional mutable tag annotation; pull-by-digest remains authoritative.
    pub tag: Option<&'a str>,
    /// Verifier limits applied before upload.
    pub limits: PortableLimits,
    /// Optional authenticity policy (signature verification is explicit).
    pub authenticity: PortableOciAuthenticityPolicy,
    /// Optional signature to attach as an OCI referrer after publish.
    pub signature: Option<PortableOciSignatureMaterial>,
    /// Optional bearer token or basic `user:pass` supplied by the caller.
    /// Never persisted or logged by GraphForge.
    pub credential: Option<&'a str>,
}

/// Pull request resolved by OCI manifest digest (or tag that is immediately
/// re-resolved to a digest before materialization).
#[derive(Clone)]
pub struct PortableOciPullRequest<'a> {
    /// Registry host without scheme or credentials.
    pub registry: &'a str,
    /// Repository path.
    pub repository: &'a str,
    /// Prefer an OCI manifest digest (`sha256:…`). Tags are allowed only as a
    /// mutable lookup that must be re-recorded as a digest.
    pub reference: &'a str,
    /// When set with a tag reference, disagreeing resolved digests fail closed.
    pub expected_oci_digest: Option<&'a str>,
    /// Destination path for the verified local package.
    pub destination: &'a Path,
    /// Verifier limits.
    pub limits: PortableLimits,
    /// Optional authenticity policy.
    pub authenticity: PortableOciAuthenticityPolicy,
    /// Optional credential (never logged).
    pub credential: Option<&'a str>,
}

/// Verify locally, map to OCI descriptors, and publish by digest (optional tag).
pub(crate) fn publish_portable_oci(
    registry: &dyn PortableOciRegistry,
    request: &PortableOciPublishRequest<'_>,
    cancelled: Option<&AtomicBool>,
) -> Result<PortableOciReference, PortableError> {
    publish_portable_oci_with_progress(registry, request, cancelled, |_| {})
}

/// Publish with sanitized progress callbacks.
#[expect(
    clippy::too_many_lines,
    reason = "keeps verify/upload/observe and signature attach in one fail-closed publish path"
)]
pub(crate) fn publish_portable_oci_with_progress(
    registry: &dyn PortableOciRegistry,
    request: &PortableOciPublishRequest<'_>,
    cancelled: Option<&AtomicBool>,
    mut progress: impl FnMut(PortableOciProgress),
) -> Result<PortableOciReference, PortableError> {
    check_cancelled(cancelled)?;
    progress(PortableOciProgress {
        phase: PortableOciPhase::VerifyLocal,
        bytes_transferred: 0,
        digest: None,
    });
    let report = verify_portable(
        request.package_path,
        PortableMode::Full,
        request.limits,
        cancelled,
    )?;
    if report.integrity != PortableIntegrity::Verified {
        return Err(PortableError::new(
            PortableErrorCode::DigestMismatch,
            "package integrity is not verified",
        ));
    }

    let max_layer_bytes = portable_bundle_byte_limit(request.limits)?;
    let (layer_digest, layer_size) = {
        let mut source = CancellableReader {
            inner: open_package_file(request.package_path, &report)?
                .take(max_layer_bytes.saturating_add(1)),
            cancelled,
        };
        let identity = digest_reader(&mut source, request.limits.copy_buffer_bytes);
        check_cancelled(cancelled)?;
        identity.map_err(|_| PortableError::new(PortableErrorCode::Io, "failed to read package"))?
    };
    if layer_size > max_layer_bytes {
        return Err(PortableError::new(
            PortableErrorCode::LimitExceeded,
            "package exceeds byte limit",
        ));
    }
    let config = OciConfig {
        contract: CONFIG_CONTRACT.to_owned(),
        package_digest: report.package_digest.clone(),
        package_class: package_class_str(report.package_class).to_owned(),
        representation: match report.representation {
            PortableRepresentation::Bundle => "bundle".to_owned(),
            PortableRepresentation::Expanded => "expanded".to_owned(),
        },
        transport_digest: report.transport_digest.clone(),
        layer_media_type: OCI_LAYER_MEDIA_TYPE.to_owned(),
    };
    let config_bytes = serde_json::to_vec(&config)
        .map_err(|_| PortableError::new(PortableErrorCode::Io, "failed to encode OCI config"))?;
    let config_digest = digest_sha256(&config_bytes);
    let mut bytes_transferred = 0_u64;

    if !registry.blob_exists(request.repository, &config_digest)? {
        registry.put_blob(request.repository, &config_digest, &config_bytes)?;
        bytes_transferred += config_bytes.len() as u64;
        progress(PortableOciProgress {
            phase: PortableOciPhase::UploadBlob,
            bytes_transferred,
            digest: Some(config_digest.clone()),
        });
    }
    check_cancelled(cancelled)?;
    if !registry.blob_exists(request.repository, &layer_digest)? {
        // Reopen and bound to the measured size: growth or mutation after the
        // digest pass fails the upload instead of publishing different bytes.
        let mut source = CancellableReader {
            inner: open_package_file(request.package_path, &report)?.take(layer_size),
            cancelled,
        };
        let uploaded =
            registry.put_blob_stream(request.repository, &layer_digest, layer_size, &mut source);
        check_cancelled(cancelled)?;
        uploaded?;
        bytes_transferred += layer_size;
        progress(PortableOciProgress {
            phase: PortableOciPhase::UploadBlob,
            bytes_transferred,
            digest: Some(layer_digest.clone()),
        });
    }
    check_cancelled(cancelled)?;

    let manifest = OciManifest {
        schema_version: 2,
        media_type: OCI_MANIFEST_MEDIA_TYPE.to_owned(),
        artifact_type: OCI_ARTIFACT_TYPE.to_owned(),
        config: OciDescriptor {
            media_type: OCI_CONFIG_MEDIA_TYPE.to_owned(),
            digest: config_digest,
            size: config_bytes.len() as u64,
        },
        layers: vec![OciDescriptor {
            media_type: OCI_LAYER_MEDIA_TYPE.to_owned(),
            digest: layer_digest,
            size: layer_size,
        }],
        subject: None,
    };
    let manifest_bytes = serde_json::to_vec(&manifest)
        .map_err(|_| PortableError::new(PortableErrorCode::Io, "failed to encode OCI manifest"))?;
    let digest_ref = digest_sha256(&manifest_bytes);
    let published = registry.put_manifest(
        request.repository,
        &digest_ref,
        OCI_MANIFEST_MEDIA_TYPE,
        &manifest_bytes,
    )?;
    if published != digest_ref {
        return Err(PortableError::new(
            PortableErrorCode::DigestMismatch,
            "registry returned a different manifest digest",
        ));
    }
    bytes_transferred += manifest_bytes.len() as u64;
    progress(PortableOciProgress {
        phase: PortableOciPhase::UploadManifest,
        bytes_transferred,
        digest: Some(digest_ref.clone()),
    });
    if let Some(tag) = request.tag {
        validate_reference(tag)?;
        let _ = registry.put_manifest(
            request.repository,
            tag,
            OCI_MANIFEST_MEDIA_TYPE,
            &manifest_bytes,
        )?;
    }

    if let Some(material) = &request.signature {
        attach_signature_referrer(
            registry,
            request.repository,
            &digest_ref,
            &report.package_digest,
            material,
            &mut bytes_transferred,
            &mut progress,
        )?;
        progress(PortableOciProgress {
            phase: PortableOciPhase::AttachSignature,
            bytes_transferred,
            digest: Some(digest_ref.clone()),
        });
    }

    progress(PortableOciProgress {
        phase: PortableOciPhase::Observe,
        bytes_transferred,
        digest: Some(digest_ref.clone()),
    });
    let (media, observed) = registry.get_manifest(request.repository, &digest_ref)?;
    if media != OCI_MANIFEST_MEDIA_TYPE && !media.starts_with(OCI_MANIFEST_MEDIA_TYPE) {
        return Err(PortableError::new(
            PortableErrorCode::Incompatible,
            "incompatible manifest media type",
        ));
    }
    if digest_sha256(&observed) != digest_ref {
        return Err(PortableError::new(
            PortableErrorCode::DigestMismatch,
            "fresh registry observation does not match published digest",
        ));
    }

    Ok(PortableOciReference {
        registry: request.registry.to_owned(),
        repository: request.repository.to_owned(),
        oci_manifest_digest: digest_ref,
        package_digest: report.package_digest,
        package_class: report.package_class,
        tag: request.tag.map(str::to_owned),
        bytes_transferred,
        blob_count: 2,
    })
}

/// Pull by digest (or tag→digest), verify fully, then materialize to destination.
pub(crate) fn pull_portable_oci(
    registry: &dyn PortableOciRegistry,
    request: &PortableOciPullRequest<'_>,
    cancelled: Option<&AtomicBool>,
) -> Result<PortableOciPullReceipt, PortableError> {
    pull_portable_oci_with_progress(registry, request, cancelled, |_| {})
}

/// Pull with sanitized progress callbacks.
#[expect(
    clippy::too_many_lines,
    reason = "keeps download/verify/authenticity evaluation in one fail-closed pull path"
)]
pub(crate) fn pull_portable_oci_with_progress(
    registry: &dyn PortableOciRegistry,
    request: &PortableOciPullRequest<'_>,
    cancelled: Option<&AtomicBool>,
    mut progress: impl FnMut(PortableOciProgress),
) -> Result<PortableOciPullReceipt, PortableError> {
    check_cancelled(cancelled)?;
    validate_repository(request.repository)?;
    validate_reference(request.reference)?;
    if let Some(expected) = request.expected_oci_digest {
        validate_digest(expected)?;
    }

    progress(PortableOciProgress {
        phase: PortableOciPhase::DownloadManifest,
        bytes_transferred: 0,
        digest: None,
    });
    let (_media, manifest_bytes) = registry.get_manifest(request.repository, request.reference)?;
    let oci_manifest_digest = digest_sha256(&manifest_bytes);
    let mut bytes_transferred = manifest_bytes.len() as u64;
    if request.reference.starts_with("sha256:") && request.reference != oci_manifest_digest {
        return Err(PortableError::new(
            PortableErrorCode::DigestMismatch,
            "requested digest does not match downloaded manifest",
        ));
    }
    if let Some(expected) = request.expected_oci_digest
        && expected != oci_manifest_digest
    {
        return Err(PortableError::new(
            PortableErrorCode::DigestMismatch,
            "tag or reference disagrees with expected OCI digest",
        ));
    }
    let manifest: OciManifest = serde_json::from_slice(&manifest_bytes).map_err(|_| {
        PortableError::new(
            PortableErrorCode::InvalidStructure,
            "manifest is not valid OCI JSON",
        )
    })?;
    if manifest.artifact_type != OCI_ARTIFACT_TYPE {
        return Err(PortableError::new(
            PortableErrorCode::Incompatible,
            "unsupported OCI artifact type",
        ));
    }
    if manifest.layers.len() != 1 {
        return Err(PortableError::new(
            PortableErrorCode::InvalidStructure,
            "portable OCI artifacts require exactly one layer",
        ));
    }
    if manifest.layers[0].media_type != OCI_LAYER_MEDIA_TYPE {
        return Err(PortableError::new(
            PortableErrorCode::Incompatible,
            "incompatible layer media type",
        ));
    }
    if manifest.config.media_type != OCI_CONFIG_MEDIA_TYPE {
        return Err(PortableError::new(
            PortableErrorCode::Incompatible,
            "incompatible config media type",
        ));
    }

    check_cancelled(cancelled)?;
    progress(PortableOciProgress {
        phase: PortableOciPhase::DownloadBlob,
        bytes_transferred,
        digest: Some(manifest.config.digest.clone()),
    });
    let config_bytes = registry.get_blob(request.repository, &manifest.config.digest)?;
    bytes_transferred += config_bytes.len() as u64;
    if digest_sha256(&config_bytes) != manifest.config.digest {
        return Err(PortableError::new(
            PortableErrorCode::DigestMismatch,
            "config blob digest mismatch",
        ));
    }
    let config: OciConfig = serde_json::from_slice(&config_bytes).map_err(|_| {
        PortableError::new(
            PortableErrorCode::InvalidStructure,
            "config blob is not valid GraphForge OCI config",
        )
    })?;
    if config.contract != CONFIG_CONTRACT {
        return Err(PortableError::new(
            PortableErrorCode::UnsupportedFuture,
            "unsupported OCI config contract",
        ));
    }

    check_cancelled(cancelled)?;
    progress(PortableOciProgress {
        phase: PortableOciPhase::DownloadBlob,
        bytes_transferred,
        digest: Some(manifest.layers[0].digest.clone()),
    });
    let layer = &manifest.layers[0];
    let max_layer_bytes = portable_bundle_byte_limit(request.limits)?;
    if layer.size > max_layer_bytes {
        return Err(PortableError::new(
            PortableErrorCode::LimitExceeded,
            "layer exceeds byte limit",
        ));
    }
    let layer_stream = registry.get_blob_stream(request.repository, &layer.digest, layer.size)?;
    bytes_transferred += layer.size;

    let parent = request.destination.parent().ok_or_else(|| {
        PortableError::new(PortableErrorCode::InvalidPath, "destination has no parent")
    })?;
    fs::create_dir_all(parent).map_err(|_| {
        PortableError::new(PortableErrorCode::Io, "failed to create destination parent")
    })?;
    let stage = parent.join(format!(
        ".{}.oci.partial",
        request
            .destination
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("package")
    ));
    let mut layer_source = DigestingReader::new(CancellableReader {
        inner: layer_stream.take(layer.size.saturating_add(1)),
        cancelled,
    });
    let stage = PortablePackageStage::create(&stage, &mut layer_source, max_layer_bytes);
    check_cancelled(cancelled)?;
    let stage = stage?;
    if layer_source.finish() != (layer.digest.clone(), layer.size) {
        return Err(PortableError::new(
            PortableErrorCode::DigestMismatch,
            "layer blob digest mismatch",
        ));
    }
    if check_cancelled(cancelled).is_err() {
        return Err(PortableError::new(
            PortableErrorCode::Cancelled,
            "portable OCI pull cancelled",
        ));
    }
    progress(PortableOciProgress {
        phase: PortableOciPhase::VerifyPulled,
        bytes_transferred,
        digest: Some(oci_manifest_digest.clone()),
    });
    let report = match verify_portable(stage.path(), PortableMode::Full, request.limits, cancelled)
    {
        Ok(report) => report,
        Err(error) => {
            return Err(error);
        }
    };
    if report.package_digest != config.package_digest {
        return Err(PortableError::new(
            PortableErrorCode::DigestMismatch,
            "pulled package digest does not match OCI config",
        ));
    }

    progress(PortableOciProgress {
        phase: PortableOciPhase::EvaluateAuthenticity,
        bytes_transferred,
        digest: Some(oci_manifest_digest.clone()),
    });
    let signature_state = evaluate_signature_state(
        registry,
        request.repository,
        &oci_manifest_digest,
        &report.package_digest,
        &request.authenticity,
    )?;
    if request.authenticity.require_named_signer.is_some()
        && signature_state != PortableOciSignatureState::Valid
    {
        return Err(authenticity_error(signature_state));
    }

    if request.destination.exists() {
        return Err(PortableError::new(
            PortableErrorCode::InvalidPath,
            "destination already exists",
        ));
    }
    stage.publish(request.destination)?;

    let tag = if request.reference.starts_with("sha256:") {
        None
    } else {
        Some(request.reference.to_owned())
    };
    Ok(PortableOciPullReceipt {
        reference: PortableOciReference {
            registry: request.registry.to_owned(),
            repository: request.repository.to_owned(),
            oci_manifest_digest,
            package_digest: report.package_digest.clone(),
            package_class: report.package_class,
            tag,
            bytes_transferred,
            blob_count: 2,
        },
        destination: request.destination.to_path_buf(),
        report,
        signature_state,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use graphforge_core::{OntologyMode, TypeId};
    use graphforge_ir::IrLiteral;
    use graphforge_storage::{
        GRAPH_CAPABILITY_ID, GRAPH_CAPABILITY_VERSION, GraphWriter, PortableExportLimits,
        PortableGraphSelector, PortableOutput, PortablePropertyProjection,
        PortableSelectionProfile, PortableSelectionRequest, PortableSubsetClosure,
        PortableSubsetRequest, ProjectCapability, ProjectGenerationRequest, ProjectStageOutcome,
        capture_graph_files, empty_workspace_participants, export_complete_portable,
        open_or_initialize_project, plan_complete_portable, plan_graph_subset_portable,
        plan_selected_portable, preview_portable_graph_subset, preview_portable_selection,
        resolve_project_generation, stage_project_generation_with_graph_tree,
    };
    use std::collections::HashMap;
    use std::sync::atomic::AtomicBool;
    use tempfile::tempdir;
    use uuid::Uuid;

    fn export_bundle_for(
        profile: PortableSelectionProfile,
    ) -> (tempfile::TempDir, PathBuf, PortableReport) {
        let dir = tempdir().unwrap();
        let root = dir.path().join("project");
        open_or_initialize_project(&root).unwrap();
        let generation = resolve_project_generation(&root).unwrap();
        let limits = PortableExportLimits::default();
        let plan = match profile {
            PortableSelectionProfile::Complete => {
                plan_complete_portable(&generation, limits).unwrap()
            }
            other => {
                let selection = preview_portable_selection(
                    &generation,
                    &PortableSelectionRequest {
                        profile: other,
                        strict: false,
                    },
                    limits,
                )
                .unwrap();
                plan_selected_portable(&generation, &selection, limits).unwrap()
            }
        };
        let bundle = dir.path().join("pkg.gfpb");
        export_complete_portable(
            &plan,
            &bundle,
            PortableOutput::Bundle,
            limits,
            &AtomicBool::new(false),
            |_| {},
        )
        .unwrap();
        let report =
            verify_portable(&bundle, PortableMode::Full, PortableLimits::default(), None).unwrap();
        (dir, bundle, report)
    }

    fn export_bundle() -> (tempfile::TempDir, PathBuf, PortableReport) {
        export_bundle_for(PortableSelectionProfile::Complete)
    }

    fn publish_req<'a>(
        bundle: &'a Path,
        tag: Option<&'a str>,
        signature: Option<PortableOciSignatureMaterial>,
        credential: Option<&'a str>,
    ) -> PortableOciPublishRequest<'a> {
        PortableOciPublishRequest {
            package_path: bundle,
            registry: "memory.local",
            repository: "tests/portable",
            tag,
            limits: PortableLimits::default(),
            authenticity: PortableOciAuthenticityPolicy::default(),
            signature,
            credential,
        }
    }

    fn pull_req<'a>(
        reference: &'a str,
        destination: &'a Path,
        expected: Option<&'a str>,
        authenticity: PortableOciAuthenticityPolicy,
    ) -> PortableOciPullRequest<'a> {
        PortableOciPullRequest {
            registry: "memory.local",
            repository: "tests/portable",
            reference,
            expected_oci_digest: expected,
            destination,
            limits: PortableLimits::default(),
            authenticity,
            credential: None,
        }
    }

    #[test]
    fn pull_keeps_foreign_stage_and_cleans_owned_stage_after_authentication_error() {
        let (_dir, bundle, _) = export_bundle();
        let registry = MemoryOciRegistry::default();
        let reference =
            publish_portable_oci(&registry, &publish_req(&bundle, None, None, None), None).unwrap();
        let out = tempdir().unwrap();
        let destination = out.path().join("pull.gfpb");
        let stage = out.path().join(".pull.gfpb.oci.partial");
        fs::write(&stage, b"foreign stage").unwrap();
        assert!(
            pull_portable_oci(
                &registry,
                &pull_req(
                    &reference.oci_manifest_digest,
                    &destination,
                    None,
                    Default::default()
                ),
                None
            )
            .is_err()
        );
        assert_eq!(fs::read(&stage).unwrap(), b"foreign stage");
        assert!(!destination.exists());
        fs::remove_file(&stage).unwrap();
        let registry = FaultyDownload {
            registry,
            corrupt: false,
            corrupt_config: false,
            referrer_error: true,
            cancel: None,
        };
        let error = pull_portable_oci(
            &registry,
            &pull_req(
                &reference.oci_manifest_digest,
                &destination,
                None,
                Default::default(),
            ),
            None,
        )
        .unwrap_err();
        assert_eq!(error.code, PortableErrorCode::Io);
        assert!(!stage.exists());
        assert!(!destination.exists());
    }

    #[test]
    fn tight_payload_limit_preserves_valid_bundle_framing() {
        let (_dir, bundle, report) = export_bundle();
        assert!(fs::metadata(&bundle).unwrap().len() > report.payload_bytes);
        let registry = MemoryOciRegistry::default();
        let mut request = publish_req(&bundle, None, None, None);
        request.limits.max_total_bytes = report.payload_bytes;
        let published = publish_portable_oci(&registry, &request, None).unwrap();
        let output = tempdir().unwrap();
        let destination = output.path().join("pull.gfpb");
        let mut request = pull_req(
            &published.oci_manifest_digest,
            &destination,
            None,
            Default::default(),
        );
        request.limits.max_total_bytes = report.payload_bytes;
        let receipt = pull_portable_oci(&registry, &request, None).unwrap();
        assert_eq!(receipt.report, report);
    }
    #[test]
    fn publish_and_pull_by_digest_preserves_package_identity() {
        let (_dir, bundle, report) = export_bundle();
        let registry = MemoryOciRegistry::default();
        let published = publish_portable_oci(
            &registry,
            &publish_req(&bundle, Some("latest"), None, Some("user:secret-token")),
            None,
        )
        .unwrap();
        assert_eq!(published.package_digest, report.package_digest);

        let out_dir = tempdir().unwrap();
        let destination = out_dir.path().join("pulled.gfpb");
        let pulled = pull_portable_oci(
            &registry,
            &PortableOciPullRequest {
                registry: "memory.local",
                repository: "tests/portable",
                reference: &published.oci_manifest_digest,
                expected_oci_digest: None,
                destination: &destination,
                limits: PortableLimits::default(),
                authenticity: PortableOciAuthenticityPolicy::default(),
                credential: Some("user:secret-token"),
            },
            None,
        )
        .unwrap();
        assert_eq!(pulled.report.package_digest, report.package_digest);
        assert_eq!(pulled.signature_state, PortableOciSignatureState::Absent);
        let json = serde_json::to_string(&pulled).unwrap();
        assert!(!json.contains("secret-token"));
        assert!(!json.contains("user:"));
    }

    #[test]
    fn selective_package_classes_round_trip_through_local_registry() {
        for profile in [
            PortableSelectionProfile::Complete,
            PortableSelectionProfile::OntologyOnly,
            PortableSelectionProfile::Settings,
        ] {
            let (_dir, bundle, report) = export_bundle_for(profile);
            let registry = MemoryOciRegistry::default();
            let published =
                publish_portable_oci(&registry, &publish_req(&bundle, None, None, None), None)
                    .unwrap();
            assert_eq!(published.package_class, report.package_class);
            let out_dir = tempdir().unwrap();
            let destination = out_dir.path().join("pulled.gfpb");
            let pulled = pull_portable_oci(
                &registry,
                &pull_req(
                    &published.oci_manifest_digest,
                    &destination,
                    None,
                    Default::default(),
                ),
                None,
            )
            .unwrap();
            assert_eq!(pulled.report.package_digest, report.package_digest);
            assert_eq!(pulled.report.package_class, report.package_class);
        }
    }

    #[test]
    fn graph_data_subset_package_round_trips_through_local_registry() {
        let root = tempdir().unwrap();
        open_or_initialize_project(root.path()).unwrap();
        let workspace = tempdir().unwrap();
        let nodes = {
            let mut bytes = [0_u8; 16];
            [
                {
                    bytes[15] = 1;
                    Uuid::from_bytes(bytes)
                },
                {
                    bytes[15] = 2;
                    Uuid::from_bytes(bytes)
                },
            ]
        };
        let edge = {
            let mut bytes = [0_u8; 16];
            bytes[15] = 11;
            Uuid::from_bytes(bytes)
        };
        let mut writer = GraphWriter::open_at(
            workspace.path(),
            OntologyMode::Exploratory,
            1_700_000_000_000_000,
        )
        .unwrap();
        for (index, node) in nodes.iter().enumerate() {
            writer
                .create_node(
                    *node,
                    graphforge_value::EntityTypeId::ontology(TypeId(1)).unwrap(),
                )
                .unwrap();
            writer
                .set_properties(
                    node,
                    None,
                    HashMap::from([("value".into(), IrLiteral::Int(index as i64))]),
                )
                .unwrap();
        }
        writer
            .create_edge(edge, "KNOWS", &nodes[0], &nodes[1])
            .unwrap();
        writer.flush().unwrap();
        let (_, files) = capture_graph_files(workspace.path()).unwrap();
        let mut participants = empty_workspace_participants().unwrap();
        participants.insert(0, files);
        let request = ProjectGenerationRequest {
            transaction_uuid: Uuid::now_v7(),
            generation_uuid: Uuid::now_v7(),
            capabilities: vec![
                ProjectCapability {
                    capability_id: GRAPH_CAPABILITY_ID.into(),
                    capability_version: GRAPH_CAPABILITY_VERSION,
                },
                ProjectCapability {
                    capability_id: "workspace".into(),
                    capability_version: 1,
                },
            ],
            participants,
        };
        let ProjectStageOutcome::Staged(staged) =
            stage_project_generation_with_graph_tree(root.path(), &request, Some(workspace.path()))
                .unwrap()
        else {
            panic!("expected staged publication");
        };
        staged
            .validate(|_| Ok(()), |_, _| Ok(()))
            .unwrap()
            .publish()
            .unwrap();
        let generation = resolve_project_generation(root.path()).unwrap();
        let limits = PortableLimits::default();
        let mut selected = [nodes[0], nodes[1]].map(|uuid| uuid.hyphenated().to_string());
        selected.sort();
        let subset = PortableSubsetRequest {
            selector: PortableGraphSelector {
                node_uuids: selected.to_vec(),
                edge_uuids: vec![],
            },
            closure: PortableSubsetClosure::InducedEdges,
            projection: PortablePropertyProjection { exclude: vec![] },
        };
        let preview = preview_portable_graph_subset(&generation, &subset, limits).unwrap();
        let plan = plan_graph_subset_portable(&generation, &preview, limits).unwrap();
        let bundle = root.path().join("subset.gfpb");
        export_complete_portable(
            &plan,
            &bundle,
            PortableOutput::Bundle,
            limits,
            &AtomicBool::new(false),
            |_| {},
        )
        .unwrap();
        let report = verify_portable(&bundle, PortableMode::Full, limits, None).unwrap();
        assert_eq!(report.package_class, PortablePackageClass::GraphDataSubset);
        let registry = MemoryOciRegistry::default();
        let published =
            publish_portable_oci(&registry, &publish_req(&bundle, None, None, None), None).unwrap();
        let out_dir = tempdir().unwrap();
        let destination = out_dir.path().join("pulled.gfpb");
        let pulled = pull_portable_oci(
            &registry,
            &pull_req(
                &published.oci_manifest_digest,
                &destination,
                None,
                Default::default(),
            ),
            None,
        )
        .unwrap();
        assert_eq!(pulled.report.package_digest, report.package_digest);
        assert_eq!(
            pulled.report.package_class,
            PortablePackageClass::GraphDataSubset
        );
    }

    #[test]
    fn mutable_tag_move_does_not_alter_digest_pinned_pull() {
        let (_dir, bundle, report) = export_bundle();
        let registry = MemoryOciRegistry::default();
        let first = publish_portable_oci(
            &registry,
            &publish_req(&bundle, Some("moving"), None, None),
            None,
        )
        .unwrap();

        let mut alt = serde_json::to_vec(&OciManifest {
            schema_version: 2,
            media_type: OCI_MANIFEST_MEDIA_TYPE.to_owned(),
            artifact_type: OCI_ARTIFACT_TYPE.to_owned(),
            config: OciDescriptor {
                media_type: OCI_CONFIG_MEDIA_TYPE.to_owned(),
                digest: "sha256:0000000000000000000000000000000000000000000000000000000000000000"
                    .into(),
                size: 1,
            },
            layers: vec![OciDescriptor {
                media_type: OCI_LAYER_MEDIA_TYPE.to_owned(),
                digest: "sha256:1111111111111111111111111111111111111111111111111111111111111111"
                    .into(),
                size: 1,
            }],
            subject: None,
        })
        .unwrap();
        alt.push(b'\n');
        let moved = registry
            .put_manifest("tests/portable", "moving", OCI_MANIFEST_MEDIA_TYPE, &alt)
            .unwrap();
        assert_ne!(moved, first.oci_manifest_digest);

        let out_dir = tempdir().unwrap();
        let destination = out_dir.path().join("pinned.gfpb");
        let pulled = pull_portable_oci(
            &registry,
            &pull_req(
                &first.oci_manifest_digest,
                &destination,
                None,
                Default::default(),
            ),
            None,
        )
        .unwrap();
        assert_eq!(pulled.report.package_digest, report.package_digest);

        let disagree = out_dir.path().join("disagree.gfpb");
        let error = pull_portable_oci(
            &registry,
            &pull_req(
                "moving",
                &disagree,
                Some(&first.oci_manifest_digest),
                Default::default(),
            ),
            None,
        )
        .unwrap_err();
        assert_eq!(error.code, PortableErrorCode::DigestMismatch);
    }

    #[test]
    fn failure_modes_cannot_claim_successful_receipt() {
        let (_dir, bundle, _) = export_bundle();
        let registry = MemoryOciRegistry::default();
        let published =
            publish_portable_oci(&registry, &publish_req(&bundle, None, None, None), None).unwrap();

        // Missing blob / manifest
        let empty = MemoryOciRegistry::default();
        let out_dir = tempdir().unwrap();
        let destination = out_dir.path().join("missing.gfpb");
        let error = pull_portable_oci(
            &empty,
            &pull_req(
                &published.oci_manifest_digest,
                &destination,
                None,
                Default::default(),
            ),
            None,
        )
        .unwrap_err();
        assert_eq!(error.code, PortableErrorCode::InvalidStructure);
        assert!(!destination.exists());

        // Incompatible media type
        let bad = serde_json::to_vec(&OciManifest {
            schema_version: 2,
            media_type: OCI_MANIFEST_MEDIA_TYPE.to_owned(),
            artifact_type: "application/vnd.other".into(),
            config: OciDescriptor {
                media_type: OCI_CONFIG_MEDIA_TYPE.to_owned(),
                digest: published.oci_manifest_digest.clone(),
                size: 1,
            },
            layers: vec![OciDescriptor {
                media_type: OCI_LAYER_MEDIA_TYPE.to_owned(),
                digest: published.oci_manifest_digest.clone(),
                size: 1,
            }],
            subject: None,
        })
        .unwrap();
        let digest = registry
            .put_manifest(
                "tests/portable",
                &digest_sha256(&bad),
                OCI_MANIFEST_MEDIA_TYPE,
                &bad,
            )
            .unwrap();
        let destination = out_dir.path().join("bad-media.gfpb");
        let error = pull_portable_oci(
            &registry,
            &pull_req(&digest, &destination, None, Default::default()),
            None,
        )
        .unwrap_err();
        assert_eq!(error.code, PortableErrorCode::Incompatible);
        assert!(!destination.exists());

        // Digest mismatch on layer by corrupting stored blob after publish
        let registry2 = MemoryOciRegistry::default();
        let published2 =
            publish_portable_oci(&registry2, &publish_req(&bundle, None, None, None), None)
                .unwrap();
        let registry2 = FaultyDownload {
            registry: registry2,
            corrupt: false,
            corrupt_config: true,
            referrer_error: false,
            cancel: None,
        };
        let destination = out_dir.path().join("corrupt.gfpb");
        let error = pull_portable_oci(
            &registry2,
            &pull_req(
                &published2.oci_manifest_digest,
                &destination,
                None,
                Default::default(),
            ),
            None,
        )
        .unwrap_err();
        assert_eq!(error.code, PortableErrorCode::DigestMismatch);
        assert!(!destination.exists());
    }

    #[test]
    fn streamed_publication_is_byte_identical_to_whole_file_publication() {
        let (_dir, bundle, report) = export_bundle();
        let registry = MemoryOciRegistry::default();
        let published =
            publish_portable_oci(&registry, &publish_req(&bundle, None, None, None), None).unwrap();
        // Rebuild what whole-buffer publication produced, from the file bytes alone.
        let whole = fs::read(&bundle).unwrap();
        let layer_digest = digest_sha256(&whole);
        let config_bytes = serde_json::to_vec(&OciConfig {
            contract: CONFIG_CONTRACT.to_owned(),
            package_digest: report.package_digest.clone(),
            package_class: package_class_str(report.package_class).to_owned(),
            representation: "bundle".to_owned(),
            transport_digest: report.transport_digest.clone(),
            layer_media_type: OCI_LAYER_MEDIA_TYPE.to_owned(),
        })
        .unwrap();
        let manifest_bytes = serde_json::to_vec(&OciManifest {
            schema_version: 2,
            media_type: OCI_MANIFEST_MEDIA_TYPE.to_owned(),
            artifact_type: OCI_ARTIFACT_TYPE.to_owned(),
            config: OciDescriptor {
                media_type: OCI_CONFIG_MEDIA_TYPE.to_owned(),
                digest: digest_sha256(&config_bytes),
                size: config_bytes.len() as u64,
            },
            layers: vec![OciDescriptor {
                media_type: OCI_LAYER_MEDIA_TYPE.to_owned(),
                digest: layer_digest.clone(),
                size: whole.len() as u64,
            }],
            subject: None,
        })
        .unwrap();
        assert_eq!(
            published.oci_manifest_digest,
            digest_sha256(&manifest_bytes)
        );
        let (_, stored) = registry
            .get_manifest("tests/portable", &published.oci_manifest_digest)
            .unwrap();
        assert_eq!(stored, manifest_bytes);
        assert_eq!(
            registry.get_blob("tests/portable", &layer_digest).unwrap(),
            whole
        );
        assert_eq!(
            published.bytes_transferred,
            (config_bytes.len() + whole.len() + manifest_bytes.len()) as u64
        );

        let out = tempdir().unwrap();
        let destination = out.path().join("pulled.gfpb");
        let pulled = pull_portable_oci(
            &registry,
            &pull_req(
                &published.oci_manifest_digest,
                &destination,
                None,
                Default::default(),
            ),
            None,
        )
        .unwrap();
        assert_eq!(fs::read(&destination).unwrap(), whole);
        assert_eq!(pulled.report.package_digest, report.package_digest);
    }

    #[test]
    fn a_corrupt_or_cancelled_layer_stream_admits_no_package_and_leaves_no_stage() {
        let (_dir, bundle, _) = export_bundle();
        let out = tempdir().unwrap();
        let destination = out.path().join("pull.gfpb");
        let stage = out.path().join(".pull.gfpb.oci.partial");
        for (corrupt, cancel) in [
            (true, None),
            (false, Some(std::sync::Arc::new(AtomicBool::new(false)))),
        ] {
            let registry = MemoryOciRegistry::default();
            let reference =
                publish_portable_oci(&registry, &publish_req(&bundle, None, None, None), None)
                    .unwrap();
            let registry = FaultyDownload {
                registry,
                corrupt,
                corrupt_config: false,
                referrer_error: false,
                cancel: cancel.clone(),
            };
            let error = pull_portable_oci(
                &registry,
                &pull_req(
                    &reference.oci_manifest_digest,
                    &destination,
                    None,
                    Default::default(),
                ),
                cancel.as_deref(),
            )
            .unwrap_err();
            assert_eq!(
                error.code,
                if corrupt {
                    PortableErrorCode::DigestMismatch
                } else {
                    PortableErrorCode::Cancelled
                }
            );
            if corrupt {
                // The layer's own digest refuses it before verification could.
                assert!(error.to_string().contains("layer blob digest mismatch"));
            }
            assert!(!destination.exists());
            assert!(!stage.exists());
        }
    }

    #[test]
    fn authenticity_distinguishes_absent_valid_invalid_and_mismatched() {
        let (_dir, bundle, _) = export_bundle();
        let registry = MemoryOciRegistry::default();
        let secret = b"test-signing-key".to_vec();
        let published = publish_portable_oci(
            &registry,
            &publish_req(
                &bundle,
                None,
                Some(PortableOciSignatureMaterial {
                    signer: "releases@curatelabs.ai".into(),
                    key_id: "test-key".into(),
                    secret: secret.clone(),
                }),
                None,
            ),
            None,
        )
        .unwrap();

        let absent = evaluate_portable_oci_signature_state(
            &MemoryOciRegistry::default(),
            "tests/portable",
            &published.oci_manifest_digest,
            &published.package_digest,
            &PortableOciAuthenticityPolicy {
                require_named_signer: Some("releases@curatelabs.ai".into()),
                verification_key: Some(secret.clone()),
            },
        )
        .unwrap();
        assert_eq!(absent, PortableOciSignatureState::Absent);

        let valid = evaluate_portable_oci_signature_state(
            &registry,
            "tests/portable",
            &published.oci_manifest_digest,
            &published.package_digest,
            &PortableOciAuthenticityPolicy {
                require_named_signer: Some("releases@curatelabs.ai".into()),
                verification_key: Some(secret.clone()),
            },
        )
        .unwrap();
        assert_eq!(valid, PortableOciSignatureState::Valid);

        let mismatched = evaluate_portable_oci_signature_state(
            &registry,
            "tests/portable",
            &published.oci_manifest_digest,
            &published.package_digest,
            &PortableOciAuthenticityPolicy {
                require_named_signer: Some("other@example.com".into()),
                verification_key: Some(secret.clone()),
            },
        )
        .unwrap();
        assert_eq!(mismatched, PortableOciSignatureState::PolicyMismatched);

        let invalid = evaluate_portable_oci_signature_state(
            &registry,
            "tests/portable",
            &published.oci_manifest_digest,
            &published.package_digest,
            &PortableOciAuthenticityPolicy {
                require_named_signer: Some("releases@curatelabs.ai".into()),
                verification_key: Some(b"wrong-key".to_vec()),
            },
        )
        .unwrap();
        assert_eq!(invalid, PortableOciSignatureState::Invalid);

        // Policy requiring signer with unsigned package: integrity would pass, authenticity absent.
        let unsigned_registry = MemoryOciRegistry::default();
        let unsigned = publish_portable_oci(
            &unsigned_registry,
            &publish_req(&bundle, None, None, None),
            None,
        )
        .unwrap();
        let out_dir = tempdir().unwrap();
        let destination = out_dir.path().join("unsigned.gfpb");
        let error = pull_portable_oci(
            &unsigned_registry,
            &pull_req(
                &unsigned.oci_manifest_digest,
                &destination,
                None,
                PortableOciAuthenticityPolicy {
                    require_named_signer: Some("releases@curatelabs.ai".into()),
                    verification_key: Some(secret),
                },
            ),
            None,
        )
        .unwrap_err();
        assert_eq!(error.code, PortableErrorCode::Incompatible);
        assert!(!destination.exists());
        let detail = format!("{error:?}");
        assert!(!detail.contains("DigestMismatch"));
    }

    #[test]
    fn cancellation_before_upload_does_not_claim_publication() {
        let (_dir, bundle, _) = export_bundle();
        let registry = MemoryOciRegistry::default();
        let cancelled = AtomicBool::new(true);
        let error = publish_portable_oci(
            &registry,
            &publish_req(&bundle, None, None, None),
            Some(&cancelled),
        )
        .unwrap_err();
        assert_eq!(error.code, PortableErrorCode::Cancelled);
    }

    #[test]
    fn requests_redact_credentials_and_signing_material_in_debug() {
        let secret = b"private-key-sentinel".to_vec();
        let policy = PortableOciAuthenticityPolicy {
            require_named_signer: Some("release".into()),
            verification_key: Some(secret.clone()),
        };
        let material = PortableOciSignatureMaterial {
            signer: "release".into(),
            key_id: "key-id".into(),
            secret: secret.clone(),
        };
        let mut publish = publish_req(
            Path::new("package"),
            None,
            Some(material),
            Some("credential-sentinel"),
        );
        publish.authenticity = policy.clone();
        let mut pull = pull_req("latest", Path::new("destination"), None, policy.clone());
        pull.credential = Some("credential-sentinel");
        let facade = crate::portable::PortableOciPullFacadeRequest {
            registry: "local".into(),
            repository: "repo".into(),
            reference: "latest".into(),
            expected_oci_digest: None,
            destination: PathBuf::from("destination"),
            limits: PortableLimits::default(),
            authenticity: policy,
            insecure_http: true,
            credential: Some("credential-sentinel".into()),
        };
        for debug in [
            format!("{publish:?}"),
            format!("{pull:?}"),
            format!("{facade:?}"),
        ] {
            assert!(!debug.contains("credential-sentinel"));
            assert!(!debug.contains(&format!("{secret:?}")));
            assert!(debug.contains("[REDACTED]"));
        }
    }
    #[test]
    fn http_registry_rejects_credential_in_host() {
        let error = match HttpOciRegistry::new("user:pass@ghcr.io", None, false) {
            Err(error) => error,
            Ok(_) => panic!("expected invalid registry host"),
        };
        assert_eq!(error.code, PortableErrorCode::InvalidPath);
    }
}

impl std::fmt::Debug for PortableOciPublishRequest<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PortableOciPublishRequest")
            .field("package_path", &self.package_path)
            .field("registry", &self.registry)
            .field("repository", &self.repository)
            .field("tag", &self.tag)
            .field("limits", &self.limits)
            .field("authenticity", &self.authenticity)
            .field("signature", &self.signature)
            .field("credential", &"[REDACTED]")
            .finish()
    }
}

impl std::fmt::Debug for PortableOciPullRequest<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PortableOciPullRequest")
            .field("registry", &self.registry)
            .field("repository", &self.repository)
            .field("reference", &self.reference)
            .field("expected_oci_digest", &self.expected_oci_digest)
            .field("destination", &self.destination)
            .field("limits", &self.limits)
            .field("authenticity", &self.authenticity)
            .field("credential", &"[REDACTED]")
            .finish()
    }
}

#[cfg(test)]
struct FaultyDownload {
    registry: MemoryOciRegistry,
    /// Flips the first byte of the layer stream.
    corrupt: bool,
    /// Flips the first byte of the config blob.
    corrupt_config: bool,
    referrer_error: bool,
    /// Raised by the first read of the layer stream.
    cancel: Option<std::sync::Arc<AtomicBool>>,
}
#[cfg(test)]
struct CancelOnRead {
    inner: std::io::Cursor<Vec<u8>>,
    flag: std::sync::Arc<AtomicBool>,
}
#[cfg(test)]
impl Read for CancelOnRead {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.flag.store(true, Ordering::Relaxed);
        self.inner.read(buf)
    }
}
#[cfg(test)]
impl PortableOciRegistry for FaultyDownload {
    fn list_referrers(&self, r: &str, d: &str) -> Result<Vec<Vec<u8>>, PortableError> {
        if self.referrer_error {
            Err(PortableError::new(
                PortableErrorCode::Io,
                "injected referrer failure",
            ))
        } else {
            self.registry.list_referrers(r, d)
        }
    }

    fn put_blob(&self, r: &str, d: &str, b: &[u8]) -> Result<(), PortableError> {
        self.registry.put_blob(r, d, b)
    }
    fn put_blob_stream(
        &self,
        r: &str,
        d: &str,
        size: u64,
        source: &mut dyn Read,
    ) -> Result<(), PortableError> {
        self.registry.put_blob_stream(r, d, size, source)
    }
    fn get_blob_stream<'a>(
        &'a self,
        r: &str,
        d: &str,
        size: u64,
    ) -> Result<Box<dyn Read + 'a>, PortableError> {
        let mut bytes = Vec::new();
        self.registry
            .get_blob_stream(r, d, size)?
            .read_to_end(&mut bytes)
            .unwrap();
        if self.corrupt
            && let Some(first) = bytes.first_mut()
        {
            *first ^= 0xff;
        }
        let inner = std::io::Cursor::new(bytes);
        Ok(match &self.cancel {
            Some(flag) => Box::new(CancelOnRead {
                inner,
                flag: std::sync::Arc::clone(flag),
            }),
            None => Box::new(inner),
        })
    }
    fn get_blob(&self, r: &str, d: &str) -> Result<Vec<u8>, PortableError> {
        let mut b = self.registry.get_blob(r, d)?;
        if self.corrupt_config
            && let Some(first) = b.first_mut()
        {
            *first ^= 0xff;
        }
        Ok(b)
    }
    fn blob_exists(&self, r: &str, d: &str) -> Result<bool, PortableError> {
        self.registry.blob_exists(r, d)
    }
    fn put_manifest(&self, r: &str, d: &str, m: &str, b: &[u8]) -> Result<String, PortableError> {
        self.registry.put_manifest(r, d, m, b)
    }
    fn get_manifest(&self, r: &str, d: &str) -> Result<(String, Vec<u8>), PortableError> {
        self.registry.get_manifest(r, d)
    }
}

#[cfg(test)]
#[path = "portable_oci_http_tests.rs"]
mod http_tests;

#[cfg(test)]
use graphforge_core::portable::{PortablePackageClass, PortableReport};
#[cfg(test)]
use graphforge_portable_oci::{
    HttpOciRegistry, MemoryOciRegistry, evaluate_portable_oci_signature_state,
};
