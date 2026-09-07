//! API-owned portable OCI publication and retrieval.
#![allow(missing_docs)]
use graphforge_core::portable::{
    PortableV2Error, PortableV2ErrorCode, PortableV2Integrity, PortableV2Limits, PortableV2Mode,
    PortableV2OciAuthenticityPolicy, PortableV2OciPhase, PortableV2OciProgress,
    PortableV2OciPullReceipt, PortableV2OciReference, PortableV2OciSignatureMaterial,
    PortableV2OciSignatureState, PortableV2Representation,
};
use graphforge_portable_oci::{
    CONFIG_CONTRACT, OCI_ARTIFACT_TYPE, OCI_CONFIG_MEDIA_TYPE, OCI_LAYER_MEDIA_TYPE,
    OCI_MANIFEST_MEDIA_TYPE, OciConfig, OciDescriptor, OciManifest, PortableV2OciRegistry,
    attach_signature_referrer, authenticity_error, check_cancelled, digest_sha256,
    evaluate_signature_state, package_class_str, validate_digest, validate_reference,
    validate_repository,
};
use graphforge_storage::{
    portable_bytes::{PortablePackageStage, portable_bundle_byte_limit, read_package_bytes},
    verify_portable_v2,
};
use std::fs;
use std::path::Path;
#[cfg(test)]
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
/// Publish request for a locally verified portable-v2 bundle.
#[derive(Clone)]
pub struct PortableV2OciPublishRequest<'a> {
    /// Verified local package path (bundle or expanded).
    pub package_path: &'a Path,
    /// Registry host, e.g. `127.0.0.1:5000` or `ghcr.io`.
    pub registry: &'a str,
    /// Repository name, e.g. `curatelabs/graphforge-packages`.
    pub repository: &'a str,
    /// Optional mutable tag annotation; pull-by-digest remains authoritative.
    pub tag: Option<&'a str>,
    /// Verifier limits applied before upload.
    pub limits: PortableV2Limits,
    /// Optional authenticity policy (signature verification is explicit).
    pub authenticity: PortableV2OciAuthenticityPolicy,
    /// Optional signature to attach as an OCI referrer after publish.
    pub signature: Option<PortableV2OciSignatureMaterial>,
    /// Optional bearer token or basic `user:pass` supplied by the caller.
    /// Never persisted or logged by GraphForge.
    pub credential: Option<&'a str>,
}

/// Pull request resolved by OCI manifest digest (or tag that is immediately
/// re-resolved to a digest before materialization).
#[derive(Clone)]
pub struct PortableV2OciPullRequest<'a> {
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
    pub limits: PortableV2Limits,
    /// Optional authenticity policy.
    pub authenticity: PortableV2OciAuthenticityPolicy,
    /// Optional credential (never logged).
    pub credential: Option<&'a str>,
}

/// Verify locally, map to OCI descriptors, and publish by digest (optional tag).
pub(crate) fn publish_portable_v2_oci(
    registry: &dyn PortableV2OciRegistry,
    request: &PortableV2OciPublishRequest<'_>,
    cancelled: Option<&AtomicBool>,
) -> Result<PortableV2OciReference, PortableV2Error> {
    publish_portable_v2_oci_with_progress(registry, request, cancelled, |_| {})
}

/// Publish with sanitized progress callbacks.
#[expect(
    clippy::too_many_lines,
    reason = "keeps verify/upload/observe and signature attach in one fail-closed publish path"
)]
pub(crate) fn publish_portable_v2_oci_with_progress(
    registry: &dyn PortableV2OciRegistry,
    request: &PortableV2OciPublishRequest<'_>,
    cancelled: Option<&AtomicBool>,
    mut progress: impl FnMut(PortableV2OciProgress),
) -> Result<PortableV2OciReference, PortableV2Error> {
    check_cancelled(cancelled)?;
    progress(PortableV2OciProgress {
        phase: PortableV2OciPhase::VerifyLocal,
        bytes_transferred: 0,
        digest: None,
    });
    let report = verify_portable_v2(
        request.package_path,
        PortableV2Mode::Full,
        request.limits,
        cancelled,
    )?;
    if report.integrity != PortableV2Integrity::Verified {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::DigestMismatch,
            "package integrity is not verified",
        ));
    }

    let layer_bytes = read_package_bytes(
        request.package_path,
        &report,
        portable_bundle_byte_limit(request.limits)?,
    )?;
    check_cancelled(cancelled)?;
    let layer_digest = digest_sha256(&layer_bytes);
    let config = OciConfig {
        contract: CONFIG_CONTRACT.to_owned(),
        package_digest: report.package_digest.clone(),
        package_class: package_class_str(report.package_class).to_owned(),
        representation: match report.representation {
            PortableV2Representation::Bundle => "bundle".to_owned(),
            PortableV2Representation::Expanded => "expanded".to_owned(),
        },
        transport_digest: report.transport_digest.clone(),
        layer_media_type: OCI_LAYER_MEDIA_TYPE.to_owned(),
    };
    let config_bytes = serde_json::to_vec(&config).map_err(|_| {
        PortableV2Error::new(PortableV2ErrorCode::Io, "failed to encode OCI config")
    })?;
    let config_digest = digest_sha256(&config_bytes);
    let mut bytes_transferred = 0_u64;

    if !registry.blob_exists(request.repository, &config_digest)? {
        registry.put_blob(request.repository, &config_digest, &config_bytes)?;
        bytes_transferred += config_bytes.len() as u64;
        progress(PortableV2OciProgress {
            phase: PortableV2OciPhase::UploadBlob,
            bytes_transferred,
            digest: Some(config_digest.clone()),
        });
    }
    check_cancelled(cancelled)?;
    if !registry.blob_exists(request.repository, &layer_digest)? {
        registry.put_blob(request.repository, &layer_digest, &layer_bytes)?;
        bytes_transferred += layer_bytes.len() as u64;
        progress(PortableV2OciProgress {
            phase: PortableV2OciPhase::UploadBlob,
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
            size: layer_bytes.len() as u64,
        }],
        subject: None,
    };
    let manifest_bytes = serde_json::to_vec(&manifest).map_err(|_| {
        PortableV2Error::new(PortableV2ErrorCode::Io, "failed to encode OCI manifest")
    })?;
    let digest_ref = digest_sha256(&manifest_bytes);
    let published = registry.put_manifest(
        request.repository,
        &digest_ref,
        OCI_MANIFEST_MEDIA_TYPE,
        &manifest_bytes,
    )?;
    if published != digest_ref {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::DigestMismatch,
            "registry returned a different manifest digest",
        ));
    }
    bytes_transferred += manifest_bytes.len() as u64;
    progress(PortableV2OciProgress {
        phase: PortableV2OciPhase::UploadManifest,
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
        progress(PortableV2OciProgress {
            phase: PortableV2OciPhase::AttachSignature,
            bytes_transferred,
            digest: Some(digest_ref.clone()),
        });
    }

    progress(PortableV2OciProgress {
        phase: PortableV2OciPhase::Observe,
        bytes_transferred,
        digest: Some(digest_ref.clone()),
    });
    let (media, observed) = registry.get_manifest(request.repository, &digest_ref)?;
    if media != OCI_MANIFEST_MEDIA_TYPE && !media.starts_with(OCI_MANIFEST_MEDIA_TYPE) {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::Incompatible,
            "incompatible manifest media type",
        ));
    }
    if digest_sha256(&observed) != digest_ref {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::DigestMismatch,
            "fresh registry observation does not match published digest",
        ));
    }

    Ok(PortableV2OciReference {
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
pub(crate) fn pull_portable_v2_oci(
    registry: &dyn PortableV2OciRegistry,
    request: &PortableV2OciPullRequest<'_>,
    cancelled: Option<&AtomicBool>,
) -> Result<PortableV2OciPullReceipt, PortableV2Error> {
    pull_portable_v2_oci_with_progress(registry, request, cancelled, |_| {})
}

/// Pull with sanitized progress callbacks.
#[expect(
    clippy::too_many_lines,
    reason = "keeps download/verify/authenticity evaluation in one fail-closed pull path"
)]
pub(crate) fn pull_portable_v2_oci_with_progress(
    registry: &dyn PortableV2OciRegistry,
    request: &PortableV2OciPullRequest<'_>,
    cancelled: Option<&AtomicBool>,
    mut progress: impl FnMut(PortableV2OciProgress),
) -> Result<PortableV2OciPullReceipt, PortableV2Error> {
    check_cancelled(cancelled)?;
    validate_repository(request.repository)?;
    validate_reference(request.reference)?;
    if let Some(expected) = request.expected_oci_digest {
        validate_digest(expected)?;
    }

    progress(PortableV2OciProgress {
        phase: PortableV2OciPhase::DownloadManifest,
        bytes_transferred: 0,
        digest: None,
    });
    let (_media, manifest_bytes) = registry.get_manifest(request.repository, request.reference)?;
    let oci_manifest_digest = digest_sha256(&manifest_bytes);
    let mut bytes_transferred = manifest_bytes.len() as u64;
    if request.reference.starts_with("sha256:") && request.reference != oci_manifest_digest {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::DigestMismatch,
            "requested digest does not match downloaded manifest",
        ));
    }
    if let Some(expected) = request.expected_oci_digest
        && expected != oci_manifest_digest
    {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::DigestMismatch,
            "tag or reference disagrees with expected OCI digest",
        ));
    }
    let manifest: OciManifest = serde_json::from_slice(&manifest_bytes).map_err(|_| {
        PortableV2Error::new(
            PortableV2ErrorCode::InvalidStructure,
            "manifest is not valid OCI JSON",
        )
    })?;
    if manifest.artifact_type != OCI_ARTIFACT_TYPE {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::Incompatible,
            "unsupported OCI artifact type",
        ));
    }
    if manifest.layers.len() != 1 {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::InvalidStructure,
            "portable-v2 OCI artifacts require exactly one layer",
        ));
    }
    if manifest.layers[0].media_type != OCI_LAYER_MEDIA_TYPE {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::Incompatible,
            "incompatible layer media type",
        ));
    }
    if manifest.config.media_type != OCI_CONFIG_MEDIA_TYPE {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::Incompatible,
            "incompatible config media type",
        ));
    }

    check_cancelled(cancelled)?;
    progress(PortableV2OciProgress {
        phase: PortableV2OciPhase::DownloadBlob,
        bytes_transferred,
        digest: Some(manifest.config.digest.clone()),
    });
    let config_bytes = registry.get_blob(request.repository, &manifest.config.digest)?;
    bytes_transferred += config_bytes.len() as u64;
    if digest_sha256(&config_bytes) != manifest.config.digest {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::DigestMismatch,
            "config blob digest mismatch",
        ));
    }
    let config: OciConfig = serde_json::from_slice(&config_bytes).map_err(|_| {
        PortableV2Error::new(
            PortableV2ErrorCode::InvalidStructure,
            "config blob is not valid GraphForge OCI config",
        )
    })?;
    if config.contract != CONFIG_CONTRACT {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::UnsupportedFuture,
            "unsupported OCI config contract",
        ));
    }

    check_cancelled(cancelled)?;
    progress(PortableV2OciProgress {
        phase: PortableV2OciPhase::DownloadBlob,
        bytes_transferred,
        digest: Some(manifest.layers[0].digest.clone()),
    });
    let layer_bytes = registry.get_blob(request.repository, &manifest.layers[0].digest)?;
    bytes_transferred += layer_bytes.len() as u64;
    if digest_sha256(&layer_bytes) != manifest.layers[0].digest {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::DigestMismatch,
            "layer blob digest mismatch",
        ));
    }

    let parent = request.destination.parent().ok_or_else(|| {
        PortableV2Error::new(
            PortableV2ErrorCode::InvalidPath,
            "destination has no parent",
        )
    })?;
    fs::create_dir_all(parent).map_err(|_| {
        PortableV2Error::new(
            PortableV2ErrorCode::Io,
            "failed to create destination parent",
        )
    })?;
    let stage = parent.join(format!(
        ".{}.oci.partial",
        request
            .destination
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("package")
    ));
    let stage = PortablePackageStage::create(
        &stage,
        &mut layer_bytes.as_slice(),
        portable_bundle_byte_limit(request.limits)?,
    )?;
    if check_cancelled(cancelled).is_err() {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::Cancelled,
            "portable OCI pull cancelled",
        ));
    }
    progress(PortableV2OciProgress {
        phase: PortableV2OciPhase::VerifyPulled,
        bytes_transferred,
        digest: Some(oci_manifest_digest.clone()),
    });
    let report = match verify_portable_v2(
        stage.path(),
        PortableV2Mode::Full,
        request.limits,
        cancelled,
    ) {
        Ok(report) => report,
        Err(error) => {
            return Err(error);
        }
    };
    if report.package_digest != config.package_digest {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::DigestMismatch,
            "pulled package digest does not match OCI config",
        ));
    }

    progress(PortableV2OciProgress {
        phase: PortableV2OciPhase::EvaluateAuthenticity,
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
        && signature_state != PortableV2OciSignatureState::Valid
    {
        return Err(authenticity_error(signature_state));
    }

    if request.destination.exists() {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::InvalidPath,
            "destination already exists",
        ));
    }
    stage.publish(request.destination)?;

    let tag = if request.reference.starts_with("sha256:") {
        None
    } else {
        Some(request.reference.to_owned())
    };
    Ok(PortableV2OciPullReceipt {
        reference: PortableV2OciReference {
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
        GRAPH_CAPABILITY_ID, GRAPH_CAPABILITY_VERSION, GraphWriter, PortableV2ExportLimits,
        PortableV2GraphSelector, PortableV2Output, PortableV2PropertyProjection,
        PortableV2SelectionProfile, PortableV2SelectionRequest, PortableV2SubsetClosure,
        PortableV2SubsetRequest, ProjectCapability, ProjectGenerationRequest, ProjectStageOutcome,
        capture_graph_files, empty_workspace_participants, export_complete_portable_v2,
        open_or_initialize_project, plan_complete_portable_v2, plan_graph_subset_portable_v2,
        plan_selected_portable_v2, preview_portable_v2_graph_subset, preview_portable_v2_selection,
        resolve_project_generation, stage_project_generation_with_graph_tree,
    };
    use std::collections::HashMap;
    use std::sync::atomic::AtomicBool;
    use tempfile::tempdir;
    use uuid::Uuid;

    fn export_bundle_for(
        profile: PortableV2SelectionProfile,
    ) -> (tempfile::TempDir, PathBuf, PortableV2Report) {
        let dir = tempdir().unwrap();
        let root = dir.path().join("project");
        open_or_initialize_project(&root).unwrap();
        let generation = resolve_project_generation(&root).unwrap();
        let limits = PortableV2ExportLimits::default();
        let plan = match profile {
            PortableV2SelectionProfile::Complete => {
                plan_complete_portable_v2(&generation, limits).unwrap()
            }
            other => {
                let selection = preview_portable_v2_selection(
                    &generation,
                    &PortableV2SelectionRequest {
                        profile: other,
                        strict: false,
                    },
                    limits,
                )
                .unwrap();
                plan_selected_portable_v2(&generation, &selection, limits).unwrap()
            }
        };
        let bundle = dir.path().join("pkg.gfpb");
        export_complete_portable_v2(
            &plan,
            &bundle,
            PortableV2Output::Bundle,
            limits,
            &AtomicBool::new(false),
            |_| {},
        )
        .unwrap();
        let report = verify_portable_v2(
            &bundle,
            PortableV2Mode::Full,
            PortableV2Limits::default(),
            None,
        )
        .unwrap();
        (dir, bundle, report)
    }

    fn export_bundle() -> (tempfile::TempDir, PathBuf, PortableV2Report) {
        export_bundle_for(PortableV2SelectionProfile::Complete)
    }

    fn publish_req<'a>(
        bundle: &'a Path,
        tag: Option<&'a str>,
        signature: Option<PortableV2OciSignatureMaterial>,
        credential: Option<&'a str>,
    ) -> PortableV2OciPublishRequest<'a> {
        PortableV2OciPublishRequest {
            package_path: bundle,
            registry: "memory.local",
            repository: "tests/portable",
            tag,
            limits: PortableV2Limits::default(),
            authenticity: PortableV2OciAuthenticityPolicy::default(),
            signature,
            credential,
        }
    }

    fn pull_req<'a>(
        reference: &'a str,
        destination: &'a Path,
        expected: Option<&'a str>,
        authenticity: PortableV2OciAuthenticityPolicy,
    ) -> PortableV2OciPullRequest<'a> {
        PortableV2OciPullRequest {
            registry: "memory.local",
            repository: "tests/portable",
            reference,
            expected_oci_digest: expected,
            destination,
            limits: PortableV2Limits::default(),
            authenticity,
            credential: None,
        }
    }

    #[test]
    fn pull_keeps_foreign_stage_and_cleans_owned_stage_after_authentication_error() {
        let (_dir, bundle, _) = export_bundle();
        let registry = MemoryOciRegistry::default();
        let reference =
            publish_portable_v2_oci(&registry, &publish_req(&bundle, None, None, None), None)
                .unwrap();
        let out = tempdir().unwrap();
        let destination = out.path().join("pull.gfpb");
        let stage = out.path().join(".pull.gfpb.oci.partial");
        fs::write(&stage, b"foreign stage").unwrap();
        assert!(
            pull_portable_v2_oci(
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
            referrer_error: true,
        };
        let error = pull_portable_v2_oci(
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
        assert_eq!(error.code, PortableV2ErrorCode::Io);
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
        let published = publish_portable_v2_oci(&registry, &request, None).unwrap();
        let output = tempdir().unwrap();
        let destination = output.path().join("pull.gfpb");
        let mut request = pull_req(
            &published.oci_manifest_digest,
            &destination,
            None,
            Default::default(),
        );
        request.limits.max_total_bytes = report.payload_bytes;
        let receipt = pull_portable_v2_oci(&registry, &request, None).unwrap();
        assert_eq!(receipt.report, report);
    }
    #[test]
    fn publish_and_pull_by_digest_preserves_package_identity() {
        let (_dir, bundle, report) = export_bundle();
        let registry = MemoryOciRegistry::default();
        let published = publish_portable_v2_oci(
            &registry,
            &publish_req(&bundle, Some("latest"), None, Some("user:secret-token")),
            None,
        )
        .unwrap();
        assert_eq!(published.package_digest, report.package_digest);

        let out_dir = tempdir().unwrap();
        let destination = out_dir.path().join("pulled.gfpb");
        let pulled = pull_portable_v2_oci(
            &registry,
            &PortableV2OciPullRequest {
                registry: "memory.local",
                repository: "tests/portable",
                reference: &published.oci_manifest_digest,
                expected_oci_digest: None,
                destination: &destination,
                limits: PortableV2Limits::default(),
                authenticity: PortableV2OciAuthenticityPolicy::default(),
                credential: Some("user:secret-token"),
            },
            None,
        )
        .unwrap();
        assert_eq!(pulled.report.package_digest, report.package_digest);
        assert_eq!(pulled.signature_state, PortableV2OciSignatureState::Absent);
        let json = serde_json::to_string(&pulled).unwrap();
        assert!(!json.contains("secret-token"));
        assert!(!json.contains("user:"));
    }

    #[test]
    fn selective_package_classes_round_trip_through_local_registry() {
        for profile in [
            PortableV2SelectionProfile::Complete,
            PortableV2SelectionProfile::OntologyOnly,
            PortableV2SelectionProfile::Settings,
        ] {
            let (_dir, bundle, report) = export_bundle_for(profile);
            let registry = MemoryOciRegistry::default();
            let published =
                publish_portable_v2_oci(&registry, &publish_req(&bundle, None, None, None), None)
                    .unwrap();
            assert_eq!(published.package_class, report.package_class);
            let out_dir = tempdir().unwrap();
            let destination = out_dir.path().join("pulled.gfpb");
            let pulled = pull_portable_v2_oci(
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
        let limits = PortableV2Limits::default();
        let mut selected = [nodes[0], nodes[1]].map(|uuid| uuid.hyphenated().to_string());
        selected.sort();
        let subset = PortableV2SubsetRequest {
            selector: PortableV2GraphSelector {
                node_uuids: selected.to_vec(),
                edge_uuids: vec![],
            },
            closure: PortableV2SubsetClosure::InducedEdges,
            projection: PortableV2PropertyProjection { exclude: vec![] },
        };
        let preview = preview_portable_v2_graph_subset(&generation, &subset, limits).unwrap();
        let plan = plan_graph_subset_portable_v2(&generation, &preview, limits).unwrap();
        let bundle = root.path().join("subset.gfpb");
        export_complete_portable_v2(
            &plan,
            &bundle,
            PortableV2Output::Bundle,
            limits,
            &AtomicBool::new(false),
            |_| {},
        )
        .unwrap();
        let report = verify_portable_v2(&bundle, PortableV2Mode::Full, limits, None).unwrap();
        assert_eq!(
            report.package_class,
            PortableV2PackageClass::GraphDataSubset
        );
        let registry = MemoryOciRegistry::default();
        let published =
            publish_portable_v2_oci(&registry, &publish_req(&bundle, None, None, None), None)
                .unwrap();
        let out_dir = tempdir().unwrap();
        let destination = out_dir.path().join("pulled.gfpb");
        let pulled = pull_portable_v2_oci(
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
            PortableV2PackageClass::GraphDataSubset
        );
    }

    #[test]
    fn mutable_tag_move_does_not_alter_digest_pinned_pull() {
        let (_dir, bundle, report) = export_bundle();
        let registry = MemoryOciRegistry::default();
        let first = publish_portable_v2_oci(
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
        let pulled = pull_portable_v2_oci(
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
        let error = pull_portable_v2_oci(
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
        assert_eq!(error.code, PortableV2ErrorCode::DigestMismatch);
    }

    #[test]
    fn failure_modes_cannot_claim_successful_receipt() {
        let (_dir, bundle, _) = export_bundle();
        let registry = MemoryOciRegistry::default();
        let published =
            publish_portable_v2_oci(&registry, &publish_req(&bundle, None, None, None), None)
                .unwrap();

        // Missing blob / manifest
        let empty = MemoryOciRegistry::default();
        let out_dir = tempdir().unwrap();
        let destination = out_dir.path().join("missing.gfpb");
        let error = pull_portable_v2_oci(
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
        assert_eq!(error.code, PortableV2ErrorCode::InvalidStructure);
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
        let error = pull_portable_v2_oci(
            &registry,
            &pull_req(&digest, &destination, None, Default::default()),
            None,
        )
        .unwrap_err();
        assert_eq!(error.code, PortableV2ErrorCode::Incompatible);
        assert!(!destination.exists());

        // Digest mismatch on layer by corrupting stored blob after publish
        let registry2 = MemoryOciRegistry::default();
        let published2 =
            publish_portable_v2_oci(&registry2, &publish_req(&bundle, None, None, None), None)
                .unwrap();
        let registry2 = FaultyDownload {
            registry: registry2,
            corrupt: true,
            referrer_error: false,
        };
        let destination = out_dir.path().join("corrupt.gfpb");
        let error = pull_portable_v2_oci(
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
        assert_eq!(error.code, PortableV2ErrorCode::DigestMismatch);
        assert!(!destination.exists());
    }

    #[test]
    fn authenticity_distinguishes_absent_valid_invalid_and_mismatched() {
        let (_dir, bundle, _) = export_bundle();
        let registry = MemoryOciRegistry::default();
        let secret = b"test-signing-key".to_vec();
        let published = publish_portable_v2_oci(
            &registry,
            &publish_req(
                &bundle,
                None,
                Some(PortableV2OciSignatureMaterial {
                    signer: "releases@curatelabs.ai".into(),
                    key_id: "test-key".into(),
                    secret: secret.clone(),
                }),
                None,
            ),
            None,
        )
        .unwrap();

        let absent = evaluate_portable_v2_oci_signature_state(
            &MemoryOciRegistry::default(),
            "tests/portable",
            &published.oci_manifest_digest,
            &published.package_digest,
            &PortableV2OciAuthenticityPolicy {
                require_named_signer: Some("releases@curatelabs.ai".into()),
                verification_key: Some(secret.clone()),
            },
        )
        .unwrap();
        assert_eq!(absent, PortableV2OciSignatureState::Absent);

        let valid = evaluate_portable_v2_oci_signature_state(
            &registry,
            "tests/portable",
            &published.oci_manifest_digest,
            &published.package_digest,
            &PortableV2OciAuthenticityPolicy {
                require_named_signer: Some("releases@curatelabs.ai".into()),
                verification_key: Some(secret.clone()),
            },
        )
        .unwrap();
        assert_eq!(valid, PortableV2OciSignatureState::Valid);

        let mismatched = evaluate_portable_v2_oci_signature_state(
            &registry,
            "tests/portable",
            &published.oci_manifest_digest,
            &published.package_digest,
            &PortableV2OciAuthenticityPolicy {
                require_named_signer: Some("other@example.com".into()),
                verification_key: Some(secret.clone()),
            },
        )
        .unwrap();
        assert_eq!(mismatched, PortableV2OciSignatureState::PolicyMismatched);

        let invalid = evaluate_portable_v2_oci_signature_state(
            &registry,
            "tests/portable",
            &published.oci_manifest_digest,
            &published.package_digest,
            &PortableV2OciAuthenticityPolicy {
                require_named_signer: Some("releases@curatelabs.ai".into()),
                verification_key: Some(b"wrong-key".to_vec()),
            },
        )
        .unwrap();
        assert_eq!(invalid, PortableV2OciSignatureState::Invalid);

        // Policy requiring signer with unsigned package: integrity would pass, authenticity absent.
        let unsigned_registry = MemoryOciRegistry::default();
        let unsigned = publish_portable_v2_oci(
            &unsigned_registry,
            &publish_req(&bundle, None, None, None),
            None,
        )
        .unwrap();
        let out_dir = tempdir().unwrap();
        let destination = out_dir.path().join("unsigned.gfpb");
        let error = pull_portable_v2_oci(
            &unsigned_registry,
            &pull_req(
                &unsigned.oci_manifest_digest,
                &destination,
                None,
                PortableV2OciAuthenticityPolicy {
                    require_named_signer: Some("releases@curatelabs.ai".into()),
                    verification_key: Some(secret),
                },
            ),
            None,
        )
        .unwrap_err();
        assert_eq!(error.code, PortableV2ErrorCode::Incompatible);
        assert!(!destination.exists());
        let detail = format!("{error:?}");
        assert!(!detail.contains("DigestMismatch"));
    }

    #[test]
    fn cancellation_before_upload_does_not_claim_publication() {
        let (_dir, bundle, _) = export_bundle();
        let registry = MemoryOciRegistry::default();
        let cancelled = AtomicBool::new(true);
        let error = publish_portable_v2_oci(
            &registry,
            &publish_req(&bundle, None, None, None),
            Some(&cancelled),
        )
        .unwrap_err();
        assert_eq!(error.code, PortableV2ErrorCode::Cancelled);
    }

    #[test]
    fn requests_redact_credentials_and_signing_material_in_debug() {
        let secret = b"private-key-sentinel".to_vec();
        let policy = PortableV2OciAuthenticityPolicy {
            require_named_signer: Some("release".into()),
            verification_key: Some(secret.clone()),
        };
        let material = PortableV2OciSignatureMaterial {
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
        let facade = crate::portable::PortableV2OciPullFacadeRequest {
            registry: "local".into(),
            repository: "repo".into(),
            reference: "latest".into(),
            expected_oci_digest: None,
            destination: PathBuf::from("destination"),
            limits: PortableV2Limits::default(),
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
        assert_eq!(error.code, PortableV2ErrorCode::InvalidPath);
    }
}

impl std::fmt::Debug for PortableV2OciPublishRequest<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PortableV2OciPublishRequest")
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

impl std::fmt::Debug for PortableV2OciPullRequest<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PortableV2OciPullRequest")
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
    corrupt: bool,
    referrer_error: bool,
}
#[cfg(test)]
impl PortableV2OciRegistry for FaultyDownload {
    fn list_referrers(&self, r: &str, d: &str) -> Result<Vec<Vec<u8>>, PortableV2Error> {
        if self.referrer_error {
            Err(PortableV2Error::new(
                PortableV2ErrorCode::Io,
                "injected referrer failure",
            ))
        } else {
            self.registry.list_referrers(r, d)
        }
    }

    fn put_blob(&self, r: &str, d: &str, b: &[u8]) -> Result<(), PortableV2Error> {
        self.registry.put_blob(r, d, b)
    }
    fn get_blob(&self, r: &str, d: &str) -> Result<Vec<u8>, PortableV2Error> {
        let mut b = self.registry.get_blob(r, d)?;
        if self.corrupt
            && let Some(first) = b.first_mut()
        {
            *first ^= 0xff;
        }
        Ok(b)
    }
    fn blob_exists(&self, r: &str, d: &str) -> Result<bool, PortableV2Error> {
        self.registry.blob_exists(r, d)
    }
    fn put_manifest(&self, r: &str, d: &str, m: &str, b: &[u8]) -> Result<String, PortableV2Error> {
        self.registry.put_manifest(r, d, m, b)
    }
    fn get_manifest(&self, r: &str, d: &str) -> Result<(String, Vec<u8>), PortableV2Error> {
        self.registry.get_manifest(r, d)
    }
}

#[cfg(test)]
#[path = "portable_oci_http_tests.rs"]
mod http_tests;

#[cfg(test)]
use graphforge_core::portable::{PortableV2PackageClass, PortableV2Report};
#[cfg(test)]
use graphforge_portable_oci::{
    HttpOciRegistry, MemoryOciRegistry, evaluate_portable_v2_oci_signature_state,
};
