//! Rust-owned deterministic artifact generation for the public Hub fixture.

use crate::hub_publication::{
    ManifestInputs, PACKAGE_MEDIA_TYPE, build_manifest, derive_summary, digest_bytes,
    export_module_packages, object_descriptor, summary_reference, verify_full,
};
use graphforge_api::{
    ActivationMode, ActivationProfileChangeRequest, DiscoveryOntologyModuleRequest, GraphForge,
    ModuleAdoptionRequest, OntologyAuthorityExpectation, OntologyDoc, OperationId,
    PortableSelection, PortableV2ExportRequest, PortableV2ImportRequest, PortableV2Limits,
    PortableV2Output, PortableV2SelectionProfile, ResearchAccessPolicyMetadata, ResearchCorpusSize,
    UpdateResearchMetadataRequest, WorkspaceResearchMetadata, WriteContext,
    repack_verified_expanded_portable_v2, resolve_discovered_ontology_module,
};
use graphforge_discovery::{
    DISCOVERY_FORMAT, DiscoveryLimits, DiscoveryManifest, ExactIdentity, ObjectDescriptor,
    PORTABLE_V2_FORMAT, PORTABLE_V2_MEDIA_TYPE, PROJECT_SUMMARY_MEDIA_TYPE,
    PortablePackageReference, ProjectSummary, ProtocolVersion, RefSet, RepositoryIdentity,
    RepositoryRef, Sha256Digest,
};
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use uuid::Uuid;

/// Stable generator contract. Git commit evidence belongs to the invoking checkout,
/// rather than caller-controlled artifact metadata.
pub const GENERATOR_CONTRACT: &str = "graphforge-hub-fixture-generator/1";
/// Object location base used by the checked-in fixture. Locations are transport,
/// never identity, so a different base changes only `objects[].locations`.
pub const DEFAULT_LOCATION_BASE: &str = "https://data.graphforge.sh/objects/sha256/";
const OBJECT_PATH: &str = "objects/openalex-openalex.gfpb";
const SUMMARY_PATH: &str = "objects/openalex-openalex.summary.json";
const MODULE_OBJECT_PREFIX: &str = "objects/ontology-module-";
const MODULE_OBJECT_SUFFIX: &str = ".gfpb";
const FIXED_FILES: [&str; 5] = [
    "fixture.json",
    "manifest.json",
    "refs.json",
    OBJECT_PATH,
    SUMMARY_PATH,
];

const MODULE_ID: &str = "https://openalex.org/ontology/works";
const MODULE_VERSION: &str = "2026.01";
const SOURCE_METADATA_OPERATION: u128 = 0x91f2_1a60_d89e_54e1_a000_0000_0000_0001;
const SOURCE_ADOPTION_OPERATION: u128 = 0x91f2_1a60_d89e_54e1_a000_0000_0000_0002;
const SOURCE_PROFILE_OPERATION: u128 = 0x91f2_1a60_d89e_54e1_a000_0000_0000_0003;
const MODULE_EXPORT_OPERATION: u128 = 0x91f2_1a60_d89e_54e1_b000_0000_0000_0001;

#[derive(Debug, Serialize)]
struct GeneratorIdentity<'a> {
    contract: &'a str,
    crate_name: &'a str,
    source_digest: String,
}

#[derive(Debug, Serialize)]
struct SourceIdentity {
    tree_digest: String,
    package_digest: String,
}

/// One per-module package object advertised by the manifest.
#[derive(Debug, PartialEq, Serialize)]
struct ModuleObjectRecord {
    ontology_id: String,
    version: String,
    content_digest: String,
    package_digest: String,
    object_digest: String,
    object_length: u64,
    object_path: String,
}

#[derive(Debug, Serialize)]
struct FixtureMetadata<'a> {
    format: &'a str,
    generator: GeneratorIdentity<'a>,
    source: SourceIdentity,
    object_path: &'a str,
    object_digest: String,
    object_length: u64,
    package_digest: String,
    summary_path: &'a str,
    summary_digest: String,
    summary_object_digest: String,
    summary_object_length: u64,
    module_objects: Vec<ModuleObjectRecord>,
    manifest_digest: String,
}

fn err(error: impl std::fmt::Display) -> String {
    error.to_string()
}

fn module_document() -> Result<OntologyDoc, String> {
    serde_json::from_value(serde_json::json!({
        "ontology_id": MODULE_ID,
        "version": MODULE_VERSION,
        "entity_types": [
            {"name": "Work", "abstract": false, "parent": null},
            {"name": "Author", "abstract": false, "parent": null},
            {"name": "Institution", "abstract": false, "parent": null}
        ],
        "relation_types": [{"name": "AUTHORED", "src": "Author", "dst": "Work"}],
        "properties": [],
        "constraints": [],
        "migrations": []
    }))
    .map_err(err)
}

/// Synthetic public research metadata. These are fixture values, not product
/// behavior; `update_research_metadata` canonicalizes them.
fn fixture_research_metadata() -> WorkspaceResearchMetadata {
    let mut metadata = WorkspaceResearchMetadata::empty();
    metadata.title = Some("OpenAlex".into());
    metadata.authors = vec!["OurResearch".into()];
    metadata.languages = vec!["en".into()];
    metadata.subjects = vec!["scholarly-communication".into()];
    metadata.source_types = vec!["bibliographic-database".into()];
    metadata.ontologies = vec![MODULE_ID.into()];
    metadata.license = Some("CC0-1.0".into());
    metadata.corpus_size = Some(ResearchCorpusSize {
        node_count: Some(1_000),
        relationship_count: Some(2_500),
        source_count: Some(1),
        artifact_count: Some(0),
    });
    metadata.access = ResearchAccessPolicyMetadata {
        visibility: Some("public".into()),
        access_policy: Some("open".into()),
        collaborators: Vec::new(),
    };
    metadata.created_at = Some("2026-01-01T00:00:00.000000Z".into());
    metadata.updated_at = Some("2026-01-01T00:00:00.000000Z".into());
    metadata
}

/// Rebuild the synthetic expanded source package through the public facade and
/// replace `destination` with it.
///
/// The Project is durable, every write identity is fixed, and the export is the
/// complete expanded package, so repeated rebuilds are byte-identical. The new
/// tree is built and fully verified beside `destination` and swapped in only
/// when complete; a failed swap restores the previous tree.
pub fn rebuild_source(destination: &Path) -> Result<(), String> {
    let limits = PortableV2Limits::default();
    let parent = destination
        .parent()
        .ok_or("fixture source destination has no parent directory")?;
    fs::create_dir_all(parent).map_err(err)?;
    let scratch = tempfile::tempdir().map_err(err)?;
    let project = scratch.path().join("project");
    fs::create_dir(&project).map_err(err)?;
    let mut graph = GraphForge::new(project.to_str()).map_err(err)?;
    graph
        .update_research_metadata(UpdateResearchMetadataRequest {
            context: write_context(SOURCE_METADATA_OPERATION),
            metadata: fixture_research_metadata(),
        })
        .map_err(err)?;
    let candidate = graph
        .create_ontology_module(module_document()?, Vec::new(), None)
        .map_err(err)?;
    graph
        .adopt_ontology_module(
            &ModuleAdoptionRequest {
                authority: authority(&graph, SOURCE_ADOPTION_OPERATION)?,
                candidate,
            },
            None,
        )
        .map_err(err)?;
    let (_, activation) = graph.ontology_activation_profile().map_err(err)?;
    graph
        .change_ontology_activation_profile(
            &ActivationProfileChangeRequest {
                authority: authority(&graph, SOURCE_PROFILE_OPERATION)?,
                profile_default: ActivationMode::Advisory,
                activation,
            },
            None,
        )
        .map_err(err)?;
    // Stage beside the destination so the final swap is a same-volume rename.
    let staging = tempfile::Builder::new()
        .prefix(".hub-source-rebuild-")
        .tempdir_in(parent)
        .map_err(err)?;
    let staged = staging.path().join("source");
    graph
        .export_portable_v2(
            &PortableV2ExportRequest {
                selection: PortableSelection::Current,
                output_path: staged.clone(),
                representation: PortableV2Output::Expanded,
                profile: PortableV2SelectionProfile::Complete,
                subset: None,
                limits,
            },
            None,
            |_| {},
        )
        .map_err(err)?;
    verify_full(&staged)?;
    let previous = staging.path().join("previous");
    let had_previous = match fs::rename(destination, &previous) {
        Ok(()) => true,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(error) => return Err(err(error)),
    };
    if let Err(error) = fs::rename(&staged, destination) {
        if had_previous {
            fs::rename(&previous, destination).map_err(|restore| {
                format!("{error}; restoring the previous source also failed: {restore}")
            })?;
        }
        return Err(err(error));
    }
    Ok(())
}

fn write_context(operation: u128) -> WriteContext {
    WriteContext {
        operation_uuid: OperationId(Uuid::from_u128(operation)),
        actor_uuid: None,
    }
}

fn authority(graph: &GraphForge, operation: u128) -> Result<OntologyAuthorityExpectation, String> {
    let state = graph.ontology_authority_state().map_err(err)?;
    Ok(OntologyAuthorityExpectation {
        context: write_context(operation),
        expected_project_generation_uuid: state.project_generation_uuid,
        expected_composition_fingerprint: state.composition_fingerprint,
    })
}

fn location(location_base: &str, digest: &str) -> Result<String, String> {
    let hex = digest
        .strip_prefix("sha256:")
        .ok_or("object digest prefix is invalid")?;
    Ok(format!("{location_base}{hex}"))
}

fn descriptor(
    bytes: &[u8],
    media_type: &str,
    location_base: &str,
) -> Result<ObjectDescriptor, String> {
    let digest = digest_bytes(bytes);
    Ok(ObjectDescriptor {
        locations: vec![location(location_base, &digest)?],
        digest: Sha256Digest(digest),
        length: u64::try_from(bytes.len()).map_err(|_| "object length overflow")?,
        media_type: media_type.into(),
    })
}

fn module_object_path(content_digest: &str) -> Result<String, String> {
    let hex = content_digest
        .strip_prefix("sha256:")
        .filter(|hex| hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .ok_or("module content digest is invalid")?;
    Ok(format!("{MODULE_OBJECT_PREFIX}{hex}{MODULE_OBJECT_SUFFIX}"))
}

fn repository_identity() -> RepositoryIdentity {
    RepositoryIdentity {
        owner: "openalex".into(),
        repository: "openalex".into(),
    }
}

/// Generate canonical object, discovery, and provenance artifacts from the checked-in source.
///
/// `location_base` prefixes every digest-addressed object location. It is
/// transport only: the summary bytes and the manifest's `summary` and `ontology`
/// descriptors do not depend on it.
#[expect(
    clippy::too_many_lines,
    reason = "one transaction binds portable and discovery artifacts"
)]
pub fn generate(source: &Path, destination: &Path, location_base: &str) -> Result<(), String> {
    let limits = PortableV2Limits::default();
    let source_report = verify_full(source)?;
    let scratch = tempfile::tempdir().map_err(err)?;
    let imported = scratch.path().join("imported");
    let operation_id = OperationId(Uuid::from_u128(0x91f2_1a60_d89e_54e1_9000_0000_0000_0001));
    GraphForge::import_portable_v2(
        &imported,
        &PortableV2ImportRequest {
            input: source.to_path_buf(),
            operation_id,
            limits,
        },
        None,
    )
    .map_err(err)?;
    let mut imported_project = GraphForge::new(imported.to_str()).map_err(err)?;
    // A component-selective package records the exporting generation's manifest
    // hash, and the manifest of a first import binds its parent: the random
    // initial generation. Re-committing the identical research metadata under a
    // fixed identity gives the exporting generation a deterministic parent, so
    // module packages are reproducible byte for byte.
    let research_metadata = imported_project.research_project_metadata().map_err(err)?;
    imported_project
        .update_research_metadata(UpdateResearchMetadataRequest {
            context: write_context(MODULE_EXPORT_OPERATION),
            metadata: research_metadata,
        })
        .map_err(err)?;
    fs::create_dir_all(destination.join("objects")).map_err(err)?;
    let object_path = destination.join(OBJECT_PATH);
    let exported = repack_verified_expanded_portable_v2(
        &graphforge_api::PortableV2RepackRequest {
            source: source.to_path_buf(),
            destination: object_path.clone(),
            limits,
        },
        &std::sync::atomic::AtomicBool::new(false),
    )
    .map_err(err)?;
    let verified = verify_full(&object_path)?;
    let imported_bundle = scratch.path().join("imported-bundle");
    let bundle_import = GraphForge::import_portable_v2(
        &imported_bundle,
        &PortableV2ImportRequest {
            input: object_path.clone(),
            operation_id: OperationId(Uuid::from_u128(0x91f2_1a60_d89e_54e1_9000_0000_0000_0002)),
            limits,
        },
        None,
    )
    .map_err(err)?;
    let reopened_bundle = GraphForge::new(imported_bundle.to_str()).map_err(err)?;
    if reopened_bundle
        .committed_generation_identity()
        .map_err(err)?
        .generation_uuid
        != bundle_import.generation_uuid
    {
        return Err("generated bundle did not reopen at its imported generation".into());
    }
    let exported_package_digest = format!("sha256:{}", hex(&exported.package_digest));
    let exported_transport_digest = format!("sha256:{}", hex(&exported.transport_digest));
    if exported_package_digest != verified.package_digest
        || exported_transport_digest != verified.transport_digest.clone().unwrap_or_default()
    {
        return Err("portable export and verification receipts disagree".into());
    }
    let bytes = fs::read(&object_path).map_err(err)?;
    let object_digest = digest_bytes(&bytes);
    if object_digest != exported_transport_digest {
        return Err("transport digest does not bind emitted object bytes".into());
    }
    let repository = repository_identity();
    let immutable_version = Sha256Digest(exported_package_digest.clone());

    // The summary is derived from the verified bundle, never entered by hand.
    let derived = derive_summary(&repository, &immutable_version, &object_path)?;
    write(destination.join(SUMMARY_PATH), &derived.bytes)?;

    // One component-selective package per advertised module, exported from the
    // Project reopened from the same checked-in source.
    let (ontology, modules) =
        export_module_packages(&imported_project, &derived.summary, |digest| {
            Ok(destination.join(module_object_path(digest)?))
        })?;
    let mut module_objects = Vec::new();
    let mut module_records = Vec::new();
    for module in &modules {
        let module_object = object_descriptor(
            module.bundle.object_digest.clone(),
            module.bundle.length,
            PACKAGE_MEDIA_TYPE,
            location(location_base, &module.bundle.object_digest)?,
        );
        module_records.push(ModuleObjectRecord {
            ontology_id: module.descriptor.id.clone(),
            version: module.descriptor.version.clone(),
            content_digest: module.descriptor.content_digest.0.clone(),
            package_digest: module.bundle.package_digest.clone(),
            object_digest: module_object.digest.0.clone(),
            object_length: module_object.length,
            object_path: module_object_path(&module.descriptor.content_digest.0)?,
        });
        module_objects.push(module_object);
    }

    let project_object = descriptor(&bytes, PORTABLE_V2_MEDIA_TYPE, location_base)?;
    let summary_object = descriptor(&derived.bytes, PROJECT_SUMMARY_MEDIA_TYPE, location_base)?;
    let mut objects = vec![project_object.clone(), summary_object.clone()];
    objects.extend(module_objects);
    let manifest = build_manifest(ManifestInputs {
        repository: repository.clone(),
        default_ref: "main".into(),
        resolved_ref: "main".into(),
        package: PortablePackageReference {
            format: PORTABLE_V2_FORMAT.into(),
            package_digest: Sha256Digest(exported_package_digest.clone()),
            object_digest: project_object.digest.clone(),
        },
        summary: Some(summary_reference(&derived, &summary_object)),
        ontology,
        lineage: None,
        objects,
    })?;
    let summary = derived.summary;
    let summary_digest = derived.summary_digest;
    let manifest_bytes = manifest.to_canonical_json().map_err(err)?;
    let manifest_digest = manifest.canonical_digest().map_err(err)?.0;
    let refs = RefSet {
        format: DISCOVERY_FORMAT.into(),
        version: ProtocolVersion::CURRENT,
        repository,
        default_ref: "main".into(),
        refs: vec![RepositoryRef {
            name: "main".into(),
            target: manifest.immutable_version.clone(),
            validator: Sha256Digest(manifest_digest.clone()),
        }],
        extensions: BTreeMap::new(),
    };
    refs.validate_manifest(&manifest).map_err(err)?;
    manifest.bind_summary(&summary).map_err(err)?;
    let refs_bytes = refs.to_canonical_json().map_err(err)?;
    let metadata = FixtureMetadata {
        format: "graphforge-hub-fixture/1",
        generator: GeneratorIdentity {
            contract: GENERATOR_CONTRACT,
            crate_name: "graphforge-cli",
            source_digest: generator_source_digest(),
        },
        source: SourceIdentity {
            tree_digest: tree_digest(source)?,
            package_digest: source_report.package_digest,
        },
        object_path: OBJECT_PATH,
        object_digest,
        object_length: project_object.length,
        package_digest: exported_package_digest,
        summary_path: SUMMARY_PATH,
        summary_digest: summary_digest.0,
        summary_object_digest: summary_object.digest.0,
        summary_object_length: summary_object.length,
        module_objects: module_records,
        manifest_digest,
    };
    write(destination.join("manifest.json"), &manifest_bytes)?;
    write(destination.join("refs.json"), &refs_bytes)?;
    let mut metadata_bytes = serde_json::to_vec(&metadata).map_err(err)?;
    metadata_bytes.push(b'\n');
    write(destination.join("fixture.json"), &metadata_bytes)
}

/// Regenerate in a private directory and compare every checked-in artifact byte.
pub fn check(source: &Path, expected: &Path) -> Result<(), String> {
    validate(source, expected)?;
    let scratch = tempfile::tempdir().map_err(err)?;
    generate(source, scratch.path(), DEFAULT_LOCATION_BASE)?;
    let regenerated = artifact_files(scratch.path())?;
    if regenerated != artifact_files(expected)? {
        return Err("generated Hub fixture artifact set drifted".into());
    }
    for relative in regenerated {
        let actual = fs::read(scratch.path().join(&relative)).map_err(err)?;
        let expected_bytes = fs::read(expected.join(&relative)).map_err(err)?;
        if actual != expected_bytes {
            return Err(format!("generated Hub fixture artifact drift: {relative}"));
        }
    }
    Ok(())
}

/// Sorted relative paths of every regular file under `root`.
fn artifact_files(root: &Path) -> Result<Vec<String>, String> {
    let mut files = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(&directory).map_err(err)? {
            let path = entry.map_err(err)?.path();
            if fs::symlink_metadata(&path).map_err(err)?.is_dir() {
                pending.push(path);
            } else {
                files.push(canonical_relative_path(
                    path.strip_prefix(root).map_err(err)?,
                )?);
            }
        }
    }
    files.sort();
    Ok(files)
}

/// Validate all generator, source, portable, discovery, summary, and metadata bindings.
fn validate(source: &Path, artifacts: &Path) -> Result<(), String> {
    let metadata: serde_json::Value =
        serde_json::from_slice(&fs::read(artifacts.join("fixture.json")).map_err(err)?)
            .map_err(err)?;
    let manifest_bytes = fs::read(artifacts.join("manifest.json")).map_err(err)?;
    let refs_bytes = fs::read(artifacts.join("refs.json")).map_err(err)?;
    let manifest =
        DiscoveryManifest::from_json(&manifest_bytes, DiscoveryLimits::default()).map_err(err)?;
    let refs = RefSet::from_json(&refs_bytes, DiscoveryLimits::default()).map_err(err)?;
    refs.validate_manifest(&manifest).map_err(err)?;
    let module_paths = manifest
        .ontology
        .iter()
        .flat_map(|inventory| &inventory.modules)
        .map(|module| module_object_path(&module.content_digest.0))
        .collect::<Result<Vec<_>, _>>()?;
    validate_expected_tree(artifacts, &module_paths)?;
    let expected_repository = repository_identity();
    if manifest.repository != expected_repository
        || manifest.default_ref != "main"
        || manifest.resolved_ref != "main"
        || refs.repository != expected_repository
        || refs.default_ref != "main"
        || manifest.immutable_version != manifest.package.package_digest
    {
        return Err("fixture repository, ref, or immutable version binding is invalid".into());
    }
    let manifest_digest = manifest.canonical_digest().map_err(err)?.0;
    let reference = refs
        .refs
        .iter()
        .find(|reference| reference.name == "main")
        .ok_or("fixture main ref is absent")?;
    if reference.validator.0 != manifest_digest {
        return Err("fixture validator does not bind canonical manifest bytes".into());
    }
    let selected = manifest.package_object().map_err(err)?;
    if selected.locations != [location(DEFAULT_LOCATION_BASE, &selected.digest.0)?] {
        return Err("fixture object location is not digest addressed".into());
    }
    let object_path = metadata["object_path"]
        .as_str()
        .ok_or("fixture object path is absent")?;
    if object_path != OBJECT_PATH {
        return Err("fixture object path is invalid".into());
    }
    let object = artifacts.join(object_path);
    let bytes = fs::read(&object).map_err(err)?;
    let report = verify_full(&object)?;
    if digest_bytes(&bytes) != selected.digest.0
        || bytes.len() as u64 != selected.length
        || report.package_digest != manifest.package.package_digest.0
        || metadata["format"] != "graphforge-hub-fixture/1"
        || metadata["object_digest"] != selected.digest.0
        || metadata["object_length"] != selected.length
        || metadata["package_digest"] != manifest.package.package_digest.0
        || metadata["manifest_digest"] != manifest_digest
        || metadata["generator"]["contract"] != GENERATOR_CONTRACT
        || metadata["generator"]["crate_name"] != "graphforge-cli"
        || metadata["generator"]["source_digest"] != generator_source_digest()
        || metadata["source"]["tree_digest"] != tree_digest(source)?
        || metadata["source"]["package_digest"] != verify_full(source)?.package_digest
    {
        return Err(
            "fixture portable, discovery, metadata, provenance, or source binding is invalid"
                .into(),
        );
    }
    validate_summary(artifacts, &manifest, &metadata)?;
    validate_modules(
        artifacts,
        &manifest,
        &manifest_bytes,
        &refs_bytes,
        &metadata,
    )
}

/// The summary object must hash to the manifest descriptor, parse through the
/// public contract, and bind to the manifest it is advertised by.
fn validate_summary(
    artifacts: &Path,
    manifest: &DiscoveryManifest,
    metadata: &serde_json::Value,
) -> Result<(), String> {
    let reference = manifest
        .summary
        .as_ref()
        .ok_or("fixture manifest does not advertise a summary")?;
    let selected = manifest.summary_object().map_err(err)?;
    if selected.locations != [location(DEFAULT_LOCATION_BASE, &selected.digest.0)?] {
        return Err("fixture summary location is not digest addressed".into());
    }
    let bytes = fs::read(artifacts.join(SUMMARY_PATH)).map_err(err)?;
    if digest_bytes(&bytes) != reference.object_digest.0 || bytes.len() as u64 != selected.length {
        return Err("fixture summary object bytes do not match the manifest descriptor".into());
    }
    let summary = ProjectSummary::from_json(&bytes, DiscoveryLimits::default()).map_err(err)?;
    manifest.bind_summary(&summary).map_err(err)?;
    if summary.to_canonical_json().map_err(err)? != bytes {
        return Err("fixture summary object is not canonical".into());
    }
    if metadata["summary_path"] != SUMMARY_PATH
        || metadata["summary_digest"] != reference.summary_digest.0
        || metadata["summary_object_digest"] != reference.object_digest.0
        || metadata["summary_object_length"] != selected.length
    {
        return Err("fixture summary metadata binding is invalid".into());
    }
    Ok(())
}

/// Every advertised module package must verify and resolve to exactly the
/// advertised identity, and `fixture.json` must record it.
fn validate_modules(
    artifacts: &Path,
    manifest: &DiscoveryManifest,
    manifest_bytes: &[u8],
    refs_bytes: &[u8],
    metadata: &serde_json::Value,
) -> Result<(), String> {
    let mut records = Vec::new();
    for module in manifest
        .ontology
        .iter()
        .flat_map(|inventory| &inventory.modules)
    {
        let package = module
            .package
            .as_ref()
            .ok_or("fixture ontology module has no package")?;
        let identity = ExactIdentity {
            id: module.id.clone(),
            version: module.version.clone(),
            content_digest: module.content_digest.clone(),
        };
        let (_, selected) = manifest.ontology_module_object(&identity).map_err(err)?;
        if selected.locations != [location(DEFAULT_LOCATION_BASE, &selected.digest.0)?] {
            return Err("fixture module object location is not digest addressed".into());
        }
        let path = module_object_path(&module.content_digest.0)?;
        let file = artifacts.join(&path);
        let bytes = fs::read(&file).map_err(err)?;
        if digest_bytes(&bytes) != selected.digest.0 || bytes.len() as u64 != selected.length {
            return Err("fixture module object bytes do not match the manifest descriptor".into());
        }
        let resolved = resolve_discovered_ontology_module(&DiscoveryOntologyModuleRequest {
            manifest_json: manifest_bytes,
            refs_json: refs_bytes,
            expected_repository: &repository_identity(),
            module: &identity,
            package: &file,
            discovery_limits: DiscoveryLimits::default(),
            portable_limits: PortableV2Limits::default(),
            cancelled: None,
        })
        .map_err(err)?;
        if resolved.module.ontology_id != module.id
            || resolved.module.authored_version != module.version
            || format!("sha256:{}", resolved.module.canonical_digest) != module.content_digest.0
            || resolved.package_digest != package.package_digest.0
        {
            return Err("fixture module package does not resolve to the advertised module".into());
        }
        records.push(ModuleObjectRecord {
            ontology_id: module.id.clone(),
            version: module.version.clone(),
            content_digest: module.content_digest.0.clone(),
            package_digest: package.package_digest.0.clone(),
            object_digest: selected.digest.0.clone(),
            object_length: selected.length,
            object_path: path,
        });
    }
    if metadata["module_objects"] != serde_json::to_value(&records).map_err(err)? {
        return Err("fixture module object records do not match the manifest".into());
    }
    Ok(())
}

fn validate_expected_tree(root: &Path, module_paths: &[String]) -> Result<(), String> {
    let mut actual = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(&directory).map_err(err)? {
            let entry = entry.map_err(err)?;
            let path = entry.path();
            let relative = canonical_relative_path(path.strip_prefix(root).map_err(err)?)?;
            let metadata = fs::symlink_metadata(&path).map_err(err)?;
            if metadata.file_type().is_symlink() {
                return Err(format!(
                    "fixture artifact tree contains a symlink: {relative}"
                ));
            }
            if metadata.is_dir() {
                actual.push(format!("{relative}/"));
                pending.push(path);
            } else if metadata.is_file() {
                actual.push(relative);
            } else {
                return Err("fixture artifact tree contains a non-file entry".into());
            }
        }
    }
    actual.sort();
    let mut expected: Vec<String> = FIXED_FILES
        .iter()
        .map(|path| (*path).to_owned())
        .chain(module_paths.iter().cloned())
        .chain(["objects/".to_owned()])
        .collect();
    expected.sort();
    if actual != expected {
        return Err("fixture artifact tree does not match the exact generated contract".into());
    }
    Ok(())
}

fn write(path: PathBuf, bytes: &[u8]) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(err)?;
    }
    fs::write(path, bytes).map_err(err)
}

fn generator_source_digest() -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"graphforge-hub-fixture-generator-source/1\0");
    hasher.update(include_bytes!("hub_fixture_artifacts.rs"));
    hasher.update(b"\0");
    hasher.update(include_bytes!("hub_publication.rs"));
    format!("sha256:{}", hex(&hasher.finalize()))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(
        String::with_capacity(bytes.len() * 2),
        |mut output, byte| {
            write!(output, "{byte:02x}").expect("writing to String cannot fail");
            output
        },
    )
}

fn canonical_relative_path(path: &Path) -> Result<String, String> {
    path.components()
        .map(|component| match component {
            std::path::Component::Normal(value) => value
                .to_str()
                .map(str::to_owned)
                .ok_or_else(|| "fixture path component is not UTF-8".to_owned()),
            _ => Err("fixture path is not a normalized relative path".to_owned()),
        })
        .collect::<Result<Vec<_>, _>>()
        .map(|components| components.join("/"))
}

fn tree_digest(root: &Path) -> Result<String, String> {
    fn walk(root: &Path, current: &Path, files: &mut Vec<PathBuf>) -> Result<(), String> {
        for entry in fs::read_dir(current).map_err(|error| error.to_string())? {
            let path = entry.map_err(|error| error.to_string())?.path();
            let metadata = fs::symlink_metadata(&path).map_err(|error| error.to_string())?;
            if metadata.file_type().is_symlink() {
                return Err("fixture source may not contain symlinks".into());
            }
            if metadata.is_dir() {
                walk(root, &path, files)?;
            } else if metadata.is_file() {
                files.push(
                    path.strip_prefix(root)
                        .map_err(|error| error.to_string())?
                        .to_path_buf(),
                );
            } else {
                return Err("fixture source contains a non-file entry".into());
            }
        }
        Ok(())
    }
    let mut files = Vec::new();
    walk(root, root, &mut files)?;
    files.sort();
    let mut hasher = Sha256::new();
    hasher.update(b"graphforge-hub-fixture-source/1\0");
    for relative in files {
        let canonical = canonical_relative_path(&relative)?;
        let name = canonical.as_bytes();
        let bytes = fs::read(root.join(&relative)).map_err(|error| error.to_string())?;
        hasher.update((name.len() as u64).to_be_bytes());
        hasher.update(name);
        hasher.update((bytes.len() as u64).to_be_bytes());
        hasher.update(&bytes);
    }
    Ok(format!("sha256:{}", hex(&hasher.finalize())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use graphforge_api::{PortableV2Mode, PortableVerifyRequest, verify_portable_v2};

    fn root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
    }

    fn private_source() -> tempfile::TempDir {
        fn copy_tree(source: &Path, destination: &Path) {
            fs::create_dir_all(destination).unwrap();
            for entry in fs::read_dir(source).unwrap() {
                let entry = entry.unwrap();
                let destination = destination.join(entry.file_name());
                if entry.file_type().unwrap().is_dir() {
                    copy_tree(&entry.path(), &destination);
                } else {
                    fs::copy(entry.path(), destination).unwrap();
                }
            }
        }
        let private = tempfile::tempdir().unwrap();
        copy_tree(
            &root().join("tests/fixtures/hub/openalex-source"),
            private.path(),
        );
        private
    }

    #[test]
    fn relative_paths_use_platform_independent_slash_encoding() {
        let nested = PathBuf::from("objects").join("sha256").join("fixture.gfpb");
        assert_eq!(
            canonical_relative_path(&nested).unwrap(),
            "objects/sha256/fixture.gfpb"
        );
        assert!(canonical_relative_path(Path::new("../outside")).is_err());
    }

    fn copy_artifacts(source: &Path) -> tempfile::TempDir {
        let private = tempfile::tempdir().unwrap();
        copy_tree_for_test(source, private.path());
        private
    }

    fn copy_tree_for_test(source: &Path, destination: &Path) {
        fs::create_dir_all(destination).unwrap();
        for entry in fs::read_dir(source).unwrap() {
            let entry = entry.unwrap();
            let destination = destination.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                copy_tree_for_test(&entry.path(), &destination);
            } else {
                fs::copy(entry.path(), destination).unwrap();
            }
        }
    }

    #[test]
    fn checked_in_hub_fixture_is_exact_rust_output() {
        let source = private_source();
        let artifacts = copy_artifacts(&root().join("tests/fixtures/hub/generated/v1"));
        check(source.path(), artifacts.path()).unwrap();
    }

    #[test]
    fn check_rejects_tampered_artifact() {
        let source = private_source();
        let output = tempfile::tempdir().unwrap();
        generate(source.path(), output.path(), DEFAULT_LOCATION_BASE).unwrap();
        fs::write(output.path().join("refs.json"), b"{}\n").unwrap();
        let error = check(source.path(), output.path()).unwrap_err();
        assert!(!error.is_empty());
    }

    #[test]
    fn generated_artifacts_are_cross_contract_coherent() {
        let generated = root().join("tests/fixtures/hub/generated/v1");
        let manifest_bytes = fs::read(generated.join("manifest.json")).unwrap();
        let refs_bytes = fs::read(generated.join("refs.json")).unwrap();
        let manifest = DiscoveryManifest::from_json(
            &manifest_bytes,
            graphforge_discovery::DiscoveryLimits::default(),
        )
        .unwrap();
        let refs = RefSet::from_json(
            &refs_bytes,
            graphforge_discovery::DiscoveryLimits::default(),
        )
        .unwrap();
        refs.validate_manifest(&manifest).unwrap();
        let object = fs::read(generated.join("objects/openalex-openalex.gfpb")).unwrap();
        let selected = manifest.package_object().unwrap();
        assert_eq!(selected.digest.0, digest_bytes(&object));
        assert_eq!(selected.length, object.len() as u64);
        assert_eq!(
            refs.refs[0].validator.0,
            manifest.canonical_digest().unwrap().0
        );
        let private_object = tempfile::NamedTempFile::new().unwrap();
        fs::write(private_object.path(), &object).unwrap();
        let verified = verify_portable_v2(
            &PortableVerifyRequest {
                input: private_object.path().to_path_buf(),
                mode: PortableV2Mode::Full,
                limits: PortableV2Limits::default(),
            },
            None,
        )
        .unwrap();
        assert_eq!(verified.package_digest, manifest.package.package_digest.0);
        let metadata: serde_json::Value =
            serde_json::from_slice(&fs::read(generated.join("fixture.json")).unwrap()).unwrap();
        assert_eq!(metadata["object_digest"], selected.digest.0);
        assert_eq!(metadata["object_length"], selected.length);
        assert_eq!(
            metadata["package_digest"],
            manifest.package.package_digest.0
        );
        assert_eq!(metadata["manifest_digest"], refs.refs[0].validator.0);
    }

    #[test]
    fn repeated_generation_is_byte_identical() {
        let source = private_source();
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        generate(source.path(), first.path(), DEFAULT_LOCATION_BASE).unwrap();
        generate(source.path(), second.path(), DEFAULT_LOCATION_BASE).unwrap();
        let files = artifact_files(first.path()).unwrap();
        assert_eq!(files, artifact_files(second.path()).unwrap());
        for relative in files {
            assert_eq!(
                fs::read(first.path().join(&relative)).unwrap(),
                fs::read(second.path().join(&relative)).unwrap(),
                "{relative}"
            );
        }
    }

    #[test]
    fn adversarial_binding_matrix_fails_semantic_validation_and_drift() {
        let source = private_source();
        let pristine = root().join("tests/fixtures/hub/generated/v1");
        let cases: &[(&str, &str, fn(&mut serde_json::Value))] = &[
            ("version", "manifest.json", |value| {
                value["version"]["major"] = serde_json::json!(999)
            }),
            ("object-digest", "manifest.json", |value| {
                value["package"]["object_digest"] =
                    serde_json::json!(format!("sha256:{}", "0".repeat(64)))
            }),
            ("object-length", "manifest.json", |value| {
                value["objects"][0]["length"] = serde_json::json!(1)
            }),
            ("object-location", "manifest.json", |value| {
                value["objects"][0]["locations"][0] =
                    serde_json::json!("https://example.com/object")
            }),
            ("repository", "manifest.json", |value| {
                value["repository"]["owner"] = serde_json::json!("other")
            }),
            ("default-ref", "manifest.json", |value| {
                value["default_ref"] = serde_json::json!("other")
            }),
            ("immutable-version", "manifest.json", |value| {
                value["immutable_version"] = serde_json::json!(format!("sha256:{}", "1".repeat(64)))
            }),
            ("package-digest", "manifest.json", |value| {
                value["package"]["package_digest"] =
                    serde_json::json!(format!("sha256:{}", "5".repeat(64)))
            }),
            ("refs-target", "refs.json", |value| {
                value["refs"][0]["target"] = serde_json::json!(format!("sha256:{}", "2".repeat(64)))
            }),
            ("refs-validator", "refs.json", |value| {
                value["refs"][0]["validator"] =
                    serde_json::json!(format!("sha256:{}", "3".repeat(64)))
            }),
            ("provenance", "fixture.json", |value| {
                value["generator"]["source_digest"] =
                    serde_json::json!(format!("sha256:{}", "4".repeat(64)))
            }),
            ("object-path", "fixture.json", |value| {
                value["object_path"] = serde_json::json!("../../outside.gfpb")
            }),
        ];
        for (name, relative, mutate) in cases {
            let candidate = copy_artifacts(&pristine);
            let path = candidate.path().join(relative);
            let mut value: serde_json::Value =
                serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
            mutate(&mut value);
            fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
            assert!(
                validate(source.path(), candidate.path()).is_err(),
                "semantic case {name}"
            );
            assert!(
                check(source.path(), candidate.path()).is_err(),
                "drift case {name}"
            );
        }
        let package = copy_artifacts(&pristine);
        let path = package.path().join("objects/openalex-openalex.gfpb");
        let mut bytes = fs::read(&path).unwrap();
        bytes[1024] ^= 1;
        fs::write(path, bytes).unwrap();
        assert!(validate(source.path(), package.path()).is_err());
        assert!(check(source.path(), package.path()).is_err());
        let altered_source = private_source();
        fs::write(altered_source.path().join("bag-info.txt"), b"changed").unwrap();
        assert!(validate(altered_source.path(), &pristine).is_err());
    }

    #[test]
    fn exact_artifact_tree_rejects_extra_files_and_directories() {
        let source = private_source();
        let pristine = root().join("tests/fixtures/hub/generated/v1");
        for relative in ["extra.json", "objects/extra.gfpb", "extra/nested.json"] {
            let candidate = copy_artifacts(&pristine);
            let path = candidate.path().join(relative);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, b"extra").unwrap();
            assert!(
                validate(source.path(), candidate.path()).is_err(),
                "{relative}"
            );
            assert!(
                check(source.path(), candidate.path()).is_err(),
                "{relative}"
            );
        }
    }

    /// Rewrite a JSON artifact in place.
    fn mutate_json(path: &Path, mutate: impl FnOnce(&mut serde_json::Value)) {
        let mut value: serde_json::Value =
            serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
        mutate(&mut value);
        fs::write(path, serde_json::to_vec(&value).unwrap()).unwrap();
    }

    /// Re-bind the ref validator and fixture metadata to the current manifest so a
    /// test reaches the check it targets instead of the manifest-digest binding.
    fn reseal_manifest_digest(artifacts: &Path) {
        let manifest = DiscoveryManifest::from_json(
            &fs::read(artifacts.join("manifest.json")).unwrap(),
            DiscoveryLimits::default(),
        )
        .unwrap();
        let digest = manifest.canonical_digest().unwrap().0;
        mutate_json(&artifacts.join("refs.json"), |value| {
            value["refs"][0]["validator"] = serde_json::json!(digest);
        });
        mutate_json(&artifacts.join("fixture.json"), |value| {
            value["manifest_digest"] = serde_json::json!(digest);
        });
    }

    fn other_digest() -> serde_json::Value {
        serde_json::json!(format!("sha256:{}", "7".repeat(64)))
    }

    /// Apply `mutate` to a pristine copy and require `validate` to reject it with
    /// a message containing `expected`, and `check` to reject it too.
    fn assert_rejected(name: &str, expected: &str, mutate: impl FnOnce(&Path)) {
        let source = private_source();
        let candidate = copy_artifacts(&root().join("tests/fixtures/hub/generated/v1"));
        mutate(candidate.path());
        let error = validate(source.path(), candidate.path()).unwrap_err();
        assert!(error.contains(expected), "{name}: {error}");
        assert!(check(source.path(), candidate.path()).is_err(), "{name}");
    }

    fn mutate_manifest(dir: &Path, mutate: impl FnOnce(&mut serde_json::Value)) {
        mutate_json(&dir.join("manifest.json"), mutate);
    }

    #[test]
    fn manifest_summary_and_ontology_descriptors_are_each_enforced() {
        assert_rejected("summary-digest", "disagrees with manifest", |dir| {
            mutate_manifest(dir, |value| {
                value["summary"]["summary_digest"] = other_digest();
            });
            reseal_manifest_digest(dir);
        });
        assert_rejected("summary-removed", "does not advertise a summary", |dir| {
            mutate_manifest(dir, |value| {
                value.as_object_mut().unwrap().remove("summary");
            });
            reseal_manifest_digest(dir);
        });
        assert_rejected(
            "ontology-composition-digest",
            "disagrees with manifest",
            |dir| {
                mutate_manifest(dir, |value| {
                    value["ontology"]["composition_digest"] = other_digest();
                });
                reseal_manifest_digest(dir);
            },
        );
        assert_rejected("ontology-removed", "exact generated contract", |dir| {
            mutate_manifest(dir, |value| {
                value.as_object_mut().unwrap().remove("ontology");
            });
            reseal_manifest_digest(dir);
        });
        assert_rejected(
            "module-package-digest",
            "reference mismatch: PackageDigest",
            |dir| {
                mutate_manifest(dir, |value| {
                    value["ontology"]["modules"][0]["package"]["package_digest"] = other_digest();
                });
                reseal_manifest_digest(dir);
            },
        );
        // The manifest itself no longer parses, so no reseal is possible.
        assert_rejected(
            "module-package-is-project-package",
            "module package object is incompatible",
            |dir| {
                mutate_manifest(dir, |value| {
                    let project = value["package"]["object_digest"].clone();
                    value["ontology"]["modules"][0]["package"]["object_digest"] = project;
                });
            },
        );
    }

    #[test]
    fn summary_and_module_objects_and_records_are_each_enforced() {
        let pristine = root().join("tests/fixtures/hub/generated/v1");
        let module_object = fs::read_dir(pristine.join("objects"))
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .find(|name| name.starts_with("ontology-module-"))
            .unwrap();
        assert_rejected("summary-object-bytes", "summary object bytes", |dir| {
            let path = dir.join(SUMMARY_PATH);
            let mut bytes = fs::read(&path).unwrap();
            bytes[10] ^= 1;
            fs::write(path, bytes).unwrap();
        });
        assert_rejected("module-object-bytes", "module object bytes", |dir| {
            let path = dir.join("objects").join(&module_object);
            let mut bytes = fs::read(&path).unwrap();
            bytes[1024] ^= 1;
            fs::write(path, bytes).unwrap();
        });
        assert_rejected("fixture-summary-digest", "summary metadata", |dir| {
            mutate_json(&dir.join("fixture.json"), |value| {
                value["summary_digest"] = other_digest();
            });
        });
        assert_rejected("fixture-module-record", "module object records", |dir| {
            mutate_json(&dir.join("fixture.json"), |value| {
                value["module_objects"][0]["package_digest"] = other_digest();
            });
        });
    }

    #[test]
    fn summary_and_descriptors_do_not_depend_on_the_object_location_base() {
        let source = private_source();
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        generate(source.path(), first.path(), DEFAULT_LOCATION_BASE).unwrap();
        let mirror = "https://mirror.example.org/content/";
        generate(source.path(), second.path(), mirror).unwrap();

        assert_eq!(
            fs::read(first.path().join(SUMMARY_PATH)).unwrap(),
            fs::read(second.path().join(SUMMARY_PATH)).unwrap()
        );
        let manifest = |dir: &Path| -> serde_json::Value {
            serde_json::from_slice(&fs::read(dir.join("manifest.json")).unwrap()).unwrap()
        };
        let (first_manifest, second_manifest) = (manifest(first.path()), manifest(second.path()));
        for field in [
            "summary",
            "ontology",
            "package",
            "immutable_version",
            "repository",
        ] {
            assert_eq!(first_manifest[field], second_manifest[field], "{field}");
            assert!(!first_manifest[field].is_null(), "{field}");
        }
        // Only transport differs: same objects, same digests, different locations.
        let objects =
            |manifest: &serde_json::Value| -> Vec<(serde_json::Value, serde_json::Value)> {
                manifest["objects"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|object| (object["digest"].clone(), object["locations"].clone()))
                    .collect()
            };
        let (default_objects, mirror_objects) =
            (objects(&first_manifest), objects(&second_manifest));
        assert_eq!(default_objects.len(), 3);
        for ((first_digest, first_locations), (second_digest, second_locations)) in
            default_objects.iter().zip(&mirror_objects)
        {
            assert_eq!(first_digest, second_digest);
            assert_ne!(first_locations, second_locations);
            assert!(
                first_locations[0]
                    .as_str()
                    .unwrap()
                    .starts_with(DEFAULT_LOCATION_BASE)
            );
            assert_eq!(
                second_locations[0].as_str().unwrap(),
                format!(
                    "{mirror}{}",
                    first_digest
                        .as_str()
                        .unwrap()
                        .strip_prefix("sha256:")
                        .unwrap()
                )
            );
        }
        // Every non-manifest artifact is byte-identical across bases.
        for relative in artifact_files(first.path()).unwrap() {
            if relative == "manifest.json" || relative == "refs.json" || relative == "fixture.json"
            {
                continue;
            }
            assert_eq!(
                fs::read(first.path().join(&relative)).unwrap(),
                fs::read(second.path().join(&relative)).unwrap(),
                "{relative}"
            );
        }
        let metadata = |dir: &Path| -> serde_json::Value {
            serde_json::from_slice(&fs::read(dir.join("fixture.json")).unwrap()).unwrap()
        };
        let (first_metadata, second_metadata) = (metadata(first.path()), metadata(second.path()));
        for field in [
            "summary_digest",
            "summary_object_digest",
            "module_objects",
            "package_digest",
        ] {
            assert_eq!(first_metadata[field], second_metadata[field], "{field}");
        }
        // The mirror manifest is self-consistent but is not the checked-in
        // location contract.
        assert!(validate(source.path(), second.path()).is_err());
    }

    fn tree_bytes(root: &Path) -> BTreeMap<String, Vec<u8>> {
        artifact_files(root)
            .unwrap()
            .into_iter()
            .map(|relative| {
                let bytes = fs::read(root.join(&relative)).unwrap();
                (relative, bytes)
            })
            .collect()
    }

    #[test]
    fn rebuilt_source_is_deterministic_and_matches_the_checked_in_tree() {
        let workspace = tempfile::tempdir().unwrap();
        let first = workspace.path().join("first");
        let second = workspace.path().join("second");
        rebuild_source(&first).unwrap();
        rebuild_source(&second).unwrap();
        // Rebuilding over an existing tree replaces it with identical bytes.
        rebuild_source(&first).unwrap();
        let rebuilt = tree_bytes(&first);
        assert_eq!(rebuilt, tree_bytes(&second));
        assert_eq!(
            rebuilt,
            tree_bytes(&root().join("tests/fixtures/hub/openalex-source")),
            "checked-in source is not the Rust rebuild; run the generator with --rebuild-source"
        );
        assert_eq!(
            fs::read_dir(workspace.path()).unwrap().count(),
            2,
            "rebuild left staging directories behind"
        );
        verify_full(&first).unwrap();
    }
}
