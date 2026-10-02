//! Client-side derivation of Hub discovery documents for one repository snapshot.
//!
//! Both the checked-in Hub fixture generator and `gf publish` derive the
//! Project summary, the per-module ontology packages, and the discovery
//! manifest here, from verified portable packages only. A Hub stores what this
//! module derives; it never re-derives semantics.

use graphforge_api::{
    GraphForge, PortableSelection, PortableV2ExactIdentity, PortableV2ExportRequest,
    PortableV2Limits, PortableV2Mode, PortableV2Output, PortableV2SelectionProfile,
    PortableVerifyRequest, PortableVerifyResult, ProjectSummaryRequest,
    summarize_verified_portable_v2, verify_portable_v2,
};
use graphforge_discovery::{
    DISCOVERY_FORMAT, DiscoveryLimits, DiscoveryManifest, ObjectDescriptor, OntologyInventory,
    OntologyModuleDescriptor, PORTABLE_V2_FORMAT, PORTABLE_V2_MEDIA_TYPE, PROJECT_SUMMARY_FORMAT,
    PROJECT_SUMMARY_MEDIA_TYPE, PortablePackageReference, ProjectSummary, ProjectSummaryReference,
    ProtocolRequirement, ProtocolVersion, RepositoryIdentity, ResearchLineageReference,
    Sha256Digest,
};
use sha2::{Digest as _, Sha256};
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::io::Read as _;
use std::path::{Path, PathBuf};

/// Error text from any derivation step.
pub type DerivationError = String;

fn err(error: impl std::fmt::Display) -> DerivationError {
    error.to_string()
}

/// Lowercase `sha256:` digest of `bytes`.
#[must_use]
pub fn digest_bytes(bytes: &[u8]) -> String {
    format!("sha256:{}", hex(&Sha256::digest(bytes)))
}

/// Streamed `sha256:` digest and length of the file at `path`.
pub fn digest_file(path: &Path) -> Result<(String, u64), DerivationError> {
    let mut file = std::fs::File::open(path).map_err(err)?;
    let mut hash = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();
    let mut length = 0_u64;
    loop {
        let read = file.read(&mut buffer).map_err(err)?;
        if read == 0 {
            break;
        }
        hash.update(&buffer[..read]);
        length += read as u64;
    }
    Ok((format!("sha256:{}", hex(&hash.finalize())), length))
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

/// Fully verify the portable package at `input`.
pub fn verify_full(input: &Path) -> Result<PortableVerifyResult, DerivationError> {
    verify_portable_v2(
        &PortableVerifyRequest {
            input: input.to_path_buf(),
            mode: PortableV2Mode::Full,
            limits: PortableV2Limits::default(),
        },
        None,
    )
    .map_err(err)
}

/// Descriptor for `digest`/`length` at the single `location`.
#[must_use]
pub fn object_descriptor(
    digest: String,
    length: u64,
    media_type: &str,
    location: String,
) -> ObjectDescriptor {
    ObjectDescriptor {
        locations: vec![location],
        digest: Sha256Digest(digest),
        length,
        media_type: media_type.into(),
    }
}

/// One verified portable bundle on disk.
#[derive(Clone, Debug)]
pub struct VerifiedBundle {
    /// Bundle file.
    pub path: PathBuf,
    /// Semantic package identity.
    pub package_digest: String,
    /// Transport object digest: SHA-256 of the bundle bytes.
    pub object_digest: String,
    /// Bundle length in bytes.
    pub length: u64,
}

impl VerifiedBundle {
    /// Discovery package reference for this bundle.
    #[must_use]
    pub fn package_reference(&self) -> PortablePackageReference {
        PortablePackageReference {
            format: PORTABLE_V2_FORMAT.into(),
            package_digest: Sha256Digest(self.package_digest.clone()),
            object_digest: Sha256Digest(self.object_digest.clone()),
        }
    }
}

/// Bind an export receipt to the bytes it wrote: full verification must report
/// the same package and transport identity, and the transport identity must be
/// the SHA-256 of the file.
pub fn verify_exported_bundle(
    path: &Path,
    package_digest: &str,
    transport_digest: &str,
) -> Result<VerifiedBundle, DerivationError> {
    let report = verify_full(path)?;
    let (object_digest, length) = digest_file(path)?;
    if report.package_digest != package_digest
        || report.transport_digest.as_deref() != Some(transport_digest)
        || object_digest != transport_digest
    {
        return Err("portable export and verification receipts disagree".into());
    }
    Ok(VerifiedBundle {
        path: path.to_path_buf(),
        package_digest: package_digest.to_owned(),
        object_digest,
        length,
    })
}

/// Export the current generation of `graph` with `profile` as one verified bundle.
pub fn export_verified_bundle(
    graph: &GraphForge,
    profile: PortableV2SelectionProfile,
    path: &Path,
) -> Result<VerifiedBundle, DerivationError> {
    let receipt = graph
        .export_portable_v2(
            &PortableV2ExportRequest {
                selection: PortableSelection::Current,
                output_path: path.to_path_buf(),
                representation: PortableV2Output::Bundle,
                profile,
                subset: None,
                limits: PortableV2Limits::default(),
            },
            None,
            |_| {},
        )
        .map_err(err)?;
    verify_exported_bundle(path, &receipt.package_digest, &receipt.transport_digest)
}

/// The Project summary derived from a verified Project package.
pub struct DerivedSummary {
    /// Validated summary document.
    pub summary: ProjectSummary,
    /// Canonical summary bytes, the transport object.
    pub bytes: Vec<u8>,
    /// Canonical summary digest.
    pub summary_digest: Sha256Digest,
}

/// Derive the Project summary from the verified Project package at `package`.
pub fn derive_summary(
    repository: &RepositoryIdentity,
    immutable_version: &Sha256Digest,
    package: &Path,
) -> Result<DerivedSummary, DerivationError> {
    let summary = summarize_verified_portable_v2(&ProjectSummaryRequest {
        repository,
        immutable_version,
        package,
        discovery_limits: DiscoveryLimits::default(),
        portable_limits: PortableV2Limits::default(),
        cancelled: None,
    })
    .map_err(err)?;
    let bytes = summary.to_canonical_json().map_err(err)?;
    let summary_digest = summary.canonical_digest().map_err(err)?;
    Ok(DerivedSummary {
        summary,
        bytes,
        summary_digest,
    })
}

/// One exported per-module ontology package.
pub struct ModulePackage {
    /// Manifest descriptor naming the module and its package.
    pub descriptor: OntologyModuleDescriptor,
    /// Verified module bundle.
    pub bundle: VerifiedBundle,
}

/// Export one component-selective package per module the summary advertises.
///
/// `module_path` names the bundle file for a module content digest. Returns the
/// manifest ontology inventory (absent without a composition) and the packages
/// in summary order.
pub fn export_module_packages(
    graph: &GraphForge,
    summary: &ProjectSummary,
    module_path: impl Fn(&str) -> Result<PathBuf, DerivationError>,
) -> Result<(Option<OntologyInventory>, Vec<ModulePackage>), DerivationError> {
    let composition = summary.facts.ontology_composition.as_ref();
    let mut packages = Vec::new();
    for module in composition.map_or(&[][..], |composition| composition.modules.as_slice()) {
        let bundle = export_verified_bundle(
            graph,
            PortableV2SelectionProfile::OntologyComposition(vec![PortableV2ExactIdentity {
                id: module.id.clone(),
                version: module.version.clone(),
                content_digest: module.content_digest.0.clone(),
            }]),
            &module_path(&module.content_digest.0)?,
        )?;
        packages.push(ModulePackage {
            descriptor: OntologyModuleDescriptor {
                id: module.id.clone(),
                version: module.version.clone(),
                content_digest: module.content_digest.clone(),
                package: Some(bundle.package_reference()),
            },
            bundle,
        });
    }
    let inventory = composition.map(|composition| OntologyInventory {
        composition_digest: composition.composition_digest.clone(),
        modules: packages
            .iter()
            .map(|package| package.descriptor.clone())
            .collect(),
        bridge_sets: composition.bridge_sets.clone(),
    });
    Ok((inventory, packages))
}

/// Everything one discovery manifest binds.
pub struct ManifestInputs {
    /// Repository identity.
    pub repository: RepositoryIdentity,
    /// Repository default ref.
    pub default_ref: String,
    /// Ref this manifest is published for.
    pub resolved_ref: String,
    /// Project package; its package digest is the immutable version.
    pub package: PortablePackageReference,
    /// Project summary reference.
    pub summary: Option<ProjectSummaryReference>,
    /// Ontology module inventory.
    pub ontology: Option<OntologyInventory>,
    /// Research lineage reference.
    pub lineage: Option<ResearchLineageReference>,
    /// Every transport object; sorted and deduplicated here.
    pub objects: Vec<ObjectDescriptor>,
}

/// Summary reference for a derived summary at transport `object`.
#[must_use]
pub fn summary_reference(
    summary: &DerivedSummary,
    object: &ObjectDescriptor,
) -> ProjectSummaryReference {
    ProjectSummaryReference {
        format: PROJECT_SUMMARY_FORMAT.into(),
        summary_digest: summary.summary_digest.clone(),
        object_digest: object.digest.clone(),
    }
}

/// Build and validate a discovery manifest. Objects are ordered by digest and
/// a digest listed twice must describe the same object.
pub fn build_manifest(inputs: ManifestInputs) -> Result<DiscoveryManifest, DerivationError> {
    let mut objects: BTreeMap<String, ObjectDescriptor> = BTreeMap::new();
    for object in inputs.objects {
        match objects.get(&object.digest.0) {
            Some(existing) if *existing != object => {
                return Err("one object digest is listed with two descriptors".into());
            }
            Some(_) => {}
            None => {
                objects.insert(object.digest.0.clone(), object);
            }
        }
    }
    let manifest = DiscoveryManifest {
        format: DISCOVERY_FORMAT.into(),
        version: ProtocolVersion::CURRENT,
        repository: inputs.repository,
        default_ref: inputs.default_ref,
        resolved_ref: inputs.resolved_ref,
        immutable_version: inputs.package.package_digest.clone(),
        package: inputs.package,
        summary: inputs.summary,
        ontology: inputs.ontology,
        lineage: inputs.lineage,
        requirements: vec![ProtocolRequirement {
            capability: "portable-v2".into(),
            major: 1,
        }],
        capabilities: vec![],
        objects: objects.into_values().collect(),
        extensions: BTreeMap::new(),
    };
    manifest.validate(DiscoveryLimits::default()).map_err(err)?;
    Ok(manifest)
}

/// Media type of a Project or module package object.
pub const PACKAGE_MEDIA_TYPE: &str = PORTABLE_V2_MEDIA_TYPE;
/// Media type of a Project summary object.
pub const SUMMARY_MEDIA_TYPE: &str = PROJECT_SUMMARY_MEDIA_TYPE;
