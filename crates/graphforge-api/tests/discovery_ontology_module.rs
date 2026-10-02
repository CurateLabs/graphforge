//! Exact ontology module resolution from discovered per-module packages, over
//! real durable Projects and real exports.

use graphforge_api::{
    DiscoveryOntologyModuleRequest, DiscoveryPortableV2Error, DiscoveryPortableV2Mismatch,
    GraphForge, ModuleAdoptionRequest, OntologyAuthorityExpectation, OntologyModuleId,
    PortableSelection, PortableV2ErrorCode, PortableV2ExportRequest, PortableV2Limits,
    PortableV2Output, PortableV2SelectionProfile, ResolvedOntologyModule, WriteContext,
    resolve_discovered_ontology_module,
};
use graphforge_core::portable::PortableV2ExactIdentity;
use graphforge_discovery::{
    DISCOVERY_FORMAT, DiscoveryErrorCode, DiscoveryLimits, DiscoveryManifest, ExactIdentity,
    ObjectDescriptor, OntologyInventory, OntologyModuleDescriptor, PORTABLE_V2_FORMAT,
    PORTABLE_V2_MEDIA_TYPE, PortablePackageReference, ProtocolRequirement, ProtocolVersion, RefSet,
    RepositoryIdentity, RepositoryRef, Sha256Digest,
};
use graphforge_ontology::OntologyDoc;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use uuid::Uuid;

const MODULE_ID: &str = "https://example.org/ontology/shared-module";
const MODULE_VERSION: &str = "2026.1";

fn hex(bytes: impl AsRef<[u8]>) -> String {
    bytes
        .as_ref()
        .iter()
        .fold(String::new(), |mut output, byte| {
            write!(output, "{byte:02x}").unwrap();
            output
        })
}

fn sha256(bytes: &[u8]) -> Sha256Digest {
    Sha256Digest(format!("sha256:{}", hex(Sha256::digest(bytes))))
}

fn marker(character: char) -> Sha256Digest {
    Sha256Digest(format!("sha256:{}", character.to_string().repeat(64)))
}

fn module_document() -> OntologyDoc {
    serde_json::from_value(serde_json::json!({
        "ontology_id": MODULE_ID,
        "version": MODULE_VERSION,
        "entity_types": [
            {"name": "Person", "abstract": false, "parent": null},
            {"name": "Place", "abstract": false, "parent": null}
        ],
        "relation_types": [],
        "properties": [],
        "constraints": [],
        "migrations": []
    }))
    .unwrap()
}

/// Create a real durable Project that adopts `document` as an exact module.
///
/// `operation` seeds the write operation identities. Different seeds give
/// different committed generations, exactly like two independent publishers.
fn project_adopting(
    root: &Path,
    document: &OntologyDoc,
    operation: u128,
) -> (GraphForge, OntologyModuleId) {
    fs::create_dir(root).expect("Project directory");
    let mut graph = GraphForge::new(root.to_str()).expect("durable Project");
    let candidate = graph
        .create_ontology_module(document.clone(), Vec::new(), None)
        .expect("validate module");
    let state = graph.ontology_authority_state().expect("authority state");
    graph
        .adopt_ontology_module(
            &ModuleAdoptionRequest {
                authority: OntologyAuthorityExpectation {
                    context: WriteContext {
                        operation_uuid: graphforge_api::OperationId(Uuid::from_u128(operation)),
                        actor_uuid: Some(Uuid::from_u128(operation + 1)),
                    },
                    expected_project_generation_uuid: state.project_generation_uuid,
                    expected_composition_fingerprint: state.composition_fingerprint,
                },
                candidate: candidate.clone(),
            },
            None,
        )
        .expect("adopt module");
    (graph, candidate.id)
}

/// Export the exact-module package of `exact` from `graph`.
fn export_module_package(
    graph: &GraphForge,
    exact: &OntologyModuleId,
    output_path: PathBuf,
    representation: PortableV2Output,
) -> graphforge_api::PortableV2ExportFacadeResult {
    graph
        .export_portable_v2(
            &PortableV2ExportRequest {
                selection: PortableSelection::Current,
                output_path,
                representation,
                profile: PortableV2SelectionProfile::OntologyComposition(vec![
                    PortableV2ExactIdentity {
                        id: exact.ontology_id.clone(),
                        version: exact.authored_version.clone(),
                        content_digest: format!("sha256:{}", exact.canonical_digest),
                    },
                ]),
                subset: None,
                limits: PortableV2Limits::default(),
            },
            None,
            |_| {},
        )
        .expect("export exact module package")
}

fn identity(exact: &OntologyModuleId) -> ExactIdentity {
    ExactIdentity {
        id: exact.ontology_id.clone(),
        version: exact.authored_version.clone(),
        content_digest: Sha256Digest(format!("sha256:{}", exact.canonical_digest)),
    }
}

struct Discovery {
    manifest_json: Vec<u8>,
    refs_json: Vec<u8>,
    repository: RepositoryIdentity,
}

/// Build discovery documents for a Project whose per-module package is the
/// bundle at `module_package`. The Project package itself is never read by
/// module resolution, so its identity is a stand-in.
fn discovery(
    exact: &ExactIdentity,
    module_package: &Path,
    advertised_package_digest: &str,
    module_object_digest_override: Option<Sha256Digest>,
) -> Discovery {
    let repository = RepositoryIdentity::parse("example/shared-module").unwrap();
    let immutable_version = marker('a');
    let project_object = marker('c');
    let package_bytes = fs::read(module_package).unwrap();
    let module_object_digest = sha256(&package_bytes);
    let package_object =
        module_object_digest_override.unwrap_or_else(|| module_object_digest.clone());
    let manifest = DiscoveryManifest {
        format: DISCOVERY_FORMAT.into(),
        version: ProtocolVersion::CURRENT,
        repository: repository.clone(),
        default_ref: "main".into(),
        resolved_ref: "main".into(),
        immutable_version: immutable_version.clone(),
        package: PortablePackageReference {
            format: PORTABLE_V2_FORMAT.into(),
            package_digest: marker('1'),
            object_digest: project_object.clone(),
        },
        summary: None,
        ontology: Some(OntologyInventory {
            composition_digest: marker('2'),
            modules: vec![OntologyModuleDescriptor {
                id: exact.id.clone(),
                version: exact.version.clone(),
                content_digest: exact.content_digest.clone(),
                package: Some(PortablePackageReference {
                    format: PORTABLE_V2_FORMAT.into(),
                    package_digest: Sha256Digest(advertised_package_digest.into()),
                    object_digest: package_object.clone(),
                }),
            }],
            bridge_sets: vec![],
        }),
        requirements: vec![ProtocolRequirement {
            capability: "portable-v2".into(),
            major: 1,
        }],
        capabilities: vec![],
        objects: {
            let mut objects = vec![ObjectDescriptor {
                digest: project_object,
                length: 1,
                media_type: PORTABLE_V2_MEDIA_TYPE.into(),
                locations: vec!["https://data.example.org/project".into()],
            }];
            if package_object != marker('c') {
                objects.push(ObjectDescriptor {
                    digest: package_object,
                    length: package_bytes.len() as u64,
                    media_type: PORTABLE_V2_MEDIA_TYPE.into(),
                    locations: vec!["https://data.example.org/module".into()],
                });
            }
            objects.sort_by(|left, right| left.digest.0.cmp(&right.digest.0));
            objects
        },
        extensions: BTreeMap::default(),
    };
    let refs = RefSet {
        format: DISCOVERY_FORMAT.into(),
        version: ProtocolVersion::CURRENT,
        repository: repository.clone(),
        default_ref: "main".into(),
        refs: vec![RepositoryRef {
            name: "main".into(),
            target: immutable_version,
            validator: marker('d'),
        }],
        extensions: BTreeMap::default(),
    };
    Discovery {
        manifest_json: serde_json::to_vec(&manifest).unwrap(),
        refs_json: serde_json::to_vec(&refs).unwrap(),
        repository,
    }
}

fn resolve(
    discovery: &Discovery,
    exact: &ExactIdentity,
    package: &Path,
) -> Result<ResolvedOntologyModule, DiscoveryPortableV2Error> {
    resolve_discovered_ontology_module(&DiscoveryOntologyModuleRequest {
        manifest_json: &discovery.manifest_json,
        refs_json: &discovery.refs_json,
        expected_repository: &discovery.repository,
        module: exact,
        package,
        discovery_limits: DiscoveryLimits::default(),
        portable_limits: PortableV2Limits::default(),
        cancelled: None,
    })
}

struct Published {
    _roots: tempfile::TempDir,
    exact: OntologyModuleId,
    bundle: PathBuf,
    package_digest: String,
}

fn publish(operation: u128, document: &OntologyDoc) -> Published {
    let roots = tempfile::tempdir().unwrap();
    let (graph, exact) = project_adopting(&roots.path().join("project"), document, operation);
    let bundle = roots.path().join("module.gfpb");
    let exported = export_module_package(&graph, &exact, bundle.clone(), PortableV2Output::Bundle);
    Published {
        exact,
        bundle,
        package_digest: exported.package_digest,
        _roots: roots,
    }
}

#[test]
fn two_projects_adopting_the_same_exact_module_resolve_to_the_same_module() {
    let document = module_document();
    let first = publish(8_100, &document);
    let second = publish(8_200, &document);
    assert_eq!(first.exact, second.exact);
    let exact = identity(&first.exact);

    let resolved: Vec<_> = [&first, &second]
        .into_iter()
        .map(|published| {
            let documents = discovery(&exact, &published.bundle, &published.package_digest, None);
            let resolved = resolve(&documents, &exact, &published.bundle).expect("resolve module");
            assert_eq!(resolved.package_digest, published.package_digest);
            resolved
        })
        .collect();

    assert_eq!(resolved[0].module, resolved[1].module);
    assert_eq!(resolved[0].module, first.exact);
    assert_eq!(resolved[0].document, resolved[1].document);
    assert_eq!(resolved[0].module_sha256, resolved[1].module_sha256);
    assert_eq!(
        resolved[0].module_sha256,
        hex(Sha256::digest(&resolved[0].document))
    );
    let parsed: OntologyDoc = serde_json::from_slice(&resolved[0].document).unwrap();
    assert_eq!(parsed.ontology_id, MODULE_ID);
    assert_eq!(parsed.version, MODULE_VERSION);

    // Package identity is deliberately NOT module identity: each package binds
    // the `source_generation` of the Project that exported it, and the two
    // Projects committed different generations, so the semantic package digests
    // differ even though the module they carry is byte-for-byte the same.
    assert_ne!(first.package_digest, second.package_digest);
    assert_ne!(resolved[0].package_digest, resolved[1].package_digest);
}

#[test]
fn expanded_and_bundle_representations_resolve_identically() {
    let roots = tempfile::tempdir().unwrap();
    let (graph, exact_id) =
        project_adopting(&roots.path().join("project"), &module_document(), 8_300);
    let bundle = roots.path().join("module.gfpb");
    let expanded = roots.path().join("module.gfproject");
    let bundled =
        export_module_package(&graph, &exact_id, bundle.clone(), PortableV2Output::Bundle);
    let directory = export_module_package(
        &graph,
        &exact_id,
        expanded.clone(),
        PortableV2Output::Expanded,
    );
    assert_eq!(bundled.package_digest, directory.package_digest);
    let exact = identity(&exact_id);
    let documents = discovery(&exact, &bundle, &bundled.package_digest, None);
    let from_bundle = resolve(&documents, &exact, &bundle).unwrap();
    let from_directory = resolve(&documents, &exact, &expanded).unwrap();
    assert_eq!(from_bundle.document, from_directory.document);
    assert_eq!(from_bundle.module_sha256, from_directory.module_sha256);
    assert_eq!(from_bundle.package_digest, from_directory.package_digest);
}

#[test]
fn tampered_module_byte_fails_portable_verification() {
    let published = publish(8_400, &module_document());
    let exact = identity(&published.exact);
    let documents = discovery(&exact, &published.bundle, &published.package_digest, None);
    let authentic = resolve(&documents, &exact, &published.bundle)
        .unwrap()
        .document;

    let mut package = fs::read(&published.bundle).unwrap();
    let offset = package
        .windows(authentic.len())
        .position(|window| window == &authentic[..])
        .expect("bundle carries the module document bytes");
    package[offset + authentic.len() / 2] ^= 1;
    let tampered = published.bundle.with_file_name("tampered.gfpb");
    fs::write(&tampered, package).unwrap();

    let error = resolve(&documents, &exact, &tampered).unwrap_err();
    assert!(
        matches!(
            &error,
            DiscoveryPortableV2Error::Portable(portable)
                if portable.code == PortableV2ErrorCode::DigestMismatch
        ),
        "{error:?}"
    );
}

#[test]
fn absent_identity_is_missing_object_before_any_package_io() {
    let published = publish(8_500, &module_document());
    let exact = identity(&published.exact);
    let documents = discovery(&exact, &published.bundle, &published.package_digest, None);
    let mut absent = exact.clone();
    absent.content_digest = marker('9');
    let never_created = published.bundle.with_file_name("does-not-exist.gfpb");
    let error = resolve(&documents, &absent, &never_created).unwrap_err();
    assert!(
        matches!(
            &error,
            DiscoveryPortableV2Error::Discovery(discovery)
                if discovery.code == DiscoveryErrorCode::MissingObject
        ),
        "{error:?}"
    );
    // The same call against the same missing path succeeds at discovery and
    // then fails in the package verifier, proving the error above preceded I/O.
    let error = resolve(&documents, &exact, &never_created).unwrap_err();
    assert!(
        matches!(&error, DiscoveryPortableV2Error::Portable(_)),
        "{error:?}"
    );
}

#[test]
fn descriptor_pointing_at_the_project_package_is_rejected_before_any_package_io() {
    let published = publish(8_600, &module_document());
    let exact = identity(&published.exact);
    // The module descriptor names the Project package object.
    let documents = discovery(
        &exact,
        &published.bundle,
        &published.package_digest,
        Some(marker('c')),
    );
    let never_created = published.bundle.with_file_name("does-not-exist.gfpb");
    let error = resolve(&documents, &exact, &never_created).unwrap_err();
    assert!(
        matches!(
            &error,
            DiscoveryPortableV2Error::Discovery(discovery)
                if discovery.code == DiscoveryErrorCode::MalformedResponse
        ),
        "{error:?}"
    );
}

#[test]
fn package_digest_and_module_identity_must_match_the_advertisement() {
    let document = module_document();
    let published = publish(8_700, &document);
    let exact = identity(&published.exact);

    // A package whose digest differs from the advertised one.
    let wrong_package = discovery(&exact, &published.bundle, &marker('7').0, None);
    assert!(matches!(
        resolve(&wrong_package, &exact, &published.bundle),
        Err(DiscoveryPortableV2Error::ReferenceMismatch(
            DiscoveryPortableV2Mismatch::PackageDigest
        ))
    ));

    // A valid package that does not carry the advertised exact module.
    let mut other_document = document;
    other_document.version = "2026.2".into();
    let other = publish(8_800, &other_document);
    let advertised = identity(&other.exact);
    let swapped = discovery(
        &advertised,
        &published.bundle,
        &published.package_digest,
        None,
    );
    assert!(matches!(
        resolve(&swapped, &advertised, &published.bundle),
        Err(DiscoveryPortableV2Error::ReferenceMismatch(
            DiscoveryPortableV2Mismatch::ModuleIdentity
        ))
    ));
}
