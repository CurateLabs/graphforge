//! Discovery lineage binding into the storage-owned portable-v2 verifier, with
//! real research interchange packages exported through the facade.

use graphforge_api::{
    BranchSource, BuildResearchLineageRequest, CancellationToken, CreateResearchBranchRequest,
    DiscoveryPortableV2Mismatch, DiscoveryResearchVersionError, DiscoveryResearchVersionRequest,
    ExecuteResearchBranchRequest, ExportResearchRequest, GraphForge, PortableV2Limits,
    PortableV2Mode, verify_discovered_research_version,
};
use graphforge_discovery::{
    DISCOVERY_FORMAT, DiscoveryLimits, DiscoveryManifest, ObjectDescriptor, PORTABLE_V2_FORMAT,
    PortablePackageReference, ProtocolRequirement, ProtocolVersion, RefSet, RepositoryIdentity,
    RepositoryRef, ResearchLineage, ResearchLineageReference, Sha256Digest,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs;
use std::path::PathBuf;
use uuid::Uuid;

const BAGIT: &[u8] = b"BagIt-Version: 1.0\nTag-File-Character-Encoding: UTF-8\n";
const BAG_INFO: &[u8] = b"Bag-Software-Agent: GraphForge portable-v2\nBagging-Date: 1970-01-01\n";
const MANIFEST_PATH: &str = "data/graphforge-project.json";

fn hex(bytes: impl AsRef<[u8]>) -> String {
    bytes
        .as_ref()
        .iter()
        .fold(String::new(), |mut output, byte| {
            write!(output, "{byte:02x}").unwrap();
            output
        })
}

fn digest(marker: char) -> Sha256Digest {
    Sha256Digest(format!("sha256:{}", marker.to_string().repeat(64)))
}

fn package() -> (tempfile::TempDir, String) {
    let root = tempfile::tempdir().unwrap();
    let mut value: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/portable-v2/ontology-only.manifest.json"
    ))
    .unwrap();
    value.as_object_mut().unwrap().remove("package_digest");
    let semantic = serde_json::to_vec(&value).unwrap();
    let package_digest = format!(
        "sha256:{}",
        hex(Sha256::digest(
            [b"graphforge-project/2\0".as_slice(), semantic.as_slice()].concat()
        ))
    );
    value["package_digest"] = Value::String(package_digest.clone());
    let manifest = serde_json::to_vec(&value).unwrap();
    let payload_path = "data/components/ontology/core-ontology/ontology.json";
    let manifest_path = root.path().join(MANIFEST_PATH);
    fs::create_dir_all(manifest_path.parent().unwrap()).unwrap();
    fs::write(&manifest_path, &manifest).unwrap();
    let payload = root.path().join(payload_path);
    fs::create_dir_all(payload.parent().unwrap()).unwrap();
    fs::write(payload, b"{}").unwrap();
    fs::write(root.path().join("bagit.txt"), BAGIT).unwrap();
    fs::write(root.path().join("bag-info.txt"), BAG_INFO).unwrap();
    let data_manifest = format!(
        "{}  {}\n{}  {}\n",
        hex(Sha256::digest(b"{}")),
        payload_path,
        hex(Sha256::digest(&manifest)),
        MANIFEST_PATH
    );
    fs::write(root.path().join("manifest-sha256.txt"), &data_manifest).unwrap();
    let tag_manifest = format!(
        "{}  bag-info.txt\n{}  bagit.txt\n{}  manifest-sha256.txt\n",
        hex(Sha256::digest(BAG_INFO)),
        hex(Sha256::digest(BAGIT)),
        hex(Sha256::digest(data_manifest.as_bytes()))
    );
    fs::write(root.path().join("tagmanifest-sha256.txt"), tag_manifest).unwrap();
    (root, package_digest)
}

/// One Branch with an immutable base and a later head, each exported as its own
/// research package, plus the discovery documents a Hub would publish for them.
struct Published {
    _root: tempfile::TempDir,
    base: Uuid,
    head: Uuid,
    packages: BTreeMap<Uuid, (PortablePackageReference, PathBuf)>,
    lineage: ResearchLineage,
    repository: RepositoryIdentity,
}

fn generation(graph: &GraphForge) -> Uuid {
    graph
        .committed_generation_identity()
        .unwrap()
        .generation_uuid
}

fn publish() -> Published {
    let cancellation = CancellationToken::new();
    let root = tempfile::tempdir().unwrap();
    let mut graph = GraphForge::new(root.path().join("project").to_str()).unwrap();
    graph.execute("CREATE (:Item {x:0})").unwrap();
    let branch = Uuid::now_v7();
    let base = Uuid::now_v7();
    graph
        .create_research_branch(
            &CreateResearchBranchRequest {
                author: None,
                committer: None,
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: generation(&graph),
                branch_uuid: branch,
                version_uuid: base,
                source: BranchSource::Current {
                    origin_version_uuid: Uuid::now_v7(),
                    context_uuid: Uuid::now_v7(),
                },
                creator_uuid: Uuid::now_v7(),
                created_at: 1,
                label: "main".into(),
            },
            &cancellation,
        )
        .unwrap();
    let head = Uuid::now_v7();
    graph
        .execute_research_branch(
            &ExecuteResearchBranchRequest {
                author: None,
                committer: None,
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: generation(&graph),
                branch_uuid: branch,
                version_uuid: head,
                created_at: 2,
                query: "MATCH (n:Item) SET n.x=1".into(),
            },
            &cancellation,
        )
        .unwrap();
    let mut packages = BTreeMap::new();
    for (version, marker) in [(base, 'b'), (head, 'c')] {
        let output = root.path().join(format!("{version}.gfpb"));
        let exported = graph
            .export_research(
                &ExportResearchRequest {
                    version_uuid: version,
                    output: output.clone(),
                    bundled: true,
                    projection: None,
                },
                &cancellation,
            )
            .unwrap();
        packages.insert(
            version,
            (
                PortablePackageReference {
                    format: PORTABLE_V2_FORMAT.into(),
                    package_digest: Sha256Digest(exported.package_digest),
                    object_digest: digest(marker),
                },
                output,
            ),
        );
    }
    let repository = RepositoryIdentity::parse("openalex/openalex-fork").unwrap();
    let project_uuid = graph
        .research_reference(
            &graphforge_api::ResearchReferenceTarget::Version { version_uuid: head },
            &cancellation,
        )
        .unwrap()
        .project_uuid;
    let lineage = graph
        .build_research_lineage_for_discovery(
            &BuildResearchLineageRequest {
                repository: repository.clone(),
                immutable_version: digest('a'),
                project_uuid,
                branch_ref_names: BTreeMap::from([(branch, "main".to_owned())]),
                version_packages: packages
                    .iter()
                    .map(|(version, (package, _))| (*version, package.clone()))
                    .collect(),
                fork_origin_repository: None,
                published_proposals: Default::default(),
            },
            &cancellation,
        )
        .unwrap();
    Published {
        _root: root,
        base,
        head,
        packages,
        lineage,
        repository,
    }
}

/// Canonical manifest and refs for a lineage; package objects use the
/// lineage's object digests, distinct from the Project package object.
fn documents(lineage: &ResearchLineage) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
    let mut objects = vec![
        ObjectDescriptor {
            digest: digest('e'),
            length: 1,
            media_type: "application/vnd.graphforge.project".into(),
            locations: vec!["https://data.graphforge.sh/project".into()],
        },
        ObjectDescriptor {
            digest: digest('f'),
            length: 4096,
            media_type: graphforge_discovery::RESEARCH_LINEAGE_MEDIA_TYPE.into(),
            locations: vec!["https://data.graphforge.sh/lineage".into()],
        },
    ];
    for version in &lineage.versions {
        let package = version.package.as_ref().unwrap();
        if objects
            .iter()
            .all(|object| object.digest != package.object_digest)
        {
            objects.push(ObjectDescriptor {
                digest: package.object_digest.clone(),
                length: 1,
                media_type: "application/vnd.graphforge.project".into(),
                locations: vec![format!(
                    "https://data.graphforge.sh/{}",
                    package.object_digest.0.trim_start_matches("sha256:")
                )],
            });
        }
    }
    objects.sort_by(|left, right| left.digest.0.cmp(&right.digest.0));
    let manifest = DiscoveryManifest {
        format: DISCOVERY_FORMAT.into(),
        version: ProtocolVersion::CURRENT,
        repository: lineage.repository.clone(),
        default_ref: "main".into(),
        resolved_ref: "main".into(),
        immutable_version: lineage.immutable_version.clone(),
        package: PortablePackageReference {
            format: PORTABLE_V2_FORMAT.into(),
            package_digest: digest('9'),
            object_digest: digest('e'),
        },
        summary: None,
        ontology: None,
        lineage: Some(ResearchLineageReference {
            format: graphforge_discovery::RESEARCH_LINEAGE_FORMAT.into(),
            lineage_digest: lineage.canonical_digest().unwrap(),
            object_digest: digest('f'),
        }),
        requirements: vec![ProtocolRequirement {
            capability: "portable-v2".into(),
            major: 1,
        }],
        capabilities: vec![],
        objects,
        extensions: BTreeMap::default(),
    };
    let refs = RefSet {
        format: DISCOVERY_FORMAT.into(),
        version: ProtocolVersion::CURRENT,
        repository: lineage.repository.clone(),
        default_ref: "main".into(),
        refs: vec![RepositoryRef {
            name: "main".into(),
            target: lineage.immutable_version.clone(),
            validator: digest('d'),
        }],
        extensions: BTreeMap::default(),
    };
    (
        manifest.to_canonical_json().unwrap(),
        refs.to_canonical_json().unwrap(),
        lineage.to_canonical_json().unwrap(),
    )
}

fn verify(
    lineage: &ResearchLineage,
    repository: &RepositoryIdentity,
    version_uuid: Uuid,
    package: &std::path::Path,
) -> Result<graphforge_api::DiscoveredResearchVersion, DiscoveryResearchVersionError> {
    let (manifest_json, refs_json, lineage_json) = documents(lineage);
    verify_discovered_research_version(&DiscoveryResearchVersionRequest {
        manifest_json: &manifest_json,
        refs_json: &refs_json,
        lineage_json: &lineage_json,
        expected_repository: repository,
        version_uuid: &version_uuid.to_string(),
        package,
        discovery_limits: DiscoveryLimits::default(),
        portable_limits: PortableV2Limits::default(),
        mode: PortableV2Mode::Full,
        cancelled: None,
        scratch: None,
    })
}

fn version_mut(
    lineage: &mut ResearchLineage,
    version: Uuid,
) -> &mut graphforge_discovery::LineageVersion {
    lineage
        .versions
        .iter_mut()
        .find(|entry| entry.version_uuid == version.to_string())
        .unwrap()
}

#[test]
fn valid_lineage_version_maps_to_the_storage_verified_package() {
    let published = publish();
    for version in [published.head, published.base] {
        let (package, path) = &published.packages[&version];
        let (manifest_json, refs_json, lineage_json) = documents(&published.lineage);
        let accepted = verify_discovered_research_version(&DiscoveryResearchVersionRequest {
            manifest_json: &manifest_json,
            refs_json: &refs_json,
            lineage_json: &lineage_json,
            expected_repository: &published.repository,
            version_uuid: &version.to_string(),
            package: path,
            discovery_limits: DiscoveryLimits::default(),
            portable_limits: PortableV2Limits::default(),
            mode: PortableV2Mode::Full,
            cancelled: None,
            scratch: None,
        })
        .unwrap();
        assert_eq!(accepted.repository, published.repository);
        assert_eq!(accepted.version_uuid, version.to_string());
        assert_eq!(accepted.version.kind, "complete");
        assert_eq!(accepted.report.package_digest, package.package_digest.0);
    }
}

#[test]
fn research_package_digest_mismatch_fails_without_acceptance() {
    let published = publish();
    // The lineage names a different semantic digest for the head's research
    // package; the Project package reference is untouched.
    let mut lineage = published.lineage.clone();
    version_mut(&mut lineage, published.head)
        .package
        .as_mut()
        .unwrap()
        .package_digest = digest('7');
    let (_, path) = &published.packages[&published.head];
    assert!(matches!(
        verify(&lineage, &published.repository, published.head, path),
        Err(DiscoveryResearchVersionError::ReferenceMismatch(
            DiscoveryPortableV2Mismatch::PackageDigest
        ))
    ));
}

#[test]
fn version_identity_must_match_the_verified_research_registry() {
    let published = publish();
    let (_, head_path) = &published.packages[&published.head];
    // A tampered identity digest for the selected Version.
    let mut lineage = published.lineage.clone();
    version_mut(&mut lineage, published.head).identity_digest = digest('7');
    assert!(matches!(
        verify(&lineage, &published.repository, published.head, head_path),
        Err(DiscoveryResearchVersionError::ReferenceMismatch(
            DiscoveryPortableV2Mismatch::ResearchVersionIdentity
        ))
    ));
    // The base Version advertised with the head's package: the package digest
    // agrees with the lineage, but the package does not carry the base Version
    // with the base identity, so the head is never accepted as the base.
    let mut lineage = published.lineage.clone();
    let head_package = published.packages[&published.head].0.clone();
    version_mut(&mut lineage, published.base).package = Some(head_package);
    assert!(matches!(
        verify(&lineage, &published.repository, published.base, head_path),
        Err(DiscoveryResearchVersionError::ReferenceMismatch(
            DiscoveryPortableV2Mismatch::ResearchVersionIdentity
        ))
    ));
}

#[test]
fn package_without_research_interchange_never_proves_a_version() {
    let published = publish();
    let (package, package_digest) = package();
    let mut lineage = published.lineage.clone();
    version_mut(&mut lineage, published.head)
        .package
        .as_mut()
        .unwrap()
        .package_digest = Sha256Digest(package_digest);
    assert!(matches!(
        verify(
            &lineage,
            &published.repository,
            published.head,
            package.path()
        ),
        Err(DiscoveryResearchVersionError::ReferenceMismatch(
            DiscoveryPortableV2Mismatch::ResearchVersionIdentity
        ))
    ));
}

#[test]
fn repository_mismatch_fails_before_package_acceptance() {
    let published = publish();
    let other = RepositoryIdentity::parse("example/other").unwrap();
    let (_, path) = &published.packages[&published.head];
    assert!(matches!(
        verify(&published.lineage, &other, published.head, path),
        Err(DiscoveryResearchVersionError::ReferenceMismatch(
            DiscoveryPortableV2Mismatch::Repository
        ))
    ));
}
