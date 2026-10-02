//! Discovery lineage binding into the storage-owned portable-v2 verifier.

use graphforge_api::{
    DiscoveryPortableV2Mismatch, DiscoveryResearchVersionError, DiscoveryResearchVersionRequest,
    PortableV2Limits, PortableV2Mode, verify_discovered_research_version,
};
use graphforge_discovery::{
    DISCOVERY_FORMAT, DiscoveryLimits, DiscoveryManifest, ObjectDescriptor, PORTABLE_V2_FORMAT,
    PortablePackageReference, ProtocolRequirement, ProtocolVersion, RefSet, RepositoryIdentity,
    RepositoryRef, ResearchLineage, Sha256Digest,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs;

const BAGIT: &[u8] = b"BagIt-Version: 1.0\nTag-File-Character-Encoding: UTF-8\n";
const BAG_INFO: &[u8] = b"Bag-Software-Agent: GraphForge portable-v2\nBagging-Date: 1970-01-01\n";
const MANIFEST_PATH: &str = "data/graphforge-project.json";
const LINEAGE_VERSION: &str = "01900000-0000-7000-8000-000000000022";

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

fn lineage_json(package_digest: &str) -> Vec<u8> {
    let lineage = serde_json::json!({
        "format":"graphforge-research-lineage/1","version":{"major":1,"minor":1},
        "repository":{"owner":"openalex","repository":"openalex-fork"},
        "immutable_version":digest('a').0,
        "project_uuid":"01900000-0000-7000-8000-000000000010",
        "requirements":[{"capability":"research-lineage","major":1}],"capabilities":[],
        "branches":[{"branch_uuid":"01900000-0000-7000-8000-000000000020","ref_name":"main","project_uuid":"01900000-0000-7000-8000-000000000010","head_version_uuid":LINEAGE_VERSION,"parent_branch_uuid":null,"origin_version_uuid":LINEAGE_VERSION,"base_version_uuid":LINEAGE_VERSION,"selection_sha256":digest('1').0,"label":"main"}],
        "versions":[{"version_uuid":LINEAGE_VERSION,"identity_digest":digest('2').0,"kind":"complete","branch_uuid":"01900000-0000-7000-8000-000000000020","source_version_uuid":null,"package":{"format":"graphforge-project/2","package_digest":package_digest,"object_digest":digest('b').0}}],
        "proposals":[]
    });
    serde_json::to_vec(&lineage).unwrap()
}

fn discovery(
    package_digest: String,
) -> (
    DiscoveryManifest,
    RefSet,
    ResearchLineage,
    RepositoryIdentity,
) {
    let repository = RepositoryIdentity::parse("openalex/openalex-fork").unwrap();
    let lineage =
        ResearchLineage::from_json(&lineage_json(&package_digest), DiscoveryLimits::default())
            .unwrap();
    let lineage_digest = lineage.canonical_digest().unwrap();
    let manifest = DiscoveryManifest {
        format: DISCOVERY_FORMAT.into(),
        version: ProtocolVersion::CURRENT,
        repository: repository.clone(),
        default_ref: "main".into(),
        resolved_ref: "main".into(),
        immutable_version: digest('a'),
        package: PortablePackageReference {
            format: PORTABLE_V2_FORMAT.into(),
            package_digest: digest('f'),
            object_digest: digest('c'),
        },
        summary: None,
        ontology: None,
        lineage: Some(graphforge_discovery::ResearchLineageReference {
            format: graphforge_discovery::RESEARCH_LINEAGE_FORMAT.into(),
            lineage_digest,
            object_digest: digest('d'),
        }),
        requirements: vec![ProtocolRequirement {
            capability: "portable-v2".into(),
            major: 1,
        }],
        capabilities: vec![],
        objects: vec![
            ObjectDescriptor {
                digest: digest('b'),
                length: 1,
                media_type: "application/vnd.graphforge.project".into(),
                locations: vec!["https://data.graphforge.sh/version".into()],
            },
            ObjectDescriptor {
                digest: digest('c'),
                length: 1,
                media_type: "application/vnd.graphforge.project".into(),
                locations: vec!["https://data.graphforge.sh/project".into()],
            },
            ObjectDescriptor {
                digest: digest('d'),
                length: 512,
                media_type: graphforge_discovery::RESEARCH_LINEAGE_MEDIA_TYPE.into(),
                locations: vec!["https://data.graphforge.sh/lineage".into()],
            },
        ],
        extensions: BTreeMap::default(),
    };
    let refs = RefSet {
        format: DISCOVERY_FORMAT.into(),
        version: ProtocolVersion::CURRENT,
        repository: repository.clone(),
        default_ref: "main".into(),
        refs: vec![RepositoryRef {
            name: "main".into(),
            target: digest('a'),
            validator: digest('d'),
        }],
        extensions: BTreeMap::default(),
    };
    (manifest, refs, lineage, repository)
}

#[test]
fn valid_lineage_version_maps_to_the_storage_verified_package() {
    let (package, package_digest) = package();
    let (manifest, refs, lineage, repository) = discovery(package_digest.clone());
    let manifest_json = serde_json::to_vec(&manifest).unwrap();
    let refs_json = serde_json::to_vec(&refs).unwrap();
    let lineage_json = serde_json::to_vec(&lineage).unwrap();
    let accepted = verify_discovered_research_version(&DiscoveryResearchVersionRequest {
        manifest_json: &manifest_json,
        refs_json: &refs_json,
        lineage_json: &lineage_json,
        expected_repository: &repository,
        version_uuid: LINEAGE_VERSION,
        package: package.path(),
        discovery_limits: DiscoveryLimits::default(),
        portable_limits: PortableV2Limits::default(),
        mode: PortableV2Mode::Full,
        cancelled: None,
    })
    .unwrap();
    assert_eq!(accepted.repository, repository);
    assert_eq!(accepted.version_uuid, LINEAGE_VERSION);
    assert_eq!(accepted.version.kind, "complete");
    assert_eq!(accepted.report.package_digest, package_digest);
}

#[test]
fn package_digest_mismatch_fails_without_acceptance() {
    let (package, package_digest) = package();
    let (mut manifest, refs, lineage, repository) = discovery(package_digest);
    manifest.objects[1].digest = digest('e');
    let manifest_json = serde_json::to_vec(&manifest).unwrap();
    let refs_json = serde_json::to_vec(&refs).unwrap();
    let lineage_json = serde_json::to_vec(&lineage).unwrap();
    assert!(matches!(
        verify_discovered_research_version(&DiscoveryResearchVersionRequest {
            manifest_json: &manifest_json,
            refs_json: &refs_json,
            lineage_json: &lineage_json,
            expected_repository: &repository,
            version_uuid: LINEAGE_VERSION,
            package: package.path(),
            discovery_limits: DiscoveryLimits::default(),
            portable_limits: PortableV2Limits::default(),
            mode: PortableV2Mode::Full,
            cancelled: None,
        }),
        Err(DiscoveryResearchVersionError::Discovery(_))
    ));
}

#[test]
fn repository_mismatch_fails_before_package_acceptance() {
    let (package, package_digest) = package();
    let (manifest, refs, lineage, _repository) = discovery(package_digest);
    let other = RepositoryIdentity::parse("example/other").unwrap();
    let manifest_json = serde_json::to_vec(&manifest).unwrap();
    let refs_json = serde_json::to_vec(&refs).unwrap();
    let lineage_json = serde_json::to_vec(&lineage).unwrap();
    assert!(matches!(
        verify_discovered_research_version(&DiscoveryResearchVersionRequest {
            manifest_json: &manifest_json,
            refs_json: &refs_json,
            lineage_json: &lineage_json,
            expected_repository: &other,
            version_uuid: LINEAGE_VERSION,
            package: package.path(),
            discovery_limits: DiscoveryLimits::default(),
            portable_limits: PortableV2Limits::default(),
            mode: PortableV2Mode::Full,
            cancelled: None,
        }),
        Err(DiscoveryResearchVersionError::ReferenceMismatch(
            DiscoveryPortableV2Mismatch::Repository
        ))
    ));
}
