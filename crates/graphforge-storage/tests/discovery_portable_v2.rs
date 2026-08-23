//! End-to-end discovery selection into the storage-owned portable-v2 verifier.

use graphforge_discovery::{
    DISCOVERY_FORMAT, DiscoveryLimits, DiscoveryManifest, ObjectDescriptor, PORTABLE_V2_FORMAT,
    PortablePackageReference, ProtocolRequirement, ProtocolVersion, RefSet, RepositoryIdentity,
    RepositoryRef, Sha256Digest,
};
use graphforge_storage::{
    DiscoveryPortableV2Error, DiscoveryPortableV2Mismatch, DiscoveryPortableV2Request,
    PortableV2ErrorCode, PortableV2Limits, PortableV2Mode, verify_discovered_portable_v2,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs;

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

fn digest(marker: char) -> Sha256Digest {
    Sha256Digest(format!("sha256:{}", marker.to_string().repeat(64)))
}

fn discovery(package_digest: String) -> (DiscoveryManifest, RefSet, RepositoryIdentity) {
    let repository = RepositoryIdentity::parse("openalex/openalex").unwrap();
    let immutable_version = digest('a');
    let manifest = DiscoveryManifest {
        format: DISCOVERY_FORMAT.into(),
        version: ProtocolVersion::CURRENT,
        repository: repository.clone(),
        default_ref: "main".into(),
        resolved_ref: "main".into(),
        immutable_version: immutable_version.clone(),
        package: PortablePackageReference {
            format: PORTABLE_V2_FORMAT.into(),
            package_digest: Sha256Digest(package_digest),
            object_digest: digest('c'),
        },
        requirements: vec![ProtocolRequirement {
            capability: "portable-v2".into(),
            major: 1,
        }],
        capabilities: vec![],
        objects: vec![ObjectDescriptor {
            digest: digest('c'),
            length: 1,
            media_type: "application/vnd.graphforge.project".into(),
            locations: vec!["https://data.graphforge.sh/object".into()],
        }],
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
            validator: digest('d'),
        }],
        extensions: BTreeMap::default(),
    };
    (manifest, refs, repository)
}

fn verify(
    manifest: &DiscoveryManifest,
    refs: &RefSet,
    expected: &RepositoryIdentity,
    package: &tempfile::TempDir,
) -> Result<graphforge_storage::DiscoveredPortableV2, DiscoveryPortableV2Error> {
    let manifest_json = serde_json::to_vec(manifest).unwrap();
    let refs_json = serde_json::to_vec(refs).unwrap();
    verify_discovered_portable_v2(&DiscoveryPortableV2Request {
        manifest_json: &manifest_json,
        refs_json: &refs_json,
        expected_repository: expected,
        package: package.path(),
        discovery_limits: DiscoveryLimits::default(),
        portable_limits: PortableV2Limits::default(),
        mode: PortableV2Mode::Full,
        cancelled: None,
    })
}

#[test]
fn valid_discovery_maps_to_the_storage_verified_package() {
    let (package, package_digest) = package();
    let (manifest, refs, repository) = discovery(package_digest.clone());
    let accepted = verify(&manifest, &refs, &repository, &package).unwrap();
    assert_eq!(accepted.repository, repository);
    assert_eq!(accepted.resolved_ref, "main");
    assert_eq!(accepted.immutable_version, digest('a').0);
    assert_eq!(accepted.report.package_digest, package_digest);
}

#[test]
fn repository_package_and_version_mismatches_fail_without_acceptance() {
    let (package, package_digest) = package();
    let (manifest, refs, repository) = discovery(package_digest);
    let other = RepositoryIdentity::parse("example/other").unwrap();
    assert!(matches!(
        verify(&manifest, &refs, &other, &package),
        Err(DiscoveryPortableV2Error::ReferenceMismatch(
            DiscoveryPortableV2Mismatch::Repository
        ))
    ));

    let mut wrong_package = manifest.clone();
    wrong_package.package.package_digest = digest('f');
    assert!(matches!(
        verify(&wrong_package, &refs, &repository, &package),
        Err(DiscoveryPortableV2Error::ReferenceMismatch(
            DiscoveryPortableV2Mismatch::PackageDigest
        ))
    ));

    let mut wrong_version = refs.clone();
    wrong_version.refs[0].target = digest('e');
    assert!(matches!(
        verify(&manifest, &wrong_version, &repository, &package),
        Err(DiscoveryPortableV2Error::ReferenceMismatch(
            DiscoveryPortableV2Mismatch::ImmutableVersion
        ))
    ));
}

#[test]
fn unsupported_future_discovery_stops_before_package_acceptance() {
    let (package, package_digest) = package();
    let (mut manifest, refs, repository) = discovery(package_digest);
    manifest.version.major = 2;
    assert!(
        matches!(verify(&manifest, &refs, &repository, &package), Err(DiscoveryPortableV2Error::Discovery(error)) if error.code == graphforge_discovery::DiscoveryErrorCode::UnsupportedFuture)
    );
}

#[test]
fn portable_integrity_failure_is_not_partially_accepted() {
    let (package, package_digest) = package();
    let (manifest, refs, repository) = discovery(package_digest);
    fs::write(
        package
            .path()
            .join("data/components/ontology/core-ontology/ontology.json"),
        b"tampered",
    )
    .unwrap();
    assert!(
        matches!(verify(&manifest, &refs, &repository, &package), Err(DiscoveryPortableV2Error::Portable(error)) if error.code == PortableV2ErrorCode::DigestMismatch)
    );
}
