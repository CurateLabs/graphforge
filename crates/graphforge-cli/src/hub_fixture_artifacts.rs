//! Rust-owned deterministic artifact generation for the public Hub fixture.

use graphforge_api::{
    GraphForge, OperationId, PortableV2ImportRequest, PortableVerifyRequest, verify_portable_v2,
};
use graphforge_discovery::{
    DISCOVERY_FORMAT, DiscoveryManifest, ObjectDescriptor, PORTABLE_V2_FORMAT,
    PORTABLE_V2_MEDIA_TYPE, PortablePackageReference, ProtocolRequirement, ProtocolVersion, RefSet,
    RepositoryIdentity, RepositoryRef, Sha256Digest,
};
use graphforge_storage::{PortableV2Limits, PortableV2Mode, repack_verified_expanded_portable_v2};
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
const OBJECT_NAME: &str = "openalex-openalex.gfpb";

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

#[derive(Debug, Serialize)]
struct FixtureMetadata<'a> {
    format: &'a str,
    generator: GeneratorIdentity<'a>,
    source: SourceIdentity,
    object_path: &'a str,
    object_digest: String,
    object_length: u64,
    package_digest: String,
    manifest_digest: String,
}

/// Generate canonical object, discovery, and provenance artifacts from the checked-in source.
#[expect(
    clippy::too_many_lines,
    reason = "one transaction binds portable and discovery artifacts"
)]
pub fn generate(source: &Path, destination: &Path) -> Result<(), String> {
    let limits = PortableV2Limits::default();
    let source_report = verify_portable_v2(
        &PortableVerifyRequest {
            input: source.to_path_buf(),
            mode: PortableV2Mode::Full,
            limits,
        },
        None,
    )
    .map_err(|error| error.to_string())?;
    let scratch = tempfile::tempdir().map_err(|error| error.to_string())?;
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
    .map_err(|error| error.to_string())?;
    GraphForge::new(imported.to_str()).map_err(|error| error.to_string())?;
    fs::create_dir_all(destination.join("objects")).map_err(|error| error.to_string())?;
    let object_path = destination.join("objects").join(OBJECT_NAME);
    let exported = repack_verified_expanded_portable_v2(
        source,
        &object_path,
        limits,
        &std::sync::atomic::AtomicBool::new(false),
    )
    .map_err(|error| error.to_string())?;
    let verified = verify_portable_v2(
        &PortableVerifyRequest {
            input: object_path.clone(),
            mode: PortableV2Mode::Full,
            limits,
        },
        None,
    )
    .map_err(|error| error.to_string())?;
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
    .map_err(|error| error.to_string())?;
    let reopened_bundle =
        GraphForge::new(imported_bundle.to_str()).map_err(|error| error.to_string())?;
    if reopened_bundle
        .committed_generation_identity()
        .map_err(|error| error.to_string())?
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
    let bytes = fs::read(&object_path).map_err(|error| error.to_string())?;
    let object_digest = digest_bytes(&bytes);
    let object_location = format!(
        "https://data.graphforge.sh/objects/sha256/{}",
        object_digest
            .strip_prefix("sha256:")
            .ok_or("transport digest prefix is invalid")?
    );
    if object_digest != exported_transport_digest {
        return Err("transport digest does not bind emitted object bytes".into());
    }
    let repository = RepositoryIdentity {
        owner: "openalex".into(),
        repository: "openalex".into(),
    };
    let manifest = DiscoveryManifest {
        format: DISCOVERY_FORMAT.into(),
        version: ProtocolVersion::CURRENT,
        repository: repository.clone(),
        default_ref: "main".into(),
        resolved_ref: "main".into(),
        immutable_version: Sha256Digest(exported_package_digest.clone()),
        package: PortablePackageReference {
            format: PORTABLE_V2_FORMAT.into(),
            package_digest: Sha256Digest(exported_package_digest.clone()),
            object_digest: Sha256Digest(object_digest.clone()),
        },
        requirements: vec![ProtocolRequirement {
            capability: "portable-v2".into(),
            major: 1,
        }],
        capabilities: vec![],
        objects: vec![ObjectDescriptor {
            digest: Sha256Digest(object_digest.clone()),
            length: u64::try_from(bytes.len()).map_err(|_| "object length overflow")?,
            media_type: PORTABLE_V2_MEDIA_TYPE.into(),
            locations: vec![object_location],
        }],
        extensions: BTreeMap::new(),
    };
    let manifest_bytes = manifest
        .to_canonical_json()
        .map_err(|error| error.to_string())?;
    let manifest_digest = manifest
        .canonical_digest()
        .map_err(|error| error.to_string())?
        .0;
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
    refs.validate_manifest(&manifest)
        .map_err(|error| error.to_string())?;
    let refs_bytes = refs
        .to_canonical_json()
        .map_err(|error| error.to_string())?;
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
        object_path: "objects/openalex-openalex.gfpb",
        object_digest,
        object_length: u64::try_from(bytes.len()).map_err(|_| "object length overflow")?,
        package_digest: exported_package_digest,
        manifest_digest,
    };
    write(destination.join("manifest.json"), &manifest_bytes)?;
    write(destination.join("refs.json"), &refs_bytes)?;
    let mut metadata_bytes = serde_json::to_vec(&metadata).map_err(|error| error.to_string())?;
    metadata_bytes.push(b'\n');
    write(destination.join("fixture.json"), &metadata_bytes)
}

/// Regenerate in a private directory and compare every checked-in artifact byte.
pub fn check(source: &Path, expected: &Path) -> Result<(), String> {
    validate(source, expected)?;
    let scratch = tempfile::tempdir().map_err(|error| error.to_string())?;
    generate(source, scratch.path())?;
    for relative in [
        "fixture.json",
        "manifest.json",
        "refs.json",
        "objects/openalex-openalex.gfpb",
    ] {
        let actual = fs::read(scratch.path().join(relative)).map_err(|error| error.to_string())?;
        let expected_bytes =
            fs::read(expected.join(relative)).map_err(|error| error.to_string())?;
        if actual != expected_bytes {
            return Err(format!("generated Hub fixture artifact drift: {relative}"));
        }
    }
    Ok(())
}

/// Validate all generator, source, portable, discovery, and metadata bindings.
#[expect(
    clippy::too_many_lines,
    reason = "one validator must check every cross-artifact fixture binding"
)]
fn validate(source: &Path, artifacts: &Path) -> Result<(), String> {
    validate_expected_tree(artifacts)?;
    let metadata: serde_json::Value = serde_json::from_slice(
        &fs::read(artifacts.join("fixture.json")).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    let manifest_bytes =
        fs::read(artifacts.join("manifest.json")).map_err(|error| error.to_string())?;
    let refs_bytes = fs::read(artifacts.join("refs.json")).map_err(|error| error.to_string())?;
    let manifest = DiscoveryManifest::from_json(
        &manifest_bytes,
        graphforge_discovery::DiscoveryLimits::default(),
    )
    .map_err(|error| error.to_string())?;
    let refs = RefSet::from_json(
        &refs_bytes,
        graphforge_discovery::DiscoveryLimits::default(),
    )
    .map_err(|error| error.to_string())?;
    refs.validate_manifest(&manifest)
        .map_err(|error| error.to_string())?;
    let expected_repository = RepositoryIdentity {
        owner: "openalex".into(),
        repository: "openalex".into(),
    };
    if manifest.repository != expected_repository
        || manifest.default_ref != "main"
        || manifest.resolved_ref != "main"
        || refs.repository != expected_repository
        || refs.default_ref != "main"
        || manifest.immutable_version != manifest.package.package_digest
    {
        return Err("fixture repository, ref, or immutable version binding is invalid".into());
    }
    let manifest_digest = manifest
        .canonical_digest()
        .map_err(|error| error.to_string())?
        .0;
    let reference = refs
        .refs
        .iter()
        .find(|reference| reference.name == "main")
        .ok_or("fixture main ref is absent")?;
    if reference.validator.0 != manifest_digest {
        return Err("fixture validator does not bind canonical manifest bytes".into());
    }
    let selected = manifest
        .package_object()
        .map_err(|error| error.to_string())?;
    let expected_location = format!(
        "https://data.graphforge.sh/objects/sha256/{}",
        selected
            .digest
            .0
            .strip_prefix("sha256:")
            .ok_or("object digest prefix is invalid")?
    );
    if selected.locations != [expected_location] {
        return Err("fixture object location is not digest addressed".into());
    }
    let object_path = metadata["object_path"]
        .as_str()
        .ok_or("fixture object path is absent")?;
    if object_path != "objects/openalex-openalex.gfpb" {
        return Err("fixture object path is invalid".into());
    }
    let object = artifacts.join(object_path);
    let bytes = fs::read(&object).map_err(|error| error.to_string())?;
    let report = verify_portable_v2(
        &PortableVerifyRequest {
            input: object,
            mode: PortableV2Mode::Full,
            limits: PortableV2Limits::default(),
        },
        None,
    )
    .map_err(|error| error.to_string())?;
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
        || metadata["source"]["package_digest"]
            != verify_portable_v2(
                &PortableVerifyRequest {
                    input: source.to_path_buf(),
                    mode: PortableV2Mode::Full,
                    limits: PortableV2Limits::default(),
                },
                None,
            )
            .map_err(|error| error.to_string())?
            .package_digest
    {
        return Err(
            "fixture portable, discovery, metadata, provenance, or source binding is invalid"
                .into(),
        );
    }
    Ok(())
}

fn validate_expected_tree(root: &Path) -> Result<(), String> {
    let mut actual = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(&directory).map_err(|error| error.to_string())? {
            let entry = entry.map_err(|error| error.to_string())?;
            let path = entry.path();
            let relative = path
                .strip_prefix(root)
                .map_err(|error| error.to_string())?
                .to_str()
                .ok_or("fixture artifact path is not UTF-8")?
                .to_owned();
            let metadata = fs::symlink_metadata(&path).map_err(|error| error.to_string())?;
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
    let expected = [
        "fixture.json",
        "manifest.json",
        "objects/",
        "objects/openalex-openalex.gfpb",
        "refs.json",
    ];
    if actual != expected {
        return Err("fixture artifact tree does not match the exact generated contract".into());
    }
    Ok(())
}

fn write(path: PathBuf, bytes: &[u8]) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    fs::write(path, bytes).map_err(|error| error.to_string())
}

fn digest_bytes(bytes: &[u8]) -> String {
    format!("sha256:{}", hex(&Sha256::digest(bytes)))
}

fn generator_source_digest() -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"graphforge-hub-fixture-generator-source/1\0");
    hasher.update(include_bytes!("hub_fixture_artifacts.rs"));
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
        let name = relative
            .to_str()
            .ok_or("fixture source path is not UTF-8")?
            .as_bytes();
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
        generate(source.path(), output.path()).unwrap();
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
        generate(source.path(), first.path()).unwrap();
        generate(source.path(), second.path()).unwrap();
        for relative in [
            "fixture.json",
            "manifest.json",
            "refs.json",
            "objects/openalex-openalex.gfpb",
        ] {
            assert_eq!(
                fs::read(first.path().join(relative)).unwrap(),
                fs::read(second.path().join(relative)).unwrap(),
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
}
