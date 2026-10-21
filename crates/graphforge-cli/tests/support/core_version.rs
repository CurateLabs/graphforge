//! Relabel real Core output as written by another Core version (#1936).
//!
//! A generation is relabelled by changing its one serialized
//! `producing_core_version` field and recomputing the manifest digest `CURRENT`
//! records. A package is relabelled by changing the declared version and
//! recomputing `package_digest` and the BagIt checksums. No digest is typed in.

use std::fs;
use std::path::Path;

use serde_json::Value;
use sha2::{Digest, Sha256};

pub const RUNNING: &str = graphforge_api::GRAPHFORGE_VERSION;

pub fn hex(bytes: impl AsRef<[u8]>) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub fn stamp(version: &str) -> String {
    format!(r#","producing_core_version":"{version}"}}"#)
}

pub fn relabel_generation(project: &Path, to: &str) {
    let current_path = project.join("CURRENT");
    let current_text = fs::read_to_string(&current_path).unwrap();
    let current: Value = serde_json::from_str(&current_text).unwrap();
    let recorded = current["generation_manifest_sha256"].as_str().unwrap();
    let manifest_path = project
        .join("generations")
        .join(current["generation_uuid"].as_str().unwrap())
        .join("manifest.json");
    let manifest = fs::read_to_string(&manifest_path).unwrap();
    let written = stamp(RUNNING);
    assert!(
        manifest.trim_end().ends_with(&written),
        "the writer did not stamp the generation: {manifest}"
    );
    let relabelled = manifest.replace(&written, &stamp(to));
    fs::write(&manifest_path, &relabelled).unwrap();
    fs::write(
        current_path,
        current_text.replace(recorded, &hex(relabelled.as_bytes())),
    )
    .unwrap();
}

pub fn relabel_package(package: &Path, to: &str) {
    let manifest_path = package.join("data/graphforge-project.json");
    let mut manifest: Value = serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
    assert_eq!(manifest["producing_core_version"], RUNNING);
    manifest["producing_core_version"] = to.into();
    manifest.as_object_mut().unwrap().remove("package_digest");
    let semantic = serde_json::to_vec(&manifest).unwrap();
    manifest["package_digest"] = format!(
        "sha256:{}",
        hex([b"graphforge-project/2\0".as_slice(), semantic.as_slice()].concat())
    )
    .into();
    fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    for name in ["manifest-sha256.txt", "tagmanifest-sha256.txt"] {
        let resealed: String = fs::read_to_string(package.join(name))
            .unwrap()
            .lines()
            .map(|line| {
                let (_, path) = line.split_once("  ").unwrap();
                format!("{}  {path}\n", hex(fs::read(package.join(path)).unwrap()))
            })
            .collect();
        fs::write(package.join(name), resealed).unwrap();
    }
}
