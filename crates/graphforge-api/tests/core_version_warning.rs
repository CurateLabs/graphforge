//! Data written by another Core version is detected and warned about (#1936).
//!
//! "Core X" data is made by the real writer and then relabelled: a generation
//! by changing the one serialized `producing_core_version` field and recomputing
//! the manifest digest `CURRENT` records; a package by changing the declared
//! version and recomputing `package_digest` and the BagIt checksums. No digest
//! is typed in.

use std::fs;
use std::path::{Path, PathBuf};

use graphforge_api::{
    CORE_VERSION_MISMATCH_CODE, GRAPHFORGE_VERSION, GraphForge, OperationId, PortableExportRequest,
    PortableImportRequest, PortableSelection,
};
use graphforge_core::portable::{PortableLimits, PortableOutput, PortableSelectionProfile};
use serde_json::Value;
use sha2::{Digest, Sha256};

const CORE_X: &str = "0.0.1";

fn hex(bytes: impl AsRef<[u8]>) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn stamp(version: &str) -> String {
    format!(r#","producing_core_version":"{version}"}}"#)
}

/// Write a durable project through the real writer: its CURRENT generation is
/// stamped with the running Core version.
fn written_project(root: &Path) -> PathBuf {
    let project = root.join("project");
    let graph = GraphForge::new(project.to_str()).unwrap();
    graph.execute("CREATE (:Item {score: 1})").unwrap();
    drop(graph);
    project
}

fn item_count(graph: &GraphForge) -> usize {
    graph
        .execute("MATCH (n:Item) RETURN n")
        .unwrap()
        .batches
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum()
}

fn current_generation_manifest(project: &Path) -> (PathBuf, String) {
    let current = fs::read_to_string(project.join("CURRENT")).unwrap();
    let current: Value = serde_json::from_str(&current).unwrap();
    let generation = current["generation_uuid"].as_str().unwrap();
    (
        project
            .join("generations")
            .join(generation)
            .join("manifest.json"),
        current["generation_manifest_sha256"]
            .as_str()
            .unwrap()
            .to_owned(),
    )
}

/// Relabel the CURRENT generation's declared Core version (`None` removes the
/// stamp, as a generation written before it existed) and recompute the digest
/// `CURRENT` records. The stamp is the manifest's last field, so replacing its
/// serialized text is exactly what the writer emits for the new value.
fn relabel_generation(project: &Path, to: Option<&str>) {
    let (manifest_path, recorded_digest) = current_generation_manifest(project);
    let manifest = fs::read_to_string(&manifest_path).unwrap();
    let written = stamp(GRAPHFORGE_VERSION);
    assert!(
        manifest.trim_end().ends_with(&written),
        "the writer did not stamp the generation: {manifest}"
    );
    let relabelled = manifest.replace(
        &written,
        &to.map_or_else(|| "}".to_owned(), |version| stamp(version)),
    );
    fs::write(&manifest_path, &relabelled).unwrap();
    let current_path = project.join("CURRENT");
    let current = fs::read_to_string(&current_path).unwrap();
    assert!(current.contains(&recorded_digest));
    fs::write(
        current_path,
        current.replace(&recorded_digest, &hex(relabelled.as_bytes())),
    )
    .unwrap();
}

fn export_package(graph: &GraphForge, output: &Path) {
    graph
        .export_portable(
            &PortableExportRequest {
                selection: PortableSelection::Current,
                output_path: output.to_path_buf(),
                representation: PortableOutput::Expanded,
                profile: PortableSelectionProfile::Complete,
                subset: None,
                limits: PortableLimits::default(),
            },
            None,
            |_| {},
        )
        .unwrap();
}

/// Relabel a package's declared Core version and reseal it: the semantic
/// `package_digest` (which covers the declaration), then both BagIt checksum
/// files, so only the version differs from a package the real writer made.
fn relabel_package(package: &Path, to: &str) {
    let manifest_path = package.join("data/graphforge-project.json");
    let mut manifest: Value = serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
    assert_eq!(manifest["producing_core_version"], GRAPHFORGE_VERSION);
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

fn import(package: &Path, into: &Path, key: u128) -> graphforge_api::PortableImportResult {
    GraphForge::import_portable(
        into,
        &PortableImportRequest {
            input: package.to_path_buf(),
            operation_id: OperationId(uuid::Uuid::from_u128(key)),
            limits: PortableLimits::default(),
        },
        None,
    )
    .unwrap()
}

fn assert_names_both(warning: &graphforge_api::CoreVersionWarning) {
    assert_eq!(warning.code, CORE_VERSION_MISMATCH_CODE);
    assert_eq!(warning.code, "GF_CORE_VERSION_MISMATCH");
    assert_eq!(warning.producing_core_version, CORE_X);
    assert_eq!(warning.running_core_version, GRAPHFORGE_VERSION);
    assert!(warning.message.contains(CORE_X), "{}", warning.message);
    assert!(
        warning.message.contains(GRAPHFORGE_VERSION),
        "{}",
        warning.message
    );
    assert!(warning.message.contains(&format!("graphforge=={CORE_X}")));
}

#[test]
fn opening_a_generation_written_by_another_core_warns_naming_both_versions() {
    let root = tempfile::tempdir().unwrap();
    let project = written_project(root.path());
    relabel_generation(&project, Some(CORE_X));

    let graph = GraphForge::new(project.to_str()).unwrap();
    let warnings = graph.open_warnings();
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert_names_both(&warnings[0]);
    // A mismatch alone never refuses: the project is still readable.
    assert_eq!(item_count(&graph), 1);
}

#[test]
fn opening_a_generation_written_by_the_running_core_does_not_warn() {
    let root = tempfile::tempdir().unwrap();
    let project = written_project(root.path());

    assert!(
        GraphForge::new(project.to_str())
            .unwrap()
            .open_warnings()
            .is_empty()
    );
    assert!(GraphForge::new(None).unwrap().open_warnings().is_empty());
}

#[test]
fn opening_a_generation_without_a_stamp_does_not_warn() {
    let root = tempfile::tempdir().unwrap();
    let project = written_project(root.path());
    relabel_generation(&project, None);

    let graph = GraphForge::new(project.to_str()).unwrap();
    assert!(graph.open_warnings().is_empty());
    assert_eq!(item_count(&graph), 1);
}

#[test]
fn a_commit_restamps_the_generation_with_the_running_core() {
    let root = tempfile::tempdir().unwrap();
    let project = written_project(root.path());
    relabel_generation(&project, Some(CORE_X));
    assert_eq!(
        GraphForge::new(project.to_str())
            .unwrap()
            .open_warnings()
            .len(),
        1
    );

    let graph = GraphForge::new(project.to_str()).unwrap();
    graph.execute("CREATE (:Item {score: 2})").unwrap();
    drop(graph);

    // The last writer is now the running Core.
    let reopened = GraphForge::new(project.to_str()).unwrap();
    assert!(reopened.open_warnings().is_empty());
}

#[test]
fn importing_a_package_from_another_core_warns_and_still_imports() {
    let root = tempfile::tempdir().unwrap();
    let project = written_project(root.path());
    let graph = GraphForge::new(project.to_str()).unwrap();
    let package = root.path().join("package");
    export_package(&graph, &package);
    drop(graph);
    relabel_package(&package, CORE_X);

    let target = root.path().join("imported");
    let result = import(&package, &target, 1);

    assert_eq!(result.warnings.len(), 1, "{:?}", result.warnings);
    assert_names_both(&result.warnings[0]);
    // The import completed: the imported project opens and holds the data.
    let imported = GraphForge::new(target.to_str()).unwrap();
    assert_eq!(item_count(&imported), 1);
    // The imported generation was written by the running Core.
    assert!(imported.open_warnings().is_empty());
}

#[test]
fn importing_a_package_from_the_running_core_does_not_warn() {
    let root = tempfile::tempdir().unwrap();
    let project = written_project(root.path());
    let graph = GraphForge::new(project.to_str()).unwrap();
    let package = root.path().join("package");
    export_package(&graph, &package);
    drop(graph);

    let result = import(&package, &root.path().join("imported"), 2);

    assert!(result.warnings.is_empty(), "{:?}", result.warnings);
}
