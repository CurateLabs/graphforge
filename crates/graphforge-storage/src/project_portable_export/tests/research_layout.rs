//! Research components lay out one self-contained closure per Version, registry first.

use super::historical_saved_queries::{identity, publish_archive, version};
use super::large_entries::{bundle_entries, verify};
use super::*;
use crate::research_versions::{
    ResearchParticipantCommitment, ResearchParticipantKey, ResearchVersionRecord,
};

const REGISTRY_PREFIX: &str = "data/components/research/research-registry-";
const RETAINED: &str = "data/components/research/research-retained/";

struct Fixture {
    _project: tempfile::TempDir,
    generation: ResolvedProjectGeneration,
    versions: [Uuid; 2],
    /// Digests of the graph files both Versions share.
    shared: BTreeSet<String>,
}

/// Two Versions committing to the same graph: the participant, its files, and
/// nothing else.
fn two_versions_sharing_one_graph() -> Fixture {
    let (root, parent) = graph_generation();
    let snapshot = parent
        .participant_snapshots()
        .unwrap()
        .into_iter()
        .find(|p| p.capability_id == "graph" && p.record_family_id == "files")
        .unwrap();
    let mut shared = BTreeSet::new();
    for file in crate::graph_files::decode_inventory(&snapshot.bytes)
        .unwrap()
        .files
    {
        let bytes = fs::read(parent.graph_tree_root().join(&file.relative_path)).unwrap();
        crate::graph_object_store::install_graph_object_bytes(root.path(), &bytes).unwrap();
        shared.insert(file.content_sha256);
    }
    crate::graph_object_store::install_graph_object_bytes(root.path(), &snapshot.bytes).unwrap();
    shared.insert(hex(Sha256::digest(&snapshot.bytes).into()));
    let graph = ResearchParticipantCommitment {
        key: ResearchParticipantKey {
            capability: snapshot.capability_id,
            family: snapshot.record_family_id,
        },
        capability_version: snapshot.capability_version,
        record_version: snapshot.record_version,
        encoding: "json".into(),
        schema_sha256: snapshot.schema_fingerprint,
        row_count: snapshot.row_count,
        content_sha256: Sha256::digest(&snapshot.bytes).into(),
    };
    let historical = version(vec![graph.clone()]);
    let mut selected = version(vec![graph]);
    selected
        .content
        .required_versions
        .insert(historical.version_uuid);
    let versions = [selected.version_uuid, historical.version_uuid];
    let (project, generation, _) = publish_archive(selected, historical, root, parent);
    Fixture {
        _project: project,
        generation,
        versions,
        shared,
    }
}

fn export_bundle(plan: &PortableExportPlan) -> (tempfile::TempDir, PathBuf) {
    let output = tempfile::tempdir().unwrap();
    let (_, bundle) = write_test_representations(plan, output.path());
    (output, bundle)
}

/// Re-point one research entry, re-signing the manifest.
fn rename_entry(plan: &mut PortableExportPlan, old: &str, new: &str) {
    plan.files
        .iter_mut()
        .find(|file| file.path == old)
        .unwrap()
        .path = new.into();
    let mut manifest: Value = serde_json::from_slice(&plan.manifest).unwrap();
    manifest["components"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .flat_map(|component| component["files"].as_array_mut().unwrap())
        .find(|file| file["path"] == old)
        .unwrap()["path"] = new.into();
    plan.manifest = canonical_json(&manifest).unwrap();
    resign_test_manifest(plan);
}

#[test]
fn each_version_carries_its_own_closure_after_the_registry() {
    let fixture = two_versions_sharing_one_graph();
    let plan =
        plan_complete_portable(&fixture.generation, PortableExportLimits::default()).unwrap();
    let (_output, bundle) = export_bundle(&plan);
    let paths: Vec<_> = bundle_entries(&fs::read(&bundle).unwrap())
        .into_iter()
        .map(|entry| entry.path)
        .collect();
    let registry = paths
        .iter()
        .position(|path| path.starts_with(REGISTRY_PREFIX))
        .expect("the registry participant is exported");
    let retained: Vec<_> = paths
        .iter()
        .enumerate()
        .filter(|(_, path)| path.starts_with(RETAINED))
        .collect();
    assert!(!retained.is_empty());
    assert!(
        retained.iter().all(|(index, _)| *index > registry),
        "the registry precedes every retained object in canonical order"
    );
    for id in fixture.versions {
        let held: BTreeSet<_> = retained
            .iter()
            .filter_map(|(_, path)| path.strip_prefix(&format!("{RETAINED}{id}/")))
            .map(str::to_owned)
            .collect();
        assert!(
            fixture.shared.is_subset(&held),
            "Version {id} carries the shared graph: {held:?}"
        );
    }
    assert_eq!(
        retained.len(),
        fixture
            .versions
            .iter()
            .map(|id| {
                let prefix = format!("{RETAINED}{id}/");
                retained
                    .iter()
                    .filter(|(_, path)| path.starts_with(&prefix))
                    .count()
            })
            .sum::<usize>(),
        "every retained object sits under a registry Version"
    );
    verify(&bundle).unwrap();
}

#[test]
fn retained_objects_outside_the_per_version_layout_are_incompatible() {
    let fixture = two_versions_sharing_one_graph();
    let limits = PortableExportLimits::default();
    let plan = plan_complete_portable(&fixture.generation, limits).unwrap();
    let old = plan
        .files
        .iter()
        .find(|file| file.path.starts_with(RETAINED))
        .unwrap()
        .path
        .clone();
    let digest = old.rsplit('/').next().unwrap();
    let stranger = Uuid::new_v4();
    for (name, renamed) in [
        (
            "the former research-content layout",
            format!("data/components/research/research-content/{digest}"),
        ),
        (
            "a directory that is not a registry Version",
            format!("{RETAINED}{stranger}/{digest}"),
        ),
    ] {
        let mut plan = plan_complete_portable(&fixture.generation, limits).unwrap();
        rename_entry(&mut plan, &old, &renamed);
        let (_output, bundle) = export_bundle(&plan);
        let error = verify(&bundle).unwrap_err();
        assert_eq!(error.code, PortableErrorCode::Incompatible, "{name}");
        assert!(
            error.to_string().contains(
                "research component identity, schema, provenance or selected closure is invalid"
            ),
            "{name}: {error}"
        );
    }
}
