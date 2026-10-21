//! Research components lay out one self-contained closure per Version, registry first.

use super::historical_saved_queries::{publish_archive, version};
use super::large_entries::{bundle_entries, verify};
use super::payload_inventory::{bundle_members, reassemble};
use super::*;
use crate::research_versions::{ResearchParticipantCommitment, ResearchParticipantKey};
use serde_json::Value;

const REGISTRY_PREFIX: &str = "data/components/research/research-registry-";
const RETAINED: &str = "data/components/research/research-retained/";

struct Fixture {
    _project: tempfile::TempDir,
    generation: ResolvedProjectGeneration,
    versions: [Uuid; 2],
    /// Digests of the graph files both Versions share.
    shared: BTreeSet<String>,
    /// One shared graph file: carried, never read, by verification.
    shard: String,
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
    let mut shard = None;
    for file in crate::graph_files::decode_inventory(&snapshot.bytes)
        .unwrap()
        .files
    {
        let bytes = fs::read(parent.graph_tree_root().join(&file.relative_path)).unwrap();
        crate::graph_object_store::install_graph_object_bytes(root.path(), &bytes).unwrap();
        shard.get_or_insert_with(|| file.content_sha256.clone());
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
        shard: shard.unwrap(),
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
    plan.files
        .sort_by(|a, b| a.path.as_bytes().cmp(b.path.as_bytes()));
    let mut manifest: Value = serde_json::from_slice(&plan.manifest).unwrap();
    for component in manifest["components"].as_array_mut().unwrap() {
        let files = component["files"].as_array_mut().unwrap();
        if let Some(file) = files.iter_mut().find(|file| file["path"] == old) {
            file["path"] = new.into();
            files.sort_by(|a, b| {
                a["path"]
                    .as_str()
                    .unwrap()
                    .as_bytes()
                    .cmp(b["path"].as_str().unwrap().as_bytes())
            });
        }
    }
    plan.manifest = canonical_json(&manifest).unwrap();
    resign_test_manifest(plan);
}

/// Drop one research entry, re-signing the manifest.
fn remove_entry(plan: &mut PortableExportPlan, path: &str) {
    plan.files.retain(|file| file.path != path);
    let mut manifest: Value = serde_json::from_slice(&plan.manifest).unwrap();
    for component in manifest["components"].as_array_mut().unwrap() {
        component["files"]
            .as_array_mut()
            .unwrap()
            .retain(|file| file["path"] != path);
    }
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

/// `tests/fixtures/portable/cases.json` pins the layout; the producer must emit it.
#[test]
fn producer_emits_the_pinned_registry_first_layout() {
    let vector: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/portable/cases.json"
    ))
    .unwrap();
    let layout = &vector["research_layout"];
    let registry = layout["registry"].as_str().unwrap();
    let retained = layout["retained"].as_str().unwrap();
    let former = layout["former_retained"].as_str().unwrap();
    let fixture = two_versions_sharing_one_graph();
    let plan =
        plan_complete_portable(&fixture.generation, PortableExportLimits::default()).unwrap();
    let mut paths: Vec<_> = plan.files.iter().map(|file| file.path.as_str()).collect();
    paths.sort_by_key(|path| path.as_bytes());
    let first = paths
        .iter()
        .position(|path| path.starts_with("data/components/research/"))
        .unwrap();
    assert_eq!(
        paths[first], registry,
        "the registry is the first research entry"
    );
    let rest = &paths[first + 1..];
    let mut seen = 0;
    for path in rest
        .iter()
        .filter(|path| path.starts_with("data/components/research/"))
    {
        let (version, digest) = path
            .strip_prefix(RETAINED)
            .and_then(|tail| tail.split_once('/'))
            .unwrap();
        assert_eq!(
            *path,
            retained
                .replace("{version_uuid}", version)
                .replace("{sha256}", digest)
        );
        assert!(fixture.versions.iter().any(|id| id.to_string() == version));
        assert!(
            former.replace("{sha256}", digest).as_bytes() < registry.as_bytes(),
            "the former layout sorted before the registry"
        );
        seen += 1;
    }
    assert!(seen >= 2 * fixture.shared.len());
}

#[test]
fn a_relocated_registry_is_refused() {
    let fixture = two_versions_sharing_one_graph();
    let mut plan =
        plan_complete_portable(&fixture.generation, PortableExportLimits::default()).unwrap();
    let registry = plan
        .files
        .iter()
        .find(|file| file.path.starts_with(REGISTRY_PREFIX))
        .unwrap()
        .path
        .clone();
    // Manifest, checksums and canonical order are all consistent; only the place is wrong, and
    // every retained object now precedes the registry.
    rename_entry(
        &mut plan,
        &registry,
        "data/components/research/zzz/participant.json",
    );
    let (_output, bundle) = export_bundle(&plan);
    let paths: Vec<_> = bundle_entries(&fs::read(&bundle).unwrap())
        .into_iter()
        .map(|entry| entry.path)
        .collect();
    let moved = paths
        .iter()
        .position(|path| path.ends_with("/zzz/participant.json"))
        .unwrap();
    assert!(
        paths
            .iter()
            .enumerate()
            .filter(|(_, path)| path.starts_with(RETAINED))
            .all(|(index, _)| index < moved)
    );
    // A runtime participant must sit in its own component directory, so the
    // relocation is refused by the placement rule before research validation.
    let error = verify(&bundle).unwrap_err();
    assert_eq!(error.code, PortableErrorCode::InvalidStructure);
    assert!(
        error
            .to_string()
            .contains("runtime participant outside its component kind"),
        "{error}"
    );
}

/// A research package reports its byte totals and footer signals alike from every
/// path the research driver takes (expanded directory, bundle file, stream) and
/// from the plain scan: one report, apart from `source`.
#[test]
fn a_research_package_reports_the_same_facts_from_every_path() {
    let fixture = two_versions_sharing_one_graph();
    let plan =
        plan_complete_portable(&fixture.generation, PortableExportLimits::default()).unwrap();
    let output = tempfile::tempdir().unwrap();
    let (expanded, bundle) = write_test_representations(&plan, output.path());
    let driven = |path: &Path| {
        crate::project_portable::research::stream::validate_research_path(
            path,
            None,
            PortableLimits::default(),
            None,
            None,
        )
        .unwrap()
    };
    let mut reader = std::io::BufReader::new(fs::File::open(&bundle).unwrap());
    let streamed =
        crate::verify_portable_stream(&mut reader, None, PortableLimits::default(), None).unwrap();
    let plain =
        crate::verify_portable(&bundle, PortableMode::Full, PortableLimits::default(), None)
            .unwrap();
    // Every report equals the bundle file's, but for `source`; the directory's
    // also differs in `representation` and has no `transport_digest`.
    let without = |report: &crate::PortableReport, fields: &[&str]| {
        let mut value = serde_json::to_value(report).unwrap();
        for field in fields {
            value.as_object_mut().unwrap().remove(*field).unwrap();
        }
        value
    };
    let reports = [driven(&expanded), driven(&bundle), streamed, plain];
    let bundle_report = without(&reports[1], &["source"]);
    for report in &reports[2..] {
        assert_eq!(without(report, &["source"]), bundle_report);
    }
    let directory_fields = ["source", "representation", "transport_digest"];
    assert_eq!(
        without(&reports[0], &directory_fields),
        without(&reports[1], &directory_fields)
    );
    let report = &reports[0];
    assert!(report.research_interchange);
    assert_eq!(
        report.bytes_by_kind.values().sum::<u64>(),
        report.payload_bytes
    );
    assert!(report.bytes_by_kind["research"] > 0);
    let parquet = report.plausibility.parquet.expect("a full verification");
    assert!(
        parquet.files_with_footer > 0,
        "the graph tree carries Parquet"
    );
    assert!(parquet.files_with_footer <= parquet.files);
}

/// A graph tree file declared outside the graph tree directory (here in the
/// opaque `evidence` directory) would reach a decoder unwalked; it is refused
/// `InvalidStructure`, naming the file, by the placement rule on every path and
/// whether or not any other rule also covers its location.
#[test]
fn a_graph_tree_file_outside_the_graph_tree_directory_is_refused_everywhere() {
    let fixture = two_versions_sharing_one_graph();
    let mut plan =
        plan_complete_portable(&fixture.generation, PortableExportLimits::default()).unwrap();
    let shard = plan
        .files
        .iter()
        .find(|file| {
            file.path
                .starts_with("data/components/graph-data/graph-tree/")
        })
        .unwrap()
        .path
        .clone();
    let moved = "data/components/evidence/shard.parquet";
    rename_entry(&mut plan, &shard, moved);
    let output = tempfile::tempdir().unwrap();
    let (expanded, bundle) = write_test_representations(&plan, output.path());
    let mut reader = std::io::BufReader::new(fs::File::open(&bundle).unwrap());
    let streamed =
        crate::verify_portable_stream(&mut reader, None, PortableLimits::default(), None)
            .unwrap_err();
    let driven = |path: &Path| {
        crate::project_portable::research::stream::validate_research_path(
            path,
            None,
            PortableLimits::default(),
            None,
            None,
        )
        .unwrap_err()
    };
    for error in [driven(&expanded), driven(&bundle), streamed] {
        assert_eq!(error.code, PortableErrorCode::InvalidStructure, "{error}");
        assert_eq!(error.entry.as_deref(), Some(moved), "{error}");
        assert!(
            error
                .to_string()
                .contains("graph tree file outside the graph tree directory"),
            "{error}"
        );
    }
}

/// The exporter's research package with its registry component made an
/// `evidence` component, its runtime mapping kept; `move_payload` also moves
/// the registry's file under `data/components/evidence/`. Refused
/// `InvalidStructure` from a directory, a bundle file and a stream alike, with
/// one message, which must contain `detail`.
fn assert_registry_refused_everywhere(move_payload: bool, detail: &str) {
    let fixture = two_versions_sharing_one_graph();
    let mut plan =
        plan_complete_portable(&fixture.generation, PortableExportLimits::default()).unwrap();
    let registry = plan
        .files
        .iter()
        .find(|file| file.path.starts_with(REGISTRY_PREFIX))
        .unwrap()
        .path
        .clone();
    let participant = registry
        .strip_prefix("data/components/research/")
        .and_then(|rest| rest.split('/').next())
        .unwrap()
        .to_owned();
    if move_payload {
        rename_entry(
            &mut plan,
            &registry,
            &format!("data/components/evidence/{participant}/participant.json"),
        );
    }
    let mut manifest: Value = serde_json::from_slice(&plan.manifest).unwrap();
    let capabilities = manifest["requirements"]["capabilities"]
        .as_array_mut()
        .unwrap();
    capabilities.push("evidence@1".into());
    capabilities.sort_by(|left, right| left.as_str().cmp(&right.as_str()));
    let components = manifest["components"].as_array_mut().unwrap();
    components
        .iter_mut()
        .find(|component| component["participant_id"] == participant.as_str())
        .unwrap()["kind"] = "evidence".into();
    components.sort_by(|left, right| {
        let key = |component: &Value| {
            (
                component["kind"].as_str().unwrap().to_owned(),
                component["participant_id"].as_str().unwrap().to_owned(),
            )
        };
        key(left).cmp(&key(right))
    });
    plan.manifest = canonical_json(&manifest).unwrap();
    resign_test_manifest(&mut plan);

    let output = tempfile::tempdir().unwrap();
    let (expanded, bundle) = write_test_representations(&plan, output.path());
    let mut reader = std::io::BufReader::new(fs::File::open(&bundle).unwrap());
    let streamed =
        crate::verify_portable_stream(&mut reader, None, PortableLimits::default(), None)
            .unwrap_err();
    // A research package is verified by the research driver on every path, as
    // the facade's `verify_portable` does, so refusals rank alike.
    let driven = |path: &Path| {
        crate::project_portable::research::stream::validate_research_path(
            path,
            None,
            PortableLimits::default(),
            None,
            None,
        )
        .unwrap_err()
    };
    let verdicts = [driven(&expanded), driven(&bundle), streamed];
    for error in &verdicts {
        assert_eq!(error.code, PortableErrorCode::InvalidStructure, "{error}");
        assert!(error.to_string().contains(detail), "{error}");
    }
    assert_eq!(verdicts[0].to_string(), verdicts[1].to_string());
    assert_eq!(verdicts[1].to_string(), verdicts[2].to_string());
}

/// The registry's component and payload both sit under `evidence`: the research
/// driver, which keys the registry by its path, refuses the scan itself.
#[test]
fn a_registry_moved_into_evidence_is_refused_everywhere() {
    assert_registry_refused_everywhere(true, "research content precedes the registry");
}

/// Only the registry's component kind is `evidence`; its file keeps the research
/// path, so the scan is content with it. The placement rule, which runs before
/// any research check, is what refuses it.
#[test]
fn an_evidence_kind_registry_component_is_refused_as_misplaced_everywhere() {
    assert_registry_refused_everywhere(false, "runtime participant outside its component kind");
}

#[test]
fn research_entries_out_of_canonical_order_are_refused() {
    let fixture = two_versions_sharing_one_graph();
    let plan =
        plan_complete_portable(&fixture.generation, PortableExportLimits::default()).unwrap();
    let (output, bundle) = export_bundle(&plan);
    let bytes = fs::read(&bundle).unwrap();
    let members = bundle_members(&bytes);
    let mut order: Vec<_> = members.iter().collect();
    let registry = order
        .iter()
        .position(|(path, _, _)| path.starts_with(REGISTRY_PREFIX))
        .unwrap();
    // The valid layout, with the registry moved after the first retained object.
    let moved = order.remove(registry);
    let retained = order
        .iter()
        .position(|(path, _, _)| path.starts_with(RETAINED))
        .unwrap();
    order.insert(retained + 1, moved);
    let reordered = output.path().join("reordered.gfpb");
    fs::write(&reordered, reassemble(&bytes, &order)).unwrap();
    let error = verify(&reordered).unwrap_err();
    assert_eq!(error.code, PortableErrorCode::InvalidStructure);
    assert!(
        error
            .to_string()
            .ends_with("bundle entries are not canonical order"),
        "{error}"
    );
}

#[test]
fn a_shared_object_missing_from_one_version_is_incompatible() {
    let fixture = two_versions_sharing_one_graph();
    let mut plan =
        plan_complete_portable(&fixture.generation, PortableExportLimits::default()).unwrap();
    let [kept, stripped] = fixture
        .versions
        .map(|id| format!("{RETAINED}{id}/{}", fixture.shard));
    assert!(plan.files.iter().any(|file| file.path == kept));
    // The other Version still carries the object, so the package holds it; only the
    // stripped Version's own closure is incomplete.
    remove_entry(&mut plan, &stripped);
    let (_output, bundle) = export_bundle(&plan);
    let error = verify(&bundle).unwrap_err();
    assert_eq!(error.code, PortableErrorCode::Incompatible);
    assert!(
        error.to_string().contains(
            "research component identity, schema, provenance or selected closure is invalid"
        ),
        "{error}"
    );
}
