//! Deterministic CI fixture for file-backed graph generations (#338).
//!
//! Proves the public Rust facade can publish and reopen a multi-file graph
//! without assembling the workspace into one Arrow snapshot payload.
//!
//! Large-class evidence (> legacy 2 GiB snapshot envelope) is an ignored manual
//! test in this file. It does not download external 8M/128M fixtures in CI.
//! Reproduce with:
//! `GF_FILE_BACKED_OVERSIZE_EVIDENCE_OUT=build/file-backed-oversize-evidence.json \
//!  cargo test -p graphforge-api --test file_backed_graph_generation \
//!  oversize_file_backed_generation_exceeds_legacy_snapshot_envelope -- --ignored --nocapture`

#[path = "support/project_fixture.rs"]
mod project_fixture;

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use graphforge_api::{
    CheckpointRequest, GraphForge, OperationId, PortableExportRequest, PortableSelection,
};
use graphforge_storage::{
    GRAPH_CAPABILITY_ID, GRAPH_CAPABILITY_VERSION, GRAPH_FILES_FAMILY, GraphFilesOpenStrategy,
    ProjectCapability, ProjectGenerationRequest, ProjectStageOutcome, capture_graph_files,
    empty_workspace_participants, resolve_project_generation,
    stage_project_generation_with_graph_tree,
};
use uuid::Uuid;

/// Legacy graph snapshot envelope total (must remain exceeded by oversize evidence).
const LEGACY_SNAPSHOT_ENVELOPE_BYTES: u64 = 2 * 1024 * 1024 * 1024;
/// Deterministic oversize padding used for the manual large-class proof.
const OVERSIZE_PADDING_BYTES: u64 = LEGACY_SNAPSHOT_ENVELOPE_BYTES + 64 * 1024 * 1024;

#[test]
fn file_backed_multi_file_fixture_reopens_without_snapshot_envelope() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().to_str().unwrap();
    {
        let graph = GraphForge::new(Some(path)).unwrap();
        graph
            .execute("CREATE (:Person {name: 'Ada'}), (:Person {name: 'Bob'})")
            .unwrap();
        graph
            .execute(
                "MATCH (a:Person {name: 'Ada'}), (b:Person {name: 'Bob'}) \
                 CREATE (a)-[:KNOWS]->(b)",
            )
            .unwrap();
    }

    let generation = resolve_project_generation(root.path()).unwrap();
    let files = generation
        .participant_snapshot("graph", GRAPH_FILES_FAMILY)
        .unwrap()
        .expect("published generation must use graph/files");
    assert_eq!(files.encoding, "json");
    assert!(
        generation
            .participant_snapshot("graph", "snapshot")
            .unwrap()
            .is_none(),
        "new publications must not write the legacy snapshot envelope"
    );
    let inventory = generation.graph_files_inventory().unwrap().unwrap();
    assert!(inventory.file_count >= 2);
    assert!(inventory.total_byte_length > 0);
    assert!(generation.graph_tree_root().is_dir());

    let reopened = GraphForge::new(Some(path)).unwrap();
    let evidence = reopened.graph_open_evidence();
    assert_eq!(
        evidence.strategy,
        GraphFilesOpenStrategy::PrivateMaterialize
    );
    assert!(evidence.files_validated >= 2);
    assert_eq!(evidence.files_copied, evidence.files_validated);
    assert_eq!(evidence.files_opened_in_place, 0);
    assert!(evidence.bytes_copied > 0);
    assert_eq!(evidence.bytes_copied, evidence.bytes_validated);

    let result = reopened
        .execute("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS from, b.name AS to")
        .unwrap();
    assert_eq!(result.stats.rows_produced, 1);
}

#[test]
fn checkpoint_read_only_open_pins_graph_tree_in_place() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().to_str().unwrap();
    let graph = GraphForge::new(Some(path)).unwrap();
    graph
        .execute("CREATE (:Person {name: 'Ada'})-[:KNOWS]->(:Person {name: 'Bob'})")
        .unwrap();
    graph
        .checkpoint(CheckpointRequest {
            name: "pinned".into(),
            description: Some("file-backed read-only pin".into()),
            idempotency_key: OperationId(Uuid::now_v7()),
            actor_uuid: None,
        })
        .unwrap();

    let view = graph.open_checkpoint("pinned").unwrap();
    let evidence = view.graph_open_evidence();
    assert_eq!(evidence.strategy, GraphFilesOpenStrategy::PinnedInPlace);
    assert!(evidence.files_validated >= 2);
    assert_eq!(evidence.files_copied, 0);
    assert_eq!(evidence.bytes_copied, 0);
    assert_eq!(evidence.files_opened_in_place, evidence.files_validated);

    let result = view
        .execute("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN count(*) AS n")
        .unwrap();
    assert_eq!(result.stats.rows_produced, 1);
}

#[test]
fn portable_export_of_file_backed_generation_is_structured_unsupported() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().to_str().unwrap();
    let graph = GraphForge::new(Some(path)).unwrap();
    graph
        .execute("CREATE (:Person {name: 'Ada'})")
        .unwrap();

    let generation = resolve_project_generation(root.path()).unwrap();
    assert!(
        generation
            .participant_snapshot("graph", GRAPH_FILES_FAMILY)
            .unwrap()
            .is_some()
    );

    let output = root.path().join("file-backed.gfportable");
    let error = graph
        .export_portable(PortableExportRequest {
            selection: PortableSelection::Current,
            output,
        })
        .expect_err("file-backed portable export must fail closed");
    assert_eq!(error.code(), "GF_UNSUPPORTED_PROJECT_FORMAT");
    assert!(
        error
            .to_string()
            .contains("portable interchange does not yet encode file-backed graph trees")
    );
}

#[test]
fn legacy_snapshot_generations_remain_readable() {
    let root = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(workspace.path().join("topology")).unwrap();
    std::fs::write(workspace.path().join("topology/nodes.parquet"), b"legacy").unwrap();
    project_fixture::publish_graph_workspace(root.path(), workspace.path());

    let generation = resolve_project_generation(root.path()).unwrap();
    assert!(
        generation
            .participant_snapshot("graph", "snapshot")
            .unwrap()
            .is_some()
    );
    assert!(
        generation
            .participant_snapshot("graph", GRAPH_FILES_FAMILY)
            .unwrap()
            .is_none()
    );

    let opened = GraphForge::new(Some(root.path().to_str().unwrap())).unwrap();
    assert_eq!(
        opened.graph_open_evidence().strategy,
        GraphFilesOpenStrategy::LegacySnapshotHydrate
    );
}

#[test]
fn unsupported_graph_files_inventory_version_is_structured() {
    let error = graphforge_storage::decode_inventory(
        br#"{"format":"graphforge-graph-files","format_version":99,"file_count":0,"files":[],"total_byte_length":0}
"#,
    )
    .unwrap_err();
    assert_eq!(error.code(), "GF_UNSUPPORTED_PROJECT_FORMAT");
}

/// Manual large-class proof: publish a queryable file-backed generation whose
/// validated bytes exceed the legacy 2 GiB snapshot envelope, then reopen via
/// `GraphForge::new` without whole-graph Arrow hydrate.
///
/// Uses sparse padding beside a real small graph so RAM stays bounded. CI must
/// not run this (ignored; no external fixture download).
#[test]
#[ignore = "manual large-class evidence; sparse >2GiB tree, not CI"]
fn oversize_file_backed_generation_exceeds_legacy_snapshot_envelope() {
    let evidence_out = std::env::var_os("GF_FILE_BACKED_OVERSIZE_EVIDENCE_OUT")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("build/file-backed-oversize-evidence.json"));
    if let Some(parent) = evidence_out.parent() {
        fs::create_dir_all(parent).unwrap();
    }

    let root = tempfile::tempdir().unwrap();
    let project = root.path().join("project");
    fs::create_dir_all(&project).unwrap();
    let path = project.to_str().unwrap();

    {
        let graph = GraphForge::new(Some(path)).unwrap();
        graph
            .execute("CREATE (:Person {name: 'Ada'}), (:Person {name: 'Bob'})")
            .unwrap();
        graph
            .execute(
                "MATCH (a:Person {name: 'Ada'}), (b:Person {name: 'Bob'}) \
                 CREATE (a)-[:KNOWS]->(b)",
            )
            .unwrap();
    }

    let seed_generation = resolve_project_generation(&project).unwrap();
    let expected_parent = seed_generation.generation_uuid();
    let seed_tree = seed_generation.graph_tree_root();
    let workspace = root.path().join("workspace");
    copy_dir_recursive(&seed_tree, &workspace).unwrap();

    let padding_rel = Path::new("padding/oversize.bin");
    let padding_path = workspace.join(padding_rel);
    fs::create_dir_all(padding_path.parent().unwrap()).unwrap();
    create_sparse_file(&padding_path, OVERSIZE_PADDING_BYTES).unwrap();

    let (inventory, files_participant) = capture_graph_files(&workspace).unwrap();
    assert!(
        inventory.total_byte_length > LEGACY_SNAPSHOT_ENVELOPE_BYTES,
        "inventory must exceed legacy 2 GiB envelope (got {})",
        inventory.total_byte_length
    );

    let mut participants = empty_workspace_participants().unwrap();
    participants.insert(0, files_participant);
    let generation_uuid = Uuid::now_v7();
    let request = ProjectGenerationRequest {
        transaction_uuid: Uuid::now_v7(),
        generation_uuid,
        capabilities: vec![
            ProjectCapability {
                capability_id: GRAPH_CAPABILITY_ID.into(),
                capability_version: GRAPH_CAPABILITY_VERSION,
            },
            ProjectCapability {
                capability_id: "workspace".into(),
                capability_version: 1,
            },
        ],
        participants,
    };
    let ProjectStageOutcome::Staged(staged) =
        stage_project_generation_with_graph_tree(&project, &request, Some(workspace.as_path()))
            .unwrap()
    else {
        panic!("oversize publication unexpectedly replayed");
    };
    staged
        .validate(
            |_| Ok(()),
            |actual_parent, _| {
                assert_eq!(actual_parent.generation_uuid(), expected_parent);
                Ok(())
            },
        )
        .unwrap()
        .publish()
        .unwrap();

    let rss_before_open = peak_rss_bytes();
    let reopened = GraphForge::new(Some(path)).unwrap();
    let open_evidence = reopened.graph_open_evidence().clone();
    assert_ne!(
        open_evidence.strategy,
        GraphFilesOpenStrategy::LegacySnapshotHydrate
    );
    assert!(open_evidence.bytes_validated > LEGACY_SNAPSHOT_ENVELOPE_BYTES);
    assert_eq!(open_evidence.bytes_validated, inventory.total_byte_length);
    assert_eq!(
        open_evidence.strategy,
        GraphFilesOpenStrategy::PrivateMaterialize
    );
    assert_eq!(open_evidence.bytes_copied, open_evidence.bytes_validated);

    let result = reopened
        .execute("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS from, b.name AS to")
        .unwrap();
    assert_eq!(result.stats.rows_produced, 1);
    let rss_after_query = peak_rss_bytes();

    let generation = resolve_project_generation(&project).unwrap();
    assert_eq!(generation.generation_uuid(), generation_uuid);
    let committed_inventory = generation.graph_files_inventory().unwrap().unwrap();
    assert!(committed_inventory.total_byte_length > LEGACY_SNAPSHOT_ENVELOPE_BYTES);

    let payload = serde_json::json!({
        "schema": "graphforge-file-backed-oversize-evidence/1",
        "issue": 338,
        "source_sha": git_head_sha(),
        "strategy": "sparse_padding_beside_queryable_graph",
        "legacy_snapshot_envelope_bytes": LEGACY_SNAPSHOT_ENVELOPE_BYTES,
        "inventory": {
            "file_count": committed_inventory.file_count,
            "total_byte_length": committed_inventory.total_byte_length,
            "padding_relative_path": padding_rel.to_string_lossy(),
            "padding_byte_length": OVERSIZE_PADDING_BYTES,
        },
        "publication": {
            "generation_uuid": generation_uuid.hyphenated().to_string(),
            "path": "stage_project_generation_with_graph_tree + publish (same path as GraphForge)",
        },
        "open": {
            "api": "GraphForge::new",
            "strategy": format!("{:?}", open_evidence.strategy),
            "files_validated": open_evidence.files_validated,
            "bytes_validated": open_evidence.bytes_validated,
            "files_copied": open_evidence.files_copied,
            "bytes_copied": open_evidence.bytes_copied,
            "files_opened_in_place": open_evidence.files_opened_in_place,
        },
        "query": {
            "cypher": "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS from, b.name AS to",
            "rows_produced": 1,
        },
        "resources": {
            "peak_rss_bytes_before_open": rss_before_open,
            "peak_rss_bytes_after_query": rss_after_query,
            "honest_limits": [
                "Padding is sparse on supporting filesystems; validated/copied byte lengths are logical sizes.",
                "Writable GraphForge::new materializes file-by-file (PrivateMaterialize); checkpoint/RO uses PinnedInPlace.",
                "This proves public persistence beyond the 2 GiB snapshot envelope; full 8M/128M (~15 GiB) remains optional measured evidence under local resource stops."
            ]
        },
        "recorded_at_unix_secs": SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    });
    fs::write(&evidence_out, serde_json::to_vec_pretty(&payload).unwrap()).unwrap();
    eprintln!(
        "wrote oversize file-backed evidence to {}",
        evidence_out.display()
    );
}

fn create_sparse_file(path: &Path, logical_bytes: u64) -> std::io::Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?;
    if logical_bytes == 0 {
        return Ok(());
    }
    file.seek(SeekFrom::Start(logical_bytes - 1))?;
    file.write_all(&[0])?;
    file.sync_all()?;
    Ok(())
}

fn copy_dir_recursive(source: &Path, destination: &Path) -> std::io::Result<()> {
    fs::create_dir_all(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let to = destination.join(entry.file_name());
        if file_type.is_dir() {
            copy_dir_recursive(&entry.path(), &to)?;
        } else if file_type.is_file() {
            fs::copy(entry.path(), to)?;
        } else {
            return Err(std::io::Error::other("special file in graph tree"));
        }
    }
    Ok(())
}

fn peak_rss_bytes() -> Option<u64> {
    let mut file = File::open("/proc/self/status").ok()?;
    let mut contents = String::new();
    file.read_to_string(&mut contents).ok()?;
    for line in contents.lines() {
        if let Some(value) = line.strip_prefix("VmHWM:") {
            let kb = value
                .trim()
                .trim_end_matches(" kB")
                .trim()
                .parse::<u64>()
                .ok()?;
            return Some(kb.saturating_mul(1024));
        }
    }
    None
}

fn git_head_sha() -> String {
    Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .and_then(|output| {
            if output.status.success() {
                Some(String::from_utf8_lossy(&output.stdout).trim().to_owned())
            } else {
                None
            }
        })
        .unwrap_or_else(|| "unknown".into())
}
