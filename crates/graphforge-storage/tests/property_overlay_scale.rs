//! Production-path scale evidence for authenticated immutable property overlays (#940).
#![cfg(unix)]

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::Read;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::Command;

use graphforge_core::{OntologyMode, TypeId};
use graphforge_ir::IrLiteral;
use graphforge_storage::{
    GraphWriter, PropertyFragmentId, PropertyOverlayLimits, PropertyRouteKind, delete_nodes,
    enumerate_property_fragments, remove_node_properties, set_node_properties,
    visit_authenticated_property_snapshots,
};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use uuid::Uuid;

const N: usize = 8 * 1024;
const MAX_ROWS: usize = 4 * N;
// Calibrated fixed-process noise allowance. Each row carries 2 KiB, so a
// forbidden 4N materialization exceeds this allowance by at least 8x.
const RSS_STARTUP_ALLOWANCE_BYTES: u64 = 8 * 1024 * 1024;
const TS: i64 = 1_700_000_000_000_000;
const CHILD_PROJECT_ENV: &str = "GF_PROPERTY_OVERLAY_SCALE_PROJECT";
const CHILD_ROWS_ENV: &str = "GF_PROPERTY_OVERLAY_SCALE_ROWS";
const CHILD_EVIDENCE_ENV: &str = "GF_PROPERTY_OVERLAY_SCALE_EVIDENCE";
const MIXED_WINDOW: usize = 48;
// One authentication pass plus bounded validation/value decoder passes. The
// numerator is measured bytes, while the denominator is immutable payload.
const MAX_READ_AMPLIFICATION: u64 = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
struct ImmutableFragment {
    id: PropertyFragmentId,
    path: PathBuf,
    bytes: u64,
    allocated_bytes: u64,
    sha256: [u8; 32],
    device: u64,
    inode: u64,
    modified_seconds: i64,
    modified_nanoseconds: i64,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct ScaleEvidence {
    logical_rows: usize,
    graph_tree_bytes: u64,
    graph_tree_allocated_bytes: u64,
    property_fragment_bytes: u64,
    property_fragment_allocated_bytes: u64,
    prior_fragment_bytes: u64,
    prior_fragments_unchanged: bool,
    rss_before_write_bytes: u64,
    rss_after_write_bytes: u64,
    rss_before_scan_bytes: u64,
    rss_after_scan_bytes: u64,
    physical_rows: u64,
    physical_bytes: u64,
    spill_bytes: u64,
    spill_runs: u64,
    merge_passes: u64,
    peak_buffered_rows: u64,
    peak_buffered_bytes: u64,
    per_record_seeks: u64,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct WriteEvidence {
    rss_before_bytes: u64,
    rss_after_bytes: u64,
    prior_fragment_bytes: u64,
    prior_fragments_unchanged: bool,
}

fn overlay_limits() -> PropertyOverlayLimits {
    PropertyOverlayLimits {
        max_buffered_rows: 64,
        max_open_runs: 4,
        max_buffered_bytes: 512 * 1024 * 1024,
        max_row_bytes: 4 * 1024,
    }
}

fn uuid(index: usize) -> Uuid {
    let mut bytes = [0_u8; 16];
    bytes[8..].copy_from_slice(&u64::try_from(index + 1).unwrap().to_be_bytes());
    Uuid::from_bytes(bytes)
}

fn graph_tree_bytes(root: &Path) -> u64 {
    fn visit(path: &Path) -> u64 {
        let metadata = fs::symlink_metadata(path).unwrap();
        if metadata.is_file() {
            return metadata.len();
        }
        if !metadata.is_dir() {
            return 0;
        }
        fs::read_dir(path)
            .unwrap()
            .map(|entry| visit(&entry.unwrap().path()))
            .sum()
    }
    visit(root)
}

fn graph_tree_allocated_bytes(root: &Path) -> u64 {
    fn visit(path: &Path) -> u64 {
        let metadata = fs::symlink_metadata(path).unwrap();
        if metadata.is_file() {
            return metadata.blocks().saturating_mul(512);
        }
        if !metadata.is_dir() {
            return 0;
        }
        fs::read_dir(path)
            .unwrap()
            .map(|entry| visit(&entry.unwrap().path()))
            .sum()
    }
    visit(root)
}

fn fragment_inventory(root: &Path) -> Vec<ImmutableFragment> {
    enumerate_property_fragments(root, PropertyRouteKind::Node, "_untyped")
        .unwrap()
        .into_iter()
        .map(|fragment| {
            let mut file = fs::File::open(&fragment.path).unwrap();
            let metadata = file.metadata().unwrap();
            let bytes = metadata.len();
            let mut digest = Sha256::new();
            let mut buffer = [0_u8; 64 * 1024];
            loop {
                let read = file.read(&mut buffer).unwrap();
                if read == 0 {
                    break;
                }
                digest.update(&buffer[..read]);
            }
            ImmutableFragment {
                id: fragment.id,
                path: fragment.path,
                bytes,
                allocated_bytes: metadata.blocks().saturating_mul(512),
                sha256: digest.finalize().into(),
                device: metadata.dev(),
                inode: metadata.ino(),
                modified_seconds: metadata.mtime(),
                modified_nanoseconds: metadata.mtime_nsec(),
            }
        })
        .collect()
}

fn property_fragment_bytes(root: &Path) -> u64 {
    fragment_inventory(root)
        .iter()
        .map(|fragment| fragment.bytes)
        .sum()
}

fn property_fragment_allocated_bytes(root: &Path) -> u64 {
    fragment_inventory(root)
        .iter()
        .map(|fragment| fragment.allocated_bytes)
        .sum()
}

#[cfg(unix)]
fn process_peak_rss_bytes() -> u64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: getrusage initializes the supplied rusage structure on success.
    let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    assert_eq!(result, 0, "getrusage must report this test process");
    // SAFETY: the successful call above initialized the structure.
    let rss = unsafe { usage.assume_init() }.ru_maxrss;
    let rss = u64::try_from(rss).expect("peak RSS must be non-negative");
    if cfg!(target_os = "macos") {
        rss
    } else {
        rss.saturating_mul(1024)
    }
}

fn populate_through_graph_writer(root: &Path, end: usize) {
    let mut writer = GraphWriter::open_at(root, OntologyMode::Exploratory, TS).unwrap();
    for index in 0..end {
        writer
            .set_properties(
                &uuid(index),
                None,
                HashMap::from([
                    (
                        "ordinal".to_owned(),
                        IrLiteral::Int(i64::try_from(index).unwrap()),
                    ),
                    ("payload".to_owned(), IrLiteral::Str("x".repeat(2048))),
                ]),
            )
            .unwrap();
    }
    writer.flush().unwrap();
}

fn apply_fixed_write_window(root: &Path) {
    let update_end = MIXED_WINDOW / 3;
    let remove_end = 2 * MIXED_WINDOW / 3;
    let updates = (0..update_end)
        .map(|index| {
            (
                uuid(index).into_bytes(),
                HashMap::from([("ordinal".to_owned(), IrLiteral::Int(-1))]),
            )
        })
        .collect();
    set_node_properties(root, "_untyped", &updates).unwrap();
    let removals = (update_end..remove_end)
        .map(|index| {
            (
                uuid(index).into_bytes(),
                HashSet::from(["payload".to_owned()]),
            )
        })
        .collect();
    remove_node_properties(root, "_untyped", &removals).unwrap();
    let deletes = (remove_end..MIXED_WINDOW)
        .map(|index| uuid(index).into_bytes())
        .collect::<HashSet<_>>();
    delete_nodes(root, &deletes).unwrap();
}

#[cfg(unix)]
#[test]
fn property_overlay_scale_scan_child() {
    let Ok(project) = std::env::var(CHILD_PROJECT_ENV) else {
        return;
    };
    let logical_rows = std::env::var(CHILD_ROWS_ENV)
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let evidence_path = PathBuf::from(std::env::var(CHILD_EVIDENCE_ENV).unwrap());
    let scratch = TempDir::new().unwrap();
    let limits = overlay_limits();
    let rss_before_scan_bytes = process_peak_rss_bytes();
    let mut emitted_rows = 0_usize;
    let metrics = visit_authenticated_property_snapshots(
        Path::new(&project),
        PropertyRouteKind::Node,
        "_untyped",
        scratch.path(),
        limits,
        |_| {
            emitted_rows += 1;
            Ok(())
        },
    )
    .unwrap();
    let rss_after_scan_bytes = process_peak_rss_bytes();
    assert_eq!(emitted_rows, logical_rows);
    let evidence = ScaleEvidence {
        logical_rows,
        graph_tree_bytes: graph_tree_bytes(Path::new(&project)),
        graph_tree_allocated_bytes: graph_tree_allocated_bytes(Path::new(&project)),
        property_fragment_bytes: property_fragment_bytes(Path::new(&project)),
        property_fragment_allocated_bytes: property_fragment_allocated_bytes(Path::new(&project)),
        prior_fragment_bytes: 0,
        prior_fragments_unchanged: false,
        rss_before_write_bytes: 0,
        rss_after_write_bytes: 0,
        rss_before_scan_bytes,
        rss_after_scan_bytes,
        physical_rows: metrics.physical_rows,
        physical_bytes: metrics.physical_bytes,
        spill_bytes: metrics.spill_bytes,
        spill_runs: metrics.spill_runs,
        merge_passes: metrics.merge_passes,
        peak_buffered_rows: metrics.peak_buffered_rows,
        peak_buffered_bytes: metrics.peak_buffered_bytes,
        per_record_seeks: metrics.per_record_seeks,
    };
    fs::write(evidence_path, serde_json::to_vec(&evidence).unwrap()).unwrap();
    eprintln!("property-overlay-scale-child-evidence={evidence:#?}");
}

#[cfg(unix)]
#[test]
fn property_overlay_scale_write_child() {
    let Ok(project) = std::env::var(CHILD_PROJECT_ENV) else {
        return;
    };
    let evidence_path = PathBuf::from(std::env::var(CHILD_EVIDENCE_ENV).unwrap());
    let before_fragments = fragment_inventory(Path::new(&project));
    let prior_fragment_bytes = before_fragments.iter().map(|fragment| fragment.bytes).sum();
    let before = process_peak_rss_bytes();
    apply_fixed_write_window(Path::new(&project));
    let after = process_peak_rss_bytes();
    let after_fragments = fragment_inventory(Path::new(&project));
    let prior_fragments_unchanged = after_fragments
        .iter()
        .filter(|fragment| before_fragments.iter().any(|prior| prior.id == fragment.id))
        .eq(before_fragments.iter());
    fs::write(
        evidence_path,
        serde_json::to_vec(&WriteEvidence {
            rss_before_bytes: before,
            rss_after_bytes: after,
            prior_fragment_bytes,
            prior_fragments_unchanged,
        })
        .unwrap(),
    )
    .unwrap();
}

fn write_in_isolated_process(project: &Path) -> WriteEvidence {
    let output = TempDir::new().unwrap();
    let evidence_path = output.path().join("write-evidence.json");
    let status = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "property_overlay_scale_write_child",
            "--nocapture",
        ])
        .env(CHILD_PROJECT_ENV, project)
        .env(CHILD_EVIDENCE_ENV, &evidence_path)
        .status()
        .unwrap();
    assert!(
        status.success(),
        "isolated mixed property write must succeed"
    );
    serde_json::from_slice(&fs::read(evidence_path).unwrap()).unwrap()
}

fn scan_in_isolated_process(project: &Path, logical_rows: usize) -> ScaleEvidence {
    let output = TempDir::new().unwrap();
    let evidence_path = output.path().join("evidence.json");
    let status = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "property_overlay_scale_scan_child",
            "--nocapture",
        ])
        .env(CHILD_PROJECT_ENV, project)
        .env(CHILD_ROWS_ENV, logical_rows.to_string())
        .env(CHILD_EVIDENCE_ENV, &evidence_path)
        .status()
        .unwrap();
    assert!(status.success(), "isolated authenticated scan must succeed");
    serde_json::from_slice(&fs::read(evidence_path).unwrap()).unwrap()
}

#[cfg(unix)]
#[test]
fn production_property_overlay_n_2n_4n_is_disk_growing_and_memory_bounded() {
    let limits = overlay_limits();
    let mut evidence = Vec::<ScaleEvidence>::new();

    for logical_rows in [N, 2 * N, 4 * N] {
        let project = TempDir::new().unwrap();
        let mut topology =
            GraphWriter::open_at(project.path(), OntologyMode::Exploratory, TS).unwrap();
        for index in 0..MAX_ROWS {
            topology.create_node(uuid(index), TypeId(0)).unwrap();
        }
        topology.flush().unwrap();
        populate_through_graph_writer(project.path(), logical_rows);
        let disk_before = graph_tree_bytes(project.path());
        let allocated_before = graph_tree_allocated_bytes(project.path());
        let property_before = property_fragment_bytes(project.path());
        let property_allocated_before = property_fragment_allocated_bytes(project.path());
        let write = write_in_isolated_process(project.path());
        let disk_bytes = graph_tree_bytes(project.path());
        let allocated_bytes = graph_tree_allocated_bytes(project.path());
        let property_bytes = property_fragment_bytes(project.path());
        let property_allocated_bytes = property_fragment_allocated_bytes(project.path());
        assert!(write.prior_fragments_unchanged);
        assert_eq!(write.prior_fragment_bytes, property_before);
        assert!(property_bytes > property_before);
        assert!(property_allocated_bytes > property_allocated_before);
        assert!(disk_bytes > disk_before);
        assert!(allocated_bytes > allocated_before);

        let expected_live_rows = logical_rows - MIXED_WINDOW / 3;
        let mut phase = scan_in_isolated_process(project.path(), expected_live_rows);
        phase.graph_tree_bytes = disk_bytes;
        phase.graph_tree_allocated_bytes = allocated_bytes;
        phase.property_fragment_bytes = property_bytes;
        phase.property_fragment_allocated_bytes = property_allocated_bytes;
        phase.prior_fragment_bytes = write.prior_fragment_bytes;
        phase.prior_fragments_unchanged = write.prior_fragments_unchanged;
        phase.rss_before_write_bytes = write.rss_before_bytes;
        phase.rss_after_write_bytes = write.rss_after_bytes;
        assert!(phase.physical_bytes > 0);
        assert!(phase.spill_bytes > 0);
        assert!(phase.spill_runs > 0);
        assert!(phase.merge_passes > 0);
        assert!(phase.peak_buffered_rows <= u64::try_from(limits.max_buffered_rows * 2).unwrap());
        assert!(phase.peak_buffered_bytes <= limits.max_buffered_bytes);
        assert_eq!(phase.per_record_seeks, 0);
        assert!(
            phase.physical_bytes
                <= phase
                    .property_fragment_bytes
                    .saturating_mul(MAX_READ_AMPLIFICATION)
        );
        let spill_bound = phase
            .physical_rows
            .saturating_mul(limits.max_row_bytes)
            .saturating_mul(phase.merge_passes.saturating_add(1));
        assert!(phase.spill_bytes <= spill_bound);
        evidence.push(phase);
    }

    let scan_rss_growth = evidence
        .iter()
        .map(|phase| {
            phase
                .rss_after_scan_bytes
                .saturating_sub(phase.rss_before_scan_bytes)
        })
        .collect::<Vec<_>>();
    if evidence
        .iter()
        .all(|phase| phase.rss_before_scan_bytes != 0)
    {
        let earlier_peak_growth = scan_rss_growth[0].max(scan_rss_growth[1]);
        assert!(
            scan_rss_growth[2] <= earlier_peak_growth.saturating_add(RSS_STARTUP_ALLOWANCE_BYTES),
            "4N isolated scan RSS growth must plateau within the earlier peak plus the documented allocator/startup allowance: {evidence:#?}"
        );
    }
    let write_rss_growth = evidence
        .iter()
        .map(|phase| {
            phase
                .rss_after_write_bytes
                .saturating_sub(phase.rss_before_write_bytes)
        })
        .collect::<Vec<_>>();
    if evidence
        .iter()
        .all(|phase| phase.rss_before_write_bytes != 0)
    {
        let earlier_peak_growth = write_rss_growth[0].max(write_rss_growth[1]);
        assert!(
            write_rss_growth[2] <= earlier_peak_growth.saturating_add(RSS_STARTUP_ALLOWANCE_BYTES),
            "4N isolated mixed-write RSS growth must plateau within the earlier peak plus the documented allocator/startup allowance: {evidence:#?}"
        );
    }
    assert!(evidence.windows(2).all(|pair| {
        pair[1].logical_rows > pair[0].logical_rows
            && pair[1].rss_after_write_bytes >= pair[1].rss_before_write_bytes
            && pair[1].graph_tree_bytes > pair[0].graph_tree_bytes
            && pair[1].graph_tree_allocated_bytes > pair[0].graph_tree_allocated_bytes
            && pair[1].property_fragment_bytes > pair[0].property_fragment_bytes
            && pair[1].property_fragment_allocated_bytes > pair[0].property_fragment_allocated_bytes
            && pair[1].prior_fragment_bytes > pair[0].prior_fragment_bytes
            && pair[1].prior_fragments_unchanged
            && pair[1].physical_bytes > pair[0].physical_bytes
            && pair[1].spill_bytes > pair[0].spill_bytes
            && pair[1].physical_rows > pair[0].physical_rows
            && pair[1].spill_runs >= pair[0].spill_runs
            && pair[1].merge_passes >= pair[0].merge_passes
            && pair[1].peak_buffered_rows <= u64::try_from(limits.max_buffered_rows * 2).unwrap()
            && pair[1].peak_buffered_bytes <= limits.max_buffered_bytes
            && pair[1].per_record_seeks == 0
    }));
    eprintln!("property-overlay-scale-evidence={evidence:#?}");
}
