//! Ordinal identity blocks are authenticated on first touch, and a compact
//! project's identity runs stay immutable shared objects (#1388).
//!
//! The runs are hard-linked from the content-addressed store at open, so a
//! same-inode byte flip is visible through the workspace without any rename.
//! Open reads none of them; the lookup that reads a block checks it.

use std::os::unix::fs::{FileExt, PermissionsExt};
use std::path::{Path, PathBuf};

use graphforge_api::GraphForge;
use sha2::{Digest, Sha256};

#[path = "support/bulk_fixture.rs"]
mod bulk_fixture;

const NODES: usize = 16_384; // four 64 KiB ordinal blocks
const BLOCK_BYTES: u64 = 64 * 1024;
const HEALTHY: &str = "MATCH (a)-[r]->(b) RETURN b.node_uuid AS id LIMIT 3";
const WHOLE: &str = "MATCH (a)-[r]->(b) RETURN b.node_uuid AS id";
const ORDERED: &str = "MATCH (a)-[r]->(b) RETURN b.node_uuid AS id ORDER BY id LIMIT 3";

fn project(nodes: usize) -> (tempfile::TempDir, PathBuf) {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("state");
    bulk_fixture::generate_bulk_graph_with_index(&path, nodes, 4, false);
    (root, path)
}

/// `(relative path, CAS object path, SHA-256, length)` of every graph file.
fn objects(path: &Path) -> Vec<(String, PathBuf, String, u64)> {
    let generation = graphforge_storage::resolve_project_generation(path).unwrap();
    generation
        .unadmitted_graph_files_inventory()
        .unwrap()
        .unwrap()
        .files
        .into_iter()
        .map(|entry| {
            let object =
                graphforge_storage::graph_object_path(path, &entry.content_sha256).unwrap();
            (
                entry.relative_path,
                object,
                entry.content_sha256,
                entry.byte_length,
            )
        })
        .collect()
}

fn identity_run(path: &Path, prefix: &str) -> PathBuf {
    objects(path)
        .into_iter()
        .find(|(relative, ..)| {
            relative.starts_with(&format!("topology/uuid-membership/{prefix}"))
                && relative.ends_with(".uuidx")
        })
        .unwrap_or_else(|| panic!("no {prefix} run"))
        .1
}

/// Flip one byte of the shared inode in place: same inode, same length, same
/// mtime, so only a content check can notice.
fn flip_in_place(object: &Path, offset: u64) {
    let original = std::fs::metadata(object).unwrap().permissions();
    let mut writable = original.clone();
    writable.set_mode(0o644);
    std::fs::set_permissions(object, writable).unwrap();
    let file = std::fs::OpenOptions::new().write(true).read(true).open(object).unwrap();
    let modified = file.metadata().unwrap().modified().unwrap();
    let mut byte = [0_u8; 1];
    file.read_exact_at(&mut byte, offset).unwrap();
    file.write_all_at(&[byte[0] ^ 0xff], offset).unwrap();
    file.set_modified(modified).unwrap();
    drop(file);
    std::fs::set_permissions(object, original).unwrap();
}

fn rows(forge: &GraphForge, query: &str) -> Result<usize, String> {
    forge
        .execute(query)
        .map(|result| result.batches.iter().map(|b| b.num_rows()).sum())
        .map_err(|error| error.to_string())
}

#[test]
fn flipped_ordinal_block_opens_but_is_refused_by_the_lookup_that_reads_it() {
    let (_root, path) = project(NODES);
    // The last block: ordinals 12_289..=16_384.
    flip_in_place(&identity_run(&path, "ordinal-v4-"), 3 * BLOCK_BYTES + 5);

    // Open reads no identity byte, so it cannot notice.
    let forge = GraphForge::new(Some(path.to_str().unwrap())).unwrap();
    // Lookups that touch only healthy blocks keep answering.
    assert_eq!(rows(&forge, HEALTHY), Ok(3));
    // A lookup that reads the corrupted block is refused, with no rows.
    let refused = rows(&forge, WHOLE).unwrap_err();

    assert!(
        refused.contains("authentication") || refused.contains("ordinal identity"),
        "{refused}"
    );
    // The ordered fast path must prove UUID order over every block, so it
    // refuses too: a corrupted block is never reported as "unordered".
    let refused = rows(&forge, ORDERED).unwrap_err();
    assert!(
        refused.contains("v4 ordinal identity artifact authentication failed"),
        "{refused}"
    );
}

#[test]
fn healthy_project_answers_every_identity_query() {
    let (_root, path) = project(NODES);
    let forge = GraphForge::new(Some(path.to_str().unwrap())).unwrap();
    assert_eq!(rows(&forge, HEALTHY), Ok(3));
    assert_eq!(rows(&forge, ORDERED), Ok(3));
    assert_eq!(rows(&forge, WHOLE), Ok(NODES * 4));
}

#[test]
fn identity_runs_are_shared_read_only_inodes_after_open() {
    let (_root, path) = project(2_048);
    let forge = GraphForge::new(Some(path.to_str().unwrap())).unwrap();
    assert_eq!(rows(&forge, HEALTHY), Ok(3));
    for prefix in ["forward-v4-", "ordinal-v4-"] {
        let object = identity_run(&path, prefix);
        let file = std::fs::File::open(&object).unwrap();
        assert!(
            graphforge_filesystem::file_link_count(&file).unwrap() >= 2,
            "{prefix} run was copied, not linked"
        );
        assert!(file.metadata().unwrap().permissions().readonly());
    }
}

/// A mutating commit that adds and removes nodes replaces identity artifacts
/// by new files. No writer may write through a hydrated, shared run.
#[test]
fn mutating_commit_never_writes_through_a_shared_identity_run() {
    let (_root, path) = project(2_048);
    let before = objects(&path);
    let forge = GraphForge::new(Some(path.to_str().unwrap())).unwrap();
    assert_eq!(rows(&forge, "MATCH (n) RETURN n"), Ok(2_048));
    forge.execute("CREATE (:Entity), (:Entity), (:Entity)").unwrap();
    forge.execute("MATCH (n:Entity) WITH n LIMIT 5 DETACH DELETE n").unwrap();
    assert_eq!(rows(&forge, "MATCH (n) RETURN n"), Ok(2_048 + 3 - 5));
    drop(forge);

    // Every object the project began with still is what its name says.
    for (relative, object, digest, length) in &before {
        let bytes = std::fs::read(object).unwrap();
        assert_eq!(bytes.len() as u64, *length, "{relative}");
        let actual = Sha256::digest(&bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        assert_eq!(&actual, digest, "{relative} was modified in place");
    }
    let reopened = GraphForge::new(Some(path.to_str().unwrap())).unwrap();
    assert_eq!(rows(&reopened, "MATCH (n) RETURN n"), Ok(2_048 + 3 - 5));
    assert!(rows(&reopened, HEALTHY).is_ok());
    assert!(rows(&reopened, ORDERED).is_ok());
}
