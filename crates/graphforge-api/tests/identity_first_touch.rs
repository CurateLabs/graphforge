//! Ordinal identity blocks are authenticated on first touch, and a compact
//! project's identity runs stay immutable shared objects (#1388).
//!
//! The runs are hard-linked from the content-addressed store at open, so a
//! same-inode byte flip is visible through the workspace without any rename.
//! Open reads none of them; the lookup that reads a block checks it.

use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use arrow::array::{Array, FixedSizeBinaryArray};
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
    writable.set_readonly(false);
    std::fs::set_permissions(object, writable).unwrap();
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .read(true)
        .open(object)
        .unwrap();
    let modified = file.metadata().unwrap().modified().unwrap();
    let mut byte = [0_u8; 1];
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.read_exact(&mut byte).unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&[byte[0] ^ 0xff]).unwrap();
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
    // The publisher recorded that UUIDs ascend with ordinals, so the ordered
    // fast path asks nothing of the corrupted block until a lookup reads it.
    assert_eq!(rows(&forge, ORDERED), Ok(3));
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
    forge
        .execute("CREATE (:Entity), (:Entity), (:Entity)")
        .unwrap();
    forge
        .execute("MATCH (n:Entity) WITH n LIMIT 5 DETACH DELETE n")
        .unwrap();
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

/// The order fact the publisher recorded in the project's ordinal manifest.
fn recorded_order(path: &Path) -> Option<bool> {
    const MANIFEST: &str = "topology/uuid-membership/ordinal-v4-manifest.json";
    let generation = graphforge_storage::resolve_project_generation(path).unwrap();
    // An expanded (mutated) generation keeps its tree; a compact one is
    // addressed through the content store.
    let tree = generation.graph_tree_root().join(MANIFEST);
    let bytes = match std::fs::read(&tree) {
        Ok(bytes) => bytes,
        Err(_) => std::fs::read(
            objects(path)
                .into_iter()
                .find(|(relative, ..)| relative == MANIFEST)
                .expect("ordinal manifest")
                .1,
        )
        .unwrap(),
    };
    let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    value
        .get("uuid_order_matches_ordinals")
        .map(|flag| flag.as_bool().expect("boolean"))
}

fn ids(forge: &GraphForge, query: &str) -> Vec<[u8; 16]> {
    let result = forge.execute(query).unwrap();
    let mut ids = Vec::new();
    for batch in &result.batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        ids.extend((0..column.len()).map(|row| <[u8; 16]>::try_from(column.value(row)).unwrap()));
    }
    ids
}

/// The ordered one-hop answer, and the same answer derived without it: every
/// destination, sorted here.
fn ordered_and_oracle(forge: &GraphForge, limit: usize) -> (Vec<[u8; 16]>, Vec<[u8; 16]>) {
    let ordered = ids(
        forge,
        &format!("MATCH (a)-[r]->(b) RETURN b.node_uuid AS id ORDER BY id LIMIT {limit}"),
    );
    let mut all = ids(forge, "MATCH (a)-[r]->(b) RETURN b.node_uuid AS id");
    all.sort_unstable();
    all.truncate(limit);
    (ordered, all)
}

#[test]
fn publication_records_the_uuid_order_it_streamed() {
    let (_root, path) = project(4_096);
    assert_eq!(recorded_order(&path), Some(true));

    // A node whose UUID sorts below every existing one (the fixture's UUIDs
    // are above any version-7 timestamp) breaks the order, and the commit that
    // publishes it must say so.
    let forge = GraphForge::new(Some(path.to_str().unwrap())).unwrap();
    forge
        .execute("MATCH (a:Entity) WITH a LIMIT 1 CREATE (a)-[:LINK]->(:Entity)")
        .unwrap();
    drop(forge);
    assert_eq!(recorded_order(&path), Some(false));
}

#[test]
fn ordered_queries_stay_correct_when_a_mutation_breaks_uuid_order() {
    const LIMIT: usize = 20;
    let (_root, path) = project(4_096);
    let forge = GraphForge::new(Some(path.to_str().unwrap())).unwrap();
    let (ordered, oracle) = ordered_and_oracle(&forge, LIMIT);
    assert_eq!(ordered, oracle);
    forge
        .execute("MATCH (a:Entity) WITH a LIMIT 1 CREATE (a)-[:LINK]->(:Entity)")
        .unwrap();
    drop(forge);

    // Ordinal order no longer follows UUID order: the fast path must refuse,
    // and the generic plan must still answer in UUID order, newest node first.
    let forge = GraphForge::new(Some(path.to_str().unwrap())).unwrap();
    let (ordered, oracle) = ordered_and_oracle(&forge, LIMIT);
    assert_eq!(ordered, oracle);
    assert!(
        ordered[0] < *bulk_fixture::fixture_node_uuid(0).as_bytes(),
        "the new node's UUID sorts first"
    );
}
