//! Ordinal identity blocks are authenticated on first touch, and a compact
//! project's identity runs stay immutable shared objects (#1388).
//!
//! The runs are hard-linked from the content-addressed store at open, so a
//! same-inode byte flip is visible through the workspace without any rename.
//! Open reads none of them; the lookup that reads a block checks it. No query
//! reads a forward run: the commit that builds the next identity generation on
//! it does, and refuses a flipped one.
//!
//! The small ordinal controls (the manifest, the receipt, the tombstone runs
//! and the lock) are different: hydration copies each into a
//! private single-link file and checks the copy against the manifest, so the
//! open is the operation that touches them, and it refuses a flipped one. The
//! lock is published empty, so it has no byte to flip; it is opened and
//! flocked, never read.
//!
//! Append validation reads the node and edge Parquet that identity is answered
//! from (#1902). Those objects are hard-linked like the ordinal runs and read by
//! no query that does not need their columns; the commit's identity probe is the
//! operation that touches them, and it refuses a flipped one before it names a
//! new digest.

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
        .map_err(|error| format!("{} {error}", error.code()))
}

#[test]
fn flipped_ordinal_block_opens_but_is_refused_by_the_lookup_that_reads_it() {
    let (_root, path) = project(NODES);
    // The second block (ordinals 4_097..=8_192): neither a range end nor a block
    // the small queries below resolve.
    flip_in_place(&identity_run(&path, "ordinal-v4-"), BLOCK_BYTES + 5);

    // Open reads no identity byte, so it cannot notice.
    let forge = GraphForge::new(Some(path.to_str().unwrap())).unwrap();
    // Lookups that touch only healthy blocks keep answering.
    assert_eq!(rows(&forge, HEALTHY), Ok(3));
    // A lookup that reads the corrupted block is refused, with no rows.
    let refused = rows(&forge, WHOLE).unwrap_err();
    // The same public class as every other first-touch refusal of committed
    // data (`graph_admission`), not an execution error.
    assert!(
        refused.starts_with("GF_VALIDATION ")
            && refused.contains("v4 ordinal identity artifact authentication failed"),
        "{refused}"
    );
    // The publisher recorded that UUIDs ascend with ordinals, so the ordered
    // fast path asks nothing of the corrupted block until a lookup reads it.
    assert_eq!(rows(&forge, ORDERED), Ok(3));

    // The first and last block of a range are read to check the record, so the
    // ordered fast path refuses a flip there.
    let (_last_root, last) = project(NODES);
    flip_in_place(&identity_run(&last, "ordinal-v4-"), 3 * BLOCK_BYTES + 5);
    let forge = GraphForge::new(Some(last.to_str().unwrap())).unwrap();
    assert_eq!(rows(&forge, HEALTHY), Ok(3));
    let refused = rows(&forge, ORDERED).unwrap_err();
    assert!(refused.starts_with("GF_VALIDATION "), "{refused}");
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

fn generation_uuid(path: &Path) -> uuid::Uuid {
    graphforge_storage::resolve_project_generation(path)
        .unwrap()
        .generation_uuid()
}

/// Forward runs carry `(UUID, surrogate)` records of 24 bytes and have no
/// block fences: no lookup reads them, so a query never refuses one. The
/// commit that builds the next identity generation reads the whole run, and
/// must refuse a flipped one before it names a new digest.
#[test]
fn flipped_forward_run_is_refused_by_the_commit_that_builds_on_it() {
    const RECORD: u64 = 24;
    let (_root, path) = project(NODES);
    let forward = identity_run(&path, "forward-v4-");
    // The last surrogate byte of a record in the middle of the run.
    flip_in_place(&forward, (NODES as u64 / 2) * RECORD + RECORD - 1);
    let published = generation_uuid(&path);

    let forge = GraphForge::new(Some(path.to_str().unwrap())).unwrap();
    // Reads never touch the forward run.
    assert_eq!(rows(&forge, HEALTHY), Ok(3));
    assert_eq!(rows(&forge, ORDERED), Ok(3));
    // A commit that adds a node builds on it, and is refused.
    let refused = forge
        .execute("CREATE (:Entity)")
        .map(drop)
        .map_err(|error| format!("{} {error}", error.code()))
        .expect_err("a commit built on a flipped forward run must be refused");
    eprintln!("refused: {refused}");
    assert!(refused.starts_with("GF_"), "{refused}");
    assert!(
        refused.to_lowercase().contains("authenticat")
            || refused.to_lowercase().contains("checksum"),
        "refused for the wrong reason: {refused}"
    );
    assert_eq!(
        generation_uuid(&path),
        published,
        "a refused commit published a generation"
    );
}

/// Replace one byte of a content-store object in place (same inode, same
/// length, same mtime) and put it back on drop.
struct ByteSwap {
    object: PathBuf,
    offset: u64,
    original: u8,
}

impl ByteSwap {
    fn apply(object: &Path, offset: u64, replacement: impl FnOnce(u8) -> u8) -> Self {
        let original = write_byte(object, offset, replacement);
        Self {
            object: object.to_path_buf(),
            offset,
            original,
        }
    }
}

impl Drop for ByteSwap {
    fn drop(&mut self) {
        let original = self.original;
        write_byte(&self.object, self.offset, |_| original);
    }
}

fn identity(object: &Path) -> graphforge_filesystem::FileIdentity {
    graphforge_filesystem::file_identity(&std::fs::File::open(object).unwrap()).unwrap()
}

/// Write one byte through the shared inode, keeping its length and mtime;
/// returns the byte replaced.
#[allow(
    clippy::permissions_set_readonly_false,
    reason = "the content-store object is made writable for one byte and its permissions restored"
)]
fn write_byte(object: &Path, offset: u64, replacement: impl FnOnce(u8) -> u8) -> u8 {
    let before = std::fs::metadata(object).unwrap();
    let inode = identity(object);
    let permissions = before.permissions();
    let mut writable = permissions.clone();
    writable.set_readonly(false);
    std::fs::set_permissions(object, writable).unwrap();
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(object)
        .unwrap();
    let modified = file.metadata().unwrap().modified().unwrap();
    let mut byte = [0_u8; 1];
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.read_exact(&mut byte).unwrap();
    let replaced = replacement(byte[0]);
    assert_ne!(replaced, byte[0], "the swap must change the byte");
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&[replaced]).unwrap();
    file.set_modified(modified).unwrap();
    drop(file);
    std::fs::set_permissions(object, permissions).unwrap();
    assert_eq!(identity(object), inode, "same inode");
    let after = std::fs::metadata(object).unwrap();
    assert_eq!(after.len(), before.len(), "same length");
    assert_eq!(after.modified().unwrap(), modified, "same mtime");
    byte[0]
}

/// A byte whose change leaves the file well formed, so that only a checksum
/// can notice: in JSON, the last digit of the first 64-digit hex string (a
/// digest) becomes another hex digit; in a binary run, any byte.
fn inert_swap(relative: &str, bytes: &[u8]) -> (u64, fn(u8) -> u8) {
    fn other_hex_digit(byte: u8) -> u8 {
        if byte == b'0' { b'1' } else { b'0' }
    }
    if Path::new(relative)
        .extension()
        .is_none_or(|extension| extension != "json")
    {
        return ((bytes.len() / 2) as u64, |byte| byte ^ 0xff);
    }
    let digest = bytes
        .windows(64)
        .position(|window| window.iter().all(u8::is_ascii_hexdigit))
        .unwrap_or_else(|| panic!("{relative} carries no 64-digit hex digest"));
    ((digest + 63) as u64, other_hex_digit)
}

/// The copied ordinal controls of a project that has been mutated, so that a
/// non-empty tombstone run is published too.
#[test]
fn flipped_identity_controls_are_refused_by_the_open_that_copies_them() {
    let (_root, path) = project(2_048);
    {
        let forge = GraphForge::new(Some(path.to_str().unwrap())).unwrap();
        forge
            .execute("MATCH (n:Entity) WITH n LIMIT 3 DETACH DELETE n")
            .unwrap();
    }
    let objects = objects(&path);
    let control = |name: &str| {
        let relative = format!("topology/uuid-membership/{name}");
        objects
            .iter()
            .find(|(path, ..)| *path == relative)
            .unwrap_or_else(|| panic!("{relative} is not published"))
            .clone()
    };
    // The lock is published empty: there is no byte to flip, and nothing
    // reads its content.
    assert_eq!(control("ordinal-v4.lock").3, 0, "the lock carries bytes");
    let tombstones = objects
        .iter()
        .find(|(relative, .., length)| {
            relative.starts_with("topology/uuid-membership/tombstones-v4-") && *length > 0
        })
        .expect("a DELETE publishes a non-empty tombstone run")
        .clone();
    // The membership manifest and topology receipt are no longer published.
    for retired in ["manifest.json", "topology-receipt.json"] {
        let relative = format!("topology/uuid-membership/{retired}");
        assert!(
            objects.iter().all(|(path, ..)| *path != relative),
            "{relative} must not be published"
        );
    }
    let cases = [
        control("ordinal-v4-manifest.json"),
        control("ordinal-v4-receipt.json"),
        tombstones,
    ];
    let published = generation_uuid(&path);
    // Every class is tried before asserting, so one run reports each class
    // the open fails to refuse.
    let mut unrefused = Vec::new();
    for (relative, object, _, _) in &cases {
        let (offset, replacement) = inert_swap(relative, &std::fs::read(object).unwrap());
        let _swap = ByteSwap::apply(object, offset, replacement);
        match GraphForge::new(Some(path.to_str().unwrap())) {
            Ok(_) => unrefused.push(format!("{relative}: opened")),
            Err(error) if error.to_string().contains("do not match inventory") => {
                eprintln!("{relative}: refused: {} {error}", error.code());
            }
            Err(error) => unrefused.push(format!(
                "{relative}: refused for the wrong reason: {} {error}",
                error.code()
            )),
        }
    }
    assert!(
        unrefused.is_empty(),
        "the open that copies a flipped control must refuse it:\n{}",
        unrefused.join("\n")
    );
    // Every swap was restored: the project opens and answers again.
    assert_eq!(generation_uuid(&path), published);
    let forge = GraphForge::new(Some(path.to_str().unwrap())).unwrap();
    assert_eq!(rows(&forge, "MATCH (n) RETURN n"), Ok(2_048 - 3));
    assert_eq!(rows(&forge, ORDERED), Ok(3));
}

/// The commit that appends builds its identity probe over the published node
/// Parquet. The objects are hard-linked from the content store, so a flipped byte
/// is visible through the workspace; the probe's first touch checks the whole
/// object against its inventory checksum and refuses the commit before it names
/// a new digest.
#[test]
fn flipped_node_parquet_is_refused_by_the_commit_that_probes_it() {
    let (_root, path) = project(NODES);
    let (relative, object, _, length) = objects(&path)
        .into_iter()
        .find(|(relative, .., length)| {
            relative.starts_with("topology/nodes/") && relative.ends_with(".parquet") && *length > 0
        })
        .expect("a published node fragment");
    // Inside the UUID column pages, well clear of the footer a reader parses.
    let _swap = ByteSwap::apply(&object, length / 4, |byte| byte ^ 0x80);
    let published = generation_uuid(&path);

    let forge = GraphForge::new(Some(path.to_str().unwrap())).unwrap();
    let refused = forge
        .execute("CREATE (:Entity)")
        .map(drop)
        .map_err(|error| format!("{} {error}", error.code()))
        .expect_err("a commit probing a flipped node fragment must be refused");
    eprintln!("{relative}: refused: {refused}");
    assert!(
        refused.to_lowercase().contains("checksum")
            || refused.to_lowercase().contains("do not match")
            || refused.to_lowercase().contains("authenticat"),
        "refused for the wrong reason: {refused}"
    );
    drop(forge);
    assert_eq!(
        generation_uuid(&path),
        published,
        "a refused commit published a generation"
    );
}
