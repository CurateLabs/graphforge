//! A corrupted derived adjacency index is refused, never silently rebuilt
//! (#1388, acceptance criterion 4 and decision 4).
//!
//! The adjacency index (`indexes/adjacency/`) is derived and rebuildable, so a
//! missing or stale one is repaired by a rebuild. A same-inode, same-length
//! byte flip is neither: it is corruption of an object the project's manifest
//! authenticates. Rebuilding on it would turn a bounded query into an O(E)
//! edge-table scan and never report the damage, so the query that touches the
//! flipped object must refuse with `GF_VALIDATION` and must write nothing.
//!
//! Each case publishes a compact (V2) project exactly as construction ships it
//! (CSR shards, shard manifests and `index_manifest.parquet` included), flips
//! one byte of one content-addressed object in place, then opens the project
//! and runs a bounded query. The flip is before open on purpose: opening a
//! compact generation reads no payload, so the first touch is the query's.
//!
//! The assertion that matters is "no rebuild": the project must keep its
//! generation, and the files of the whole project must be byte-for-byte what
//! they were before the query ran.

use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};

use graphforge_api::{GraphForge, ResultSinkFormat, ResultSinkOptions};
use graphforge_exec::demand;
use graphforge_storage::{graph_object_path, resolve_project_generation};

#[path = "support/bulk_fixture.rs"]
mod bulk_fixture;

const NODES: usize = 1 << 11;
const FAN_OUT: usize = 4;

/// The ladder's ordered one-hop and two-hop. The ordered fast paths walk the
/// destination side of the union index, so they read its `.in` objects.
const ORDERED_ONE_HOP: &str = "MATCH (a)-[r]->(b) RETURN b.node_uuid AS id ORDER BY id LIMIT 1000";
const ORDERED_TWO_HOP: &str =
    "MATCH (a)-[r1]->(b)-[r2]->(c) RETURN c.node_uuid AS id ORDER BY id LIMIT 1000";
/// A reverse hop, which expands the `.in` objects.
const REVERSE_HOP: &str = "MATCH (a)<-[r]-(b) RETURN b.node_uuid AS id ORDER BY id LIMIT 1000";
/// Unordered bounded hops expand the `.out` objects.
const FORWARD_HOP: &str = "MATCH (a)-[r]->(b) RETURN b.node_uuid AS id LIMIT 1000";
const FORWARD_TWO_HOP: &str = "MATCH (a)-[r1]->(b)-[r2]->(c) RETURN c.node_uuid AS id LIMIT 1000";
/// UNION ALL coalesces multiple partitions on DataFusion worker tasks.
const FORWARD_UNION: &str = "MATCH (a)-[r]->(b) WITH b.node_uuid AS id LIMIT 1000 RETURN id UNION ALL MATCH (a)-[r]->(b) WITH b.node_uuid AS id LIMIT 1000 RETURN id";

const VARIABLE_LENGTH_UNION: &str = "MATCH (a)-[r*1..2]->(b) WITH b.node_uuid AS id LIMIT 1000 RETURN id UNION ALL MATCH (a)-[r*1..2]->(b) WITH b.node_uuid AS id LIMIT 1000 RETURN id";

/// Relative prefix of the files of the union (`_all`) index, whose names the
/// storage layer encodes.
fn union_prefix() -> String {
    let path = graphforge_storage::adjacency::csr_path(
        Path::new(""),
        graphforge_storage::adjacency::ALL_RELATIONS_STEM,
        graphforge_storage::adjacency::Direction::Out,
    );
    let name = path.file_name().unwrap().to_str().unwrap();
    format!(
        "indexes/adjacency/{}.",
        name.strip_suffix(".out.csr").expect("out shard name")
    )
}

/// The object of the derived index one case corrupts.
#[derive(Clone, Copy, Debug)]
enum Target {
    OutShard,
    InShard,
    OutShardManifest,
    InShardManifest,
    IndexManifest,
}

impl Target {
    fn selects(self, relative_path: &str) -> bool {
        if matches!(self, Self::IndexManifest) {
            return relative_path == "indexes/adjacency/index_manifest.parquet";
        }
        // The untyped patterns below are served by the `_all` union index.
        let union = relative_path.starts_with(&union_prefix());
        let shard = relative_path.ends_with(".csr");
        let shard_manifest = relative_path.ends_with(".csr.json");
        let outgoing = relative_path.contains(".out.csr");
        let incoming = relative_path.contains(".in.csr");
        union
            && match self {
                Self::OutShard => shard && outgoing,
                Self::InShard => shard && incoming,
                Self::OutShardManifest => shard_manifest && outgoing,
                Self::InShardManifest => shard_manifest && incoming,
                Self::IndexManifest => unreachable!(),
            }
    }
}

/// Every file under `root` with its length and a content hash.
fn snapshot(root: &Path) -> BTreeMap<PathBuf, (u64, u64)> {
    fn walk(path: &Path, files: &mut BTreeMap<PathBuf, (u64, u64)>) {
        for entry in std::fs::read_dir(path).unwrap().flatten() {
            let child = entry.path();
            // Query scratch space holds a per-process lock; it is not project
            // state, and a query that writes no project file still creates it.
            if child
                .file_name()
                .is_some_and(|name| name == ".graphforge-query-spill")
            {
                continue;
            }
            if child.is_dir() {
                walk(&child, files);
            } else if let Ok(bytes) = std::fs::read(&child) {
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                bytes.hash(&mut hasher);
                files.insert(child, (bytes.len() as u64, hasher.finish()));
            }
        }
    }
    let mut files = BTreeMap::new();
    walk(root, &mut files);
    files
}

/// Flip one byte of the content-addressed object in place: same inode, same
/// length.
fn flip_object(project: &Path, target: Target) -> String {
    let generation = resolve_project_generation(project).unwrap();
    let inventory = generation.graph_files_inventory().unwrap().unwrap();
    let entry = inventory
        .files
        .iter()
        .find(|entry| target.selects(&entry.relative_path))
        .unwrap_or_else(|| panic!("no published object for {target:?}"));
    let object = graph_object_path(generation.container_root(), &entry.content_sha256).unwrap();
    let before = std::fs::metadata(&object).unwrap();
    let mut permissions = before.permissions();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        permissions.set_mode(0o600);
    }
    #[cfg(not(unix))]
    permissions.set_readonly(false);
    std::fs::set_permissions(&object, permissions).unwrap();
    let mut bytes = std::fs::read(&object).unwrap();
    // A byte in the middle is inside the payload for every object kind.
    let at = bytes.len() / 2;
    bytes[at] ^= 1;
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&object)
        .unwrap();
    #[cfg(unix)]
    std::os::unix::fs::FileExt::write_all_at(&file, &bytes, 0).unwrap();
    #[cfg(not(unix))]
    {
        use std::io::Write as _;
        let mut file = file;
        file.write_all(&bytes).unwrap();
    }
    drop(file);
    let after = std::fs::metadata(&object).unwrap();
    assert_eq!(before.len(), after.len(), "same length");
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        assert_eq!(before.ino(), after.ino(), "same inode");
    }
    entry.relative_path.clone()
}

fn published_project() -> (tempfile::TempDir, String) {
    let directory = tempfile::TempDir::new().unwrap();
    // The index construction publishes, as a compact (V2) generation. An
    // explicit `index_adjacency` would republish an expanded tree.
    bulk_fixture::generate_bulk_graph_with_index(directory.path(), NODES, FAN_OUT, false);
    let path = directory.path().to_str().unwrap().to_owned();
    (directory, path)
}

fn rows(forge: &GraphForge, query: &str) -> usize {
    forge
        .execute(query)
        .unwrap()
        .batches
        .iter()
        .map(arrow::record_batch::RecordBatch::num_rows)
        .sum()
}

/// A flipped byte of `target`, bounded queries touching it: each is refused
/// with `GF_VALIDATION`, twice, with the generation and every file unchanged.
fn assert_refused(target: Target, queries: &[&str]) {
    let (directory, path) = published_project();
    let generation = resolve_project_generation(directory.path())
        .unwrap()
        .generation_uuid();
    // The healthy project answers every query, so the refusals below are the
    // corruption and not an empty or unsupported query.
    {
        let forge = GraphForge::new(Some(&path)).unwrap();
        for query in queries {
            assert!(rows(&forge, query) > 0, "{target:?}: {query}");
        }
    }
    let flipped = flip_object(directory.path(), target);
    let forge = GraphForge::new(Some(&path)).unwrap();
    let container = resolve_project_generation(directory.path())
        .unwrap()
        .container_root()
        .to_path_buf();
    let before = snapshot(&container);
    for query in queries {
        // The second touch must refuse the same way: the refusal is not
        // consumed by the first, and a retry does not find a repaired index.
        for touch in ["first", "second"] {
            let error = forge.execute(query).expect_err(&format!(
                "{target:?} ({flipped}) flip must refuse on the {touch} touch: {query}"
            ));
            assert_eq!(
                error.code(),
                "GF_VALIDATION",
                "{target:?} ({flipped}): {query}: {error}"
            );
        }
    }
    // Taken while the project is open: closing it removes its private
    // workspace, which is where a rebuild would have written.
    let after = snapshot(&container);
    drop(forge);
    let changed: Vec<_> = before
        .keys()
        .chain(after.keys())
        .filter(|path| before.get(*path) != after.get(*path))
        .collect();
    assert!(
        changed.is_empty(),
        "{target:?} ({flipped}): a refused query must write nothing, but changed {changed:#?}"
    );
    assert_eq!(
        resolve_project_generation(directory.path())
            .unwrap()
            .generation_uuid(),
        generation,
        "{target:?} ({flipped}): a refused query must not publish a generation"
    );
}

#[test]
fn flipped_out_shard_is_refused_by_a_forward_hop_and_two_hop() {
    assert_refused(Target::OutShard, &[FORWARD_HOP, FORWARD_TWO_HOP]);
}

#[test]
fn flipped_in_shard_is_refused_by_ordered_one_hop_and_two_hop() {
    assert_refused(
        Target::InShard,
        &[ORDERED_ONE_HOP, ORDERED_TWO_HOP, REVERSE_HOP],
    );
}

#[test]
fn flipped_out_shard_manifest_is_refused_by_a_forward_hop_and_two_hop() {
    assert_refused(Target::OutShardManifest, &[FORWARD_HOP, FORWARD_TWO_HOP]);
}

#[test]
fn flipped_in_shard_manifest_is_refused_by_ordered_one_hop_and_two_hop() {
    assert_refused(
        Target::InShardManifest,
        &[ORDERED_ONE_HOP, ORDERED_TWO_HOP, REVERSE_HOP],
    );
}

#[test]
fn flipped_index_manifest_is_refused_by_any_hop() {
    assert_refused(
        Target::IndexManifest,
        &[
            ORDERED_ONE_HOP,
            ORDERED_TWO_HOP,
            REVERSE_HOP,
            FORWARD_HOP,
            FORWARD_TWO_HOP,
        ],
    );
}

/// The refusal names the remedy that already exists: an explicit
/// `index_adjacency` rebuilds the derived index from the authenticated edges,
/// after which the same query answers.
#[test]
fn explicit_index_adjacency_replaces_a_corrupted_index() {
    for target in [
        Target::InShard,
        Target::InShardManifest,
        Target::IndexManifest,
    ] {
        let (directory, path) = published_project();
        flip_object(directory.path(), target);
        let forge = GraphForge::new(Some(&path)).unwrap();
        let error = forge.execute(ORDERED_ONE_HOP).unwrap_err();
        assert_eq!(error.code(), "GF_VALIDATION", "{target:?}: {error}");
        forge.index_adjacency().unwrap();
        assert!(rows(&forge, ORDERED_ONE_HOP) > 0, "{target:?}");
        assert!(rows(&forge, REVERSE_HOP) > 0, "{target:?}");
        drop(forge);

        // The repair is the published content-addressed object, not the
        // session's private rebuild: a fresh open hydrates CURRENT and every
        // object it names authenticates against its address.
        let generation = resolve_project_generation(directory.path()).unwrap();
        for entry in generation.graph_files_inventory().unwrap().unwrap().files {
            graphforge_storage::read_graph_object(
                generation.container_root(),
                &entry.content_sha256,
                entry.byte_length,
            )
            .unwrap_or_else(|error| {
                panic!("{target:?}: {} after repair: {error}", entry.relative_path)
            });
        }
        let reopened = GraphForge::new(Some(&path)).unwrap();
        assert!(rows(&reopened, ORDERED_ONE_HOP) > 0, "{target:?} reopened");
        assert!(rows(&reopened, REVERSE_HOP) > 0, "{target:?} reopened");
    }
}

/// Construction publishes a current index. Every bounded query explains a hit
/// and its public execution evidence records zero rebuilds.
#[test]
fn published_compact_generation_serves_its_index_without_rebuilding() {
    let (directory, path) = published_project();
    let forge = GraphForge::new(Some(&path)).unwrap();
    for (index, query) in [
        ORDERED_ONE_HOP,
        ORDERED_TWO_HOP,
        REVERSE_HOP,
        FORWARD_HOP,
        FORWARD_TWO_HOP,
    ]
    .into_iter()
    .enumerate()
    {
        let explanation = forge.explain(query).unwrap();
        assert!(
            explanation.contains("adjacency=hit"),
            "{query}: {explanation}"
        );
        assert!(
            !explanation.contains("adjacency_rebuild=stale"),
            "{query}: {explanation}"
        );
        let sink = directory.path().join(format!("query-{index}.arrow"));
        let receipt = forge
            .execute_to_result_sink_with_evidence(
                query,
                &Default::default(),
                sink.to_str().unwrap(),
                ResultSinkFormat::ArrowIpc,
                &ResultSinkOptions::default(),
                None,
            )
            .unwrap();
        assert!(receipt.sink.progress.rows > 0, "{query}");
        assert_eq!(receipt.evidence.adjacency_rebuilds, 0, "{query}");
    }
}

/// A delete honestly stales the index. Explain names the pending rebuild
/// without performing it; only the executing query pays and reports the work.
#[test]
fn a_stale_index_rebuild_is_reported_by_the_query_that_pays_for_it() {
    for query in [
        ORDERED_ONE_HOP,
        ORDERED_TWO_HOP,
        REVERSE_HOP,
        FORWARD_HOP,
        FORWARD_TWO_HOP,
    ] {
        let (directory, path) = published_project();
        let forge = GraphForge::new(Some(&path)).unwrap();
        forge
            .execute("MATCH (a)-[r]->(b) WITH r LIMIT 1 DELETE r")
            .unwrap();
        let (explanation, snapshot) = demand::capture(|| forge.explain(query));
        let explanation = explanation.unwrap();
        assert!(
            explanation.contains("adjacency_rebuild=stale"),
            "{query}: {explanation}"
        );
        assert_eq!(snapshot.adjacency_rebuilds, 0, "explain rebuilt: {query}");
        let sink = directory.path().join("query.arrow");
        let receipt = forge
            .execute_to_result_sink_with_evidence(
                query,
                &Default::default(),
                sink.to_str().unwrap(),
                ResultSinkFormat::ArrowIpc,
                &ResultSinkOptions::default(),
                None,
            )
            .unwrap();
        assert!(receipt.sink.progress.rows > 0, "{query}");
        assert_eq!(receipt.evidence.adjacency_rebuilds, 1, "{query}");
        let explanation = forge.explain(query).unwrap();
        assert!(
            !explanation.contains("adjacency_rebuild=stale"),
            "{query}: {explanation}"
        );
    }
}

#[test]
fn a_stale_index_rebuild_on_a_worker_is_reported_by_its_query() {
    for query in [FORWARD_UNION, VARIABLE_LENGTH_UNION] {
        let (directory, path) = published_project();
        let forge = GraphForge::new(Some(&path)).unwrap();
        forge
            .execute("MATCH (a)-[r]->(b) WITH r LIMIT 1 DELETE r")
            .unwrap();
        let explanation = forge.explain(query).unwrap();
        assert!(explanation.contains("UnionExec"), "{explanation}");
        if query == VARIABLE_LENGTH_UNION {
            assert!(explanation.contains("VarLenExpandExec"), "{explanation}");
        }
        assert!(
            explanation.contains("adjacency_rebuild=stale"),
            "{explanation}"
        );
        let sink = directory.path().join("worker-query.arrow");
        let receipt = forge
            .execute_to_result_sink_with_evidence(
                query,
                &Default::default(),
                sink.to_str().unwrap(),
                ResultSinkFormat::ArrowIpc,
                &ResultSinkOptions::default(),
                None,
            )
            .unwrap();
        assert_eq!(receipt.sink.progress.rows, 2000);
        assert_eq!(receipt.evidence.adjacency_rebuilds, 1);
    }
}
