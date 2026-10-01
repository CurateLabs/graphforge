//! A pinned graph workspace aliases a published generation's own `graph/` tree
//! and must never be written (#1709). These tests hold published trees to
//! their recorded inventory, so any in-place write is observed directly.
use crate::GraphForge;
use graphforge_storage::{
    GRAPH_CAPABILITY_ID, GRAPH_FILES_CHECKSUM_ROOT_RECORD_VERSION, GRAPH_FILES_FAMILY,
    GRAPH_FILES_MAPPED_CHECKSUM_ROOT_RECORD_VERSION, ResolvedProjectGeneration,
    filesystem_admission::ProjectLifecycleMode,
};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use uuid::Uuid;

/// Exact identity of one file: bytes, length, modification time and inode.
/// Content hashing alone cannot see a rewrite that preserves the bytes.
#[derive(Debug, PartialEq, Eq)]
struct FileStamp {
    bytes: Vec<u8>,
    len: u64,
    modified: SystemTime,
    #[cfg(unix)]
    inode: u64,
}

/// Every regular file beneath one tree, keyed by relative path.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct TreeStamp(BTreeMap<PathBuf, FileStamp>);

impl TreeStamp {
    pub(crate) fn capture(root: &Path) -> Self {
        fn visit(root: &Path, directory: &Path, out: &mut BTreeMap<PathBuf, FileStamp>) {
            for entry in std::fs::read_dir(directory).unwrap() {
                let path = entry.unwrap().path();
                let metadata = std::fs::symlink_metadata(&path).unwrap();
                if metadata.is_dir() {
                    visit(root, &path, out);
                } else {
                    #[cfg(unix)]
                    let inode = std::os::unix::fs::MetadataExt::ino(&metadata);
                    out.insert(
                        path.strip_prefix(root).unwrap().to_path_buf(),
                        FileStamp {
                            bytes: std::fs::read(&path).unwrap(),
                            len: metadata.len(),
                            modified: metadata.modified().unwrap(),
                            #[cfg(unix)]
                            inode,
                        },
                    );
                }
            }
        }
        let mut files = BTreeMap::new();
        // An empty initial generation has no graph tree at all.
        if root.exists() {
            visit(root, root, &mut files);
        }
        Self(files)
    }

    /// Paths added, removed or changed relative to `self`, each with its kind.
    fn differences(&self, after: &Self) -> Vec<String> {
        let mut out = Vec::new();
        for (path, before) in &self.0 {
            match after.0.get(path) {
                None => out.push(format!("REMOVED {}", path.display())),
                Some(now) if now != before => out.push(format!("CHANGED {}", path.display())),
                Some(_) => {}
            }
        }
        for path in after.0.keys().filter(|path| !self.0.contains_key(*path)) {
            out.push(format!("ADDED {}", path.display()));
        }
        out
    }
}

/// Everything wrong with `generation`'s own graph tree: any file differing from
/// `before`, plus any disagreement with the recorded `graph/files` inventory.
pub(crate) fn tree_drift(
    generation: &ResolvedProjectGeneration,
    before: &TreeStamp,
) -> Vec<String> {
    let tree = generation.graph_tree_root();
    let mut drift = before.differences(&TreeStamp::capture(&tree));
    let Some(snapshot) = generation
        .participant_snapshot(GRAPH_CAPABILITY_ID, GRAPH_FILES_FAMILY)
        .unwrap()
    else {
        // No recorded inventory means no graph files: any file is drift.
        return drift;
    };
    assert!(
        !matches!(
            snapshot.record_version,
            GRAPH_FILES_CHECKSUM_ROOT_RECORD_VERSION
                | GRAPH_FILES_MAPPED_CHECKSUM_ROOT_RECORD_VERSION
        ),
        "a compact graph root has no generation tree to drift; this test would be vacuous"
    );
    let inventory = graphforge_storage::decode_inventory(&snapshot.bytes).unwrap();
    assert!(
        !inventory.files.is_empty(),
        "the tree must hold graph files"
    );
    if let Err(error) = graphforge_storage::verify_graph_tree(&tree, &inventory) {
        drift.push(format!("INVENTORY {error}"));
    }
    drift
}

/// Every published generation of the project, each with its current stamp.
pub(crate) fn published_generations(root: &Path) -> Vec<(ResolvedProjectGeneration, TreeStamp)> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(root.join("generations")).unwrap() {
        let entry = entry.unwrap();
        let uuid = Uuid::parse_str(entry.file_name().to_str().unwrap()).unwrap();
        let generation = graphforge_storage::resolve_generation_by_uuid(root, uuid).unwrap();
        let stamp = TreeStamp::capture(&generation.graph_tree_root());
        out.push((generation, stamp));
    }
    assert!(!out.is_empty());
    out
}

/// Assert none of `before`'s published trees changed, as bytes, inodes or inventory.
pub(crate) fn assert_published_trees_untouched(
    before: &[(ResolvedProjectGeneration, TreeStamp)],
    what: &str,
) {
    for (generation, stamp) in before {
        let drift = tree_drift(generation, stamp);
        assert!(
            drift.is_empty(),
            "{what} modified published generation {} in place: {drift:?}",
            generation.generation_uuid()
        );
    }
}

#[cfg(feature = "research")]
/// Whether the generation's graph participant is a compact root. A compact root
/// always hydrates into a private workspace; only a generation tree can be aliased.
pub(crate) fn has_compact_graph_root(generation: &ResolvedProjectGeneration) -> bool {
    let snapshot = generation
        .participant_snapshot(GRAPH_CAPABILITY_ID, GRAPH_FILES_FAMILY)
        .unwrap()
        .expect("the generation records a graph files participant");
    matches!(
        snapshot.record_version,
        GRAPH_FILES_CHECKSUM_ROOT_RECORD_VERSION | GRAPH_FILES_MAPPED_CHECKSUM_ROOT_RECORD_VERSION
    )
}

#[cfg(feature = "research")]
/// The owner project's graph is a generation tree, so a pinned alias of it is
/// possible; a test over a compact-root owner could not exhibit the hazard.
pub(crate) fn assert_tree_backed_owner(graph: &GraphForge) {
    assert!(
        !has_compact_graph_root(&graph.generation_for_read().unwrap()),
        "the owner must be tree-backed, otherwise a pinned alias cannot arise and the test proves nothing"
    );
}

fn durable_project() -> (tempfile::TempDir, GraphForge) {
    let directory = tempfile::tempdir().unwrap();
    let graph = GraphForge::new(directory.path().join("project").to_str()).unwrap();
    graph.execute("CREATE (:Person {name:'original'})").unwrap();
    (directory, graph)
}

fn open_pinned(graph: &GraphForge) -> (GraphForge, ResolvedProjectGeneration) {
    let generation = graph.generation_for_read().unwrap();
    let view = GraphForge::open_resolved_with_lifecycle_mode(
        generation.container_root().to_path_buf(),
        generation.clone(),
        true,
        ProjectLifecycleMode::Durable,
    )
    .unwrap();
    assert_eq!(
        view.dir().path(),
        generation.graph_tree_root(),
        "a read-only open must alias the published tree; otherwise this test proves nothing"
    );
    (view, generation)
}

fn first_file(generation: &ResolvedProjectGeneration, directory: &str) -> PathBuf {
    fn collect(directory: &Path, out: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                collect(&path, out);
            } else {
                out.push(path);
            }
        }
    }
    let mut files = Vec::new();
    collect(&generation.graph_tree_root().join(directory), &mut files);
    files.sort();
    files.into_iter().next().expect("tree holds a file here")
}

/// The detector is validated against known positives, not only a clean tree.
#[test]
fn known_positive_detector_flags_flipped_and_appended_bytes() {
    let (_directory, graph) = durable_project();
    let generation = graph.generation_for_read().unwrap();
    let pristine = TreeStamp::capture(&generation.graph_tree_root());
    assert_eq!(tree_drift(&generation, &pristine), Vec::<String>::new());

    let flipped = first_file(&generation, "properties");
    let mut bytes = std::fs::read(&flipped).unwrap();
    *bytes.last_mut().unwrap() ^= 0xff;
    std::fs::write(&flipped, &bytes).unwrap();
    let drift = tree_drift(&generation, &pristine);
    assert!(
        drift.iter().any(|line| line.starts_with("CHANGED")),
        "{drift:?}"
    );
    assert!(
        drift.iter().any(|line| line.starts_with("INVENTORY")),
        "{drift:?}"
    );

    // A fresh project, so the appended byte is the only difference.
    let (_other_directory, other) = durable_project();
    let other_generation = other.generation_for_read().unwrap();
    let other_pristine = TreeStamp::capture(&other_generation.graph_tree_root());
    let appended = first_file(&other_generation, "properties");
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&appended)
        .unwrap();
    std::io::Write::write_all(&mut file, b"\0").unwrap();
    drop(file);
    let drift = tree_drift(&other_generation, &other_pristine);
    assert!(
        drift.iter().any(|line| line.starts_with("CHANGED")),
        "{drift:?}"
    );
    assert!(
        drift.iter().any(|line| line.starts_with("INVENTORY")),
        "{drift:?}"
    );
}

#[test]
fn write_over_a_pinned_workspace_fails_closed_before_touching_the_published_tree() {
    let (_directory, graph) = durable_project();
    let (mut view, generation) = open_pinned(&graph);
    let before = TreeStamp::capture(&generation.graph_tree_root());

    // The invalid state: a writable facade whose workspace is the pinned alias.
    view.read_only = false;
    let error = view
        .execute("CREATE (:Person {name:'forbidden'})")
        .expect_err("a pinned workspace must refuse a write");

    assert_eq!(error.code(), "GF_READ_ONLY_VIEW", "{error}");
    assert!(error.to_string().contains("pinned"), "{error}");
    assert_eq!(tree_drift(&generation, &before), Vec::<String>::new());
    assert_eq!(
        graphforge_storage::resolve_project_generation(generation.container_root())
            .unwrap()
            .generation_uuid(),
        generation.generation_uuid(),
        "the refused write must not publish a generation"
    );
}

#[test]
fn enabling_writes_on_a_pinned_workspace_is_refused_and_a_private_copy_is_allowed() {
    let (_directory, graph) = durable_project();
    let (mut view, generation) = open_pinned(&graph);
    assert!(view.dir().is_pinned_alias());
    let error = view
        .enable_writes()
        .expect_err("a pinned alias cannot be made writable");
    assert_eq!(error.code(), "GF_READ_ONLY_VIEW", "{error}");
    assert!(
        view.read_only,
        "a refused transition must leave the facade read-only"
    );

    let mut private = GraphForge::open_resolved_with_access(
        generation.container_root().to_path_buf(),
        generation.clone(),
        crate::workspace_hydration::WorkspaceAccess::PrivateReadOnly,
        crate::GraphForgeOptions::default(),
        crate::GraphForgeOptions::default().validate().unwrap().1,
        graphforge_storage::ProjectOpenRecoveryEvidence::checkpoint_view(
            generation.generation_uuid(),
        ),
    )
    .unwrap();
    assert!(!private.dir().is_pinned_alias());
    assert_ne!(private.dir().path(), generation.graph_tree_root());
    private.enable_writes().unwrap();
    assert!(!private.read_only);
}

#[test]
fn publication_over_a_pinned_workspace_fails_closed() {
    let (_directory, graph) = durable_project();
    let (mut view, generation) = open_pinned(&graph);
    let before = TreeStamp::capture(&generation.graph_tree_root());
    view.read_only = false;

    let receipt = graphforge_exec::MutationReceipt::default();
    let error = view
        .publish_graph_mutation(&receipt)
        .expect_err("publishing from a pinned workspace must be refused");
    assert_eq!(error.code(), "GF_READ_ONLY_VIEW", "{error}");
    assert!(error.to_string().contains("pinned"), "{error}");
    let error = view
        .publish_graph_mutation_with_generation(
            &receipt,
            Uuid::now_v7(),
            Uuid::now_v7(),
            generation.generation_uuid(),
            1,
        )
        .expect_err("generation-scoped publication must be refused too");
    assert!(error.to_string().contains("pinned"), "{error}");
    assert_eq!(tree_drift(&generation, &before), Vec::<String>::new());
}

#[test]
fn a_private_writable_open_is_not_pinned_and_publishes_without_touching_the_parent() {
    let (_directory, graph) = durable_project();
    let parent = graph.generation_for_read().unwrap();
    let before = published_generations(parent.container_root());

    graph.execute("CREATE (:Person {name:'added'})").unwrap();

    assert_ne!(graph.dir().path(), parent.graph_tree_root());
    assert_published_trees_untouched(&before, "an ordinary writable facade");
}
