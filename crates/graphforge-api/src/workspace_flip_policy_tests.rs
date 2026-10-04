//! `GraphForge.read_only` must only leave `true` through `enable_writes`, which
//! refuses a pinned alias of a published generation's graph tree (#1709).
//!
//! Field selection, adoption and projection open private views over prepared
//! Branch content. That content's graph is always a compact root, so those views
//! can never be a pinned alias (see
//! `research_proposals::tests::pinned_workspace`), and they do write: a flip at
//! those sites would be inert today. It would also bypass the one transition
//! that checks the workspace, so a later change that let prepared content alias
//! a published tree would write it in place. These tests make the old
//! open-read-only-then-flip pattern fail at the source, per site.
use std::path::{Path, PathBuf};

fn source_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("src")
}

fn rust_files(directory: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(directory).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            out.push(path);
        }
    }
}

/// Test-only sources may build the invalid state on purpose to prove the guards.
fn is_test_source(path: &Path) -> bool {
    let stem = path.file_stem().unwrap().to_string_lossy();
    stem.ends_with("tests")
        || stem.ends_with("test_support")
        || path
            .components()
            .any(|component| component.as_os_str() == "tests")
}

/// Line numbers of `<expr>.read_only = ...` assignments, ignoring comments.
fn read_only_assignments(text: &str) -> Vec<usize> {
    const FIELD: &str = "read_only";
    let mut lines = Vec::new();
    for (index, line) in text.lines().enumerate() {
        let code = line.split("//").next().unwrap_or_default();
        let mut rest = code;
        while let Some(at) = rest.find(FIELD) {
            let after = rest[at + FIELD.len()..].trim_start();
            if rest[..at].ends_with('.') && after.starts_with('=') && !after.starts_with("==") {
                lines.push(index + 1);
            }
            rest = &rest[at + FIELD.len()..];
        }
    }
    lines
}

fn source(relative: &str) -> String {
    std::fs::read_to_string(source_root().join(relative))
        .unwrap_or_else(|error| panic!("cannot read {relative}: {error}"))
}

/// The detector must see the old pattern, or every assertion below is vacuous.
#[test]
fn known_positive_detector_flags_the_old_flip() {
    let old_flip =
        "let mut view = private_view::open(owner, &prepared)?;\n    view.read_only = false;\n";
    assert_eq!(read_only_assignments(old_flip), [2]);
    assert_eq!(
        read_only_assignments("graph.read_only=false;\nlet x = graph.read_only == false;\n"),
        [1]
    );
    assert!(read_only_assignments("// view.read_only = false\nlet read_only = true;\n").is_empty());
}

#[test]
fn only_enable_writes_may_leave_read_only_and_it_checks_the_workspace_first() {
    let mut files = Vec::new();
    rust_files(&source_root(), &mut files);
    assert!(files.len() > 100, "the scan must cover the crate's sources");
    let mut flips = Vec::new();
    for path in files.iter().filter(|path| !is_test_source(path)) {
        let text = std::fs::read_to_string(path).unwrap();
        for line in read_only_assignments(&text) {
            let relative = path.strip_prefix(source_root()).unwrap();
            flips.push((relative.display().to_string(), line));
        }
    }
    let files_with_flips: Vec<_> = flips.iter().map(|(path, _)| path.as_str()).collect();
    assert_eq!(
        files_with_flips,
        ["workspace_hydration.rs"],
        "a facade may only become writable through GraphForge::enable_writes; open it writable \
         (a private copy) instead of opening read-only and flipping: {flips:?}"
    );
    let hydration = source("workspace_hydration.rs");
    let normalized: String = hydration.split_whitespace().collect::<Vec<_>>().join(" ");
    assert!(
        normalized.contains(
            "fn enable_writes(&mut self) -> Result<(), GfError> { \
             self.require_private_workspace()?; self.read_only = false;"
        ),
        "enable_writes must refuse a pinned alias before it clears read_only"
    );
}

fn assert_site_opens_writable(relative: &str, opener: &str) {
    let text = source(relative);
    assert_eq!(
        read_only_assignments(&text),
        Vec::<usize>::new(),
        "{relative} flips read_only instead of opening writable"
    );
    assert!(
        text.contains(opener),
        "{relative} must open its writable facade with {opener}"
    );
}

#[test]
fn field_selection_opens_its_redaction_view_writable() {
    assert_site_opens_writable(
        "branches/field_selection.rs",
        "private_view::open_writable(owner, &prepared)",
    );
}

#[test]
fn adoption_opens_its_composition_view_writable() {
    assert_site_opens_writable(
        "research_upstream/adoption.rs",
        "private_view::open_writable(owner, &prepared)",
    );
}

#[test]
fn projection_opens_its_redaction_view_writable() {
    assert_site_opens_writable(
        "research_upstream/projection.rs",
        "private_view::open_writable(owner, &prepared)",
    );
}
