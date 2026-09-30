//! Ephemeral normalization of the vendored openCypher TCK corpus.
//!
//! Shared by the Cucumber runner (`tests/bdd/main.rs`) and the in-process Divan
//! TCK benchmark (`benches/tck_scenarios/`), so both execute the same scenario
//! set from the same normalized feature text.

/// The scenario key used by the passing baseline and every per-scenario
/// report: `<feature-name>:<line>:<scenario-name>`.
///
/// Deliberately keyed by FEATURE NAME (unique per TCK file), not the file
/// path: the normalized corpus lives under a temp dir whose canonicalization
/// differs by platform (macOS `/private` symlinks), which would make keys, and
/// thus the baseline, non-portable between local and CI.
pub fn scenario_key(feature_name: &str, line: usize, scenario_name: &str) -> String {
    format!("{feature_name}:{line}:{scenario_name}")
}

/// The `TCK_ONLY` local-iteration substring filter, or `None` if unset/empty.
/// An empty value is treated as unset so a stray `TCK_ONLY=` can't silently
/// bypass the baseline gate (`contains("")` is always true).
pub fn tck_only_filter() -> Option<String> {
    std::env::var("TCK_ONLY")
        .ok()
        .filter(|s| !s.trim().is_empty())
}

/// Recursively copy the vendored TCK feature tree from `src` into `dst`, rewriting
/// only block-leading `And`/`But` continuation keywords to `Given` (see
/// [`normalize_leading_continuations`]). The vendored source files are never modified.
pub fn copy_features_normalized(src: &std::path::Path, dst: &std::path::Path) {
    for entry in std::fs::read_dir(src).expect("read TCK feature dir") {
        let path = entry.expect("dir entry").path();
        let target = dst.join(path.file_name().expect("entry file name"));
        if path.is_dir() {
            std::fs::create_dir_all(&target).expect("create temp subdir");
            copy_features_normalized(&path, &target);
        } else if path.extension().is_some_and(|e| e == "feature") {
            // Local iteration: `TCK_ONLY=<substr>` restricts the corpus to
            // feature files whose path contains `<substr>` (e.g.
            // `TCK_ONLY=Temporal`) for a fast subset run. Unset in CI → the
            // whole corpus. The baseline gate is skipped when set (see `main`),
            // since a subset can't satisfy the whole-corpus baseline.
            if let Some(filter) = tck_only_filter()
                && !path.to_string_lossy().contains(&filter)
            {
                continue;
            }
            let content = std::fs::read_to_string(&path).expect("read feature file");
            std::fs::write(&target, normalize_leading_continuations(&content))
                .expect("write temp feature");
        }
    }
}

/// Rewrite a step that is the FIRST step of its `Scenario`/`Scenario Outline`/
/// `Background`/`Rule`/`Example` block and uses the `And`/`But` continuation keyword
/// into `Given`. The Rust `gherkin` parser rejects a block-leading `And`/`But`, while
/// cucumber-js accepts it; semantics are unchanged (the continuation inherits `Given`).
/// Only block-leading steps are touched — `And`/`But` after a concrete step are left as-is.
pub fn normalize_leading_continuations(content: &str) -> String {
    let mut out = String::with_capacity(content.len() + 64);
    let mut awaiting_first_step = false;
    for line in content.lines() {
        let trimmed = line.trim_start();
        let is_header = trimmed.starts_with("Scenario:")
            || trimmed.starts_with("Scenario Outline:")
            || trimmed.starts_with("Background:")
            || trimmed.starts_with("Rule:")
            || trimmed.starts_with("Example:");
        let is_step = ["Given ", "When ", "Then ", "And ", "But "]
            .iter()
            .any(|kw| trimmed.starts_with(kw));
        if is_header {
            awaiting_first_step = true;
            out.push_str(line);
        } else if awaiting_first_step
            && (trimmed.starts_with("And ") || trimmed.starts_with("But "))
        {
            let indent = &line[..line.len() - trimmed.len()];
            let rest = trimmed
                .strip_prefix("And ")
                .or_else(|| trimmed.strip_prefix("But "))
                .expect("And/But prefix present");
            out.push_str(indent);
            out.push_str("Given ");
            out.push_str(rest);
            awaiting_first_step = false;
        } else {
            if is_step {
                awaiting_first_step = false;
            }
            out.push_str(line);
        }
        out.push('\n');
    }
    out
}
