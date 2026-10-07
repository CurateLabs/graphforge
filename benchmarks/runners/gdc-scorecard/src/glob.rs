//! Mapping file patterns. A pattern is a relative path whose components may
//! use `*` (any run of characters) and `?` (one character); neither crosses a
//! `/`, and neither matches a leading `.`, so Hadoop `.crc` side files and
//! hidden files stay out unless a component names them. Matches are returned
//! in byte order of their relative path, which fixes the row order, and so
//! the edge ordinals, of a table read from many part files.

use std::fs;
use std::path::Path;

use crate::error::{Cause, ConvertError, io_error};

fn has_wildcard(text: &str) -> bool {
    text.contains(['*', '?'])
}

/// Whether `name` matches the single-component `pattern`.
fn matches(pattern: &[char], name: &[char]) -> bool {
    match pattern.split_first() {
        None => name.is_empty(),
        Some(('*', rest)) => (0..=name.len()).any(|skip| matches(rest, &name[skip..])),
        Some(('?', rest)) => !name.is_empty() && matches(rest, &name[1..]),
        Some((literal, rest)) => name.first() == Some(literal) && matches(rest, &name[1..]),
    }
}

fn component_matches(pattern: &str, name: &str) -> bool {
    if name.starts_with('.') && !pattern.starts_with('.') {
        return false;
    }
    let pattern: Vec<char> = pattern.chars().collect();
    let name: Vec<char> = name.chars().collect();
    matches(&pattern, &name)
}

/// The sorted names in `root/relative` matching `pattern` that are
/// directories (`want_dir`) or regular files.
fn matching_entries(
    root: &Path,
    relative: &str,
    pattern: &str,
    want_dir: bool,
) -> Result<Vec<String>, ConvertError> {
    let directory = root.join(relative);
    let entries = match fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(io_error(&directory.display().to_string(), &error)),
    };
    let mut names = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| io_error(&directory.display().to_string(), &error))?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            return Err(ConvertError::new(
                Cause::MalformedInput,
                format!(
                    "{}: a file name is not UTF-8, so pattern {pattern} cannot be decided",
                    directory.display()
                ),
            ));
        };
        if !component_matches(pattern, &name) {
            continue;
        }
        let metadata = fs::metadata(entry.path())
            .map_err(|error| io_error(&entry.path().display().to_string(), &error))?;
        if (want_dir && metadata.is_dir()) || (!want_dir && metadata.is_file()) {
            names.push(name);
        }
    }
    names.sort_unstable();
    Ok(names)
}

/// Expands one mapping file entry against `root`.
///
/// An entry without wildcards is returned unchanged, so a missing literal file
/// is reported by the reader as `input_missing`.
///
/// # Errors
/// `input_missing` when a pattern matches no file; `malformed_input` for a
/// non-UTF-8 file name where a wildcard must decide; `io_error` otherwise.
pub fn expand(root: &Path, pattern: &str) -> Result<Vec<String>, ConvertError> {
    if !has_wildcard(pattern) {
        return Ok(vec![pattern.to_owned()]);
    }
    let components: Vec<&str> = pattern.split('/').filter(|part| !part.is_empty()).collect();
    let mut candidates = vec![String::new()];
    for (index, component) in components.iter().enumerate() {
        let last = index + 1 == components.len();
        let mut next = Vec::new();
        for relative in &candidates {
            let join = |name: &str| {
                if relative.is_empty() {
                    name.to_owned()
                } else {
                    format!("{relative}/{name}")
                }
            };
            if has_wildcard(component) {
                for name in matching_entries(root, relative, component, !last)? {
                    next.push(join(&name));
                }
            } else {
                let path = join(component);
                let metadata = fs::metadata(root.join(&path));
                let keep = match metadata {
                    Ok(metadata) => (last && metadata.is_file()) || (!last && metadata.is_dir()),
                    Err(_) => false,
                };
                if keep {
                    next.push(path);
                }
            }
        }
        candidates = next;
    }
    if candidates.is_empty() {
        return Err(ConvertError::new(
            Cause::InputMissing,
            format!("{}: pattern {pattern} matched no file", root.display()),
        ));
    }
    candidates.sort_unstable();
    Ok(candidates)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn matches_text(pattern: &str, name: &str) -> bool {
        component_matches(pattern, name)
    }

    #[test]
    fn wildcards_match_within_one_component() {
        assert!(matches_text("*.csv.gz", "part-00000-abc-c000.csv.gz"));
        assert!(matches_text("part-?????-*.csv.gz", "part-00012-x.csv.gz"));
        assert!(!matches_text("part-?????-*.csv.gz", "part-0001-x.csv.gz"));
        assert!(!matches_text("*.csv.gz", "_SUCCESS"));
        assert!(!matches_text("*.csv", "part.csv.crc"));
        assert!(!matches_text("*", ".part-00000.csv.gz.crc"));
        assert!(matches_text(".*.crc", ".part.crc"));
        assert!(matches_text("person_0_0.csv", "person_0_0.csv"));
        assert!(matches_text("*_0_0.csv", "person_knows_person_0_0.csv"));
        assert!(matches_text("é*", "éa"));
    }

    #[test]
    fn expansion_is_sorted_typed_and_restricted_to_files() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        for path in [
            "b/Person/part-00001.csv.gz",
            "b/Person/part-00000.csv.gz",
            "b/Person/_SUCCESS",
            "b/Person/.part-00000.csv.gz.crc",
            "a/Person/part-00000.csv.gz",
            "a/Forum/part-00000.csv.gz",
        ] {
            let path = root.join(path);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, b"x").unwrap();
        }
        fs::create_dir_all(root.join("a/Person/part-dir.csv.gz")).unwrap();
        assert_eq!(
            expand(root, "*/Person/*.csv.gz").unwrap(),
            [
                "a/Person/part-00000.csv.gz",
                "b/Person/part-00000.csv.gz",
                "b/Person/part-00001.csv.gz",
            ]
        );
        assert_eq!(
            expand(root, "b/Person/_SUCCESS").unwrap(),
            ["b/Person/_SUCCESS"]
        );
        assert_eq!(
            expand(root, "missing.csv").unwrap(),
            ["missing.csv"],
            "a literal is left for the reader to report"
        );
        let error = expand(root, "*/Comment/*.csv.gz").unwrap_err();
        assert_eq!(error.cause(), Cause::InputMissing);
        let error = expand(root, "a/Person/*.csv").unwrap_err();
        assert_eq!(error.cause(), Cause::InputMissing);
    }
}
