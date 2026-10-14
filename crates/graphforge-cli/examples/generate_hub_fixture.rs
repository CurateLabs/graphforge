//! Generate or check the deterministic Rust-owned Hub fixture artifacts.
//!
//! With no flags, check the checked-in artifacts for drift. `--update`
//! regenerates them; `--rebuild-source` rebuilds the synthetic source first.
//! `--location-base` and `--output-dir` select a public HTTPS preview fixture.

use clap::Parser;
use graphforge_cli::hub_fixture_artifacts::{
    DEFAULT_LOCATION_BASE, check, generate, rebuild_source, validate_location_base,
};
use std::path::{Path, PathBuf};

#[derive(Parser)]
#[command(about = "Generate or check the Rust-owned Hub fixture")]
struct Arguments {
    /// Regenerate the artifacts instead of checking them.
    #[arg(long)]
    update: bool,
    /// Rebuild the synthetic source through the public facade.
    #[arg(long)]
    rebuild_source: bool,
    /// Canonical public HTTPS base ending in / for digest-addressed objects.
    #[arg(long, default_value = DEFAULT_LOCATION_BASE)]
    location_base: String,
    /// Artifact directory (defaults to tests/fixtures/hub/generated/v1).
    #[arg(long)]
    output_dir: Option<PathBuf>,
}

// Resolve existing ancestors without creating anything inside the source tree.
fn resolve_output_path(path: &Path) -> Result<PathBuf, String> {
    match std::fs::canonicalize(path) {
        Ok(path) => Ok(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let parent = path
                .parent()
                .filter(|path| !path.as_os_str().is_empty())
                .unwrap_or(Path::new("."));
            Ok(resolve_output_path(parent)?
                .join(path.file_name().ok_or("output directory has no name")?))
        }
        Err(error) => Err(error.to_string()),
    }
}

fn regenerate(source: &Path, expected: &Path, location_base: &str) -> Result<(), String> {
    let parent = expected
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let output = resolve_output_path(expected)?;
    let canonical_source = std::fs::canonicalize(source).map_err(|error| error.to_string())?;
    if output.starts_with(&canonical_source) || canonical_source.starts_with(&output) {
        return Err("output directory must not overlap the fixture source".into());
    }
    std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    let staging = tempfile::Builder::new()
        .prefix(".hub-fixture-")
        .tempdir_in(parent)
        .map_err(|error| error.to_string())?;
    let staged = staging.path().join("generated");
    generate(source, &staged, location_base)?;
    let previous = staging.path().join("previous");
    let exists = expected.try_exists().map_err(|error| error.to_string())?;
    if exists {
        std::fs::rename(expected, &previous).map_err(|error| error.to_string())?;
    }
    if let Err(error) = std::fs::rename(&staged, expected) {
        if exists {
            restore_output(staging, expected).map_err(|restore| format!("{error}; {restore}"))?;
        }
        return Err(error.to_string());
    }
    Ok(())
}

// Keep the backup recoverable if another writer prevents restoration.
fn restore_output(staging: tempfile::TempDir, expected: &Path) -> Result<(), String> {
    let previous = staging.path().join("previous");
    if let Err(error) = std::fs::rename(&previous, expected) {
        let retained = staging.keep();
        return Err(format!(
            "restore failed: {error}; previous output preserved at {}",
            retained.join("previous").display()
        ));
    }
    Ok(())
}

fn run(arguments: Arguments) -> Result<(), String> {
    validate_location_base(&arguments.location_base)?;
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let source = root.join("tests/fixtures/hub/openalex-source");
    let expected = arguments
        .output_dir
        .unwrap_or_else(|| root.join("tests/fixtures/hub/generated/v1"));
    if arguments.rebuild_source {
        rebuild_source(&source)?;
    }
    if arguments.update {
        regenerate(&source, &expected, &arguments.location_base)
    } else if arguments.rebuild_source {
        Ok(())
    } else {
        check(&source, &expected, &arguments.location_base)
    }
}

fn main() {
    if let Err(error) = run(Arguments::parse()) {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn options_select_the_same_base_and_directory_in_both_modes() {
        let defaults = Arguments::try_parse_from(["generate_hub_fixture"]).unwrap();
        assert_eq!(defaults.location_base, DEFAULT_LOCATION_BASE);
        assert!(defaults.output_dir.is_none());
        assert!(!defaults.update);
        for update in [false, true] {
            let mut args = vec![
                "generate_hub_fixture",
                "--location-base",
                "https://preview.example.org/objects/",
                "--output-dir",
                "preview",
            ];
            if update {
                args.push("--update");
            }
            let parsed = Arguments::try_parse_from(args).unwrap();
            assert_eq!(parsed.location_base, "https://preview.example.org/objects/");
            assert_eq!(parsed.output_dir, Some(PathBuf::from("preview")));
            assert_eq!(parsed.update, update);
        }
        for args in [
            vec!["generate_hub_fixture", "--unknown"],
            vec!["generate_hub_fixture", "--location-base"],
            vec!["generate_hub_fixture", "--output-dir"],
        ] {
            assert!(Arguments::try_parse_from(args).is_err());
        }
    }

    #[test]
    fn output_replacement_cannot_remove_or_pollute_the_source() {
        let workspace = tempfile::tempdir().unwrap();
        let source = workspace.path().join("source");
        std::fs::create_dir(&source).unwrap();
        std::fs::write(source.join("sentinel"), b"preserved").unwrap();
        for output in [
            source.clone(),
            source.join("nested"),
            source.join("nested/deeper"),
            workspace.path().to_path_buf(),
        ] {
            let error = regenerate(&source, &output, DEFAULT_LOCATION_BASE).unwrap_err();
            assert!(error.contains("must not overlap"), "{error}");
            assert_eq!(
                std::fs::read(source.join("sentinel")).unwrap(),
                b"preserved"
            );
        }
        assert_eq!(std::fs::read_dir(&source).unwrap().count(), 1);
    }

    #[test]
    fn failed_restoration_preserves_the_backup_and_reports_its_path() {
        let workspace = tempfile::tempdir().unwrap();
        let expected = workspace.path().join("output");
        std::fs::create_dir(&expected).unwrap();
        std::fs::write(expected.join("another-writer"), b"new").unwrap();
        let staging = tempfile::tempdir_in(workspace.path()).unwrap();
        let staged_path = staging.path().to_path_buf();
        let previous = staging.path().join("previous");
        std::fs::create_dir(&previous).unwrap();
        std::fs::write(previous.join("sentinel"), b"old").unwrap();
        let error = restore_output(staging, &expected).unwrap_err();
        assert!(error.contains("restore failed"), "{error}");
        assert!(error.contains(previous.to_str().unwrap()), "{error}");
        assert_eq!(std::fs::read(previous.join("sentinel")).unwrap(), b"old");
        assert_eq!(
            std::fs::read(expected.join("another-writer")).unwrap(),
            b"new"
        );
        std::fs::remove_dir_all(staged_path).unwrap();
    }
}
