//! Generate or check the deterministic Rust-owned Hub fixture artifacts.
//!
//! - no flag: check the checked-in artifacts for drift
//! - `--rebuild-source`: rebuild `openalex-source` through the public facade
//! - `--update`: regenerate `generated/v1` from the checked-in source
//!
//! The flags combine. After `--rebuild-source`, run `--update` because the
//! generated artifacts bind the source tree.

use graphforge_cli::hub_fixture_artifacts::{
    DEFAULT_LOCATION_BASE, check, generate, rebuild_source,
};
use std::path::PathBuf;

fn regenerate(source: &std::path::Path, expected: &std::path::Path) -> Result<(), String> {
    std::fs::remove_dir_all(expected)
        .or_else(|error| {
            (error.kind() == std::io::ErrorKind::NotFound)
                .then_some(())
                .ok_or(error)
        })
        .map_err(|error| error.to_string())
        .and_then(|()| generate(source, expected, DEFAULT_LOCATION_BASE))
}

fn main() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let source = root.join("tests/fixtures/hub/openalex-source");
    let expected = root.join("tests/fixtures/hub/generated/v1");
    let arguments: Vec<String> = std::env::args().collect();
    let has = |flag: &str| arguments.iter().any(|argument| argument == flag);
    let result = if has("--rebuild-source") || has("--update") {
        let rebuilt = if has("--rebuild-source") {
            rebuild_source(&source)
        } else {
            Ok(())
        };
        rebuilt.and_then(|()| {
            if has("--update") {
                regenerate(&source, &expected)
            } else {
                Ok(())
            }
        })
    } else {
        check(&source, &expected)
    };
    if let Err(error) = result {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
