//! Generate or check the deterministic Rust-owned Hub fixture artifacts.

use std::path::PathBuf;

fn main() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let source = root.join("tests/fixtures/hub/openalex-source");
    let expected = root.join("tests/fixtures/hub/generated/v1");
    let result = if std::env::args().any(|argument| argument == "--update") {
        std::fs::remove_dir_all(&expected)
            .or_else(|error| {
                (error.kind() == std::io::ErrorKind::NotFound)
                    .then_some(())
                    .ok_or(error)
            })
            .map_err(|error| error.to_string())
            .and_then(|()| graphforge_cli::hub_fixture_artifacts::generate(&source, &expected))
    } else {
        graphforge_cli::hub_fixture_artifacts::check(&source, &expected)
    };
    if let Err(error) = result {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
