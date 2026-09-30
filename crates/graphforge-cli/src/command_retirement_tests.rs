//! Coverage for retired standalone commands and preserved package verification.

use super::*;

#[test]
fn standalone_verify_is_retired_and_portable_verify_remains_available() {
    let error = match Cli::try_parse_from(["gf", "--project", "/tmp/project", "verify"]) {
        Ok(_) => panic!("retired standalone verify command must be rejected"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), clap::error::ErrorKind::InvalidSubcommand);
    let cli = Cli::try_parse_from([
        "gf",
        "portable",
        "verify",
        "--input",
        "/tmp/package.gfportable",
        "--mode",
        "full",
    ])
    .expect("portable package verification remains available");
    assert!(matches!(
        cli.command,
        Some(Command::Portable {
            command: portable_cli::PortableCommand::Verify(_)
        })
    ));
}
