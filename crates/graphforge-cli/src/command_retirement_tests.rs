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
        "/tmp/package.gfpb",
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

fn research_help_texts(command: &clap::Command, found: &mut Vec<(String, String)>) {
    let mut text = command.clone();
    found.push((
        command.get_name().to_owned(),
        text.render_long_help().to_string(),
    ));
    for sub in command
        .get_subcommands()
        .filter(|sub| sub.get_name() != "help")
    {
        research_help_texts(sub, found);
    }
}

#[test]
fn every_research_help_text_has_an_example_or_pointer() {
    let command = research_help::command();
    let research = command.find_subcommand("research").expect("research");
    let mut found = Vec::new();
    research_help_texts(research, &mut found);
    assert!(found.len() >= 60, "walked {} commands", found.len());
    for (name, help) in &found {
        assert!(
            help.contains("Example:") || help.contains("gf --help"),
            "gf research ... {name} has neither an example nor a pointer:\n{help}"
        );
        assert!(
            !help.contains("Execute the native"),
            "gf research ... {name} describes an internal request instead of its effect:\n{help}"
        );
    }
    let branch = found
        .iter()
        .find(|(name, _)| name == "execute")
        .expect("branch execute");
    assert!(branch.1.contains("gf commit -m"), "{}", branch.1);
}
