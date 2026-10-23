//! Every `gf research` help text carries an example or a pointer (#2126).
//!
//! The research verbs take a native JSON request (`--file`); the plain-argument
//! workflow is the top-level porcelain (ADR 0061). Those that a porcelain verb
//! wraps show its example; every other one points at the porcelain and the
//! reference for its request.
use clap::{Command, CommandFactory as _};

const POINTER: &str = "This command takes a native JSON request; see docs/book/research/README.md.\nThe plain-argument workflow is gf status, gf log, gf commit, gf branch, gf switch, gf tag, gf push and gf pull (gf --help).";

fn help(path: &[&str]) -> String {
    match path.join(" ").as_str() {
        "research branch create" => "Example: gf branch feature/score\nThe plain-argument form of this native request.".to_owned(),
        "research branch execute" => "Example: gf commit -m \"Raise scores\" --query \"MATCH (n:Item) SET n.score = 2\"\nThe plain-argument form of this native request.".to_owned(),
        "research upstream update" => "Example: gf pull\nFetches the Hub head of the Branch's ref and incorporates every compatible change; this native request resolves the rest explicitly.".to_owned(),
        _ => POINTER.to_owned(),
    }
}

fn decorate(command: Command, path: &mut Vec<String>) -> Command {
    let names: Vec<String> = command
        .get_subcommands()
        .map(|sub| sub.get_name().to_owned())
        .filter(|name| name != "help")
        .collect();
    let text = help(&path.iter().map(String::as_str).collect::<Vec<_>>());
    let mut command = command.after_help(text);
    for name in names {
        path.push(name.clone());
        command = command.mut_subcommand(name, |sub| decorate(sub, path));
        path.pop();
    }
    command
}

/// The CLI definition with the research help text attached.
pub(crate) fn command() -> Command {
    crate::Cli::command().mut_subcommand("research", |research| {
        decorate(research, &mut vec!["research".to_owned()])
    })
}
