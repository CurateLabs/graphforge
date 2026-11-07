//! Console execution with inherited OS streams and invocation-scoped state.
use crate::{
    Diagnostics, argument_error_execution, error_exit_code, parse_cli, requests_json, run,
    write_runtime_error,
};
use std::io::{self, Write};

thread_local! {
    // Invocation-local: a streaming call must not enable vault access, browser
    // prompts or clone progress for later captured calls in the host.
    static PROCESS_ATTACHED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

pub(crate) fn process_attached() -> bool {
    PROCESS_ATTACHED.get()
}

/// Prompts require an attached console with both input and diagnostics on terminals.
pub(crate) fn interactive_process() -> bool {
    use std::io::IsTerminal as _;
    process_attached() && io::stdin().is_terminal() && io::stderr().is_terminal()
}

struct ProcessAttachment(bool);

impl Drop for ProcessAttachment {
    fn drop(&mut self) {
        PROCESS_ATTACHED.set(self.0);
    }
}

/// Execute with the actual process stdin, stdout and stderr, returning its status.
///
/// Intended for console launchers: notices stream immediately and terminal
/// detection uses the OS descriptors. This never exits the embedding host or
/// installs signal handlers; the host retains signal ownership.
/// Captured callers should continue to use [`crate::execute`].
pub fn execute_process<I, T>(args: I) -> i32
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
{
    let args: Vec<std::ffi::OsString> = args.into_iter().map(Into::into).collect();
    let cli = match parse_cli(&args) {
        Ok(cli) => cli,
        Err(error) if !requests_json(&args) || !error.use_stderr() => {
            let _ = error.print();
            return error.exit_code();
        }
        Err(error) => {
            let refused = argument_error_execution(&error, true);
            let _ = io::stderr().write_all(&refused.stderr);
            return refused.exit_code;
        }
    };
    let _attachment = ProcessAttachment(PROCESS_ATTACHED.replace(true));
    let json = cli.json;
    let mut output = io::stdout().lock();
    let mut stderr = io::stderr();
    let mut diagnostics = Diagnostics::new(json, &mut stderr);
    let result = run(cli, &mut output, &mut diagnostics);
    let warnings = diagnostics.collected();
    drop(diagnostics);
    let _ = output.flush();
    match result {
        Ok(exit_code) => exit_code,
        Err(error) => {
            let _ = write_runtime_error(&error, json, &mut stderr.lock(), &warnings);
            error_exit_code(&error)
        }
    }
}

/// Stream process output directly and terminate with the native CLI exit status.
pub fn run_process() {
    crate::hub_clone::own_process_signals();
    std::process::exit(execute_process(std::env::args_os()));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execute;

    #[test]
    fn process_attachment_is_thread_local_and_restored_after_failure() {
        assert!(!process_attached());
        {
            let _attachment = ProcessAttachment(PROCESS_ATTACHED.replace(true));
            assert!(process_attached());
            std::thread::spawn(|| assert!(!process_attached()))
                .join()
                .unwrap();
        }
        assert!(!process_attached());
        let root = tempfile::tempdir().unwrap();
        let status = execute_process([
            "gf",
            "--project-dir",
            root.path().join("missing").to_str().unwrap(),
            "config",
            "validate",
        ]);
        assert_eq!(status, 3);
        assert!(!process_attached());
        assert_eq!(execute(["gf", "--version"]).exit_code, 0);
    }
}
