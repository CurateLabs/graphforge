//! Command-line front end for `gdc_scorecard::query`.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use gdc_scorecard::query::{QueryCause, QueryError, run, sha256_hex};

pub const USAGE: &str = "graphforge-benchmark-gdc-scorecard query --project DIR --workload FILE --expected-counts FILE --output FILE";

fn read(path: &Path, cause: QueryCause) -> Result<Vec<u8>, QueryError> {
    std::fs::read(path)
        .map_err(|error| QueryError::new(cause, format!("{}: {error}", path.display())))
}

fn execute(
    project: &Path,
    workload: &Path,
    expected: &Path,
    output: &Path,
) -> Result<(), QueryError> {
    if output.exists() {
        return Err(QueryError::new(
            QueryCause::OutputExists,
            output.display().to_string(),
        ));
    }
    let workload = read(workload, QueryCause::InvalidWorkload)?;
    let expected = read(expected, QueryCause::InvalidExpectedCounts)?;
    let executable = std::env::current_exe()
        .and_then(std::fs::read)
        .map_err(|error| QueryError::new(QueryCause::Io, format!("driver executable: {error}")))?;
    let executable_sha256 = sha256_hex(&executable);
    let evidence = run(project, &workload, &expected, executable_sha256)?;
    let mut bytes = serde_json::to_vec_pretty(&evidence)
        .map_err(|error| QueryError::new(QueryCause::Io, error.to_string()))?;
    bytes.push(b'\n');
    let io = |error: std::io::Error| {
        QueryError::new(QueryCause::Io, format!("{}: {error}", output.display()))
    };
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output)
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                QueryError::new(QueryCause::OutputExists, output.display().to_string())
            } else {
                io(error)
            }
        })?;
    file.write_all(&bytes)
        .and_then(|()| file.sync_all())
        .map_err(io)
}

pub fn main(mut args: impl Iterator<Item = String>) -> ExitCode {
    let (mut project, mut workload, mut expected, mut output) = (None, None, None, None);
    while let Some(flag) = args.next() {
        let Some(value) = args.next() else {
            return super::usage();
        };
        let slot = match flag.as_str() {
            "--project" => &mut project,
            "--workload" => &mut workload,
            "--expected-counts" => &mut expected,
            "--output" => &mut output,
            _ => return super::usage(),
        };
        *slot = Some(PathBuf::from(value));
    }
    let (Some(project), Some(workload), Some(expected), Some(output)) =
        (project, workload, expected, output)
    else {
        return super::usage();
    };
    match execute(&project, &workload, &expected, &output) {
        Ok(()) => {
            println!("{}", output.display());
            ExitCode::SUCCESS
        }
        Err(error) => {
            super::report(error.cause().as_str(), error.message());
            ExitCode::from(2)
        }
    }
}
