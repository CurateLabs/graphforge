//! Command-line front end for `gdc_scorecard::query`.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use gdc_scorecard::query::{
    Isolation, QueryCause, QueryError, ResultsDir, run_isolated, run_variant_child,
};
use sha2::{Digest, Sha256};

pub const USAGE: &str = "graphforge-benchmark-gdc-scorecard query --project DIR --workload FILE --expected-counts FILE --output FILE --query-timeout-seconds N [--results-dir EMPTY_DIR]";

fn read(path: &Path, cause: QueryCause) -> Result<Vec<u8>, QueryError> {
    std::fs::read(path)
        .map_err(|error| QueryError::new(cause, format!("{}: {error}", path.display())))
}

struct Paths {
    project: PathBuf,
    workload: PathBuf,
    expected: PathBuf,
    output: PathBuf,
    results: Option<PathBuf>,
    timeout_seconds: u64,
}

fn execute(paths: &Paths) -> Result<usize, QueryError> {
    let output = &paths.output;
    if output.exists() {
        return Err(QueryError::new(
            QueryCause::OutputExists,
            output.display().to_string(),
        ));
    }
    let workload = read(&paths.workload, QueryCause::InvalidWorkload)?;
    let expected = read(&paths.expected, QueryCause::InvalidExpectedCounts)?;
    let results = paths.results.as_deref().map(ResultsDir::new).transpose()?;
    // Streamed, so hashing the executable never holds it in memory.
    let executable_sha256 = std::env::current_exe()
        .and_then(std::fs::File::open)
        .and_then(|mut file| {
            let mut hasher = Sha256::new();
            let mut buffer = vec![0_u8; 1 << 20];
            loop {
                let read = file.read(&mut buffer)?;
                if read == 0 {
                    break;
                }
                hasher.update(&buffer[..read]);
            }
            Ok(hasher
                .finalize()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>())
        })
        .map_err(|error| QueryError::new(QueryCause::Io, format!("driver executable: {error}")))?;
    let evidence = run_isolated(
        &paths.project,
        &workload,
        &expected,
        executable_sha256,
        results.as_ref(),
        &Isolation {
            workload_path: &paths.workload,
            timeout_seconds: paths.timeout_seconds,
        },
    )?;
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
        .map_err(io)?;
    Ok(evidence.failures.len())
}

pub fn main(mut args: impl Iterator<Item = String>) -> ExitCode {
    let (mut project, mut workload, mut expected, mut output, mut results) =
        (None, None, None, None, None);
    let mut timeout_seconds: Option<u64> = None;
    while let Some(flag) = args.next() {
        let Some(value) = args.next() else {
            return super::usage();
        };
        if flag == "--query-timeout-seconds" {
            match value.parse::<u64>() {
                Ok(seconds) if seconds >= 1 => timeout_seconds = Some(seconds),
                _ => return super::usage(),
            }
            continue;
        }
        let slot = match flag.as_str() {
            "--project" => &mut project,
            "--workload" => &mut workload,
            "--expected-counts" => &mut expected,
            "--output" => &mut output,
            "--results-dir" => &mut results,
            _ => return super::usage(),
        };
        *slot = Some(PathBuf::from(value));
    }
    let (Some(project), Some(workload), Some(expected), Some(output), Some(timeout_seconds)) =
        (project, workload, expected, output, timeout_seconds)
    else {
        return super::usage();
    };
    let paths = Paths {
        project,
        workload,
        expected,
        output,
        results,
        timeout_seconds,
    };
    // Exit 0: every binding measured. Exit 3: evidence written with
    // `status: failed`. Exit 2: refused before or during reconciliation.
    match execute(&paths) {
        Ok(0) => {
            println!("{}", paths.output.display());
            ExitCode::SUCCESS
        }
        Ok(failures) => {
            println!("{}", paths.output.display());
            super::report(
                QueryCause::QueryFailed.as_str(),
                &format!(
                    "{failures} sample(s) failed; see failures in {}",
                    paths.output.display()
                ),
            );
            ExitCode::from(3)
        }
        Err(error) => {
            super::report(error.cause().as_str(), error.message());
            ExitCode::from(2)
        }
    }
}

/// `query-variant`, the child a timeout-bound `query` runs once per variant:
/// one JSON line per event on stdout, for the parent's watchdog to read. Not a
/// user-facing command.
pub fn variant_main(mut args: impl Iterator<Item = String>) -> ExitCode {
    let (mut project, mut workload, mut variant, mut results) = (None, None, None, None);
    let mut first: Option<u64> = None;
    while let Some(flag) = args.next() {
        let Some(value) = args.next() else {
            return super::usage();
        };
        match flag.as_str() {
            "--project" => project = Some(PathBuf::from(value)),
            "--workload" => workload = Some(PathBuf::from(value)),
            "--variant-id" => variant = Some(value),
            "--results-dir" => results = Some(PathBuf::from(value)),
            "--results-first" => match value.parse() {
                Ok(ordinal) => first = Some(ordinal),
                Err(_) => return super::usage(),
            },
            _ => return super::usage(),
        }
    }
    let (Some(project), Some(workload), Some(variant)) = (project, workload, variant) else {
        return super::usage();
    };
    let results = match (results, first) {
        (Some(path), Some(first)) => Some(ResultsDir::starting_at(&path, first)),
        (None, None) => None,
        _ => return super::usage(),
    };
    let outcome = read(&workload, QueryCause::InvalidWorkload).and_then(|bytes| {
        run_variant_child(
            &project,
            &bytes,
            &variant,
            results.as_ref(),
            &mut std::io::stdout().lock(),
        )
    });
    match outcome {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            super::report(error.cause().as_str(), error.message());
            ExitCode::from(2)
        }
    }
}
