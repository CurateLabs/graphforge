//! One variant per child process, so a call that never returns costs one
//! sample and not the run.
//!
//! `GraphForge::execute_with_params` and the analyst verbs cannot be
//! cancelled, so the only bound on a call is to kill the process running it.
//! With a timeout the parent spawns this executable's `query-variant`
//! subcommand once per variant. The child opens the project, runs
//! [`measure_variant_observed`] and writes one JSON line per [`Event`] on
//! stdout; the parent reads them on a thread and kills the child when a call
//! has been in flight for longer than the timeout. The parent alone reconciles
//! counts and writes the evidence.
//!
//! The clock is unchanged: latency is still the child's `Instant` interval
//! around one API call. The timeout is a bound, never a measurement. It covers
//! a call and the digest and result write that follow it, because the child
//! reports a sample only after both. Opening the project is not bounded.

use std::io::{BufRead, BufReader, Read, Write};
use std::path::Path;
use std::process::{Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread;
use std::time::{Duration, Instant};

use graphforge_api::GraphForge;
use serde_json::{Value, json};

use super::measure::{
    Event, Failure, Measured, Outcome, ResultsDir, Sample, VariantMeasurement, finish_variant,
    measure_variant_observed,
};
use super::workload::{Variant, parse_workload};
use super::{QueryCause, QueryError, open_project};

/// The hidden subcommand a timeout-bound run re-executes itself with.
pub const VARIANT_COMMAND: &str = "query-variant";
/// The end of a child's stderr kept in the evidence when it dies.
pub const STDERR_TAIL_BYTES: usize = 1024;

/// How a run bounds each variant's process.
#[derive(Clone, Copy, Debug)]
pub struct Isolation<'a> {
    /// The workload document the child reads, the same bytes the parent parsed.
    pub workload_path: &'a Path,
    /// The longest a call may be in flight before its process is killed.
    pub timeout_seconds: u64,
}

/// Why a variant's process ended.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Termination {
    /// It reported `done` and exited with status 0.
    Completed,
    /// The watchdog killed it.
    TimedOut { timeout_seconds: u64 },
    /// It died, or exited, before reporting `done`.
    Exited {
        code: Option<i32>,
        signal: Option<i32>,
        stderr_tail: String,
    },
}

/// Classify the end of a child from what the parent observed.
#[must_use]
pub fn classify_termination(
    done: bool,
    killed_by_watchdog: bool,
    code: Option<i32>,
    signal: Option<i32>,
    stderr_tail: &str,
    timeout_seconds: u64,
) -> Termination {
    if killed_by_watchdog {
        Termination::TimedOut { timeout_seconds }
    } else if done && code == Some(0) {
        Termination::Completed
    } else {
        Termination::Exited {
            code,
            signal,
            stderr_tail: stderr_tail.to_owned(),
        }
    }
}

/// The failure cause and error text an abandoned call gets; `None` when the
/// variant completed.
#[must_use]
pub fn abandonment(termination: &Termination) -> Option<(&'static str, String)> {
    match termination {
        Termination::Completed => None,
        Termination::TimedOut { timeout_seconds } => Some((
            "query_timeout",
            format!("no result after {timeout_seconds} s; the variant's process was killed"),
        )),
        Termination::Exited {
            code,
            signal,
            stderr_tail,
        } => {
            let how = match (signal, code) {
                (Some(signal), _) => format!("was killed by signal {signal}"),
                (None, Some(code)) => format!("exited with status {code} before finishing"),
                (None, None) => "ended without an exit status".to_owned(),
            };
            Some((
                "query_crashed",
                format!("the variant's process {how}; stderr tail: {stderr_tail}"),
            ))
        }
    }
}

fn encode(value: &Value) -> String {
    value.to_string()
}

/// One stdout line for `event`.
#[must_use]
pub fn encode_event(event: &Event<'_>) -> String {
    encode(&match event {
        Event::Start { binding_id, phase } => {
            json!({"event": "start", "binding_id": binding_id, "phase": phase})
        }
        Event::Warmup { completed } => json!({"event": "warmup", "completed": completed}),
        Event::Sample(sample) => json!({
            "event": "sample",
            "sample": serde_json::to_value(sample).unwrap_or(Value::Null),
        }),
    })
}

enum Wire {
    Start,
    Warmup(bool),
    Sample(Sample),
    Done,
}

fn static_cause(cause: &str) -> Result<&'static str, String> {
    match cause {
        "query_failed" => Ok("query_failed"),
        "result_unrenderable" => Ok("result_unrenderable"),
        other => Err(format!(
            "a child reported the unknown failure cause {other:?}"
        )),
    }
}

fn field<'a>(value: &'a Value, name: &str) -> Result<&'a Value, String> {
    value
        .get(name)
        .ok_or_else(|| format!("a child event lacks {name:?}"))
}

fn text(value: &Value, name: &str) -> Result<String, String> {
    field(value, name)?
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| format!("a child event's {name:?} is not a string"))
}

fn number(value: &Value, name: &str) -> Result<u64, String> {
    field(value, name)?
        .as_u64()
        .ok_or_else(|| format!("a child event's {name:?} is not an unsigned integer"))
}

fn parse_sample(value: &Value) -> Result<Sample, String> {
    let outcome = match text(value, "status")?.as_str() {
        "measured" => Outcome::Measured(Measured {
            latency_ns: number(value, "latency_ns")?,
            rows: number(value, "rows")?,
            result_sha256: text(value, "result_sha256")?,
        }),
        "failed" => Outcome::Failed(Failure {
            cause: static_cause(&text(value, "cause")?)?,
            error_code: field(value, "error_code")?.as_str().map(str::to_owned),
            error: text(value, "error")?,
        }),
        other => return Err(format!("a child reported the unknown status {other:?}")),
    };
    Ok(Sample {
        binding_id: text(value, "binding_id")?,
        outcome,
    })
}

fn parse_event(line: &str) -> Result<Wire, String> {
    let value: Value =
        serde_json::from_str(line).map_err(|error| format!("a child wrote {line:?}: {error}"))?;
    match text(&value, "event")?.as_str() {
        "start" => Ok(Wire::Start),
        "warmup" => field(&value, "completed")?
            .as_bool()
            .map(Wire::Warmup)
            .ok_or_else(|| "a child warmup event's \"completed\" is not a boolean".to_owned()),
        "sample" => parse_sample(field(&value, "sample")?).map(Wire::Sample),
        "done" => Ok(Wire::Done),
        other => Err(format!("a child reported the unknown event {other:?}")),
    }
}

/// What the child's `start`/`warmup`/`sample` lines established.
#[derive(Default)]
struct Progress {
    warmup: Option<bool>,
    samples: Vec<Sample>,
}

/// The variant's record when its process ended before `done`: the binding that
/// was in flight (the first, when the warm-up had not returned) gets `cause`,
/// every later binding is `not_run`.
fn abandoned(
    variant: &Variant,
    progress: Progress,
    cause: &'static str,
    error: String,
) -> Result<VariantMeasurement, QueryError> {
    let Progress {
        warmup,
        mut samples,
    } = progress;
    let culprit = if warmup.is_none() { 0 } else { samples.len() };
    let Some(binding) = variant.bindings.get(culprit) else {
        return Err(QueryError::new(
            QueryCause::Io,
            format!(
                "variant {}: its process ended abnormally after every binding had a sample",
                variant.id
            ),
        ));
    };
    samples.push(Sample {
        binding_id: binding.id.clone(),
        outcome: Outcome::Failed(Failure {
            cause,
            error_code: None,
            error,
        }),
    });
    for later in &variant.bindings[culprit + 1..] {
        samples.push(Sample {
            binding_id: later.id.clone(),
            outcome: Outcome::Failed(Failure {
                cause: "not_run",
                error_code: None,
                error: format!("variant abandoned after {cause} on binding {}", binding.id),
            }),
        });
    }
    Ok(finish_variant(variant, warmup.unwrap_or(false), samples))
}

#[cfg(unix)]
fn exit_parts(status: ExitStatus) -> (Option<i32>, Option<i32>) {
    use std::os::unix::process::ExitStatusExt;
    (status.code(), status.signal())
}

#[cfg(not(unix))]
fn exit_parts(status: ExitStatus) -> (Option<i32>, Option<i32>) {
    (status.code(), None)
}

/// Drain `stream` to its end, keeping only the last [`STDERR_TAIL_BYTES`].
fn tail_of(mut stream: impl Read) -> String {
    let mut kept: Vec<u8> = Vec::new();
    let mut chunk = [0_u8; 4096];
    while let Ok(read) = stream.read(&mut chunk) {
        if read == 0 {
            break;
        }
        kept.extend_from_slice(&chunk[..read]);
        if kept.len() > 2 * STDERR_TAIL_BYTES {
            kept.drain(..kept.len() - STDERR_TAIL_BYTES);
        }
    }
    let start = kept.len().saturating_sub(STDERR_TAIL_BYTES);
    String::from_utf8_lossy(&kept[start..]).into_owned()
}

/// Measure `variant` in a child process, killing it when a call has been in
/// flight for `isolation.timeout_seconds`.
///
/// # Errors
/// `io_error` when the child cannot be spawned or reaped, writes a line that
/// is not an event, or ends abnormally with no binding left to blame.
pub fn measure_isolated(
    project: &Path,
    isolation: &Isolation<'_>,
    variant: &Variant,
    results: Option<&ResultsDir>,
) -> Result<VariantMeasurement, QueryError> {
    let io = |what: &str, error: &dyn std::fmt::Display| {
        QueryError::new(
            QueryCause::Io,
            format!("variant {}: {what}: {error}", variant.id),
        )
    };
    let executable = std::env::current_exe().map_err(|error| io("current_exe", &error))?;
    let first_result = results.map(ResultsDir::next_ordinal);
    let mut command = Command::new(executable);
    command
        .arg(VARIANT_COMMAND)
        .arg("--project")
        .arg(project)
        .arg("--workload")
        .arg(isolation.workload_path)
        .arg("--variant-id")
        .arg(&variant.id);
    if let (Some(results), Some(first)) = (results, first_result) {
        command
            .arg("--results-dir")
            .arg(results.path())
            .arg("--results-first")
            .arg(first.to_string());
    }
    let mut child = command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| io("spawn", &error))?;
    eprintln!("{VARIANT_COMMAND} {}: child pid {}", variant.id, child.id());
    let (Some(stdout), Some(stderr)) = (child.stdout.take(), child.stderr.take()) else {
        let _ = child.kill();
        let _ = child.wait();
        return Err(io("pipes", &"the child's output was not captured"));
    };
    let (sender, receiver) = mpsc::channel();
    let reader = thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            let failed = line.is_err();
            if sender.send(line).is_err() || failed {
                break;
            }
        }
    });
    let stderr_reader = thread::spawn(move || tail_of(stderr));

    let timeout = Duration::from_secs(isolation.timeout_seconds);
    let mut progress = Progress::default();
    let mut in_flight: Option<Instant> = None;
    let (mut done, mut timed_out) = (false, false);
    let mut protocol_error: Option<String> = None;
    loop {
        let message = match in_flight {
            None => receiver.recv().map_err(|_| RecvTimeoutError::Disconnected),
            Some(since) => receiver.recv_timeout(timeout.saturating_sub(since.elapsed())),
        };
        match message {
            Ok(Ok(line)) => match parse_event(&line) {
                Ok(Wire::Start) => in_flight = Some(Instant::now()),
                Ok(Wire::Warmup(completed)) => {
                    progress.warmup = Some(completed);
                    in_flight = None;
                }
                Ok(Wire::Sample(sample)) => {
                    progress.samples.push(sample);
                    in_flight = None;
                }
                Ok(Wire::Done) => {
                    done = true;
                    break;
                }
                Err(error) => {
                    protocol_error = Some(error);
                    break;
                }
            },
            Ok(Err(error)) => {
                protocol_error = Some(format!("reading the child's output: {error}"));
                break;
            }
            Err(RecvTimeoutError::Timeout) => {
                timed_out = true;
                break;
            }
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }
    if !done {
        // SIGKILL on unix; a child that already exited makes this a no-op.
        let _ = child.kill();
    }
    let status = child.wait().map_err(|error| io("wait", &error))?;
    let _ = reader.join();
    let stderr_tail = stderr_reader.join().unwrap_or_default();
    let measured_results = progress
        .samples
        .iter()
        .filter(|sample| sample.measured().is_some())
        .count() as u64;
    if let Some(error) = protocol_error {
        if let (Some(results), Some(first)) = (results, first_result) {
            results.discard_from(first)?;
        }
        return Err(io("protocol", &error));
    }
    let (code, signal) = exit_parts(status);
    let termination = classify_termination(
        done,
        timed_out,
        code,
        signal,
        &stderr_tail,
        isolation.timeout_seconds,
    );
    if let (Some(results), Some(first)) = (results, first_result) {
        if abandonment(&termination).is_some() {
            results.discard_from(first + measured_results)?;
        } else {
            results.advance_to(first + measured_results);
        }
    }
    match abandonment(&termination) {
        None => Ok(finish_variant(
            variant,
            progress.warmup.unwrap_or(false),
            progress.samples,
        )),
        Some((cause, error)) => abandoned(variant, progress, cause, error),
    }
}

/// The child half: open the project, measure `variant_id`, and write one JSON
/// line per event followed by `done` to `out`.
///
/// # Errors
/// `invalid_workload` for an unknown variant, the project-open causes, and
/// `io_error` when `out` cannot be written.
pub fn run_variant_child(
    project: &Path,
    workload: &[u8],
    variant_id: &str,
    results: Option<&ResultsDir>,
    out: &mut impl Write,
) -> Result<(), QueryError> {
    let parsed = parse_workload(workload)?;
    let variant = parsed
        .variants
        .iter()
        .find(|variant| variant.id == variant_id)
        .ok_or_else(|| {
            QueryError::new(
                QueryCause::InvalidWorkload,
                format!("the workload has no variant {variant_id:?}"),
            )
        })?;
    let forge: GraphForge = open_project(project)?;
    let mut written: Result<(), std::io::Error> = Ok(());
    let mut emit = |line: String| {
        if written.is_ok() {
            written = writeln!(out, "{line}").and_then(|()| out.flush());
        }
    };
    measure_variant_observed(&forge, variant, results, &mut |event| {
        emit(encode_event(&event));
    })?;
    emit(encode(&json!({"event": "done"})));
    written.map_err(|error| QueryError::new(QueryCause::Io, format!("stdout: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn variant(bindings: &[&str]) -> Variant {
        serde_json::from_value(json!({
            "id": "v",
            "ordered": false,
            "operation": {"kind": "cypher", "text": "RETURN 1"},
            "bindings": bindings.iter().map(|id| json!({"id": id})).collect::<Vec<_>>(),
        }))
        .unwrap()
    }

    fn measured(binding: &str) -> Sample {
        Sample {
            binding_id: binding.to_owned(),
            outcome: Outcome::Measured(Measured {
                latency_ns: 7,
                rows: 1,
                result_sha256: "0".repeat(64),
            }),
        }
    }

    #[test]
    fn a_child_exit_is_classified_by_what_the_parent_saw() {
        let tail = "boom";
        assert_eq!(
            classify_termination(true, false, Some(0), None, tail, 5),
            Termination::Completed
        );
        assert_eq!(
            classify_termination(false, true, None, Some(9), tail, 5),
            Termination::TimedOut { timeout_seconds: 5 }
        );
        let crashed = |code, signal| Termination::Exited {
            code,
            signal,
            stderr_tail: tail.to_owned(),
        };
        assert_eq!(
            classify_termination(false, false, None, Some(9), tail, 5),
            crashed(None, Some(9))
        );
        assert_eq!(
            classify_termination(false, false, Some(2), None, tail, 5),
            crashed(Some(2), None)
        );
        assert_eq!(
            classify_termination(false, false, Some(0), None, tail, 5),
            crashed(Some(0), None),
            "a clean exit without `done` is not a completed variant"
        );
        assert_eq!(
            classify_termination(true, false, Some(1), None, tail, 5),
            crashed(Some(1), None),
            "`done` followed by a failing exit is not a completed variant"
        );
        assert_eq!(abandonment(&Termination::Completed), None);
        assert_eq!(
            abandonment(&Termination::TimedOut { timeout_seconds: 5 }),
            Some((
                "query_timeout",
                "no result after 5 s; the variant's process was killed".to_owned()
            ))
        );
        let (cause, error) = abandonment(&crashed(None, Some(9))).unwrap();
        assert_eq!(cause, "query_crashed");
        assert_eq!(
            error,
            "the variant's process was killed by signal 9; stderr tail: boom"
        );
        let (_, error) = abandonment(&crashed(Some(2), None)).unwrap();
        assert_eq!(
            error,
            "the variant's process exited with status 2 before finishing; stderr tail: boom"
        );
    }

    #[test]
    fn an_abandoned_warmup_fails_the_first_binding_and_skips_the_rest() {
        let variant = variant(&["a", "b", "c"]);
        let record = abandoned(
            &variant,
            Progress::default(),
            "query_timeout",
            "late".into(),
        )
        .unwrap();
        assert!(!record.warmup.completed);
        assert_eq!(record.status, "failed");
        assert!(record.summary.is_none());
        let causes: Vec<_> = record
            .samples
            .iter()
            .map(|sample| (sample.binding_id.as_str(), sample.failure().unwrap().cause))
            .collect();
        assert_eq!(
            causes,
            [("a", "query_timeout"), ("b", "not_run"), ("c", "not_run")]
        );
        assert_eq!(
            record.samples[2].failure().unwrap().error,
            "variant abandoned after query_timeout on binding a"
        );
    }

    #[test]
    fn an_abandoned_measured_pass_keeps_earlier_samples_and_their_summary() {
        let variant = variant(&["a", "b", "c"]);
        let progress = Progress {
            warmup: Some(true),
            samples: vec![measured("a")],
        };
        let record = abandoned(&variant, progress, "query_crashed", "died".into()).unwrap();
        assert!(record.warmup.completed);
        let summary = record.summary.as_ref().unwrap();
        assert_eq!((summary.count, summary.p50_ns, summary.p95_ns), (1, 7, 7));
        let causes: Vec<_> = record
            .samples
            .iter()
            .map(|sample| sample.failure().map(|failure| failure.cause))
            .collect();
        assert_eq!(causes, [None, Some("query_crashed"), Some("not_run")]);
    }

    #[test]
    fn events_survive_the_wire() {
        let sample = measured("a");
        let line = encode_event(&Event::Sample(&sample));
        let Ok(Wire::Sample(parsed)) = parse_event(&line) else {
            panic!("a sample line parses back to a sample")
        };
        assert_eq!(parsed.binding_id, "a");
        assert_eq!(parsed.measured().unwrap().latency_ns, 7);
        let failed = Sample {
            binding_id: "b".to_owned(),
            outcome: Outcome::Failed(Failure {
                cause: "query_failed",
                error_code: Some("GF_PARSE".to_owned()),
                error: "bad".to_owned(),
            }),
        };
        let line = encode_event(&Event::Sample(&failed));
        let Ok(Wire::Sample(parsed)) = parse_event(&line) else {
            panic!("a failed sample line parses back")
        };
        let failure = parsed.failure().unwrap();
        assert_eq!(failure.error_code.as_deref(), Some("GF_PARSE"));
        assert!(parse_event("{\"event\":\"nope\"}").is_err());
        assert!(parse_event("not json").is_err());
    }
}
