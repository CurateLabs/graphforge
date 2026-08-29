#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use std::fmt;
use std::fs;
use std::path::Path;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

pub const EVIDENCE_SCHEMA: &str = "graphforge-public-certification/1";
pub const PHASE_EVENT_SCHEMA: &str = "graphforge-public-certification-phase-event/1";

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    Admission,
    Generate,
    Ingest,
    Reopen,
    Recount,
    Query,
    Export,
    Verify,
    CleanImport,
    ReopenProof,
}

impl Phase {
    pub const ALL: [Self; 10] = [
        Self::Admission,
        Self::Generate,
        Self::Ingest,
        Self::Reopen,
        Self::Recount,
        Self::Query,
        Self::Export,
        Self::Verify,
        Self::CleanImport,
        Self::ReopenProof,
    ];
}

impl fmt::Display for Phase {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = serde_json::to_value(self).map_err(|_| fmt::Error)?;
        output.write_str(value.as_str().ok_or(fmt::Error)?)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    pub schema: String,
    pub id: String,
    pub executable: String,
    pub phases: Vec<PhaseCommand>,
    #[serde(default)]
    pub scale: Option<u8>,
    #[serde(default)]
    pub execution: Option<String>,
    #[serde(default)]
    pub generator: Option<serde_json::Value>,
    #[serde(default)]
    pub lifecycle: Option<serde_json::Value>,
    #[serde(default)]
    pub gate: Option<serde_json::Value>,
}

impl Profile {
    pub fn validate(&self) -> Result<(), RunnerError> {
        if !matches!(
            self.schema.as_str(),
            "graphforge-public-certification-profile/1"
                | "graphforge-progressive-qualification-profile/1"
        ) {
            return Err(RunnerError::Profile("unsupported profile schema"));
        }
        if !is_safe_token(&self.id) {
            return Err(RunnerError::Profile("profile id must be a safe token"));
        }
        if self.schema == "graphforge-progressive-qualification-profile/1"
            && !self.progressive_contract_is_valid()
        {
            return Err(RunnerError::Profile(
                "progressive profile requires its qualification contract",
            ));
        }
        if !is_graphforge_executable(&self.executable) {
            return Err(RunnerError::Profile(
                "executable must resolve to the public gf command",
            ));
        }
        if self.phases.len() != Phase::ALL.len()
            || self
                .phases
                .iter()
                .map(|command| command.phase)
                .ne(Phase::ALL)
        {
            return Err(RunnerError::Profile(
                "profile must declare every phase once in lifecycle order",
            ));
        }
        if self
            .phases
            .iter()
            .any(|command| !action_matches_phase(command))
        {
            return Err(RunnerError::Profile(
                "phase action must select the matching benchmark or public gf operation",
            ));
        }
        Ok(())
    }

    fn progressive_contract_is_valid(&self) -> bool {
        let (Some(scale), Some(execution), Some(generator), Some(lifecycle), Some(gate)) = (
            self.scale,
            self.execution.as_deref(),
            self.generator.clone(),
            self.lifecycle.clone(),
            self.gate.clone(),
        ) else {
            return false;
        };
        let Ok(generator) = serde_json::from_value::<ProgressiveGenerator>(generator) else {
            return false;
        };
        let Ok(lifecycle) = serde_json::from_value::<ProgressiveLifecycle>(lifecycle) else {
            return false;
        };
        let Ok(gate) = serde_json::from_value::<ProgressiveGate>(gate) else {
            return false;
        };
        let expected = match scale {
            18 | 19 => ("local", None),
            20 => ("provider", Some([18, 19])),
            22 => ("provider", Some([19, 20])),
            26 => ("provider", Some([24, 25])),
            _ => return false,
        };
        execution == expected.0
            && generator.identity == generator_identity(&self.phases)
            && generator.edge_factor == 16
            && generator.seed == 13_907_095_936_298_285_200
            && lifecycle.mechanics == "public-certification-v1"
            && lifecycle.phases == Phase::ALL
            && lifecycle.evidence_schema == EVIDENCE_SCHEMA
            && gate.requires_previous_pass
            && gate.projection_source_scales == expected.1
            && gate.limits.wall_seconds == 14_400
            && gate.limits.rss_bytes == 4_294_967_296
            && gate.limits.volume_bytes == 536_870_912_000
            && gate.headroom.time_fraction == 0.2
            && gate.headroom.rss_fraction == 0.2
            && gate.headroom.storage_fraction == 0.15
            && gate.headroom.max_adjacent_rss_growth_fraction == 0.1
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProgressiveGenerator {
    identity: String,
    edge_factor: u8,
    seed: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProgressiveLifecycle {
    mechanics: String,
    phases: Vec<Phase>,
    evidence_schema: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProgressiveGate {
    requires_previous_pass: bool,
    projection_source_scales: Option<[u8; 2]>,
    limits: ProgressiveLimits,
    headroom: ProgressiveHeadroom,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProgressiveLimits {
    wall_seconds: u64,
    rss_bytes: u64,
    volume_bytes: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProgressiveHeadroom {
    time_fraction: f64,
    rss_fraction: f64,
    storage_fraction: f64,
    max_adjacent_rss_growth_fraction: f64,
}

fn generator_identity(phases: &[PhaseCommand]) -> String {
    phases
        .iter()
        .find_map(|command| match &command.action {
            PhaseAction::BenchmarkGenerator { identity, .. } => Some(identity.clone()),
            _ => None,
        })
        .unwrap_or_default()
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PhaseCommand {
    pub phase: Phase,
    pub action: PhaseAction,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "interface", rename_all = "snake_case", deny_unknown_fields)]
pub enum PhaseAction {
    BenchmarkGenerator {
        identity: String,
        executable: String,
        args: Vec<String>,
    },
    GraphForgeCli {
        args: Vec<String>,
    },
    GraphForgeCliWorkflow {
        commands: Vec<Vec<String>>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Execution {
    pub exit_code: Option<i32>,
    pub duration_ms: u64,
    pub peak_rss_bytes: Option<u64>,
    pub failure: Option<FailureKind>,
}

pub trait PhaseExecutor {
    fn execute(&mut self, profile: &Profile, command: &PhaseCommand) -> Result<Execution, String>;
}

#[derive(Default)]
pub struct PublicProcessExecutor;

impl PhaseExecutor for PublicProcessExecutor {
    fn execute(&mut self, profile: &Profile, command: &PhaseCommand) -> Result<Execution, String> {
        if let PhaseAction::GraphForgeCliWorkflow { commands } = &command.action {
            let started = Instant::now();
            let mut peak_rss_bytes = None;
            for args in commands {
                let execution = match execute_process(&profile.executable, args) {
                    Ok(execution) => execution,
                    Err(_) => {
                        return Ok(Execution {
                            exit_code: None,
                            duration_ms: millis(started.elapsed()),
                            peak_rss_bytes,
                            failure: Some(FailureKind::CommandUnavailable),
                        });
                    }
                };
                peak_rss_bytes = max_optional(peak_rss_bytes, execution.peak_rss_bytes);
                if execution.exit_code != Some(0) {
                    return Ok(Execution {
                        exit_code: execution.exit_code,
                        duration_ms: millis(started.elapsed()),
                        peak_rss_bytes,
                        failure: execution.failure,
                    });
                }
            }
            return Ok(Execution {
                exit_code: Some(0),
                duration_ms: millis(started.elapsed()),
                peak_rss_bytes,
                failure: None,
            });
        }
        let (executable, args) = match &command.action {
            PhaseAction::BenchmarkGenerator {
                executable, args, ..
            } => (executable.as_str(), args.as_slice()),
            PhaseAction::GraphForgeCli { args } => (profile.executable.as_str(), args.as_slice()),
            PhaseAction::GraphForgeCliWorkflow { .. } => unreachable!("handled above"),
        };
        execute_process(executable, args)
    }
}

fn execute_process(executable: &str, args: &[String]) -> Result<Execution, String> {
    let started = Instant::now();
    let mut child = Command::new(executable)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|_| "public command could not start".to_owned())?;
    let mut peak_rss_bytes = None;
    loop {
        peak_rss_bytes = max_optional(peak_rss_bytes, resident_bytes(child.id()));
        if let Some(status) = child
            .try_wait()
            .map_err(|_| "public command wait failed".to_owned())?
        {
            return Ok(Execution {
                exit_code: status.code(),
                duration_ms: millis(started.elapsed()),
                peak_rss_bytes,
                failure: None,
            });
        }
        thread::sleep(Duration::from_millis(10));
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OutcomeStatus {
    Passed,
    Failed,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureKind {
    CommandFailed,
    CommandUnavailable,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PhaseOutcome {
    pub phase: Phase,
    pub status: OutcomeStatus,
    pub duration_ms: u64,
    pub peak_rss_bytes: Option<u64>,
    pub exit_code: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure: Option<FailureKind>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Evidence {
    pub schema: String,
    pub profile_id: String,
    pub status: OutcomeStatus,
    pub phases: Vec<PhaseOutcome>,
    pub failed_phase: Option<Phase>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PhaseEvent {
    pub schema: &'static str,
    pub profile_id: String,
    pub outcome: PhaseOutcome,
}

pub fn certify(
    profile: &Profile,
    executor: &mut dyn PhaseExecutor,
) -> Result<Evidence, RunnerError> {
    certify_with_events(profile, executor, |_| Ok(()))
}

pub fn certify_with_events(
    profile: &Profile,
    executor: &mut dyn PhaseExecutor,
    mut emit: impl FnMut(&PhaseEvent) -> Result<(), RunnerError>,
) -> Result<Evidence, RunnerError> {
    profile.validate()?;
    let mut phases = Vec::with_capacity(Phase::ALL.len());
    for command in &profile.phases {
        let outcome = match executor.execute(profile, command) {
            Ok(execution) => {
                let passed = execution.exit_code == Some(0) && execution.failure.is_none();
                PhaseOutcome {
                    phase: command.phase,
                    status: if passed {
                        OutcomeStatus::Passed
                    } else {
                        OutcomeStatus::Failed
                    },
                    duration_ms: execution.duration_ms,
                    peak_rss_bytes: execution.peak_rss_bytes,
                    exit_code: execution.exit_code,
                    failure: if passed {
                        None
                    } else {
                        execution.failure.or(Some(FailureKind::CommandFailed))
                    },
                }
            }
            Err(_) => PhaseOutcome {
                phase: command.phase,
                status: OutcomeStatus::Failed,
                duration_ms: 0,
                peak_rss_bytes: None,
                exit_code: None,
                failure: Some(FailureKind::CommandUnavailable),
            },
        };
        let failed = outcome.status == OutcomeStatus::Failed;
        emit(&PhaseEvent {
            schema: PHASE_EVENT_SCHEMA,
            profile_id: profile.id.clone(),
            outcome: outcome.clone(),
        })?;
        phases.push(outcome);
        if failed {
            break;
        }
    }
    let failed_phase = phases
        .iter()
        .find(|outcome| outcome.status == OutcomeStatus::Failed)
        .map(|outcome| outcome.phase);
    Ok(Evidence {
        schema: EVIDENCE_SCHEMA.to_owned(),
        profile_id: profile.id.clone(),
        status: if failed_phase.is_some() {
            OutcomeStatus::Failed
        } else {
            OutcomeStatus::Passed
        },
        phases,
        failed_phase,
    })
}

#[derive(Deserialize)]
#[serde(untagged)]
enum EvidenceInput {
    Current(Evidence),
    Legacy(LegacyEvidence),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyEvidence {
    profile: String,
    phases: Vec<LegacyPhase>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyPhase {
    name: Phase,
    ok: bool,
    duration_secs: f64,
    max_rss_kib: Option<u64>,
    exit_code: Option<i32>,
}

pub fn normalize_evidence(input: &[u8]) -> Result<Evidence, RunnerError> {
    match serde_json::from_slice(input).map_err(|_| RunnerError::Legacy)? {
        EvidenceInput::Current(evidence) => validate_evidence(evidence),
        EvidenceInput::Legacy(legacy) => {
            if !is_safe_token(&legacy.profile) || legacy.phases.is_empty() {
                return Err(RunnerError::Legacy);
            }
            let mut phases = Vec::with_capacity(legacy.phases.len());
            for phase in legacy.phases {
                if !phase.duration_secs.is_finite() || phase.duration_secs < 0.0 {
                    return Err(RunnerError::Legacy);
                }
                phases.push(PhaseOutcome {
                    phase: phase.name,
                    status: if phase.ok {
                        OutcomeStatus::Passed
                    } else {
                        OutcomeStatus::Failed
                    },
                    duration_ms: (phase.duration_secs * 1_000.0).round() as u64,
                    peak_rss_bytes: phase.max_rss_kib.and_then(|value| value.checked_mul(1_024)),
                    exit_code: phase.exit_code,
                    failure: (!phase.ok).then_some(FailureKind::CommandFailed),
                });
                if !phase.ok {
                    break;
                }
            }
            let failed_phase = phases
                .iter()
                .find(|outcome| outcome.status == OutcomeStatus::Failed)
                .map(|outcome| outcome.phase);
            validate_evidence(Evidence {
                schema: EVIDENCE_SCHEMA.to_owned(),
                profile_id: legacy.profile,
                status: if failed_phase.is_some() {
                    OutcomeStatus::Failed
                } else {
                    OutcomeStatus::Passed
                },
                phases,
                failed_phase,
            })
        }
    }
}

fn validate_evidence(evidence: Evidence) -> Result<Evidence, RunnerError> {
    if evidence.schema != EVIDENCE_SCHEMA
        || !is_safe_token(&evidence.profile_id)
        || evidence.phases.is_empty()
        || evidence
            .phases
            .iter()
            .map(|outcome| outcome.phase)
            .ne(Phase::ALL.into_iter().take(evidence.phases.len()))
    {
        return Err(RunnerError::Legacy);
    }
    let observed_failure = evidence
        .phases
        .iter()
        .find(|outcome| outcome.status == OutcomeStatus::Failed)
        .map(|outcome| outcome.phase);
    let statuses_are_consistent = evidence.phases.iter().enumerate().all(|(index, outcome)| {
        let failed = outcome.status == OutcomeStatus::Failed;
        failed == outcome.failure.is_some() && (!failed || index + 1 == evidence.phases.len())
    });
    if observed_failure != evidence.failed_phase
        || (observed_failure.is_some()) != (evidence.status == OutcomeStatus::Failed)
        || !statuses_are_consistent
    {
        return Err(RunnerError::Legacy);
    }
    Ok(evidence)
}

pub fn read_profile(path: &Path) -> Result<Profile, RunnerError> {
    let input = fs::read(path).map_err(|_| RunnerError::Io)?;
    serde_json::from_slice(&input).map_err(|_| RunnerError::Profile("invalid profile JSON"))
}

pub fn write_evidence(path: &Path, evidence: &Evidence) -> Result<(), RunnerError> {
    let encoded = serde_json::to_vec_pretty(evidence).map_err(|_| RunnerError::Io)?;
    fs::write(path, encoded).map_err(|_| RunnerError::Io)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RunnerError {
    Io,
    Legacy,
    Profile(&'static str),
}

fn is_safe_token(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 80
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn is_graphforge_executable(value: &str) -> bool {
    !value.contains('\0')
        && Path::new(value)
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| matches!(name, "gf" | "gf.exe"))
}

fn action_matches_phase(command: &PhaseCommand) -> bool {
    if let PhaseAction::BenchmarkGenerator {
        identity,
        executable,
        args,
    } = &command.action
    {
        return command.phase == Phase::Generate
            && is_sha256_identity(identity)
            && !executable.is_empty()
            && !executable.contains('\0')
            && args.iter().all(|argument| !argument.contains('\0'));
    }
    if let PhaseAction::GraphForgeCliWorkflow { commands } = &command.action {
        return match command.phase {
            Phase::Ingest => ingest_workflow_is_valid(commands),
            Phase::Recount | Phase::Query | Phase::ReopenProof => query_workflow_is_valid(commands),
            _ => false,
        };
    }
    let PhaseAction::GraphForgeCli { args } = &command.action else {
        return false;
    };
    if command.phase == Phase::Generate || args.iter().any(|argument| argument.contains('\0')) {
        return false;
    }
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    if args
        .iter()
        .any(|argument| matches!(*argument, "--version" | "--help"))
    {
        return false;
    }
    match command.phase {
        Phase::Admission => args.as_slice() == ["--info"],
        Phase::Generate => false,
        Phase::Ingest => contains_command(&args, &["import-session"]),
        Phase::Reopen => contains_command(&args, &["recovery"]),
        Phase::Recount | Phase::Query | Phase::ReopenProof => contains_command(&args, &["query"]),
        Phase::Export => contains_command(&args, &["portable", "export"]),
        Phase::Verify => contains_command(&args, &["portable", "verify"]),
        Phase::CleanImport => contains_command(&args, &["portable", "import"]),
    }
}

fn is_sha256_identity(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|digest| {
        digest.len() == 64
            && digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    })
}

fn query_workflow_is_valid(commands: &[Vec<String>]) -> bool {
    commands.len() == 2
        && commands.iter().all(|args| {
            args.iter().all(|argument| !argument.contains('\0'))
                && contains_command(
                    &args.iter().map(String::as_str).collect::<Vec<_>>(),
                    &["query"],
                )
        })
}

fn ingest_workflow_is_valid(commands: &[Vec<String>]) -> bool {
    const OPERATIONS: [&str; 5] = [
        "begin",
        "register-parquet",
        "register-parquet",
        "validate",
        "commit",
    ];
    commands.len() == OPERATIONS.len()
        && commands.iter().zip(OPERATIONS).all(|(args, operation)| {
            !args.is_empty()
                && args.iter().all(|argument| !argument.contains('\0'))
                && contains_command(
                    &args.iter().map(String::as_str).collect::<Vec<_>>(),
                    &["import-session", operation],
                )
        })
}

fn contains_command(arguments: &[&str], command: &[&str]) -> bool {
    arguments
        .windows(command.len())
        .any(|window| window == command)
}

fn millis(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}

fn max_optional(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (left, right) => left.or(right),
    }
}

#[cfg(target_os = "linux")]
fn resident_bytes(pid: u32) -> Option<u64> {
    let status = fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    let rss_kib = status
        .lines()
        .find_map(|line| line.strip_prefix("VmHWM:"))?
        .split_ascii_whitespace()
        .next()?
        .parse::<u64>()
        .ok()?;
    rss_kib.checked_mul(1_024)
}

#[cfg(not(target_os = "linux"))]
fn resident_bytes(_pid: u32) -> Option<u64> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    struct FakeExecutor {
        executions: VecDeque<Result<Execution, String>>,
        calls: Vec<Phase>,
    }

    impl PhaseExecutor for FakeExecutor {
        fn execute(
            &mut self,
            _profile: &Profile,
            command: &PhaseCommand,
        ) -> Result<Execution, String> {
            self.calls.push(command.phase);
            self.executions.pop_front().expect("fixture execution")
        }
    }

    fn tiny_profile() -> Profile {
        Profile {
            schema: "graphforge-public-certification-profile/1".to_owned(),
            id: "tiny-public".to_owned(),
            executable: "gf".to_owned(),
            phases: Phase::ALL
                .into_iter()
                .map(|phase| PhaseCommand {
                    phase,
                    action: if phase == Phase::Generate {
                        PhaseAction::BenchmarkGenerator {
                            identity: format!("sha256:{}", "0".repeat(64)),
                            executable: "tiny-generator".to_owned(),
                            args: vec!["--scale".to_owned(), "tiny".to_owned()],
                        }
                    } else if phase == Phase::Ingest {
                        PhaseAction::GraphForgeCliWorkflow {
                            commands: ingest_commands(),
                        }
                    } else {
                        PhaseAction::GraphForgeCli {
                            args: tiny_args(phase),
                        }
                    },
                })
                .collect(),
            scale: None,
            execution: None,
            generator: None,
            lifecycle: None,
            gate: None,
        }
    }

    fn progressive_profile() -> Profile {
        let mut profile = tiny_profile();
        let identity = generator_identity(&profile.phases);
        profile.schema = "graphforge-progressive-qualification-profile/1".to_owned();
        profile.id = "graph500-s20-provider".to_owned();
        profile.scale = Some(20);
        profile.execution = Some("provider".to_owned());
        profile.generator = Some(serde_json::json!({
            "identity": identity,
            "edge_factor": 16,
            "seed": 13_907_095_936_298_285_200_u64
        }));
        profile.lifecycle = Some(serde_json::json!({
            "mechanics": "public-certification-v1",
            "phases": Phase::ALL,
            "evidence_schema": EVIDENCE_SCHEMA
        }));
        profile.gate = Some(serde_json::json!({
            "requires_previous_pass": true,
            "projection_source_scales": [18, 19],
            "limits": {
                "wall_seconds": 14_400,
                "rss_bytes": 4_294_967_296_u64,
                "volume_bytes": 536_870_912_000_u64
            },
            "headroom": {
                "time_fraction": 0.2,
                "rss_fraction": 0.2,
                "storage_fraction": 0.15,
                "max_adjacent_rss_growth_fraction": 0.1
            }
        }));
        profile
    }

    fn ingest_commands() -> Vec<Vec<String>> {
        [
            "begin",
            "register-parquet",
            "register-parquet",
            "validate",
            "commit",
        ]
        .into_iter()
        .map(|operation| {
            [
                "--project",
                "generated/tiny-source",
                "import-session",
                operation,
            ]
            .into_iter()
            .map(str::to_owned)
            .collect()
        })
        .collect()
    }

    fn tiny_args(phase: Phase) -> Vec<String> {
        let values: &[&str] = match phase {
            Phase::Admission => &["--info"],
            Phase::Generate => unreachable!("generate uses the benchmark-owned typed action"),
            Phase::Ingest => &[
                "--project",
                "generated/tiny-source",
                "import-session",
                "open",
            ],
            Phase::Reopen => &["--project", "generated/tiny-source", "recovery"],
            Phase::Recount => &[
                "--project",
                "generated/tiny-source",
                "query",
                "--cypher",
                "MATCH (n) RETURN count(n)",
                "--output",
                "generated/recount.arrow",
            ],
            Phase::Query => &[
                "--project",
                "generated/tiny-source",
                "query",
                "--cypher",
                "MATCH (n) RETURN n.id",
                "--output",
                "generated/query.arrow",
            ],
            Phase::Export => &[
                "--project",
                "generated/tiny-source",
                "portable",
                "export",
                "--current",
                "--output",
                "generated/tiny-portable",
            ],
            Phase::Verify => &["portable", "verify", "--input", "generated/tiny-portable"],
            Phase::CleanImport => &[
                "--project",
                "generated/tiny-import",
                "portable",
                "import",
                "--input",
                "generated/tiny-portable",
                "--idempotency-key",
                "00000000-0000-4000-8000-000000000001",
            ],
            Phase::ReopenProof => &[
                "--project",
                "generated/tiny-import",
                "query",
                "--cypher",
                "MATCH (n) RETURN count(n)",
                "--output",
                "generated/reopen-proof.arrow",
            ],
        };
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    fn passed_execution(index: u64) -> Result<Execution, String> {
        Ok(Execution {
            exit_code: Some(0),
            duration_ms: index + 1,
            peak_rss_bytes: Some((index + 1) * 1_024),
            failure: None,
        })
    }

    #[test]
    fn tiny_fixture_proves_every_phase_and_sanitized_evidence() {
        let mut executor = FakeExecutor {
            executions: (0..10).map(passed_execution).collect(),
            calls: Vec::new(),
        };
        let evidence = certify(&tiny_profile(), &mut executor).unwrap();
        assert_eq!(executor.calls, Phase::ALL);
        assert_eq!(evidence.status, OutcomeStatus::Passed);
        assert_eq!(evidence.phases.len(), 10);
        assert!(
            evidence
                .phases
                .iter()
                .all(|phase| phase.peak_rss_bytes.is_some())
        );
        let encoded = serde_json::to_string(&evidence).unwrap();
        for forbidden in ["args", "stdout", "stderr", "path", "credential", "secret"] {
            assert!(!encoded.contains(forbidden));
        }
    }

    #[test]
    fn first_failure_stops_before_later_public_commands() {
        let mut executions: VecDeque<_> = (0..3).map(passed_execution).collect();
        executions.push_back(Ok(Execution {
            exit_code: Some(7),
            duration_ms: 4,
            peak_rss_bytes: Some(4_096),
            failure: None,
        }));
        executions.extend((4..10).map(passed_execution));
        let mut executor = FakeExecutor {
            executions,
            calls: Vec::new(),
        };
        let mut events = Vec::new();
        let evidence = certify_with_events(&tiny_profile(), &mut executor, |event| {
            events.push(event.clone());
            Ok(())
        })
        .unwrap();
        assert_eq!(evidence.failed_phase, Some(Phase::Reopen));
        assert_eq!(executor.calls, Phase::ALL[..4]);
        assert_eq!(evidence.phases.len(), 4);
        assert_eq!(events.len(), 4);
        assert_eq!(events.last().unwrap().outcome.status, OutcomeStatus::Failed);
    }

    #[test]
    fn legacy_evidence_normalizes_to_current_typed_contract() {
        let normalized = normalize_evidence(
            br#"{"profile":"legacy-s20","phases":[{"name":"admission","ok":true,"duration_secs":1.25,"max_rss_kib":2,"exit_code":0}]}"#,
        )
        .unwrap();
        assert_eq!(normalized.schema, EVIDENCE_SCHEMA);
        assert_eq!(normalized.phases[0].duration_ms, 1_250);
        assert_eq!(normalized.phases[0].peak_rss_bytes, Some(2_048));
    }

    #[test]
    fn profile_rejects_non_graphforge_executable() {
        let mut profile = tiny_profile();
        profile.executable = "/usr/bin/true".to_owned();
        assert_eq!(
            profile.validate(),
            Err(RunnerError::Profile(
                "executable must resolve to the public gf command"
            ))
        );
    }

    #[test]
    fn profile_rejects_noop_commands_labeled_as_lifecycle_phases() {
        let mut profile = tiny_profile();
        for command in &mut profile.phases {
            command.action = PhaseAction::GraphForgeCli {
                args: vec!["--version".to_owned()],
            };
        }
        assert_eq!(
            profile.validate(),
            Err(RunnerError::Profile(
                "phase action must select the matching benchmark or public gf operation"
            ))
        );
    }

    #[test]
    fn ingest_workflow_requires_the_complete_ordered_transaction() {
        let mut profile = tiny_profile();
        let PhaseAction::GraphForgeCliWorkflow { commands } = &mut profile.phases[2].action else {
            panic!("ingest fixture must be a workflow");
        };
        commands.swap(3, 4);
        assert!(profile.validate().is_err());
    }

    #[test]
    fn progressive_profile_contract_is_typed_and_scale_specific() {
        let profile = progressive_profile();
        assert_eq!(profile.validate(), Ok(()));
        for malformed in [
            serde_json::json!([]),
            serde_json::json!({}),
            serde_json::json!({"identity": 42, "edge_factor": 16, "seed": 1}),
        ] {
            let mut profile = progressive_profile();
            profile.generator = Some(malformed);
            assert!(profile.validate().is_err());
        }
        let mut profile = progressive_profile();
        profile.scale = Some(0);
        assert!(profile.validate().is_err());
        let mut profile = progressive_profile();
        profile.gate.as_mut().unwrap()["projection_source_scales"] = serde_json::json!([24, 25]);
        assert!(profile.validate().is_err());
    }

    #[test]
    fn generator_digest_must_be_lowercase_hex() {
        assert!(is_sha256_identity(&format!("sha256:{}", "a".repeat(64))));
        assert!(!is_sha256_identity(&format!("sha256:{}", "A".repeat(64))));
    }

    #[cfg(unix)]
    #[test]
    fn public_executor_runs_ingest_transaction_in_order() {
        let root = std::env::temp_dir().join(format!("gf-certify-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let executable = root.join("gf");
        let state = root.join("state");
        fs::write(
            &executable,
            format!(
                "#!/bin/sh\nstate='{}'\nn=0\n[ ! -f \"$state\" ] || n=$(cat \"$state\")\ncase \"$*\" in\n  *'import-session begin') expected=0;;\n  *'import-session register-parquet') if [ \"$n\" = 1 ]; then expected=1; else expected=2; fi;;\n  *'import-session validate') expected=3;;\n  *'import-session commit') expected=4;;\n  *) exit 41;;\nesac\n[ \"$n\" = \"$expected\" ] || exit 42\necho $((n + 1)) > \"$state\"\n",
                state.display()
            ),
        )
        .unwrap();
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o755)).unwrap();
        let mut profile = tiny_profile();
        profile.executable = executable.to_string_lossy().into_owned();
        let mut executor = PublicProcessExecutor;
        let result = executor.execute(&profile, &profile.phases[2]).unwrap();
        assert_eq!(result.exit_code, Some(0));
        assert_eq!(fs::read_to_string(state).unwrap().trim(), "5");
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn public_executor_stops_ingest_at_first_child_failure() {
        let root = std::env::temp_dir().join(format!("gf-certify-fail-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let executable = root.join("gf");
        let calls = root.join("calls");
        fs::write(
            &executable,
            format!(
                "#!/bin/sh\necho \"$*\" >> '{}'\ncase \"$*\" in *'import-session validate'*) exit 23;; esac\n",
                calls.display()
            ),
        )
        .unwrap();
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o755)).unwrap();
        let mut profile = tiny_profile();
        profile.executable = executable.to_string_lossy().into_owned();
        let result = PublicProcessExecutor
            .execute(&profile, &profile.phases[2])
            .unwrap();
        assert_eq!(result.exit_code, Some(23));
        let calls = fs::read_to_string(calls).unwrap();
        assert_eq!(calls.lines().count(), 4);
        assert!(!calls.contains("import-session commit"));
        fs::remove_dir_all(root).unwrap();
    }
}
