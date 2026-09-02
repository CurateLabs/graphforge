#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::io::Read;
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
        if self.schema == "graphforge-public-certification-profile/1"
            && self.lifecycle.is_some()
            && !self.lifecycle_storage_requested()
        {
            return Err(RunnerError::Profile(
                "public profile lifecycle declaration is invalid",
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
            24 => ("provider", Some([20, 22])),
            25 => ("provider", Some([22, 24])),
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
            && lifecycle.storage_receipt == "graphforge-lifecycle-storage/1"
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

    fn lifecycle_storage_requested(&self) -> bool {
        let Some(lifecycle) = self.lifecycle.clone() else {
            return false;
        };
        serde_json::from_value::<ProgressiveLifecycle>(lifecycle).is_ok_and(|lifecycle| {
            lifecycle.mechanics == "public-certification-v1"
                && lifecycle.phases == Phase::ALL
                && lifecycle.evidence_schema == EVIDENCE_SCHEMA
                && lifecycle.storage_receipt == "graphforge-lifecycle-storage/1"
        })
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
    storage_receipt: String,
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
    pub receipts: Vec<serde_json::Value>,
}

pub trait PhaseExecutor {
    fn execute(&mut self, profile: &Profile, command: &PhaseCommand) -> Result<Execution, String>;
}

#[derive(Default)]
pub struct PublicProcessExecutor {
    lifecycle: LifecycleStorageSession,
}

#[derive(Default)]
struct LifecycleStorageSession {
    allocation: graphforge_storage::StorageAllocationLifecycle,
    transient_peak_storage_bytes: u64,
    source_project_current_allocated_bytes: Option<u64>,
    generator_observed: bool,
    construction_peak_observed: bool,
    source_project_observed: bool,
    portable_package_observed: bool,
    portable_import_peak_observed: bool,
    imported_project_observed: bool,
    finalized: bool,
}

impl LifecycleStorageSession {
    fn observe(
        &mut self,
        phase: Phase,
        command: &PhaseCommand,
        receipts: &[serde_json::Value],
    ) -> Result<Option<serde_json::Value>, String> {
        if self.finalized {
            return Err("lifecycle storage session was already finalized".to_owned());
        }
        let phase_baseline_allocated_bytes = self.allocation.current_allocated_bytes();
        let commands: Vec<&[String]> = match &command.action {
            PhaseAction::BenchmarkGenerator { args, .. } | PhaseAction::GraphForgeCli { args } => {
                vec![args]
            }
            PhaseAction::GraphForgeCliWorkflow { commands } => {
                commands.iter().map(Vec::as_slice).collect()
            }
        };
        let mut files = BTreeMap::new();
        for args in &commands {
            for flag in ["--nodes", "--edges", "--output"] {
                if let Some(path) = argument_path(args, flag) {
                    if path.is_file() {
                        merge_file_identity(&mut files, path)?;
                    } else if flag == "--output" && path.is_dir() {
                        merge_directory_identities(&mut files, path)?;
                    }
                }
            }
        }
        if !files.is_empty() {
            self.allocation
                .replace_owner(format!("phase-{phase}"), &files)
                .map_err(|error| error.to_string())?;
            if phase == Phase::Generate {
                self.generator_observed = true;
            } else if phase == Phase::Export {
                self.portable_package_observed = true;
            }
        }
        if let Some(project) = commands
            .iter()
            .find_map(|args| argument_path(args, "--project"))
        {
            if project.join("FORMAT").is_file() && project.join("CURRENT").is_file() {
                let selected = graphforge_storage::resolve_project_generation(project)
                    .map_err(|error| error.to_string())?;
                let union = graphforge_storage::capture_project_storage_identity_union(&selected)
                    .map_err(|error| error.to_string())?;
                let owner = if matches!(phase, Phase::CleanImport | Phase::ReopenProof) {
                    self.imported_project_observed = true;
                    "imported-project"
                } else {
                    self.source_project_observed = true;
                    if phase == Phase::Reopen {
                        self.source_project_current_allocated_bytes = Some(union.allocated_bytes);
                    }
                    "source-project"
                };
                self.allocation
                    .replace_owner(owner, &union.physical_identity_allocated_bytes)
                    .map_err(|error| error.to_string())?;
            }
        }
        for receipt in receipts {
            if receipt.get("contract").and_then(serde_json::Value::as_str)
                == Some("graphforge-import-session/1")
                && receipt.get("outcome").and_then(serde_json::Value::as_str) == Some("committed")
            {
                let transient = receipt
                    .get("construction")
                    .and_then(|value| value.get("transient_peak_allocated_bytes"))
                    .and_then(serde_json::Value::as_u64)
                    .ok_or_else(|| {
                        "committed import omitted transient allocation evidence".to_owned()
                    })?;
                self.transient_peak_storage_bytes = self
                    .transient_peak_storage_bytes
                    .max(phase_baseline_allocated_bytes.saturating_add(transient));
                self.construction_peak_observed = true;
            }
            if receipt.get("contract").and_then(serde_json::Value::as_str)
                == Some("graphforge-portable-import/2")
            {
                let transient = receipt
                    .get("transient_peak_allocated_bytes")
                    .and_then(serde_json::Value::as_u64)
                    .ok_or_else(|| {
                        "portable import omitted transient allocation evidence".to_owned()
                    })?;
                self.transient_peak_storage_bytes = self
                    .transient_peak_storage_bytes
                    .max(phase_baseline_allocated_bytes.saturating_add(transient));
                self.portable_import_peak_observed = true;
            }
        }
        self.transient_peak_storage_bytes = self
            .transient_peak_storage_bytes
            .max(self.allocation.peak_allocated_bytes());
        if phase != Phase::ReopenProof {
            return Ok(None);
        }
        if !self.generator_observed
            || !self.construction_peak_observed
            || !self.source_project_observed
            || !self.portable_package_observed
            || !self.portable_import_peak_observed
            || !self.imported_project_observed
            || self.source_project_current_allocated_bytes.is_none()
        {
            return Err("lifecycle storage session is missing an authenticated owner or transient phase"
                .to_owned());
        }
        let source_project_current_allocated_bytes = self
            .source_project_current_allocated_bytes
            .ok_or_else(|| "source-project reopen allocation was not captured".to_owned())?;
        self.finalized = true;
        let retained = self.allocation.current_allocated_bytes();
        let peak = self.transient_peak_storage_bytes.max(retained);
        Ok(Some(serde_json::json!({
            "contract": "graphforge-lifecycle-storage/1",
            "source_project_current_allocated_bytes": source_project_current_allocated_bytes,
            "retained_storage_bytes": retained,
            "transient_peak_storage_bytes": peak,
        })))
    }
}

fn argument_path<'a>(args: &'a [String], flag: &str) -> Option<&'a Path> {
    args.windows(2)
        .find(|values| values[0] == flag)
        .map(|values| Path::new(&values[1]))
}

fn merge_file_identity(identities: &mut BTreeMap<String, u64>, path: &Path) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| "lifecycle allocation path has no parent".to_owned())?;
    let name = path
        .file_name()
        .ok_or_else(|| "lifecycle allocation path has no file name".to_owned())?;
    let directory = graphforge_filesystem::StableDirectory::open(parent)
        .map_err(|_| "lifecycle allocation parent could not be retained".to_owned())?;
    let file = directory
        .open_child_file(name)
        .map_err(|_| "lifecycle allocation owner is not a stable regular file".to_owned())?;
    merge_open_file_identity(identities, &file)
}

fn merge_directory_identities(
    identities: &mut BTreeMap<String, u64>,
    path: &Path,
) -> Result<(), String> {
    let directory = graphforge_filesystem::StableDirectory::open(path)
        .map_err(|_| "lifecycle allocation directory could not be retained".to_owned())?;
    let mut remaining = 1_000_000_usize;
    merge_stable_directory_identities(identities, &directory, &mut remaining)
}

fn merge_stable_directory_identities(
    identities: &mut BTreeMap<String, u64>,
    directory: &graphforge_filesystem::StableDirectory,
    remaining: &mut usize,
) -> Result<(), String> {
    let names = directory
        .child_names_bounded(*remaining)
        .map_err(|_| "lifecycle allocation directory exceeds identity bound".to_owned())?;
    *remaining = remaining.saturating_sub(names.len());
    for name in names {
        match directory.open_child_directory(&name) {
            Ok(child) => merge_stable_directory_identities(identities, &child, remaining)?,
            Err(_) => {
                let file = directory.open_child_file(&name).map_err(|_| {
                    "lifecycle allocation entry is not an authenticated file or directory"
                        .to_owned()
                })?;
                merge_open_file_identity(identities, &file)?;
            }
        }
    }
    Ok(())
}

fn merge_open_file_identity(
    identities: &mut BTreeMap<String, u64>,
    file: &fs::File,
) -> Result<(), String> {
    let identity = graphforge_filesystem::file_identity(file)
        .map_err(|_| "lifecycle allocation identity unavailable".to_owned())?;
    let usage = graphforge_filesystem::file_space_usage(file)
        .map_err(|_| "lifecycle allocation usage unavailable".to_owned())?;
    let key = format!(
        "{:016x}:{}",
        identity.volume_serial,
        identity
            .file_id
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    );
    match identities.insert(key, usage.allocated_bytes) {
        Some(existing) if existing != usage.allocated_bytes => {
            Err("lifecycle allocation changed during observation".to_owned())
        }
        _ => Ok(()),
    }
}

impl PhaseExecutor for PublicProcessExecutor {
    fn execute(&mut self, profile: &Profile, command: &PhaseCommand) -> Result<Execution, String> {
        let produce_lifecycle_storage = profile.lifecycle_storage_requested();
        if let PhaseAction::GraphForgeCliWorkflow { commands } = &command.action {
            let started = Instant::now();
            let mut peak_rss_bytes = None;
            let mut receipts = Vec::new();
            for args in commands {
                let execution = match execute_process(&profile.executable, args) {
                    Ok(execution) => execution,
                    Err(_) => {
                        return Ok(Execution {
                            exit_code: None,
                            duration_ms: millis(started.elapsed()),
                            peak_rss_bytes,
                            failure: Some(FailureKind::CommandUnavailable),
                            receipts,
                        });
                    }
                };
                peak_rss_bytes = max_optional(peak_rss_bytes, execution.peak_rss_bytes);
                receipts.extend(execution.receipts);
                if execution.exit_code != Some(0) {
                    return Ok(Execution {
                        exit_code: execution.exit_code,
                        duration_ms: millis(started.elapsed()),
                        peak_rss_bytes,
                        failure: execution.failure,
                        receipts,
                    });
                }
            }
            let mut result = Execution {
                exit_code: Some(0),
                duration_ms: millis(started.elapsed()),
                peak_rss_bytes,
                failure: None,
                receipts,
            };
            if produce_lifecycle_storage {
                match self
                    .lifecycle
                    .observe(command.phase, command, &result.receipts)
                {
                    Ok(Some(receipt)) => result.receipts.push(receipt),
                    Ok(None) => {}
                    Err(_) => result.failure = Some(FailureKind::EvidenceInvalid),
                }
            }
            return Ok(result);
        }
        let (executable, args) = match &command.action {
            PhaseAction::BenchmarkGenerator {
                executable, args, ..
            } => (executable.as_str(), args.as_slice()),
            PhaseAction::GraphForgeCli { args } => (profile.executable.as_str(), args.as_slice()),
            PhaseAction::GraphForgeCliWorkflow { .. } => unreachable!("handled above"),
        };
        let mut result = execute_process(executable, args)?;
        if result.exit_code == Some(0) && produce_lifecycle_storage {
            match self
                .lifecycle
                .observe(command.phase, command, &result.receipts)
            {
                Ok(Some(receipt)) => result.receipts.push(receipt),
                Ok(None) => {}
                Err(_) => result.failure = Some(FailureKind::EvidenceInvalid),
            }
        }
        Ok(result)
    }
}

fn execute_process(executable: &str, args: &[String]) -> Result<Execution, String> {
    let started = Instant::now();
    let mut child = Command::new(executable)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|_| "public command could not start".to_owned())?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "public command stdout unavailable".to_owned())?;
    let stdout_reader = thread::spawn(move || read_bounded(stdout, 1_048_576));
    let mut peak_rss_bytes = None;
    loop {
        peak_rss_bytes = max_optional(peak_rss_bytes, resident_bytes(child.id()));
        if let Some(status) = child
            .try_wait()
            .map_err(|_| "public command wait failed".to_owned())?
        {
            let stdout = stdout_reader
                .join()
                .map_err(|_| "public command stdout reader failed".to_owned())??;
            let receipts = match parse_receipts(
                &stdout,
                status.success() && args.iter().any(|argument| argument == "--json"),
            ) {
                Ok(receipts) => receipts,
                Err(_) if status.success() => {
                    release_cgroup_page_cache();
                    return Ok(Execution {
                        exit_code: status.code(),
                        duration_ms: millis(started.elapsed()),
                        peak_rss_bytes,
                        failure: Some(FailureKind::EvidenceInvalid),
                        receipts: Vec::new(),
                    });
                }
                Err(error) => {
                    release_cgroup_page_cache();
                    return Err(error);
                }
            };
            release_cgroup_page_cache();
            return Ok(Execution {
                exit_code: status.code(),
                duration_ms: millis(started.elapsed()),
                peak_rss_bytes,
                failure: None,
                receipts,
            });
        }
        thread::sleep(Duration::from_millis(10));
    }
}

/// Drop Linux page-cache pressure attributed to the BenchExec cgroup between
/// sequential public-command invocations. Without this, file-backed cache from
/// prior `gf` subprocesses accumulates until the 4 GiB memlimit even though
/// each invocation's anonymous RSS stays bounded (#904).
#[cfg(target_os = "linux")]
fn release_cgroup_page_cache() {
    use std::io::Write;

    let _ = std::process::Command::new("sync").status();
    if let Ok(mut drop_caches) = fs::File::create("/proc/sys/vm/drop_caches") {
        let _ = drop_caches.write_all(b"3");
    }
}

#[cfg(not(target_os = "linux"))]
fn release_cgroup_page_cache() {}

fn read_bounded(mut input: impl Read, limit: usize) -> Result<Vec<u8>, String> {
    let mut kept = Vec::new();
    let mut chunk = [0_u8; 8_192];
    let mut exceeded = false;
    loop {
        let read = input
            .read(&mut chunk)
            .map_err(|_| "public command stdout read failed".to_owned())?;
        if read == 0 {
            break;
        }
        if kept.len().saturating_add(read) <= limit {
            kept.extend_from_slice(&chunk[..read]);
        } else {
            exceeded = true;
        }
    }
    if exceeded {
        Err("public command receipt exceeded one MiB".to_owned())
    } else {
        Ok(kept)
    }
}

fn parse_receipts(stdout: &[u8], expected: bool) -> Result<Vec<serde_json::Value>, String> {
    if !expected {
        return Ok(Vec::new());
    }
    if stdout.is_empty() {
        return Err("public command omitted its JSON receipt".to_owned());
    }
    let text =
        std::str::from_utf8(stdout).map_err(|_| "public receipt was not UTF-8".to_owned())?;
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            let value: serde_json::Value = serde_json::from_str(line)
                .map_err(|_| "public receipt was not one JSON object per line".to_owned())?;
            sanitize_receipt(&value)
                .ok_or_else(|| "public receipt contract is not allowlisted".to_owned())
        })
        .collect()
}

fn sanitize_receipt(value: &serde_json::Value) -> Option<serde_json::Value> {
    let object = value.as_object()?;
    match object.get("contract").and_then(serde_json::Value::as_str) {
        Some("graphforge-import-session/1") => {
            if !object
                .get("outcome")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|outcome| {
                    matches!(
                        outcome,
                        "begun"
                            | "resumed"
                            | "registered"
                            | "checkpointed"
                            | "validated"
                            | "committed"
                            | "aborted"
                    )
                })
            {
                return None;
            }
            if object
                .get("construction")
                .is_some_and(|construction| !sanitized_construction_tree(construction))
            {
                return None;
            }
            let mut receipt = serde_json::Map::new();
            for key in [
                "contract",
                "outcome",
                "rows_accepted",
                "rows_rejected",
                "bytes_accepted",
                "construction",
            ] {
                if let Some(item) = object.get(key) {
                    receipt.insert(key.to_owned(), item.clone());
                }
            }
            Some(receipt.into())
        }
        Some("graphforge-result-sink/1") => sanitize_legacy_result_sink(object),
        Some("graphforge-result-sink/2") => sanitize_result_sink(object),
        Some("graphforge-storage-attribution-command/1") => sanitize_storage_command(object),
        Some("graphforge-lifecycle-storage/1") => copy_closed_receipt(
            object,
            &[
                "contract",
                "source_project_current_allocated_bytes",
                "retained_storage_bytes",
                "transient_peak_storage_bytes",
            ],
        )
        .filter(|receipt| {
            sanitized_numeric_fields(
                receipt,
                &[
                    "source_project_current_allocated_bytes",
                    "retained_storage_bytes",
                    "transient_peak_storage_bytes",
                ],
            )
        }),
        Some("graphforge-query-qualification/1") => copy_closed_receipt(
            object,
            &[
                "contract",
                "live_nodes",
                "live_edges",
                "one_hop_rows",
                "two_hop_rows",
                "source_fingerprint",
                "imported_fingerprint",
                "equivalent",
            ],
        ),
        Some("graphforge-portable-import/2") => copy_selected_receipt(
            object,
            &[
                "contract",
                "package_digest",
                "transport_digest",
                "idempotent_replay",
                "transient_peak_allocated_bytes",
            ],
        )
        .filter(|receipt| {
            sanitized_numeric_fields(receipt, &["transient_peak_allocated_bytes"])
        }),
        Some(contract) if contract.starts_with("graphforge-portable-") => copy_selected_receipt(
            object,
            &[
                "contract",
                "package_digest",
                "transport_digest",
                "entry_count",
                "payload_bytes",
                "representation",
                "selection_fingerprint",
                "integrity",
                "compatibility",
                "idempotent_replay",
            ],
        ),
        _ if object.contains_key("selected_generation_uuid") => {
            if !object
                .get("kind")
                .and_then(serde_json::Value::as_str)
                .is_some_and(is_safe_token)
            {
                return None;
            }
            let mut receipt = serde_json::Map::new();
            for key in [
                "kind",
                "selected_generation_class",
                "work_detected",
                "repaired_journals",
                "aborted_journals",
                "removed_generations",
                "preserved_unknown_entries",
                "deferred",
                "elapsed_ms",
            ] {
                if let Some(item) = object.get(key) {
                    receipt.insert(key.to_owned(), item.clone());
                }
            }
            Some(receipt.into())
        }
        _ => None,
    }
}

fn sanitized_numeric_tree(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => true,
        serde_json::Value::Array(items) => items.iter().all(sanitized_numeric_tree),
        serde_json::Value::Object(items) => items.iter().all(|(key, value)| {
            key.len() <= 80
                && key
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
                && sanitized_numeric_tree(value)
        }),
        serde_json::Value::String(_) => false,
    }
}

fn sanitized_construction_tree(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Object(items) => items.iter().all(|(key, value)| {
            key.len() <= 80
                && key
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
                && if key == "contract" {
                    value.as_str() == Some("graphforge-publication-work/1")
                } else {
                    sanitized_construction_tree(value)
                }
        }),
        _ => sanitized_numeric_tree(value),
    }
}

fn sanitized_numeric_fields(value: &serde_json::Value, keys: &[&str]) -> bool {
    value.as_object().is_some_and(|object| {
        keys.iter().all(|key| {
            object
                .get(*key)
                .and_then(serde_json::Value::as_u64)
                .is_some()
        })
    })
}

fn sanitize_storage_command(
    object: &serde_json::Map<String, serde_json::Value>,
) -> Option<serde_json::Value> {
    if object
        .keys()
        .any(|key| !matches!(key.as_str(), "contract" | "storage" | "reopen_agrees"))
        || object
            .get("reopen_agrees")
            .and_then(serde_json::Value::as_bool)
            != Some(true)
    {
        return None;
    }
    let storage = object.get("storage")?.as_object()?;
    const STORAGE_KEYS: [&str; 7] = [
        "contract",
        "categories",
        "logical_references",
        "logical_bytes",
        "retained_logical_eof_bytes",
        "allocated_physical_bytes",
        "physical_objects",
    ];
    if storage
        .keys()
        .any(|key| !STORAGE_KEYS.contains(&key.as_str()))
        || storage.get("contract").and_then(serde_json::Value::as_str)
            != Some("graphforge-storage-attribution/1")
        || !sanitized_numeric_fields(
            &serde_json::Value::Object(storage.clone()),
            &STORAGE_KEYS[2..],
        )
        || !sanitized_storage_categories(storage.get("categories")?)
    {
        return None;
    }
    Some(serde_json::json!({
        "contract": "graphforge-storage-attribution-command/1",
        "storage": storage,
        "reopen_agrees": true,
    }))
}

fn sanitize_legacy_result_sink(
    object: &serde_json::Map<String, serde_json::Value>,
) -> Option<serde_json::Value> {
    const KEYS: [&str; 7] = [
        "contract",
        "destination",
        "format",
        "rows",
        "batches",
        "bytes",
        "complete",
    ];
    if object.keys().any(|key| !KEYS.contains(&key.as_str()))
        || !matches!(
            object.get("format").and_then(serde_json::Value::as_str),
            Some("ArrowIpc" | "Parquet")
        )
        || object.get("complete").and_then(serde_json::Value::as_bool) != Some(true)
        || !sanitized_numeric_fields(
            &serde_json::Value::Object(object.clone()),
            &["rows", "batches", "bytes"],
        )
    {
        return None;
    }
    copy_selected_receipt(
        object,
        &["contract", "format", "rows", "batches", "bytes", "complete"],
    )
}

fn sanitize_result_sink(
    object: &serde_json::Map<String, serde_json::Value>,
) -> Option<serde_json::Value> {
    const KEYS: [&str; 10] = [
        "contract",
        "destination",
        "format",
        "rows",
        "batches",
        "bytes",
        "complete",
        "result_sha256",
        "scalar_u64",
        "query_evidence",
    ];
    let digest = object.get("result_sha256")?.as_str()?;
    if object.keys().any(|key| !KEYS.contains(&key.as_str()))
        || !matches!(
            object.get("format").and_then(serde_json::Value::as_str),
            Some("ArrowIpc" | "Parquet")
        )
        || object.get("complete").and_then(serde_json::Value::as_bool) != Some(true)
        || digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        || !sanitized_numeric_fields(
            &serde_json::Value::Object(object.clone()),
            &["rows", "batches", "bytes"],
        )
        || !object
            .get("scalar_u64")
            .is_some_and(|value| value.is_null() || value.as_u64().is_some())
        || !sanitized_query_evidence(object.get("query_evidence")?)
    {
        return None;
    }
    copy_selected_receipt(
        object,
        &[
            "contract",
            "format",
            "rows",
            "batches",
            "bytes",
            "complete",
            "result_sha256",
            "scalar_u64",
            "query_evidence",
        ],
    )
}

fn sanitized_query_evidence(value: &serde_json::Value) -> bool {
    const KEYS: [&str; 11] = [
        "contract",
        "hops",
        "sorts",
        "operator_rss",
        "max_in_flight_reads",
        "memory_reserved_before",
        "memory_reserved_after",
        "returned_batch_bytes",
        "execution_batch_rows",
        "peak_rss_bytes",
        "rss_after_release_bytes",
    ];
    let object = match value.as_object() {
        Some(object) => object,
        None => return false,
    };
    object.len() == KEYS.len()
        && object.keys().all(|key| KEYS.contains(&key.as_str()))
        && object.get("contract").and_then(serde_json::Value::as_str)
            == Some("graphforge-query-evidence/1")
        && sanitized_numeric_fields(value, &KEYS[4..])
        && sanitized_query_records(object.get("hops"), &QUERY_HOP_KEYS, None)
        && sanitized_query_records(object.get("sorts"), &QUERY_SORT_KEYS, Some("fetch_rows"))
        && sanitized_operator_rss(object.get("operator_rss"))
}

const QUERY_HOP_KEYS: [&str; 25] = [
    "ordinal",
    "input_batches",
    "input_rows",
    "candidates_generated",
    "rows_emitted",
    "projected_chunks",
    "projected_rows",
    "projected_columns",
    "edge_projected_columns",
    "node_projected_columns",
    "edge_reader_calls",
    "edge_rows_returned",
    "edge_logical_rows_scanned",
    "edge_full_reads",
    "node_reader_calls",
    "node_rows_returned",
    "node_logical_rows_scanned",
    "node_full_reads",
    "identity_reader_calls",
    "identity_logical_bytes",
    "identity_ranges_selected",
    "identity_peak_buffer_bytes",
    "identity_per_record_seeks",
    "identity_revalidation_calls",
    "identity_revalidation_bytes",
];

const QUERY_SORT_KEYS: [&str; 7] = [
    "ordinal",
    "fetch_rows",
    "output_rows",
    "spill_count",
    "spilled_rows",
    "spilled_bytes",
    "retained_bytes",
];

fn sanitized_query_records(
    value: Option<&serde_json::Value>,
    keys: &[&str],
    nullable: Option<&str>,
) -> bool {
    value
        .and_then(serde_json::Value::as_array)
        .is_some_and(|records| {
            records.iter().all(|record| {
                record.as_object().is_some_and(|object| {
                    object.len() == keys.len()
                        && object.keys().all(|key| keys.contains(&key.as_str()))
                        && keys.iter().all(|key| {
                            object.get(*key).is_some_and(|value| {
                                (nullable == Some(*key) && value.is_null())
                                    || value.as_u64().is_some()
                            })
                        })
                })
            })
        })
}

fn sanitized_operator_rss(value: Option<&serde_json::Value>) -> bool {
    const KEYS: [&str; 5] = [
        "ordinal",
        "operator",
        "before_bytes",
        "peak_bytes",
        "after_bytes",
    ];
    value
        .and_then(serde_json::Value::as_array)
        .is_some_and(|records| {
            records.iter().all(|record| {
                record.as_object().is_some_and(|object| {
                    object.len() == KEYS.len()
                        && object.keys().all(|key| KEYS.contains(&key.as_str()))
                        && sanitized_numeric_fields(
                            record,
                            &["ordinal", "before_bytes", "peak_bytes", "after_bytes"],
                        )
                        && object
                            .get("operator")
                            .and_then(serde_json::Value::as_str)
                            .is_some_and(is_safe_token)
                })
            })
        })
}

fn sanitized_storage_categories(value: &serde_json::Value) -> bool {
    const CATEGORIES: [&str; 10] = [
        "topology_nodes",
        "topology_edges",
        "properties",
        "uuid_and_surrogates",
        "adjacency",
        "catalog_and_manifests",
        "construction_staging",
        "portable_package",
        "clean_imported_project",
        "other",
    ];
    const TOTAL_KEYS: [&str; 5] = [
        "logical_references",
        "logical_bytes",
        "physical_objects",
        "physical_logical_bytes",
        "allocated_bytes",
    ];
    value.as_object().is_some_and(|categories| {
        categories.len() == CATEGORIES.len()
            && CATEGORIES.iter().all(|category| {
                categories.get(*category).is_some_and(|totals| {
                    totals.as_object().is_some_and(|object| {
                        object.len() == TOTAL_KEYS.len()
                            && object.keys().all(|key| TOTAL_KEYS.contains(&key.as_str()))
                            && sanitized_numeric_fields(totals, &TOTAL_KEYS)
                    })
                })
            })
    })
}

fn copy_closed_receipt(
    object: &serde_json::Map<String, serde_json::Value>,
    keys: &[&str],
) -> Option<serde_json::Value> {
    if object.keys().any(|key| !keys.contains(&key.as_str())) {
        return None;
    }
    Some(
        keys.iter()
            .filter_map(|key| {
                object
                    .get(*key)
                    .map(|value| ((*key).to_owned(), value.clone()))
            })
            .collect::<serde_json::Map<_, _>>()
            .into(),
    )
}

fn copy_selected_receipt(
    object: &serde_json::Map<String, serde_json::Value>,
    keys: &[&str],
) -> Option<serde_json::Value> {
    Some(
        keys.iter()
            .filter_map(|key| {
                object
                    .get(*key)
                    .map(|value| ((*key).to_owned(), value.clone()))
            })
            .collect::<serde_json::Map<_, _>>()
            .into(),
    )
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
    EvidenceInvalid,
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub receipts: Vec<serde_json::Value>,
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
                    receipts: execution.receipts,
                }
            }
            Err(_) => PhaseOutcome {
                phase: command.phase,
                status: OutcomeStatus::Failed,
                duration_ms: 0,
                peak_rss_bytes: None,
                exit_code: None,
                failure: Some(FailureKind::CommandUnavailable),
                receipts: Vec::new(),
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
                    receipts: Vec::new(),
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
            Phase::Reopen => reopen_workflow_is_valid(commands),
            Phase::Recount | Phase::Query => query_workflow_is_valid(commands, 2),
            Phase::ReopenProof => reopen_proof_workflow_is_valid(commands),
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

fn query_workflow_is_valid(commands: &[Vec<String>], expected: usize) -> bool {
    commands.len() == expected
        && commands.iter().all(|args| {
            args.iter().all(|argument| !argument.contains('\0'))
                && contains_command(
                    &args.iter().map(String::as_str).collect::<Vec<_>>(),
                    &["query"],
                )
        })
}

fn reopen_workflow_is_valid(commands: &[Vec<String>]) -> bool {
    commands.len() == 2
        && cli_command_is_valid(&commands[0], &["recovery"])
        && cli_command_is_valid(&commands[1], &["storage-attribution"])
}

fn reopen_proof_workflow_is_valid(commands: &[Vec<String>]) -> bool {
    commands.len() == 5
        && query_workflow_is_valid(&commands[..4], 4)
        && cli_command_is_valid(&commands[4], &["storage-attribution"])
}

fn cli_command_is_valid(args: &[String], operation: &[&str]) -> bool {
    args.iter().all(|argument| !argument.contains('\0'))
        && contains_command(
            &args.iter().map(String::as_str).collect::<Vec<_>>(),
            operation,
        )
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

    #[test]
    fn child_receipts_are_bounded_allowlisted_and_strip_content_bearing_paths() {
        let query = br#"{"contract":"graphforge-result-sink/1","destination":"/secret/result.arrow","format":"ArrowIpc","rows":7,"batches":1,"bytes":99,"complete":true}"#;
        let receipts = parse_receipts(query, true).expect("allowlisted query receipt");
        assert_eq!(receipts.len(), 1);
        assert!(receipts[0].get("destination").is_none());
        assert_eq!(receipts[0]["rows"], 7);
        assert!(parse_receipts(br#"{"contract":"unknown/1"}"#, true).is_err());
        assert!(parse_receipts(&[], true).is_err());
        assert!(parse_receipts(b"human output\n", false).unwrap().is_empty());
        let import = br#"{"contract":"graphforge-import-session/1","outcome":"committed","construction":{"configured_batch_rows":65536,"publication_work":{"contract":"graphforge-publication-work/1","semantic_total_operations":9}}}"#;
        assert_eq!(parse_receipts(import, true).unwrap().len(), 1);
        let leaked = br#"{"contract":"graphforge-import-session/1","outcome":"committed","construction":{"project_path":"/secret"}}"#;
        assert!(parse_receipts(leaked, true).is_err());
        let query = serde_json::json!({
            "contract": "graphforge-result-sink/2",
            "destination": "/secret/query.arrow",
            "format": "ArrowIpc",
            "rows": 1,
            "batches": 1,
            "bytes": 64,
            "complete": true,
            "result_sha256": "a".repeat(64),
            "scalar_u64": 7,
            "query_evidence": {
                "contract": "graphforge-query-evidence/1",
                "hops": [],
                "sorts": [],
                "operator_rss": [],
                "max_in_flight_reads": 0,
                "memory_reserved_before": 0,
                "memory_reserved_after": 0,
                "returned_batch_bytes": 8,
                "execution_batch_rows": 8192,
                "peak_rss_bytes": 0,
                "rss_after_release_bytes": 0
            }
        });
        let encoded = serde_json::to_vec(&query).expect("query receipt JSON");
        let sanitized = parse_receipts(&encoded, true).expect("closed query receipt");
        assert!(sanitized[0].get("destination").is_none());
        let mut leaked_query = query;
        leaked_query["query_evidence"]["project_path"] = serde_json::json!("/secret");
        let encoded = serde_json::to_vec(&leaked_query).expect("leaked query receipt JSON");
        assert!(parse_receipts(&encoded, true).is_err());
        let recovery = serde_json::json!({
            "kind": "project_open",
            "selected_generation_uuid": "00000000-0000-4000-8000-000000000001",
            "selected_generation_class": "published",
            "work_detected": false,
            "repaired_journals": 0,
            "aborted_journals": 0,
            "removed_generations": 0,
            "preserved_unknown_entries": 0,
            "deferred": null,
            "elapsed_ms": 1
        });
        let encoded = serde_json::to_vec(&recovery).expect("recovery receipt JSON");
        let sanitized = parse_receipts(&encoded, true).expect("ordinary recovery receipt");
        assert_eq!(sanitized[0]["kind"], "project_open");
        let mut leaked_recovery = recovery;
        leaked_recovery["kind"] = serde_json::json!("/private/project");
        let encoded = serde_json::to_vec(&leaked_recovery).expect("leaked recovery receipt JSON");
        assert!(parse_receipts(&encoded, true).is_err());
    }

    #[test]
    fn storage_receipts_are_closed_and_preserve_only_semantic_attribution() {
        let totals = serde_json::json!({
            "logical_references": 0,
            "logical_bytes": 0,
            "physical_objects": 0,
            "physical_logical_bytes": 0,
            "allocated_bytes": 0
        });
        let categories = [
            "topology_nodes",
            "topology_edges",
            "properties",
            "uuid_and_surrogates",
            "adjacency",
            "catalog_and_manifests",
            "construction_staging",
            "portable_package",
            "clean_imported_project",
            "other",
        ]
        .into_iter()
        .map(|name| (name.to_owned(), totals.clone()))
        .collect::<serde_json::Map<_, _>>();
        let receipt = serde_json::json!({
            "contract": "graphforge-storage-attribution-command/1",
            "storage": {
                "contract": "graphforge-storage-attribution/1",
                "categories": categories,
                "logical_references": 0,
                "logical_bytes": 0,
                "retained_logical_eof_bytes": 64,
                "allocated_physical_bytes": 128,
                "physical_objects": 1
            },
            "reopen_agrees": true
        });
        let encoded = serde_json::to_vec(&receipt).expect("storage receipt JSON");
        let sanitized = parse_receipts(&encoded, true).expect("closed storage receipt");
        assert_eq!(sanitized[0]["storage"]["allocated_physical_bytes"], 128);

        let mut leaked = receipt;
        leaked["storage"]["project_path"] = serde_json::json!("/secret");
        let encoded = serde_json::to_vec(&leaked).expect("leaked receipt JSON");
        assert!(parse_receipts(&encoded, true).is_err());

        let lifecycle = serde_json::json!({
            "contract": "graphforge-lifecycle-storage/1",
            "source_project_current_allocated_bytes": 256,
            "retained_storage_bytes": 384,
            "transient_peak_storage_bytes": 512
        });
        let encoded = serde_json::to_vec(&lifecycle).expect("lifecycle receipt JSON");
        let sanitized = parse_receipts(&encoded, true).expect("closed lifecycle receipt");
        assert_eq!(
            sanitized[0]["source_project_current_allocated_bytes"],
            256
        );
        for invalid in [
            serde_json::Value::Null,
            serde_json::json!(true),
            serde_json::json!(-1),
            serde_json::json!("256"),
        ] {
            let mut malformed = lifecycle.clone();
            malformed["source_project_current_allocated_bytes"] = invalid;
            let encoded =
                serde_json::to_vec(&malformed).expect("malformed lifecycle receipt JSON");
            assert!(parse_receipts(&encoded, true).is_err());
        }
        let mut missing = lifecycle;
        missing
            .as_object_mut()
            .expect("lifecycle receipt object")
            .remove("source_project_current_allocated_bytes");
        let encoded = serde_json::to_vec(&missing).expect("incomplete lifecycle receipt JSON");
        assert!(parse_receipts(&encoded, true).is_err());
    }

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
            "evidence_schema": EVIDENCE_SCHEMA,
            "storage_receipt": "graphforge-lifecycle-storage/1"
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
            receipts: Vec::new(),
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
            receipts: Vec::new(),
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
    fn public_profile_lifecycle_storage_is_explicit_and_typed() {
        let mut profile = tiny_profile();
        assert!(!profile.lifecycle_storage_requested());
        profile.lifecycle = progressive_profile().lifecycle;
        assert!(profile.lifecycle_storage_requested());
        assert_eq!(profile.validate(), Ok(()));
        profile.lifecycle.as_mut().unwrap()["storage_receipt"] = serde_json::json!("unknown/1");
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
        let mut executor = PublicProcessExecutor::default();
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
        let result = PublicProcessExecutor::default()
            .execute(&profile, &profile.phases[2])
            .unwrap();
        assert_eq!(result.exit_code, Some(23));
        let calls = fs::read_to_string(calls).unwrap();
        assert_eq!(calls.lines().count(), 4);
        assert!(!calls.contains("import-session commit"));
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn public_executor_classifies_invalid_success_receipt_as_evidence_invalid() {
        let root = std::env::temp_dir().join(format!(
            "gf-certify-invalid-receipt-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let executable = root.join("gf");
        fs::write(
            &executable,
            "#!/bin/sh\nprintf '%s\\n' '{\"contract\":\"graphforge-portable-import/2\"}'\n",
        )
        .unwrap();
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o755)).unwrap();
        let result = execute_process(
            executable.to_str().unwrap(),
            &["--json".to_owned()],
        )
        .unwrap();
        assert_eq!(result.exit_code, Some(0));
        assert_eq!(result.failure, Some(FailureKind::EvidenceInvalid));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn reopen_emits_authoritative_source_project_union_allocation() {
        let root = std::env::temp_dir().join(format!(
            "gf-certify-source-union-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let project = root.join("project");
        let current =
            graphforge_storage::open_or_initialize_ephemeral_project(&project).unwrap();
        let selected = graphforge_storage::capture_storage_attribution(&current).unwrap();
        let union =
            graphforge_storage::capture_project_storage_identity_union(&current).unwrap();
        assert!(
            union
                .retained_generation_uuids
                .contains(&current.generation_uuid())
        );
        assert!(union.allocated_bytes > selected.allocated_bytes);

        let mut session = LifecycleStorageSession::default();
        let reopen = PhaseCommand {
            phase: Phase::Reopen,
            action: PhaseAction::GraphForgeCli {
                args: vec![
                    "--project".to_owned(),
                    project.to_string_lossy().into_owned(),
                    "storage-attribution".to_owned(),
                ],
            },
        };
        assert!(session.observe(Phase::Reopen, &reopen, &[]).unwrap().is_none());
        session.generator_observed = true;
        session.construction_peak_observed = true;
        session.portable_package_observed = true;
        session.portable_import_peak_observed = true;
        session.imported_project_observed = true;
        let final_command = PhaseCommand {
            phase: Phase::ReopenProof,
            action: PhaseAction::GraphForgeCli { args: Vec::new() },
        };
        let receipt = session
            .observe(Phase::ReopenProof, &final_command, &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            receipt["source_project_current_allocated_bytes"].as_u64(),
            Some(union.allocated_bytes)
        );
        assert_ne!(
            receipt["source_project_current_allocated_bytes"].as_u64(),
            Some(selected.allocated_bytes)
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn lifecycle_storage_deduplicates_aliases_and_finalizes_once() {
        let root =
            std::env::temp_dir().join(format!("gf-certify-lifecycle-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let nodes = root.join("nodes.parquet");
        let alias = root.join("edges.parquet");
        fs::write(&nodes, vec![7_u8; 8_192]).unwrap();
        fs::hard_link(&nodes, &alias).unwrap();
        let mut session = LifecycleStorageSession::default();
        let generate = PhaseCommand {
            phase: Phase::Generate,
            action: PhaseAction::BenchmarkGenerator {
                identity: format!("sha256:{}", "0".repeat(64)),
                executable: "generator".to_owned(),
                args: vec![
                    "--nodes".to_owned(),
                    nodes.to_string_lossy().into_owned(),
                    "--edges".to_owned(),
                    alias.to_string_lossy().into_owned(),
                ],
            },
        };
        assert!(
            session
                .observe(Phase::Generate, &generate, &[])
                .unwrap()
                .is_none()
        );
        let one_identity_allocation = session.allocation.current_allocated_bytes();
        assert!(one_identity_allocation > 0);

        let ingest = PhaseCommand {
            phase: Phase::Ingest,
            action: PhaseAction::GraphForgeCli { args: Vec::new() },
        };
        let committed = serde_json::json!({
            "contract": "graphforge-import-session/1",
            "outcome": "committed",
            "construction": {"transient_peak_allocated_bytes": 4_096}
        });
        session
            .observe(Phase::Ingest, &ingest, &[committed])
            .unwrap();
        let expanded = root.join("portable-expanded");
        let nested = expanded.join("graph");
        fs::create_dir_all(&nested).unwrap();
        fs::hard_link(&nodes, nested.join("nodes.parquet")).unwrap();
        let export = PhaseCommand {
            phase: Phase::Export,
            action: PhaseAction::GraphForgeCli {
                args: vec![
                    "--output".to_owned(),
                    expanded.to_string_lossy().into_owned(),
                ],
            },
        };
        session.observe(Phase::Export, &export, &[]).unwrap();
        assert!(session.portable_package_observed);
        session.source_project_observed = true;
        session.source_project_current_allocated_bytes = Some(one_identity_allocation);
        session.portable_import_peak_observed = true;
        session.imported_project_observed = true;

        let final_command = PhaseCommand {
            phase: Phase::ReopenProof,
            action: PhaseAction::GraphForgeCli { args: Vec::new() },
        };
        let receipt = session
            .observe(Phase::ReopenProof, &final_command, &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            receipt["source_project_current_allocated_bytes"].as_u64(),
            Some(one_identity_allocation)
        );
        assert_eq!(
            receipt["retained_storage_bytes"].as_u64(),
            Some(one_identity_allocation)
        );
        assert_eq!(
            receipt["transient_peak_storage_bytes"].as_u64(),
            Some(one_identity_allocation.saturating_add(4_096))
        );
        assert!(
            session
                .observe(Phase::ReopenProof, &final_command, &[])
                .unwrap_err()
                .contains("already finalized")
        );
        let encoded = serde_json::to_string(&receipt).unwrap();
        for forbidden in ["path", "uuid", "identity", "volume", "file_id"] {
            assert!(!encoded.contains(forbidden));
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn lifecycle_storage_refuses_close_with_missing_ordinary_authority() {
        let final_command = PhaseCommand {
            phase: Phase::ReopenProof,
            action: PhaseAction::GraphForgeCli { args: Vec::new() },
        };
        let error = LifecycleStorageSession::default()
            .observe(Phase::ReopenProof, &final_command, &[])
            .unwrap_err();
        assert!(error.contains("missing an authenticated owner or transient phase"));
    }

}
