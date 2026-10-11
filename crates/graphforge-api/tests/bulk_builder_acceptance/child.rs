//! Builds that run in a separate process: killed at a failpoint, held to a
//! memory budget, or pinned to a clock.
//!
//! The parent re-executes this test binary on the one `child` test, which does
//! nothing outside a child process. The child opens the project, begins an
//! import session or resumes one, validates it and optionally commits it,
//! printing one `NAME {json}` line per milestone. A parent reading a killed
//! child therefore knows which session to resume.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use graphforge_api::{GraphForge, ImportPhase};
use graphforge_core::{GfError, ProjectErrorCode};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::support::*;

const CHILD_ENV: &str = "GF_ACCEPT_CHILD";
/// The session clock every build is pinned to, so two builds of one input are
/// comparable byte for byte (`GF_TEST_SESSION_NOW_MICROS`, a `test-support`
/// feature of the storage crate).
pub const CLOCK: i64 = 1_789_000_000_000_000;
const FAILPOINT_COOKIE: &str = "graphforge-construction-test-v1";
const PROJECT_FAILPOINT_COOKIE: &str = "graphforge-internal-subprocess-v1";
/// The exit status of a process that hit its failpoint.
pub const KILLED: i32 = 86;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ChildSpec {
    pub project: PathBuf,
    /// Register these sources in a new session, or resume `session`.
    pub sources: Option<PathBuf>,
    pub session: Option<Uuid>,
    pub lanes: Option<usize>,
    pub commit: bool,
}

/// What a child printed, by milestone.
#[derive(Debug)]
pub struct Outcome {
    pub code: Option<i32>,
    pub lines: BTreeMap<String, serde_json::Value>,
    pub stderr: String,
}

impl Outcome {
    pub fn session(&self) -> Uuid {
        self.lines["SESSION"].as_str().unwrap().parse().unwrap()
    }

    pub fn succeeded(&self) -> bool {
        self.code == Some(0) && self.lines.contains_key("DONE")
    }

    pub fn validated(&self) -> &serde_json::Value {
        &self.lines["VALIDATED"]
    }

    pub fn inventory(&self) -> BTreeMap<String, (u64, String)> {
        serde_json::from_value(self.validated()["inventory"].clone()).unwrap()
    }

    pub fn report(&self) -> &serde_json::Value {
        self.lines.get("VALIDATED").map_or(
            &serde_json::Value::Null,
            |validated| &validated["bulk_build"],
        )
    }

    pub fn peak_rss_bytes(&self) -> u64 {
        self.lines["DONE"]["peak_rss_bytes"].as_u64().unwrap()
    }
}

fn status_kib(field: &str) -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .unwrap()
        .lines()
        .find_map(|line| line.strip_prefix(field))
        .and_then(|rest| rest.split_whitespace().next()?.parse().ok())
        .unwrap_or(0)
}

fn emit(name: &str, value: &serde_json::Value) {
    // libtest prefixes the first line of uncaptured output with the test name.
    println!("\n@@{name} {value}");
}

/// The child: run the build `$GF_ACCEPT_CHILD` describes.
#[test]
fn child() {
    let Ok(spec) = std::env::var(CHILD_ENV) else {
        return;
    };
    let spec: ChildSpec = serde_json::from_str(&spec).unwrap();
    let graph = match spec.lanes {
        Some(lanes) => forge_with_lanes(&spec.project, lanes),
        None => GraphForge::new(spec.project.to_str()).unwrap(),
    };
    let mut session = if let Some(id) = spec.session {
        graph.resume_import_session(id).unwrap()
    } else {
        let sources = Sources::at(spec.sources.as_ref().expect("sources or a session"));
        register(&graph, &sources)
    };
    emit("SESSION", &session.session_uuid().to_string().into());
    let (phase, _) = session.status();
    emit("PHASE", &format!("{phase:?}").into());
    if phase == ImportPhase::Open {
        match session.validate(&graph) {
            Ok(progress) => {
                let report = progress.construction.and_then(|c| c.bulk_build);
                emit(
                    "VALIDATED",
                    &serde_json::json!({
                        "inventory": comparable_inventory(&spec.project),
                        "bulk_build": report,
                    }),
                );
            }
            Err(error) => {
                let resource_limit = matches!(
                    &error,
                    GfError::Project {
                        code: ProjectErrorCode::ResourceLimit,
                        ..
                    }
                );
                emit(
                    "REFUSED",
                    &serde_json::json!({"resource_limit": resource_limit, "message": error.to_string()}),
                );
                return;
            }
        }
    } else {
        // A resumed session that already validated: its inventory is pinned.
        emit(
            "VALIDATED",
            &serde_json::json!({
                "inventory": comparable_inventory(&spec.project),
                "bulk_build": serde_json::Value::Null,
            }),
        );
    }
    if spec.commit {
        match session.commit(&graph, None) {
            Ok(generation) => emit("COMMITTED", &generation.to_string().into()),
            Err(error) => {
                emit("COMMIT_REFUSED", &error.to_string().into());
                return;
            }
        }
    }
    emit(
        "DONE",
        &serde_json::json!({"peak_rss_bytes": status_kib("VmHWM:") * 1024}),
    );
}

/// Environment of a child: which failpoints it dies at and what memory it has.
#[derive(Clone, Debug, Default)]
pub struct Conditions {
    /// Exit at the first hit of this construction failpoint.
    pub construction_failpoint: Option<String>,
    /// Exit at this project-publication failpoint.
    pub project_failpoint: Option<String>,
    /// `GF_BULK_BUILD_MEMORY_BUDGET_BYTES`.
    pub budget: Option<u64>,
    /// The session clock, if not [`CLOCK`].
    pub clock: Option<i64>,
    /// Cap the address space, so a decode that ignores the budget aborts.
    pub address_space_kib: Option<u64>,
}

pub fn run(spec: &ChildSpec, conditions: &Conditions) -> Outcome {
    let executable = std::env::current_exe().unwrap();
    let mut command = Command::new("sh");
    command.arg("-c").arg(format!(
        "{}exec \"$0\" --exact child::child --nocapture --test-threads 1",
        conditions
            .address_space_kib
            .map_or_else(String::new, |kib| format!("ulimit -v {kib}; "))
    ));
    command
        .arg(executable)
        .env(CHILD_ENV, serde_json::to_string(spec).unwrap())
        .env(
            "GF_TEST_SESSION_NOW_MICROS",
            conditions.clock.unwrap_or(CLOCK).to_string(),
        );
    if let Some(name) = &conditions.construction_failpoint {
        command
            .env("GF_CONSTRUCTION_FAILPOINT_COOKIE", FAILPOINT_COOKIE)
            .env("GF_CONSTRUCTION_FAILPOINT", name);
    }
    if let Some(name) = &conditions.project_failpoint {
        command
            .env("GRAPHFORGE_PROJECT_FAILPOINTS", PROJECT_FAILPOINT_COOKIE)
            .env("GRAPHFORGE_PROJECT_FAILPOINT", name);
    }
    if let Some(budget) = conditions.budget {
        command.env("GF_BULK_BUILD_MEMORY_BUDGET_BYTES", budget.to_string());
    }
    let output = command.output().unwrap();
    let mut lines = BTreeMap::new();
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        for name in [
            "SESSION",
            "PHASE",
            "VALIDATED",
            "REFUSED",
            "COMMITTED",
            "COMMIT_REFUSED",
            "DONE",
        ] {
            if let Some(json) = line.strip_prefix(&format!("@@{name} ")) {
                lines.insert(name.to_owned(), serde_json::from_str(json).unwrap());
            }
        }
    }
    Outcome {
        code: output.status.code(),
        lines,
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    }
}

pub fn spec_for(project: &Path, sources: &Sources) -> ChildSpec {
    ChildSpec {
        project: project.to_owned(),
        sources: Some(sources.directory.clone()),
        ..ChildSpec::default()
    }
}
