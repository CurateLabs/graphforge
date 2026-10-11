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
/// The memory budget of a build that is meant to stay resident: far above the
/// resident estimate of any fixture here, and always set, so the route does not
/// depend on the host's headroom or on a budget in the caller's environment.
pub const RESIDENT_BUDGET: u64 = 16 << 30;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ChildSpec {
    pub project: PathBuf,
    /// Register these sources in a new session, or resume `session`.
    pub sources: Option<PathBuf>,
    pub session: Option<Uuid>,
    pub lanes: Option<usize>,
    pub commit: bool,
    /// Report the bytes the process wrote while validating, from the
    /// construction region counters of this process alone.
    pub measure: bool,
    /// Record every barrier the process makes and report, per published
    /// object, the file barriers its inode received (`BARRIERS`).
    pub audit: bool,
    /// Exit abruptly as soon as validation has finished, as a process lost
    /// before `commit` does. The exit status is [`KILLED`].
    pub abandon_after_validate: bool,
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
        let succeeded = self.code == Some(0) && self.lines.contains_key("DONE");
        if !succeeded && !self.stderr.is_empty() {
            eprintln!("child stderr:\n{}", self.stderr);
        }
        succeeded
    }

    pub fn validated(&self) -> &serde_json::Value {
        &self.lines["VALIDATED"]
    }

    pub fn inventory(&self) -> BTreeMap<String, (u64, String)> {
        serde_json::from_value(self.validated()["inventory"].clone()).unwrap()
    }

    pub fn barriers(&self) -> &serde_json::Value {
        &self.lines["BARRIERS"]
    }

    pub fn regions(&self) -> &serde_json::Value {
        &self.lines["REGIONS"]
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
    let audit = spec
        .audit
        .then(graphforge_filesystem::observation::BarrierAudit::start);
    if phase == ImportPhase::Open {
        let capture = spec
            .measure
            .then(|| graphforge_storage::concurrency_attribution::RegionCapture::start("import"));
        let validated = session.validate(&graph);
        if let Some(capture) = capture {
            let regions = capture.finish();
            let written = |suffix: &str| {
                regions
                    .regions
                    .iter()
                    .find(|(path, _)| path.ends_with(suffix))
                    .and_then(|(_, row)| row.inclusive.written_bytes)
            };
            emit(
                "REGIONS",
                &serde_json::json!({
                    "encoding_written": written("/canonical_encoding"),
                    "validate_written": written("/stage+seal"),
                }),
            );
        }
        match validated {
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
    if spec.abandon_after_validate {
        if let Some(audit) = audit {
            emit(
                "BARRIERS",
                &barrier_report(&spec.project, audit.finish(), false),
            );
        }
        std::process::exit(KILLED);
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
    if let Some(audit) = audit {
        emit(
            "BARRIERS",
            &barrier_report(&spec.project, audit.finish(), spec.commit),
        );
    }
    emit(
        "DONE",
        &serde_json::json!({"peak_rss_bytes": status_kib("VmHWM:") * 1024}),
    );
}

/// One file for the whole life of the process: an inode number and the
/// generation number the filesystem gave that inode. A filesystem reuses the
/// number of a deleted file but not its generation, so the number alone would
/// credit a deleted temporary's barriers to a later file, and a creation time,
/// which repeats within a coarse timer tick, would not separate them either.
type Inode = (u64, [u8; 16], u32);

/// The inode behind `path`. A filesystem that reports no generation number
/// cannot be audited, and says so rather than falling back to the bare number.
fn inode(path: &Path) -> Inode {
    let file = std::fs::File::open(path).unwrap();
    let identity = graphforge_filesystem::file_identity(&file).unwrap();
    let generation = graphforge_filesystem::inode_generation(&file).unwrap_or_else(|error| {
        panic!(
            "no inode generation for {}: {error}; barriers cannot be attributed to inodes",
            path.display()
        )
    });
    (identity.volume_serial, identity.file_id, generation)
}

/// What the barriers this process made reached. Each published object (one per
/// distinct digest) is found by inode: in the object store once committed,
/// else under the session's encoded tree, where it is the same inode.
fn barrier_report(
    project: &Path,
    log: graphforge_filesystem::observation::BarrierLog,
    committed: bool,
) -> serde_json::Value {
    let mut per_inode: BTreeMap<Inode, Vec<(usize, bool)>> = BTreeMap::new();
    for (sequence, record) in log.records.iter().enumerate() {
        per_inode
            .entry((
                record.identity.volume_serial,
                record.identity.file_id,
                record.generation,
            ))
            .or_default()
            .push((sequence, record.directory));
    }
    let sequences = |inode: Inode, directory: bool| -> Vec<usize> {
        per_inode.get(&inode).map_or_else(Vec::new, |reached| {
            reached
                .iter()
                .filter(|(_, is_directory)| *is_directory == directory)
                .map(|(sequence, _)| *sequence)
                .collect()
        })
    };
    let mut published = inventory(project);
    published.sort_by(|a, b| a.sha256.cmp(&b.sha256));
    published.dedup_by(|a, b| a.sha256 == b.sha256);
    let tree = construction_root(project).join("encoded-v1/graph");
    let mut objects = BTreeMap::new();
    let mut last_object = None::<usize>;
    let mut buckets_acknowledged = true;
    for artifact in &published {
        let object = object_path(project, &artifact.sha256);
        let path = if committed {
            object.clone()
        } else {
            tree.join(&artifact.path)
        };
        let files = sequences(inode(&path), false);
        objects.insert(artifact.path.clone(), files.len());
        last_object = last_object.max(files.iter().max().copied());
        if committed
            && let Some(last) = files.iter().max()
            && !sequences(inode(object.parent().unwrap()), true)
                .iter()
                .any(|sequence| sequence > last)
        {
            buckets_acknowledged = false;
        }
    }
    let current = committed.then(|| sequences(inode(&project.join("CURRENT")), false));
    serde_json::json!({
        // Barriers that reached a file the audit could not name; a correct run
        // has none, and a test asserts so.
        "unattributed": log.unattributed,
        "objects": objects,
        "buckets_acknowledged": buckets_acknowledged,
        "current_file_barriers": current.as_ref().map(Vec::len),
        "objects_before_current": current
            .as_ref()
            .and_then(|current| current.first())
            .map(|current| last_object.is_none_or(|last| last < *current)),
        "root_synced_after_current": current.as_ref().and_then(|current| current.first()).map(|current| {
            sequences(inode(project), true).iter().any(|sequence| sequence > current)
        }),
    })
}

/// Environment of a child: which failpoints it dies at and what memory it has.
#[derive(Clone, Debug, Default)]
pub struct Conditions {
    /// Exit at the first hit of this construction failpoint.
    pub construction_failpoint: Option<String>,
    /// Exit at this project-publication failpoint.
    pub project_failpoint: Option<String>,
    /// `GF_BULK_BUILD_MEMORY_BUDGET_BYTES`; [`RESIDENT_BUDGET`] when absent.
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
    // Always set: an inherited budget or the host's headroom must not pick the
    // route.
    command.env(
        "GF_BULK_BUILD_MEMORY_BUDGET_BYTES",
        conditions.budget.unwrap_or(RESIDENT_BUDGET).to_string(),
    );
    let output = command.output().unwrap();
    let mut lines = BTreeMap::new();
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        for name in [
            "SESSION",
            "PHASE",
            "REGIONS",
            "BARRIERS",
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
