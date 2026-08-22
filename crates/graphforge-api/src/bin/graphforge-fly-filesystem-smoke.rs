//! Exact-path durable filesystem qualification for provisioned Fly Machines.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use graphforge_api::{
    GfError, GraphForge, OperationId, PortableSelection, PortableV2ExportRequest,
    PortableV2ImportRequest, PortableV2Limits, PortableV2Mode, PortableV2Output,
    PortableV2SelectionProfile, PortableVerifyRequest, PropValue, verify_portable_v2,
};
use graphforge_core::uuid::Uuid;
use serde::Serialize;

const SCHEMA: &str = "graphforge-fly-filesystem-qualification/1";
const PROJECT_NAME: &str = ".graphforge-fly-filesystem-qualification-project";

#[derive(Debug)]
struct Config {
    work_root: PathBuf,
    evidence_out: PathBuf,
    timeout: Duration,
    git_sha: String,
    image_digest: String,
    region: String,
}

#[derive(Debug, Serialize)]
struct Evidence {
    schema: &'static str,
    git_sha: String,
    image_digest: String,
    provider: &'static str,
    region: String,
    host: Host,
    volume: Volume,
    phase_peak_rss_bytes: PhasePeakRss,
    admission: Admission,
    result: &'static str,
    full_run_authorized: bool,
}

#[derive(Debug, Serialize)]
struct Host {
    os: &'static str,
    filesystem: String,
    memory_bytes: Option<u64>,
}

#[derive(Debug, Serialize)]
struct Volume {
    mount_role: &'static str,
    capacity_bytes: Option<u64>,
}

#[derive(Debug, Default, Serialize)]
struct PhasePeakRss {
    filesystem_admission: Option<u64>,
    durable_reopen: Option<u64>,
    portable_verify: Option<u64>,
    portable_import_reopen: Option<u64>,
}

#[derive(Debug, Serialize)]
struct Admission {
    status: &'static str,
    code: Option<String>,
    cause: Option<String>,
}

fn main() {
    let config = parse_config(std::env::args_os().skip(1))
        .and_then(validate_paths)
        .unwrap_or_else(|cause| {
            eprintln!("qualification configuration rejected: {cause}");
            std::process::exit(2);
        });
    let output = config.evidence_out.clone();
    let timeout = config.timeout;
    let metadata = (
        config.git_sha.clone(),
        config.image_digest.clone(),
        config.region.clone(),
    );
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let _ = sender.send(qualify(&config));
    });
    let evidence = receiver.recv_timeout(timeout).unwrap_or_else(|_| {
        failed(
            metadata,
            "unproven",
            None,
            None,
            "GF_RESOURCE_LIMIT",
            "timeout",
        )
    });
    let encoded = serde_json::to_vec(&evidence).expect("serialize evidence");
    if write_evidence(&output, &encoded).is_err() {
        eprintln!("qualification evidence write failed");
        std::process::exit(1);
    }
    println!("{}", String::from_utf8(encoded).expect("JSON is UTF-8"));
    std::process::exit(i32::from(evidence.result != "qualified"));
}

fn parse_config(args: impl Iterator<Item = std::ffi::OsString>) -> Result<Config, &'static str> {
    let mut root = std::env::var_os("GF_FLY_QUALIFICATION_WORK_ROOT").map(PathBuf::from);
    let mut output = std::env::var_os("GF_FLY_QUALIFICATION_EVIDENCE_OUT").map(PathBuf::from);
    let mut timeout = std::env::var("GF_FLY_QUALIFICATION_TIMEOUT_S")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(900);
    let mut args = args;
    while let Some(flag) = args.next() {
        let value = args.next().ok_or("missing_option_value")?;
        match flag.to_str() {
            Some("--work-root") => root = Some(value.into()),
            Some("--evidence-out") => output = Some(value.into()),
            Some("--timeout-s") => {
                timeout = value
                    .to_str()
                    .and_then(|v| v.parse().ok())
                    .ok_or("invalid_timeout")?;
            }
            _ => return Err("unknown_option"),
        }
    }
    let work_root = root.ok_or("work_root_required")?;
    let evidence_out = output.ok_or("evidence_out_required")?;
    if !work_root.is_absolute() || !evidence_out.is_absolute() {
        return Err("absolute_path_required");
    }
    if timeout == 0 {
        return Err("invalid_timeout");
    }
    let git_sha = std::env::var("GF_FLY_QUALIFICATION_GIT_SHA").map_err(|_| "git_sha_required")?;
    let image_digest =
        std::env::var("GF_FLY_QUALIFICATION_IMAGE_DIGEST").map_err(|_| "image_digest_required")?;
    let region = std::env::var("GF_FLY_QUALIFICATION_REGION").map_err(|_| "region_required")?;
    validate_metadata(&git_sha, &image_digest, &region)?;
    Ok(Config {
        work_root,
        evidence_out,
        timeout: Duration::from_secs(timeout),
        git_sha,
        image_digest,
        region,
    })
}

fn validate_metadata(sha: &str, digest: &str, region: &str) -> Result<(), &'static str> {
    let lower_hex = |byte: u8| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase();
    if sha.len() != 40 || !sha.bytes().all(lower_hex) {
        return Err("invalid_git_sha");
    }
    if digest.len() != 71 || !digest.starts_with("sha256:") || !digest[7..].bytes().all(lower_hex) {
        return Err("invalid_image_digest");
    }
    if !(2..=20).contains(&region.len())
        || !region
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    {
        return Err("invalid_region");
    }
    Ok(())
}

fn validate_paths(mut config: Config) -> Result<Config, &'static str> {
    let root = config
        .work_root
        .canonicalize()
        .map_err(|_| "work_root_unavailable")?;
    if !root.is_dir() {
        return Err("work_root_unavailable");
    }
    let parent = config
        .evidence_out
        .parent()
        .and_then(|p| p.canonicalize().ok())
        .ok_or("evidence_parent_unavailable")?;
    if !parent.starts_with(&root) {
        return Err("evidence_outside_work_root");
    }
    config.work_root = root;
    Ok(config)
}

#[allow(clippy::too_many_lines)]
fn qualify(config: &Config) -> Evidence {
    let project = config.work_root.join(PROJECT_NAME);
    let package = config
        .work_root
        .join(".graphforge-fly-filesystem-qualification.gfpb");
    let imported = config
        .work_root
        .join(".graphforge-fly-filesystem-qualification-imported");
    let memory = linux_memory_bytes();
    let capacity = filesystem_capacity_bytes(&config.work_root);
    let mut phase_peak_rss = PhasePeakRss::default();
    if project.exists() || package.exists() || imported.exists() {
        return failure(
            config,
            "unproven",
            memory,
            capacity,
            "GF_VALIDATION",
            "qualification_state_exists",
        );
    }
    let filesystem =
        match graphforge_storage::filesystem_admission::filesystem_durability_preflight(&project) {
            Ok(value) => value.filesystem_class,
            Err(error) => return gf_failure(config, "unproven", memory, capacity, &error),
        };
    phase_peak_rss.filesystem_admission = linux_peak_rss_bytes();
    let graph = match GraphForge::new(project.to_str()) {
        Ok(graph) => graph,
        Err(error) => return gf_failure(config, &filesystem, memory, capacity, &error),
    };
    let props = HashMap::from([("marker".into(), PropValue::Str("qualification".into()))]);
    if let Err(error) = graph.add_node("FlyQualification", &props) {
        return gf_failure(config, &filesystem, memory, capacity, &error);
    }
    drop(graph);
    let reopened = match GraphForge::new(project.to_str()) {
        Ok(graph) => graph,
        Err(error) => return gf_failure(config, &filesystem, memory, capacity, &error),
    };
    match reopened.node_count("FlyQualification") {
        Ok(1) => {}
        Ok(_) => {
            return failure(
                config,
                &filesystem,
                memory,
                capacity,
                "GF_EXECUTION",
                "persisted_count_mismatch",
            );
        }
        Err(error) => return gf_failure(config, &filesystem, memory, capacity, &error),
    }
    drop(reopened);
    phase_peak_rss.durable_reopen = linux_peak_rss_bytes();
    let limits = PortableV2Limits::default();
    let source = match GraphForge::new(project.to_str()) {
        Ok(graph) => graph,
        Err(error) => return gf_failure(config, &filesystem, memory, capacity, &error),
    };
    if source
        .export_portable_v2(
            &PortableV2ExportRequest {
                selection: PortableSelection::Current,
                output_path: package.clone(),
                representation: PortableV2Output::Bundle,
                profile: PortableV2SelectionProfile::Complete,
                subset: None,
                limits,
            },
            None,
            |_| {},
        )
        .is_err()
    {
        return failure(
            config,
            &filesystem,
            memory,
            capacity,
            "GF_PORTABLE_V2",
            "portable_export_failed",
        );
    }
    drop(source);
    if verify_portable_v2(
        &PortableVerifyRequest {
            input: package.clone(),
            mode: PortableV2Mode::Full,
            limits,
        },
        None,
    )
    .is_err()
    {
        return failure(
            config,
            &filesystem,
            memory,
            capacity,
            "GF_PORTABLE_V2",
            "portable_verify_failed",
        );
    }
    phase_peak_rss.portable_verify = linux_peak_rss_bytes();
    if GraphForge::import_portable_v2(
        &imported,
        &PortableV2ImportRequest {
            input: package.clone(),
            operation_id: OperationId(Uuid::from_u128(0x882)),
            limits,
        },
        None,
    )
    .is_err()
    {
        return failure(
            config,
            &filesystem,
            memory,
            capacity,
            "GF_PORTABLE_V2",
            "portable_import_failed",
        );
    }
    let imported_graph = match GraphForge::new(imported.to_str()) {
        Ok(graph) => graph,
        Err(error) => return gf_failure(config, &filesystem, memory, capacity, &error),
    };
    if !matches!(imported_graph.node_count("FlyQualification"), Ok(1)) {
        return failure(
            config,
            &filesystem,
            memory,
            capacity,
            "GF_PORTABLE_V2",
            "portable_reopen_mismatch",
        );
    }
    drop(imported_graph);
    phase_peak_rss.portable_import_reopen = linux_peak_rss_bytes();
    let project_removed = fs::remove_dir_all(project).is_ok();
    let imported_removed = fs::remove_dir_all(imported).is_ok();
    let package_removed = fs::remove_file(package).is_ok();
    if !(project_removed && imported_removed && package_removed) {
        return failure(
            config,
            &filesystem,
            memory,
            capacity,
            "GF_IO",
            "project_cleanup_failed",
        );
    }
    Evidence {
        schema: SCHEMA,
        git_sha: config.git_sha.clone(),
        image_digest: config.image_digest.clone(),
        provider: "fly.io",
        region: config.region.clone(),
        host: Host {
            os: "Linux",
            filesystem,
            memory_bytes: memory,
        },
        volume: Volume {
            mount_role: "process_work_root",
            capacity_bytes: capacity,
        },
        phase_peak_rss_bytes: phase_peak_rss,
        admission: Admission {
            status: "accepted",
            code: None,
            cause: None,
        },
        result: "qualified",
        full_run_authorized: false,
    }
}

fn gf_failure(
    config: &Config,
    fs: &str,
    memory: Option<u64>,
    capacity: Option<u64>,
    error: &GfError,
) -> Evidence {
    let cause = match error {
        GfError::Project { message, .. } => safe_cause(message).unwrap_or("project_failure"),
        _ => "public_facade_failure",
    };
    failure(config, fs, memory, capacity, error.code(), cause)
}

fn safe_cause(message: &str) -> Option<&str> {
    message
        .split("cause=")
        .nth(1)?
        .split_whitespace()
        .next()
        .filter(|value| {
            !value.is_empty()
                && value
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_')
        })
}

fn failure(
    config: &Config,
    fs: &str,
    memory: Option<u64>,
    capacity: Option<u64>,
    code: &str,
    cause: &str,
) -> Evidence {
    failed(
        (
            config.git_sha.clone(),
            config.image_digest.clone(),
            config.region.clone(),
        ),
        fs,
        memory,
        capacity,
        code,
        cause,
    )
}

fn failed(
    meta: (String, String, String),
    fs: &str,
    memory: Option<u64>,
    capacity: Option<u64>,
    code: &str,
    cause: &str,
) -> Evidence {
    Evidence {
        schema: SCHEMA,
        git_sha: meta.0,
        image_digest: meta.1,
        provider: "fly.io",
        region: meta.2,
        host: Host {
            os: "Linux",
            filesystem: fs.into(),
            memory_bytes: memory,
        },
        volume: Volume {
            mount_role: "process_work_root",
            capacity_bytes: capacity,
        },
        phase_peak_rss_bytes: PhasePeakRss::default(),
        admission: Admission {
            status: "rejected",
            code: Some(code.into()),
            cause: Some(cause.into()),
        },
        result: "disqualified",
        full_run_authorized: false,
    }
}

fn linux_memory_bytes() -> Option<u64> {
    fs::read_to_string("/proc/meminfo")
        .ok()?
        .lines()
        .find_map(|line| {
            line.strip_prefix("MemTotal:")?
                .split_whitespace()
                .next()?
                .parse::<u64>()
                .ok()
                .map(|kib| kib.saturating_mul(1024))
        })
}

fn linux_peak_rss_bytes() -> Option<u64> {
    fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix("VmHWM:"))?
        .split_whitespace()
        .next()?
        .parse::<u64>()
        .ok()?
        .checked_mul(1024)
}

fn filesystem_capacity_bytes(path: &Path) -> Option<u64> {
    let output = Command::new("df")
        .args(["--output=size", "-B1"])
        .arg(path)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout)
        .ok()?
        .lines()
        .nth(1)?
        .trim()
        .parse()
        .ok()
}

fn write_evidence(path: &Path, encoded: &[u8]) -> std::io::Result<()> {
    if path.exists() {
        return Err(std::io::ErrorKind::AlreadyExists.into());
    }
    let name = path.file_name().ok_or(std::io::ErrorKind::InvalidInput)?;
    let temporary = path.with_file_name(format!(".{}.tmp", name.to_string_lossy()));
    match fs::remove_file(&temporary) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)?;
    file.write_all(encoded)?;
    file.sync_all()?;
    fs::rename(temporary, path)?;
    File::open(path.parent().unwrap_or_else(|| Path::new(".")))?.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_failure_matches_sanitized_schema_shape() {
        let value = serde_json::to_value(failed(
            (
                "a".repeat(40),
                format!("sha256:{}", "b".repeat(64)),
                "iad".into(),
            ),
            "unproven",
            Some(1),
            Some(1),
            "GF_UNSUPPORTED_FILESYSTEM",
            "ancestor_cross_volume",
        ))
        .unwrap();
        assert_eq!(value["admission"]["cause"], "ancestor_cross_volume");
        assert_eq!(value["result"], "disqualified");
        assert_eq!(value["full_run_authorized"], false);
        assert!(!value.to_string().contains("/work"));
    }

    #[test]
    fn safe_cause_rejects_path_like_diagnostics() {
        assert_eq!(
            safe_cause("phase=X cause=ancestor_cross_volume"),
            Some("ancestor_cross_volume")
        );
        assert_eq!(safe_cause("phase=X cause=/work/private"), None);
    }

    #[test]
    fn atomic_evidence_write_refuses_overwrite() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("evidence.json");
        write_evidence(&path, b"{}").unwrap();
        assert!(write_evidence(&path, b"changed").is_err());
    }

    #[test]
    fn atomic_evidence_write_recovers_from_stale_temporary_file() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("evidence.json");
        std::fs::write(directory.path().join(".evidence.json.tmp"), b"stale").unwrap();
        write_evidence(&path, b"fresh").unwrap();
        assert_eq!(std::fs::read(path).unwrap(), b"fresh");
    }
}
