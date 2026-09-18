//! Replicates the certify runner's chained allocation observation for a
//! hand-driven import, and prints the composition of the winning peak.
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::io::Write;

fn main() {
    let mut args = std::env::args().skip(1);
    let gf = args.next().expect("gf path");
    let project = PathBuf::from(args.next().expect("project dir"));
    let nodes = PathBuf::from(args.next().expect("nodes.parquet"));
    let edges = PathBuf::from(args.next().expect("edges.parquet"));
    let uuid = args.next().expect("operation uuid");

    let invocations: Vec<Vec<String>> = vec![
        vec!["import-session".into(), "begin".into(), "--operation-uuid".into(), uuid.clone()],
        vec!["import-session".into(), "register-parquet".into(), "--session-uuid".into(), uuid.clone(), "--path".into(), nodes.display().to_string(), "--kind".into(), "nodes".into()],
        vec!["import-session".into(), "register-parquet".into(), "--session-uuid".into(), uuid.clone(), "--path".into(), edges.display().to_string(), "--kind".into(), "edges".into()],
        vec!["import-session".into(), "validate".into(), "--session-uuid".into(), uuid.clone()],
        vec!["import-session".into(), "commit".into(), "--session-uuid".into(), uuid.clone()],
    ];

    let mut paths: Vec<PathBuf> = vec![nodes.clone(), edges.clone()];
    let mut best_peak = 0_u64;
    let mut best: Option<serde_json::Value> = None;
    let mut best_step = String::new();

    for invocation in invocations {
        let mut all = paths.clone();
        all.extend(
            graphforge_storage::StorageAllocationOperation::project_paths(&project)
                .map(|p| p.to_vec())
                .unwrap_or_default(),
        );
        all.sort();
        all.dedup();
        let baseline = graphforge_storage::StorageAllocationOperation::from_paths(&all)
            .expect("baseline");
        let input = serde_json::to_vec(&baseline.snapshot().expect("snapshot")).expect("json");
        let mut child = Command::new(&gf)
            .arg("--json")
            .arg("--allocation-diagnostics")
            .arg("--project")
            .arg(&project)
            .args(&invocation)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("spawn");
        child.stdin.as_mut().unwrap().write_all(&input).unwrap();
        drop(child.stdin.take());
        let out = child.wait_with_output().expect("wait");
        assert!(out.status.success(), "{} failed: {}", invocation[1], String::from_utf8_lossy(&out.stdout));
        let text = String::from_utf8_lossy(&out.stdout);
        let receipt = text
            .lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .find(|value| value.get("contract").and_then(|c| c.as_str())
                == Some("graphforge-allocation-operation/1"))
            .expect("allocation receipt");
        let peak = receipt["peak_allocated_bytes"].as_u64().unwrap();
        println!("STEP {} peak={} current={}", invocation[1], peak, receipt["current_allocated_bytes"]);
        if peak > best_peak {
            best_peak = peak;
            best_step = invocation[1].clone();
            best = Some(receipt);
        }
        paths = all;
    }
    println!(
        "WINNING {}",
        serde_json::json!({"step": best_step, "peak": best_peak, "receipt": best})
    );
}
