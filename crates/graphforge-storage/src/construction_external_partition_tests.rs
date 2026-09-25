// #1585, ADR 0047: an endpoint partition over its resident budget is sorted
// externally and publishes the bytes an unconstrained ingest publishes.
//
// Every case runs a complete construction in a fresh process, so a failpoint
// can end the process at a precise point and a later process can resume it.

/// Recorded partition count for the hub fixture.
const EXTERNAL_TEST_PARTITIONS: u32 = 64;

/// Half of all edges point at this node, so its endpoint records concentrate
/// in one partition that no splitter can divide.
const EXTERNAL_TEST_HUB: usize = 7;

/// A resident budget every uniform partition fits and the hub's does not.
const EXTERNAL_TEST_TIGHT_BYTES: u64 = 16_384;

fn external_hub_edge_rows(start: usize, edges: &[[u8; 16]], nodes: &[[u8; 16]]) -> RecordBatch {
    let src = (0..edges.len())
        .map(|index| nodes[(start + index) % nodes.len()])
        .collect::<Vec<_>>();
    let dst = (0..edges.len())
        .map(|index| {
            let edge = start + index;
            if edge.is_multiple_of(2) {
                nodes[EXTERNAL_TEST_HUB]
            } else {
                nodes[(edge * 31 + 1) % nodes.len()]
            }
        })
        .collect::<Vec<_>>();
    RecordBatch::try_new(
        CONSTRUCTION_EDGE_SCHEMA.clone(),
        vec![
            Arc::new(fixed(edges)),
            Arc::new(StringArray::from(vec!["R"; edges.len()])),
            Arc::new(fixed(&src)),
            Arc::new(fixed(&dst)),
        ],
    )
    .unwrap()
}

/// Set when the cancellation callback fired while a run was on disk.
static EXTERNAL_CANCEL_FIRED_WITH_RUNS: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// One ingest through shape, encode, publish and reopen. With
/// `cancel_with_runs`, the shape is cancelled at the first poll that finds a
/// run temporary in the project.
fn external_ingest(
    project: &Path,
    budgets: GraphConstructionBudgets,
    resume: bool,
    cancel_with_runs: bool,
) -> Result<serde_json::Value, String> {
    let (nodes, edges) = (node_ids(1_024), edge_ids(8_192));
    let mut session =
        GraphConstructionSession::open(project, Uuid::from_u128(OPERATION), 0, budgets)
            .map_err(|error| error.to_string())?;
    session.checkpoint.session_now_micros = FIXED_NOW_MICROS;
    if !resume {
        for (index, window) in nodes.chunks(128).enumerate() {
            session
                .append(
                    ConstructionChunkKind::Node,
                    &format!("nodes-{index}"),
                    &node_rows(window),
                )
                .map_err(|error| error.to_string())?;
        }
        for (index, window) in edges.chunks(512).enumerate() {
            session
                .append(
                    ConstructionChunkKind::Edge,
                    &format!("edges-{index}"),
                    &external_hub_edge_rows(index * 512, window, &nodes),
                )
                .map_err(|error| error.to_string())?;
        }
        session.seal().map_err(|error| error.to_string())?;
    }
    let shape = session
        .shape_canonical_with_cancellation(|| {
            let fire = cancel_with_runs && !external_run_temps(project).is_empty();
            if fire {
                EXTERNAL_CANCEL_FIRED_WITH_RUNS.store(true, std::sync::atomic::Ordering::Release);
            }
            fire
        })
        .map_err(|error| error.to_string())?;
    let evidence = session.evidence().clone();
    let (fingerprint, encoding) =
        fingerprint_and_encoding(&mut session, &shape).map_err(|error| error.to_string())?;
    let published = session
        .publish_canonical(&encoding, Uuid::from_u128(0x71), Uuid::from_u128(0x72))
        .map_err(|error| error.to_string())?;
    drop(session);
    // Reopen as a query process does and count the published adjacency.
    let generation = crate::resolve_project_generation(project).map_err(|error| error.to_string())?;
    assert_eq!(generation.generation_uuid(), published.generation_uuid);
    let inventory = generation
        .graph_files_inventory()
        .map_err(|error| error.to_string())?
        .ok_or("published generation has no graph inventory")?;
    let workspace = TempDir::new().unwrap();
    crate::materialize_graph_objects(generation.container_root(), &inventory, workspace.path())
        .map_err(|error| error.to_string())?;
    let union = crate::adjacency::ShardedCsrIndex::open(&crate::adjacency::csr_path(
        workspace.path(),
        "_all",
        crate::adjacency::Direction::Out,
    ))
    .map_err(|error| error.to_string())?;
    Ok(serde_json::json!({
        "fingerprint": format!("{fingerprint:?}"),
        "reopened_edge_count": union.edge_count(),
        "peak_partition_records": evidence.peak_partition_records,
        "external_partitions": evidence.external_partitions,
        "external_runs": evidence.external_runs,
        "external_run_bytes": evidence.external_run_bytes,
    }))
}

/// Child body; inert unless the parent sets `GF_EXTERNAL_TEST_ROOT`.
#[test]
fn external_partition_subprocess() {
    let Some(root) = std::env::var_os("GF_EXTERNAL_TEST_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let case = std::env::var("GF_EXTERNAL_TEST_CASE").unwrap();
    let project = root.join(std::env::var("GF_EXTERNAL_TEST_PROJECT").unwrap());
    std::fs::create_dir_all(&project).unwrap();
    let mut budgets = budgets(EXTERNAL_TEST_PARTITIONS);
    if let Ok(value) = std::env::var("GF_EXTERNAL_TEST_PARTITION_BYTES") {
        budgets.max_partition_bytes = value.parse().unwrap();
    }
    if let Ok(value) = std::env::var("GF_EXTERNAL_TEST_EXTERNAL_BYTES") {
        budgets.max_external_partition_bytes = value.parse().unwrap();
    }
    let resume = std::env::var_os("GF_EXTERNAL_TEST_RESUME").is_some();
    let cancel_with_runs = std::env::var_os("GF_EXTERNAL_TEST_CANCEL_WITH_RUNS").is_some();
    let outcome = external_ingest(&project, budgets, resume, cancel_with_runs);
    let published = crate::resolve_project_generation(&project)
        .ok()
        .and_then(|generation| generation.graph_files_inventory().ok().flatten())
        .map(|inventory| inventory.files.len());
    let result = serde_json::json!({
        "result": outcome.as_ref().ok(),
        "error": outcome.as_ref().err(),
        "published": published,
        "construction_temps_clean": tree_has_no_temps(&project),
        "cancel_fired_with_runs":
            EXTERNAL_CANCEL_FIRED_WITH_RUNS.load(std::sync::atomic::Ordering::Acquire),
    });
    std::fs::write(
        root.join(format!("{case}.json")),
        serde_json::to_vec_pretty(&result).unwrap(),
    )
    .unwrap();
}

/// Run one case in a fresh process. Returns its recorded outcome, or `Null`
/// when the process ended at a failpoint, and the exit code.
fn run_external_case(
    root: &Path,
    name: &str,
    project: &str,
    env: &[(&str, &str)],
) -> (serde_json::Value, Option<i32>) {
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--exact",
            "graph_construction::tests::determinism::external_partition_subprocess",
            "--nocapture",
        ])
        .env("GF_EXTERNAL_TEST_ROOT", root)
        .env("GF_EXTERNAL_TEST_CASE", name)
        .env("GF_EXTERNAL_TEST_PROJECT", project)
        .env_remove("GF_EXTERNAL_TEST_PARTITION_BYTES")
        .env_remove("GF_EXTERNAL_TEST_EXTERNAL_BYTES")
        .env_remove("GF_EXTERNAL_TEST_RESUME")
        .env_remove("GF_EXTERNAL_TEST_CANCEL_WITH_RUNS")
        .env_remove("GF_CONSTRUCTION_FAILPOINT")
        .env_remove("GF_CONSTRUCTION_FAILPOINT_COOKIE")
        .env_remove("GF_SHAPE_SPILL_SPIKE")
        .env_remove("GF_SHAPE_SORT_SPIKE")
        .env_remove("GF_SHAPE_LOAD_SCHEDULER")
        .env_remove("GF_SHAPE_LOAD_WORKERS")
        .env_remove("GF_SHAPE_MAX_PARTITION_BYTES")
        .env_remove("GF_SHAPE_MAX_EXTERNAL_PARTITION_BYTES");
    for (key, value) in env {
        command.env(key, value);
    }
    let output = command.output().unwrap();
    let recorded = std::fs::read(root.join(format!("{name}.json")))
        .ok()
        .map(|bytes| serde_json::from_slice(&bytes).unwrap())
        .unwrap_or(serde_json::Value::Null);
    if recorded.is_null() {
        assert!(
            !output.status.success(),
            "{name}: child exited cleanly without recording an outcome: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    (recorded, output.status.code())
}

/// Every run temporary under `project`.
fn external_run_temps(project: &Path) -> Vec<String> {
    fn walk(path: &Path, found: &mut Vec<String>) {
        for entry in std::fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            if entry.path().is_dir() {
                walk(&entry.path(), found);
            } else {
                let name = entry.file_name().to_string_lossy().into_owned();
                if name.starts_with(".artifact-xrun-") {
                    found.push(name);
                }
            }
        }
    }
    let mut found = Vec::new();
    walk(project, &mut found);
    found
}

#[test]
fn external_partitions_publish_the_unconstrained_answer() {
    let root = TempDir::new().unwrap();
    let tight = EXTERNAL_TEST_TIGHT_BYTES.to_string();

    // Control: the default resident budget holds every partition.
    let (control, _) = run_external_case(root.path(), "control", "control", &[]);
    let control = &control["result"];
    assert!(control.is_object(), "control failed: {control}");
    assert_eq!(control["external_partitions"], 0, "{control}");
    assert_eq!(control["reopened_edge_count"], 8_192);
    let peak = control["peak_partition_records"].as_u64().unwrap();
    // Premise: the hub partition exceeds the tight budget.
    assert!(peak * 33 > EXTERNAL_TEST_TIGHT_BYTES, "{control}");
    let fingerprint = control["fingerprint"].as_str().unwrap();

    // The pre-ADR-0047 contract, still selected by a zero external bound.
    let (refused, _) = run_external_case(
        root.path(),
        "refused",
        "refused",
        &[
            ("GF_EXTERNAL_TEST_PARTITION_BYTES", &tight),
            ("GF_EXTERNAL_TEST_EXTERNAL_BYTES", "0"),
        ],
    );
    assert!(
        refused["error"]
            .as_str()
            .unwrap_or_default()
            .contains("exceeds recorded budget"),
        "{refused}"
    );
    assert!(refused["published"].is_null(), "{refused}");
    assert_eq!(refused["construction_temps_clean"], true, "{refused}");

    // The default external bound sorts the hub partition in bounded runs.
    let (external, _) = run_external_case(
        root.path(),
        "external",
        "external",
        &[("GF_EXTERNAL_TEST_PARTITION_BYTES", &tight)],
    );
    let result = &external["result"];
    assert_eq!(result["fingerprint"].as_str(), Some(fingerprint), "{external}");
    assert_eq!(result["reopened_edge_count"], 8_192, "{external}");
    assert!(result["external_partitions"].as_u64().unwrap() >= 1, "{external}");
    assert!(result["external_runs"].as_u64().unwrap() >= 2, "{external}");
    assert!(
        result["peak_partition_records"].as_u64().unwrap() * 33 <= EXTERNAL_TEST_TIGHT_BYTES,
        "no partition may be materialized over the resident budget: {external}"
    );
    assert_eq!(external["construction_temps_clean"], true, "{external}");
    assert!(external_run_temps(&root.path().join("external")).is_empty());
    println!("EXTERNAL_PARTITION {external}");
}

/// A process ended after writing a run, or after installing a shaped output
/// from runs, resumes to the unconstrained answer and leaves no run behind.
#[test]
fn an_interrupted_external_partition_resumes_to_the_same_answer() {
    let root = TempDir::new().unwrap();
    let tight = EXTERNAL_TEST_TIGHT_BYTES.to_string();
    let (control, _) = run_external_case(root.path(), "control", "control", &[]);
    let fingerprint = control["result"]["fingerprint"].as_str().unwrap().to_owned();

    for failpoint in [
        "shape.external_run.after_write",
        "shape.partition_output.after_install",
    ] {
        let project = failpoint.replace('.', "-");
        let (crashed, code) = run_external_case(
            root.path(),
            &format!("{project}-crash"),
            &project,
            &[
                ("GF_EXTERNAL_TEST_PARTITION_BYTES", &tight),
                ("GF_CONSTRUCTION_FAILPOINT", failpoint),
                (
                    "GF_CONSTRUCTION_FAILPOINT_COOKIE",
                    "graphforge-construction-test-v1",
                ),
            ],
        );
        assert!(crashed.is_null(), "{failpoint}: child did not stop: {crashed}");
        assert_eq!(code, Some(86), "{failpoint}");
        if failpoint == "shape.external_run.after_write" {
            // The known positive: the process ended holding a run.
            assert!(
                !external_run_temps(&root.path().join(&project)).is_empty(),
                "{failpoint}: no run was on disk at the failpoint"
            );
        }
        let (resumed, _) = run_external_case(
            root.path(),
            &format!("{project}-resume"),
            &project,
            &[
                ("GF_EXTERNAL_TEST_PARTITION_BYTES", &tight),
                ("GF_EXTERNAL_TEST_RESUME", "1"),
            ],
        );
        assert_eq!(
            resumed["result"]["fingerprint"].as_str(),
            Some(fingerprint.as_str()),
            "{failpoint}: {resumed}"
        );
        assert_eq!(resumed["construction_temps_clean"], true, "{failpoint}");
        assert!(
            external_run_temps(&root.path().join(&project)).is_empty(),
            "{failpoint}: a run survived the resume"
        );
    }
}

/// A shape cancelled while runs are on disk publishes nothing and leaves no
/// run; a retry resumes to the unconstrained answer.
#[test]
fn a_cancelled_external_partition_leaves_no_run_and_retries() {
    let root = tempfile::TempDir::new().unwrap();
    let tight = EXTERNAL_TEST_TIGHT_BYTES.to_string();
    let (control, _) = run_external_case(root.path(), "control", "control", &[]);
    let fingerprint = control["result"]["fingerprint"].as_str().unwrap().to_owned();

    let (cancelled, _) = run_external_case(
        root.path(),
        "cancel",
        "cancel",
        &[
            ("GF_EXTERNAL_TEST_PARTITION_BYTES", &tight),
            ("GF_EXTERNAL_TEST_CANCEL_WITH_RUNS", "1"),
        ],
    );
    // The known positive: cancellation was observed with a run on disk.
    assert_eq!(cancelled["cancel_fired_with_runs"], true, "{cancelled}");
    assert!(
        cancelled["error"]
            .as_str()
            .unwrap_or_default()
            .contains("cancelled"),
        "{cancelled}"
    );
    assert!(cancelled["published"].is_null(), "{cancelled}");
    assert_eq!(cancelled["construction_temps_clean"], true, "{cancelled}");
    assert!(external_run_temps(&root.path().join("cancel")).is_empty());

    let (retried, _) = run_external_case(
        root.path(),
        "cancel-retry",
        "cancel",
        &[
            ("GF_EXTERNAL_TEST_PARTITION_BYTES", &tight),
            ("GF_EXTERNAL_TEST_RESUME", "1"),
        ],
    );
    assert_eq!(
        retried["result"]["fingerprint"].as_str(),
        Some(fingerprint.as_str()),
        "{retried}"
    );
    assert!(external_run_temps(&root.path().join("cancel")).is_empty());
}
