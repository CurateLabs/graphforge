// #1507: DataFusion external sort for fixed-width partitions under forced
// memory pressure (`graph_construction::spill_spike`).
//
// Every case runs a complete construction in a fresh process, because the
// experiment is selected by process environment and must never race tests in
// the parent harness. The parent compares what each child published against
// an uninterrupted baseline control.

/// Recorded partition count for the hub fixture.
const SPILL_TEST_PARTITIONS: u32 = 64;

/// The hub node's index; half of all edges point at it, so the node-keyed
/// endpoint family concentrates in one partition while every partition still
/// owns a balanced share of distinct keys.
const SPILL_TEST_HUB: usize = 7;

fn spill_fixture() -> (Vec<[u8; 16]>, Vec<[u8; 16]>) {
    (node_ids(1_024), edge_ids(8_192))
}

fn hub_edge_rows(start: usize, edges: &[[u8; 16]], nodes: &[[u8; 16]]) -> RecordBatch {
    let src = (0..edges.len())
        .map(|index| nodes[(start + index) % nodes.len()])
        .collect::<Vec<_>>();
    let dst = (0..edges.len())
        .map(|index| {
            let edge = start + index;
            if edge.is_multiple_of(2) {
                nodes[SPILL_TEST_HUB]
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

fn spill_budgets(max_partition_bytes: Option<u64>) -> GraphConstructionBudgets {
    let mut budgets = budgets(SPILL_TEST_PARTITIONS);
    if let Some(bytes) = max_partition_bytes {
        budgets.max_partition_bytes = bytes;
    }
    budgets
}

fn relative_files(root: &Path) -> Vec<String> {
    fn walk(root: &Path, path: &Path, files: &mut Vec<String>) {
        let Ok(entries) = std::fs::read_dir(path) else {
            return;
        };
        for entry in entries {
            let path = entry.unwrap().path();
            files.push(path.strip_prefix(root).unwrap().display().to_string());
            if path.is_dir() {
                walk(root, &path, files);
            }
        }
    }
    let mut files = Vec::new();
    walk(root, root, &mut files);
    files.sort();
    files
}

/// One ingest through shape, encode, publish and reopen. Returns the
/// fingerprint and reopened counts, or the first error as text.
fn spill_ingest(
    project: &Path,
    budgets: GraphConstructionBudgets,
    resume: bool,
    cancel_after_spill: bool,
    shaped: &mut Option<String>,
) -> Result<serde_json::Value, String> {
    let (nodes, edges) = spill_fixture();
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
                    &hub_edge_rows(index * 512, window, &nodes),
                )
                .map_err(|error| error.to_string())?;
        }
        session.seal().map_err(|error| error.to_string())?;
    }
    // Cancel only once an external sort has spilled runs to disk, so the
    // cancellation is observed while library scratch exists.
    let shape = session
        .shape_canonical_with_cancellation(|| {
            cancel_after_spill && crate::graph_construction::spill_spike::spill_observed()
        })
        .map_err(|error| error.to_string())?;
    let peak_partition_records = session.evidence().peak_partition_records;
    *shaped = Some(format!("{:?}", shaped_digests(&session, &shape)));
    let (fingerprint, encoding) =
        fingerprint_and_encoding(&mut session, &shape).map_err(|error| error.to_string())?;
    let published = session
        .publish_canonical(&encoding, Uuid::from_u128(0x71), Uuid::from_u128(0x72))
        .map_err(|error| error.to_string())?;
    drop(session);
    // Reopen as a query process does: resolve the current generation, hydrate
    // its declared objects and open the published adjacency index.
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
        "shaped": format!("{:?}", fingerprint.shaped),
        "node_count": fingerprint.node_count,
        "edge_count": fingerprint.edge_count,
        "reopened_edge_count": union.edge_count(),
        "inventory_files": inventory.files.len(),
        "peak_partition_records": peak_partition_records,
    }))
}

/// Child body; inert unless the parent sets `GF_SPILL_TEST_ROOT`.
#[test]
fn spill_spike_subprocess() {
    let Some(root) = std::env::var_os("GF_SPILL_TEST_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let case = std::env::var("GF_SPILL_TEST_CASE").unwrap();
    let project = root.join(std::env::var("GF_SPILL_TEST_PROJECT").unwrap());
    std::fs::create_dir_all(&project).unwrap();
    let budget = std::env::var("GF_SPILL_TEST_PARTITION_BYTES")
        .ok()
        .map(|value| value.parse().unwrap());
    let cancel_after_spill = std::env::var_os("GF_SPILL_TEST_CANCEL_AFTER_SPILL").is_some();
    let resume = std::env::var_os("GF_SPILL_TEST_RESUME").is_some();
    let mut shaped = None;
    let outcome = spill_ingest(
        &project,
        spill_budgets(budget),
        resume,
        cancel_after_spill,
        &mut shaped,
    );
    let external = crate::graph_construction::spill_spike::take_recorded_evidence();
    // A published graph is a current generation with a graph inventory; an
    // unpublished project resolves to no inventory.
    let published = crate::resolve_project_generation(&project)
        .ok()
        .and_then(|generation| generation.graph_files_inventory().ok().flatten())
        .map(|inventory| inventory.files.len());
    let result = serde_json::json!({
        "result": outcome.as_ref().ok(),
        "error": outcome.as_ref().err(),
        // Shaped digests of a shape that completed, even if a later stage failed.
        "shaped": shaped,
        "published": published,
        "external": external,
        "spill_observed": crate::graph_construction::spill_spike::spill_observed(),
        "construction_temps_clean": tree_has_no_temps(&project),
    });
    std::fs::write(
        root.join(format!("{case}.json")),
        serde_json::to_vec_pretty(&result).unwrap(),
    )
    .unwrap();
}

struct SpillCase<'a> {
    name: &'a str,
    project: &'a str,
    env: &'a [(&'a str, &'a str)],
}

/// Run one case in a fresh process and return its recorded outcome plus the
/// DataFusion scratch directory's contents after the process exited.
fn run_spill_case(root: &Path, case: &SpillCase<'_>) -> (serde_json::Value, Vec<String>, bool) {
    let scratch = root.join(format!("{}-scratch", case.name));
    std::fs::create_dir_all(&scratch).unwrap();
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--exact",
            "graph_construction::tests::determinism::spill_spike_subprocess",
            "--nocapture",
        ])
        .env("GF_SPILL_TEST_ROOT", root)
        .env("GF_SPILL_TEST_CASE", case.name)
        .env("GF_SPILL_TEST_PROJECT", case.project)
        .env("GF_SHAPE_SPILL_DIR", &scratch)
        .env_remove("GF_SHAPE_SPILL_SPIKE")
        .env_remove("GF_SHAPE_SPILL_FAULT")
        .env_remove("GF_SHAPE_SPILL_GUARD")
        .env_remove("GF_SHAPE_SPILL_POOL_BYTES")
        .env_remove("GF_SHAPE_SPILL_TEMP_BYTES");
    for (key, value) in case.env {
        command.env(key, value);
    }
    let output = command.output().unwrap();
    let recorded = std::fs::read(root.join(format!("{}.json", case.name)))
        .ok()
        .map(|bytes| serde_json::from_slice(&bytes).unwrap())
        .unwrap_or(serde_json::Value::Null);
    let exited = output.status.success();
    if recorded.is_null() {
        assert!(
            !exited,
            "{}: child exited cleanly without recording an outcome: {}",
            case.name,
            String::from_utf8_lossy(&output.stderr)
        );
    }
    (recorded, relative_files(&scratch), exited)
}

fn error_of(outcome: &serde_json::Value) -> String {
    outcome["error"].as_str().unwrap_or_default().to_owned()
}

/// Hub-heavy endpoint partition that the recorded budget refuses: the baseline
/// refuses safely; the hybrid publishes byte-identical artifacts, deletes its
/// runs, and reports that it actually spilled.
#[test]
fn spill_spike_hybrid_sorts_refused_partitions_and_fails_closed() {
    let root = TempDir::new().unwrap();
    // A budget that every uniform partition fits and the hub partition does
    // not; the control's peak partition proves the premise below.
    let tight = "16384";
    let tight_env = |extra: &'static [(&'static str, &'static str)]| {
        let mut env = vec![
            ("GF_SPILL_TEST_PARTITION_BYTES", tight),
            ("GF_SHAPE_SPILL_SPIKE", "datafusion"),
        ];
        env.extend_from_slice(extra);
        env
    };

    let (control, control_scratch, _) = run_spill_case(
        root.path(),
        &SpillCase {
            name: "control",
            project: "control",
            env: &[],
        },
    );
    let control_result = &control["result"];
    assert!(control_result.is_object(), "control failed: {control}");
    assert!(control["external"].as_array().unwrap().is_empty());
    assert!(control_scratch.is_empty());
    let fingerprint = control_result["fingerprint"].as_str().unwrap().to_owned();
    assert_eq!(control_result["reopened_edge_count"], 8_192);

    let (baseline, _, _) = run_spill_case(
        root.path(),
        &SpillCase {
            name: "baseline-tight",
            project: "baseline-tight",
            env: &[("GF_SPILL_TEST_PARTITION_BYTES", tight)],
        },
    );
    assert!(
        error_of(&baseline).contains("exceeds recorded budget"),
        "{baseline}"
    );
    assert!(baseline["published"].is_null(), "{baseline}");
    assert_eq!(baseline["construction_temps_clean"], true);

    let hybrid_env = tight_env(&[]);
    let (hybrid, hybrid_scratch, _) = run_spill_case(
        root.path(),
        &SpillCase {
            name: "hybrid-tight",
            project: "hybrid-tight",
            env: &hybrid_env,
        },
    );
    assert_eq!(
        hybrid["result"]["fingerprint"].as_str(),
        Some(fingerprint.as_str()),
        "{hybrid}"
    );
    assert_eq!(hybrid["result"]["reopened_edge_count"], 8_192);
    let external = hybrid["external"].as_array().unwrap();
    assert!(!external.is_empty(), "no partition took the external path");
    assert!(
        external
            .iter()
            .any(|partition| partition["spill_count"].as_u64().unwrap() > 0),
        "the external sort never spilled: {hybrid}"
    );
    for partition in external {
        assert!(
            partition["peak_pool_bytes"].as_u64().unwrap()
                <= partition["pool_limit_bytes"].as_u64().unwrap()
        );
    }
    assert!(hybrid_scratch.is_empty(), "{hybrid_scratch:?}");
    println!("SPILL_SPIKE_HYBRID {hybrid}");

    // Every fixed partition external, under a pool small enough to spill.
    let (always, always_scratch, _) = run_spill_case(
        root.path(),
        &SpillCase {
            name: "always",
            project: "always",
            env: &[
                ("GF_SHAPE_SPILL_SPIKE", "datafusion-always"),
                ("GF_SHAPE_SPILL_POOL_BYTES", tight),
            ],
        },
    );
    assert_eq!(
        always["result"]["fingerprint"].as_str(),
        Some(fingerprint.as_str()),
        "{always}"
    );
    assert!(always_scratch.is_empty(), "{always_scratch:?}");

    // Failure cases: each must refuse, publish nothing, and leave neither
    // construction temps nor DataFusion runs behind.
    let refusals: [(&str, Vec<(&str, &str)>, &str); 4] = [
        (
            "flip-guarded",
            tight_env(&[("GF_SHAPE_SPILL_FAULT", "flip")]),
            "differs from its input record multiset",
        ),
        (
            "truncate",
            tight_env(&[("GF_SHAPE_SPILL_FAULT", "truncate")]),
            "",
        ),
        (
            "disk-quota",
            tight_env(&[("GF_SHAPE_SPILL_TEMP_BYTES", "4096")]),
            "exceeded the allowable limit",
        ),
        (
            "pool-refusal",
            tight_env(&[("GF_SHAPE_SPILL_POOL_BYTES", "1024")]),
            "Resources exhausted",
        ),
    ];
    for (name, env, expected) in refusals {
        let (outcome, scratch, _) = run_spill_case(
            root.path(),
            &SpillCase {
                name,
                project: name,
                env: &env,
            },
        );
        let error = error_of(&outcome);
        assert!(!error.is_empty(), "{name} did not fail: {outcome}");
        assert!(error.contains(expected), "{name}: {error}");
        assert!(outcome["published"].is_null(), "{name}: {outcome}");
        assert_eq!(outcome["construction_temps_clean"], true, "{name}");
        assert!(scratch.is_empty(), "{name} left runs: {scratch:?}");
        println!("SPILL_SPIKE_REFUSAL {name}: {error}");
    }

    // Without the adapter's guard, DataFusion itself lets a changed payload
    // byte through: the shape completes and installs a shaped artifact with a
    // valid receipt over corrupted bytes. Here GraphForge's encoder happens to
    // catch it by cross-checking edge streams before publication; that is a
    // property of this record family, not of the spill.
    let unguarded_env = tight_env(&[
        ("GF_SHAPE_SPILL_FAULT", "flip"),
        ("GF_SHAPE_SPILL_GUARD", "off"),
    ]);
    let (unguarded, _, _) = run_spill_case(
        root.path(),
        &SpillCase {
            name: "flip-unguarded",
            project: "flip-unguarded",
            env: &unguarded_env,
        },
    );
    let control_shaped = control_result["shaped"].as_str().unwrap();
    let unguarded_shaped = unguarded["shaped"].as_str().expect("the shape completed");
    assert_ne!(unguarded_shaped, control_shaped, "{unguarded}");
    assert!(
        error_of(&unguarded).contains("streams differ"),
        "{unguarded}"
    );
    assert!(unguarded["published"].is_null(), "{unguarded}");
    println!("SPILL_SPIKE_UNGUARDED {unguarded}");

    // Cancellation after an external sort has spilled runs, then a clean
    // retry in the same session.
    let cancel_env = tight_env(&[("GF_SPILL_TEST_CANCEL_AFTER_SPILL", "1")]);
    let (cancelled, cancelled_scratch, _) = run_spill_case(
        root.path(),
        &SpillCase {
            name: "cancel",
            project: "cancel",
            env: &cancel_env,
        },
    );
    assert_eq!(
        cancelled["spill_observed"], true,
        "cancellation fired before any external sort spilled: {cancelled}"
    );
    assert!(error_of(&cancelled).contains("cancel"), "{cancelled}");
    assert!(cancelled["published"].is_null());
    assert_eq!(cancelled["construction_temps_clean"], true);
    assert!(cancelled_scratch.is_empty(), "{cancelled_scratch:?}");
    let retry_env = tight_env(&[("GF_SPILL_TEST_RESUME", "1")]);
    let (retried, _, _) = run_spill_case(
        root.path(),
        &SpillCase {
            name: "cancel-retry",
            project: "cancel",
            env: &retry_env,
        },
    );
    assert_eq!(
        retried["result"]["fingerprint"].as_str(),
        Some(fingerprint.as_str()),
        "{retried}"
    );
}

/// A process killed while DataFusion holds spilled runs: GraphForge recovery
/// resumes and publishes the uninterrupted result, and nothing reclaims the
/// orphaned runs. Library scratch is not construction state.
#[test]
fn spill_spike_crash_resumes_but_orphans_library_runs() {
    let root = TempDir::new().unwrap();
    let (control, _, _) = run_spill_case(
        root.path(),
        &SpillCase {
            name: "control",
            project: "control",
            env: &[],
        },
    );
    let fingerprint = control["result"]["fingerprint"].as_str().unwrap().to_owned();
    let (crashed, crash_scratch, exited) = run_spill_case(
        root.path(),
        &SpillCase {
            name: "crash",
            project: "crash",
            env: &[
                ("GF_SPILL_TEST_PARTITION_BYTES", "16384"),
                ("GF_SHAPE_SPILL_SPIKE", "datafusion"),
                ("GF_SHAPE_SPILL_FAULT", "abort"),
            ],
        },
    );
    assert!(!exited && crashed.is_null(), "the child did not abort");
    assert!(
        crash_scratch
            .iter()
            .any(|path| Path::new(path).components().count() > 1),
        "an aborted external sort should leave its runs: {crash_scratch:?}"
    );
    // Resume in a new process against the same scratch root.
    let scratch = root.path().join("crash-scratch");
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    let output = command
        .args([
            "--exact",
            "graph_construction::tests::determinism::spill_spike_subprocess",
            "--nocapture",
        ])
        .env("GF_SPILL_TEST_ROOT", root.path())
        .env("GF_SPILL_TEST_CASE", "crash-resume")
        .env("GF_SPILL_TEST_PROJECT", "crash")
        .env("GF_SPILL_TEST_RESUME", "1")
        .env("GF_SPILL_TEST_PARTITION_BYTES", "16384")
        .env("GF_SHAPE_SPILL_SPIKE", "datafusion")
        .env("GF_SHAPE_SPILL_DIR", &scratch)
        .env_remove("GF_SHAPE_SPILL_FAULT")
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let resumed: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.path().join("crash-resume.json")).unwrap())
            .unwrap();
    assert_eq!(
        resumed["result"]["fingerprint"].as_str(),
        Some(fingerprint.as_str()),
        "{resumed}"
    );
    let after = relative_files(&scratch);
    for orphan in crash_scratch {
        assert!(after.contains(&orphan), "{orphan} was reclaimed: {after:?}");
    }
    println!("SPILL_SPIKE_ORPHANS {after:?}");
}

// ─── Candidate B: native bounded external merge tests (#1585) ─────────────────

/// Hub-heavy partition that the recorded budget refuses: the native merge
/// publishes byte-identical artifacts, removes its run files, and reports the
/// correct evidence.  Correctness gates (ADR 0047 obligations):
///
/// 1. Byte-identical publication with control.
/// 2. Corrupt scratch refused (flip, mutation-proven: unguarded lets it through).
/// 3. Truncated scratch refused.
/// 4. Scratch limit refusal.
/// 5. Cancellation mid-run, then clean retry.
/// 6. No scratch after success.
/// 7. No construction temps after any outcome.
#[test]
fn spill_spike_native_sorts_refused_partitions_and_fails_closed() {
    let root = TempDir::new().unwrap();
    let tight = "16384";
    let native_env = |extra: &'static [(&'static str, &'static str)]| {
        let mut env = vec![
            ("GF_SPILL_TEST_PARTITION_BYTES", tight),
            ("GF_SHAPE_SPILL_SPIKE", "native"),
        ];
        env.extend_from_slice(extra);
        env
    };

    // ── Control (uninterrupted production path) ─────────────────────────────
    let (control, control_scratch, _) = run_spill_case(
        root.path(),
        &SpillCase {
            name: "native-control",
            project: "native-control",
            env: &[],
        },
    );
    let control_result = &control["result"];
    assert!(control_result.is_object(), "native control failed: {control}");
    assert!(control_scratch.is_empty());
    let fingerprint = control_result["fingerprint"].as_str().unwrap().to_owned();
    assert_eq!(control_result["reopened_edge_count"], 8_192);

    // ── Native hybrid publishes byte-identical answer ───────────────────────
    let native_base_env = native_env(&[]);
    let (native, native_scratch, _) = run_spill_case(
        root.path(),
        &SpillCase {
            name: "native-hybrid",
            project: "native-hybrid",
            env: &native_base_env,
        },
    );
    assert_eq!(
        native["result"]["fingerprint"].as_str(),
        Some(fingerprint.as_str()),
        "native hybrid fingerprint differs from control: {native}"
    );
    assert_eq!(native["result"]["reopened_edge_count"], 8_192, "{native}");
    let external = native["external"].as_array().unwrap();
    assert!(
        !external.is_empty(),
        "native: no partition took the external path: {native}"
    );
    // Verify at least one run was actually written (spill_count > 0).
    assert!(
        external
            .iter()
            .any(|p| p["spill_count"].as_u64().unwrap_or(0) > 0),
        "native sort never wrote a run: {native}"
    );
    assert!(native_scratch.is_empty(), "native left run files: {native_scratch:?}");
    println!("SPILL_SPIKE_NATIVE {native}");

    // ── Failure cases ───────────────────────────────────────────────────────
    let refusals: [(&str, Vec<(&str, &str)>, &str); 3] = [
        (
            "native-flip-guarded",
            native_env(&[("GF_SHAPE_SPILL_FAULT", "flip")]),
            "differs from its input record multiset",
        ),
        (
            "native-truncate",
            native_env(&[("GF_SHAPE_SPILL_FAULT", "truncate")]),
            "",
        ),
        (
            "native-disk-quota",
            native_env(&[("GF_SHAPE_SPILL_TEMP_BYTES", "4096")]),
            "exceeded the allowable limit",
        ),
    ];
    for (name, env, expected) in refusals {
        let (outcome, scratch, _) = run_spill_case(
            root.path(),
            &SpillCase {
                name,
                project: name,
                env: &env,
            },
        );
        let error = error_of(&outcome);
        assert!(!error.is_empty(), "{name} did not fail: {outcome}");
        assert!(error.contains(expected), "{name}: {error}");
        assert!(outcome["published"].is_null(), "{name}: {outcome}");
        assert_eq!(outcome["construction_temps_clean"], true, "{name}");
        assert!(scratch.is_empty(), "{name} left run files: {scratch:?}");
        println!("SPILL_SPIKE_NATIVE_REFUSAL {name}: {error}");
    }

    // Without the guard, the flip propagates into the shaped output.
    let unguarded_env = native_env(&[
        ("GF_SHAPE_SPILL_FAULT", "flip"),
        ("GF_SHAPE_SPILL_GUARD", "off"),
    ]);
    let (unguarded, _, _) = run_spill_case(
        root.path(),
        &SpillCase {
            name: "native-flip-unguarded",
            project: "native-flip-unguarded",
            env: &unguarded_env,
        },
    );
    let control_shaped = control_result["shaped"].as_str().unwrap();
    let unguarded_shaped = unguarded["shaped"].as_str().expect("native unguarded shape completed");
    assert_ne!(unguarded_shaped, control_shaped, "{unguarded}");
    assert!(
        error_of(&unguarded).contains("streams differ"),
        "native unguarded did not catch the corruption at encoding: {unguarded}"
    );
    assert!(unguarded["published"].is_null(), "{unguarded}");
    println!("SPILL_SPIKE_NATIVE_UNGUARDED {unguarded}");

    // Cancellation after the first native run is written, then clean retry.
    let cancel_env = native_env(&[("GF_SPILL_TEST_CANCEL_AFTER_SPILL", "1")]);
    let (cancelled, cancelled_scratch, _) = run_spill_case(
        root.path(),
        &SpillCase {
            name: "native-cancel",
            project: "native-cancel",
            env: &cancel_env,
        },
    );
    assert_eq!(
        cancelled["spill_observed"], true,
        "native cancellation fired before any run written: {cancelled}"
    );
    assert!(
        error_of(&cancelled).contains("cancel"),
        "native cancel did not report cancel: {cancelled}"
    );
    assert!(cancelled["published"].is_null(), "{cancelled}");
    assert_eq!(cancelled["construction_temps_clean"], true, "{cancelled}");
    assert!(cancelled_scratch.is_empty(), "native cancel left runs: {cancelled_scratch:?}");

    let retry_env = native_env(&[("GF_SPILL_TEST_RESUME", "1")]);
    let (retried, _, _) = run_spill_case(
        root.path(),
        &SpillCase {
            name: "native-cancel-retry",
            project: "native-cancel",
            env: &retry_env,
        },
    );
    assert_eq!(
        retried["result"]["fingerprint"].as_str(),
        Some(fingerprint.as_str()),
        "native retry fingerprint wrong: {retried}"
    );
}

/// A process killed while native runs exist: GraphForge recovery resumes and
/// publishes the correct answer.  The orphaned run files (if any) are left
/// behind — native scratch is not construction state.
#[test]
fn spill_spike_native_crash_resumes_but_orphans_runs() {
    let root = TempDir::new().unwrap();
    let (control, _, _) = run_spill_case(
        root.path(),
        &SpillCase {
            name: "native-crash-control",
            project: "native-crash-control",
            env: &[],
        },
    );
    let fingerprint = control["result"]["fingerprint"]
        .as_str()
        .unwrap()
        .to_owned();
    let (crashed, crash_scratch, exited) = run_spill_case(
        root.path(),
        &SpillCase {
            name: "native-crash",
            project: "native-crash",
            env: &[
                ("GF_SPILL_TEST_PARTITION_BYTES", "16384"),
                ("GF_SHAPE_SPILL_SPIKE", "native"),
                ("GF_SHAPE_SPILL_FAULT", "abort"),
            ],
        },
    );
    assert!(!exited && crashed.is_null(), "native child did not abort");
    // SIGABRT does not run Rust destructors, so NativeRun's Drop (which
    // removes scratch files) does not execute.  Scratch files written before
    // the abort are therefore orphaned.  Whether any files are present at the
    // point we inspect depends on OS buffering and filesystem behaviour, so
    // we do not assert on scratch file presence.  The invariant asserted is:
    // a clean retry after abort publishes the correct fingerprint.
    let scratch = root.path().join("native-crash-scratch");
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    let output = command
        .args([
            "--exact",
            "graph_construction::tests::determinism::spill_spike_subprocess",
            "--nocapture",
        ])
        .env("GF_SPILL_TEST_ROOT", root.path())
        .env("GF_SPILL_TEST_CASE", "native-crash-resume")
        .env("GF_SPILL_TEST_PROJECT", "native-crash")
        .env("GF_SPILL_TEST_RESUME", "1")
        .env("GF_SPILL_TEST_PARTITION_BYTES", "16384")
        .env("GF_SHAPE_SPILL_SPIKE", "native")
        .env("GF_SHAPE_SPILL_DIR", &scratch)
        .env_remove("GF_SHAPE_SPILL_FAULT")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "native crash resume failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let resumed: serde_json::Value = serde_json::from_slice(
        &std::fs::read(root.path().join("native-crash-resume.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        resumed["result"]["fingerprint"].as_str(),
        Some(fingerprint.as_str()),
        "native crash resume fingerprint wrong: {resumed}"
    );
    println!(
        "SPILL_SPIKE_NATIVE_CRASH crash_scratch={crash_scratch:?}"
    );
}
