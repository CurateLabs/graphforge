use graphforge_benchmark_gdc_graphalytics::{
    Algorithm, AlgorithmJob, DatasetLadder, ExecutionMode, JOB_SCHEMA, LADDER_SCHEMA,
    MappingOutcome, assemble_evidence, determinism_rules, load_live_graph, load_vertex_value_file,
    map_algorithm, run_job, run_live_job,
};
use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::ExitCode;

fn main() -> ExitCode {
    let mut args = env::args().skip(1);
    let Some(command) = args.next() else {
        eprintln!(
            "usage: graphforge-benchmark-gdc-graphalytics <list-algorithms|list-ladder|map-job|run-suite|run-live> ..."
        );
        return ExitCode::from(2);
    };
    match command.as_str() {
        "list-algorithms" => {
            let rules = determinism_rules();
            for algorithm in Algorithm::ALL {
                let mode = algorithm.validation_mode();
                println!(
                    "{} validation={} determinism={}",
                    algorithm.workload_key(),
                    mode.name(),
                    rules[algorithm.workload_key()]
                );
            }
            ExitCode::SUCCESS
        }
        "list-ladder" => {
            let Some(path) = args.next() else {
                eprintln!("usage: list-ladder LADDER.json");
                return ExitCode::from(2);
            };
            match load_ladder(&path) {
                Ok(ladder) => {
                    for id in ladder.ordered_ids() {
                        println!("{id}");
                    }
                    ExitCode::SUCCESS
                }
                Err(error) => {
                    eprintln!("{error}");
                    ExitCode::FAILURE
                }
            }
        }
        "map-job" => {
            let Some(path) = args.next() else {
                eprintln!("usage: map-job JOB.json");
                return ExitCode::from(2);
            };
            match load_job(&path)
                .and_then(|job| map_algorithm(&job).map_err(|error| error.to_string()))
            {
                Ok(MappingOutcome::Compatible(mapping)) => {
                    println!("{}", serde_json::to_string_pretty(&mapping).unwrap());
                    ExitCode::SUCCESS
                }
                Ok(MappingOutcome::SemanticIncompatibility { cause, detail }) => {
                    eprintln!("semantic_incompatibility:{cause}: {detail}");
                    ExitCode::from(3)
                }
                Err(error) => {
                    eprintln!("{error}");
                    ExitCode::FAILURE
                }
            }
        }
        "run-suite" => {
            let Some(jobs_dir) = args.next() else {
                eprintln!(
                    "usage: run-suite JOBS_DIR REFERENCE_DIR OUTPUT_DIR IDENTITIES.json EVIDENCE.json"
                );
                return ExitCode::from(2);
            };
            let Some(reference_dir) = args.next() else {
                eprintln!("missing REFERENCE_DIR");
                return ExitCode::from(2);
            };
            let Some(output_dir) = args.next() else {
                eprintln!("missing OUTPUT_DIR");
                return ExitCode::from(2);
            };
            let Some(identities_path) = args.next() else {
                eprintln!("missing IDENTITIES.json");
                return ExitCode::from(2);
            };
            let Some(evidence_path) = args.next() else {
                eprintln!("missing EVIDENCE.json");
                return ExitCode::from(2);
            };
            match run_suite(
                &jobs_dir,
                &reference_dir,
                &output_dir,
                &identities_path,
                &evidence_path,
            ) {
                Ok(code) => code,
                Err(error) => {
                    eprintln!("{error}");
                    ExitCode::FAILURE
                }
            }
        }
        "run-live" => {
            let arguments = args.collect::<Vec<_>>();
            if arguments.len() != 5 {
                eprintln!(
                    "usage: run-live EDGES_FILE JOBS_DIR REFERENCE_DIR IDENTITIES.json EVIDENCE.json"
                );
                return ExitCode::from(2);
            }
            match run_live_suite(
                &arguments[0],
                &arguments[1],
                &arguments[2],
                &arguments[3],
                &arguments[4],
            ) {
                Ok(code) => code,
                Err(error) => {
                    eprintln!("{error}");
                    ExitCode::FAILURE
                }
            }
        }
        other => {
            eprintln!("unknown command: {other}");
            ExitCode::from(2)
        }
    }
}

fn load_job(path: &str) -> Result<AlgorithmJob, String> {
    let text = fs::read_to_string(path).map_err(|error| error.to_string())?;
    let job: AlgorithmJob = serde_json::from_str(&text).map_err(|error| error.to_string())?;
    if job.schema != JOB_SCHEMA {
        return Err(format!("unexpected job schema: {}", job.schema));
    }
    Ok(job)
}

fn load_ladder(path: &str) -> Result<DatasetLadder, String> {
    let text = fs::read_to_string(path).map_err(|error| error.to_string())?;
    let ladder: DatasetLadder = serde_json::from_str(&text).map_err(|error| error.to_string())?;
    if ladder.schema != LADDER_SCHEMA {
        return Err(format!("unexpected ladder schema: {}", ladder.schema));
    }
    ladder.validate().map_err(|error| error.to_string())?;
    Ok(ladder)
}

fn run_suite(
    jobs_dir: &str,
    reference_dir: &str,
    output_dir: &str,
    identities_path: &str,
    evidence_path: &str,
) -> Result<ExitCode, String> {
    let identities: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(identities_path).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    let jobs = load_jobs(jobs_dir)?;
    let dataset_id = jobs[0].dataset_id.clone();
    let mut outcomes = Vec::new();
    for job in &jobs {
        let reference_path =
            PathBuf::from(reference_dir).join(format!("{}-{}.ref", dataset_id, job.algorithm));
        let reference =
            load_vertex_value_file(&reference_path).map_err(|error| error.to_string())?;
        let output_path =
            PathBuf::from(output_dir).join(format!("{}-{}.out", dataset_id, job.algorithm));
        let system = if output_path.is_file() {
            Some(load_vertex_value_file(&output_path).map_err(|error| error.to_string())?)
        } else {
            None
        };
        outcomes.push(run_job(job, &reference, system.as_ref()));
    }
    write_evidence(
        &dataset_id,
        identities,
        ExecutionMode::StaticReplay,
        outcomes,
        evidence_path,
    )
}

fn run_live_suite(
    edges_path: &str,
    jobs_dir: &str,
    reference_dir: &str,
    identities_path: &str,
    evidence_path: &str,
) -> Result<ExitCode, String> {
    let identities: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(identities_path).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    let jobs = load_jobs(jobs_dir)?;
    validate_live_inputs(edges_path, reference_dir, &identities, &jobs)?;
    let dataset_id = jobs[0].dataset_id.clone();
    let live =
        load_live_graph(PathBuf::from(edges_path).as_path()).map_err(|error| error.to_string())?;
    let mut outcomes = Vec::new();
    for job in &jobs {
        let reference_path =
            PathBuf::from(reference_dir).join(format!("{}-{}.ref", dataset_id, job.algorithm));
        let reference =
            load_vertex_value_file(&reference_path).map_err(|error| error.to_string())?;
        outcomes.push(run_live_job(&live, job, &reference));
    }
    write_evidence(
        &dataset_id,
        identities,
        ExecutionMode::LivePublicApi,
        outcomes,
        evidence_path,
    )
}

fn validate_live_inputs(
    edges_path: &str,
    reference_dir: &str,
    identities: &serde_json::Value,
    jobs: &[AlgorithmJob],
) -> Result<(), String> {
    use sha2::{Digest, Sha256};
    let pin: serde_json::Value = serde_json::from_str(include_str!(
        "../../../profiles/gdc/graphalytics-live-identity.json"
    ))
    .map_err(|error| error.to_string())?;
    let datasets = pin["datasets"].as_array().unwrap().iter().map(|item| serde_json::json!({
        "id": item["id"], "checksum_sha256": item["checksum_sha256"], "license": item["license"]
    })).collect::<Vec<_>>();
    let references = pin["references"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| {
            serde_json::json!({
                "dataset_id": item["dataset_id"], "workload_key": item["workload_key"],
                "checksum_sha256": item["checksum_sha256"]
            })
        })
        .collect::<Vec<_>>();
    let expected = serde_json::json!({
        "spec": pin["spec"], "generator": pin["generator"], "driver": pin["driver"],
        "datasets": datasets, "references": references
    });
    if identities != &expected {
        return Err("live_identity_mismatch".into());
    }
    let check = |path: &std::path::Path, digest: &serde_json::Value| -> Result<(), String> {
        let bytes = fs::read(path).map_err(|error| error.to_string())?;
        let actual = Sha256::digest(bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        if Some(actual.as_str()) != digest.as_str() {
            return Err(format!("live_asset_checksum_mismatch:{}", path.display()));
        }
        Ok(())
    };
    check(
        std::path::Path::new(edges_path),
        &pin["datasets"][0]["checksum_sha256"],
    )?;
    for reference in pin["references"].as_array().unwrap() {
        check(
            &PathBuf::from(reference_dir).join(format!(
                "ga-tiny-{}.ref",
                reference["workload_key"].as_str().unwrap()
            )),
            &reference["checksum_sha256"],
        )?;
    }
    let pinned_jobs = [
        include_str!("../../../fixtures/gdc/graphalytics-tiny/compatible/jobs/bfs.json"),
        include_str!("../../../fixtures/gdc/graphalytics-tiny/compatible/jobs/pr.json"),
        include_str!("../../../fixtures/gdc/graphalytics-tiny/compatible/jobs/wcc.json"),
        include_str!("../../../fixtures/gdc/graphalytics-tiny/compatible/jobs/cdlp.json"),
        include_str!("../../../fixtures/gdc/graphalytics-tiny/compatible/jobs/lcc.json"),
        include_str!("../../../fixtures/gdc/graphalytics-tiny/compatible/jobs/sssp.json"),
    ];
    for job in jobs {
        let expected = pinned_jobs
            .iter()
            .map(|text| serde_json::from_str::<AlgorithmJob>(text).unwrap())
            .find(|expected| expected.algorithm == job.algorithm)
            .unwrap();
        if serde_json::to_value(job).unwrap() != serde_json::to_value(expected).unwrap() {
            return Err(format!("live_job_context_mismatch:{}", job.algorithm));
        }
    }
    Ok(())
}

fn load_jobs(jobs_dir: &str) -> Result<Vec<AlgorithmJob>, String> {
    let mut jobs = Vec::new();
    for entry in fs::read_dir(jobs_dir).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        jobs.push(load_job(path.to_str().unwrap())?);
    }
    jobs.sort_by_key(|job| job.algorithm);
    if jobs.len() != Algorithm::ALL.len() {
        return Err(format!(
            "suite requires exactly six algorithm jobs, found {}",
            jobs.len()
        ));
    }
    if jobs
        .iter()
        .map(|job| job.algorithm)
        .collect::<std::collections::BTreeSet<_>>()
        != Algorithm::ALL.into_iter().collect()
    {
        return Err("suite requires each of the six algorithms exactly once".into());
    }
    let dataset_id = jobs[0].dataset_id.clone();
    if jobs.iter().any(|job| job.dataset_id != dataset_id) {
        return Err("suite jobs must share one dataset_id".into());
    }
    Ok(jobs)
}

fn write_evidence(
    dataset_id: &str,
    identities: serde_json::Value,
    execution_mode: ExecutionMode,
    outcomes: Vec<graphforge_benchmark_gdc_graphalytics::AlgorithmOutcome>,
    evidence_path: &str,
) -> Result<ExitCode, String> {
    let evidence = assemble_evidence(dataset_id, identities, execution_mode, outcomes);
    let payload = serde_json::to_string_pretty(&evidence).map_err(|error| error.to_string())?;
    fs::write(evidence_path, format!("{payload}\n")).map_err(|error| error.to_string())?;
    let failed = evidence.algorithms.iter().any(|outcome| {
        matches!(
            outcome.status,
            graphforge_benchmark_gdc_graphalytics::AlgorithmStatus::Failed
        )
    });
    Ok(if failed {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    })
}
