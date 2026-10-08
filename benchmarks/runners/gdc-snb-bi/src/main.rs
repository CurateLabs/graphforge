use graphforge_benchmark_gdc_snb_bi::queries::{
    BI_QUERIES, REFUSED_READS, UPSTREAM_QUERY_COMMIT, UPSTREAM_QUERY_SOURCE,
};
use graphforge_benchmark_gdc_snb_bi::query_fixture::run_query_fixture;
use graphforge_benchmark_gdc_snb_bi::{
    JOB_SCHEMA, MappingOutcome, Operation, OperationJob, OperationStatus, assemble_evidence,
    load_resource_report, load_result_rows, map_operation, operation_rules, run_job,
};
use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::ExitCode;

fn main() -> ExitCode {
    let mut args = env::args().skip(1);
    let Some(command) = args.next() else {
        eprintln!(
            "usage: graphforge-benchmark-gdc-snb-bi \
             <list-operations|list-queries|map-operation|run-queries|run-static-suite> ..."
        );
        return ExitCode::from(2);
    };
    match command.as_str() {
        "list-operations" => {
            let rules = operation_rules();
            for operation in Operation::ALL {
                println!("{} {}", operation.code(), rules[operation.code()]);
            }
            ExitCode::SUCCESS
        }
        "list-queries" => {
            if args.next().is_some() {
                eprintln!("list-queries takes no arguments");
                return ExitCode::from(2);
            }
            println!(
                "{}",
                serde_json::to_string_pretty(&queries_document()).unwrap()
            );
            ExitCode::SUCCESS
        }
        "map-operation" => {
            let Some(path) = args.next() else {
                eprintln!("usage: map-operation JOB.json");
                return ExitCode::from(2);
            };
            match load_job(&path) {
                Ok(job) => match map_operation(job.operation) {
                    MappingOutcome::Compatible(mapping) => {
                        println!("{}", serde_json::to_string_pretty(&mapping).unwrap());
                        ExitCode::SUCCESS
                    }
                    MappingOutcome::SemanticIncompatibility { cause, detail } => {
                        eprintln!("semantic_incompatibility:{cause}: {detail}");
                        ExitCode::from(3)
                    }
                },
                Err(error) => {
                    eprintln!("{error}");
                    ExitCode::FAILURE
                }
            }
        }
        "run-queries" => {
            let (Some(fixture_path), Some(evidence_path), None) =
                (args.next(), args.next(), args.next())
            else {
                eprintln!("usage: run-queries FIXTURE_DIR EVIDENCE.json");
                return ExitCode::from(2);
            };
            match run_query_fixture(&PathBuf::from(fixture_path)) {
                Ok(evidence) => {
                    let payload = serde_json::to_string_pretty(&evidence).unwrap();
                    if let Err(error) = fs::write(evidence_path, format!("{payload}\n")) {
                        eprintln!("failed to write query evidence: {error}");
                        return ExitCode::FAILURE;
                    }
                    if evidence["status"] == "passed" {
                        ExitCode::SUCCESS
                    } else {
                        eprintln!("reference_mismatch: a runnable SNB BI read failed");
                        ExitCode::FAILURE
                    }
                }
                Err(error) => {
                    eprintln!("{error}");
                    ExitCode::FAILURE
                }
            }
        }
        "run-static-suite" => {
            let Some(jobs_dir) = args.next() else {
                eprintln!(
                    "usage: run-static-suite JOBS_DIR REFERENCE_DIR OUTPUT_DIR \
                     RESOURCES.json IDENTITIES.json EVIDENCE.json"
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
            let Some(resources_path) = args.next() else {
                eprintln!("missing RESOURCES.json");
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
                &resources_path,
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
        other => {
            eprintln!("unknown command: {other}");
            ExitCode::from(2)
        }
    }
}

/// Every runnable read and every refused read as data, for a scorecard
/// workload builder (`graphforge_bench.gdc_snb_scorecard`).
fn queries_document() -> serde_json::Value {
    let queries: Vec<serde_json::Value> = BI_QUERIES
        .iter()
        .map(|query| {
            serde_json::json!({
                "operation": query.operation.code(),
                "cypher": query.cypher,
                "parameters": query.parameters.iter().map(|parameter| serde_json::json!({
                    "name": parameter.name,
                    "kind": parameter.kind.name(),
                    "fixed": parameter.fixed,
                })).collect::<Vec<_>>(),
                "columns": query.columns,
                "upstream": query.upstream,
                "rewrite": query.rewrite,
            })
        })
        .collect();
    let refused: Vec<serde_json::Value> = REFUSED_READS
        .iter()
        .map(|refusal| {
            serde_json::json!({
                "operation": refusal.operation.code(),
                "cause": refusal.cause,
            })
        })
        .collect();
    serde_json::json!({
        "schema": "graphforge-gdc-snb-bi-queries/1",
        "upstream": {"source": UPSTREAM_QUERY_SOURCE, "commit": UPSTREAM_QUERY_COMMIT},
        "queries": queries,
        "refused": refused,
    })
}

fn load_job(path: &str) -> Result<OperationJob, String> {
    let text = fs::read_to_string(path).map_err(|error| error.to_string())?;
    let job: OperationJob = serde_json::from_str(&text).map_err(|error| error.to_string())?;
    if job.schema != JOB_SCHEMA {
        return Err(format!("unexpected job schema: {}", job.schema));
    }
    Ok(job)
}

fn run_suite(
    jobs_dir: &str,
    reference_dir: &str,
    output_dir: &str,
    resources_path: &str,
    identities_path: &str,
    evidence_path: &str,
) -> Result<ExitCode, String> {
    let identities: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(identities_path).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    let mut jobs = Vec::new();
    for entry in fs::read_dir(jobs_dir).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        jobs.push(load_job(path.to_str().unwrap())?);
    }
    jobs.sort_by_key(|job| job.operation);
    let declared: Vec<Operation> = jobs.iter().map(|job| job.operation).collect();
    if declared != Operation::ALL.to_vec() {
        return Err(format!(
            "suite requires exactly the {} modeled SNB BI operations, found {}",
            Operation::ALL.len(),
            declared.len()
        ));
    }
    let dataset_id = jobs[0].dataset_id.clone();
    if jobs.iter().any(|job| job.dataset_id != dataset_id) {
        return Err("suite jobs must share one dataset_id".into());
    }

    // Resource evidence is recorded separately from correctness and fails closed
    // when absent or shaped wrong.
    let resources =
        load_resource_report(&PathBuf::from(resources_path)).map_err(|error| error.to_string())?;
    resources
        .validate(&dataset_id)
        .map_err(|error| error.to_string())?;

    let mut outcomes = Vec::new();
    for job in &jobs {
        let is_read = matches!(map_operation(job.operation), MappingOutcome::Compatible(_));
        let (reference, system) = if is_read {
            let reference_path = PathBuf::from(reference_dir).join(format!(
                "{}-{}.ref",
                dataset_id,
                job.operation.code()
            ));
            let reference = if reference_path.is_file() {
                Some(load_result_rows(&reference_path).map_err(|error| error.to_string())?)
            } else {
                None
            };
            let output_path = PathBuf::from(output_dir).join(format!(
                "{}-{}.out",
                dataset_id,
                job.operation.code()
            ));
            let system = if output_path.is_file() {
                Some(load_result_rows(&output_path).map_err(|error| error.to_string())?)
            } else {
                None
            };
            (reference, system)
        } else {
            (None, None)
        };
        outcomes.push(run_job(job, reference.as_ref(), system.as_ref()));
    }
    let evidence = assemble_evidence(&dataset_id, identities, resources, outcomes);
    let payload = serde_json::to_string_pretty(&evidence).map_err(|error| error.to_string())?;
    fs::write(evidence_path, format!("{payload}\n")).map_err(|error| error.to_string())?;
    let failed = evidence
        .operations
        .iter()
        .any(|outcome| matches!(outcome.status, OperationStatus::Failed));
    Ok(if failed {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    })
}
