use graphforge_benchmark_gdc_snb_interactive::{
    assemble_evidence, load_result_rows, map_operation, operation_rules, run_job,
    run_trusted_live_is1, MappingOutcome, Operation, OperationJob, OperationStatus, JOB_SCHEMA,
};
use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::ExitCode;

fn main() -> ExitCode {
    let mut args = env::args().skip(1);
    let Some(command) = args.next() else {
        eprintln!(
            "usage: graphforge-benchmark-gdc-snb-interactive \
             <list-operations|map-operation|run-suite|run-live-is1> ..."
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
        "run-live-is1" => {
            let Some(evidence_path) = args.next() else {
                eprintln!("usage: run-live-is1 EVIDENCE.json");
                return ExitCode::from(2);
            };
            if args.next().is_some() {
                eprintln!("run-live-is1 accepts only EVIDENCE.json");
                return ExitCode::from(2);
            }
            match run_live_is1(&evidence_path) {
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

fn run_live_is1(evidence_path: &str) -> Result<ExitCode, String> {
    if PathBuf::from(evidence_path).exists() {
        return Err("refusing to overwrite existing live evidence".into());
    }
    let evidence = run_trusted_live_is1().map_err(|error| error.to_string())?;
    let payload = serde_json::to_string_pretty(&evidence).map_err(|error| error.to_string())?;
    fs::write(evidence_path, format!("{payload}\n")).map_err(|error| error.to_string())?;
    Ok(ExitCode::SUCCESS)
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
            "suite requires exactly the {} modeled SNB Interactive operations, found {}",
            Operation::ALL.len(),
            declared.len()
        ));
    }
    let dataset_id = jobs[0].dataset_id.clone();
    if jobs.iter().any(|job| job.dataset_id != dataset_id) {
        return Err("suite jobs must share one dataset_id".into());
    }
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
    let evidence = assemble_evidence(&dataset_id, identities, outcomes);
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
