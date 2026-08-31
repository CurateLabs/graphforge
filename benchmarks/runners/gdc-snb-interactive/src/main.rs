use graphforge_benchmark_gdc_snb_interactive::{
    assemble_evidence, load_result_file, map_operation, run_operation, DatasetLadder,
    MappingOutcome, Operation, OperationJob, PhasePlan, JOB_SCHEMA, LADDER_SCHEMA, PHASES_SCHEMA,
};
use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::ExitCode;

fn main() -> ExitCode {
    let mut args = env::args().skip(1);
    let Some(command) = args.next() else {
        eprintln!(
            "usage: graphforge-benchmark-gdc-snb-interactive <list-operations|list-ladder|map-job|run-suite> ..."
        );
        return ExitCode::from(2);
    };
    match command.as_str() {
        "list-operations" => list_operations(),
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
                .and_then(|job| map_operation(&job).map_err(|error| error.to_string()))
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
                    "usage: run-suite JOBS_DIR REFERENCE_DIR OUTPUT_DIR PHASES.json IDENTITIES.json EVIDENCE.json"
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
            let Some(phases_path) = args.next() else {
                eprintln!("missing PHASES.json");
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
                &phases_path,
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

fn list_operations() -> ExitCode {
    for operation in Operation::ALL {
        let job = OperationJob {
            schema: JOB_SCHEMA.into(),
            suite_id: "snb-interactive".into(),
            dataset_id: "snb-sf0.003".into(),
            operation,
            person_id: Some(1),
            message_id: Some(1),
            parameters: Default::default(),
        };
        let support = match map_operation(&job) {
            Ok(MappingOutcome::Compatible(_)) => "supported".to_string(),
            Ok(MappingOutcome::SemanticIncompatibility { cause, .. }) => cause.to_string(),
            Err(error) => format!("invalid:{error}"),
        };
        println!(
            "{} class={} validation={} support={}",
            operation.workload_key(),
            operation.class().name(),
            operation.validation_mode().name(),
            support
        );
    }
    ExitCode::SUCCESS
}

fn load_job(path: &str) -> Result<OperationJob, String> {
    let text = fs::read_to_string(path).map_err(|error| error.to_string())?;
    let job: OperationJob = serde_json::from_str(&text).map_err(|error| error.to_string())?;
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

fn load_phases(path: &str) -> Result<PhasePlan, String> {
    let text = fs::read_to_string(path).map_err(|error| error.to_string())?;
    let plan: PhasePlan = serde_json::from_str(&text).map_err(|error| error.to_string())?;
    if plan.schema != PHASES_SCHEMA {
        return Err(format!("unexpected phases schema: {}", plan.schema));
    }
    plan.validate().map_err(|error| error.to_string())?;
    Ok(plan)
}

fn run_suite(
    jobs_dir: &str,
    reference_dir: &str,
    output_dir: &str,
    phases_path: &str,
    identities_path: &str,
    evidence_path: &str,
) -> Result<ExitCode, String> {
    let identities: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(identities_path).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    let phases = load_phases(phases_path)?;
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
    if jobs.len() != Operation::ALL.len() {
        return Err(format!(
            "completeness requires exactly {} operation jobs, found {}",
            Operation::ALL.len(),
            jobs.len()
        ));
    }
    let dataset_id = jobs[0].dataset_id.clone();
    if jobs.iter().any(|job| job.dataset_id != dataset_id) {
        return Err("suite jobs must share one dataset_id".into());
    }
    let mut seen = std::collections::BTreeSet::new();
    for job in &jobs {
        if !seen.insert(job.operation) {
            return Err(format!(
                "duplicate operation job: {}",
                job.operation.workload_key()
            ));
        }
    }
    for expected in Operation::ALL {
        if !seen.contains(&expected) {
            return Err(format!(
                "missing catalog operation job: {}",
                expected.workload_key()
            ));
        }
    }
    let mut outcomes = Vec::new();
    for job in &jobs {
        let reference_path =
            PathBuf::from(reference_dir).join(format!("{}.ref", job.operation.workload_key()));
        let reference = if reference_path.is_file() {
            Some(load_result_file(&reference_path).map_err(|error| error.to_string())?)
        } else {
            None
        };
        let output_path =
            PathBuf::from(output_dir).join(format!("{}.out", job.operation.workload_key()));
        let system = if output_path.is_file() {
            Some(load_result_file(&output_path).map_err(|error| error.to_string())?)
        } else {
            None
        };
        outcomes.push(run_operation(job, reference.as_deref(), system.as_deref()));
    }
    let evidence = assemble_evidence(&dataset_id, identities, phases.phases, outcomes, false)
        .map_err(|error| error.to_string())?;
    let payload = serde_json::to_string_pretty(&evidence).map_err(|error| error.to_string())?;
    fs::write(evidence_path, format!("{payload}\n")).map_err(|error| error.to_string())?;
    let failed = evidence.operations.iter().any(|outcome| {
        matches!(
            outcome.status,
            graphforge_benchmark_gdc_snb_interactive::OperationStatus::Failed
        )
    });
    Ok(if failed {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    })
}
