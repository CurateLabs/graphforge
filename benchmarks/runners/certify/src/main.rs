use graphforge_benchmark_certify::{
    PublicProcessExecutor, certify_with_events, normalize_evidence, read_profile, write_evidence,
};
use std::env;
use std::fs;
use std::io::{self, Write};
use std::path::Path;

fn main() {
    if let Err(message) = run() {
        eprintln!("graphforge certification runner: {message}");
        std::process::exit(2);
    }
}

fn run() -> Result<(), &'static str> {
    let arguments: Vec<String> = env::args().skip(1).collect();
    match arguments.as_slice() {
        [command, profile] if command == "validate" => {
            let profile = read_profile(Path::new(profile)).map_err(|_| "invalid profile")?;
            profile.validate().map_err(|_| "invalid profile")
        }
        [command, profile, evidence] if command == "run" => {
            let profile = read_profile(Path::new(profile)).map_err(|_| "invalid profile")?;
            let mut executor = PublicProcessExecutor;
            let stdout = io::stdout();
            let mut output = stdout.lock();
            let outcome = certify_with_events(&profile, &mut executor, |event| {
                serde_json::to_writer(&mut output, event)
                    .map_err(|_| graphforge_benchmark_certify::RunnerError::Io)?;
                writeln!(output).map_err(|_| graphforge_benchmark_certify::RunnerError::Io)
            })
            .map_err(|_| "certification failed")?;
            output.flush().map_err(|_| "phase event write failed")?;
            write_evidence(Path::new(evidence), &outcome).map_err(|_| "evidence write failed")?;
            if outcome.failed_phase.is_some() {
                std::process::exit(1);
            }
            Ok(())
        }
        [command, input, output] if command == "normalize" => {
            let input = fs::read(input).map_err(|_| "legacy evidence read failed")?;
            let evidence = normalize_evidence(&input).map_err(|_| "legacy evidence invalid")?;
            write_evidence(Path::new(output), &evidence).map_err(|_| "evidence write failed")
        }
        _ => Err(
            "usage: graphforge-benchmark-certify <validate PROFILE|run PROFILE OUTPUT|normalize INPUT OUTPUT>",
        ),
    }
}
