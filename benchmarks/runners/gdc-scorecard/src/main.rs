#![forbid(unsafe_code)]

use std::path::PathBuf;
use std::process::ExitCode;

fn usage() -> ExitCode {
    eprintln!(
        "usage: graphforge-benchmark-gdc-scorecard convert --mapping FILE --input-root DIR --output-dir DIR"
    );
    ExitCode::from(1)
}

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    if args.next().as_deref() != Some("convert") {
        return usage();
    }
    let (mut mapping, mut input_root, mut output_dir) = (None, None, None);
    while let Some(flag) = args.next() {
        let Some(value) = args.next() else {
            return usage();
        };
        match flag.as_str() {
            "--mapping" => mapping = Some(PathBuf::from(value)),
            "--input-root" => input_root = Some(PathBuf::from(value)),
            "--output-dir" => output_dir = Some(PathBuf::from(value)),
            _ => return usage(),
        }
    }
    let (Some(mapping), Some(input_root), Some(output_dir)) = (mapping, input_root, output_dir)
    else {
        return usage();
    };
    let bytes = match std::fs::read(&mapping) {
        Ok(bytes) => bytes,
        Err(error) => {
            report(
                gdc_scorecard::Cause::InputMissing.as_str(),
                &format!("{}: {error}", mapping.display()),
            );
            return ExitCode::from(2);
        }
    };
    match gdc_scorecard::convert(&bytes, &input_root, &output_dir) {
        Ok(conversion) => {
            println!("{}", conversion.manifest_path.display());
            ExitCode::SUCCESS
        }
        Err(error) => {
            report(error.cause().as_str(), error.message());
            ExitCode::from(2)
        }
    }
}

/// One JSON object on stderr so callers read the cause without parsing prose.
fn report(cause: &str, message: &str) {
    eprintln!(
        "{}",
        serde_json::json!({"error": {"cause": cause, "message": message}})
    );
}
