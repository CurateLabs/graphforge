//! CLI validation for caller-produced provider-neutral decision batches.
use std::{
    io::{Read, Write},
    path::PathBuf,
};

use arrow::ipc::writer::StreamWriter;
use clap::{Args, Subcommand};
use graphforge_api::{DecisionBatchV1, GfError, GraphForge};

#[derive(Subcommand)]
pub(crate) enum DecisionCommand {
    /// Validate a JSON decision batch and write Arrow decision_result/1 IPC.
    Validate(ValidateArgs),
}

#[derive(Args)]
pub(crate) struct ValidateArgs {
    /// JSON DecisionBatchV1 file supplied by the caller or producer.
    #[arg(long)]
    file: PathBuf,
    /// Output Arrow IPC file; parent directories must already exist.
    #[arg(long)]
    output: PathBuf,
}

fn validate_file(path: PathBuf) -> Result<DecisionBatchV1, GfError> {
    let file = std::fs::File::open(path)
        .map_err(|_| GfError::Validation("cannot read decision batch file".into()))?;
    let mut bytes = Vec::new();
    file.take(1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| GfError::Validation("cannot read decision batch file".into()))?;
    if bytes.len() > 1024 * 1024 {
        return Err(GfError::Api {
            code: graphforge_api::ApiErrorCode::ResourceLimit,
            message: "decision batch input exceeds its byte bound".into(),
        });
    }
    serde_json::from_slice(&bytes)
        .map_err(|_| GfError::Validation("invalid decision batch JSON contract".into()))
}

pub(crate) fn run(
    _graph: &GraphForge,
    command: DecisionCommand,
    output: &mut dyn Write,
) -> Result<(), GfError> {
    match command {
        DecisionCommand::Validate(args) => {
            let batch = validate_file(args.file)?;
            let result = batch.validate()?;
            let mut ipc = Vec::new();
            {
                let mut writer = StreamWriter::try_new(&mut ipc, result.schema().as_ref())
                    .map_err(|_| {
                        GfError::Validation("cannot encode decision Arrow result".into())
                    })?;
                writer.write(&result).map_err(|_| {
                    GfError::Validation("cannot encode decision Arrow result".into())
                })?;
                writer.finish().map_err(|_| {
                    GfError::Validation("cannot encode decision Arrow result".into())
                })?;
            }
            std::fs::write(args.output, ipc)
                .map_err(|_| GfError::Validation("cannot write decision Arrow result".into()))?;
            writeln!(output, "validated {} decision rows", result.num_rows())
                .map_err(|_| GfError::Validation("cannot write CLI output".into()))?;
        }
    }
    Ok(())
}
