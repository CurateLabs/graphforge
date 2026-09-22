//! Thin native Slice requests and bounded Arrow capsule input.
use clap::{Args, Subcommand};
use graphforge_api::{
    CancellationToken, GfError, GraphForge, PageRequest, PageToken, SlicePageKind,
};
use std::{
    io::{Read, Write},
    path::PathBuf,
};
#[derive(Subcommand)]
pub(crate) enum SliceCommand {
    /// Preview a JSON SliceRequest against one explicit source.
    Preview {
        #[arg(long)]
        file: PathBuf,
        #[command(flatten)]
        page: SlicePage,
    },
    /// Freeze a Version-based JSON SliceRequest to Arrow IPC on stdout.
    Freeze {
        #[arg(long)]
        file: PathBuf,
    },
    /// Inspect a frozen Arrow IPC capsule without reevaluating it.
    Inspect {
        #[arg(long)]
        capsule: PathBuf,
        #[command(flatten)]
        page: SlicePage,
    },
    /// Revise exact membership using a JSON SliceRevisionRequest.
    Revise {
        #[arg(long)]
        capsule: PathBuf,
        #[arg(long)]
        file: PathBuf,
    },
}
#[derive(Args)]
pub(crate) struct SlicePage {
    #[arg(long, default_value="included", value_parser=["included", "boundary", "explanations", "dependencies", "counts"])]
    kind: String,
    #[arg(long, default_value_t = 100)]
    limit: u32,
    #[arg(long)]
    after: Option<String>,
}
impl SlicePage {
    fn native(self) -> Result<(SlicePageKind, PageRequest), GfError> {
        let kind = serde_json::from_value(serde_json::Value::String(self.kind))
            .map_err(|_| GfError::Validation("invalid Slice page kind".into()))?;
        Ok((
            kind,
            PageRequest {
                limit: self.limit,
                after: self.after.as_deref().map(PageToken::parse).transpose()?,
                cancellation: None,
            },
        ))
    }
}
fn bytes(path: PathBuf, bound: usize) -> Result<Vec<u8>, GfError> {
    let file = std::fs::File::open(path)
        .map_err(|_| GfError::Validation("cannot open Slice input file".into()))?;
    let mut bytes = Vec::new();
    file.take(bound as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| GfError::Validation("cannot read Slice input file".into()))?;
    if bytes.len() > bound {
        return Err(GfError::Api {
            code: graphforge_api::ApiErrorCode::ResourceLimit,
            message: "Slice input exceeds its byte bound".into(),
        });
    }
    Ok(bytes)
}
fn request<T: serde::de::DeserializeOwned>(path: PathBuf) -> Result<T, GfError> {
    serde_json::from_slice(&bytes(path, 1024 * 1024)?)
        .map_err(|_| GfError::Validation("invalid Slice JSON contract".into()))
}
pub(crate) fn run(
    graph: &GraphForge,
    command: SliceCommand,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), GfError> {
    let result = match command {
        SliceCommand::Preview { file, page } => {
            let (kind, page) = page.native()?;
            graph.preview_slice(&request(file)?, kind, page)?
        }
        SliceCommand::Inspect { capsule, page } => {
            let (kind, page) = page.native()?;
            graph.inspect_frozen_slice(&bytes(capsule, 16 * 1024 * 1024)?, kind, page)?
        }
        SliceCommand::Freeze { file } => {
            if json {
                return Err(GfError::Validation(
                    "Slice freeze produces an Arrow IPC capsule; omit --json".into(),
                ));
            }
            graph.freeze_slice(&request(file)?, &CancellationToken::new())?
        }
        SliceCommand::Revise { capsule, file } => {
            if json {
                return Err(GfError::Validation(
                    "Slice revision produces an Arrow IPC capsule; omit --json".into(),
                ));
            }
            graph.revise_frozen_slice(
                &bytes(capsule, 16 * 1024 * 1024)?,
                &request(file)?,
                &CancellationToken::new(),
            )?
        }
    };
    crate::write_execution_result(&result, json, output)
}
