//! Thin JSON requests and Arrow/control output for native contextual research.
use clap::Args;
use graphforge_api::{CancellationToken, GfError, GraphForge};
use std::{
    io::{Read, Write},
    path::PathBuf,
};
#[derive(Args)]
pub(crate) struct Request {
    /// Exact native JSON request file.
    #[arg(long)]
    file: PathBuf,
}
fn request<T: serde::de::DeserializeOwned>(args: Request) -> Result<T, GfError> {
    let file = std::fs::File::open(args.file)
        .map_err(|_| GfError::Validation("cannot open research comparison request".into()))?;
    let mut bytes = Vec::new();
    file.take(1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| GfError::Validation("cannot read research comparison request".into()))?;
    if bytes.len() > 1024 * 1024 {
        return Err(GfError::Api {
            code: graphforge_api::ApiErrorCode::ResourceLimit,
            message: "research comparison request exceeds byte limit".into(),
        });
    }
    serde_json::from_slice(&bytes)
        .map_err(|_| GfError::Validation("invalid research comparison JSON contract".into()))
}
pub(crate) fn run(
    graph: &GraphForge,
    args: Request,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), GfError> {
    let result = graph.compare_research(&request(args)?, &CancellationToken::new())?;
    crate::write_execution_result(&result, json, output)
}
