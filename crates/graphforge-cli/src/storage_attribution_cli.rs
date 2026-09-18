//! `gf storage-attribution`: authenticated, identity-free retained storage
//! attribution, with the per-phase application I/O this command performed.

use std::io::Write;
use std::path::Path;

use clap::Args;
use graphforge_api::GraphForge;

use crate::open_cli_graph;

#[derive(Args)]
pub(crate) struct StorageAttributionArgs {
    /// Also emit the recovery-on-open evidence of this command's first open as
    /// its own receipt line, before the attribution receipt, so one process
    /// carries both receipts of a reopen phase. The evidence is identical to a
    /// separate `gf recovery`; the reopen-agreement comparison is unchanged.
    #[arg(long)]
    recovery: bool,
}

#[derive(serde::Serialize)]
#[serde(deny_unknown_fields)]
struct StorageAttributionCommandReceipt {
    contract: &'static str,
    storage: graphforge_api::StorageAttributionReceipt,
    reopen_agrees: bool,
    /// Per-phase application I/O this command performed, in the same shape the
    /// construction path emits on an import receipt (#1389).
    application_io: graphforge_api::LifecyclePhaseAttribution,
}

/// Per-phase application I/O this process has performed so far.
///
/// One `gf` invocation is one lifecycle phase, so the process-wide counters are
/// exactly that phase's attribution (`gf query` splits them per statement so
/// its receipts still sum to the process). The document carries no paths,
/// identifiers, query text or graph content — only the closed phase inventory
/// and its counters.
pub(crate) fn lifecycle_application_io()
-> Result<graphforge_api::LifecyclePhaseAttribution, graphforge_api::GfError> {
    let attribution = graphforge_api::lifecycle_io_snapshot();
    attribution.validate_for_qualification()?;
    Ok(attribution)
}

pub(crate) fn run_storage_attribution(
    graph: GraphForge,
    path: &Path,
    args: &StorageAttributionArgs,
    json: bool,
    output: &mut dyn Write,
    allocation: Option<&graphforge_api::StorageAllocationDiagnostics>,
) -> Result<(), graphforge_api::GfError> {
    if args.recovery {
        crate::maintenance_cli::run_recovery(&graph, json, output)?;
    }
    let storage = graph.storage_attribution_receipt()?;
    storage.validate_reconciliation()?;
    drop(graph);
    let path_text = path.to_str().ok_or_else(|| {
        graphforge_api::GfError::Validation("--project must be valid UTF-8".into())
    })?;
    let reopened = open_cli_graph(Path::new(path_text), allocation)?;
    let reopened_storage = reopened.storage_attribution_receipt()?;
    reopened_storage.validate_reconciliation()?;
    if reopened_storage != storage {
        return Err(graphforge_api::GfError::Validation(
            "storage attribution changed across reopen".into(),
        ));
    }
    if json {
        serde_json::to_writer(
            &mut *output,
            &StorageAttributionCommandReceipt {
                contract: "graphforge-storage-attribution-command/1",
                storage,
                reopen_agrees: true,
                application_io: lifecycle_application_io()?,
            },
        )
        .map_err(|error| graphforge_api::GfError::Execution(error.to_string()))?;
        writeln!(output).map_err(|error| graphforge_api::GfError::Execution(error.to_string()))?;
    } else {
        writeln!(
            output,
            "retained_logical_eof_bytes={} allocated_physical_bytes={} reopen_agrees=true",
            storage.retained_logical_eof_bytes, storage.allocated_physical_bytes
        )
        .map_err(|error| graphforge_api::GfError::Execution(error.to_string()))?;
    }
    Ok(())
}
