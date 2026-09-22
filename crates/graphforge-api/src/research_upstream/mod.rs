//! Reviewed incorporation of exact immediate-upstream research state.
mod adoption;
mod baselines;
mod dependencies;
mod history;
mod model;
mod preferences;
mod preview;
mod projection;
mod retain_both;
mod selection;
#[cfg(test)]
mod tests;
mod update;
use crate::{CancellationToken, ExecutionResult, GfError, GraphForge};
pub use model::*;

impl GraphForge {
    /// Preview selected upstream changes against each field's incorporated baseline.
    /// Opening or previewing a Branch never advances its immutable base or current head.
    pub fn preview_research_upstream(
        &self,
        request: &PreviewResearchUpstreamRequest,
        cancellation: &CancellationToken,
    ) -> Result<ExecutionResult, GfError> {
        let preview = preview::load(self, request, cancellation)?;
        let metadata = std::collections::HashMap::from([
            ("graphforge.upstream.contract".into(), "1".into()),
            (
                "graphforge.upstream.preview_sha256".into(),
                preview::hex(&preview.digest),
            ),
            (
                "graphforge.upstream.authority_generation_uuid".into(),
                preview.current.generation_uuid().to_string(),
            ),
            (
                "graphforge.upstream.original_base_version_uuid".into(),
                preview.branch.base_version_uuid.to_string(),
            ),
            (
                "graphforge.upstream.local_version_uuid".into(),
                preview
                    .local
                    .version
                    .map(|id| id.to_string())
                    .unwrap_or_default(),
            ),
            (
                "graphforge.upstream.upstream_version_uuid".into(),
                preview
                    .upstream
                    .version
                    .map(|id| id.to_string())
                    .unwrap_or_default(),
            ),
            (
                "graphforge.upstream.upstream_generation_uuid".into(),
                preview.upstream.generation.to_string(),
            ),
        ]);
        let batch = crate::research_comparison::output::changes(&preview.rows, metadata)?;
        let mut fields = batch.schema().fields().to_vec();
        let mut columns = batch.columns().to_vec();
        for (name, evidence) in [
            ("required_fields", false),
            ("required_evidence_acknowledgements", true),
        ] {
            fields.push(std::sync::Arc::new(arrow::datatypes::Field::new(
                name,
                arrow::datatypes::DataType::Utf8,
                false,
            )));
            let values = preview
                .rows
                .iter()
                .map(|row| {
                    let Some(requirement) = preview.requirements.get(&row.key) else {
                        return Ok("[]".to_owned());
                    };
                    if evidence {
                        serde_json::to_string(&requirement.evidence)
                    } else {
                        serde_json::to_string(&requirement.fields)
                    }
                    .map_err(|_| invalid("invalid upstream dependency row"))
                })
                .collect::<Result<Vec<_>, GfError>>()?;
            columns.push(std::sync::Arc::new(arrow::array::StringArray::from(values)));
        }
        let batch = arrow::record_batch::RecordBatch::try_new(
            std::sync::Arc::new(arrow::datatypes::Schema::new_with_metadata(
                fields,
                batch.schema().metadata().clone(),
            )),
            columns,
        )
        .map_err(|error| invalid(&error.to_string()))?;
        if batch.get_array_memory_size() > 16 * 1024 * 1024 {
            return Err(GfError::Api {
                code: graphforge_core::ApiErrorCode::ResourceLimit,
                message: "upstream preview exceeds 16 MiB; narrow its field scope".into(),
            });
        }
        cancellation.checkpoint()?;
        Ok(crate::knowledge::assertion_result(batch))
    }
}
fn invalid(message: &str) -> GfError {
    GfError::Validation(message.into())
}
