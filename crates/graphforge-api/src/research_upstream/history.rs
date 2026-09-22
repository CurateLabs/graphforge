//! Arrow inspection of immutable review decisions, independent of restored content.
use super::{ResearchUpstreamHistoryRequest, invalid, preview::hex};
use crate::{CancellationToken, ExecutionResult, GfError, GraphForge};
use arrow::{
    array::{ArrayRef, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use sha2::{Digest, Sha256};
use std::{collections::HashMap, sync::Arc};

impl GraphForge {
    /// Inspect exact upstream resolutions after restore, cleanup or reopen.
    pub fn research_upstream_history(
        &self,
        request: &ResearchUpstreamHistoryRequest,
        cancel: &CancellationToken,
    ) -> Result<ExecutionResult, GfError> {
        cancel.checkpoint()?;
        crate::research_claims::authority::require_owner(self)?;
        if !(1..=1000).contains(&request.page_size)
            || request.after.as_ref().is_some_and(|s| s.len() > 128)
        {
            return Err(invalid("invalid upstream history page bounds"));
        }
        let current = graphforge_storage::resolve_project_generation(
            self.resolved_generation.container_root(),
        )?;
        let registry = graphforge_storage::research_versions::read_research_registry(&current)?;
        if !registry.branches.contains_key(&request.branch_uuid) {
            return Err(invalid("upstream history Branch is unavailable"));
        }
        let mut digest = Sha256::new();
        digest.update(b"graphforge-upstream-history/1");
        digest.update(current.generation_uuid().as_bytes());
        digest.update(request.branch_uuid.as_bytes());
        digest.update((request.page_size as u64).to_le_bytes());
        let binding = hex(&digest.finalize().into());
        let offset = request
            .after
            .as_ref()
            .map(|after| {
                let (commitment, offset) = after.split_once(':').ok_or_else(stale)?;
                let offset = offset.parse::<usize>().map_err(|_| stale())?;
                if commitment != binding || offset > 4096 * 256 {
                    return Err(stale());
                }
                Ok(offset)
            })
            .transpose()?
            .unwrap_or(0);
        let mut reviews: Vec<_> = registry
            .upstream
            .reviews
            .values()
            .filter(|review| review.branch_uuid == request.branch_uuid)
            .collect();
        reviews.sort_by_key(|review| review.sequence);
        let total: usize = reviews.iter().map(|review| review.fields.len()).sum();
        if offset > total {
            return Err(stale());
        }
        let mut rows = Vec::new();
        let mut bytes = 0usize;
        for (review, field) in reviews
            .iter()
            .flat_map(|review| review.fields.iter().map(move |field| (*review, field)))
            .skip(offset)
            .take(request.page_size)
        {
            cancel.checkpoint()?;
            let resolution = serde_json::to_string(&field.resolution)
                .map_err(|_| invalid("invalid upstream resolution"))?;
            let values = vec![
                review.sequence.to_string(),
                review.operation_uuid.to_string(),
                review.branch_uuid.to_string(),
                review.original_base_version_uuid.to_string(),
                review.prior_version_uuid.to_string(),
                review.upstream_version_uuid.to_string(),
                review.version_uuid.to_string(),
                review.preview_generation_uuid.to_string(),
                review.actor_uuid.to_string(),
                review.created_at.to_string(),
                review.explanation.clone(),
                field.unit.object_kind.clone(),
                field.unit.object_uuid.to_string(),
                field.unit.field.clone(),
                resolution,
                field.baseline_sha256.as_ref().map(hex).unwrap_or_default(),
                field.local_sha256.as_ref().map(hex).unwrap_or_default(),
                field.upstream_sha256.as_ref().map(hex).unwrap_or_default(),
                field.result_sha256.as_ref().map(hex).unwrap_or_default(),
            ];
            bytes = bytes.saturating_add(values.iter().map(String::len).sum::<usize>() + 1024);
            if bytes > 16 * 1024 * 1024 {
                return Err(GfError::Api {
                    code: graphforge_core::ApiErrorCode::ResourceLimit,
                    message: "upstream history page exceeds 16 MiB".into(),
                });
            }
            rows.push(values);
        }
        let names = [
            "sequence",
            "operation_uuid",
            "branch_uuid",
            "original_base_version_uuid",
            "prior_version_uuid",
            "upstream_version_uuid",
            "version_uuid",
            "preview_generation_uuid",
            "actor_uuid",
            "created_at",
            "explanation",
            "object_kind",
            "object_uuid",
            "field",
            "resolution",
            "baseline_sha256",
            "local_sha256",
            "upstream_sha256",
            "result_sha256",
        ];
        let next = offset + rows.len();
        let metadata = HashMap::from([
            ("graphforge.upstream.history_contract".into(), "1".into()),
            (
                "graphforge.upstream.generation_uuid".into(),
                current.generation_uuid().to_string(),
            ),
            (
                "graphforge.upstream.next_cursor".into(),
                if next < total {
                    format!("{binding}:{next}")
                } else {
                    String::new()
                },
            ),
        ]);
        let schema = Arc::new(Schema::new_with_metadata(
            names
                .iter()
                .map(|name| Field::new(*name, DataType::Utf8, false))
                .collect::<Vec<_>>(),
            metadata,
        ));
        let columns: Vec<ArrayRef> = (0..names.len())
            .map(|column| {
                Arc::new(StringArray::from(
                    rows.iter()
                        .map(|row| row[column].as_str())
                        .collect::<Vec<_>>(),
                )) as ArrayRef
            })
            .collect();
        let batch =
            RecordBatch::try_new(schema, columns).map_err(|error| invalid(&error.to_string()))?;
        Ok(crate::knowledge::assertion_result(batch))
    }
}
fn stale() -> GfError {
    GfError::Api {
        code: graphforge_core::ApiErrorCode::PageSnapshotGone,
        message: "upstream history continuation is stale; restart the same query".into(),
    }
}
