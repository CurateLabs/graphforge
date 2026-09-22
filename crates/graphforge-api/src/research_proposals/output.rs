//! Bounded Arrow item previews; no data-bearing JSON result shortcut.
use super::preview::Preview;
use crate::{ExecutionResult, GfError};
use arrow::{
    array::{Array, ArrayRef, BooleanArray, ListBuilder, StringArray, StringBuilder},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use std::sync::Arc;

pub(super) fn hex(bytes: &[u8; 32]) -> String {
    use std::fmt::Write;
    bytes
        .iter()
        .fold(String::with_capacity(64), |mut out, byte| {
            write!(&mut out, "{byte:02x}").expect("String writing");
            out
        })
}

pub(super) fn preview(preview: &Preview) -> Result<ExecutionResult, GfError> {
    let (mut fields, mut arrays) = text_columns(preview);
    for (name, values) in [
        (
            "conflict",
            preview
                .rows
                .iter()
                .map(|row| row.conflict)
                .collect::<Vec<_>>(),
        ),
        (
            "already_accepted",
            preview
                .rows
                .iter()
                .map(|row| row.already_accepted)
                .collect(),
        ),
    ] {
        fields.push(Field::new(name, DataType::Boolean, false));
        arrays.push(Arc::new(BooleanArray::from(values)));
    }
    for name in [
        "required_items",
        "unavailable_dependencies",
        "evidence_gaps",
    ] {
        let mut list = ListBuilder::new(StringBuilder::new());
        for row in &preview.rows {
            if name == "required_items" {
                for id in &row.required_items {
                    list.values().append_value(id.to_string());
                }
            } else if name == "unavailable_dependencies" {
                for value in &row.unavailable {
                    list.values().append_value(value);
                }
            } else {
                for (artifact, availability) in &row.evidence_gaps {
                    list.values()
                        .append_value(format!("{artifact}:{availability}"));
                }
            }
            list.append(true);
        }
        let array = list.finish();
        fields.push(Field::new(name, array.data_type().clone(), false));
        arrays.push(Arc::new(array));
    }
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|error| GfError::Validation(error.to_string()))?;
    if batch.get_array_memory_size() > 16 * 1024 * 1024 {
        return Err(GfError::Project {
            code: graphforge_core::ProjectErrorCode::ResourceLimit,
            message: "Proposal preview exceeds its 16 MiB Arrow output limit".into(),
        });
    }
    Ok(crate::knowledge::assertion_result(batch))
}

fn text_columns(preview: &Preview) -> (Vec<Field>, Vec<ArrayRef>) {
    let proposal = &preview.proposal;
    let mut fields = Vec::new();
    let mut arrays: Vec<ArrayRef> = Vec::new();
    macro_rules! text {
        ($name:literal, $values:expr) => {{
            fields.push(Field::new($name, DataType::Utf8, true));
            arrays.push(Arc::new($values.collect::<StringArray>()));
        }};
    }
    let n = preview.rows.len();
    text!(
        "proposal_uuid",
        (0..n).map(|_| Some(proposal.proposal_uuid.to_string()))
    );
    text!(
        "generation_uuid",
        (0..n).map(|_| Some(preview.generation.to_string()))
    );
    text!("preview_sha256", (0..n).map(|_| Some(hex(&preview.digest))));
    text!(
        "source_branch_uuid",
        (0..n).map(|_| Some(proposal.source_branch_uuid.to_string()))
    );
    text!(
        "source_version_uuid",
        (0..n).map(|_| Some(proposal.source_version_uuid.to_string()))
    );
    text!(
        "item_uuid",
        proposal
            .items
            .iter()
            .map(|item| Some(item.item_uuid.to_string()))
    );
    text!(
        "contribution_uuid",
        proposal
            .items
            .iter()
            .map(|item| Some(item.contribution_uuid.to_string()))
    );
    text!(
        "object_kind",
        proposal
            .items
            .iter()
            .map(|item| Some(item.unit.object_kind.clone()))
    );
    text!(
        "object_uuid",
        proposal
            .items
            .iter()
            .map(|item| Some(item.unit.object_uuid.to_string()))
    );
    text!(
        "field",
        proposal
            .items
            .iter()
            .map(|item| Some(item.unit.field.clone()))
    );
    text!(
        "proposed_sha256",
        proposal
            .items
            .iter()
            .map(|item| item.value_sha256.as_ref().map(hex))
    );
    text!(
        "baseline_sha256",
        proposal
            .items
            .iter()
            .map(|item| item.baseline_sha256.as_ref().map(hex))
    );
    text!(
        "review_baseline_sha256",
        preview
            .rows
            .iter()
            .map(|row| row.review_baseline.as_ref().map(hex))
    );
    text!(
        "destination_sha256",
        preview
            .rows
            .iter()
            .map(|row| row.destination_value.as_ref().map(hex))
    );
    text!(
        "motivation",
        (0..n).map(|_| Some(proposal.motivation.clone()))
    );
    (fields, arrays)
}
