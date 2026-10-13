//! The relationship-only predicate of a single-hop expansion (#1984).
//!
//! `MATCH (f)<-[m:HAS_MEMBER]-(forum) WHERE m.joinDate > $minDate` keeps a
//! fraction of the traversed relationships, but the destination of every one of
//! them is read before the filter above the expansion sees it. The planner
//! copies a total conjunct that reads only the hop's own relationship
//! properties into the expansion (see [`graphforge_plan::ExpandNode::edge_predicate`]);
//! this module is the physical form of that conjunct. The expansion evaluates it
//! on the relationships' property columns, which the edge-property index
//! supplies by edge id, and reads destination nodes only for the relationships
//! that pass.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray};
use arrow::datatypes::{Field, FieldRef, Schema, SchemaRef};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::physical_expr::PhysicalExpr;
use graphforge_core::GfError;

use super::edge_filter::declared_type;

const CONTEXT: &str = "Expand edge predicate";

/// A planned predicate over some of a hop's relationship properties.
pub(crate) struct PhysicalEdgePredicate {
    predicate: Arc<dyn PhysicalExpr>,
    /// The properties the predicate reads, in the order its columns were
    /// planned against.
    fields: Vec<FieldRef>,
    /// The schema of the batch the predicate is evaluated over.
    schema: SchemaRef,
}

impl PhysicalEdgePredicate {
    /// Wrap a planned `predicate` over a batch whose columns are `fields`.
    pub(super) fn new(predicate: Arc<dyn PhysicalExpr>, fields: &[FieldRef]) -> Self {
        let columns: Vec<Field> = fields.iter().map(|field| field.as_ref().clone()).collect();
        Self {
            predicate,
            fields: fields.to_vec(),
            schema: Arc::new(Schema::new(columns)),
        }
    }

    /// The predicate, for plan display.
    pub(super) fn predicate(&self) -> &Arc<dyn PhysicalExpr> {
        &self.predicate
    }

    /// The properties the predicate reads, in column order.
    pub(super) fn fields(&self) -> &[FieldRef] {
        &self.fields
    }

    /// Evaluate the predicate for `rows` relationships whose property values
    /// are `children`, one array per [`fields`](Self::fields) entry in order.
    ///
    /// `true` keeps the relationship. `false` and `NULL` both drop it, as a
    /// `WHERE` does.
    pub(super) fn passing(
        &self,
        children: Vec<ArrayRef>,
        rows: usize,
    ) -> Result<Vec<bool>, GfError> {
        let exec_err = |m: String| GfError::Execution(format!("{CONTEXT}: {m}"));
        if children.len() != self.fields.len() {
            return Err(exec_err(format!(
                "{} property columns for {} fields",
                children.len(),
                self.fields.len()
            )));
        }
        let mut typed = Vec::with_capacity(children.len());
        for (field, child) in self.fields.iter().zip(children) {
            if child.len() != rows {
                return Err(exec_err(format!(
                    "property {} has {} rows for {rows} edges",
                    field.name(),
                    child.len()
                )));
            }
            typed.push(declared_type(field, child, rows, CONTEXT)?);
        }
        let batch = RecordBatch::try_new_with_options(
            Arc::clone(&self.schema),
            typed,
            &RecordBatchOptions::new().with_row_count(Some(rows)),
        )
        .map_err(|e| exec_err(e.to_string()))?;
        let verdict = self
            .predicate
            .evaluate(&batch)
            .and_then(|value| value.into_array(rows))
            .map_err(|e| exec_err(e.to_string()))?;
        let verdict = verdict
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| exec_err(format!("predicate yields {}", verdict.data_type())))?;
        Ok((0..rows)
            .map(|row| verdict.is_valid(row) && verdict.value(row))
            .collect())
    }
}
