//! The ontology-inference pass-through operator (#605).

use super::partitioned_properties;
use arrow::datatypes::SchemaRef;
use datafusion::common::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::UserDefinedLogicalNode;
use datafusion::physical_plan::DisplayAs;
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::PlanProperties;
use datafusion::physical_plan::SendableRecordBatchStream;
use std::fmt;
use std::sync::Arc;

/// Pass-through physical node for [`OntologyInferNode`](graphforge_plan::OntologyInferNode)
/// (#605). VarLenExpand already computes the transitive/symmetric closure, so this
/// simply delegates to its single input. It exists so (a) the extension planner has
/// a handler (an unhandled extension node panics at plan time), and (b) the physical
/// plan + `explain()` surface the inference `rule_id`. The provenance event itself is
/// recorded once per rule by the execution session (see `record_inference_provenance`).
pub struct OntologyInferExec {
    input: Arc<dyn ExecutionPlan>,
    rule_id: String,
    confidence_model: String,
    schema: SchemaRef,
    props: Arc<PlanProperties>,
}

impl OntologyInferExec {
    /// Build the pass-through from its logical counterpart + planned input.
    #[must_use]
    pub fn new(node: &graphforge_plan::OntologyInferNode, input: Arc<dyn ExecutionPlan>) -> Self {
        let schema: SchemaRef = Arc::new(node.schema().as_arrow().clone());
        let props = partitioned_properties(&schema, &input);
        Self {
            input,
            rule_id: node.rule_id.clone(),
            confidence_model: node.confidence_model.clone(),
            schema,
            props,
        }
    }
}

impl fmt::Debug for OntologyInferExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OntologyInferExec {{ rule_id: {} }}", self.rule_id)
    }
}

impl DisplayAs for OntologyInferExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "OntologyInferExec: rule_id={}, confidence_model={}",
            self.rule_id, self.confidence_model
        )
    }
}

impl ExecutionPlan for OntologyInferExec {
    fn name(&self) -> &str {
        "OntologyInferExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(
            &std::sync::Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        ) -> datafusion::common::Result<
            datafusion::common::tree_node::TreeNodeRecursion,
        >,
    ) -> datafusion::common::Result<datafusion::common::tree_node::TreeNodeRecursion> {
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let input = children
            .into_iter()
            .next()
            .ok_or_else(|| DataFusionError::Internal("OntologyInferExec needs one child".into()))?;
        let props = partitioned_properties(&self.schema, &input);
        Ok(Arc::new(Self {
            input,
            rule_id: self.rule_id.clone(),
            confidence_model: self.confidence_model.clone(),
            schema: self.schema.clone(),
            props,
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        // Pass-through: the wrapped input already produced the closure rows.
        self.input.execute(partition, context)
    }
}
