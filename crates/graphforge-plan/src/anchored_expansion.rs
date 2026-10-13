//! Logical node for a pattern expanded once per distinct bound anchor.

use std::cmp::Ordering;
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::datatypes::Field;
use datafusion::common::{DFSchema, DFSchemaRef, Result as DfResult, TableReference};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};

/// A pattern expanded once per distinct anchor node, then joined back to the
/// input rows that bound that anchor.
///
/// `MATCH (a)-[...]->(b) ...` after a stage that already bound `a` would, read
/// literally, expand `a`'s relationships once per input row, so `N` rows that
/// share one anchor expand it `N` times and materialise `N x degree` rows
/// before a filter discards most of them. This node expands the pattern once
/// per distinct anchor instead: [`expansion`](Self::expansion) starts from a
/// [`CorrelatedSeedNode`] holding the distinct anchors of
/// [`input`](Self::input), and the physical node hash-joins the expansion back
/// to the input on [`join_keys`](Self::join_keys). The input is the build side,
/// so memory is proportional to the input, not to the expansion.
///
/// # Output schema
///
/// `input` fields, followed by the `expansion` fields at
/// [`expansion_keep_idx`](Self::expansion_keep_idx). Rows are inner-joined, so
/// no column is made nullable: an input row with no match yields no row, as the
/// per-row expansion would.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AnchoredExpansionNode {
    /// The rows that bound the anchor.
    pub input: Arc<LogicalPlan>,
    /// The pattern, rooted at the [`CorrelatedSeedNode`] with id
    /// [`seed`](Self::seed).
    pub expansion: Arc<LogicalPlan>,
    /// Equality join keys as `(input_col_idx, expansion_col_idx)` pairs: the
    /// anchor, every other input-bound node the pattern touches, and every
    /// equality between an expansion-local expression and an input column.
    pub join_keys: Vec<(usize, usize)>,
    /// Expansion column indices to append to the output, in order. Excludes
    /// every column the input already carries and every synthesized key column.
    pub expansion_keep_idx: Vec<usize>,
    /// The id of the [`CorrelatedSeedNode`] the expansion starts from. The
    /// physical node executes the input once and binds its distinct anchors
    /// there, so the seed and the build side come from one evaluation.
    pub seed: u64,
    schema: DFSchemaRef,
}

impl AnchoredExpansionNode {
    /// Create the node. `expansion_keep_idx` is resolved by the lowerer against
    /// the qualified expansion schema, so this crate needs no dependency on the
    /// lowering layer.
    #[must_use]
    pub fn new(
        input: Arc<LogicalPlan>,
        expansion: Arc<LogicalPlan>,
        join_keys: Vec<(usize, usize)>,
        expansion_keep_idx: Vec<usize>,
        seed: u64,
    ) -> Self {
        let schema = Self::build_schema(&input, &expansion, &expansion_keep_idx);
        Self {
            input,
            expansion,
            join_keys,
            expansion_keep_idx,
            seed,
            schema,
        }
    }

    /// The output [`DFSchema`]: input fields, then the kept expansion fields.
    fn build_schema(
        input: &Arc<LogicalPlan>,
        expansion: &Arc<LogicalPlan>,
        expansion_keep_idx: &[usize],
    ) -> DFSchemaRef {
        let mut qualified: Vec<(Option<TableReference>, Arc<Field>)> = input
            .schema()
            .iter()
            .map(|(q, f)| (q.cloned(), Arc::clone(f)))
            .collect();
        let expanded: Vec<(Option<TableReference>, Arc<Field>)> = expansion
            .schema()
            .iter()
            .map(|(q, f)| (q.cloned(), Arc::clone(f)))
            .collect();
        qualified.extend(
            expansion_keep_idx
                .iter()
                .map(|&i| (expanded[i].0.clone(), Arc::clone(&expanded[i].1))),
        );
        Arc::new(
            DFSchema::new_with_metadata(qualified, std::collections::HashMap::new()).expect(
                "AnchoredExpansionNode schema must be constructible from input + expansion fields",
            ),
        )
    }
}

impl_partial_ord!(AnchoredExpansionNode);

impl UserDefinedLogicalNodeCore for AnchoredExpansionNode {
    fn name(&self) -> &str {
        "AnchoredExpansion"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input, &self.expansion]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AnchoredExpansion: keys={}", self.join_keys.len())
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> DfResult<Self> {
        let expansion = Arc::new(inputs.pop().unwrap_or_else(|| (*self.expansion).clone()));
        let input = Arc::new(inputs.pop().unwrap_or_else(|| (*self.input).clone()));
        Ok(Self::new(
            input,
            expansion,
            self.join_keys.clone(),
            self.expansion_keep_idx.clone(),
            self.seed,
        ))
    }
}
