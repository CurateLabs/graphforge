//! Logical descriptors for the current graph's read resources.

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableSource};
use graphforge_value::{EntityTypeId, RelationTypeId};
use std::sync::Arc;

/// Semantic assumptions used to admit a read binding, independent of location.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GraphReadContract {
    /// Checked node identities and their logical names/routes.
    pub labels: Vec<(EntityTypeId, String)>,
    /// Checked relationship identities and logical names/routes.
    pub relations: Vec<(RelationTypeId, String)>,
    /// Semantic composition, never a project or generation identity.
    pub composition: Option<String>,
}

/// A deterministic role in the query, independent of its execution location.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum GraphReadTable {
    /// Node topology, including the selected generation's overlays.
    Nodes,
    /// Named edge topology, or `_exploratory` for the all-relations union.
    Edges(String),
    /// An authenticated semantic relation.
    SemanticEdges(RelationTypeId),
    /// Node property route selected by semantic lowering.
    Properties(String),
    /// Edge properties, optionally requiring a semantic relation provider.
    EdgeProperties(String, Option<RelationTypeId>),
    /// The `node_uuid` keys of a node property route, for a plan that reads no
    /// property value: binding it admits no route content.
    PropertyKeys(String),
    /// The `edge_uuid` keys of an edge property route; see
    /// [`PropertyKeys`](Self::PropertyKeys).
    EdgePropertyKeys(String),
}

/// Schema-discovered logical source. Contains no executable provider.
#[derive(Debug, Clone)]
pub struct GraphReadSource {
    /// Resource to bind in the execution session.
    pub table: GraphReadTable,
    /// Semantic composition required by this source, where applicable.
    pub composition: Option<String>,
    /// Complete binding assumptions supplied by semantic lowering.
    pub contract: Option<GraphReadContract>,
    schema: SchemaRef,
}

impl GraphReadSource {
    /// Construct a source from logical routing and a discovered output schema.
    #[must_use]
    pub fn new(
        table: GraphReadTable,
        schema: &SchemaRef,
        composition: Option<String>,
    ) -> Arc<Self> {
        Arc::new(Self {
            table,
            schema: semantic_read_schema(schema),
            composition,
            contract: None,
        })
    }
}

/// Remove storage-observation metadata from logical schema identity. The bound
/// physical provider retains its original schema and metadata.
#[must_use]
pub fn semantic_read_schema(schema: &SchemaRef) -> SchemaRef {
    let mut metadata = schema.metadata().clone();
    metadata.remove("graphforge.property_live_schema");
    Arc::new(datafusion::arrow::datatypes::Schema::new_with_metadata(
        schema.fields().clone(),
        metadata,
    ))
}

impl TableSource for GraphReadSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// A node property route can answer `column = literal` from footer
    /// statistics. The filter stays in the plan, so the pushdown is a hint:
    /// the bound provider returns every row the equality can select and the
    /// plan's own filter decides which of them match.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|filter| {
                if matches!(self.table, GraphReadTable::Properties(_))
                    && is_stored_equality(filter, &self.schema)
                {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }
}

/// `column = literal` on a stored column of the literal's own Arrow type, for
/// the types whose equality needs no coercion.
fn is_stored_equality(filter: &Expr, schema: &SchemaRef) -> bool {
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::Operator;
    let Expr::BinaryExpr(binary) = filter else {
        return false;
    };
    if binary.op != Operator::Eq {
        return false;
    }
    let ((Expr::Column(column), Expr::Literal(literal, _))
    | (Expr::Literal(literal, _), Expr::Column(column))) =
        (binary.left.as_ref(), binary.right.as_ref())
    else {
        return false;
    };
    let data_type = match literal {
        ScalarValue::Int64(Some(_)) => datafusion::arrow::datatypes::DataType::Int64,
        ScalarValue::Utf8(Some(_)) => datafusion::arrow::datatypes::DataType::Utf8,
        ScalarValue::Boolean(Some(_)) => datafusion::arrow::datatypes::DataType::Boolean,
        _ => return false,
    };
    schema
        .field_with_name(&column.name)
        .is_ok_and(|field| field.data_type() == &data_type)
}
