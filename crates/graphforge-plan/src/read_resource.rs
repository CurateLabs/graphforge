//! Logical descriptors for the current graph's read resources.

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::logical_expr::TableSource;
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
}
