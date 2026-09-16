//! Streaming DataFusion providers for topology and property tables.

use super::discover_parquet_schema;
use super::visit_property_overlay_batched_with_inventory;
use crate::parquet_scan::ParquetFragment;
use crate::parquet_scan::scan_fragments;
use crate::schemas::EXPLORATORY_EDGE_SCHEMA;
use crate::schemas::TOPOLOGY_NODES_SCHEMA;
use crate::schemas::TYPED_EDGE_SCHEMA;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::datasource::TableType;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use datafusion_catalog::Session;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// TopologyNodeTable
// ---------------------------------------------------------------------------

/// [`TableProvider`] for the logical legacy-plus-canonical node shard union.
#[derive(Debug, Clone)]
pub struct TopologyNodeTable {
    paths: Vec<PathBuf>,
}

impl TopologyNodeTable {
    /// Create a table backed by one legacy or canonical Parquet fragment.
    #[must_use]
    pub fn new(path: PathBuf) -> Self {
        Self { paths: vec![path] }
    }

    /// Open every canonical node topology fragment in deterministic order.
    pub fn open_project(dir: &Path) -> Result<Self, DataFusionError> {
        Ok(Self {
            paths: crate::mutator::node_parquet_files(dir)
                .map_err(|error| DataFusionError::Execution(error.to_string()))?,
        })
    }
}

#[async_trait]
impl TableProvider for TopologyNodeTable {
    fn schema(&self) -> SchemaRef {
        TOPOLOGY_NODES_SCHEMA.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        // Existence only — no Parquet decode during planning (#339).
        let fragments = self
            .paths
            .iter()
            .cloned()
            .map(|path| ParquetFragment::for_path(path, true))
            .collect();
        scan_fragments(
            TOPOLOGY_NODES_SCHEMA.clone(),
            fragments,
            projection,
            limit,
            state.config().batch_size(),
        )
    }
}

// ---------------------------------------------------------------------------
// TypedEdgeTable
// ---------------------------------------------------------------------------

/// [`TableProvider`] for `topology/edges/TYPENAME.parquet`.
#[derive(Debug, Clone)]
pub struct TypedEdgeTable {
    dir: PathBuf,
    inventory: Option<Arc<crate::AuthenticatedPropertyInventory>>,
    rel_type_name: String,
    schema: SchemaRef,
}

impl TypedEdgeTable {
    /// Open the edge table for `rel_type_name` inside `dir`.
    ///
    /// - `"_exploratory"` → schema includes `rel_type_name` column
    /// - any other name → [`TYPED_EDGE_SCHEMA`]
    #[must_use]
    pub fn open(dir: &Path, rel_type_name: &str) -> Self {
        let schema = if rel_type_name == "_exploratory" {
            EXPLORATORY_EDGE_SCHEMA.clone()
        } else {
            TYPED_EDGE_SCHEMA.clone()
        };
        Self {
            dir: dir.to_path_buf(),
            rel_type_name: rel_type_name.to_owned(),
            inventory: None,
            schema,
        }
    }
}

impl TypedEdgeTable {
    pub(super) fn with_inventory(
        mut self,
        inventory: Option<Arc<crate::AuthenticatedPropertyInventory>>,
    ) -> Self {
        self.inventory = inventory;
        self
    }
}

#[async_trait]
impl TableProvider for TypedEdgeTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let paths = match &self.inventory {
            Some(inventory) => inventory.edge_files(Some(&self.rel_type_name)),
            None => crate::mutator::edge_parquet_files(&self.dir, Some(&self.rel_type_name))
                .map_err(|error| DataFusionError::Execution(error.to_string()))?,
        };
        let fragments = paths
            .into_iter()
            .map(|(_, path)| ParquetFragment::for_path(path, false))
            .collect();
        scan_fragments(
            self.schema.clone(),
            fragments,
            projection,
            limit,
            state.config().batch_size(),
        )
    }
}

// ---------------------------------------------------------------------------
// UnionEdgeTable
// ---------------------------------------------------------------------------

/// [`TableProvider`] over the union of every relation's edge file (#823) — the
/// scan source for an **untyped** single-hop pattern (`(a)-[]->(b)`) in a typed
/// project, where the `_exploratory` table does not exist. Streams each
/// relation file as a natural fragment via [`scan_fragments`] (stem order);
/// the schema is always [`EXPLORATORY_EDGE_SCHEMA`] (each row tagged with its
/// source relation).
#[derive(Debug, Clone)]
pub struct UnionEdgeTable {
    pub(super) dir: PathBuf,
    pub(super) inventory: Option<Arc<crate::AuthenticatedPropertyInventory>>,
}

impl UnionEdgeTable {
    /// Open a union edge table over `dir`'s `topology/edges/`.
    #[must_use]
    pub fn open(dir: &Path) -> Self {
        Self {
            dir: dir.to_path_buf(),
            inventory: None,
        }
    }
}

#[async_trait]
impl TableProvider for UnionEdgeTable {
    fn schema(&self) -> SchemaRef {
        EXPLORATORY_EDGE_SCHEMA.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let paths = match &self.inventory {
            Some(inventory) => inventory.edge_files(None),
            None => crate::mutator::edge_parquet_files(&self.dir, None)
                .map_err(|error| DataFusionError::Execution(error.to_string()))?,
        };
        let fragments: Vec<ParquetFragment> = paths
            .into_iter()
            .map(|(stem, path)| ParquetFragment::for_union_edge(path, stem))
            .collect();
        scan_fragments(
            EXPLORATORY_EDGE_SCHEMA.clone(),
            fragments,
            projection,
            limit,
            state.config().batch_size(),
        )
    }
}

// ---------------------------------------------------------------------------
// PropertyTable
// ---------------------------------------------------------------------------

/// [`TableProvider`] for `properties/ENTITY_TYPE.parquet`.
#[derive(Debug, Clone)]
pub struct PropertyTable {
    project: PathBuf,
    pub(super) inventory: Option<Arc<crate::AuthenticatedPropertyInventory>>,
    route: String,
    schema: SchemaRef,
}

impl PropertyTable {
    /// Open a property table.
    ///
    /// If the file does not yet exist scans return an empty batch with the
    /// correct schema.
    #[must_use]
    pub fn open(dir: &Path, entity_type: &str, schema: SchemaRef) -> Self {
        Self {
            project: dir.to_path_buf(),
            inventory: None,
            route: entity_type.to_owned(),
            schema,
        }
    }

    /// Open a property table for `stem` (an entity type name, or `"_untyped"`),
    /// discovering the column schema from the Parquet file on disk.
    ///
    /// The exploratory `_untyped.parquet` file's schema is inferred at write
    /// time from the observed property literals, so the read path cannot know it
    /// statically — it must be read back from the file. When the file does not
    /// exist yet, falls back to [`PROPERTY_BASE_SCHEMA`](crate::schemas::PROPERTY_BASE_SCHEMA) (just `node_uuid`), so a
    /// join against an as-yet-unwritten property table yields zero property rows
    /// rather than an error.
    #[must_use]
    pub fn open_discovered(dir: &Path, stem: &str) -> Self {
        let path = dir.join("properties").join(format!("{stem}.parquet"));
        let inventory = admitted_property_route(dir, crate::PropertyRouteKind::Node, stem);
        let schema = inventory
            .as_ref()
            .and_then(|inventory| inventory.route_schema(crate::PropertyRouteKind::Node, stem))
            .or_else(|| discover_parquet_schema(&path))
            .unwrap_or_else(|| crate::schemas::PROPERTY_BASE_SCHEMA.clone());
        Self {
            project: dir.to_path_buf(),
            inventory,
            route: stem.to_owned(),
            schema,
        }
    }

    /// Open from one already-authenticated immutable generation inventory.
    #[must_use]
    pub fn open_authenticated(
        dir: &Path,
        stem: &str,
        inventory: Arc<crate::AuthenticatedPropertyInventory>,
    ) -> Self {
        let schema = inventory
            .route_schema(crate::PropertyRouteKind::Node, stem)
            .unwrap_or_else(|| crate::schemas::PROPERTY_BASE_SCHEMA.clone());
        Self {
            project: dir.to_path_buf(),
            inventory: Some(inventory),
            route: stem.to_owned(),
            schema,
        }
    }

    /// Visit node-property batches using this provider's retained admission.
    ///
    /// # Errors
    /// Rejects unadmitted providers and propagates storage or visitor errors.
    pub fn visit_authenticated_batches<F>(
        &self,
        batch_size: usize,
        visit: F,
    ) -> Result<(), DataFusionError>
    where
        F: FnMut(&RecordBatch) -> Result<bool, DataFusionError>,
    {
        let inventory = self.inventory.as_deref().ok_or_else(|| {
            DataFusionError::Execution("property provider has no retained inventory".into())
        })?;
        visit_property_overlay_batched_with_inventory(
            &self.project,
            Some(inventory),
            &self.route,
            false,
            batch_size,
            visit,
        )
    }

    /// The property column schema (including the `node_uuid` join key).
    #[must_use]
    pub fn schema_ref(&self) -> SchemaRef {
        self.schema.clone()
    }
}

// ---------------------------------------------------------------------------
// EdgePropertyTable
// ---------------------------------------------------------------------------

/// [`TableProvider`] for `edge_properties/REL_TYPE.parquet` (#784).
///
/// The edge analogue of [`PropertyTable`], keyed by `edge_uuid` and read from
/// the dedicated `edge_properties/` directory so a relation type cannot collide
/// with a same-named node label under `properties/`.
#[derive(Debug, Clone)]
pub struct EdgePropertyTable {
    project: PathBuf,
    pub(super) inventory: Option<Arc<crate::AuthenticatedPropertyInventory>>,
    route: String,
    schema: SchemaRef,
}

impl EdgePropertyTable {
    /// Open an edge-property table for `rel_type`, discovering the column schema
    /// from the Parquet file on disk.
    ///
    /// The per-relation schema is inferred at write time from the observed
    /// property literals, so the read path reads it back from the file. When the
    /// file does not exist yet, falls back to [`EDGE_PROPERTY_BASE_SCHEMA`](crate::schemas::EDGE_PROPERTY_BASE_SCHEMA) (just
    /// `edge_uuid`), so a join against an as-yet-unwritten edge-property table
    /// yields zero property rows rather than an error.
    #[must_use]
    pub fn open_discovered(dir: &Path, rel_type: &str) -> Self {
        let path = dir
            .join("edge_properties")
            .join(format!("{rel_type}.parquet"));
        let inventory = admitted_property_route(dir, crate::PropertyRouteKind::Edge, rel_type);
        let schema = inventory
            .as_ref()
            .and_then(|inventory| inventory.route_schema(crate::PropertyRouteKind::Edge, rel_type))
            .or_else(|| discover_parquet_schema(&path))
            .unwrap_or_else(|| crate::schemas::EDGE_PROPERTY_BASE_SCHEMA.clone());
        Self {
            project: dir.to_path_buf(),
            inventory,
            route: rel_type.to_owned(),
            schema,
        }
    }

    /// Open from one already-authenticated immutable generation inventory.
    #[must_use]
    pub fn open_authenticated(
        dir: &Path,
        route: &str,
        inventory: Arc<crate::AuthenticatedPropertyInventory>,
    ) -> Self {
        let schema = inventory
            .route_schema(crate::PropertyRouteKind::Edge, route)
            .unwrap_or_else(|| crate::schemas::EDGE_PROPERTY_BASE_SCHEMA.clone());
        Self {
            project: dir.to_path_buf(),
            inventory: Some(inventory),
            route: route.to_owned(),
            schema,
        }
    }

    /// The property column schema (including the `edge_uuid` join key).
    #[must_use]
    pub fn schema_ref(&self) -> SchemaRef {
        self.schema.clone()
    }
}

fn admitted_property_route(
    dir: &Path,
    kind: crate::PropertyRouteKind,
    route: &str,
) -> Option<Arc<crate::AuthenticatedPropertyInventory>> {
    crate::property_overlay::authenticated_property_inventory_for_route(dir, kind, route)
        .ok()
        .map(Arc::new)
}

#[async_trait]
impl TableProvider for EdgePropertyTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(Arc::new(
            crate::property_scan::PropertyOverlayExec::try_new(
                self.project.clone(),
                self.inventory.clone(),
                self.route.clone(),
                true,
                self.schema.clone(),
                crate::property_scan::PropertyScanOptions {
                    projection,
                    limit,
                    batch_size: state.config().batch_size(),
                },
            )?,
        ))
    }
}

#[async_trait]
impl TableProvider for PropertyTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(Arc::new(
            crate::property_scan::PropertyOverlayExec::try_new(
                self.project.clone(),
                self.inventory.clone(),
                self.route.clone(),
                false,
                self.schema.clone(),
                crate::property_scan::PropertyScanOptions {
                    projection,
                    limit,
                    batch_size: state.config().batch_size(),
                },
            )?,
        ))
    }
}

#[cfg(test)]
mod tests;
