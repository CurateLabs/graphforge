//! Streaming DataFusion providers for topology and property tables.

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
    /// Physical path and, when the authenticated inventory declared it, the
    /// inventory-relative path the row-count hint is taken from.
    fragments: Vec<(PathBuf, Option<String>)>,
}

impl TopologyNodeTable {
    /// Bind the node reader to its selected files, with no directory capability.
    #[must_use]
    pub fn from_files(files: &crate::TopologyFiles) -> Self {
        Self {
            fragments: files
                .node_fragments()
                .iter()
                .map(|(path, relative)| (path.clone(), Some(relative.clone())))
                .collect(),
        }
    }
    /// Create a table backed by one legacy or canonical Parquet fragment.
    #[must_use]
    pub fn new(path: PathBuf) -> Self {
        Self {
            fragments: vec![(path, None)],
        }
    }

    /// Open every canonical node topology fragment in deterministic order,
    /// listed from the directory. For a hydrated compact workspace prefer
    /// [`Self::open_with_inventory`]: a directory listing would also read a
    /// file nothing registered for admission.
    pub fn open_project(dir: &Path) -> Result<Self, DataFusionError> {
        Ok(Self {
            fragments: crate::mutator::node_parquet_files(dir)
                .map_err(|error| DataFusionError::Execution(error.to_string()))?
                .into_iter()
                .map(|path| (path, None))
                .collect(),
        })
    }

    /// Open the node fragments the authenticated inventory declares, so a
    /// file in `topology/nodes/` that the manifest does not name is never
    /// read (#1388). An inventory without topology authority (route-scoped)
    /// is refused. `None` is the explicit manifest-less standalone boundary.
    pub fn open_with_inventory(
        dir: &Path,
        inventory: Option<&crate::AuthenticatedPropertyInventory>,
    ) -> Result<Self, DataFusionError> {
        match inventory {
            Some(inventory) => Ok(Self::from_files(
                &crate::TopologyFiles::from_inventory(inventory)
                    .map_err(|error| DataFusionError::Execution(error.to_string()))?,
            )),
            None => Self::open_project(dir),
        }
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
            .fragments
            .iter()
            .map(|(path, relative)| match relative {
                Some(relative) => ParquetFragment::for_declared(path.clone(), relative, true),
                None => ParquetFragment::for_path(path.clone(), true),
            })
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
    files: Option<crate::TopologyFiles>,
    dir: PathBuf,
    inventory: Option<Arc<crate::AuthenticatedPropertyInventory>>,
    rel_type_name: String,
    schema: SchemaRef,
}

impl TypedEdgeTable {
    /// Build a relation reader that receives selected payloads without a directory.
    #[must_use]
    pub fn from_files(files: crate::TopologyFiles, route: &str) -> Self {
        let mut table = Self::open(Path::new(""), route);
        table.files = Some(files);
        table
    }

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
            files: None,
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
        let fragments = if let Some(files) = &self.files {
            files
                .edges
                .iter()
                .filter(|(route, _, _)| route == &self.rel_type_name)
                .map(|(_, path, relative)| {
                    ParquetFragment::for_declared(path.clone(), relative, false)
                })
                .collect()
        } else {
            match &self.inventory {
                Some(inventory) => inventory
                    .edge_fragments(Some(&self.rel_type_name))
                    .into_iter()
                    .map(|(_, path, relative)| {
                        ParquetFragment::for_declared(path, &relative, false)
                    })
                    .collect(),
                None => crate::mutator::edge_parquet_files(&self.dir, Some(&self.rel_type_name))
                    .map_err(|error| DataFusionError::Execution(error.to_string()))?
                    .into_iter()
                    .map(|(_, path)| ParquetFragment::for_path(path, false))
                    .collect(),
            }
        };
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
    files: Option<crate::TopologyFiles>,
    pub(super) dir: PathBuf,
    pub(super) inventory: Option<Arc<crate::AuthenticatedPropertyInventory>>,
}

impl UnionEdgeTable {
    /// Build a union reader from selected payloads without a directory.
    #[must_use]
    pub fn from_files(files: crate::TopologyFiles) -> Self {
        let mut table = Self::open(Path::new(""));
        table.files = Some(files);
        table
    }

    /// Open a union edge table over `dir`'s `topology/edges/`.
    #[must_use]
    pub fn open(dir: &Path) -> Self {
        Self {
            files: None,
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
        let fragments: Vec<ParquetFragment> = if let Some(files) = &self.files {
            files
                .edges
                .iter()
                .map(|(route, path, relative)| {
                    ParquetFragment::for_declared_union_edge(path.clone(), route.clone(), relative)
                })
                .collect()
        } else {
            match &self.inventory {
                Some(inventory) => inventory
                    .edge_fragments(None)
                    .into_iter()
                    .map(|(stem, path, relative)| {
                        ParquetFragment::for_declared_union_edge(path, stem, &relative)
                    })
                    .collect(),
                None => crate::mutator::edge_parquet_files(&self.dir, None)
                    .map_err(|error| DataFusionError::Execution(error.to_string()))?
                    .into_iter()
                    .map(|(stem, path)| ParquetFragment::for_union_edge(path, stem))
                    .collect(),
            }
        };
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
    keys_only: bool,
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
            keys_only: false,
        }
    }

    /// Open the route's `node_uuid` keys alone. Nothing is admitted to open:
    /// the schema is the key column and planning takes no footer statistics,
    /// while a scan still authenticates every fragment it reads.
    #[must_use]
    pub fn open_keys(
        dir: &Path,
        stem: &str,
        inventory: Option<Arc<crate::AuthenticatedPropertyInventory>>,
    ) -> Self {
        Self {
            project: dir.to_path_buf(),
            inventory,
            route: stem.to_owned(),
            schema: crate::schemas::PROPERTY_BASE_SCHEMA.clone(),
            keys_only: true,
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
    pub fn open_discovered(dir: &Path, stem: &str) -> Result<Self, DataFusionError> {
        let inventory = admitted_property_route(dir, crate::PropertyRouteKind::Node, stem)?;
        let schema = match inventory.as_ref() {
            Some(inventory) => inventory
                .route_schema(crate::PropertyRouteKind::Node, stem)
                .map_err(|error| DataFusionError::External(Box::new(error)))?,
            None => None,
        }
        .unwrap_or_else(|| crate::schemas::PROPERTY_BASE_SCHEMA.clone());
        Ok(Self {
            project: dir.to_path_buf(),
            inventory,
            route: stem.to_owned(),
            schema,
            keys_only: false,
        })
    }

    /// Open from one already-authenticated immutable generation inventory.
    pub fn open_authenticated(
        dir: &Path,
        stem: &str,
        inventory: Arc<crate::AuthenticatedPropertyInventory>,
    ) -> Result<Self, DataFusionError> {
        let schema = inventory
            .route_schema(crate::PropertyRouteKind::Node, stem)
            .map_err(|error| DataFusionError::External(Box::new(error)))?
            .unwrap_or_else(|| crate::schemas::PROPERTY_BASE_SCHEMA.clone());
        Ok(Self {
            project: dir.to_path_buf(),
            inventory: Some(inventory),
            route: stem.to_owned(),
            schema,
            keys_only: false,
        })
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
    keys_only: bool,
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
    pub fn open_discovered(dir: &Path, rel_type: &str) -> Result<Self, DataFusionError> {
        let inventory = admitted_property_route(dir, crate::PropertyRouteKind::Edge, rel_type)?;
        let schema = match inventory.as_ref() {
            Some(inventory) => inventory
                .route_schema(crate::PropertyRouteKind::Edge, rel_type)
                .map_err(|error| DataFusionError::External(Box::new(error)))?,
            None => None,
        }
        .unwrap_or_else(|| crate::schemas::EDGE_PROPERTY_BASE_SCHEMA.clone());
        Ok(Self {
            project: dir.to_path_buf(),
            inventory,
            route: rel_type.to_owned(),
            schema,
            keys_only: false,
        })
    }

    /// Open from one already-authenticated immutable generation inventory.
    pub fn open_authenticated(
        dir: &Path,
        route: &str,
        inventory: Arc<crate::AuthenticatedPropertyInventory>,
    ) -> Result<Self, DataFusionError> {
        let schema = inventory
            .route_schema(crate::PropertyRouteKind::Edge, route)
            .map_err(|error| DataFusionError::External(Box::new(error)))?
            .unwrap_or_else(|| crate::schemas::EDGE_PROPERTY_BASE_SCHEMA.clone());
        Ok(Self {
            project: dir.to_path_buf(),
            inventory: Some(inventory),
            route: route.to_owned(),
            schema,
            keys_only: false,
        })
    }

    /// Open the route's `edge_uuid` keys alone; see [`PropertyTable::open_keys`].
    #[must_use]
    pub fn open_keys(
        dir: &Path,
        route: &str,
        inventory: Option<Arc<crate::AuthenticatedPropertyInventory>>,
    ) -> Self {
        Self {
            project: dir.to_path_buf(),
            inventory,
            route: route.to_owned(),
            schema: crate::schemas::EDGE_PROPERTY_BASE_SCHEMA.clone(),
            keys_only: true,
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
) -> Result<Option<Arc<crate::AuthenticatedPropertyInventory>>, DataFusionError> {
    if !dir.exists() {
        return Ok(None);
    }
    crate::property_overlay::authenticated_property_inventory_for_route(dir, kind, route)
        .map(|inventory| Some(Arc::new(inventory)))
        .map_err(|error| DataFusionError::External(Box::new(error)))
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
                    footer_statistics: !self.keys_only,
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
                    footer_statistics: !self.keys_only,
                },
            )?,
        ))
    }
}

#[cfg(test)]
mod tests;
