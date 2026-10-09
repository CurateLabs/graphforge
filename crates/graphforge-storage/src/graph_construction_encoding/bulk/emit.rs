//! Pass 3: runtime catalog, node and edge tables, and property overlays.

use std::collections::{BTreeMap, BTreeSet};

use arrow::array::Array;
use arrow::compute::concat_batches;
use graphforge_ir::RuntimeCatalog;
use rayon::prelude::*;

use super::install::Installer;
use super::tables::{EdgeTable, NodeTable, first_appearance};
use super::{
    Arc, AtomicBool, CompositionBindingContext, ConstructionChunkKind, EntityTypeId, GfError,
    GraphConstructionBudgets, OntologyMode, RecordBatch, SemanticRouteKind,
    SemanticStorageBindings, StringArray, SymbolKind, UInt64Array, edge_batch,
    encoded_route_component, node_batch, required_string, resolve_owner, select_rows, storage,
    with_route_metadata_batch,
};

/// Rows per canonical topology file, matching the staged encoder's window.
pub(super) fn window_rows(budgets: GraphConstructionBudgets, row_bytes: usize) -> usize {
    budgets
        .max_batch_rows
        .min((budgets.max_batch_bytes / row_bytes).max(1))
}

// -------------------------------------------------------- schema groups

/// Rows of one exact schema, ordered by UUID: the staged path's shaped row
/// artifact for that schema.
pub(super) struct SchemaGroup {
    pub(super) batch: RecordBatch,
}

pub(super) fn schema_groups(
    kept: &[RecordBatch],
    uuid_name: &str,
) -> Result<Vec<SchemaGroup>, GfError> {
    let mut by_digest = BTreeMap::<String, Vec<&RecordBatch>>::new();
    for batch in kept {
        by_digest
            .entry(crate::graph_construction::normalized_schema_digest(
                batch.schema().as_ref(),
            ))
            .or_default()
            .push(batch);
    }
    by_digest
        .into_values()
        .map(|batches| {
            let schema = batches[0].schema();
            let merged = concat_batches(&schema, batches.iter().copied()).map_err(storage)?;
            let uuids = crate::graph_construction::batch_uuid_column(&merged, uuid_name)?;
            let mut order =
                (0..u32::try_from(merged.num_rows()).map_err(storage)?).collect::<Vec<_>>();
            order.par_sort_unstable_by_key(|&row| uuids.value(row as usize).to_vec());
            let indices = arrow::array::UInt32Array::from(order);
            let columns = merged
                .columns()
                .iter()
                .map(|column| arrow::compute::take(column.as_ref(), &indices, None))
                .collect::<Result<Vec<_>, _>>()
                .map_err(storage)?;
            Ok(SchemaGroup {
                batch: RecordBatch::try_new(schema, columns).map_err(storage)?,
            })
        })
        .collect()
}

// -------------------------------------------------------------- catalog

pub(super) struct BuiltCatalog {
    pub(super) catalog: RuntimeCatalog,
    pub(super) entity_ids: BTreeMap<String, EntityTypeId>,
}

fn admit(catalog: &RuntimeCatalog, budgets: GraphConstructionBudgets) -> Result<(), GfError> {
    if catalog.entry_count() > budgets.max_catalog_entries
        || catalog.retained_identifier_bytes() > budgets.max_catalog_identifier_bytes
    {
        return Err(storage("runtime catalog admission budget exhausted"));
    }
    Ok(())
}

fn intern_rows(
    catalog: &mut RuntimeCatalog,
    groups: &[SchemaGroup],
    kind: ConstructionChunkKind,
    budgets: GraphConstructionBudgets,
) -> Result<(), GfError> {
    for group in groups {
        let required = match kind {
            ConstructionChunkKind::Node => 2,
            ConstructionChunkKind::Edge => 4,
        };
        intern_batch(catalog, &group.batch, kind, required, budgets)?;
    }
    Ok(())
}

pub(super) fn intern_batch(
    catalog: &mut RuntimeCatalog,
    batch: &RecordBatch,
    kind: ConstructionChunkKind,
    required: usize,
    budgets: GraphConstructionBudgets,
) -> Result<(), GfError> {
    let owner_column = match kind {
        ConstructionChunkKind::Node => "label",
        ConstructionChunkKind::Edge => "rel_type",
    };
    let owners = required_string(batch, owner_column)?;
    let schema = batch.schema();
    for row in 0..batch.num_rows() {
        let owner = owners.value(row);
        match kind {
            ConstructionChunkKind::Node => {
                catalog.intern_label_at(owner, 0)?;
            }
            ConstructionChunkKind::Edge => {
                catalog.intern_relation_type_at(owner, 0)?;
            }
        }
        for (offset, field) in schema.fields()[required..].iter().enumerate() {
            if !batch.column(required + offset).is_null(row) {
                catalog.intern_property_at(field.name(), Some(owner), 0)?;
            }
        }
        admit(catalog, budgets)?;
    }
    Ok(())
}

/// Rows per name: the observation count the staged path's per-row interning leaves.
pub(super) fn observation_counts(values: &[u32], names: usize) -> Vec<u64> {
    values
        .par_chunks(1 << 20)
        .map(|chunk| {
            let mut counts = vec![0_u64; names];
            for value in chunk {
                counts[*value as usize] += 1;
            }
            counts
        })
        .reduce(
            || vec![0_u64; names],
            |mut left, right| {
                for (total, count) in left.iter_mut().zip(right) {
                    *total += count;
                }
                left
            },
        )
}

/// What the catalog needs to know about the edges' relation types: names, the
/// order they first appear in UUID order, and how many edges carry each.
pub(super) struct RelationStats<'a> {
    pub(super) names: &'a [String],
    pub(super) first_appearance: Vec<u32>,
    pub(super) counts: Vec<u64>,
}

impl<'a> RelationStats<'a> {
    /// From the ranked edge table (the in-memory build).
    pub(super) fn from_ranked(edges: &'a EdgeTable) -> Self {
        Self {
            names: &edges.rel_names,
            first_appearance: first_appearance(&edges.rels, edges.rel_names.len()),
            counts: observation_counts(&edges.rels, edges.rel_names.len()),
        }
    }

    /// No statistics: the edge kind carries properties and interns row by row.
    pub(super) fn unused() -> Self {
        Self {
            names: &[],
            first_appearance: Vec::new(),
            counts: Vec::new(),
        }
    }
}

/// Intern in the order the staged path does: every node observation in UUID
/// order (by schema group for property-bearing input), then every edge's.
#[allow(clippy::too_many_arguments)]
pub(super) fn build_catalog(
    budgets: GraphConstructionBudgets,
    nodes: &NodeTable,
    edge_relations: &RelationStats<'_>,
    node_groups: Option<&[SchemaGroup]>,
    edge_groups: Option<&[SchemaGroup]>,
    node_scratch: Option<(
        &super::property_rows::PropertyRows<'_>,
        &[super::property_rows::SortedGroup],
    )>,
    edge_scratch: Option<(
        &super::property_rows::PropertyRows<'_>,
        &[super::property_rows::SortedGroup],
    )>,
    cancel: &AtomicBool,
) -> Result<BuiltCatalog, GfError> {
    let mut catalog = RuntimeCatalog::new();
    if let Some((rows, groups)) = node_scratch {
        for group in groups {
            let mut reader = rows.group_reader(group)?;
            while let Some(batch) = reader.next()? {
                super::tables::check_cancelled(cancel)?;
                intern_batch(
                    &mut catalog,
                    &batch,
                    ConstructionChunkKind::Node,
                    super::property_rows::REQUIRED_COLUMNS,
                    budgets,
                )?;
            }
        }
    } else if let Some(groups) = node_groups {
        intern_rows(&mut catalog, groups, ConstructionChunkKind::Node, budgets)?;
    } else {
        {
            let counts = observation_counts(&nodes.labels, nodes.label_names.len());
            for label in first_appearance(&nodes.labels, nodes.label_names.len()) {
                catalog.intern_label_observed_at(
                    &nodes.label_names[label as usize],
                    0,
                    counts[label as usize],
                )?;
                admit(&catalog, budgets)?;
            }
        }
    }
    if let Some((rows, groups)) = edge_scratch {
        for group in groups {
            let mut reader = rows.group_reader(group)?;
            while let Some(batch) = reader.next()? {
                super::tables::check_cancelled(cancel)?;
                intern_batch(
                    &mut catalog,
                    &batch,
                    ConstructionChunkKind::Edge,
                    super::property_rows::REQUIRED_COLUMNS,
                    budgets,
                )?;
            }
        }
    } else if let Some(groups) = edge_groups {
        intern_rows(&mut catalog, groups, ConstructionChunkKind::Edge, budgets)?;
    } else {
        for rel in &edge_relations.first_appearance {
            catalog.intern_relation_type_observed_at(
                &edge_relations.names[*rel as usize],
                0,
                edge_relations.counts[*rel as usize],
            )?;
            admit(&catalog, budgets)?;
        }
    }
    let entity_ids = catalog
        .entity_type_names_with_ids()
        .map(|(id, name)| (name.to_owned(), EntityTypeId::runtime(id)))
        .collect();
    Ok(BuiltCatalog {
        catalog,
        entity_ids,
    })
}

// ---------------------------------------------------------------- nodes

pub(super) struct Semantics<'a> {
    pub(super) mode: OntologyMode,
    pub(super) context: Option<&'a CompositionBindingContext>,
    pub(super) bindings: Option<&'a SemanticStorageBindings>,
}

pub(super) fn node_types(
    names: &[String],
    ids: &BTreeMap<String, EntityTypeId>,
    semantics: &Semantics<'_>,
) -> Result<Vec<EntityTypeId>, GfError> {
    names
        .iter()
        .map(|label| {
            let runtime_route = if semantics.mode == OntologyMode::Exploratory {
                "_untyped"
            } else {
                label.as_str()
            };
            let owner = resolve_owner(
                semantics.context,
                semantics.bindings,
                SymbolKind::Entity,
                SemanticRouteKind::Entity,
                label,
                runtime_route,
            )?;
            match owner.storage_id {
                Some(storage_id) => EntityTypeId::decode(storage_id.encode()).map_err(storage),
                None => ids
                    .get(label)
                    .copied()
                    .ok_or_else(|| storage("node label is absent from runtime catalog")),
            }
        })
        .collect()
}

pub(super) fn emit_nodes(
    installer: &Installer<'_>,
    nodes: &NodeTable,
    types: &[EntityTypeId],
    window: usize,
    now: i64,
    cancel: &AtomicBool,
) -> Result<(), GfError> {
    let windows = nodes.uuids.len().div_ceil(window);
    (0..windows).into_par_iter().try_for_each(|index| {
        super::tables::check_cancelled(cancel)?;
        let start = index * window;
        let end = (start + window).min(nodes.uuids.len());
        let ids = (start as u64 + 1..=end as u64).collect::<Vec<_>>();
        let row_types = nodes.labels[start..end]
            .iter()
            .map(|label| types[*label as usize])
            .collect::<Vec<_>>();
        let batch = node_batch(&nodes.uuids[start..end], &ids, &row_types, now)?;
        let path = format!(
            "topology/nodes/{:020}-{:020}.parquet",
            ids[0],
            ids[ids.len() - 1]
        );
        installer.install_parquet(&path, &batch)
    })
}

// ---------------------------------------------------------------- edges

/// Where one relation's edges are stored and indexed.
#[derive(Clone)]
pub(super) struct RelationRoute {
    pub(super) logical: String,
    pub(super) topology_route: String,
    pub(super) qualified: bool,
    pub(super) exploratory: bool,
}

impl RelationRoute {
    fn key(&self) -> (String, bool, bool) {
        (
            self.topology_route.clone(),
            self.qualified,
            self.exploratory,
        )
    }

    /// The adjacency group the relation's edges belong to.
    pub(super) fn adjacency_group(&self) -> &str {
        if self.exploratory {
            &self.logical
        } else {
            &self.topology_route
        }
    }
}

pub(super) fn relation_routes(
    names: &[String],
    semantics: &Semantics<'_>,
) -> Result<Vec<RelationRoute>, GfError> {
    names
        .iter()
        .map(|route| {
            let runtime_route = if semantics.mode == OntologyMode::Exploratory {
                "_exploratory"
            } else {
                route.as_str()
            };
            let owner = resolve_owner(
                semantics.context,
                semantics.bindings,
                SymbolKind::Relation,
                SemanticRouteKind::Relation,
                route,
                runtime_route,
            )?;
            let exploratory = owner.symbol.is_none() && semantics.mode == OntologyMode::Exploratory;
            Ok(RelationRoute {
                logical: route.clone(),
                topology_route: if exploratory {
                    "_exploratory".to_owned()
                } else {
                    owner.topology_route
                },
                qualified: owner.symbol.is_some(),
                exploratory,
            })
        })
        .collect()
}

/// A run of consecutive ranked edges that one canonical edge file holds.
pub(super) struct EdgeWindow<'a> {
    /// `edge_id` of the first edge.
    pub(super) first_id: u64,
    pub(super) uuids: &'a [[u8; 16]],
    pub(super) src: &'a [u32],
    pub(super) dst: &'a [u32],
    pub(super) rels: &'a [u32],
}

/// Everything an edge window needs besides its edges.
pub(super) struct EdgeEmitter<'a> {
    pub(super) installer: &'a Installer<'a>,
    pub(super) nodes: &'a NodeTable,
    pub(super) relations: &'a [RelationRoute],
    pub(super) components: &'a BTreeMap<String, String>,
    pub(super) semantics: &'a Semantics<'a>,
    pub(super) now: i64,
}

pub(super) fn emit_edges(
    emitter: &EdgeEmitter<'_>,
    edges: &EdgeTable,
    window: usize,
    cancel: &AtomicBool,
) -> Result<(), GfError> {
    let windows = edges.uuids.len().div_ceil(window);
    (0..windows).into_par_iter().try_for_each(|index| {
        super::tables::check_cancelled(cancel)?;
        let start = index * window;
        let end = (start + window).min(edges.uuids.len());
        emitter.emit_window(&EdgeWindow {
            first_id: start as u64 + 1,
            uuids: &edges.uuids[start..end],
            src: &edges.src[start..end],
            dst: &edges.dst[start..end],
            rels: &edges.rels[start..end],
        })
    })
}

impl EdgeEmitter<'_> {
    /// Encode one window into its canonical file or files, one per route.
    #[allow(clippy::too_many_lines)]
    pub(super) fn emit_window(&self, window: &EdgeWindow<'_>) -> Result<(), GfError> {
        let Self {
            installer,
            nodes,
            relations,
            components,
            semantics,
            now,
        } = *self;
        let count = window.uuids.len() as u64;
        let ids = (window.first_id..window.first_id + count).collect::<Vec<_>>();
        let src_ids = window
            .src
            .iter()
            .map(|rank| u64::from(*rank))
            .collect::<Vec<_>>();
        let dst_ids = window
            .dst
            .iter()
            .map(|rank| u64::from(*rank))
            .collect::<Vec<_>>();
        let src_uuids = window
            .src
            .iter()
            .map(|rank| nodes.uuids[*rank as usize - 1])
            .collect::<Vec<_>>();
        let dst_uuids = window
            .dst
            .iter()
            .map(|rank| nodes.uuids[*rank as usize - 1])
            .collect::<Vec<_>>();
        let canonical = edge_batch(
            window.uuids,
            &src_uuids,
            &dst_uuids,
            &ids,
            &src_ids,
            &dst_ids,
            now,
        )?;
        let rels = window.rels;
        let mut groups = BTreeMap::<(String, bool, bool), Vec<u32>>::new();
        let uniform = rels.iter().all(|rel| *rel == rels[0]);
        if uniform {
            groups.insert(relations[rels[0] as usize].key(), Vec::new());
        } else {
            for (row, rel) in rels.iter().enumerate() {
                groups
                    .entry(relations[*rel as usize].key())
                    .or_default()
                    .push(u32::try_from(row).map_err(storage)?);
            }
        }
        for ((topology_route, qualified, exploratory), rows) in groups {
            let mut selected = if uniform {
                canonical.clone()
            } else {
                select_rows(&canonical, &rows)?
            };
            if exploratory {
                let logical = if uniform {
                    StringArray::from_iter_values(
                        std::iter::repeat_n(&relations[rels[0] as usize].logical, rels.len())
                            .map(String::as_str),
                    )
                } else {
                    StringArray::from_iter_values(
                        rows.iter()
                            .map(|row| relations[rels[*row as usize] as usize].logical.as_str()),
                    )
                };
                let mut columns = selected.columns().to_vec();
                columns.push(Arc::new(logical));
                selected =
                    RecordBatch::try_new(crate::schemas::EXPLORATORY_EDGE_SCHEMA.clone(), columns)
                        .map_err(storage)?;
            }
            if qualified {
                selected = with_route_metadata_batch(
                    &selected,
                    &topology_route,
                    semantics
                        .context
                        .expect("qualified owner has context")
                        .fingerprint(),
                )?;
            }
            let edge_ids = selected
                .column_by_name("edge_id")
                .and_then(|array| array.as_any().downcast_ref::<UInt64Array>())
                .ok_or_else(|| storage("canonical edge ids are incompatible"))?;
            let path = format!(
                "topology/edges/{}/{:020}-{:020}.parquet",
                components
                    .get(&topology_route)
                    .ok_or_else(|| storage("edge route was not registered"))?,
                edge_ids.value(0),
                edge_ids.value(edge_ids.len() - 1)
            );
            installer.install_parquet(&path, &selected)?;
        }
        Ok(())
    }
}

/// The distinct physical routes edges are stored under, as route components.
pub(super) fn register_routes(
    routes: &mut crate::route_component::RouteTable,
    relations: &[RelationRoute],
) -> Result<BTreeMap<String, String>, GfError> {
    let mut components = BTreeMap::new();
    for topology_route in relations
        .iter()
        .map(|relation| relation.topology_route.as_str())
        .collect::<BTreeSet<_>>()
    {
        components.insert(
            topology_route.to_owned(),
            encoded_route_component(routes, topology_route)?,
        );
    }
    Ok(components)
}
