//! Bulk builder for initial generations (#1883; passes 0-3 of #1881).
//!
//! An initial build's published graph is a projection of three things: the
//! node UUIDs in sorted order (`node_id` is a node's rank), the edge UUIDs in
//! sorted order (`edge_id` is an edge's rank), and each edge's endpoint ranks.
//! The builder computes those once, in memory, with dense `u32` ids, and
//! emits every canonical artifact straight from them. It produces exactly the
//! artifact inventory [`encode`] produces from a shaped session, so the
//! publisher cannot tell the two apart.
//!
//! - Pass 0 plans from the source footers: row counts and tasks.
//! - Pass 1 decodes nodes in parallel, validates them, orders them and ranks
//!   them. Already-sorted input skips the sort.
//! - Pass 2 decodes edges in parallel, resolves endpoints through the node
//!   index, orders and ranks edges, and rejects identity collisions.
//! - Pass 3 emits catalog, node and edge tables, property overlays, the
//!   ordinal node-identity facet, and the adjacency CSR.
//!
//! When the arrays do not fit the memory budget the same passes run through
//! scratch files: edges (`scratch_edges`, #1900) and, when the node tables do
//! not fit either, nodes as well (`scratch_nodes`, #1929), whose endpoint
//! pass resolves edge endpoints per node-UUID range partition.
//!
//! Intermediates are never synced or hashed. A crash discards them and the
//! build reruns from the sources (ADR 0038 as amended for initial builds).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::{
    Component, CompositionBindingContext, ConstructionChunkKind, ConstructionEncodedArtifact,
    ConstructionSemanticAuthority, ConstructionShape, Digest, ENCODED_ROOT,
    ENCODING_FORMAT_VERSION, ENCODING_INTENT, EncodingIntent, EntityTypeId, FixedSizeBinaryArray,
    GfError, GraphConstructionBudgets, GraphConstructionEncoding,
    GraphConstructionEncodingEvidence, GraphConstructionEncodingInvocationEvidence, INVENTORY,
    OntologyMode, OsStr, Path, RecordBatch, SemanticRouteKind, SemanticStorageBindings, Sha256,
    StableDirectory, StringArray, SymbolKind, UInt64Array, Uuid, Write, adjacency,
    authenticate_inventory_control, copy_artifact, edge_batch, edge_property_batch,
    encoded_route_component, hex, index_artifact, install_json, lanes, node_batch,
    node_property_batch, remove_encoding_intent, required_string, resolve_owner, select_rows,
    storage, with_route_metadata_batch, write_surrogate_tails,
};

mod budget;
mod csr;
mod emit;
mod install;
mod ordered;
mod plan;
mod property_emit;
mod property_rows;
mod scratch;
mod scratch_csr;
mod scratch_edges;
mod scratch_nodes;
mod scratch_ranges;
mod tables;

#[cfg(test)]
pub(crate) use budget::ForcedPartitions;
pub use budget::{BulkRoute, BulkStagedReason};
pub use plan::{BulkBatchReader, BulkBuildPlan, BulkBuildReport, BulkPassReport, BulkSource};
#[cfg(test)]
pub(crate) use property_rows::ForcedPropertyFrames;

use budget::ScratchPlan;
use emit::{EdgeEmitter, RelationStats, Semantics};
use install::Installer;
use plan::PassMeter;
use scratch::Scratch;
pub(crate) use scratch::discard_scratch;
use scratch_csr::CsrScratch;
use scratch_edges::{Endpoints, RankedNodes};
use tables::{EdgeTable, NodeIndex, NodeTable, check_cancelled};

/// Run `work` on `pool` while the calling thread polls `cancelled`.
fn run_pass<T: Send>(
    pool: &rayon::ThreadPool,
    cancelled: &mut impl FnMut() -> bool,
    cancel: &AtomicBool,
    work: impl FnOnce() -> Result<T, GfError> + Send,
) -> Result<T, GfError> {
    let result = std::thread::scope(|scope| {
        let handle = scope.spawn(|| pool.install(work));
        while !handle.is_finished() {
            if cancelled() {
                cancel.store(true, Ordering::Release);
            }
            std::thread::sleep(Duration::from_millis(2));
        }
        handle
            .join()
            .map_err(|_| storage("bulk builder worker panicked"))?
    });
    if cancelled() {
        return Err(storage("construction encoding cancelled"));
    }
    result
}

fn wipe_encoded_root(output: &StableDirectory) -> Result<(), GfError> {
    for name in output.child_names().map_err(storage)? {
        let path = output.path().join(&name);
        let metadata = std::fs::symlink_metadata(&path).map_err(storage)?;
        if metadata.is_dir() {
            if let Some(allocation) = output.allocation() {
                allocation.remove_owned_tree(&path)?;
            }
            if path.exists() {
                std::fs::remove_dir_all(&path).map_err(storage)?;
            }
        } else {
            if let Some(allocation) = output.allocation() {
                allocation.remove_file_at(&path)?;
            }
            std::fs::remove_file(&path).map_err(storage)?;
        }
    }
    output.acknowledge().map_err(storage)
}

/// The ranked edges: resident columns, or scattered partitions on scratch.
enum EdgeSide {
    Memory(EdgeTable),
    Scratch(scratch_edges::ScatteredEdges),
}

impl EdgeSide {
    fn count(&self) -> u64 {
        match self {
            Self::Memory(edges) => edges.uuids.len() as u64,
            Self::Scratch(scattered) => scattered.total,
        }
    }

    fn rel_names(&self) -> &[String] {
        match self {
            Self::Memory(edges) => &edges.rel_names,
            Self::Scratch(scattered) => &scattered.rel_names,
        }
    }
}

/// The ranked nodes: resident tables, or runs on scratch.
#[allow(clippy::large_enum_variant)] // one value per build; boxing buys nothing
enum NodeSide {
    Memory(NodeTable),
    Scratch(ScratchNodes),
}

/// Nodes on scratch: scattered by the node pass, then ranked and resolved by
/// the endpoint pass.
struct ScratchNodes {
    scattered: scratch_nodes::ScatteredNodes,
    resolved: Option<scratch_nodes::ResolvedNodes>,
}

impl NodeSide {
    fn count(&self) -> u64 {
        match self {
            Self::Memory(nodes) => nodes.uuids.len() as u64,
            Self::Scratch(nodes) => nodes.scattered.total,
        }
    }

    fn table(&self) -> Option<&NodeTable> {
        match self {
            Self::Memory(nodes) => Some(nodes),
            Self::Scratch(_) => None,
        }
    }

    fn labels(&self) -> Result<emit::NodeLabels<'_>, GfError> {
        match self {
            Self::Memory(nodes) => Ok(emit::NodeLabels::Table(nodes)),
            Self::Scratch(ScratchNodes {
                resolved: Some(resolved),
                ..
            }) => Ok(emit::NodeLabels::Stats(&resolved.labels)),
            Self::Scratch(_) => Err(storage(
                "the node labels were requested before the nodes ranked",
            )),
        }
    }
}

/// What a scratch build reports about its scratch files.
#[derive(Default)]
struct ScratchReport {
    node_partitions: u64,
    largest_node_partition: u64,
    node_refinement_steps: u64,
    node_refinement_write_bytes: u64,
    node_refinement_read_bytes: u64,
    node_write_bytes: u64,
    node_read_bytes: u64,
    endpoint_write_bytes: u64,
    endpoint_read_bytes: u64,
    concurrency: u64,
    edge_partitions: u64,
    csr_partitions: u64,
    write_bytes: u64,
    read_bytes: u64,
    /// The largest number of bytes scratch files occupied at once: files leave
    /// the count as their final reads reclaim them, so this is the scratch the
    /// build really held, not the bytes it moved.
    peak_occupied_bytes: u64,
    largest_partition: u64,
    refinement_steps: u64,
    refinement_write_bytes: u64,
    refinement_read_bytes: u64,
    csr_spool_write_bytes: u64,
    csr_spool_read_bytes: u64,
    peak_csr_carry_entries: u64,
}

/// The ordinal node-identity facet a build publishes.
struct OrdinalFacet {
    artifacts: Vec<crate::uuid_membership::ConstructionIndexOutput>,
    publication: crate::uuid_membership::V4OrdinalPublicationMetrics,
    metrics: crate::uuid_membership::V4OrdinalBuildMetrics,
}

/// How the ordinal facet reads the nodes.
enum OrdinalNodes<'a> {
    /// The resident, sorted node table.
    Memory(&'a NodeTable),
    /// Node runs on scratch: one sweep over them writes the canonical node
    /// files and feeds the ordinal pushes, so every scratch block is read once.
    Scratch {
        sweep: scratch_nodes::NodeSweep<'a>,
        pool: &'a rayon::ThreadPool,
        count: u64,
    },
}

/// The v4 ordinal artifacts, streamed from the ranked node array.
fn build_ordinal_facet(
    output: &StableDirectory,
    nodes: &OrdinalNodes<'_>,
    generation: u64,
    cancel: &AtomicBool,
) -> Result<OrdinalFacet, GfError> {
    let mut cancelled = || cancel.load(Ordering::Acquire);
    crate::uuid_membership::clear_private_ordinal_residue(output)?;
    let node_count = match nodes {
        OrdinalNodes::Memory(table) => table.uuids.len() as u64,
        OrdinalNodes::Scratch { count, .. } => *count,
    };
    let membership_dir = output
        .create_child_directory(OsStr::new("graph"))
        .map_err(storage)?
        .create_child_directory(OsStr::new("topology"))
        .map_err(storage)?
        .create_child_directory(OsStr::new("uuid-membership"))
        .map_err(storage)?;
    let cache_window =
        graphforge_filesystem::cache_release_window_for_streams(5).map_err(storage)?;
    let mut writer = crate::uuid_membership::V4OrdinalConstructionWriter::start_with_cache_window(
        generation,
        membership_dir.physical(),
        cache_window,
        membership_dir.allocation(),
    )?;
    match nodes {
        OrdinalNodes::Memory(table) => {
            for (position, uuid) in table.uuids.iter().enumerate() {
                writer.push_pair(Uuid::from_bytes(*uuid), position as u64 + 1, &mut cancelled)?;
            }
        }
        OrdinalNodes::Scratch { sweep, pool, .. } => {
            let seen = pool.install(|| {
                scratch_nodes::sweep_nodes(sweep, |uuid, rank| {
                    writer.push_pair(Uuid::from_bytes(*uuid), rank, &mut cancelled)
                })
            })?;
            if seen != node_count {
                return Err(storage("the node runs lost nodes"));
            }
        }
    }
    let bundle = writer.finish()?;
    let (artifacts, publication, metrics) =
        crate::uuid_membership::publish_v4_construction_artifacts(
            output.physical(),
            bundle,
            generation,
            None,
            &mut cancelled,
            output.allocation(),
        )?;
    Ok(OrdinalFacet {
        artifacts,
        publication,
        metrics,
    })
}

/// Build the complete encoded inventory of an initial generation.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(crate) fn encode_bulk(
    source: &StableDirectory,
    shape: &ConstructionShape,
    generation: u64,
    ontology_mode: OntologyMode,
    semantic_authority: Option<&ConstructionSemanticAuthority>,
    shape_authority_sha256: &str,
    budgets: GraphConstructionBudgets,
    admission: Option<&Arc<crate::graph_construction::cpu_admission::ConstructionCpuAdmission>>,
    plan: &BulkBuildPlan<'_>,
    report: &Mutex<BulkBuildReport>,
    cancelled: &mut impl FnMut() -> bool,
) -> Result<GraphConstructionEncoding, GfError> {
    let _diagnostic_scope =
        crate::graph_construction::diagnostics::Scope::start("canonical_encoding");
    if shape.parent_topology_generation != 0 || generation != 1 {
        return Err(storage("the bulk builder only builds initial generations"));
    }
    if shape.ontology_mode != ontology_mode {
        return Err(storage(
            "shape ontology mode differs from session authority",
        ));
    }
    let semantic_digest = semantic_authority
        .map(ConstructionSemanticAuthority::digest)
        .transpose()?;
    if shape.semantic_authority_sha256 != semantic_digest {
        return Err(storage("shape semantic authority differs from session"));
    }
    // Restart, not resume: whatever an earlier attempt left is scratch.
    let output = source
        .create_child_directory(OsStr::new(ENCODED_ROOT))
        .map_err(storage)?;
    wipe_encoded_root(&output)?;
    let intent = EncodingIntent {
        generation,
        parent_generation: 0,
        ontology_mode,
        semantic_authority_sha256: shape.semantic_authority_sha256.clone(),
        shape_inputs_sha256: shape.runtime_catalog_inputs_sha256.clone(),
        shape_authority_sha256: shape_authority_sha256.to_owned(),
    };
    install_json(&output, ENCODING_INTENT, &intent)?;

    let lease = admission.and_then(|admission| {
        admission
            .acquire(
                std::num::NonZeroUsize::new(admission.limit()).expect("positive admission"),
                &mut || false,
            )
            .ok()
    });
    let workers = lease.as_ref().map_or_else(
        || std::thread::available_parallelism().map_or(1, usize::from),
        |lease| lease.lanes().get(),
    );
    // The route is a function of the footers and the budget (ADR 0058). A
    // A durable bulk route cannot switch to staging after a budget drop.
    // Refuse that attempt before loading data; it can retry when memory returns.
    let route = plan.route();
    let scratch_plan = match (route, plan.memory_budget) {
        (BulkRoute::Scratch | BulkRoute::ScratchNodes, Some(budget)) => {
            // The route sees only the node tables; the property workspace can
            // also tip them onto scratch.
            let minimum = if budget::node_tables_on_scratch(plan, budget, budgets) {
                plan.scratch_floor_bytes()
            } else {
                plan.node_tables_resident_bytes()
            }
            .saturating_add(budget::property_extra_if_any(plan, budgets));
            if budget < minimum {
                return Err(GfError::Project {
                    code: graphforge_core::ProjectErrorCode::ResourceLimit,
                    message: format!(
                        "graph construction encoding: scratch requires {minimum} resident bytes before decoding; budget is {budget}"
                    ),
                });
            }
            Some(ScratchPlan::derive_with_budgets(
                plan, budget, workers, budgets,
            ))
        }
        (BulkRoute::Staged(reason), _) => {
            return Err(GfError::Project {
                code: graphforge_core::ProjectErrorCode::ResourceLimit,
                message: format!(
                    "graph construction encoding: the fixed bulk route cannot fit the current \
                     memory budget ({reason:?}); retry with an adequate budget"
                ),
            });
        }
        _ => None,
    };
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(
            scratch_plan
                .as_ref()
                .map_or(workers, |sized| sized.concurrency),
        )
        .thread_name(|index| format!("gf-bulk-{index}"))
        .build()
        .map_err(storage)?;
    let cancel = AtomicBool::new(false);
    let mut passes = std::collections::BTreeMap::new();
    let mut evidence = GraphConstructionEncodingEvidence {
        peak_open_writers: 1,
        ..Default::default()
    };

    // Pass 0: the plan was fixed from the footers; validate its totals.
    let meter = PassMeter::start("plan");
    let node_rows: u64 = plan.nodes.iter().map(|source| source.rows).sum();
    let edge_rows: u64 = plan.edges.iter().map(|source| source.rows).sum();
    tables::require_dense(node_rows, "nodes")?;
    tables::require_dense(edge_rows, "edges")?;
    let retain_nodes = plan.nodes.iter().any(|source| !source.property_free);
    let retain_edges = plan.edges.iter().any(|source| !source.property_free);
    passes.extend([meter.finish()]);

    let scratch = scratch_plan
        .as_ref()
        .map(|_| Scratch::create(source))
        .transpose()?;
    let node_properties = scratch.as_ref().filter(|_| retain_nodes).map(|scratch| {
        property_rows::PropertyRows::new(
            scratch,
            ConstructionChunkKind::Node,
            budgets,
            plan.max_source_schema_bytes(),
        )
    });
    let edge_properties = scratch.as_ref().filter(|_| retain_edges).map(|scratch| {
        property_rows::PropertyRows::new(
            scratch,
            ConstructionChunkKind::Edge,
            budgets,
            plan.max_source_schema_bytes(),
        )
    });

    // Pass 1: nodes. When the node tables do not fit the budget they scatter
    // into node-UUID range partitions on scratch instead (#1929).
    let meter = PassMeter::start("nodes");
    let mut nodes = match (&scratch_plan, &scratch) {
        (Some(sized), Some(scratch)) if sized.node_tables_on_scratch => {
            NodeSide::Scratch(ScratchNodes {
                scattered: run_pass(&pool, cancelled, &cancel, || {
                    scratch_nodes::scatter_nodes(
                        &plan.nodes,
                        budgets,
                        node_properties.as_ref(),
                        sized,
                        scratch,
                        &cancel,
                    )
                })?,
                resolved: None,
            })
        }
        _ => NodeSide::Memory(run_pass(&pool, cancelled, &cancel, || {
            tables::collect_nodes(
                &plan.nodes,
                retain_nodes && node_properties.is_none(),
                node_properties.as_ref(),
                budgets,
                &cancel,
            )
        })?),
    };
    let node_count = nodes.count();
    // What the node scatter and its refinement moved; the endpoint pass adds
    // its own reads below.
    let node_scatter_traffic = scratch.as_ref().map_or((0, 0), |scratch| {
        (scratch.written_bytes(), scratch.read_bytes())
    });
    let mut node_leaf_read = 0_u64;
    passes.extend([meter.finish()]);
    crate::graph_construction::construction_failpoint("bulk.after_nodes");

    // Pass 2: edges and endpoint resolution. Over budget, the edges scatter
    // into scratch partitions instead of landing in resident columns.
    let meter = PassMeter::start("edges");
    // Endpoint references and the identity probes of the edges, and the CSR
    // key partitions the endpoint pass derives from the exact degrees, when
    // the nodes are on scratch.
    let mut refs = None::<scratch::Partitions>;
    let mut identity_probes = None::<scratch::Partitions>;
    let mut node_keys = None::<(scratch_csr::KeyPartitioner, scratch_csr::KeyPartitioner)>;
    let mut edge_side = match (&scratch_plan, &scratch, &nodes) {
        (Some(sized), Some(scratch), NodeSide::Scratch(on_scratch)) => {
            let leaves = scratch::Partitions::create(
                scratch,
                "refs",
                on_scratch.scattered.leaves.len(),
                scratch_nodes::REF_RECORD,
            )?;
            let probes = scratch::Partitions::create(
                scratch,
                "probes",
                on_scratch.scattered.leaves.len(),
                scratch_nodes::PROBE_RECORD,
            )?;
            let sink = scratch_nodes::RefSink {
                router: &on_scratch.scattered.router,
                refs: &leaves,
                probes: &probes,
            };
            let scattered = run_pass(&pool, cancelled, &cancel, || {
                scratch_edges::scatter_edges(
                    &plan.edges,
                    budgets,
                    edge_properties.as_ref(),
                    &Endpoints::Deferred(&sink),
                    sized,
                    scratch,
                    &cancel,
                )
            })?;
            refs = Some(leaves);
            identity_probes = Some(probes);
            EdgeSide::Scratch(scattered)
        }
        (Some(sized), Some(scratch), NodeSide::Memory(table)) => {
            let index = pool.install(|| NodeIndex::build(&table.uuids));
            EdgeSide::Scratch(run_pass(&pool, cancelled, &cancel, || {
                scratch_edges::scatter_edges(
                    &plan.edges,
                    budgets,
                    edge_properties.as_ref(),
                    &Endpoints::Resident {
                        nodes: table,
                        index: &index,
                    },
                    sized,
                    scratch,
                    &cancel,
                )
            })?)
        }
        (_, _, NodeSide::Memory(table)) => {
            let index = pool.install(|| NodeIndex::build(&table.uuids));
            EdgeSide::Memory(run_pass(&pool, cancelled, &cancel, || {
                tables::collect_edges(&plan.edges, retain_edges, budgets, table, &index, &cancel)
            })?)
        }
        _ => return Err(storage("the over-budget build lost its scratch state")),
    };
    let edge_count = edge_side.count();
    if node_count == 0 && edge_count == 0 {
        return Err(storage("construction contains no identities"));
    }
    passes.extend([meter.finish()]);
    crate::graph_construction::construction_failpoint("bulk.after_edges");

    // Endpoints over scratch node tables: rank every node leaf, resolve the
    // endpoint references routed to it, and learn the exact degrees.
    if let (
        NodeSide::Scratch(on_scratch),
        EdgeSide::Scratch(scattered),
        Some(refs),
        Some(probes),
        Some(sized),
        Some(scratch),
    ) = (
        &mut nodes,
        &edge_side,
        &refs,
        &identity_probes,
        &scratch_plan,
        &scratch,
    ) {
        let meter = PassMeter::start("endpoints");
        let read_before = scratch.read_bytes();
        let (resolved, keys) = run_pass(&pool, cancelled, &cancel, || {
            scratch_nodes::resolve_endpoints(&scratch_nodes::ResolveContext {
                scratch,
                plan: sized,
                nodes: &on_scratch.scattered,
                refs,
                probes,
                edges: scattered,
                cancel: &cancel,
            })
        })?;
        if resolved.collision || resolved.miss.is_some() {
            return Err(scattered.refusal(
                scratch,
                sized,
                None,
                resolved.collision,
                resolved.miss,
                &cancel,
            ));
        }
        // Reads of the endpoint pass that were neither references nor probes:
        // the node leaves.
        node_leaf_read =
            scratch.read_bytes() - read_before - refs.read_bytes() - probes.read_bytes();
        on_scratch.resolved = Some(resolved);
        node_keys = Some(keys);
        passes.extend([meter.finish()]);
        crate::graph_construction::construction_failpoint("bulk.after_endpoints");
    }

    // The catalog's schema groups, routes and windows.
    let meter = PassMeter::start("catalog");
    let semantic_context = semantic_authority
        .map(ConstructionSemanticAuthority::context)
        .transpose()?;
    let semantics = Semantics {
        mode: ontology_mode,
        context: semantic_context.as_ref(),
        bindings: semantic_authority.map(|authority| &authority.bindings),
    };
    // The decoded batches are released as soon as their sorted schema groups exist.
    let node_kept = match &mut nodes {
        NodeSide::Memory(table) => std::mem::take(&mut table.kept),
        NodeSide::Scratch(_) => Vec::new(),
    };
    let edge_kept = match &mut edge_side {
        EdgeSide::Memory(edges) => std::mem::take(&mut edges.kept),
        EdgeSide::Scratch(_) => Vec::new(),
    };
    let (node_groups, edge_groups) = run_pass(&pool, cancelled, &cancel, || {
        Ok((
            (retain_nodes && node_properties.is_none())
                .then(|| emit::schema_groups(&node_kept, "node_uuid"))
                .transpose()?,
            (retain_edges && edge_properties.is_none())
                .then(|| emit::schema_groups(&edge_kept, "edge_uuid"))
                .transpose()?,
        ))
    })?;
    drop((node_kept, edge_kept));
    let (node_scratch_groups, edge_scratch_groups) = run_pass(&pool, cancelled, &cancel, || {
        Ok((
            node_properties
                .as_ref()
                .map(|rows| rows.finish(&cancel))
                .transpose()?,
            edge_properties
                .as_ref()
                .map(|rows| rows.finish(&cancel))
                .transpose()?,
        ))
    })?;

    // The staged path admits at most `max_schema_groups` exact schemas across
    // both kinds; a property-free kind is the one bare schema.
    let schema_groups = node_scratch_groups.as_ref().map_or_else(
        || {
            node_groups
                .as_ref()
                .map_or(usize::from(node_count != 0), Vec::len)
        },
        Vec::len,
    ) + edge_scratch_groups.as_ref().map_or_else(
        || {
            edge_groups
                .as_ref()
                .map_or(usize::from(edge_count != 0), Vec::len)
        },
        Vec::len,
    );
    if schema_groups > budgets.max_schema_groups {
        return Err(storage("construction schema-group budget exhausted"));
    }
    let relations = emit::relation_routes(edge_side.rel_names(), &semantics)?;
    let mut routes = crate::route_component::RouteTable::default();
    let components = emit::register_routes(&mut routes, &relations)?;
    passes.extend([meter.finish()]);

    let installer = Installer::new(&output);
    let node_window = emit::window_rows(budgets, 128);
    let edge_window = emit::window_rows(budgets, 192);
    let now = shape.runtime_catalog_now_micros;
    let emitter = EdgeEmitter {
        installer: &installer,
        nodes: nodes.table(),
        relations: &relations,
        components: &components,
        semantics: &semantics,
        now,
    };

    // Pass 3 over budget: rank each edge partition, write its canonical edge
    // files, and stage its adjacency entries.
    let ranked_edges = match (&edge_side, &scratch_plan, &scratch) {
        (EdgeSide::Scratch(scattered), Some(sized), Some(scratch)) => {
            let meter = PassMeter::start("ranks");
            let (csr, ranked_nodes) = match &nodes {
                NodeSide::Memory(table) => (
                    CsrScratch::create(
                        scratch,
                        scattered
                            .histogram
                            .as_ref()
                            .ok_or_else(|| storage("the edge pass kept no degrees"))?,
                        sized.csr_partitions,
                    )?,
                    RankedNodes::Resident(table),
                ),
                NodeSide::Scratch(ScratchNodes {
                    resolved: Some(resolved),
                    ..
                }) => {
                    let (out_keys, in_keys) = node_keys
                        .take()
                        .ok_or_else(|| storage("the node pass kept no key partitions"))?;
                    (
                        CsrScratch::with_partitioners(scratch, out_keys, in_keys)?,
                        RankedNodes::Resolved(&resolved.resolved),
                    )
                }
                NodeSide::Scratch(_) => {
                    return Err(storage("the over-budget build lost its node pass"));
                }
            };
            let ranked = run_pass(&pool, cancelled, &cancel, || {
                scratch_edges::rank_partitions(&scratch_edges::RankContext {
                    scratch,
                    scattered,
                    csr: &csr,
                    nodes: ranked_nodes,
                    emitter: &emitter,
                    plan: sized,
                    window: edge_window,
                    cancel: &cancel,
                })
            })?;
            passes.extend([meter.finish()]);
            crate::graph_construction::construction_failpoint("bulk.after_ranks");
            Some((csr, ranked))
        }
        _ => None,
    };

    let meter = PassMeter::start("tables");
    let relation_stats = match (&edge_side, &ranked_edges) {
        (EdgeSide::Memory(edges), _) if edge_groups.is_none() => RelationStats::from_ranked(edges),
        (EdgeSide::Scratch(scattered), Some((_, ranked))) => RelationStats {
            names: &scattered.rel_names,
            first_appearance: ranked.first_appearance.clone(),
            counts: ranked.counts.clone(),
        },
        _ => RelationStats::unused(),
    };
    let node_labels = nodes.labels()?;
    let built = run_pass(&pool, cancelled, &cancel, || {
        emit::build_catalog(
            budgets,
            node_labels,
            &relation_stats,
            node_groups.as_deref(),
            edge_groups.as_deref(),
            node_properties.as_ref().zip(node_scratch_groups.as_deref()),
            edge_properties.as_ref().zip(edge_scratch_groups.as_deref()),
            &cancel,
        )
    })?;
    drop(relation_stats);
    let types = emit::node_types(node_labels.names(), &built.entity_ids, &semantics)?;
    run_pass(&pool, cancelled, &cancel, || {
        installer.install_parquet(
            "topology/runtime_catalog.parquet",
            &built.catalog.to_record_batch(),
        )?;
        // Nodes on scratch stream to their files in the ordinal sweep below.
        if let NodeSide::Memory(table) = &nodes {
            emit::emit_nodes(&installer, table, &types, node_window, now, &cancel)?;
        }
        if let EdgeSide::Memory(edges) = &edge_side {
            emit::emit_edges(&emitter, edges, edge_window, &cancel)?;
        }
        Ok(())
    })?;
    passes.extend([meter.finish()]);
    crate::graph_construction::construction_failpoint("bulk.after_tables");

    // The ordinal facet streams the sorted node UUIDs; the edge UUIDs (16 B per
    // edge) are released before the adjacency pass sorts its entries.
    let meter = PassMeter::start("ordinal");
    let ordinal_nodes = match &nodes {
        NodeSide::Memory(table) => OrdinalNodes::Memory(table),
        NodeSide::Scratch(ScratchNodes {
            resolved: Some(resolved),
            ..
        }) => OrdinalNodes::Scratch {
            sweep: scratch_nodes::NodeSweep {
                scratch: scratch
                    .as_ref()
                    .ok_or_else(|| storage("the over-budget build lost its scratch state"))?,
                runs: &resolved.runs,
                installer: &installer,
                types: &types,
                window: node_window,
                now,
                cancel: &cancel,
            },
            pool: &pool,
            count: node_count,
        },
        NodeSide::Scratch(_) => {
            return Err(storage("the over-budget build lost its node pass"));
        }
    };
    let ordinal = build_ordinal_facet(&output, &ordinal_nodes, generation, &cancel)?;
    check_cancelled(&cancel)?;
    passes.extend([meter.finish()]);
    crate::graph_construction::construction_failpoint("bulk.after_ordinal");
    if let EdgeSide::Memory(edges) = &mut edge_side {
        edges.uuids = Vec::new();
    }

    let meter = PassMeter::start("adjacency");
    // Built here, on the calling thread, as the staged encoder builds its own.
    let adjacency_options = crate::adjacency::AdjacencyBuildOptions::default().effective();
    let adjacency = match (&edge_side, &ranked_edges, &scratch, &scratch_plan) {
        (EdgeSide::Memory(edges), _, _, _) => run_pass(&pool, cancelled, &cancel, || {
            csr::write_adjacency(
                &output.path().join("graph"),
                edges,
                &relations,
                generation,
                now,
                output.allocation(),
                &adjacency_options,
                &cancel,
            )
        })?,
        (EdgeSide::Scratch(_), Some((csr, _)), Some(scratch), Some(sized)) => {
            let groups = csr::AdjacencyGroups::new(&relations)?;
            let graph_root = output.path().join("graph");
            run_pass(&pool, cancelled, &cancel, || {
                scratch_csr::write_scratch_adjacency(
                    &scratch_csr::AdjacencyContext {
                        scratch,
                        plan: sized,
                        graph_root: &graph_root,
                        groups: &groups,
                        generation,
                        built_at_micros: now,
                        total_edges: edge_count,
                        allocation: output.allocation(),
                        options: &adjacency_options,
                        cancel: &cancel,
                    },
                    csr,
                )
            })?
        }
        _ => return Err(storage("the over-budget build lost its scratch state")),
    };
    passes.extend([meter.finish()]);
    crate::graph_construction::construction_failpoint("bulk.after_adjacency");
    // Snapshot topology scratch traffic; property scratch remains live until
    // the overlays finish and contributes its separately counted traffic.
    let mut scratch_report = ScratchReport::default();
    if let (Some(scratch), Some(sized)) = (&scratch, &scratch_plan) {
        scratch_report = ScratchReport {
            concurrency: sized.concurrency as u64,
            edge_partitions: match &edge_side {
                EdgeSide::Scratch(scattered) => scattered.partitions.len() as u64,
                EdgeSide::Memory(_) => 0,
            },
            csr_partitions: ranked_edges
                .as_ref()
                .map_or(0, |(csr, _)| csr.out.len().max(csr.inn.len()) as u64),
            write_bytes: scratch.written_bytes(),
            read_bytes: scratch.read_bytes(),
            largest_partition: match &edge_side {
                EdgeSide::Scratch(scattered) => scattered.counts.iter().copied().max().unwrap_or(0),
                EdgeSide::Memory(_) => 0,
            },
            refinement_steps: match &edge_side {
                EdgeSide::Scratch(scattered) => scattered.refinement_steps,
                EdgeSide::Memory(_) => 0,
            },
            refinement_write_bytes: match &edge_side {
                EdgeSide::Scratch(scattered) => scattered.refinement_write_bytes,
                EdgeSide::Memory(_) => 0,
            },
            refinement_read_bytes: match &edge_side {
                EdgeSide::Scratch(scattered) => scattered.refinement_read_bytes,
                EdgeSide::Memory(_) => 0,
            },
            csr_spool_write_bytes: ranked_edges
                .as_ref()
                .map_or(0, |(csr, _)| csr.csr_spool_write_bytes()),
            csr_spool_read_bytes: ranked_edges
                .as_ref()
                .map_or(0, |(csr, _)| csr.csr_spool_read_bytes()),
            peak_csr_carry_entries: ranked_edges
                .as_ref()
                .map_or(0, |(csr, _)| csr.peak_carry_entries()),
            ..ScratchReport::default()
        };
        drop(ranked_edges);
        // Node tables on scratch: every written block was read once. The
        // UUID-only runs were read to their end by the identity stream.
        if let NodeSide::Scratch(ScratchNodes {
            scattered,
            resolved: Some(resolved),
        }) = &nodes
        {
            scratch_report.node_partitions = scattered.leaves.len() as u64;
            scratch_report.largest_node_partition =
                scattered.counts.iter().copied().max().unwrap_or(0);
            scratch_report.node_refinement_steps = scattered.refinement_steps;
            scratch_report.node_refinement_write_bytes = scattered.refinement_write_bytes;
            scratch_report.node_refinement_read_bytes = scattered.refinement_read_bytes;
            scratch_report.node_write_bytes =
                node_scatter_traffic.0 + resolved.runs.written_bytes();
            scratch_report.node_read_bytes =
                node_scatter_traffic.1 + node_leaf_read + resolved.runs.read_bytes();
            if let EdgeSide::Scratch(_) = &edge_side {
                let refs = refs.as_ref();
                let probes = identity_probes.as_ref();
                scratch_report.endpoint_write_bytes = refs
                    .map_or(0, scratch::Partitions::written_bytes)
                    + probes.map_or(0, scratch::Partitions::written_bytes)
                    + resolved.resolved.written_bytes();
                scratch_report.endpoint_read_bytes = refs
                    .map_or(0, scratch::Partitions::read_bytes)
                    + probes.map_or(0, scratch::Partitions::read_bytes)
                    + resolved.resolved.read_bytes();
            }
        }
    }

    // Property overlays: sequential, through the staged encoder's own writer,
    // which leases its own compression lanes. Return ours first.
    drop(lease);
    let meter = PassMeter::start("properties");
    let mut artifacts = Vec::new();
    {
        let cache_window =
            graphforge_filesystem::cache_release_window_for_streams(2).map_err(storage)?;
        let mut lanes = lanes::ParquetLanes::new(
            if scratch.is_some() { None } else { admission },
            budgets.max_batch_bytes,
        );
        let mut property_evidence = GraphConstructionEncodingEvidence::default();
        for (rows, groups, kind) in [
            (
                node_properties.as_ref(),
                node_scratch_groups.as_deref(),
                ConstructionChunkKind::Node,
            ),
            (
                edge_properties.as_ref(),
                edge_scratch_groups.as_deref(),
                ConstructionChunkKind::Edge,
            ),
        ] {
            if let (Some(rows), Some(groups)) = (rows, groups) {
                property_emit::emit(
                    rows,
                    groups,
                    kind,
                    budgets,
                    &semantics,
                    &mut routes,
                    &mut lanes,
                    &output,
                    cache_window,
                    &mut property_evidence,
                    cancelled,
                    &mut artifacts,
                )?;
            }
        }

        for (groups, kind) in [
            (node_groups.as_deref(), ConstructionChunkKind::Node),
            (edge_groups.as_deref(), ConstructionChunkKind::Edge),
        ] {
            let mut ordinals = std::collections::BTreeMap::<String, u64>::new();
            for group in groups.unwrap_or_default() {
                let mut offset = 0;
                while offset < group.batch.num_rows() {
                    let length = budgets.max_batch_rows.min(group.batch.num_rows() - offset);
                    let slice = group.batch.slice(offset, length);
                    offset += length;
                    match kind {
                        ConstructionChunkKind::Node if slice.num_columns() > 2 => {
                            node_property_batch(
                                &slice,
                                &mut ordinals,
                                0,
                                ontology_mode,
                                semantics.context,
                                semantics.bindings,
                                &mut routes,
                                &mut lanes,
                                &output,
                                cache_window,
                                &mut property_evidence,
                                cancelled,
                                &mut artifacts,
                            )?;
                        }
                        ConstructionChunkKind::Edge if slice.num_columns() > 4 => {
                            edge_property_batch(
                                &slice,
                                &mut ordinals,
                                0,
                                ontology_mode,
                                semantics.context,
                                semantics.bindings,
                                &mut routes,
                                &mut lanes,
                                &output,
                                cache_window,
                                &mut property_evidence,
                                cancelled,
                                &mut artifacts,
                            )?;
                        }
                        _ => {}
                    }
                }
            }
        }
        lanes.flush(&output, &mut property_evidence, cancelled, &mut artifacts)?;
        evidence.output_write_bytes += property_evidence.output_write_bytes;
        evidence.output_write_operations += property_evidence.output_write_operations;
        evidence.fsync_operations += property_evidence.fsync_operations;
    }
    passes.extend([meter.finish()]);
    let property_scratch_write_bytes = node_properties
        .as_ref()
        .map_or(0, property_rows::PropertyRows::written_bytes)
        + edge_properties
            .as_ref()
            .map_or(0, property_rows::PropertyRows::written_bytes);
    let property_scratch_read_bytes = node_properties
        .as_ref()
        .map_or(0, property_rows::PropertyRows::read_bytes)
        + edge_properties
            .as_ref()
            .map_or(0, property_rows::PropertyRows::read_bytes);
    scratch_report.write_bytes += property_scratch_write_bytes;
    scratch_report.read_bytes += property_scratch_read_bytes;
    // The peak is a high-water mark the appends have kept current all along;
    // read it before the tree goes away.
    scratch_report.peak_occupied_bytes = scratch
        .as_ref()
        .map_or(0, scratch::Scratch::peak_occupied_bytes);
    drop((node_properties, edge_properties));
    if let Some(scratch) = scratch {
        scratch.remove()?;
    }

    // Small controls, then the inventory.
    let meter = PassMeter::start("finalize");
    copy_artifact(
        std::io::Cursor::new(crate::runtime_entity_labels::runtime_entity_label_encoding_bytes()?),
        &output,
        "topology/runtime_entity_label_encoding.json",
        &mut artifacts,
        &mut evidence,
    )?;
    write_surrogate_tails(
        &output,
        node_count,
        edge_count,
        cancelled,
        &mut artifacts,
        &mut evidence,
    )?;
    copy_artifact(
        std::io::Cursor::new(
            format!(
                "{{\"topology_generation\":{generation},\"search_generation\":{generation},\"property_generation\":{generation}}}\n"
            )
            .into_bytes(),
        ),
        &output,
        "topology/generation.json",
        &mut artifacts,
        &mut evidence,
    )?;
    copy_artifact(
        std::io::Cursor::new(routes.encode(64 * 1024 * 1024)?),
        &output,
        crate::route_component::TABLE_FILE,
        &mut artifacts,
        &mut evidence,
    )?;
    drop((nodes, edge_side, node_groups, edge_groups));

    installer_extend_adjacency(
        &installer,
        &output,
        &output.path().join("graph"),
        adjacency.captured,
        cancelled,
        &mut evidence,
    )?;
    evidence.adjacency.source_rows = adjacency.source_rows;
    evidence.adjacency.csr_shards = adjacency.shards;
    evidence.output_write_bytes += installer.written_bytes();

    let OrdinalFacet {
        artifacts: v4_artifacts,
        publication: v4_publication,
        metrics: v4_metrics,
    } = ordinal;
    evidence.ordinal_records = v4_metrics.input_records;
    evidence.edge_records = edge_count;
    evidence.ordinal_artifact_write_bytes = v4_metrics.artifact_bytes;
    evidence.ordinal_artifact_write_operations = v4_metrics.write_blocks;
    evidence.ordinal_ranges = u64::try_from(v4_metrics.ranges).map_err(storage)?;
    evidence.ordinal_work_operations = v4_metrics.cancellation_polls;
    evidence.ordinal_peak_buffer_bytes = v4_metrics.peak_buffer_bytes as u64;
    evidence.ordinal_publication_write_bytes = v4_publication.write_bytes;
    evidence.ordinal_publication_write_operations = v4_publication.write_operations;
    evidence.ordinal_fsync_operations = v4_metrics
        .fsync_operations
        .checked_add(v4_publication.fsync_operations)
        .ok_or_else(|| storage("ordinal fsync operation count overflow"))?;
    evidence.ordinal_peak_temporary_bytes = v4_metrics
        .peak_temporary_bytes
        .max(v4_publication.peak_temporary_bytes);
    artifacts.extend(installer.into_artifacts()?);
    artifacts.extend(v4_artifacts.into_iter().map(index_artifact));

    artifacts.sort_unstable_by(|left, right| left.path.cmp(&right.path));
    if artifacts
        .windows(2)
        .any(|pair| pair[0].path == pair[1].path)
    {
        return Err(storage("canonical encoding contains duplicate paths"));
    }
    let mut completed = GraphConstructionEncoding {
        format_version: ENCODING_FORMAT_VERSION,
        root: ENCODED_ROOT.to_owned(),
        generation,
        ontology_mode,
        semantic_authority_sha256: shape.semantic_authority_sha256.clone(),
        shape_inputs_sha256: shape.runtime_catalog_inputs_sha256.clone(),
        shape_authority_sha256: shape_authority_sha256.to_owned(),
        artifacts,
        evidence,
        invocation: GraphConstructionEncodingInvocationEvidence::default(),
    };
    crate::graph_construction::construction_failpoint("bulk.before_inventory");
    install_json(&output, INVENTORY, &completed)?;
    crate::graph_construction::construction_failpoint("bulk.after_inventory_before_intent_removal");
    authenticate_inventory_control(&completed)?;
    remove_encoding_intent(&output)?;
    passes.extend([meter.finish()]);
    let invocation = completed.evidence.clone();
    completed.invocation = GraphConstructionEncodingInvocationEvidence {
        performed: true,
        reused: false,
        evidence: invocation,
    };
    crate::concurrency_attribution::RegionScope::record_work("nodes", node_count);
    crate::concurrency_attribution::RegionScope::record_work("edges", edge_count);
    if let Ok(mut report) = report.lock() {
        *report = BulkBuildReport {
            workers,
            nodes: node_count,
            edges: edge_count,
            passes,
            scratch_concurrency: scratch_report.concurrency,
            edge_partitions: scratch_report.edge_partitions,
            csr_partitions: scratch_report.csr_partitions,
            scratch_write_bytes: scratch_report.write_bytes,
            scratch_read_bytes: scratch_report.read_bytes,
            scratch_peak_occupied_bytes: scratch_report.peak_occupied_bytes,
            largest_edge_partition: scratch_report.largest_partition,
            edge_refinement_steps: scratch_report.refinement_steps,
            edge_refinement_write_bytes: scratch_report.refinement_write_bytes,
            edge_refinement_read_bytes: scratch_report.refinement_read_bytes,
            csr_spool_write_bytes: scratch_report.csr_spool_write_bytes,
            csr_spool_read_bytes: scratch_report.csr_spool_read_bytes,
            peak_csr_carry_entries: scratch_report.peak_csr_carry_entries,
            node_partitions: scratch_report.node_partitions,
            largest_node_partition: scratch_report.largest_node_partition,
            node_refinement_steps: scratch_report.node_refinement_steps,
            node_refinement_write_bytes: scratch_report.node_refinement_write_bytes,
            node_refinement_read_bytes: scratch_report.node_refinement_read_bytes,
            node_scratch_write_bytes: scratch_report.node_write_bytes,
            node_scratch_read_bytes: scratch_report.node_read_bytes,
            endpoint_scratch_write_bytes: scratch_report.endpoint_write_bytes,
            endpoint_scratch_read_bytes: scratch_report.endpoint_read_bytes,
            property_scratch_write_bytes,
            property_scratch_read_bytes,
            property_workspace_reserved_bytes: if scratch_plan.is_some()
                && (retain_nodes || retain_edges)
            {
                budget::property_workspace(budgets)
                    .saturating_add(plan.max_source_schema_bytes().saturating_mul(8))
                    .saturating_add(plan.source_decoder_bytes())
            } else {
                0
            },
        };
    }
    Ok(completed)
}

/// Register the CSR shards (digests already computed while writing) and the
/// small manifests the adjacency build left in the graph tree.
fn installer_extend_adjacency(
    installer: &Installer<'_>,
    output: &StableDirectory,
    graph_root: &Path,
    captured: Vec<crate::adjacency::CapturedAdjacencyArtifact>,
    cancelled: &mut impl FnMut() -> bool,
    evidence: &mut GraphConstructionEncodingEvidence,
) -> Result<(), GfError> {
    let mut artifacts = Vec::new();
    adjacency::register_adjacency_artifacts(
        output,
        graph_root,
        &crate::adjacency::adjacency_dir(graph_root),
        captured,
        cancelled,
        &mut artifacts,
        evidence,
    )?;
    installer.extend(artifacts)
}
