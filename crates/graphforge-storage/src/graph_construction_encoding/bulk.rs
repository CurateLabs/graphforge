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
//!   membership and ordinal indexes, and the adjacency CSR.
//!
//! Intermediates are never synced or hashed. A crash discards them and the
//! build reruns from the sources (ADR 0038 as amended for initial builds).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::*;

mod csr;
mod emit;
mod identities;
mod install;
mod plan;
mod tables;

pub use plan::{BulkBatchReader, BulkBuildPlan, BulkBuildReport, BulkPassReport, BulkSource};

use emit::Semantics;
use install::Installer;
use plan::PassMeter;
use tables::{NodeIndex, NodeTable, check_cancelled};

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

struct Membership {
    index: crate::uuid_membership::ConstructionIndexEncoding,
    v4_artifacts: Vec<crate::uuid_membership::ConstructionIndexOutput>,
    v4_publication: crate::uuid_membership::V4OrdinalPublicationMetrics,
    v4_metrics: crate::uuid_membership::V4OrdinalBuildMetrics,
}

/// UUID membership index (`identities-v5`, `node-surrogates-v5`) and the v4
/// ordinal artifacts, both streamed from the ranked arrays.
fn build_membership(
    output: &StableDirectory,
    nodes: &NodeTable,
    edge_uuids: &[[u8; 16]],
    generation: u64,
    cancel: &AtomicBool,
) -> Result<Membership, GfError> {
    let mut cancelled = || cancel.load(Ordering::Acquire);
    let stream = identities::IdentityStream::new(&nodes.uuids, edge_uuids);
    let len = stream.byte_len();
    let index = crate::uuid_membership::encode_construction_index(
        crate::uuid_membership::ConstructionIdentityInput::Stream {
            reader: Box::new(stream),
            len,
        },
        output.physical(),
        generation,
        0,
        None,
        nodes.uuids.len() as u64,
        edge_uuids.len() as u64,
        &mut cancelled,
        output.allocation(),
    )?;
    let membership_dir = output
        .open_child_directory(OsStr::new("graph"))
        .map_err(storage)?
        .open_child_directory(OsStr::new("topology"))
        .map_err(storage)?
        .open_child_directory(OsStr::new("uuid-membership"))
        .map_err(storage)?;
    let cache_window = graphforge_filesystem::cache_release_window_for_streams(5).map_err(storage)?;
    let mut writer = crate::uuid_membership::V4OrdinalConstructionWriter::start_with_cache_window(
        generation,
        membership_dir.physical(),
        cache_window,
        membership_dir.allocation(),
    )?;
    for (position, uuid) in nodes.uuids.iter().enumerate() {
        writer.push_pair(Uuid::from_bytes(*uuid), position as u64 + 1, &mut cancelled)?;
    }
    let bundle = writer.finish()?;
    let (v4_artifacts, v4_publication, v4_metrics) =
        crate::uuid_membership::publish_v4_construction_artifacts(
            output.physical(),
            bundle,
            generation,
            &index.source_sha256,
            None,
            &mut cancelled,
            output.allocation(),
        )?;
    Ok(Membership {
        index,
        v4_artifacts,
        v4_publication,
        v4_metrics,
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
        admission.acquire(
            std::num::NonZeroUsize::new(admission.limit()).expect("positive admission"),
            &mut || false,
        )
        .ok()
    });
    let workers = lease.as_ref().map_or_else(
        || std::thread::available_parallelism().map_or(1, usize::from),
        |lease| lease.lanes().get(),
    );
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(workers)
        .thread_name(|index| format!("gf-bulk-{index}"))
        .build()
        .map_err(storage)?;
    let cancel = AtomicBool::new(false);
    let mut passes = Vec::new();
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
    passes.push(meter.finish());

    // Pass 1: nodes.
    let meter = PassMeter::start("nodes");
    let nodes = run_pass(&pool, cancelled, &cancel, || {
        tables::collect_nodes(&plan.nodes, retain_nodes, &cancel)
    })?;
    passes.push(meter.finish());

    // Pass 2: edges and endpoint resolution.
    let meter = PassMeter::start("edges");
    let index = pool.install(|| NodeIndex::build(&nodes.uuids));
    let edges = run_pass(&pool, cancelled, &cancel, || {
        tables::collect_edges(&plan.edges, retain_edges, &nodes, &index, &cancel)
    })?;
    drop(index);
    passes.push(meter.finish());

    // Pass 3: catalog and ranked tables.
    let meter = PassMeter::start("catalog");
    let semantic_context = semantic_authority
        .map(ConstructionSemanticAuthority::context)
        .transpose()?;
    let semantics = Semantics {
        mode: ontology_mode,
        context: semantic_context.as_ref(),
        bindings: semantic_authority.map(|authority| &authority.bindings),
    };
    let (node_groups, edge_groups) = run_pass(&pool, cancelled, &cancel, || {
        Ok((
            retain_nodes
                .then(|| emit::schema_groups(&nodes.kept, "node_uuid"))
                .transpose()?,
            retain_edges
                .then(|| emit::schema_groups(&edges.kept, "edge_uuid"))
                .transpose()?,
        ))
    })?;
    let built = emit::build_catalog(
        budgets,
        &nodes,
        &edges,
        node_groups.as_deref(),
        edge_groups.as_deref(),
    )?;
    let types = emit::node_types(&nodes.label_names, &built.entity_ids, &semantics)?;
    let relations = emit::relation_routes(&edges.rel_names, &semantics)?;
    let mut routes = crate::route_component::RouteTable::default();
    let components = emit::register_routes(&mut routes, &relations)?;
    passes.push(meter.finish());

    let installer = Installer::new(&output);
    let node_window = emit::window_rows(budgets, 128);
    let edge_window = emit::window_rows(budgets, 192);
    let now = shape.runtime_catalog_now_micros;

    let meter = PassMeter::start("tables");
    run_pass(&pool, cancelled, &cancel, || {
        installer.install_parquet(
            "topology/runtime_catalog.parquet",
            &built.catalog.to_record_batch(),
        )?;
        emit::emit_nodes(&installer, &nodes, &types, node_window, now, &cancel)?;
        emit::emit_edges(
            &installer,
            &nodes,
            &edges,
            &relations,
            &components,
            &semantics,
            edge_window,
            now,
            &cancel,
        )
    })?;
    passes.push(meter.finish());

    let meter = PassMeter::start("adjacency");
    let adjacency = run_pass(&pool, cancelled, &cancel, || {
        csr::write_adjacency(
            &output.path().join("graph"),
            &edges,
            &relations,
            generation,
            now,
            output.allocation(),
            &cancel,
        )
    })?;
    passes.push(meter.finish());

    let meter = PassMeter::start("membership");
    let membership = build_membership(&output, &nodes, &edges.uuids, generation, &cancel)?;
    check_cancelled(&cancel)?;
    passes.push(meter.finish());

    // Property overlays: sequential, through the staged encoder's own writer.
    let meter = PassMeter::start("properties");
    let mut artifacts = Vec::new();
    {
        let cache_window =
            graphforge_filesystem::cache_release_window_for_streams(2).map_err(storage)?;
        let mut lanes = lanes::ParquetLanes::new(admission, budgets.max_batch_bytes);
        let mut property_evidence = GraphConstructionEncodingEvidence::default();
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
    passes.push(meter.finish());

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
        nodes.uuids.len() as u64,
        edges.uuids.len() as u64,
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
    let node_count = nodes.uuids.len() as u64;
    let edge_count = edges.uuids.len() as u64;
    drop((nodes, edges, node_groups, edge_groups));

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
    evidence.input_read_bytes = 0;

    let Membership {
        index,
        v4_artifacts,
        v4_publication,
        v4_metrics,
    } = membership;
    evidence.ordinal_records = v4_metrics.input_records;
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
    evidence.membership_records = index.input_records;
    evidence.membership_write_bytes = index.final_write_bytes;
    evidence.membership_total_write_bytes = index.write_bytes;
    evidence.membership_read_bytes = index.read_bytes;
    evidence.membership_read_operations = index.read_operations;
    evidence.membership_write_operations = index.write_operations;
    evidence.membership_fsync_operations = index.fsync_operations;
    evidence.membership_created_runs = index.created_runs;
    evidence.membership_peak_buffer_bytes = index.peak_buffer_bytes;
    evidence.membership_peak_temporary_bytes = index.peak_temporary_bytes;
    account_cache_release(index.cache_release, &mut evidence)?;
    artifacts.extend(installer.into_artifacts()?);
    artifacts.extend(index.artifacts.into_iter().map(index_artifact));
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
        retained_artifacts: Vec::new(),
        evidence,
        invocation: GraphConstructionEncodingInvocationEvidence::default(),
    };
    install_json(&output, INVENTORY, &completed)?;
    authenticate_inventory_control(&completed, None)?;
    remove_encoding_intent(&output)?;
    passes.push(meter.finish());
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
        };
    }
    drop(lease);
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
