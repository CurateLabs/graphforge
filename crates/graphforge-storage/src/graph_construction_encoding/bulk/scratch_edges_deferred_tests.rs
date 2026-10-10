//! The deferred route's phase order, against the real passes: the raw edge
//! pass refines before any reference or probe exists, the reference pass
//! replays the planned tasks under a canonical topology proof, and a replay
//! that differs refuses before anything is resolved (#1929).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use arrow::array::{Array, ArrayRef, FixedSizeBinaryArray, StringArray};
use arrow::record_batch::RecordBatch;
use graphforge_core::ApiErrorCode;

use super::super::ENDPOINT_REFERENCE_SEGMENT_BYTES;
use super::super::gate::ByteGate;
use super::super::scratch::SCRATCH_DIRECTORY;
use super::super::scratch_nodes::{
    PROBE_RECORD, REF_RECORD, ResolveContext, ScatteredNodes, resolve_endpoints, scatter_nodes,
};
use super::super::{BulkBatchReader, BulkBuildPlan, BulkSource};
use super::*;
use crate::graph_construction::GraphConstructionSession;
use uuid::Uuid;

/// A reader over stored canonical batches, `per_task` batches per task. Its
/// read counter counts across the passes, exactly as a production source is
/// read twice: once by the raw pass, once by the reference pass.
struct Fixed {
    batches: Vec<RecordBatch>,
    per_task: usize,
    reads: Vec<AtomicUsize>,
}

impl Fixed {
    fn new(batches: Vec<RecordBatch>, per_task: usize) -> Self {
        let tasks = batches.len().div_ceil(per_task);
        Self {
            batches,
            per_task,
            reads: (0..tasks).map(|_| AtomicUsize::new(0)).collect(),
        }
    }

    fn stored<'a>(&'a self, task: usize) -> impl Iterator<Item = &'a RecordBatch> {
        self.batches
            .iter()
            .skip(task * self.per_task)
            .take(self.per_task)
    }

    fn read(&self, task: usize) -> usize {
        self.reads[task].fetch_add(1, Ordering::SeqCst)
    }
}

impl BulkBatchReader for Fixed {
    fn task_rows(&self, task: usize) -> usize {
        self.stored(task).map(RecordBatch::num_rows).sum()
    }

    fn read_task(
        &self,
        task: usize,
        sink: &mut dyn FnMut(RecordBatch) -> Result<(), GfError>,
    ) -> Result<(), GfError> {
        self.read(task);
        for batch in self.stored(task) {
            sink(batch.clone())?;
        }
        Ok(())
    }
}

/// A reader whose task reads regroup the same rows differently every time:
/// read `n` of a task emits its rows in chunks of `splits[n - 1]`. Only the
/// batch boundaries move; the row stream does not.
struct Regrouping(Fixed, [usize; 2]);

impl Regrouping {
    fn new(batches: Vec<RecordBatch>, per_task: usize, splits: [usize; 2]) -> Self {
        Self(Fixed::new(batches, per_task), splits)
    }
}

impl BulkBatchReader for Regrouping {
    fn task_rows(&self, task: usize) -> usize {
        self.0.task_rows(task)
    }

    fn read_task(
        &self,
        task: usize,
        sink: &mut dyn FnMut(RecordBatch) -> Result<(), GfError>,
    ) -> Result<(), GfError> {
        let split = self.1[self.0.read(task).min(self.1.len() - 1)];
        for batch in self.0.stored(task) {
            for offset in (0..batch.num_rows()).step_by(split) {
                let length = split.min(batch.num_rows() - offset);
                sink(batch.slice(offset, length))?;
            }
        }
        Ok(())
    }
}

/// A reader whose second read of a task hands out different endpoint UUIDs:
/// the topology the reference pass would see is not the one the raw pass
/// accepted.
struct Mutating(Fixed);

impl Mutating {
    fn new(batches: Vec<RecordBatch>, per_task: usize) -> Self {
        Self(Fixed::new(batches, per_task))
    }
}

impl BulkBatchReader for Mutating {
    fn task_rows(&self, task: usize) -> usize {
        self.0.task_rows(task)
    }

    fn read_task(
        &self,
        task: usize,
        sink: &mut dyn FnMut(RecordBatch) -> Result<(), GfError>,
    ) -> Result<(), GfError> {
        let replayed = self.0.read(task) > 0;
        for batch in self.0.stored(task) {
            if !replayed {
                sink(batch.clone())?;
                continue;
            }
            let endpoints = batch
                .column(3)
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            let shifted: Vec<[u8; 16]> = (0..endpoints.len())
                .map(|row| {
                    let mut value = <[u8; 16]>::try_from(endpoints.value(row)).unwrap();
                    value[15] ^= 0xff;
                    value
                })
                .collect();
            let shifted =
                FixedSizeBinaryArray::try_from_iter(shifted.iter().map(|value| value.as_slice()))
                    .unwrap();
            let columns: Vec<ArrayRef> = vec![
                batch.column(0).clone(),
                batch.column(1).clone(),
                batch.column(2).clone(),
                Arc::new(shifted),
            ];
            sink(RecordBatch::try_new(batch.schema(), columns).unwrap())?;
        }
        Ok(())
    }
}

/// A reader that cancels the build in the middle of a task's second read,
/// after its first batch has been handed over and written.
struct Cancelling {
    fixed: Fixed,
    cancel: Arc<AtomicBool>,
}

impl Cancelling {
    fn new(batches: Vec<RecordBatch>, per_task: usize, cancel: Arc<AtomicBool>) -> Self {
        Self {
            fixed: Fixed::new(batches, per_task),
            cancel,
        }
    }
}

impl BulkBatchReader for Cancelling {
    fn task_rows(&self, task: usize) -> usize {
        self.fixed.task_rows(task)
    }

    fn read_task(
        &self,
        task: usize,
        sink: &mut dyn FnMut(RecordBatch) -> Result<(), GfError>,
    ) -> Result<(), GfError> {
        let replayed = self.fixed.read(task) > 0;
        let mut batches = self.fixed.stored(task);
        if let Some(first) = batches.next() {
            sink(first.clone())?;
        }
        if replayed {
            self.cancel.store(true, Ordering::SeqCst);
        }
        for batch in batches {
            sink(batch.clone())?;
        }
        Ok(())
    }
}

/// One planned edge source over a shared reader: pass one and the reference
/// pass read the very same reader instance, as a production build does.
/// One planned edge source over a shared reader: pass one and the reference
/// pass read the very same reader instance, as a production build does. The
/// reader must have been built with the same `per_task`.
fn edge_source(
    reader: Arc<dyn BulkBatchReader>,
    batches: &[RecordBatch],
    per_task: usize,
) -> BulkSource<'static> {
    BulkSource {
        tasks: batches.len().div_ceil(per_task),
        rows: batches.iter().map(|batch| batch.num_rows() as u64).sum(),
        property_free: batches.iter().all(|batch| batch.num_columns() == 4),
        decoded_bytes: batches
            .iter()
            .map(|batch| batch.get_array_memory_size() as u64)
            .sum(),
        reader,
    }
}

fn node_source(batches: &[RecordBatch]) -> BulkSource<'static> {
    BulkSource {
        tasks: batches.len(),
        rows: batches.iter().map(|batch| batch.num_rows() as u64).sum(),
        property_free: batches.iter().all(|batch| batch.num_columns() == 2),
        decoded_bytes: batches
            .iter()
            .map(|batch| batch.get_array_memory_size() as u64)
            .sum(),
        reader: Arc::new(Fixed::new(batches.to_vec(), 1)),
    }
}

fn node_batch_of(uuids: &[[u8; 16]], labels: &[&str]) -> RecordBatch {
    RecordBatch::try_new(
        crate::graph_construction::CONSTRUCTION_NODE_SCHEMA.clone(),
        vec![
            Arc::new(fixed(uuids)),
            Arc::new(StringArray::from(labels.to_vec())),
        ],
    )
    .unwrap()
}

fn edge_batch_of(
    uuids: &[[u8; 16]],
    rels: &[&str],
    src: &[[u8; 16]],
    dst: &[[u8; 16]],
) -> RecordBatch {
    RecordBatch::try_new(
        crate::graph_construction::CONSTRUCTION_EDGE_SCHEMA.clone(),
        vec![
            Arc::new(fixed(uuids)),
            Arc::new(StringArray::from(rels.to_vec())),
            Arc::new(fixed(src)),
            Arc::new(fixed(dst)),
        ],
    )
    .unwrap()
}

fn fixed(values: &[[u8; 16]]) -> FixedSizeBinaryArray {
    FixedSizeBinaryArray::try_from_iter(values.iter().map(|value| value.as_slice())).unwrap()
}

fn uuid(kind: u8, index: u64) -> [u8; 16] {
    let mut value = [0_u8; 16];
    value[0] = kind;
    value[8..].copy_from_slice(&index.to_be_bytes());
    value
}

/// A plan assembled by hand with `gate` bytes of partitions-in-flight
/// allowance, whose admitted leaf size is `gate / 2 / edge_row_bytes` rows.
fn sized_plan(gate: u64, staging_bytes: usize) -> ScratchPlan {
    let mut sized = ScratchPlan::sized(1, 1, 1, gate, staging_bytes);
    sized.node_tables_on_scratch = true;
    sized.node_partitions = 1;
    sized.node_row_bytes = 40;
    sized
}

/// A plan whose passes keep every fixture in one partition without
/// refinement.
fn plan(staging_bytes: usize) -> ScratchPlan {
    sized_plan(1 << 20, staging_bytes)
}

/// The real node scatter followed by the real raw edge pass, in the state the
/// build holds between the passes: the edges refined, their topology proof
/// registered, and no reference or probe writer created yet.
fn build(
    scratch: &Scratch,
    sized: &ScratchPlan,
    node_batches: &[RecordBatch],
    edge_sources: &[BulkSource<'_>],
) -> Result<(ScatteredNodes, ScatteredEdges), GfError> {
    let decode = ByteGate::new(0);
    let cancel = AtomicBool::new(false);
    let budgets = GraphConstructionBudgets::default();
    let scattered_nodes = scatter_nodes(
        &[node_source(node_batches)],
        budgets,
        None,
        &decode,
        sized,
        scratch,
        &cancel,
    )?;
    let scattered_edges = scatter_edges(
        edge_sources,
        budgets,
        None,
        &decode,
        &Endpoints::Deferred,
        sized,
        scratch,
        &cancel,
    )?;
    Ok((scattered_nodes, scattered_edges))
}

/// The reference and probe writers a build creates between the passes, the
/// way `encode_bulk` does.
fn references<'a>(
    scratch: &'a Scratch,
    leaves: usize,
) -> Result<(Partitions, Partitions), GfError> {
    let refs = Partitions::create_segmented(
        scratch,
        "refs",
        leaves,
        REF_RECORD,
        ENDPOINT_REFERENCE_SEGMENT_BYTES,
    )?;
    let probes = Partitions::create(scratch, "probes", leaves, PROBE_RECORD)?;
    Ok((refs, probes))
}

/// `nodes` nodes and one edge source of `edges` edges over them, all in one
/// task each.
fn fixture(count: usize, edges: usize) -> (Vec<RecordBatch>, Vec<RecordBatch>) {
    let node_uuids = (0..count as u64).map(|i| uuid(0x10, i)).collect::<Vec<_>>();
    let edge_uuids = (0..edges as u64).map(|i| uuid(0x20, i)).collect::<Vec<_>>();
    let src = (0..edges)
        .map(|i| node_uuids[i % count])
        .collect::<Vec<_>>();
    let dst = (0..edges)
        .map(|i| node_uuids[(i * 7 + 3) % count])
        .collect::<Vec<_>>();
    let nodes = vec![node_batch_of(&node_uuids, &vec!["Person"; count])];
    let edges = vec![edge_batch_of(
        &edge_uuids,
        &vec!["KNOWS"; edges],
        &src,
        &dst,
    )];
    (nodes, edges)
}

fn scratch_names(scratch: &Scratch) -> Vec<String> {
    let mut names = std::fs::read_dir(scratch.file(""))
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    names.sort();
    names
}

fn has_reference_files(scratch: &Scratch) -> bool {
    scratch_names(scratch)
        .iter()
        .any(|name| name.starts_with("refs-") || name.starts_with("probes-"))
}

fn has_resolved_files(scratch: &Scratch) -> bool {
    scratch_names(scratch)
        .iter()
        .any(|name| name.starts_with("resolved-") || name.starts_with("node-runs-"))
}

/// The no-refinement control of the refinement occupancy test below: the
/// same fixture under a gate that admits it whole stays one raw leaf, and
/// the tracked logical occupancy is exactly the node leaf plus that leaf.
#[test]
fn raw_edges_without_refinement_leave_no_refs_or_probes_and_no_extra_bytes_in_the_occupancy() {
    let root = tempfile::tempdir().unwrap();
    let directory = crate::graph_construction_encoding::StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    let sized = plan(4096);
    let (nodes, edges) = fixture(40, 100);
    let reader = Arc::new(Fixed::new(edges.clone(), 1));
    let sources = [edge_source(reader, &edges, 1)];
    let (_, scattered) = build(&scratch, &sized, &nodes, &sources).unwrap();
    assert_eq!(scattered.total, 100);
    assert_eq!(scattered.counts, vec![100]);
    assert!(scattered.topology_proof.is_some());
    // The refinement boundary: no reference or probe writer was ever created,
    // and the only live scratch is the node leaf and the raw edge partition.
    assert!(!has_reference_files(&scratch));
    assert_eq!(
        scratch_names(&scratch),
        vec!["edges-000000.blocks", "nodes-000000.blocks"]
    );
    // One 8-byte block header per file: 8 + 20*40 nodes, 8 + 28*100 edges.
    let expected = (8 + 20 * 40) + (8 + 28 * 100);
    assert_eq!(scratch.occupied_bytes(), expected);
    assert_eq!(scratch.peak_occupied_bytes(), expected);
    scratch.remove().unwrap();
}

#[test]
fn the_reference_pass_replays_the_planned_tasks_and_the_proofs_agree() {
    let root = tempfile::tempdir().unwrap();
    let directory = crate::graph_construction_encoding::StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    let sized = plan(4096);
    let (nodes, edges) = fixture(40, 100);
    let reader = Arc::new(Fixed::new(edges.clone(), 1));
    let sources = [edge_source(reader, &edges, 1)];
    let (scattered_nodes, scattered_edges) = build(&scratch, &sized, &nodes, &sources).unwrap();
    let accepted = scattered_edges.topology_proof.unwrap();
    let (refs, probes) = references(&scratch, scattered_nodes.leaves.len()).unwrap();
    let sink = RefSink {
        router: &scattered_nodes.router,
        refs: &refs,
        probes: &probes,
    };
    let decode = ByteGate::new(0);
    let cancel = AtomicBool::new(false);
    let replayed = replay_edges(
        &sources,
        GraphConstructionBudgets::default(),
        &decode,
        &sink,
        &sized,
        &scratch,
        &cancel,
    )
    .unwrap();
    assert_eq!(replayed.proof, accepted);
    assert!(replayed.miss.is_none());
    assert_eq!(refs.counts().unwrap(), vec![200]);
    assert_eq!(probes.counts().unwrap(), vec![100]);
    // The replayed endpoints resolve: the same rows the raw pass accepted.
    let (resolved, _) = resolve_endpoints(&ResolveContext {
        scratch: &scratch,
        plan: &sized,
        nodes: &scattered_nodes,
        refs: &refs,
        probes: &probes,
        edges: &scattered_edges,
        cancel: &cancel,
    })
    .unwrap();
    assert!(!resolved.collision);
    assert!(resolved.miss.is_none());
    scratch.remove().unwrap();
}

#[test]
fn a_replay_that_differs_refuses_as_a_source_identity_conflict_before_resolution() {
    let root = tempfile::tempdir().unwrap();
    let directory = crate::graph_construction_encoding::StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    let sized = plan(4096);
    let (nodes, edges) = fixture(40, 100);
    let reader = Arc::new(Mutating::new(edges.clone(), 1));
    let sources = [edge_source(reader, &edges, 1)];
    let (scattered_nodes, scattered_edges) = build(&scratch, &sized, &nodes, &sources).unwrap();
    let (refs, probes) = references(&scratch, scattered_nodes.leaves.len()).unwrap();
    let sink = RefSink {
        router: &scattered_nodes.router,
        refs: &refs,
        probes: &probes,
    };
    let decode = ByteGate::new(0);
    let cancel = AtomicBool::new(false);
    let replayed = replay_edges(
        &sources,
        GraphConstructionBudgets::default(),
        &decode,
        &sink,
        &sized,
        &scratch,
        &cancel,
    )
    .unwrap();
    // The replay read a different tuple stream; the production guard refuses
    // it before anything is resolved or published.
    assert_ne!(replayed.proof, scattered_edges.topology_proof.unwrap());
    let error = scattered_edges
        .ensure_replay_matches(&scratch, &sized, replayed.proof, &cancel)
        .unwrap_err();
    assert!(
        matches!(
            error,
            GfError::Api {
                code: ApiErrorCode::IdentityConflict,
                ..
            }
        ),
        "{error}"
    );
    assert!(error.to_string().contains("topology versions"), "{error}");
    // The refusal happened before any endpoint resolution: the writers are
    // still in place, unconsumed, and no resolved output exists.
    assert!(has_reference_files(&scratch));
    assert_eq!(refs.counts().unwrap(), vec![200]);
    for leaf in 0..refs.len() {
        assert!(refs.path(leaf).exists());
        assert!(probes.path(leaf).exists());
    }
    assert!(!has_resolved_files(&scratch));
    scratch.remove().unwrap();
}

#[test]
fn a_batch_boundary_change_between_the_passes_keeps_the_proof_stable() {
    let root = tempfile::tempdir().unwrap();
    let directory = crate::graph_construction_encoding::StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    let sized = plan(4096);
    let (nodes, edges) = fixture(40, 100);
    let reader = Arc::new(Regrouping::new(edges.clone(), 1, [7, 3]));
    let sources = [edge_source(reader, &edges, 1)];
    let (scattered_nodes, scattered_edges) = build(&scratch, &sized, &nodes, &sources).unwrap();
    let accepted = scattered_edges.topology_proof.unwrap();
    let (refs, probes) = references(&scratch, scattered_nodes.leaves.len()).unwrap();
    let sink = RefSink {
        router: &scattered_nodes.router,
        refs: &refs,
        probes: &probes,
    };
    let decode = ByteGate::new(0);
    let cancel = AtomicBool::new(false);
    let replayed = replay_edges(
        &sources,
        GraphConstructionBudgets::default(),
        &decode,
        &sink,
        &sized,
        &scratch,
        &cancel,
    )
    .unwrap();
    assert_eq!(replayed.proof, accepted, "only the batch boundaries moved");
    assert!(replayed.miss.is_none());
    assert_eq!(refs.counts().unwrap(), vec![200]);
    assert_eq!(probes.counts().unwrap(), vec![100]);
    let (resolved, _) = resolve_endpoints(&ResolveContext {
        scratch: &scratch,
        plan: &sized,
        nodes: &scattered_nodes,
        refs: &refs,
        probes: &probes,
        edges: &scattered_edges,
        cancel: &cancel,
    })
    .unwrap();
    assert!(!resolved.collision);
    assert!(resolved.miss.is_none());
    scratch.remove().unwrap();
}

fn split_batch(batch: &RecordBatch, at: usize) -> Vec<RecordBatch> {
    let rows = batch.num_rows();
    assert!(at > 0 && at < rows);
    vec![batch.slice(0, at), batch.slice(at, rows - at)]
}

#[test]
fn cancellation_during_the_reference_pass_stops_before_resolution_and_keeps_the_partial_refs() {
    let root = tempfile::tempdir().unwrap();
    let directory = crate::graph_construction_encoding::StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    // Per-record staging, so the first replayed batch's references are in
    // their files, not only in a worker's buffer, when the token fires.
    let sized = plan(2);
    let (nodes, edges) = fixture(40, 100);
    // Both batches are one task: the replay hands over the first batch — its
    // references and probes land on scratch — then arms the token, and the
    // next batch is refused inside the same task.
    let edge_batches = split_batch(&edges[0], 50);
    // The very token the reference pass observes is the one the reader arms.
    let replay_cancel = Arc::new(AtomicBool::new(false));
    let reader = Arc::new(Cancelling::new(
        edge_batches.clone(),
        2,
        Arc::clone(&replay_cancel),
    ));
    let sources = [edge_source(reader, &edge_batches, 2)];
    let (scattered_nodes, scattered_edges) = build(&scratch, &sized, &nodes, &sources).unwrap();
    assert_eq!(scattered_edges.total, 100);
    let (refs, probes) = references(&scratch, scattered_nodes.leaves.len()).unwrap();
    let sink = RefSink {
        router: &scattered_nodes.router,
        refs: &refs,
        probes: &probes,
    };
    let decode = ByteGate::new(0);
    let error = replay_edges(
        &sources,
        GraphConstructionBudgets::default(),
        &decode,
        &sink,
        &sized,
        &scratch,
        &replay_cancel,
    )
    .unwrap_err();
    assert!(error.to_string().contains("cancelled"), "{error}");
    // The first batch's references and probes are on scratch, unconsumed, and
    // nothing was resolved or published.
    assert_eq!(refs.counts().unwrap(), vec![100]);
    assert!(probes.counts().unwrap()[0] > 0);
    assert!(has_reference_files(&scratch));
    assert!(!has_resolved_files(&scratch));
    scratch.remove().unwrap();
    assert!(!root.path().join(SCRATCH_DIRECTORY).exists());
}

#[test]
fn multi_source_edges_replay_stable_rows_under_one_proof() {
    let root = tempfile::tempdir().unwrap();
    let directory = crate::graph_construction_encoding::StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    let sized = plan(4096);
    let node_uuids = (0..40_u64).map(|i| uuid(0x10, i)).collect::<Vec<_>>();
    let nodes = vec![node_batch_of(&node_uuids, &vec!["Person"; 40])];
    let first = {
        let uuids = (0..30_u64).map(|i| uuid(0x20, i)).collect::<Vec<_>>();
        let src = (0..30).map(|i| node_uuids[i % 40]).collect::<Vec<_>>();
        let dst = (0..30)
            .map(|i| node_uuids[(i * 3 + 1) % 40])
            .collect::<Vec<_>>();
        vec![edge_batch_of(&uuids, &vec!["KNOWS"; 30], &src, &dst)]
    };
    let second = {
        let uuids = (30..100_u64).map(|i| uuid(0x20, i)).collect::<Vec<_>>();
        let src = (0..70)
            .map(|i| node_uuids[(i * 5) % 40])
            .collect::<Vec<_>>();
        let dst = (0..70)
            .map(|i| node_uuids[(i * 7 + 2) % 40])
            .collect::<Vec<_>>();
        vec![edge_batch_of(&uuids, &vec!["OWNS"; 70], &src, &dst)]
    };
    let sources = [
        edge_source(Arc::new(Fixed::new(first.clone(), 1)), &first, 1),
        edge_source(Arc::new(Fixed::new(second.clone(), 1)), &second, 1),
    ];
    let (scattered_nodes, scattered_edges) = build(&scratch, &sized, &nodes, &sources).unwrap();
    assert_eq!(scattered_edges.total, 100);
    let accepted = scattered_edges.topology_proof.unwrap();
    let (refs, probes) = references(&scratch, scattered_nodes.leaves.len()).unwrap();
    let sink = RefSink {
        router: &scattered_nodes.router,
        refs: &refs,
        probes: &probes,
    };
    let decode = ByteGate::new(0);
    let cancel = AtomicBool::new(false);
    let replayed = replay_edges(
        &sources,
        GraphConstructionBudgets::default(),
        &decode,
        &sink,
        &sized,
        &scratch,
        &cancel,
    )
    .unwrap();
    assert_eq!(replayed.proof, accepted);
    assert!(replayed.miss.is_none());
    // Every row of both sources is accounted exactly once.
    assert_eq!(refs.counts().unwrap(), vec![200]);
    assert_eq!(probes.counts().unwrap(), vec![100]);
    scratch.remove().unwrap();
}

#[test]
fn raw_edge_refinement_keeps_reference_and_probe_bytes_out_of_the_tracked_occupancy() {
    let root = tempfile::tempdir().unwrap();
    let directory = crate::graph_construction_encoding::StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    // A legitimately smaller gate, as a smaller budget would derive: its
    // admitted leaf size is below the 100-edge raw leaf, so the raw pass
    // must refine it.
    let sized = sized_plan(8192, 4096);
    let limit = sized.gate_bytes / (2 * sized.concurrency as u64) / sized.edge_row_bytes;
    assert!(limit < 100, "{limit}");
    let (nodes, edges) = fixture(40, 100);
    let reader = Arc::new(Fixed::new(edges.clone(), 1));
    let sources = [edge_source(reader, &edges, 1)];
    let (_, scattered) = build(&scratch, &sized, &nodes, &sources).unwrap();
    assert_eq!(scattered.total, 100);
    // The raw leaf refined into several ordered leaves, none over the
    // admitted size; the fixture pins the input, not which leaves come out.
    let counts = scattered.counts.clone();
    assert!(counts.len() >= 2, "{counts:?}");
    assert_eq!(counts.iter().sum::<u64>(), 100);
    assert!(counts.iter().all(|count| *count <= limit), "{counts:?}");
    assert!(scattered.refinement_steps > 0);
    assert!(scattered.refinement_read_bytes > 0);
    assert!(scattered.refinement_write_bytes > 0);
    // No reference or probe writer ever existed: the only live scratch is
    // the node leaf and the leaves the refinement kept (`edge-*` names the
    // refinement's own outputs).
    assert!(!has_reference_files(&scratch));
    for name in scratch_names(&scratch) {
        assert!(
            name.starts_with("nodes-") || name.starts_with("edge"),
            "{name}"
        );
    }
    // The tracked occupancy is logical reserved bytes, headers included —
    // not the filesystem's allocated volume. At rest it is exactly the node
    // leaf plus the leaves the refinement kept: at this gate the refinement
    // stages one record per block, so every record it kept occupies one
    // 28-byte record plus one 8-byte header.
    let node_bytes = 8 + 20 * 40;
    let leaf_bytes: u64 = counts.iter().map(|count| count * (8 + 28)).sum();
    assert_eq!(scratch.occupied_bytes(), node_bytes + leaf_bytes);
    // The peak is the parent-and-children overlap of the one radix step:
    // the raw parent was still live once every child had been written (one
    // record per staged block at this gate), and nothing else — no
    // reference or probe — ever entered the tree.
    let parent_bytes = 8 + 28 * 100;
    let child_bytes = (8 + 28) * 100;
    assert_eq!(
        scratch.peak_occupied_bytes(),
        node_bytes + parent_bytes + child_bytes
    );
    scratch.remove().unwrap();
}

/// A reader that arms the token just after the final sink call of a replayed
/// task returned: the reader owes nothing more, so only a check after
/// `read_task` can stop the task before it completes and publishes.
struct ArmingAfterFinalSink {
    fixed: Fixed,
    cancel: Arc<AtomicBool>,
}

impl BulkBatchReader for ArmingAfterFinalSink {
    fn task_rows(&self, task: usize) -> usize {
        self.fixed.task_rows(task)
    }

    fn read_task(
        &self,
        task: usize,
        sink: &mut dyn FnMut(RecordBatch) -> Result<(), GfError>,
    ) -> Result<(), GfError> {
        let replayed = self.fixed.read(task) > 0;
        for batch in self.fixed.stored(task) {
            sink(batch.clone())?;
        }
        if replayed {
            self.cancel.store(true, Ordering::SeqCst);
        }
        Ok(())
    }
}

#[test]
fn a_token_armed_after_the_final_sink_call_refuses_the_task_before_it_completes() {
    let root = tempfile::tempdir().unwrap();
    let directory = crate::graph_construction_encoding::StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    // Per-record staging, so every replayed row's references are in their
    // files by the time the final sink call returns and the token arms.
    let sized = plan(2);
    let (nodes, edges) = fixture(40, 100);
    let replay_cancel = Arc::new(AtomicBool::new(false));
    let reader = Arc::new(ArmingAfterFinalSink {
        fixed: Fixed::new(edges.clone(), 1),
        cancel: Arc::clone(&replay_cancel),
    });
    let sources = [edge_source(reader, &edges, 1)];
    let (scattered_nodes, _) = build(&scratch, &sized, &nodes, &sources).unwrap();
    let (refs, probes) = references(&scratch, scattered_nodes.leaves.len()).unwrap();
    let sink = RefSink {
        router: &scattered_nodes.router,
        refs: &refs,
        probes: &probes,
    };
    let decode = ByteGate::new(0);
    let error = replay_edges(
        &sources,
        GraphConstructionBudgets::default(),
        &decode,
        &sink,
        &sized,
        &scratch,
        &replay_cancel,
    )
    .unwrap_err();
    assert!(error.to_string().contains("cancelled"), "{error}");
    // The token armed only after every row had been handed over and written:
    // the task must still refuse before its writers finish or its proof
    // publishes, and nothing was resolved.
    assert_eq!(refs.counts().unwrap(), vec![200]);
    assert_eq!(probes.counts().unwrap(), vec![100]);
    assert!(has_reference_files(&scratch));
    assert!(!has_resolved_files(&scratch));
    scratch.remove().unwrap();
}

/// Arms the token the moment the replay's reference leaf holds `records`
/// records on disk. The replay of the single admitted batch is then still in
/// flight, so only a check inside the replay's row loop can observe the
/// token before the batch ends. An ordinary spin on the file's length: no
/// sleep, no retry, no product hook.
fn arm_once_references_reach(path: &std::path::Path, records: u64, cancel: &AtomicBool) {
    let target = 8 + records * REF_RECORD as u64;
    loop {
        let length = std::fs::metadata(path).map(|meta| meta.len()).unwrap_or(0);
        if length >= target {
            break;
        }
        std::thread::yield_now();
    }
    cancel.store(true, Ordering::SeqCst);
}

#[test]
fn a_token_armed_mid_batch_stops_the_replay_inside_the_batch() {
    let root = tempfile::tempdir().unwrap();
    let directory = crate::graph_construction_encoding::StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    // Per-record staging, so the reference leaf's length tracks the rows the
    // replay has processed. One batch of 60_000 rows stays within the
    // admission window, so the whole task is a single batch.
    let sized = plan(2);
    let (nodes, edges) = fixture(40, 60_000);
    let reader = Arc::new(Fixed::new(edges.clone(), 1));
    let sources = [edge_source(reader, &edges, 1)];
    let (scattered_nodes, scattered_edges) = build(&scratch, &sized, &nodes, &sources).unwrap();
    assert_eq!(scattered_edges.total, 60_000);
    let (refs, probes) = references(&scratch, scattered_nodes.leaves.len()).unwrap();
    let sink = RefSink {
        router: &scattered_nodes.router,
        refs: &refs,
        probes: &probes,
    };
    let decode = ByteGate::new(0);
    let mid_batch_cancel = Arc::new(AtomicBool::new(false));
    let watcher_path = refs.path(0).to_path_buf();
    let watcher_cancel = Arc::clone(&mid_batch_cancel);
    let watcher =
        std::thread::spawn(move || arm_once_references_reach(&watcher_path, 64, &watcher_cancel));
    let error = replay_edges(
        &sources,
        GraphConstructionBudgets::default(),
        &decode,
        &sink,
        &sized,
        &scratch,
        &mid_batch_cancel,
    )
    .unwrap_err();
    watcher.join().unwrap();
    assert!(error.to_string().contains("cancelled"), "{error}");
    // The batch stopped inside the row loop: the token armed while a few
    // dozen rows were written, and the bounded-row check refused the rest
    // of the batch's 120_000 references instead of replaying them all.
    let written = refs.counts().unwrap()[0];
    assert!(written > 0, "{written}");
    assert!(written < 60_000, "{written}");
    assert!(!has_resolved_files(&scratch));
    scratch.remove().unwrap();
}

fn edge_uuid_at(batch: &RecordBatch, row: usize) -> [u8; 16] {
    let uuids = batch
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    <[u8; 16]>::try_from(uuids.value(row)).unwrap()
}

/// The batch with row `row`'s edge UUID replaced, keeping every other column.
fn with_edge_uuid(batch: &RecordBatch, row: usize, value: [u8; 16]) -> RecordBatch {
    let uuids = (0..batch.num_rows())
        .map(|index| {
            if index == row {
                value
            } else {
                edge_uuid_at(batch, index)
            }
        })
        .collect::<Vec<_>>();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(fixed(&uuids)),
        batch.column(1).clone(),
        batch.column(2).clone(),
        batch.column(3).clone(),
    ];
    RecordBatch::try_new(batch.schema(), columns).unwrap()
}

#[test]
fn a_mismatched_replay_of_a_duplicate_raw_leaf_refuses_the_duplicate_first() {
    let root = tempfile::tempdir().unwrap();
    let directory = crate::graph_construction_encoding::StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    let sized = plan(4096);
    let (nodes, edges) = fixture(40, 100);
    // Two identical edge UUIDs inside the one in-budget raw leaf, and a
    // replay that changes an endpoint: the mismatch is terminal either way,
    // but the duplicate is the refusal the input deserves.
    let duplicated = with_edge_uuid(&edges[0], 9, edge_uuid_at(&edges[0], 3));
    let edges = vec![duplicated];
    let reader = Arc::new(Mutating::new(edges.clone(), 1));
    let sources = [edge_source(reader, &edges, 1)];
    let (scattered_nodes, scattered_edges) = build(&scratch, &sized, &nodes, &sources).unwrap();
    let (refs, probes) = references(&scratch, scattered_nodes.leaves.len()).unwrap();
    let sink = RefSink {
        router: &scattered_nodes.router,
        refs: &refs,
        probes: &probes,
    };
    let decode = ByteGate::new(0);
    let cancel = AtomicBool::new(false);
    let replayed = replay_edges(
        &sources,
        GraphConstructionBudgets::default(),
        &decode,
        &sink,
        &sized,
        &scratch,
        &cancel,
    )
    .unwrap();
    assert_ne!(replayed.proof, scattered_edges.topology_proof.unwrap());
    let raw_reads_before = scattered_edges.partitions.read_bytes();
    let error = scattered_edges
        .ensure_replay_matches(&scratch, &sized, replayed.proof, &cancel)
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("duplicate identity across construction runs (edge)"),
        "{error}"
    );
    assert!(
        !matches!(
            error,
            GfError::Api {
                code: ApiErrorCode::IdentityConflict,
                ..
            }
        ),
        "{error}"
    );
    // The mismatch path validated the accepted raw leaves with the bounded
    // ordered reads, and nothing was resolved or published.
    assert!(scattered_edges.partitions.read_bytes() > raw_reads_before);
    assert!(!has_resolved_files(&scratch));
    scratch.remove().unwrap();
}

#[test]
fn a_matching_replay_validates_no_raw_leaf_and_reads_the_source_once_per_pass() {
    let root = tempfile::tempdir().unwrap();
    let directory = crate::graph_construction_encoding::StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    let sized = plan(4096);
    let (nodes, edges) = fixture(40, 100);
    let reader = Arc::new(Fixed::new(edges.clone(), 1));
    let shared: Arc<dyn BulkBatchReader> = reader.clone();
    let sources = [edge_source(shared, &edges, 1)];
    let (scattered_nodes, scattered_edges) = build(&scratch, &sized, &nodes, &sources).unwrap();
    let accepted = scattered_edges.topology_proof.unwrap();
    let (refs, probes) = references(&scratch, scattered_nodes.leaves.len()).unwrap();
    let sink = RefSink {
        router: &scattered_nodes.router,
        refs: &refs,
        probes: &probes,
    };
    let decode = ByteGate::new(0);
    let cancel = AtomicBool::new(false);
    let replayed = replay_edges(
        &sources,
        GraphConstructionBudgets::default(),
        &decode,
        &sink,
        &sized,
        &scratch,
        &cancel,
    )
    .unwrap();
    assert_eq!(replayed.proof, accepted);
    // The matching path reads no raw leaf at all.
    let raw_reads_before = scattered_edges.partitions.read_bytes();
    scattered_edges
        .ensure_replay_matches(&scratch, &sized, replayed.proof, &cancel)
        .unwrap();
    assert_eq!(scattered_edges.partitions.read_bytes(), raw_reads_before);
    // Each edge task was read exactly twice: once by the raw pass, once by
    // the reference pass.
    assert_eq!(reader.reads[0].load(Ordering::SeqCst), 2);
    scratch.remove().unwrap();
}

// ----------------------------------------------------
// The same phase order through the real session encoder: the full bulk
// build on the node-scratch route, not the passes in isolation.

/// A real session whose bulk plan's budget puts the node tables on scratch,
/// so `prepare_bulk_encoding` runs the deferred route end to end.
fn session(root: &std::path::Path) -> GraphConstructionSession {
    GraphConstructionSession::open_with_mode(
        root,
        Uuid::from_u128(0x5f4d_9c31_a20b_4e77_9d10_33c8_41ab_6e52),
        0,
        graphforge_core::OntologyMode::Exploratory,
        GraphConstructionBudgets::default(),
    )
    .unwrap()
}

fn session_plan(
    node_batches: &[RecordBatch],
    edge_reader: Arc<dyn BulkBatchReader>,
    edge_batches: &[RecordBatch],
    per_task: usize,
) -> BulkBuildPlan<'static> {
    let mut plan = BulkBuildPlan {
        nodes: vec![node_source(node_batches)],
        edges: vec![edge_source(edge_reader, edge_batches, per_task)],
        memory_budget: None,
    };
    plan.memory_budget = Some(plan.scratch_floor_bytes() + 1);
    assert!(matches!(plan.route(), crate::BulkRoute::ScratchNodes));
    plan
}

#[test]
fn the_session_refuses_a_duplicate_raw_leaf_before_its_changed_replay() {
    let root = tempfile::tempdir().unwrap();
    let mut session = session(root.path());
    let (nodes, edges) = fixture(40, 100);
    // Two identical edge UUIDs inside the one in-budget raw leaf, and a
    // replay whose second read changes an endpoint: the established
    // duplicate-edge refusal must outrank the replay mismatch.
    let duplicated = with_edge_uuid(&edges[0], 9, edge_uuid_at(&edges[0], 3));
    let edges = vec![duplicated];
    let reader = Arc::new(Mutating::new(edges.clone(), 1));
    let plan = session_plan(&nodes, reader, &edges, 1);
    let error = session
        .prepare_bulk_encoding(1, &plan, || false)
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("duplicate identity across construction runs (edge)"),
        "{error}"
    );
    assert!(
        !matches!(
            error,
            GfError::Api {
                code: ApiErrorCode::IdentityConflict,
                ..
            }
        ),
        "{error}"
    );
    assert!(!session.publication_committed());
}

#[test]
fn a_changed_replay_cannot_publish_through_the_session() {
    let root = tempfile::tempdir().unwrap();
    let mut session = session(root.path());
    let (nodes, edges) = fixture(40, 100);
    let reader = Arc::new(Mutating::new(edges.clone(), 1));
    let plan = session_plan(&nodes, reader, &edges, 1);
    let error = session
        .prepare_bulk_encoding(1, &plan, || false)
        .unwrap_err();
    assert!(
        matches!(
            error,
            GfError::Api {
                code: ApiErrorCode::IdentityConflict,
                ..
            }
        ),
        "{error}"
    );
    assert!(error.to_string().contains("topology versions"), "{error}");
    assert!(!session.publication_committed());
}

#[test]
fn crc_valid_raw_edge_block_with_a_partial_record_is_refused() {
    let root = tempfile::tempdir().unwrap();
    let directory = crate::graph_construction_encoding::StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    let partitions = Partitions::create(&scratch, "edges", 1, EDGE_RECORD).unwrap();
    let record = EdgeRecord {
        uuid: uuid(0x20, 1),
        src: 1,
        dst: 2,
        rel: 0,
    };
    let mut block = vec![0; 8];
    block.extend_from_slice(&record.encode());
    block.push(0xff);
    // append supplies a valid CRC: the record shape, rather than corruption
    // of the transport checksum, must be rejected by the actual reader.
    partitions.append(&scratch, 0, &mut block).unwrap();
    let scattered = ScatteredEdges {
        partitions,
        lows: vec![Some(record.uuid)],
        refinement_write_bytes: 0,
        refinement_read_bytes: 0,
        refinement_steps: 0,
        counts: vec![1],
        rel_names: vec!["KNOWS".to_owned()],
        histogram: None,
        total: 1,
        topology_proof: None,
    };
    let error = match scattered.load_sorted(&scratch, 0, None) {
        Err(error) => error,
        Ok(_) => panic!("a CRC-valid partial edge record was silently accepted"),
    };
    assert!(error.to_string().contains("partial edge record"), "{error}");
    assert!(
        scattered.partitions.path(0).exists(),
        "invalid input stays owned until teardown"
    );
}
