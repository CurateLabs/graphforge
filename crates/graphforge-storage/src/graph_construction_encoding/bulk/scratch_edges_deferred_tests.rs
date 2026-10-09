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
use super::super::{BulkBatchReader, BulkSource};
use super::*;

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

/// A plan whose passes keep every fixture in one partition without
/// refinement.
fn plan(staging_bytes: usize) -> ScratchPlan {
    let mut sized = ScratchPlan::sized(1, 1, 1, 1 << 20, staging_bytes);
    sized.node_tables_on_scratch = true;
    sized.node_partitions = 1;
    sized.node_row_bytes = 40;
    sized
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

#[test]
fn raw_edge_refinement_leaves_no_reference_or_probe_files_and_their_bytes_out_of_the_occupancy() {
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
    let error = replay_refusal(scattered_edges.topology_proof, replayed.proof).unwrap_err();
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
