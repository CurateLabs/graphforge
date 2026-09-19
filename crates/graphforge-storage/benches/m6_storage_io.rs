//! Durable M6 filesystem paths for CodSpeed walltime (#782), plus the
//! continuous bulk-ingest throughput benchmark and its floor gate (#1387).
//!
//! Fixture construction happens through `with_inputs`, outside the measured
//! closure. Every sample owns a private temporary project root.
//!
//! Setting `GF_INGEST_FLOOR_GATE` runs the ingest floor gate instead of the
//! divan benchmarks; see [`ingest_floor_gate`].

use arrow::array::{FixedSizeBinaryArray, RecordBatch, StringArray};
use divan::Bencher;
use graphforge_core::OntologyMode;
use graphforge_storage::{
    CONSTRUCTION_EDGE_SCHEMA, CONSTRUCTION_NODE_SCHEMA, ConstructionChunkKind,
    ConstructionPhaseAttribution, GRAPH_CAPABILITY_ID, GRAPH_CAPABILITY_VERSION,
    GraphConstructionBudgets, GraphConstructionSession, GraphDeltaCompactionLimits,
    GraphDeltaCompactionRequest, GraphDeltaJournalLimits, GraphDeltaOp, GraphDeltaOpKind,
    GraphDeltaPayload, GraphDeltaPublishRequest, GraphWriter, ProjectCapability,
    ProjectGenerationRequest, ProjectRetentionLimits, ProjectRetentionPolicy, ProjectStageOutcome,
    capture_graph_files, compact_graph_delta, empty_workspace_participants,
    execute_project_cleanup, inspect_project_reachability, open_or_initialize_project,
    publish_graph_delta, recover_project_on_open, resolve_project_generation,
    stage_project_generation_with_graph_tree,
};
use uuid::Uuid;

mod ingest_gate;

use ingest_gate::{
    GateLimits, GateVerdict, INGEST_RATCHET_MARGIN_BYTES_READ_PER_EDGE,
    INGEST_RATCHET_MARGIN_CPU_MICROS_PER_EDGE, INGEST_RATCHET_MARGIN_READ_DEGRADATION_RATIO,
    IngestObservation, METRIC_DESCRIPTORS, MetricDescriptor, RatchetPolicy,
    THROUGHPUT_RATCHET_EXCLUSION, evaluate_ingest_gate, limits_report,
};

fn main() {
    if std::env::var_os(INGEST_GATE_ENV).is_some() {
        ingest_floor_gate();
        return;
    }
    divan::main();
}

fn prepared_publication() -> (
    tempfile::TempDir,
    tempfile::TempDir,
    ProjectGenerationRequest,
) {
    let root = tempfile::tempdir().unwrap();
    open_or_initialize_project(root.path()).unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let mut writer = GraphWriter::open_at(workspace.path(), OntologyMode::Strict, 1).unwrap();
    writer
        .create_node(
            Uuid::from_u128(1),
            graphforge_value::EntityTypeId::decode(1).unwrap(),
        )
        .unwrap();
    writer.flush().unwrap();
    let (_, files) = capture_graph_files(workspace.path()).unwrap();
    let mut participants = empty_workspace_participants().unwrap();
    participants.insert(0, files);
    let request = ProjectGenerationRequest {
        transaction_uuid: Uuid::now_v7(),
        generation_uuid: Uuid::now_v7(),
        capabilities: vec![
            ProjectCapability {
                capability_id: GRAPH_CAPABILITY_ID.into(),
                capability_version: GRAPH_CAPABILITY_VERSION,
            },
            ProjectCapability {
                capability_id: "workspace".into(),
                capability_version: 1,
            },
        ],
        participants,
    };
    (root, workspace, request)
}

fn publish_base(root: &std::path::Path) {
    let (_, workspace, request) = prepared_publication();
    let ProjectStageOutcome::Staged(staged) =
        stage_project_generation_with_graph_tree(root, &request, Some(workspace.path())).unwrap()
    else {
        panic!("fresh publication replayed")
    };
    staged
        .validate(|_| Ok(()), |_, _| Ok(()))
        .unwrap()
        .publish()
        .unwrap();
}

fn publish_delta(root: &std::path::Path) {
    publish_graph_delta(
        root,
        &GraphDeltaPublishRequest {
            transaction_uuid: Uuid::now_v7(),
            generation_uuid: Uuid::now_v7(),
            run_uuid: Uuid::now_v7(),
            operations: vec![GraphDeltaOp {
                operation_uuid: Uuid::now_v7(),
                kind: GraphDeltaOpKind::SetNodeProperty,
                payload: GraphDeltaPayload::SetNodeProperty {
                    node_uuid: Uuid::from_u128(1).to_string(),
                    property_stem: "1".into(),
                    key: "rank".into(),
                    value: graphforge_storage::encode_graph_delta_value(
                        &graphforge_ir::IrLiteral::Int(7),
                    )
                    .unwrap(),
                },
            }],
            limits: GraphDeltaJournalLimits::default(),
        },
    )
    .unwrap();
}

fn seed_generation_chain(root: &std::path::Path, delta_count: usize) {
    publish_base(root);
    for _ in 0..delta_count {
        publish_delta(root);
    }
}

#[divan::bench]
fn durable_commit(bencher: Bencher) {
    bencher
        .with_inputs(prepared_publication)
        .bench_local_refs(|(root, workspace, request)| {
            let ProjectStageOutcome::Staged(staged) = stage_project_generation_with_graph_tree(
                root.path(),
                request,
                Some(workspace.path()),
            )
            .unwrap() else {
                panic!("fresh publication replayed")
            };
            staged
                .validate(|_| Ok(()), |_, _| Ok(()))
                .unwrap()
                .publish()
                .unwrap()
        });
}

#[divan::bench]
fn durable_open(bencher: Bencher) {
    bencher
        .with_inputs(|| {
            let root = tempfile::tempdir().unwrap();
            open_or_initialize_project(root.path()).unwrap();
            seed_generation_chain(root.path(), 1);
            root
        })
        .bench_local_refs(|root| resolve_project_generation(root.path()).unwrap());
}

#[divan::bench]
fn recovery_scan(bencher: Bencher) {
    bencher
        .with_inputs(|| {
            let root = tempfile::tempdir().unwrap();
            open_or_initialize_project(root.path()).unwrap();
            seed_generation_chain(root.path(), 1);
            root
        })
        .bench_local_refs(|root| recover_project_on_open(root.path()).unwrap());
}

#[divan::bench]
fn reachability_scan(bencher: Bencher) {
    bencher
        .with_inputs(|| {
            let root = tempfile::tempdir().unwrap();
            open_or_initialize_project(root.path()).unwrap();
            seed_generation_chain(root.path(), 5);
            root
        })
        .bench_local_refs(|root| {
            inspect_project_reachability(
                root.path(),
                ProjectRetentionPolicy::default(),
                ProjectRetentionLimits::default(),
            )
            .unwrap()
        });
}

#[divan::bench]
fn garbage_collection(bencher: Bencher) {
    bencher
        .with_inputs(|| {
            let root = tempfile::tempdir().unwrap();
            open_or_initialize_project(root.path()).unwrap();
            seed_generation_chain(root.path(), 5);
            root
        })
        .bench_local_refs(|root| {
            execute_project_cleanup(
                root.path(),
                ProjectRetentionPolicy::default(),
                ProjectRetentionLimits::default(),
            )
            .unwrap()
        });
}

#[divan::bench]
fn spill_compaction(bencher: Bencher) {
    bencher
        .with_inputs(|| {
            let root = tempfile::tempdir().unwrap();
            open_or_initialize_project(root.path()).unwrap();
            publish_base(root.path());
            publish_delta(root.path());
            root
        })
        .bench_local_refs(|root| {
            let limits = GraphDeltaCompactionLimits::default();
            compact_graph_delta(
                root.path(),
                &GraphDeltaCompactionRequest {
                    transaction_uuid: Uuid::now_v7(),
                    generation_uuid: Uuid::now_v7(),
                    through_run_sequence: None,
                    limits,
                    cleanup_after_commit: false,
                    cleanup_policy: ProjectRetentionPolicy::default(),
                    cleanup_limits: ProjectRetentionLimits::default(),
                },
                None,
            )
            .unwrap()
        });
}

// ---------------------------------------------------------------------------
// Continuous bulk-ingest throughput (#1387 workstream 6)
// ---------------------------------------------------------------------------
//
// `durable_open`, `durable_commit`, `recovery_scan`, `reachability_scan`,
// `garbage_collection` and `spill_compaction` all measure control-plane work on
// a graph of one node. None of them ingests in bulk, so nothing in the project
// detected that ingest throughput *falls as the graph grows*: 79,431 edges/sec
// at 8.4M edges down to 72,989 edges/sec at 67.1M edges on `fa6447cc`.
//
// A single-size benchmark cannot see that shape, so `ingest_throughput` runs
// the same generation-publishing path at two sizes a factor of sixteen apart
// and `ingest_floor_gate` (below) checks the *ratio* between them as well as an
// absolute floor at each size. `ingest_identifier_density` sweeps a structural
// axis at fixed edge count, because a pure size sweep blurs cardinality
// collapses (GreptimeDB measured 4.6x on exactly that axis).
//
// Four of the epic's five per-edge quantities are measured here: edges per
// second, bytes read per edge, CPU microseconds per edge and effective core
// utilisation. The fifth, the serialized fraction of the ingest path, is not.
// It is a property of where time is spent inside the path rather than of the
// path's cost, so it needs a profiler or explicit in-path instrumentation, and
// neither divan nor the construction session's evidence can supply it. Core
// utilisation is the closest observable proxy this benchmark has for it and is
// reported for that reason, but it is a proxy and is not gated as if it were
// the measurement.

/// Rows in one staged Arrow chunk. Matches the scale ladder's
/// `CONSTRUCTION_BATCH_ROWS` so bench and ladder stage the same chunk shape.
const INGEST_CHUNK_ROWS: usize = 65_536;

/// Relationship type used for every generated edge.
const INGEST_REL_TYPE: &str = "LINKS";

/// Node label used for every generated vertex.
const INGEST_NODE_LABEL: &str = "Vertex";

/// Graph500 `edgefactor`: edges per vertex in the size sweep. Holding this
/// fixed is what makes the two size points comparable.
const INGEST_EDGE_FACTOR: u64 = 16;

/// The size sweep. Both rungs publish one generation from an empty parent.
///
/// The ratio between them is the reason this benchmark exists, so they are a
/// factor of 16 apart — the same span over which the ladder measured a 24%
/// throughput fall (S18 to S22). The upper rung is capped by the nightly
/// walltime budget, not by what would be most informative: 8,388,608 edges is
/// roughly 105 seconds of CPU, while the ladder's 67.1M-edge rung takes about
/// fifteen minutes and cannot run nightly. Below about half a million edges the
/// fixed cost of opening and publishing a generation dominates and the ratio
/// stops describing scaling at all, which is why the lower rung is not smaller.
/// Raise both rungs as the redesign makes larger ones affordable.
const INGEST_SWEEP_EDGES: [u64; 2] = [524_288, 8_388_608];

/// Structural axis: edges per vertex at a fixed edge count. Low fan-out means a
/// large, sparse identifier space; high fan-out means a small, dense one. Both
/// rungs stage exactly `INGEST_DENSITY_EDGES` edges, so any difference is
/// identifier-space density and not size.
const INGEST_DENSITY_FANOUT: [u64; 2] = [2, 256];

/// Edge count held constant across the density sweep.
const INGEST_DENSITY_EDGES: u64 = 131_072;

/// Throughput floor, in edges per second, enforced at *every* swept size.
///
/// This is deliberately **not** the epic's 1,000,000 edges/sec target. A gate
/// set at a value the code cannot meet is switched off within a week and then
/// protects nothing, and an unmet floor is not wording to retire: the 1M
/// acceptance gate lives in #1478 and stays separate from this interim
/// regression floor (#1476).
///
/// **Banked: 15,000 edges/sec.** Host class: shared development Linux x86_64
/// host carrying unrelated builds. Build profile: `cargo bench --release`
/// (divan, bench profile). Set by #1476, carrying forward the value the
/// pre-#1476 gate used unchanged; `git blame` this line for the exact banking
/// commit. Across repeated runs of this same workload that host measured
/// between 23,285 and 65,155 edges/sec purely on how much of a core the run
/// got, which is why this constant stays coarse.
///
/// **One-sided by policy (#1476).** Wall-clock throughput is the only metric
/// here that a busy host can depress without any code change (±48% observed),
/// so unlike the other three gates this one does **not** fail when the
/// measurement improves; an unbanked gain is reported as a note instead. The
/// exclusion lifts in the pull request that banks this floor's baseline from
/// the isolated `codspeed-macro` runner the nightly already runs on — until
/// then this limit is still ratcheted by hand exactly as before: raise it in
/// the same pull request that wins the gain, or the gain is unprotected.
const INGEST_FLOOR_EDGES_PER_SECOND: f64 = 15_000.0;

/// Ceiling on bytes read per published edge, enforced at every swept size.
///
/// Bytes read per edge is a first-class metric here because it is the number
/// that proved the problem is a constant overhead rather than a scaling curve:
/// at the 67.1M-edge reference scale, 4,579 bytes read for authentication
/// against 265 bytes retained, a factor of 17.3. This benchmark measures 2,139
/// at 524,288 edges and 2,412 at 8,388,608, so the same overhead is already
/// visible an order of magnitude smaller.
///
/// **Banked: 2,500 bytes/edge**, lowered from 3,000 by #1476. Host class:
/// shared development Linux x86_64 host. Build profile: `cargo bench
/// --release` (divan, bench profile). Derived from this benchmark's own
/// matched workload — repeated runs reproduced 2,138.937 bytes/edge at
/// 524,288 edges and 2,412.016 at 8,388,608 **to the byte** — with 3.6%
/// regression headroom over the observed worst. The 3,000 it replaces was a
/// ladder-era figure carried over from a different measurement scope; #1476
/// requires constants to be calibrated from the gate's own workload. `git
/// blame` this line for the exact banking commit.
///
/// **Two-sided (#1476).** Measured above this ceiling is a regression.
/// Measured more than [`INGEST_RATCHET_MARGIN_BYTES_READ_PER_EDGE`] — 10%,
/// for a metric that reproduces to the byte — below it is an **unbanked
/// gain**: the gate fails and prints the constant to write. Workstream 1
/// (#1384) targets an order of magnitude off it; that pull request lowers
/// this ceiling, and the ratchet side now holds it to that.
///
/// Scope: application-observed **device** read bytes summed over every
/// construction phase of one complete generation publication from an empty
/// parent, divided by published edges. These are storage-layer attribution
/// counters, not logical record sizes and not harness or whole-rung traffic;
/// do not transfer them across rung / ingest / validate scopes (#1476).
const INGEST_CEILING_BYTES_READ_PER_EDGE: f64 = 2_500.0;

/// Ceiling on process CPU microseconds per published edge, enforced at every
/// swept size.
///
/// CPU consumed per edge barely moves with host contention, so unlike wall
/// clock it means roughly the same thing on a loaded developer machine and on
/// an isolated runner.
///
/// **Banked: 14.0 µs/edge**, lowered from 15.0 by #1476. Host class: shared
/// development Linux x86_64 host. Build profile: `cargo bench --release`
/// (divan, bench profile). Derived from this benchmark's matched workload:
/// repeated runs of the same commit measured 9.84–13.82 µs/edge, so the
/// ceiling sits 1.3% over the observed worst — replacing the 15.14 anchor the
/// 15.0 was set under, which came from the epic's ladder measurement rather
/// than this benchmark (#1476). Because the metric still moves ±15% under
/// load, the ratchet margin below it is deliberately the widest here (25%).
/// `git blame` this line for the exact banking commit.
///
/// The #1387 acceptance criterion is under 9.0; ratchet this down as the
/// redesign lands — the ratchet side now enforces that a winning pull request
/// writes the new constant instead of leaving the gain unbanked.
///
/// It is the one limit here that is not architecture-independent, and the
/// nightly runs on ARM64. If the first isolated run reports outside the
/// observed band, the correct response is to re-baseline this constant
/// against that measurement in a follow-up, not to remove the gate — a first
/// failure that hands us the runner's real baseline is the gate working.
const INGEST_CEILING_CPU_MICROS_PER_EDGE: f64 = 14.0;

/// Maximum tolerated growth in bytes read per edge from the smallest swept size
/// to the largest.
///
/// This is the ratio requirement, and it is the reason a single-size benchmark
/// would not do: a flat healthy number at one size can sit on top of a cost
/// that climbs with the graph.
///
/// It is evaluated on bytes read per edge rather than on edges per second
/// because that quantity is *deterministic*. Repeated runs of this gate on a
/// host under heavy load reproduced 2,138.937 and 2,412.016 bytes per edge to
/// the byte while wall-clock throughput moved by 48% and CPU per edge by 15%.
/// A ratio gate has to survive a noisy host without being loosened into
/// uselessness, and only this one does. It expresses the same defect: the
/// ladder's throughput fall across 8.4M to 67.1M edges is read amplification
/// that grows with merge fan-in.
///
/// Measured 1.128 across this sixteen-fold span, reproduced to the byte on four
/// separate runs. Failing above 1.20 forbids a redesign from buying a better
/// absolute number with a steeper curve.
///
/// **Two-sided (#1476).** Measured above this limit is a regression. Measured
/// more than [`INGEST_RATCHET_MARGIN_READ_DEGRADATION_RATIO`] — 10%, same
/// rationale as the absolute read-byte ceiling — below it is an **unbanked
/// gain**: the gate fails and prints the constant to write, so a redesign
/// that flattens the curve must bank the flatter limit in the same pull
/// request.
///
/// This limit and the read-byte ceiling above are the two that hold the line
/// today: both are deterministic and independent of host, load and
/// architecture, which the throughput floor and the CPU ceiling are not.
///
/// Scope: the same application-observed device read bytes as
/// [`INGEST_CEILING_BYTES_READ_PER_EDGE`], taken as the ratio of per-edge
/// means between the largest and smallest swept size of this benchmark's
/// sixteen-fold sweep; not transferable to other rungs, workloads or byte
/// scopes (#1476).
const INGEST_MAX_READ_DEGRADATION_RATIO: f64 = 1.20;

/// Process CPU time (user + system) sampled at a point, where available.
#[cfg(unix)]
fn process_cpu_time() -> Option<std::time::Duration> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: `getrusage` writes a complete `rusage` on success and touches
    // nothing else; the pointer is a live, correctly aligned local.
    let status = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if status != 0 {
        return None;
    }
    // SAFETY: `getrusage` returned success, so the value is initialized.
    let usage = unsafe { usage.assume_init() };
    let micros = |value: libc::timeval| {
        std::time::Duration::new(
            u64::try_from(value.tv_sec).unwrap_or(0),
            u32::try_from(value.tv_usec)
                .unwrap_or(0)
                .saturating_mul(1_000),
        )
    };
    Some(micros(usage.ru_utime) + micros(usage.ru_stime))
}

/// Windows exposes no `getrusage`; the CPU columns report unavailable there.
#[cfg(not(unix))]
fn process_cpu_time() -> Option<std::time::Duration> {
    None
}

/// Deterministic SplitMix64 finalizer. Endpoints must scatter across the
/// identifier space — sequential endpoints would let every sorted structure on
/// the ingest path behave far better than it does on real input — and they must
/// be reproducible, so the same commit always benchmarks the same graph.
const fn ingest_scramble(state: u64) -> u64 {
    let mut value = state.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
}

/// UUIDv7-shaped identifier derived from a counter, matching the scale ladder.
fn ingest_uuid(seed: u128) -> Uuid {
    let mut bytes = seed.to_be_bytes();
    bytes[6] = (bytes[6] & 0x0f) | 0x70;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

fn ingest_node_batch(first: u64, count: usize) -> RecordBatch {
    let ids: Vec<Uuid> = (0..count)
        .map(|offset| ingest_uuid(u128::from(first + offset as u64) + 1))
        .collect();
    RecordBatch::try_new(
        CONSTRUCTION_NODE_SCHEMA.clone(),
        vec![
            std::sync::Arc::new(
                FixedSizeBinaryArray::try_from_iter(ids.iter().map(Uuid::as_bytes)).unwrap(),
            ),
            std::sync::Arc::new(StringArray::from(vec![INGEST_NODE_LABEL; count])),
        ],
    )
    .unwrap()
}

fn ingest_edge_batch(first: u64, count: usize, vertices: u64) -> RecordBatch {
    let mut edge_ids = Vec::with_capacity(count);
    let mut sources = Vec::with_capacity(count);
    let mut targets = Vec::with_capacity(count);
    for offset in 0..count as u64 {
        let ordinal = first + offset;
        edge_ids.push(ingest_uuid(0xE000_0000_0000_u128 + u128::from(ordinal) + 1));
        let source = ingest_scramble(ordinal) % vertices;
        let target = ingest_scramble(ordinal ^ 0x5555_5555_5555_5555) % vertices;
        sources.push(ingest_uuid(u128::from(source) + 1));
        targets.push(ingest_uuid(u128::from(target) + 1));
    }
    RecordBatch::try_new(
        CONSTRUCTION_EDGE_SCHEMA.clone(),
        vec![
            std::sync::Arc::new(
                FixedSizeBinaryArray::try_from_iter(edge_ids.iter().map(Uuid::as_bytes)).unwrap(),
            ),
            std::sync::Arc::new(StringArray::from(vec![INGEST_REL_TYPE; count])),
            std::sync::Arc::new(
                FixedSizeBinaryArray::try_from_iter(sources.iter().map(Uuid::as_bytes)).unwrap(),
            ),
            std::sync::Arc::new(
                FixedSizeBinaryArray::try_from_iter(targets.iter().map(Uuid::as_bytes)).unwrap(),
            ),
        ],
    )
    .unwrap()
}

/// Stage and publish one generation of `edges` edges over `edges / fan_out`
/// vertices, and report what it cost.
///
/// Chunk encoding runs inside the measured region on purpose: the scale
/// ladder's `append` phase builds its Arrow batches inline too, and encoding is
/// precisely the work #1387 intends to move out of the serialized region.
/// Measuring it outside would hide the gain.
fn ingest_generation(root: &std::path::Path, edges: u64, fan_out: u64) -> IngestObservation {
    let vertices = (edges / fan_out).max(1);
    let cpu_before = process_cpu_time();
    let started = std::time::Instant::now();

    let mut session = GraphConstructionSession::open_with_mode(
        root,
        Uuid::now_v7(),
        0,
        OntologyMode::Exploratory,
        GraphConstructionBudgets::default(),
    )
    .unwrap();

    let mut staged = 0u64;
    while staged < vertices {
        let rows = usize::try_from((vertices - staged).min(INGEST_CHUNK_ROWS as u64)).unwrap();
        session
            .append(
                ConstructionChunkKind::Node,
                &format!("nodes-{staged:016x}"),
                &ingest_node_batch(staged, rows),
            )
            .unwrap();
        staged += rows as u64;
    }

    let mut staged = 0u64;
    while staged < edges {
        let rows = usize::try_from((edges - staged).min(INGEST_CHUNK_ROWS as u64)).unwrap();
        session
            .append(
                ConstructionChunkKind::Edge,
                &format!("edges-{staged:016x}"),
                &ingest_edge_batch(staged, rows, vertices),
            )
            .unwrap();
        staged += rows as u64;
    }

    session.seal().unwrap();
    let encoding = session.prepare_canonical_encoding(1).unwrap();
    session
        .publish_canonical(&encoding, Uuid::now_v7(), Uuid::now_v7())
        .unwrap();

    let wall = started.elapsed();
    let cpu = cpu_before
        .zip(process_cpu_time())
        .map(|(before, after)| after.saturating_sub(before));
    let attribution = ConstructionPhaseAttribution::from_construction(session.evidence()).unwrap();
    IngestObservation {
        edges,
        vertices,
        wall,
        cpu,
        read_bytes: attribution.totals.read_bytes,
        write_bytes: attribution.totals.write_bytes,
        transient_peak_bytes: session
            .evidence()
            .storage_transient_peak_total_allocated_bytes,
    }
}

/// Fresh durable project root. Creating it is fixture work, so it stays outside
/// the timed region; the returned `TempDir` is torn down outside it too.
fn ingest_project_root() -> tempfile::TempDir {
    let root = tempfile::tempdir().unwrap();
    open_or_initialize_project(root.path()).unwrap();
    root
}

/// Bulk ingest across a sixteen-fold size range. Tracking both points is the
/// whole design: one fixed size would report a flat, healthy number while
/// throughput degraded underneath it.
#[divan::bench(args = INGEST_SWEEP_EDGES, sample_count = 1, sample_size = 1)]
fn ingest_throughput(bencher: Bencher, edges: u64) {
    bencher
        .with_inputs(ingest_project_root)
        .bench_local_refs(|root| ingest_generation(root.path(), edges, INGEST_EDGE_FACTOR));
}

/// Identifier-space density at a fixed edge count: the structural axis a pure
/// size sweep blurs.
#[divan::bench(args = INGEST_DENSITY_FANOUT, sample_count = 1, sample_size = 1)]
fn ingest_identifier_density(bencher: Bencher, fan_out: u64) {
    bencher
        .with_inputs(ingest_project_root)
        .bench_local_refs(|root| ingest_generation(root.path(), INGEST_DENSITY_EDGES, fan_out));
}

/// Environment switch that turns this binary into the ingest floor gate.
const INGEST_GATE_ENV: &str = "GF_INGEST_FLOOR_GATE";

/// Optional path for the machine-readable gate report.
const INGEST_GATE_JSON_ENV: &str = "GF_INGEST_FLOOR_GATE_JSON";

/// Measure the size sweep once and fail closed on any breach in **either**
/// direction.
///
/// CodSpeed compares each nightly run against the previous one, which catches a
/// step regression and nothing else: a benchmark that only compares against the
/// previous run passes indefinitely through a slow drift that never regresses
/// in a single step. This gate is the other half. Judgment lives in
/// `ingest_gate::evaluate_ingest_gate`; it fails when
///
/// - throughput at **any** swept size falls below
///   [`INGEST_FLOOR_EDGES_PER_SECOND`] (regression side),
/// - bytes read per edge at any size exceeds
///   [`INGEST_CEILING_BYTES_READ_PER_EDGE`] (regression side),
/// - CPU per edge at any size exceeds
///   [`INGEST_CEILING_CPU_MICROS_PER_EDGE`] (regression side),
/// - bytes read per edge grows from the smallest swept size to the largest by
///   more than [`INGEST_MAX_READ_DEGRADATION_RATIO`] (regression side), **or**
/// - any of the three contention-independent limits is beaten by more than
///   that metric's ratchet margin — an **unbanked gain** (#1476). The failure
///   prints the exact constant to write, and the gate stays red until the
///   pull request that won the gain records it. Wall-clock throughput is
///   excluded from this side (see [`INGEST_FLOOR_EDGES_PER_SECOND`]).
///
/// A clean pass is only trusted because `tests/ingest_gate_verdict.rs` proves
/// a deliberate regression and a deliberate improvement each fail the gate in
/// the expected direction.
///
/// Set `GF_INGEST_FLOOR_GATE` to run it and `GF_INGEST_FLOOR_GATE_JSON` to also
/// write the measurements and the verdict as JSON.
#[allow(
    clippy::too_many_lines,
    reason = "one linear gate: measure, report, judge"
)]
fn ingest_floor_gate() {
    let rows: Vec<IngestObservation> = INGEST_SWEEP_EDGES
        .into_iter()
        .map(|edges| {
            let root = ingest_project_root();
            ingest_generation(root.path(), edges, INGEST_EDGE_FACTOR)
        })
        .collect();

    println!("ingest floor gate (#1387 workstream 6)");
    print_metric_descriptors();
    println!(
        "{:>10}  {:>9}  {:>8}  {:>10}  {:>11}  {:>12}  {:>11}  {:>6}  {:>10}",
        "edges",
        "vertices",
        "wall_s",
        "edges/sec",
        "read_B/edge",
        "write_B/edge",
        "cpu_us/edge",
        "cores",
        "peak_MiB",
    );
    for row in &rows {
        println!(
            "{:>10}  {:>9}  {:>8.3}  {:>10.0}  {:>11.0}  {:>12.0}  {:>11}  {:>6}  {:>10.1}",
            row.edges,
            row.vertices,
            row.wall.as_secs_f64(),
            row.edges_per_second(),
            row.bytes_read_per_edge(),
            row.bytes_written_per_edge(),
            optional(row.cpu_micros_per_edge(), 2),
            optional(row.effective_cores(), 2),
            mebibytes(row.transient_peak_bytes),
        );
    }

    let first = rows.first().expect("swept at least one size");
    let last = rows.last().expect("swept at least one size");
    let span = last.edges / first.edges;
    let throughput_ratio = first.edges_per_second() / last.edges_per_second();
    let cpu_ratio = first
        .cpu_micros_per_edge()
        .zip(last.cpu_micros_per_edge())
        .map(|(small, large)| large / small);
    let limits = gate_limits();
    let verdict = evaluate_ingest_gate(&rows, &limits);
    println!(
        "{span}x size span: bytes read per edge {:.3}x \
         (limit {INGEST_MAX_READ_DEGRADATION_RATIO:.2}x), throughput {throughput_ratio:.3}x, \
         cpu per edge {}x",
        verdict.read_degradation_ratio,
        optional(cpu_ratio, 3),
    );

    if let Some(path) = std::env::var_os(INGEST_GATE_JSON_ENV) {
        write_gate_report(
            std::path::Path::new(&path),
            &rows,
            &limits,
            &verdict,
            throughput_ratio,
            cpu_ratio,
        );
    }

    for note in &verdict.notes {
        println!("ingest floor gate: {note}");
    }
    if verdict.must_fail() {
        for breach in &verdict.breaches {
            eprintln!("ingest floor gate: {breach}");
        }
        for breach in &verdict.ratchet_breaches {
            eprintln!("ingest floor gate: {breach}");
        }
        std::process::exit(1);
    }
    println!("ingest floor gate: pass");
}

/// The banked constants of this bench, wired to their per-metric ratchet
/// policies. The constants stay in this file (frozen there by
/// `scripts/ci/check-m6-benchmarks.py`); the judgment is shared and tested.
fn gate_limits() -> GateLimits {
    GateLimits {
        floor_edges_per_second: INGEST_FLOOR_EDGES_PER_SECOND,
        ceiling_bytes_read_per_edge: INGEST_CEILING_BYTES_READ_PER_EDGE,
        ceiling_cpu_micros_per_edge: INGEST_CEILING_CPU_MICROS_PER_EDGE,
        max_read_degradation_ratio: INGEST_MAX_READ_DEGRADATION_RATIO,
        ratchet_edges_per_second: RatchetPolicy::Excluded {
            reason: THROUGHPUT_RATCHET_EXCLUSION,
        },
        ratchet_bytes_read_per_edge: RatchetPolicy::Margin(
            INGEST_RATCHET_MARGIN_BYTES_READ_PER_EDGE,
        ),
        ratchet_cpu_micros_per_edge: RatchetPolicy::Margin(
            INGEST_RATCHET_MARGIN_CPU_MICROS_PER_EDGE,
        ),
        ratchet_read_degradation_ratio: RatchetPolicy::Margin(
            INGEST_RATCHET_MARGIN_READ_DEGRADATION_RATIO,
        ),
    }
}

/// Print each gated metric's execution scope, denominator and units (#1476):
/// a number measured in one scope must never be silently transferred into
/// another.
fn print_metric_descriptors() {
    for MetricDescriptor {
        metric,
        execution_scope,
        denominator,
        units,
    } in METRIC_DESCRIPTORS
    {
        println!("  {metric}: {execution_scope}; denominator: {denominator}; units: {units}");
    }
}

/// Render a metric the platform may not be able to report, without inventing a
/// value for it.
fn optional(value: Option<f64>, precision: usize) -> String {
    value.map_or_else(|| "n/a".to_owned(), |value| format!("{value:.precision$}"))
}

fn mebibytes(bytes: u64) -> f64 {
    #[allow(clippy::cast_precision_loss, reason = "reporting-only ratio")]
    let bytes = bytes as f64;
    bytes / 1_048_576.0
}

/// Serialize the gate's measurements, limits and verdict so a nightly run can
/// keep them as an artifact and the ratchet can be argued from recorded
/// numbers. Schema /2 adds the ratchet policies, per-metric scopes and the
/// breach lists (#1476); the /1 shape carried measurements only.
fn write_gate_report(
    path: &std::path::Path,
    rows: &[IngestObservation],
    limits: &GateLimits,
    verdict: &GateVerdict,
    throughput_ratio: f64,
    cpu_ratio: Option<f64>,
) {
    let report = serde_json::json!({
        "schema": "graphforge-ingest-floor-gate/2",
        "limits": limits_report(limits),
        "metric_scopes": METRIC_DESCRIPTORS
            .iter()
            .map(|descriptor| serde_json::json!({
                "metric": descriptor.metric,
                "execution_scope": descriptor.execution_scope,
                "denominator": descriptor.denominator,
                "units": descriptor.units,
            }))
            .collect::<Vec<_>>(),
        "read_degradation_ratio": verdict.read_degradation_ratio,
        "throughput_ratio": throughput_ratio,
        "cpu_degradation_ratio": cpu_ratio,
        "breaches": verdict.breaches,
        "ratchet_breaches": verdict.ratchet_breaches,
        "notes": verdict.notes,
        "measurements": rows
            .iter()
            .map(|row| {
                serde_json::json!({
                    "edges": row.edges,
                    "vertices": row.vertices,
                    "wall_seconds": row.wall.as_secs_f64(),
                    "edges_per_second": row.edges_per_second(),
                    "bytes_read_per_edge": row.bytes_read_per_edge(),
                    "bytes_written_per_edge": row.bytes_written_per_edge(),
                    "cpu_micros_per_edge": row.cpu_micros_per_edge(),
                    "effective_cores": row.effective_cores(),
                    "transient_peak_bytes": row.transient_peak_bytes,
                })
            })
            .collect::<Vec<_>>(),
    });
    std::fs::write(path, format!("{report}\n")).unwrap();
}
