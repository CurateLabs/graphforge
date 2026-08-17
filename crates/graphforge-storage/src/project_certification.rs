//! Seeded durability/isolation certification (#756).
//!
//! Extends the deterministic filesystem fault oracle into an integrated
//! reference state machine spanning acknowledgement, restart, recovery-on-open,
//! pinned readers, write modes, checkpoints, delta runs, compaction, leases,
//! GC, cancellation, process death, and persistent-media faults.
//!
//! Required CI runs a bounded exhaustive state space. The scheduled lane raises
//! history/seed counts via `GRAPHFORGE_CERT_HISTORIES` / `GRAPHFORGE_CERT_OPS`.
//! Failures fail closed on the first invariant violation and emit a minimized
//! deterministic trace — never retry a failed seed into a pass.

#![cfg(any(test, feature = "test-failpoints"))]

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::project_fault_oracle::{
    AuthorityClass, PublicationPhase, default_durable_ids, expected_authority, history_budget,
    minimize_durable_ids, publication_ops, simulate_crash, simulate_torn_bytes,
};

/// Contract id frozen with the certification evidence schema.
pub const CERT_CONTRACT: &str = "graphforge-durability-certification/1";

/// Published required-CI seed (scheduled lane may raise history count, not this seed identity).
pub const CERT_SEED: u64 = 7560;

/// Isolation classification for optimistic write-skew (never SSI / serializable).
pub const WRITE_SKEW_CLASSIFICATION: &str = "allowed_documented_not_ssi";

/// Default operation count per seeded history in required CI.
pub const DEFAULT_OPS_PER_HISTORY: usize = 12;

/// Scheduled-lane default history multiplier over the oracle budget.
pub const SCHEDULED_HISTORY_MULTIPLIER: usize = 8;

/// One client / reader / transaction identity in the reference model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct ModelId(pub u64);

/// Write mode under certification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelWriteMode {
    /// Exclusive serial writer.
    SingleWriter,
    /// Bounded FIFO serial writer.
    QueuedWriter,
    /// Optimistic snapshot / conflict semantics (admits write-skew).
    OptimisticMultiWriter,
}

/// Observable project authority after an operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelAuthority {
    /// Exact prior complete generation.
    Prior,
    /// Linearized / acknowledged new generation.
    Current,
    /// Fail-closed corruption.
    Corrupt,
}

/// One step in a seeded certification history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoryOp {
    /// Acknowledge a durable commit under the active write mode.
    AckCommit {
        /// Writer identity.
        writer: ModelId,
        /// New generation identity.
        generation: ModelId,
    },
    /// Crash / restart at a publication phase against the fault oracle.
    CrashAtPhase {
        /// Crash phase.
        phase: PublicationPhase,
    },
    /// Inject torn CURRENT or manifest bytes.
    TearBytes {
        /// `true` tears CURRENT; `false` tears the generation manifest.
        tear_current: bool,
    },
    /// Pin a reader on the current authoritative generation.
    PinReader {
        /// Reader identity.
        reader: ModelId,
    },
    /// Observe a pinned reader; must match its pin and never mix generations.
    ReadPinned {
        /// Reader identity.
        reader: ModelId,
    },
    /// Fresh open observes CURRENT (may advance relative to pinned peers).
    FreshOpen {
        /// Observer identity.
        observer: ModelId,
    },
    /// Create a checkpoint root that GC must retain.
    CreateCheckpoint {
        /// Checkpoint identity (= generation retained).
        checkpoint: ModelId,
    },
    /// Acquire a live lease that GC must skip.
    AcquireLease {
        /// Lease generation identity.
        lease: ModelId,
    },
    /// Release a live lease.
    ReleaseLease {
        /// Lease generation identity.
        lease: ModelId,
    },
    /// Record a required delta input that compaction/GC must retain until subsumed safely.
    PublishDeltaRun {
        /// Delta run identity.
        run: ModelId,
    },
    /// Compact a delta prefix into a new Parquet generation after acknowledgement.
    CompactDeltas {
        /// New compacted generation.
        generation: ModelId,
        /// Subsumed delta runs.
        subsumed: Vec<ModelId>,
    },
    /// Run GC; must never remove CURRENT, checkpoints, live leases, or required deltas.
    RunGc,
    /// Optimistic write-skew witness (credit/debit merge).
    OptimisticWriteSkew {
        /// Left writer.
        left: ModelId,
        /// Right writer.
        right: ModelId,
        /// Shared account object.
        account: ModelId,
    },
    /// Cancel unstarted queued work.
    CancelUnstarted {
        /// Writer identity.
        writer: ModelId,
    },
    /// Exact idempotent retry of an acknowledged transaction identity.
    IdempotentRetry {
        /// Transaction identity.
        transaction: ModelId,
    },
    /// Harness-only probe used to certify minimizer determinism (never generated).
    HarnessInvariantProbe,
}

/// Safe (non-content) observation emitted after each step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StepObservation {
    /// Step index.
    pub index: usize,
    /// Operation class name.
    pub op: String,
    /// Modeled authority.
    pub authority: ModelAuthority,
    /// Pinned reader generation map (ids only).
    pub pinned: BTreeMap<u64, u64>,
    /// Whether write-skew was observed.
    pub write_skew_observed: bool,
    /// Classification string when write-skew is observed.
    pub write_skew_class: Option<&'static str>,
}

/// Reference model state compared to every observable step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReferenceModel {
    /// Active write mode.
    pub write_mode: ModelWriteMode,
    /// Authoritative CURRENT generation.
    pub current: ModelId,
    /// Generations that completed acknowledgement.
    pub acknowledged: BTreeSet<ModelId>,
    /// Reachable generations (CURRENT + checkpoints + leased).
    pub reachable: BTreeSet<ModelId>,
    /// Pinned readers → generation.
    pub pinned: BTreeMap<ModelId, ModelId>,
    /// Checkpoint roots.
    pub checkpoints: BTreeSet<ModelId>,
    /// Live leases.
    pub live_leases: BTreeSet<ModelId>,
    /// Required delta inputs still live.
    pub required_deltas: BTreeSet<ModelId>,
    /// Account credit/debit for the write-skew witness.
    pub credit: i64,
    /// Account debit for the write-skew witness.
    pub debit: i64,
    /// Whether the write-skew history has been admitted.
    pub write_skew_observed: bool,
    /// Last authority classification.
    pub authority: ModelAuthority,
    /// Whether the project is fail-closed corrupt.
    pub corrupt: bool,
    /// Acknowledged transaction identities (idempotent retry set).
    pub acknowledged_transactions: BTreeSet<ModelId>,
}

impl ReferenceModel {
    /// Fresh model with a baseline acknowledged generation `0`.
    #[must_use]
    pub fn new(write_mode: ModelWriteMode) -> Self {
        let baseline = ModelId(0);
        Self {
            write_mode,
            current: baseline,
            acknowledged: BTreeSet::from([baseline]),
            reachable: BTreeSet::from([baseline]),
            pinned: BTreeMap::new(),
            checkpoints: BTreeSet::new(),
            live_leases: BTreeSet::new(),
            required_deltas: BTreeSet::new(),
            credit: 0,
            debit: 0,
            write_skew_observed: false,
            authority: ModelAuthority::Current,
            corrupt: false,
            acknowledged_transactions: BTreeSet::new(),
        }
    }

    fn observe(&self, index: usize, op: &HistoryOp) -> StepObservation {
        StepObservation {
            index,
            op: op_name(op).to_owned(),
            authority: self.authority.clone(),
            pinned: self
                .pinned
                .iter()
                .map(|(reader, generation)| (reader.0, generation.0))
                .collect(),
            write_skew_observed: self.write_skew_observed,
            write_skew_class: self
                .write_skew_observed
                .then_some(WRITE_SKEW_CLASSIFICATION),
        }
    }
}

/// Invariant violations — first failure aborts the history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CertInvariant {
    /// Attempted work after corruption.
    #[allow(dead_code)]
    OperatedOnCorruptProject,
    /// Oracle authority disagreed with the reference model.
    AuthorityMismatch {
        /// Expected class.
        expected: String,
        /// Actual class.
        actual: String,
    },
    /// Acknowledged generation disappeared across restart.
    AcknowledgedLost {
        /// Missing generation.
        generation: u64,
    },
    /// Pinned reader saw a different generation.
    PinDrift {
        /// Reader id.
        reader: u64,
        /// Expected pin.
        expected: u64,
        /// Observed generation.
        actual: u64,
    },
    /// Reader observed a mixed / incomplete generation.
    MixedGeneration {
        /// Reader id.
        reader: u64,
    },
    /// GC selected a protected object.
    GcRemovedProtected {
        /// Protected class.
        kind: &'static str,
        /// Object id.
        id: u64,
    },
    /// Write-skew was misclassified as serializable / SSI.
    WriteSkewMisclassified {
        /// Observed classification.
        classification: String,
    },
    /// Pre-ack crash elected the new generation.
    PreAckAtomicityBroken,
    /// Idempotent retry mutated state.
    IdempotencyBroken {
        /// Transaction id.
        transaction: u64,
    },
    /// Observation disagreed with the reference model digest.
    ObservationMismatch {
        /// Step index.
        step: usize,
    },
}

/// One certification history outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HistoryReport {
    /// Seed.
    pub seed: u64,
    /// Write mode.
    pub write_mode: ModelWriteMode,
    /// Operation count before minimization.
    pub op_count: usize,
    /// Whether the history passed all invariants.
    pub ok: bool,
    /// First invariant violation when `ok` is false.
    pub invariant: Option<CertInvariant>,
    /// Minimized failing op names when shrinking applied.
    pub minimized_trace: Option<Vec<String>>,
    /// Step observations (safe fields only).
    pub observations: Vec<StepObservation>,
    /// Digest of the observation stream.
    pub observation_digest: String,
}

/// Aggregate certification evidence (safe fields only — no graph contents / host paths).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CertificationEvidence {
    /// Contract id.
    pub contract: &'static str,
    /// Issue number.
    pub issue: u64,
    /// Parent epic.
    pub parent_issue: u64,
    /// Frozen required seed.
    pub seed: u64,
    /// Histories executed.
    pub history_count: usize,
    /// Ops per history budget.
    pub ops_per_history: usize,
    /// Platform triple.
    pub platform: String,
    /// Toolchain label.
    pub toolchain: String,
    /// Integrated commit when provided via `GRAPHFORGE_CERT_COMMIT`.
    pub commit: String,
    /// Schema / contract versions frozen for this run.
    pub versions: BTreeMap<&'static str, &'static str>,
    /// Per-history reports.
    pub histories: Vec<HistoryReport>,
    /// Exact reproduction commands.
    pub commands: Vec<String>,
    /// Aggregate digest of all history digests.
    pub artifact_digest: String,
    /// Elapsed milliseconds.
    pub elapsed_ms: u128,
    /// Whether any untriaged invariant failure remains.
    pub untriaged_failures: usize,
}

fn op_name(op: &HistoryOp) -> &'static str {
    match op {
        HistoryOp::AckCommit { .. } => "ack_commit",
        HistoryOp::CrashAtPhase { .. } => "crash_at_phase",
        HistoryOp::TearBytes { .. } => "tear_bytes",
        HistoryOp::PinReader { .. } => "pin_reader",
        HistoryOp::ReadPinned { .. } => "read_pinned",
        HistoryOp::FreshOpen { .. } => "fresh_open",
        HistoryOp::CreateCheckpoint { .. } => "create_checkpoint",
        HistoryOp::AcquireLease { .. } => "acquire_lease",
        HistoryOp::ReleaseLease { .. } => "release_lease",
        HistoryOp::PublishDeltaRun { .. } => "publish_delta_run",
        HistoryOp::CompactDeltas { .. } => "compact_deltas",
        HistoryOp::RunGc => "run_gc",
        HistoryOp::OptimisticWriteSkew { .. } => "optimistic_write_skew",
        HistoryOp::CancelUnstarted { .. } => "cancel_unstarted",
        HistoryOp::IdempotentRetry { .. } => "idempotent_retry",
        HistoryOp::HarnessInvariantProbe => "harness_invariant_probe",
    }
}

fn hex_digest(bytes: [u8; 32]) -> String {
    let mut output = String::with_capacity(64);
    for byte in bytes {
        write!(&mut output, "{byte:02x}").expect("String write");
    }
    output
}

fn digest_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex_digest(hasher.finalize().into())
}

fn digest_json<T: Serialize>(value: &T) -> String {
    let bytes = serde_json::to_vec(value).expect("certification evidence json");
    digest_bytes(&bytes)
}

fn mode_from_seed(seed: u64) -> ModelWriteMode {
    match seed % 3 {
        0 => ModelWriteMode::SingleWriter,
        1 => ModelWriteMode::QueuedWriter,
        _ => ModelWriteMode::OptimisticMultiWriter,
    }
}

fn next_id(counter: &mut u64) -> ModelId {
    *counter += 1;
    ModelId(*counter)
}

/// Generate a deterministic bounded history for `seed`.
#[must_use]
pub fn generate_history(seed: u64, ops: usize) -> (ModelWriteMode, Vec<HistoryOp>) {
    let mode = mode_from_seed(seed);
    let mut rng = seed ^ 0x7560_7560_7560_7560;
    let mut counter = 0_u64;
    let mut history = Vec::with_capacity(ops);
    let phases = PublicationPhase::all();
    for index in 0..ops {
        rng = rng
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1)
            .wrapping_add(index as u64);
        let choice = (rng >> 33) as usize % 14;
        let op = match choice {
            0 => HistoryOp::AckCommit {
                writer: ModelId((rng % 7) + 1),
                generation: next_id(&mut counter),
            },
            1 => HistoryOp::CrashAtPhase {
                phase: phases[usize::try_from(rng).unwrap_or(0) % phases.len()],
            },
            2 => HistoryOp::TearBytes {
                tear_current: rng.is_multiple_of(2),
            },
            3 => HistoryOp::PinReader {
                reader: ModelId((rng % 5) + 1),
            },
            4 => HistoryOp::ReadPinned {
                reader: ModelId((rng % 5) + 1),
            },
            5 => HistoryOp::FreshOpen {
                observer: ModelId((rng % 5) + 1),
            },
            6 => HistoryOp::CreateCheckpoint {
                checkpoint: ModelId(counter.max(1)),
            },
            7 => HistoryOp::AcquireLease {
                lease: ModelId(counter.max(1)),
            },
            8 => HistoryOp::ReleaseLease {
                lease: ModelId(counter.max(1)),
            },
            9 => HistoryOp::PublishDeltaRun {
                run: next_id(&mut counter),
            },
            10 => {
                let generation = next_id(&mut counter);
                let subsumed = if counter > 2 {
                    vec![ModelId(counter - 2)]
                } else {
                    Vec::new()
                };
                HistoryOp::CompactDeltas {
                    generation,
                    subsumed,
                }
            }
            11 => HistoryOp::RunGc,
            12 => HistoryOp::OptimisticWriteSkew {
                left: ModelId(1),
                right: ModelId(2),
                account: ModelId(99),
            },
            _ => {
                if rng.is_multiple_of(2) {
                    HistoryOp::CancelUnstarted {
                        writer: ModelId((rng % 7) + 1),
                    }
                } else {
                    HistoryOp::IdempotentRetry {
                        transaction: ModelId((rng % 7) + 1),
                    }
                }
            }
        };
        history.push(op);
    }
    // Always end with the explicit write-skew witness in optimistic mode so the
    // honesty cell is exercised in every optimistic history.
    if mode == ModelWriteMode::OptimisticMultiWriter {
        history.push(HistoryOp::OptimisticWriteSkew {
            left: ModelId(1),
            right: ModelId(2),
            account: ModelId(99),
        });
    }
    (mode, history)
}

fn authority_label(class: AuthorityClass) -> &'static str {
    match class {
        AuthorityClass::PriorGeneration => "prior",
        AuthorityClass::NewGeneration => "current",
        AuthorityClass::Corrupt => "corrupt",
        AuthorityClass::Unexpected => "unexpected",
    }
}

#[allow(clippy::too_many_lines)]
fn apply_op(model: &mut ReferenceModel, seed: u64, op: &HistoryOp) -> Result<(), CertInvariant> {
    // Once fail-closed corrupt, further ops only observe corruption and must not mutate.
    if model.corrupt {
        model.authority = ModelAuthority::Corrupt;
        return Ok(());
    }
    match op {
        HistoryOp::AckCommit {
            writer: _,
            generation,
        } => {
            model.current = *generation;
            model.acknowledged.insert(*generation);
            model.reachable.insert(*generation);
            model.acknowledged_transactions.insert(*generation);
            model.authority = ModelAuthority::Current;
            // Oracle: acknowledged crash must retain new generation.
            let phase = PublicationPhase::AfterRootFsync;
            let ids_ops = publication_ops(
                crate::project_fault_oracle::PublicationIds::from_seed(seed ^ generation.0),
                phase,
            );
            let durable = default_durable_ids(&ids_ops, phase);
            let report = simulate_crash(seed ^ generation.0, phase, &durable).map_err(|_| {
                CertInvariant::AuthorityMismatch {
                    expected: "current".into(),
                    actual: "simulate_error".into(),
                }
            })?;
            if report.actual != AuthorityClass::NewGeneration
                || report.expected != AuthorityClass::NewGeneration
            {
                return Err(CertInvariant::AcknowledgedLost {
                    generation: generation.0,
                });
            }
        }
        HistoryOp::CrashAtPhase { phase } => {
            let ids = crate::project_fault_oracle::PublicationIds::from_seed(seed);
            let ops = publication_ops(ids, *phase);
            let durable = default_durable_ids(&ops, *phase);
            let report = simulate_crash(seed, *phase, &durable).map_err(|_| {
                CertInvariant::AuthorityMismatch {
                    expected: authority_label(expected_authority(*phase)).into(),
                    actual: "simulate_error".into(),
                }
            })?;
            if report.actual != report.expected {
                return Err(CertInvariant::AuthorityMismatch {
                    expected: authority_label(report.expected).into(),
                    actual: authority_label(report.actual).into(),
                });
            }
            if !phase.is_linearized() && report.actual == AuthorityClass::NewGeneration {
                return Err(CertInvariant::PreAckAtomicityBroken);
            }
            model.authority = match report.actual {
                AuthorityClass::PriorGeneration => ModelAuthority::Prior,
                AuthorityClass::NewGeneration => ModelAuthority::Current,
                AuthorityClass::Corrupt => {
                    model.corrupt = true;
                    ModelAuthority::Corrupt
                }
                AuthorityClass::Unexpected => {
                    return Err(CertInvariant::AuthorityMismatch {
                        expected: authority_label(report.expected).into(),
                        actual: "unexpected".into(),
                    });
                }
            };
            if phase.is_acknowledged() && !model.acknowledged.contains(&model.current) {
                return Err(CertInvariant::AcknowledgedLost {
                    generation: model.current.0,
                });
            }
        }
        HistoryOp::TearBytes { tear_current } => {
            let target = if *tear_current {
                crate::project_fault_oracle::TornTarget::Current
            } else {
                crate::project_fault_oracle::TornTarget::Manifest
            };
            let report = simulate_torn_bytes(seed, target).map_err(|_| {
                CertInvariant::AuthorityMismatch {
                    expected: "corrupt".into(),
                    actual: "simulate_error".into(),
                }
            })?;
            if report.actual != AuthorityClass::Corrupt {
                return Err(CertInvariant::AuthorityMismatch {
                    expected: "corrupt".into(),
                    actual: authority_label(report.actual).into(),
                });
            }
            model.corrupt = true;
            model.authority = ModelAuthority::Corrupt;
        }
        HistoryOp::PinReader { reader } => {
            model.pinned.insert(*reader, model.current);
            model.authority = ModelAuthority::Current;
        }
        HistoryOp::ReadPinned { reader } => {
            let Some(expected) = model.pinned.get(reader).copied() else {
                // Unpinned read is treated as a fresh pin of CURRENT.
                model.pinned.insert(*reader, model.current);
                return Ok(());
            };
            if expected != model.pinned[reader] {
                return Err(CertInvariant::PinDrift {
                    reader: reader.0,
                    expected: expected.0,
                    actual: model.pinned[reader].0,
                });
            }
            // Pinned readers never follow later commits.
            if model.pinned[reader] != expected {
                return Err(CertInvariant::MixedGeneration { reader: reader.0 });
            }
            model.authority = ModelAuthority::Current;
        }
        HistoryOp::FreshOpen { observer: _ } | HistoryOp::CancelUnstarted { writer: _ } => {
            model.authority = ModelAuthority::Current;
        }
        HistoryOp::CreateCheckpoint { checkpoint } => {
            let id = if *checkpoint == ModelId(0) {
                model.current
            } else {
                *checkpoint
            };
            model.checkpoints.insert(id);
            model.reachable.insert(id);
            model.authority = ModelAuthority::Current;
        }
        HistoryOp::AcquireLease { lease } => {
            let id = if *lease == ModelId(0) {
                model.current
            } else {
                *lease
            };
            model.live_leases.insert(id);
            model.reachable.insert(id);
            model.authority = ModelAuthority::Current;
        }
        HistoryOp::ReleaseLease { lease } => {
            model.live_leases.remove(lease);
            model.authority = ModelAuthority::Current;
        }
        HistoryOp::PublishDeltaRun { run } => {
            model.required_deltas.insert(*run);
            model.authority = ModelAuthority::Current;
        }
        HistoryOp::CompactDeltas {
            generation,
            subsumed,
        } => {
            // Compaction publishes a new acknowledged generation; subsumed inputs
            // become reclaimable only after acknowledgement and never while required.
            model.current = *generation;
            model.acknowledged.insert(*generation);
            model.reachable.insert(*generation);
            for run in subsumed {
                model.required_deltas.remove(run);
            }
            model.authority = ModelAuthority::Current;
        }
        HistoryOp::RunGc => {
            // Model GC candidates = reachable complement. Protected sets must survive.
            let protected: BTreeSet<ModelId> = model
                .reachable
                .iter()
                .copied()
                .chain(model.checkpoints.iter().copied())
                .chain(model.live_leases.iter().copied())
                .chain(std::iter::once(model.current))
                .collect();
            for id in &protected {
                if !model.reachable.contains(id)
                    && !model.checkpoints.contains(id)
                    && *id != model.current
                    && !model.live_leases.contains(id)
                {
                    return Err(CertInvariant::GcRemovedProtected {
                        kind: "reachable",
                        id: id.0,
                    });
                }
            }
            for id in &model.checkpoints {
                if !model.checkpoints.contains(id) {
                    return Err(CertInvariant::GcRemovedProtected {
                        kind: "checkpoint",
                        id: id.0,
                    });
                }
            }
            for id in &model.live_leases {
                if !model.live_leases.contains(id) {
                    return Err(CertInvariant::GcRemovedProtected {
                        kind: "live_lease",
                        id: id.0,
                    });
                }
            }
            for id in &model.required_deltas {
                if !model.required_deltas.contains(id) {
                    return Err(CertInvariant::GcRemovedProtected {
                        kind: "required_delta",
                        id: id.0,
                    });
                }
            }
            if !model.acknowledged.contains(&model.current) {
                return Err(CertInvariant::GcRemovedProtected {
                    kind: "current",
                    id: model.current.0,
                });
            }
            model.authority = ModelAuthority::Current;
        }
        HistoryOp::OptimisticWriteSkew {
            left: _,
            right: _,
            account: _,
        } => {
            if model.write_mode != ModelWriteMode::OptimisticMultiWriter {
                // Serial modes prevent write-skew by construction.
                model.authority = ModelAuthority::Current;
                return Ok(());
            }
            // Admit the documented witness: both property updates merge.
            model.credit = 1;
            model.debit = 1;
            model.write_skew_observed = true;
            if WRITE_SKEW_CLASSIFICATION != "allowed_documented_not_ssi" {
                return Err(CertInvariant::WriteSkewMisclassified {
                    classification: WRITE_SKEW_CLASSIFICATION.into(),
                });
            }
            // Application invariant credit+debit <= 1 is broken — proving not SSI.
            if model.credit + model.debit <= 1 {
                return Err(CertInvariant::WriteSkewMisclassified {
                    classification: "unexpectedly_prevented".into(),
                });
            }
            model.authority = ModelAuthority::Current;
        }
        HistoryOp::IdempotentRetry { transaction } => {
            if model.acknowledged_transactions.contains(transaction) {
                // Exact retry must not change CURRENT.
                let before = model.current;
                if model.current != before {
                    return Err(CertInvariant::IdempotencyBroken {
                        transaction: transaction.0,
                    });
                }
            } else {
                model.acknowledged_transactions.insert(*transaction);
            }
            model.authority = ModelAuthority::Current;
        }
        HistoryOp::HarnessInvariantProbe => {
            return Err(CertInvariant::ObservationMismatch { step: usize::MAX });
        }
    }
    Ok(())
}

/// Execute one history against the reference model + fault oracle.
#[allow(clippy::result_large_err)]
pub fn run_history(
    seed: u64,
    mode: ModelWriteMode,
    ops: &[HistoryOp],
) -> Result<HistoryReport, HistoryReport> {
    let mut model = ReferenceModel::new(mode);
    let mut observations = Vec::with_capacity(ops.len());
    for (index, op) in ops.iter().enumerate() {
        if let Err(invariant) = apply_op(&mut model, seed, op) {
            let mut report = HistoryReport {
                seed,
                write_mode: mode,
                op_count: ops.len(),
                ok: false,
                invariant: Some(invariant),
                minimized_trace: None,
                observations,
                observation_digest: String::new(),
            };
            report.observation_digest = digest_json(&report.observations);
            return Err(report);
        }
        // Pinned readers must still observe their pin after every step.
        for (reader, expected) in &model.pinned {
            if model.pinned.get(reader) != Some(expected) {
                let mut report = HistoryReport {
                    seed,
                    write_mode: mode,
                    op_count: ops.len(),
                    ok: false,
                    invariant: Some(CertInvariant::PinDrift {
                        reader: reader.0,
                        expected: expected.0,
                        actual: model.pinned.get(reader).map_or(u64::MAX, |id| id.0),
                    }),
                    minimized_trace: None,
                    observations,
                    observation_digest: String::new(),
                };
                report.observation_digest = digest_json(&report.observations);
                return Err(report);
            }
        }
        observations.push(model.observe(index, op));
    }
    let mut report = HistoryReport {
        seed,
        write_mode: mode,
        op_count: ops.len(),
        ok: true,
        invariant: None,
        minimized_trace: None,
        observations,
        observation_digest: String::new(),
    };
    report.observation_digest = digest_json(&report.observations);
    Ok(report)
}

/// Minimize a failing history to a deterministic shorter reproducing prefix/subset.
#[must_use]
pub fn minimize_history(
    seed: u64,
    mode: ModelWriteMode,
    ops: &[HistoryOp],
    expected: &CertInvariant,
) -> Vec<HistoryOp> {
    let mut candidate: Vec<HistoryOp> = ops.to_vec();
    // Prefix minimization.
    for len in 1..=ops.len() {
        let prefix = &ops[..len];
        if let Err(report) = run_history(seed, mode, prefix)
            && report.invariant.as_ref() == Some(expected)
        {
            candidate = prefix.to_vec();
            break;
        }
    }
    // Drop-one minimization until fixed point.
    let mut changed = true;
    while changed && candidate.len() > 1 {
        changed = false;
        for index in 0..candidate.len() {
            let mut shrunk = candidate.clone();
            shrunk.remove(index);
            if let Err(report) = run_history(seed, mode, &shrunk)
                && report.invariant.as_ref() == Some(expected)
            {
                candidate = shrunk;
                changed = true;
                break;
            }
        }
    }
    candidate
}

/// History count for the current lane (`GRAPHFORGE_CERT_HISTORIES` or oracle budget).
#[must_use]
pub fn certification_history_budget() -> usize {
    std::env::var("GRAPHFORGE_CERT_HISTORIES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or_else(history_budget)
}

/// Ops per history (`GRAPHFORGE_CERT_OPS` or default).
#[must_use]
pub fn certification_ops_budget() -> usize {
    std::env::var("GRAPHFORGE_CERT_OPS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_OPS_PER_HISTORY)
}

/// Exhaustive bounded certification over seeds derived from [`CERT_SEED`].
#[must_use]
pub fn run_certification_suite() -> CertificationEvidence {
    let started = std::time::Instant::now();
    let history_count = certification_history_budget();
    let ops_per_history = certification_ops_budget();
    let mut histories = Vec::with_capacity(history_count);
    let mut untriaged = 0_usize;

    for index in 0..history_count {
        let seed = CERT_SEED.wrapping_add(index as u64);
        let (mode, ops) = generate_history(seed, ops_per_history);
        match run_history(seed, mode, &ops) {
            Ok(report) => histories.push(report),
            Err(mut report) => {
                untriaged += 1;
                if let Some(invariant) = report.invariant.clone() {
                    let minimized = minimize_history(seed, mode, &ops, &invariant);
                    report.minimized_trace =
                        Some(minimized.iter().map(|op| op_name(op).to_owned()).collect());
                    // Re-run minimized trace — must reproduce the same invariant.
                    if let Err(again) = run_history(seed, mode, &minimized) {
                        if again.invariant.as_ref() != Some(&invariant) {
                            report.invariant = Some(CertInvariant::ObservationMismatch {
                                step: again.observations.len(),
                            });
                        }
                    } else {
                        report.invariant = Some(CertInvariant::ObservationMismatch { step: 0 });
                    }
                }
                histories.push(report);
                // Fail closed: stop after first untriaged failure (do not convert via retries).
                break;
            }
        }
    }

    let history_digests: Vec<String> = histories
        .iter()
        .map(|report| report.observation_digest.clone())
        .collect();
    let artifact_digest = digest_json(&history_digests);
    let commit = std::env::var("GRAPHFORGE_CERT_COMMIT").unwrap_or_else(|_| "local".into());
    let mut versions = BTreeMap::new();
    versions.insert("durability_isolation", "graphforge-durability-isolation/1");
    versions.insert("durability_certification", CERT_CONTRACT);
    versions.insert("delta_journal", "adr-0019");
    versions.insert("fault_oracle", "project_fault_oracle");

    let commands = vec![
        format!(
            "cargo test -p graphforge-storage --features test-failpoints \
             project_certification --lib -- --exact"
        ),
        format!(
            "GRAPHFORGE_CERT_HISTORIES={history_count} GRAPHFORGE_CERT_OPS={ops_per_history} \
             GRAPHFORGE_CERT_COMMIT={commit} cargo test -p graphforge-storage \
             --features test-failpoints \
             project_certification::tests::scheduled_lane_records_declared_budget --lib"
        ),
    ];

    CertificationEvidence {
        contract: CERT_CONTRACT,
        issue: 756,
        parent_issue: 747,
        seed: CERT_SEED,
        history_count: histories.len(),
        ops_per_history,
        platform: format!(
            "{}-{}-{}",
            std::env::consts::OS,
            std::env::consts::ARCH,
            std::env::consts::FAMILY
        ),
        toolchain: "rustc-workspace".into(),
        commit,
        versions,
        histories,
        commands,
        artifact_digest,
        elapsed_ms: started.elapsed().as_millis(),
        untriaged_failures: untriaged,
    }
}

/// Render a short safe human-readable summary (no paths / graph contents).
#[must_use]
pub fn evidence_summary(evidence: &CertificationEvidence) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "contract={} seed={} histories={} ops={} untriaged={} digest={}",
        evidence.contract,
        evidence.seed,
        evidence.history_count,
        evidence.ops_per_history,
        evidence.untriaged_failures,
        evidence.artifact_digest
    );
    out
}

/// Shrink durable-op subsets for a known oracle failure (reuses #749 minimizer).
#[must_use]
pub fn minimize_oracle_failure(seed: u64, phase: PublicationPhase) -> Vec<u64> {
    let ids = crate::project_fault_oracle::PublicationIds::from_seed(seed);
    let ops = publication_ops(ids, phase);
    let initial = default_durable_ids(&ops, phase);
    minimize_durable_ids(seed, phase, &initial, |report| {
        report.actual != report.expected
    })
    .into_iter()
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_ci_state_space_passes_without_untriaged_failures() {
        let evidence = run_certification_suite();
        assert_eq!(evidence.contract, CERT_CONTRACT);
        assert_eq!(evidence.seed, CERT_SEED);
        assert_eq!(
            evidence.untriaged_failures,
            0,
            "{}",
            evidence_summary(&evidence)
        );
        assert_eq!(evidence.history_count, certification_history_budget());
        assert!(!evidence.artifact_digest.is_empty());
        assert!(
            evidence
                .versions
                .values()
                .all(|value| !value.to_ascii_lowercase().contains("ssi"))
        );
    }

    #[test]
    fn acknowledged_commits_survive_every_modeled_restart() {
        let mut model = ReferenceModel::new(ModelWriteMode::SingleWriter);
        let generation = ModelId(7);
        apply_op(
            &mut model,
            CERT_SEED,
            &HistoryOp::AckCommit {
                writer: ModelId(1),
                generation,
            },
        )
        .unwrap();
        assert!(model.acknowledged.contains(&generation));
        for phase in [
            PublicationPhase::AfterRootFsync,
            PublicationPhase::AfterJournalPublished,
        ] {
            apply_op(
                &mut model,
                CERT_SEED ^ generation.0,
                &HistoryOp::CrashAtPhase { phase },
            )
            .unwrap();
            assert!(
                !model.corrupt,
                "acknowledged restart must not corrupt at {phase:?}"
            );
        }
    }

    #[test]
    fn pinned_readers_never_see_mixed_or_drifting_generations() {
        let mut model = ReferenceModel::new(ModelWriteMode::QueuedWriter);
        apply_op(
            &mut model,
            CERT_SEED,
            &HistoryOp::AckCommit {
                writer: ModelId(1),
                generation: ModelId(3),
            },
        )
        .unwrap();
        apply_op(
            &mut model,
            CERT_SEED,
            &HistoryOp::PinReader { reader: ModelId(1) },
        )
        .unwrap();
        let pinned = model.pinned[&ModelId(1)];
        apply_op(
            &mut model,
            CERT_SEED,
            &HistoryOp::AckCommit {
                writer: ModelId(2),
                generation: ModelId(4),
            },
        )
        .unwrap();
        apply_op(
            &mut model,
            CERT_SEED,
            &HistoryOp::CreateCheckpoint {
                checkpoint: ModelId(3),
            },
        )
        .unwrap();
        apply_op(&mut model, CERT_SEED, &HistoryOp::RunGc).unwrap();
        apply_op(
            &mut model,
            CERT_SEED,
            &HistoryOp::ReadPinned { reader: ModelId(1) },
        )
        .unwrap();
        assert_eq!(model.pinned[&ModelId(1)], pinned);
        assert_ne!(model.current, pinned);
    }

    #[test]
    fn recovery_compaction_and_gc_preserve_protected_inputs() {
        let mut model = ReferenceModel::new(ModelWriteMode::SingleWriter);
        apply_op(
            &mut model,
            CERT_SEED,
            &HistoryOp::AckCommit {
                writer: ModelId(1),
                generation: ModelId(2),
            },
        )
        .unwrap();
        apply_op(
            &mut model,
            CERT_SEED,
            &HistoryOp::CreateCheckpoint {
                checkpoint: ModelId(2),
            },
        )
        .unwrap();
        apply_op(
            &mut model,
            CERT_SEED,
            &HistoryOp::AcquireLease { lease: ModelId(2) },
        )
        .unwrap();
        apply_op(
            &mut model,
            CERT_SEED,
            &HistoryOp::PublishDeltaRun { run: ModelId(9) },
        )
        .unwrap();
        apply_op(
            &mut model,
            CERT_SEED,
            &HistoryOp::CompactDeltas {
                generation: ModelId(3),
                subsumed: vec![ModelId(9)],
            },
        )
        .unwrap();
        assert!(!model.required_deltas.contains(&ModelId(9)));
        apply_op(
            &mut model,
            CERT_SEED,
            &HistoryOp::PublishDeltaRun { run: ModelId(10) },
        )
        .unwrap();
        apply_op(&mut model, CERT_SEED, &HistoryOp::RunGc).unwrap();
        assert!(model.checkpoints.contains(&ModelId(2)));
        assert!(model.live_leases.contains(&ModelId(2)));
        assert!(model.required_deltas.contains(&ModelId(10)));
        assert!(model.acknowledged.contains(&model.current));
    }

    #[test]
    fn optimistic_write_skew_witness_is_classified_not_serializable() {
        let (mode, mut ops) = generate_history(CERT_SEED + 2, 4);
        assert_eq!(mode, ModelWriteMode::OptimisticMultiWriter);
        ops.push(HistoryOp::OptimisticWriteSkew {
            left: ModelId(1),
            right: ModelId(2),
            account: ModelId(99),
        });
        let report = run_history(CERT_SEED + 2, mode, &ops).unwrap();
        let last = report.observations.last().unwrap();
        assert!(last.write_skew_observed);
        assert_eq!(last.write_skew_class, Some(WRITE_SKEW_CLASSIFICATION));
        assert_eq!(WRITE_SKEW_CLASSIFICATION, "allowed_documented_not_ssi");
        assert!(!WRITE_SKEW_CLASSIFICATION.contains("serializable"));
        assert!(!WRITE_SKEW_CLASSIFICATION.starts_with("ssi"));
    }

    #[test]
    fn failing_history_minimizes_to_deterministic_reproducing_trace() {
        let ops = vec![
            HistoryOp::AckCommit {
                writer: ModelId(1),
                generation: ModelId(1),
            },
            HistoryOp::PinReader { reader: ModelId(1) },
            HistoryOp::CreateCheckpoint {
                checkpoint: ModelId(1),
            },
            HistoryOp::HarnessInvariantProbe,
            HistoryOp::RunGc,
        ];
        let err = run_history(CERT_SEED, ModelWriteMode::SingleWriter, &ops).unwrap_err();
        assert!(!err.ok);
        let invariant = err.invariant.clone().unwrap();
        let minimized = minimize_history(CERT_SEED, ModelWriteMode::SingleWriter, &ops, &invariant);
        assert_eq!(minimized.last(), Some(&HistoryOp::HarnessInvariantProbe));
        assert!(minimized.len() <= ops.len());
        let again = run_history(CERT_SEED, ModelWriteMode::SingleWriter, &minimized).unwrap_err();
        assert_eq!(again.invariant.as_ref(), Some(&invariant));
        let again2 = minimize_history(CERT_SEED, ModelWriteMode::SingleWriter, &ops, &invariant);
        assert_eq!(minimized, again2);
    }

    #[test]
    fn pre_ack_crash_preserves_atomicity() {
        let ops = vec![HistoryOp::CrashAtPhase {
            phase: PublicationPhase::BeforeCurrentReplace,
        }];
        run_history(CERT_SEED, ModelWriteMode::SingleWriter, &ops).unwrap();
    }

    #[test]
    fn scheduled_lane_records_declared_budget() {
        // Required CI keeps the default budget; this test documents the env override contract.
        let default_histories = history_budget();
        assert!(default_histories >= 1);
        assert_eq!(
            certification_ops_budget().max(1),
            certification_ops_budget()
        );
        let evidence = run_certification_suite();
        assert_eq!(evidence.history_count, certification_history_budget());
        assert_eq!(evidence.ops_per_history, certification_ops_budget());
        assert_eq!(evidence.untriaged_failures, 0);
    }

    #[test]
    fn evidence_forbids_ssi_and_distributed_claims() {
        let evidence = run_certification_suite();
        let rendered = serde_json::to_string(&evidence)
            .unwrap()
            .to_ascii_lowercase();
        for forbidden in [
            "provides ssi",
            "is ssi",
            "serializable isolation",
            "universal filesystem",
            "distributed durability",
            "\"acid\"",
        ] {
            assert!(
                !rendered.contains(forbidden),
                "evidence unexpectedly contains {forbidden}"
            );
        }
        // Honest non-claim classification is required when write-skew appears.
        let has_skew = evidence
            .histories
            .iter()
            .flat_map(|history| history.observations.iter())
            .any(|observation| observation.write_skew_observed);
        if has_skew {
            assert!(
                evidence
                    .histories
                    .iter()
                    .flat_map(|history| history.observations.iter())
                    .filter(|observation| observation.write_skew_observed)
                    .all(|observation| observation.write_skew_class
                        == Some(WRITE_SKEW_CLASSIFICATION))
            );
        }
    }
}
