//! Immutable algorithm-run identities, lifecycle events, and Arrow encoding.

use super::ALGORITHM_RUN_CONTRACT_VERSION;
use super::ALGORITHM_RUN_EVENT_CONTRACT_VERSION;
use super::ALGORITHM_RUN_EVENT_SCHEMA;
use super::ALGORITHM_RUN_SCHEMA;
use super::KnowledgeError;
use super::binary_column;
use super::check_limit;
use super::fixed_32_at;
use super::fixed_column;
use super::invalid;
use super::optional_fixed_32;
use super::optional_text;
use super::require_schema;
use super::require_uuid;
use super::require_v7;
use super::required_binary;
use super::required_i64;
use super::required_text;
use super::required_u32;
use super::string_column;
use super::timestamp_column;
use super::u32_column;
use super::uuid_at;
use arrow::array::BinaryArray;
use arrow::array::FixedSizeBinaryBuilder;
use arrow::array::StringArray;
use arrow::array::TimestampMicrosecondArray;
use arrow::array::UInt32Array;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use uuid::Uuid;

/// Closed append-only algorithm-run lifecycle state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlgorithmRunState {
    /// Identity was durably published before dispatch.
    Started,
    /// Dispatch returned a canonical Arrow result.
    Completed,
    /// Dispatch returned a structured failure.
    Failed,
    /// Cancellation was observed at a deterministic checkpoint.
    Cancelled,
    /// Reopen found a published start without a terminal event.
    Interrupted,
}

impl AlgorithmRunState {
    /// Canonical persisted spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Interrupted => "interrupted",
        }
    }

    pub(super) fn parse(value: &str) -> Result<Self, KnowledgeError> {
        match value {
            "started" => Ok(Self::Started),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            "interrupted" => Ok(Self::Interrupted),
            _ => Err(invalid("state", "unknown closed value")),
        }
    }

    /// Whether this state closes a run.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        !matches!(self, Self::Started)
    }
}

/// Immutable identity for one recorded algorithm invocation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlgorithmRun {
    /// Caller-supplied UUIDv7 run identity.
    pub run_uuid: Uuid,
    /// Closed public algorithm name.
    pub algorithm: String,
    /// Algorithm contract version.
    pub algorithm_version: u32,
    /// Neutral descriptor contract version.
    pub descriptor_version: u32,
    /// Exact canonical descriptor bytes.
    pub descriptor: Vec<u8>,
    /// Exact resolved graph projection fingerprint.
    pub projection_fingerprint: [u8; 32],
    /// Provenance event that published the run identity.
    pub provenance_uuid: Uuid,
    /// Durable start transaction time.
    pub started_at_micros: i64,
    /// Run-record contract version.
    pub contract_version: u32,
}

impl AlgorithmRun {
    /// Construct one immutable run identity.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        run_uuid: Uuid,
        algorithm: String,
        algorithm_version: u32,
        descriptor_version: u32,
        descriptor: Vec<u8>,
        projection_fingerprint: [u8; 32],
        provenance_uuid: Uuid,
        started_at_micros: i64,
    ) -> Result<Self, KnowledgeError> {
        let row = Self {
            run_uuid,
            algorithm,
            algorithm_version,
            descriptor_version,
            descriptor,
            projection_fingerprint,
            provenance_uuid,
            started_at_micros,
            contract_version: ALGORITHM_RUN_CONTRACT_VERSION,
        };
        validate_algorithm_run(&row)?;
        Ok(row)
    }
}

/// One immutable event in a recorded algorithm lifecycle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlgorithmRunEvent {
    /// Deterministic event identity.
    pub event_uuid: Uuid,
    /// Owning run identity.
    pub run_uuid: Uuid,
    /// Closed lifecycle state.
    pub state: AlgorithmRunState,
    /// Canonical Arrow fingerprint for a completed result.
    pub result_fingerprint: Option<[u8; 32]>,
    /// Sanitized stable error code for non-success terminal states.
    pub error_code: Option<String>,
    /// Durable transaction time.
    pub recorded_at_micros: i64,
    /// Provenance event for this lifecycle transition.
    pub provenance_uuid: Uuid,
    /// Lifecycle-event contract version.
    pub contract_version: u32,
}

impl AlgorithmRunEvent {
    /// Construct one validated lifecycle event.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        event_uuid: Uuid,
        run_uuid: Uuid,
        state: AlgorithmRunState,
        result_fingerprint: Option<[u8; 32]>,
        error_code: Option<String>,
        recorded_at_micros: i64,
        provenance_uuid: Uuid,
    ) -> Result<Self, KnowledgeError> {
        let row = Self {
            event_uuid,
            run_uuid,
            state,
            result_fingerprint,
            error_code,
            recorded_at_micros,
            provenance_uuid,
            contract_version: ALGORITHM_RUN_EVENT_CONTRACT_VERSION,
        };
        validate_algorithm_run_event(&row)?;
        Ok(row)
    }
}

/// Validated immutable run identities and append-only lifecycle events.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AlgorithmRunLedger {
    /// Run identities ordered by `(started_at, run_uuid)`.
    pub runs: Vec<AlgorithmRun>,
    /// Events ordered by `(recorded_at, event_uuid)`.
    pub events: Vec<AlgorithmRunEvent>,
}

impl AlgorithmRunLedger {
    /// Validate and normalize complete run tables.
    pub fn new(
        mut runs: Vec<AlgorithmRun>,
        mut events: Vec<AlgorithmRunEvent>,
    ) -> Result<Self, KnowledgeError> {
        runs.sort_by_key(|row| (row.started_at_micros, row.run_uuid));
        events.sort_by_key(|row| (row.recorded_at_micros, row.event_uuid));
        validate_algorithm_run_rows(&runs, &events)?;
        Ok(Self { runs, events })
    }

    /// Merge immutable identities and events, rejecting conflicting reuse.
    pub fn merge(&self, staged: &Self) -> Result<Self, KnowledgeError> {
        let mut runs = self.runs.clone();
        for row in &staged.runs {
            match runs.iter().find(|current| current.run_uuid == row.run_uuid) {
                Some(current) if current == row => {}
                Some(_) => return Err(KnowledgeError::Conflict("run_uuid")),
                None => runs.push(row.clone()),
            }
        }
        let mut events = self.events.clone();
        for row in &staged.events {
            match events
                .iter()
                .find(|current| current.event_uuid == row.event_uuid)
            {
                Some(current) if current == row => {}
                Some(_) => return Err(KnowledgeError::Conflict("event_uuid")),
                None => events.push(row.clone()),
            }
        }
        Self::new(runs, events)
    }

    /// Locate one run.
    #[must_use]
    pub fn run(&self, run_uuid: Uuid) -> Option<&AlgorithmRun> {
        self.runs.iter().find(|row| row.run_uuid == run_uuid)
    }

    /// Return lifecycle events for one run in canonical order.
    #[must_use]
    pub fn events_for(&self, run_uuid: Uuid) -> Vec<AlgorithmRunEvent> {
        self.events
            .iter()
            .filter(|row| row.run_uuid == run_uuid)
            .cloned()
            .collect()
    }

    /// Return the terminal event, when one exists.
    #[must_use]
    pub fn terminal_event(&self, run_uuid: Uuid) -> Option<&AlgorithmRunEvent> {
        self.events
            .iter()
            .find(|row| row.run_uuid == run_uuid && row.state.is_terminal())
    }

    /// Encode the authoritative run table.
    pub fn run_batch(&self) -> Result<RecordBatch, KnowledgeError> {
        algorithm_run_batch(&self.runs)
    }

    /// Encode the authoritative event table.
    pub fn event_batch(&self) -> Result<RecordBatch, KnowledgeError> {
        algorithm_run_event_batch(&self.events)
    }

    /// Decode, validate, and normalize persisted tables.
    pub fn from_batches(
        run_batches: &[RecordBatch],
        event_batches: &[RecordBatch],
    ) -> Result<Self, KnowledgeError> {
        let mut runs = Vec::new();
        for batch in run_batches {
            require_schema(batch, &ALGORITHM_RUN_SCHEMA, "algorithm_runs")?;
            let ids = fixed_column(batch, "run_uuid")?;
            let algorithms = string_column(batch, "algorithm")?;
            let algorithm_versions = u32_column(batch, "algorithm_version")?;
            let descriptor_versions = u32_column(batch, "descriptor_version")?;
            let descriptors = binary_column(batch, "descriptor")?;
            let projections = fixed_column(batch, "projection_fingerprint")?;
            let provenance = fixed_column(batch, "provenance_uuid")?;
            let started = timestamp_column(batch, "started_at")?;
            let contracts = u32_column(batch, "contract_version")?;
            for row in 0..batch.num_rows() {
                runs.push(AlgorithmRun {
                    run_uuid: uuid_at(ids, row, "run_uuid")?,
                    algorithm: required_text(algorithms, row, "algorithm")?.to_owned(),
                    algorithm_version: required_u32(algorithm_versions, row, "algorithm_version")?,
                    descriptor_version: required_u32(
                        descriptor_versions,
                        row,
                        "descriptor_version",
                    )?,
                    descriptor: required_binary(descriptors, row, "descriptor")?.to_vec(),
                    projection_fingerprint: fixed_32_at(
                        projections,
                        row,
                        "projection_fingerprint",
                    )?,
                    provenance_uuid: uuid_at(provenance, row, "provenance_uuid")?,
                    started_at_micros: required_i64(started, row, "started_at")?,
                    contract_version: required_u32(contracts, row, "contract_version")?,
                });
            }
        }
        let mut events = Vec::new();
        for batch in event_batches {
            require_schema(batch, &ALGORITHM_RUN_EVENT_SCHEMA, "algorithm_run_events")?;
            let ids = fixed_column(batch, "event_uuid")?;
            let runs_column = fixed_column(batch, "run_uuid")?;
            let states = string_column(batch, "state")?;
            let results = fixed_column(batch, "result_fingerprint")?;
            let errors = string_column(batch, "error_code")?;
            let recorded = timestamp_column(batch, "recorded_at")?;
            let provenance = fixed_column(batch, "provenance_uuid")?;
            let contracts = u32_column(batch, "contract_version")?;
            for row in 0..batch.num_rows() {
                events.push(AlgorithmRunEvent {
                    event_uuid: uuid_at(ids, row, "event_uuid")?,
                    run_uuid: uuid_at(runs_column, row, "run_uuid")?,
                    state: AlgorithmRunState::parse(required_text(states, row, "state")?)?,
                    result_fingerprint: optional_fixed_32(results, row, "result_fingerprint")?,
                    error_code: optional_text(errors, row),
                    recorded_at_micros: required_i64(recorded, row, "recorded_at")?,
                    provenance_uuid: uuid_at(provenance, row, "provenance_uuid")?,
                    contract_version: required_u32(contracts, row, "contract_version")?,
                });
            }
        }
        Self::new(runs, events)
    }
}

fn validate_algorithm_run(row: &AlgorithmRun) -> Result<(), KnowledgeError> {
    require_v7(row.run_uuid, "run_uuid")?;
    require_uuid(row.provenance_uuid, "provenance_uuid")?;
    if row.algorithm.is_empty()
        || row.algorithm.len() as u64 > graphforge_core::canonical::MAX_CANONICAL_TEXT_BYTES
    {
        return Err(invalid("algorithm", "must be bounded non-empty UTF-8"));
    }
    if row.algorithm_version != 1 {
        return Err(invalid("algorithm_version", "unsupported version"));
    }
    if row.descriptor_version != 1 {
        return Err(invalid("descriptor_version", "unsupported version"));
    }
    if row.descriptor.is_empty()
        || row.descriptor.len() as u64 > graphforge_core::canonical::MAX_CANONICAL_BINARY_BYTES
    {
        return Err(invalid("descriptor", "must be bounded and non-empty"));
    }
    if row.contract_version != ALGORITHM_RUN_CONTRACT_VERSION {
        return Err(invalid(
            "contract_version",
            "unsupported algorithm-run version",
        ));
    }
    Ok(())
}

fn validate_algorithm_run_event(row: &AlgorithmRunEvent) -> Result<(), KnowledgeError> {
    require_uuid(row.event_uuid, "event_uuid")?;
    require_v7(row.run_uuid, "run_uuid")?;
    require_uuid(row.provenance_uuid, "provenance_uuid")?;
    if row.contract_version != ALGORITHM_RUN_EVENT_CONTRACT_VERSION {
        return Err(invalid(
            "contract_version",
            "unsupported algorithm-run-event version",
        ));
    }
    if row.error_code.as_ref().is_some_and(|code| {
        code.is_empty()
            || code.len() as u64 > graphforge_core::canonical::MAX_CANONICAL_TEXT_BYTES
            || !code.starts_with("GF_")
    }) {
        return Err(invalid("error_code", "must be a bounded stable GF_ code"));
    }
    match row.state {
        AlgorithmRunState::Started => {
            if row.result_fingerprint.is_some() || row.error_code.is_some() {
                return Err(invalid("state", "started has no terminal payload"));
            }
        }
        AlgorithmRunState::Completed => {
            if row.result_fingerprint.is_none() || row.error_code.is_some() {
                return Err(invalid(
                    "state",
                    "completed requires only a result fingerprint",
                ));
            }
        }
        AlgorithmRunState::Failed
        | AlgorithmRunState::Cancelled
        | AlgorithmRunState::Interrupted => {
            if row.result_fingerprint.is_some() || row.error_code.is_none() {
                return Err(invalid(
                    "state",
                    "non-success terminal requires only an error code",
                ));
            }
        }
    }
    Ok(())
}

fn validate_algorithm_run_rows(
    runs: &[AlgorithmRun],
    events: &[AlgorithmRunEvent],
) -> Result<(), KnowledgeError> {
    check_limit("algorithm_runs", runs.len())?;
    check_limit("algorithm_run_events", events.len())?;
    let mut run_ids = HashSet::with_capacity(runs.len());
    let mut run_index = HashMap::with_capacity(runs.len());
    for row in runs {
        validate_algorithm_run(row)?;
        if !run_ids.insert(row.run_uuid) {
            return Err(KnowledgeError::Duplicate("run_uuid"));
        }
        run_index.insert(row.run_uuid, row);
    }
    let mut event_ids = HashSet::with_capacity(events.len());
    let mut per_run: HashMap<Uuid, (usize, usize)> = HashMap::new();
    for row in events {
        validate_algorithm_run_event(row)?;
        if !event_ids.insert(row.event_uuid) {
            return Err(KnowledgeError::Duplicate("event_uuid"));
        }
        let run = run_index
            .get(&row.run_uuid)
            .ok_or(KnowledgeError::Dangling("run_uuid"))?;
        if row.recorded_at_micros < run.started_at_micros {
            return Err(invalid("recorded_at", "event precedes run start"));
        }
        let counts = per_run.entry(row.run_uuid).or_default();
        if row.state == AlgorithmRunState::Started {
            counts.0 += 1;
            if row.recorded_at_micros != run.started_at_micros
                || row.provenance_uuid != run.provenance_uuid
            {
                return Err(invalid(
                    "started",
                    "start event must match immutable run identity",
                ));
            }
        } else {
            counts.1 += 1;
        }
    }
    for run in runs {
        let (started, terminal) = per_run.get(&run.run_uuid).copied().unwrap_or_default();
        if started != 1 {
            return Err(invalid("started", "run requires exactly one start event"));
        }
        if terminal > 1 {
            return Err(invalid(
                "terminal",
                "run permits at most one terminal event",
            ));
        }
    }
    Ok(())
}

fn algorithm_run_batch(rows: &[AlgorithmRun]) -> Result<RecordBatch, KnowledgeError> {
    let mut ids = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    let mut projections = FixedSizeBinaryBuilder::with_capacity(rows.len(), 32);
    let mut provenance = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    for row in rows {
        ids.append_value(row.run_uuid.as_bytes())?;
        projections.append_value(row.projection_fingerprint)?;
        provenance.append_value(row.provenance_uuid.as_bytes())?;
    }
    RecordBatch::try_new(
        Arc::clone(&ALGORITHM_RUN_SCHEMA),
        vec![
            Arc::new(ids.finish()),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.algorithm.as_str()),
            )),
            Arc::new(UInt32Array::from_iter_values(
                rows.iter().map(|row| row.algorithm_version),
            )),
            Arc::new(UInt32Array::from_iter_values(
                rows.iter().map(|row| row.descriptor_version),
            )),
            Arc::new(BinaryArray::from_iter_values(
                rows.iter().map(|row| row.descriptor.as_slice()),
            )),
            Arc::new(projections.finish()),
            Arc::new(provenance.finish()),
            Arc::new(
                TimestampMicrosecondArray::from_iter_values(
                    rows.iter().map(|row| row.started_at_micros),
                )
                .with_timezone("UTC"),
            ),
            Arc::new(UInt32Array::from_iter_values(
                rows.iter().map(|row| row.contract_version),
            )),
        ],
    )
    .map_err(Into::into)
}

fn algorithm_run_event_batch(rows: &[AlgorithmRunEvent]) -> Result<RecordBatch, KnowledgeError> {
    let mut ids = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    let mut runs = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    let mut results = FixedSizeBinaryBuilder::with_capacity(rows.len(), 32);
    let mut provenance = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    for row in rows {
        ids.append_value(row.event_uuid.as_bytes())?;
        runs.append_value(row.run_uuid.as_bytes())?;
        match row.result_fingerprint {
            Some(value) => results.append_value(value)?,
            None => results.append_null(),
        }
        provenance.append_value(row.provenance_uuid.as_bytes())?;
    }
    RecordBatch::try_new(
        Arc::clone(&ALGORITHM_RUN_EVENT_SCHEMA),
        vec![
            Arc::new(ids.finish()),
            Arc::new(runs.finish()),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.state.as_str()),
            )),
            Arc::new(results.finish()),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.error_code.as_deref())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(
                TimestampMicrosecondArray::from_iter_values(
                    rows.iter().map(|row| row.recorded_at_micros),
                )
                .with_timezone("UTC"),
            ),
            Arc::new(provenance.finish()),
            Arc::new(UInt32Array::from_iter_values(
                rows.iter().map(|row| row.contract_version),
            )),
        ],
    )
    .map_err(Into::into)
}

#[cfg(test)]
mod tests;
