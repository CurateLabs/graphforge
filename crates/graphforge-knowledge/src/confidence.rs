//! Immutable confidence assessments, input snapshots, and policy validation.

use super::CONFIDENCE_ASSESSMENT_CONTRACT_VERSION;
use super::CONFIDENCE_ASSESSMENT_SCHEMA;
use super::CONFIDENCE_INPUT_CONTRACT_VERSION;
use super::CONFIDENCE_INPUT_SCHEMA;
use super::KnowledgeError;
use super::canonical_optional_f64;
use super::check_limit;
use super::f64_column;
use super::fixed_column;
use super::invalid;
use super::normalize_zero;
use super::optional_f64;
use super::require_schema;
use super::require_uuid;
use super::require_v7;
use super::required_i64;
use super::required_text;
use super::required_u32;
use super::string_column;
use super::timestamp_column;
use super::u32_column;
use super::uuid_at;
use super::validate_confidence;
use arrow::array::FixedSizeBinaryBuilder;
use arrow::array::Float64Array;
use arrow::array::StringArray;
use arrow::array::TimestampMicrosecondArray;
use arrow::array::UInt32Array;
use arrow::record_batch::RecordBatch;
use graphforge_core::canonical::CANONICAL_CONTRACT_VERSION;
use graphforge_core::canonical::CanonicalDomain;
use graphforge_core::canonical::CanonicalWriter;
use graphforge_core::canonical::fingerprint;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use uuid::Uuid;

/// Closed confidence policy registry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfidencePolicy {
    /// Caller supplies the assessment value.
    Explicit,
    /// Minimum of all available, non-null requested inputs; null if any is unavailable.
    ConservativeMin,
}

impl ConfidencePolicy {
    /// Canonical persisted spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Explicit => "explicit",
            Self::ConservativeMin => "conservative_min",
        }
    }

    pub(super) fn parse(value: &str) -> Result<Self, KnowledgeError> {
        match value {
            "explicit" => Ok(Self::Explicit),
            "conservative_min" => Ok(Self::ConservativeMin),
            _ => Err(invalid("policy", "unknown closed value")),
        }
    }
}

/// One immutable confidence assessment.
#[derive(Clone, Debug, PartialEq)]
pub struct ConfidenceAssessment {
    /// Caller-supplied UUIDv7 identity and idempotency key.
    pub confidence_uuid: Uuid,
    /// Assertion being assessed.
    pub assertion_uuid: Uuid,
    /// Closed policy.
    pub policy: ConfidencePolicy,
    /// Policy contract version.
    pub policy_version: u32,
    /// Result in `[0, 1]`, or null when conservative inputs are incomplete.
    pub value: Option<f64>,
    /// Producing provenance event.
    pub provenance_uuid: Uuid,
    /// Transaction time in UTC microseconds.
    pub recorded_at_micros: i64,
    /// Assessment record contract.
    pub contract_version: u32,
}

impl ConfidenceAssessment {
    /// Construct one validated assessment.
    pub fn new(
        confidence_uuid: Uuid,
        assertion_uuid: Uuid,
        policy: ConfidencePolicy,
        value: Option<f64>,
        provenance_uuid: Uuid,
        recorded_at_micros: i64,
    ) -> Result<Self, KnowledgeError> {
        require_v7(confidence_uuid, "confidence_uuid")?;
        require_v7(assertion_uuid, "assertion_uuid")?;
        require_uuid(provenance_uuid, "provenance_uuid")?;
        validate_confidence(value, "value")?;
        Ok(Self {
            confidence_uuid,
            assertion_uuid,
            policy,
            policy_version: 1,
            value: value.map(normalize_zero),
            provenance_uuid,
            recorded_at_micros,
            contract_version: CONFIDENCE_ASSESSMENT_CONTRACT_VERSION,
        })
    }
}

/// Immutable snapshot of one requested confidence input.
#[derive(Clone, Debug, PartialEq)]
pub struct ConfidenceInput {
    /// Owning assessment.
    pub confidence_uuid: Uuid,
    /// Requested immutable assessment identity.
    pub input_confidence_uuid: Uuid,
    /// Value observed at assessment time; null means absent or null.
    pub input_value: Option<f64>,
    /// UUID-normalized position.
    pub ordinal: u32,
    /// Input record contract.
    pub contract_version: u32,
}

impl ConfidenceInput {
    /// Construct one validated snapshot input.
    pub fn new(
        confidence_uuid: Uuid,
        input_confidence_uuid: Uuid,
        input_value: Option<f64>,
        ordinal: u32,
    ) -> Result<Self, KnowledgeError> {
        require_v7(confidence_uuid, "confidence_uuid")?;
        require_v7(input_confidence_uuid, "input_confidence_uuid")?;
        validate_confidence(input_value, "input_value")?;
        Ok(Self {
            confidence_uuid,
            input_confidence_uuid,
            input_value: input_value.map(normalize_zero),
            ordinal,
            contract_version: CONFIDENCE_INPUT_CONTRACT_VERSION,
        })
    }
}

/// Validated append-only confidence participant content.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ConfidenceLedger {
    /// Assessments ordered by `(recorded_at, confidence_uuid)`.
    pub assessments: Vec<ConfidenceAssessment>,
    /// Inputs ordered by assessment then `(ordinal, input_confidence_uuid)`.
    pub inputs: Vec<ConfidenceInput>,
}

impl ConfidenceLedger {
    /// Validate, sort, and construct confidence content.
    pub fn new(
        mut assessments: Vec<ConfidenceAssessment>,
        mut inputs: Vec<ConfidenceInput>,
    ) -> Result<Self, KnowledgeError> {
        inputs.sort_by_key(|row| (row.confidence_uuid, row.ordinal, row.input_confidence_uuid));
        validate_confidence_rows(&assessments, &inputs)?;
        let times = assessments
            .iter()
            .map(|row| (row.confidence_uuid, row.recorded_at_micros))
            .collect::<HashMap<_, _>>();
        assessments.sort_by_key(|row| (row.recorded_at_micros, row.confidence_uuid));
        inputs.sort_by_key(|row| {
            (
                times[&row.confidence_uuid],
                row.confidence_uuid,
                row.ordinal,
                row.input_confidence_uuid,
            )
        });
        Ok(Self {
            assessments,
            inputs,
        })
    }

    /// Evaluate and stage an explicit assessment.
    pub fn explicit(
        confidence_uuid: Uuid,
        assertion_uuid: Uuid,
        value: f64,
        provenance_uuid: Uuid,
        recorded_at_micros: i64,
    ) -> Result<Self, KnowledgeError> {
        Self::new(
            vec![ConfidenceAssessment::new(
                confidence_uuid,
                assertion_uuid,
                ConfidencePolicy::Explicit,
                Some(value),
                provenance_uuid,
                recorded_at_micros,
            )?],
            vec![],
        )
    }

    /// Evaluate `conservative_min@1` and persist the normalized requested-input snapshot.
    pub fn conservative_min(
        &self,
        confidence_uuid: Uuid,
        assertion_uuid: Uuid,
        mut requested: Vec<Uuid>,
        provenance_uuid: Uuid,
        recorded_at_micros: i64,
    ) -> Result<Self, KnowledgeError> {
        requested.sort_unstable();
        if requested.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(KnowledgeError::Duplicate("input_confidence_uuid"));
        }
        let values = self
            .assessments
            .iter()
            .map(|row| (row.confidence_uuid, row.value))
            .collect::<HashMap<_, _>>();
        let mut minimum = None;
        let mut complete = !requested.is_empty();
        let mut inputs = Vec::with_capacity(requested.len());
        for (ordinal, input_uuid) in requested.into_iter().enumerate() {
            require_v7(input_uuid, "input_confidence_uuid")?;
            let observed = values.get(&input_uuid).copied().flatten();
            if let Some(value) = observed {
                minimum = Some(minimum.map_or(value, |current: f64| current.min(value)));
            } else {
                complete = false;
            }
            inputs.push(ConfidenceInput::new(
                confidence_uuid,
                input_uuid,
                observed,
                u32::try_from(ordinal).map_err(|_| KnowledgeError::Limit {
                    participant: "confidence_inputs",
                    observed: ordinal,
                    limit: u32::MAX as usize,
                })?,
            )?);
        }
        Self::new(
            vec![ConfidenceAssessment::new(
                confidence_uuid,
                assertion_uuid,
                ConfidencePolicy::ConservativeMin,
                complete.then_some(minimum).flatten(),
                provenance_uuid,
                recorded_at_micros,
            )?],
            inputs,
        )
    }

    /// Merge staged content idempotently.
    pub fn merge(&self, staged: &Self) -> Result<Self, KnowledgeError> {
        let mut assessments = self.assessments.clone();
        let mut inputs = self.inputs.clone();
        for row in &staged.assessments {
            if let Some(existing) = assessments
                .iter()
                .find(|existing| existing.confidence_uuid == row.confidence_uuid)
            {
                if existing != row
                    || inputs_for(&inputs, row.confidence_uuid)
                        != inputs_for(&staged.inputs, row.confidence_uuid)
                {
                    return Err(KnowledgeError::Conflict("confidence_uuid"));
                }
            } else {
                assessments.push(row.clone());
                inputs.extend(
                    staged
                        .inputs
                        .iter()
                        .filter(|input| input.confidence_uuid == row.confidence_uuid)
                        .cloned(),
                );
            }
        }
        Self::new(assessments, inputs)
    }

    /// Canonical assessment fingerprint over policy, normalized value, and input snapshot.
    pub fn assessment_fingerprint(
        &self,
        confidence_uuid: Uuid,
    ) -> Result<[u8; 32], KnowledgeError> {
        let row = self
            .assessments
            .iter()
            .find(|row| row.confidence_uuid == confidence_uuid)
            .ok_or(KnowledgeError::Dangling("confidence_uuid"))?;
        let inputs = inputs_for(&self.inputs, confidence_uuid);
        let mut writer = CanonicalWriter::new();
        writer.raw(b"GFCA")?;
        writer.u32(CONFIDENCE_ASSESSMENT_CONTRACT_VERSION)?;
        writer.raw(row.assertion_uuid.as_bytes())?;
        writer.text(row.policy.as_str())?;
        writer.u32(row.policy_version)?;
        canonical_optional_f64(&mut writer, row.value)?;
        writer.u64(inputs.len() as u64)?;
        for input in inputs {
            writer.raw(input.input_confidence_uuid.as_bytes())?;
            canonical_optional_f64(&mut writer, input.input_value)?;
            writer.u32(input.ordinal)?;
        }
        Ok(fingerprint(
            CanonicalDomain::ConfidenceAssessment,
            CANONICAL_CONTRACT_VERSION,
            &writer.finish(),
        )?)
    }

    /// Build the authoritative assessment Arrow batch.
    pub fn assessment_batch(&self) -> Result<RecordBatch, KnowledgeError> {
        confidence_assessment_batch(&self.assessments)
    }

    /// Build the authoritative input Arrow batch.
    pub fn input_batch(&self) -> Result<RecordBatch, KnowledgeError> {
        confidence_input_batch(&self.inputs)
    }

    /// Decode authoritative Arrow batches and re-run every invariant.
    pub fn from_batches(
        assessment_batches: &[RecordBatch],
        input_batches: &[RecordBatch],
    ) -> Result<Self, KnowledgeError> {
        let mut assessments = Vec::new();
        for batch in assessment_batches {
            require_schema(batch, &CONFIDENCE_ASSESSMENT_SCHEMA, "confidence.schema")?;
            let ids = fixed_column(batch, "confidence_uuid")?;
            let assertions = fixed_column(batch, "assertion_uuid")?;
            let policies = string_column(batch, "policy")?;
            let policy_versions = u32_column(batch, "policy_version")?;
            let values = f64_column(batch, "value")?;
            let provenance = fixed_column(batch, "provenance_uuid")?;
            let recorded = timestamp_column(batch, "recorded_at")?;
            let versions = u32_column(batch, "contract_version")?;
            for row in 0..batch.num_rows() {
                assessments.push(ConfidenceAssessment {
                    confidence_uuid: uuid_at(ids, row, "confidence_uuid")?,
                    assertion_uuid: uuid_at(assertions, row, "assertion_uuid")?,
                    policy: ConfidencePolicy::parse(required_text(policies, row, "policy")?)?,
                    policy_version: required_u32(policy_versions, row, "policy_version")?,
                    value: optional_f64(values, row),
                    provenance_uuid: uuid_at(provenance, row, "provenance_uuid")?,
                    recorded_at_micros: required_i64(recorded, row, "recorded_at")?,
                    contract_version: required_u32(versions, row, "contract_version")?,
                });
            }
        }
        let mut inputs = Vec::new();
        for batch in input_batches {
            require_schema(batch, &CONFIDENCE_INPUT_SCHEMA, "confidence_input.schema")?;
            let owners = fixed_column(batch, "confidence_uuid")?;
            let ids = fixed_column(batch, "input_confidence_uuid")?;
            let values = f64_column(batch, "input_value")?;
            let ordinals = u32_column(batch, "ordinal")?;
            let versions = u32_column(batch, "contract_version")?;
            for row in 0..batch.num_rows() {
                inputs.push(ConfidenceInput {
                    confidence_uuid: uuid_at(owners, row, "confidence_uuid")?,
                    input_confidence_uuid: uuid_at(ids, row, "input_confidence_uuid")?,
                    input_value: optional_f64(values, row),
                    ordinal: required_u32(ordinals, row, "ordinal")?,
                    contract_version: required_u32(versions, row, "contract_version")?,
                });
            }
        }
        Self::new(assessments, inputs)
    }
}

fn confidence_assessment_batch(
    rows: &[ConfidenceAssessment],
) -> Result<RecordBatch, KnowledgeError> {
    let mut ids = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    let mut assertions = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    let mut provenance = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    for row in rows {
        ids.append_value(row.confidence_uuid.as_bytes())?;
        assertions.append_value(row.assertion_uuid.as_bytes())?;
        provenance.append_value(row.provenance_uuid.as_bytes())?;
    }
    RecordBatch::try_new(
        Arc::clone(&CONFIDENCE_ASSESSMENT_SCHEMA),
        vec![
            Arc::new(ids.finish()),
            Arc::new(assertions.finish()),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.policy.as_str()),
            )),
            Arc::new(UInt32Array::from_iter_values(
                rows.iter().map(|row| row.policy_version),
            )),
            Arc::new(Float64Array::from(
                rows.iter().map(|row| row.value).collect::<Vec<_>>(),
            )),
            Arc::new(provenance.finish()),
            Arc::new(
                TimestampMicrosecondArray::from_iter_values(
                    rows.iter().map(|row| row.recorded_at_micros),
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

fn confidence_input_batch(rows: &[ConfidenceInput]) -> Result<RecordBatch, KnowledgeError> {
    let mut owners = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    let mut ids = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    for row in rows {
        owners.append_value(row.confidence_uuid.as_bytes())?;
        ids.append_value(row.input_confidence_uuid.as_bytes())?;
    }
    RecordBatch::try_new(
        Arc::clone(&CONFIDENCE_INPUT_SCHEMA),
        vec![
            Arc::new(owners.finish()),
            Arc::new(ids.finish()),
            Arc::new(Float64Array::from(
                rows.iter().map(|row| row.input_value).collect::<Vec<_>>(),
            )),
            Arc::new(UInt32Array::from_iter_values(
                rows.iter().map(|row| row.ordinal),
            )),
            Arc::new(UInt32Array::from_iter_values(
                rows.iter().map(|row| row.contract_version),
            )),
        ],
    )
    .map_err(Into::into)
}

fn validate_confidence_rows(
    assessments: &[ConfidenceAssessment],
    inputs: &[ConfidenceInput],
) -> Result<(), KnowledgeError> {
    check_limit("confidence_assessments", assessments.len())?;
    check_limit("confidence_inputs", inputs.len())?;
    let mut ids = HashSet::with_capacity(assessments.len());
    let mut policies = HashMap::with_capacity(assessments.len());
    for row in assessments {
        require_v7(row.confidence_uuid, "confidence_uuid")?;
        require_v7(row.assertion_uuid, "assertion_uuid")?;
        require_uuid(row.provenance_uuid, "provenance_uuid")?;
        validate_confidence(row.value, "value")?;
        if row.policy_version != 1 {
            return Err(invalid("policy_version", "unsupported version"));
        }
        if row.contract_version != CONFIDENCE_ASSESSMENT_CONTRACT_VERSION {
            return Err(invalid(
                "confidence.contract_version",
                "unsupported version",
            ));
        }
        if !ids.insert(row.confidence_uuid) {
            return Err(KnowledgeError::Duplicate("confidence_uuid"));
        }
        policies.insert(row.confidence_uuid, row.policy);
    }
    let mut input_ids = HashSet::with_capacity(inputs.len());
    let mut normalized_inputs: HashMap<Uuid, Vec<(u32, Uuid, Option<f64>)>> = HashMap::new();
    for input in inputs {
        require_v7(input.confidence_uuid, "confidence_uuid")?;
        require_v7(input.input_confidence_uuid, "input_confidence_uuid")?;
        validate_confidence(input.input_value, "input_value")?;
        if input.contract_version != CONFIDENCE_INPUT_CONTRACT_VERSION {
            return Err(invalid(
                "confidence_input.contract_version",
                "unsupported version",
            ));
        }
        if !ids.contains(&input.confidence_uuid) {
            return Err(KnowledgeError::Dangling("confidence_uuid"));
        }
        if !input_ids.insert((input.confidence_uuid, input.input_confidence_uuid)) {
            return Err(KnowledgeError::Duplicate("input_confidence_uuid"));
        }
        normalized_inputs
            .entry(input.confidence_uuid)
            .or_default()
            .push((
                input.ordinal,
                input.input_confidence_uuid,
                input.input_value,
            ));
    }
    for (confidence_uuid, policy) in policies {
        let assessment = assessments
            .iter()
            .find(|row| row.confidence_uuid == confidence_uuid)
            .expect("validated assessment identity");
        let values = normalized_inputs.entry(confidence_uuid).or_default();
        values.sort_by_key(|(ordinal, _, _)| *ordinal);
        if values
            .iter()
            .enumerate()
            .any(|(expected, (actual, _, _))| usize::try_from(*actual) != Ok(expected))
        {
            return Err(invalid("ordinal", "must be contiguous from zero"));
        }
        if values.windows(2).any(|pair| pair[0].1 >= pair[1].1) {
            return Err(invalid(
                "input_confidence_uuid",
                "must be unique UUID-normalized order",
            ));
        }
        validate_policy_snapshot(assessment, policy, values)?;
    }
    Ok(())
}

fn validate_policy_snapshot(
    assessment: &ConfidenceAssessment,
    policy: ConfidencePolicy,
    values: &[(u32, Uuid, Option<f64>)],
) -> Result<(), KnowledgeError> {
    match policy {
        ConfidencePolicy::Explicit => {
            if !values.is_empty() {
                return Err(invalid(
                    "confidence_inputs",
                    "explicit policy has no inputs",
                ));
            }
            if assessment.value.is_none() {
                return Err(invalid("value", "explicit policy requires a value"));
            }
        }
        ConfidencePolicy::ConservativeMin => {
            let expected =
                if values.is_empty() || values.iter().any(|(_, _, value)| value.is_none()) {
                    None
                } else {
                    values
                        .iter()
                        .filter_map(|(_, _, value)| *value)
                        .reduce(f64::min)
                };
            if assessment.value != expected {
                return Err(invalid(
                    "value",
                    "does not match conservative_min input snapshot",
                ));
            }
        }
    }
    Ok(())
}

fn inputs_for(rows: &[ConfidenceInput], confidence_uuid: Uuid) -> Vec<ConfidenceInput> {
    let mut inputs = rows
        .iter()
        .filter(|row| row.confidence_uuid == confidence_uuid)
        .cloned()
        .collect::<Vec<_>>();
    inputs.sort_by_key(|row| (row.ordinal, row.input_confidence_uuid));
    inputs
}

#[cfg(test)]
mod tests;
