//! Append-only epistemic assertion supersession relations.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock};

use arrow::array::{
    Array, FixedSizeBinaryArray, FixedSizeBinaryBuilder, TimestampMicrosecondArray,
    TimestampMicrosecondBuilder, UInt32Array, UInt32Builder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use graphforge_core::canonical::{
    CANONICAL_CONTRACT_VERSION, CanonicalDomain, CanonicalWriter, fingerprint,
};
use uuid::{Uuid, Version};

use crate::{
    EPISTEMIC_CAPABILITY_VERSION, KnowledgeError, MAX_KNOWLEDGE_ROWS, SchemaRegistryEntry,
};

/// Assertion-supersession record contract.
pub const ASSERTION_SUPERSESSION_CONTRACT_VERSION: u32 = 1;
/// Branch-preserving, non-selecting supersession policy.
pub const ASSERTION_SUPERSESSION_POLICY_VERSION: u32 = 1;

/// Authoritative `knowledge/assertion_supersessions.parquet` schema.
pub static ASSERTION_SUPERSESSION_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        uuid_field("supersession_uuid"),
        uuid_field("prior_assertion_uuid"),
        uuid_field("replacement_assertion_uuid"),
        uuid_field("status_event_uuid"),
        uuid_field("reasoning_uuid"),
        uuid_field("provenance_uuid"),
        Field::new(
            "recorded_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("contract_version", DataType::UInt32, false),
    ]))
});

static ASSERTION_SUPERSESSION_SCHEMA_FINGERPRINT: LazyLock<[u8; 32]> = LazyLock::new(|| {
    fingerprint(
        CanonicalDomain::AssertionSupersession,
        CANONICAL_CONTRACT_VERSION,
        b"assertion_supersession/1|supersession_uuid:fixed[16]:required|prior_assertion_uuid:fixed[16]:required|replacement_assertion_uuid:fixed[16]:required|status_event_uuid:fixed[16]:required|reasoning_uuid:fixed[16]:required|provenance_uuid:fixed[16]:required|recorded_at:timestamp_us_utc:required|contract_version:u32:required",
    )
    .expect("registered assertion-supersession schema is within canonical bounds")
});

/// One immutable assertion-supersession relation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AssertionSupersession {
    /// Caller-supplied UUIDv7 identity/idempotency key.
    pub supersession_uuid: Uuid,
    /// Existing assertion that becomes explicitly superseded.
    pub prior_assertion_uuid: Uuid,
    /// Existing replacement assertion.
    pub replacement_assertion_uuid: Uuid,
    /// Exact paired `superseded` status-event UUID.
    pub status_event_uuid: Uuid,
    /// Existing reasoning record attached to the prior assertion.
    pub reasoning_uuid: Uuid,
    /// Producing provenance event.
    pub provenance_uuid: Uuid,
    /// Transaction time shared with the paired status event.
    pub recorded_at_micros: i64,
    /// Frozen record contract.
    pub contract_version: u32,
}

impl AssertionSupersession {
    /// Validate and construct one immutable relation.
    pub fn new(
        supersession_uuid: Uuid,
        prior_assertion_uuid: Uuid,
        replacement_assertion_uuid: Uuid,
        status_event_uuid: Uuid,
        reasoning_uuid: Uuid,
        provenance_uuid: Uuid,
        recorded_at_micros: i64,
    ) -> Result<Self, KnowledgeError> {
        for (value, field) in [
            (supersession_uuid, "supersession_uuid"),
            (prior_assertion_uuid, "prior_assertion_uuid"),
            (replacement_assertion_uuid, "replacement_assertion_uuid"),
            (status_event_uuid, "status_event_uuid"),
            (reasoning_uuid, "reasoning_uuid"),
        ] {
            require_v7(value, field)?;
        }
        require_uuid(provenance_uuid, "provenance_uuid")?;
        if prior_assertion_uuid == replacement_assertion_uuid {
            return Err(invalid(
                "replacement_assertion_uuid",
                "must differ from prior_assertion_uuid",
            ));
        }
        Ok(Self {
            supersession_uuid,
            prior_assertion_uuid,
            replacement_assertion_uuid,
            status_event_uuid,
            reasoning_uuid,
            provenance_uuid,
            recorded_at_micros,
            contract_version: ASSERTION_SUPERSESSION_CONTRACT_VERSION,
        })
    }
}

/// Validated branch-preserving supersession participant.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AssertionSupersessionLedger {
    /// Relations ordered by `(recorded_at, supersession_uuid)`.
    relations: Vec<AssertionSupersession>,
}

impl AssertionSupersessionLedger {
    /// Return validated relations in canonical order.
    #[must_use]
    pub fn relations(&self) -> &[AssertionSupersession] {
        &self.relations
    }

    /// Validate, sort, and construct one complete participant.
    pub fn new(mut relations: Vec<AssertionSupersession>) -> Result<Self, KnowledgeError> {
        if relations.len() > MAX_KNOWLEDGE_ROWS {
            return Err(KnowledgeError::Limit {
                participant: "assertion_supersessions",
                observed: relations.len(),
                limit: MAX_KNOWLEDGE_ROWS,
            });
        }
        let mut ids = HashSet::with_capacity(relations.len());
        let mut status_ids = HashSet::with_capacity(relations.len());
        for relation in &relations {
            validate_relation(relation)?;
            if !ids.insert(relation.supersession_uuid) {
                return Err(KnowledgeError::Duplicate("supersession_uuid"));
            }
            if !status_ids.insert(relation.status_event_uuid) {
                return Err(KnowledgeError::Duplicate("status_event_uuid"));
            }
        }
        validate_acyclic(&relations)?;
        relations.sort_by_key(|row| (row.recorded_at_micros, row.supersession_uuid));
        Ok(Self { relations })
    }

    /// Merge append-only relations with exact replay semantics.
    pub fn merge(&self, staged: &Self) -> Result<Self, KnowledgeError> {
        let mut relations = self.relations.clone();
        let mut by_id = relations
            .iter()
            .cloned()
            .map(|row| (row.supersession_uuid, row))
            .collect::<HashMap<_, _>>();
        for relation in &staged.relations {
            if let Some(existing) = by_id.get(&relation.supersession_uuid) {
                if existing != relation {
                    return Err(KnowledgeError::Conflict("supersession_uuid"));
                }
            } else {
                relations.push(relation.clone());
                by_id.insert(relation.supersession_uuid, relation.clone());
            }
        }
        Self::new(relations)
    }

    /// Canonical fingerprint over one exact immutable relation.
    pub fn relation_fingerprint(
        &self,
        supersession_uuid: Uuid,
    ) -> Result<[u8; 32], KnowledgeError> {
        let row = self
            .relations
            .iter()
            .find(|row| row.supersession_uuid == supersession_uuid)
            .ok_or(KnowledgeError::Dangling("supersession_uuid"))?;
        let mut writer = CanonicalWriter::new();
        for value in [
            row.supersession_uuid,
            row.prior_assertion_uuid,
            row.replacement_assertion_uuid,
            row.status_event_uuid,
            row.reasoning_uuid,
            row.provenance_uuid,
        ] {
            writer.raw(value.as_bytes())?;
        }
        writer.i64(row.recorded_at_micros)?;
        writer.u32(row.contract_version)?;
        fingerprint(
            CanonicalDomain::AssertionSupersession,
            CANONICAL_CONTRACT_VERSION,
            &writer.finish(),
        )
        .map_err(Into::into)
    }

    /// Build the authoritative Arrow batch.
    pub fn batch(&self) -> Result<RecordBatch, KnowledgeError> {
        let len = self.relations.len();
        let mut supersessions = FixedSizeBinaryBuilder::with_capacity(len, 16);
        let mut priors = FixedSizeBinaryBuilder::with_capacity(len, 16);
        let mut replacements = FixedSizeBinaryBuilder::with_capacity(len, 16);
        let mut statuses = FixedSizeBinaryBuilder::with_capacity(len, 16);
        let mut reasoning = FixedSizeBinaryBuilder::with_capacity(len, 16);
        let mut provenance = FixedSizeBinaryBuilder::with_capacity(len, 16);
        let mut times = TimestampMicrosecondBuilder::with_capacity(len).with_timezone("UTC");
        let mut versions = UInt32Builder::with_capacity(len);
        for row in &self.relations {
            for (builder, value, field) in [
                (
                    &mut supersessions,
                    row.supersession_uuid,
                    "supersession_uuid",
                ),
                (
                    &mut priors,
                    row.prior_assertion_uuid,
                    "prior_assertion_uuid",
                ),
                (
                    &mut replacements,
                    row.replacement_assertion_uuid,
                    "replacement_assertion_uuid",
                ),
                (&mut statuses, row.status_event_uuid, "status_event_uuid"),
                (&mut reasoning, row.reasoning_uuid, "reasoning_uuid"),
                (&mut provenance, row.provenance_uuid, "provenance_uuid"),
            ] {
                builder
                    .append_value(value.as_bytes())
                    .map_err(|_| invalid(field, "invalid UUID width"))?;
            }
            times.append_value(row.recorded_at_micros);
            versions.append_value(row.contract_version);
        }
        RecordBatch::try_new(
            Arc::clone(&ASSERTION_SUPERSESSION_SCHEMA),
            vec![
                Arc::new(supersessions.finish()),
                Arc::new(priors.finish()),
                Arc::new(replacements.finish()),
                Arc::new(statuses.finish()),
                Arc::new(reasoning.finish()),
                Arc::new(provenance.finish()),
                Arc::new(times.finish()),
                Arc::new(versions.finish()),
            ],
        )
        .map_err(|_| invalid("assertion_supersession", "Arrow batch construction failed"))
    }

    /// Decode and validate exact Arrow batches.
    pub fn from_batches(batches: &[RecordBatch]) -> Result<Self, KnowledgeError> {
        let mut relations = Vec::new();
        for batch in batches {
            if batch.schema().as_ref() != ASSERTION_SUPERSESSION_SCHEMA.as_ref() {
                return Err(invalid("assertion_supersession.schema", "schema mismatch"));
            }
            let supersessions = fixed(batch, "supersession_uuid")?;
            let priors = fixed(batch, "prior_assertion_uuid")?;
            let replacements = fixed(batch, "replacement_assertion_uuid")?;
            let statuses = fixed(batch, "status_event_uuid")?;
            let reasoning = fixed(batch, "reasoning_uuid")?;
            let provenance = fixed(batch, "provenance_uuid")?;
            let times = timestamp(batch, "recorded_at")?;
            let versions = uint32(batch, "contract_version")?;
            for row in 0..batch.num_rows() {
                relations.push(AssertionSupersession {
                    supersession_uuid: uuid_at(supersessions, row, "supersession_uuid")?,
                    prior_assertion_uuid: uuid_at(priors, row, "prior_assertion_uuid")?,
                    replacement_assertion_uuid: uuid_at(
                        replacements,
                        row,
                        "replacement_assertion_uuid",
                    )?,
                    status_event_uuid: uuid_at(statuses, row, "status_event_uuid")?,
                    reasoning_uuid: uuid_at(reasoning, row, "reasoning_uuid")?,
                    provenance_uuid: uuid_at(provenance, row, "provenance_uuid")?,
                    recorded_at_micros: times.value(row),
                    contract_version: versions.value(row),
                });
            }
        }
        Self::new(relations)
    }
}

pub(crate) fn schema_registry_entry() -> SchemaRegistryEntry {
    SchemaRegistryEntry {
        capability_id: "epistemic",
        capability_version: EPISTEMIC_CAPABILITY_VERSION,
        record_family: "assertion_supersessions",
        record_version: ASSERTION_SUPERSESSION_CONTRACT_VERSION,
        schema: Arc::clone(&ASSERTION_SUPERSESSION_SCHEMA),
        schema_fingerprint: *ASSERTION_SUPERSESSION_SCHEMA_FINGERPRINT,
        enum_registry_versions: &[(
            "assertion_supersession_policy",
            ASSERTION_SUPERSESSION_POLICY_VERSION,
        )],
        sort_key: &["recorded_at", "supersession_uuid"],
        diff_identity_fields: &["supersession_uuid"],
        diff_record_uuid_field: Some("supersession_uuid"),
        fingerprint_domain: CanonicalDomain::AssertionSupersession,
        owner: "graphforge-knowledge",
        implementation_issue: 778,
        max_rows: MAX_KNOWLEDGE_ROWS,
    }
}

fn validate_acyclic(relations: &[AssertionSupersession]) -> Result<(), KnowledgeError> {
    let mut adjacency = HashMap::<Uuid, Vec<Uuid>>::new();
    for row in relations {
        adjacency
            .entry(row.prior_assertion_uuid)
            .or_default()
            .push(row.replacement_assertion_uuid);
    }
    let mut visiting = HashSet::new();
    let mut acyclic = HashSet::new();
    for node in adjacency.keys().copied() {
        if !visit(node, &adjacency, &mut visiting, &mut acyclic) {
            return Err(invalid("assertion_supersession", "cycle detected"));
        }
    }
    Ok(())
}

fn visit(
    node: Uuid,
    adjacency: &HashMap<Uuid, Vec<Uuid>>,
    visiting: &mut HashSet<Uuid>,
    acyclic: &mut HashSet<Uuid>,
) -> bool {
    if acyclic.contains(&node) {
        return true;
    }
    if !visiting.insert(node) {
        return false;
    }
    if adjacency.get(&node).is_some_and(|next| {
        next.iter()
            .any(|child| !visit(*child, adjacency, visiting, acyclic))
    }) {
        return false;
    }
    visiting.remove(&node);
    acyclic.insert(node);
    true
}

fn validate_relation(row: &AssertionSupersession) -> Result<(), KnowledgeError> {
    if row.contract_version != ASSERTION_SUPERSESSION_CONTRACT_VERSION {
        return Err(invalid(
            "assertion_supersession.contract_version",
            "unsupported version",
        ));
    }
    AssertionSupersession::new(
        row.supersession_uuid,
        row.prior_assertion_uuid,
        row.replacement_assertion_uuid,
        row.status_event_uuid,
        row.reasoning_uuid,
        row.provenance_uuid,
        row.recorded_at_micros,
    )
    .map(|_| ())
}

fn require_v7(value: Uuid, field: &'static str) -> Result<(), KnowledgeError> {
    if value.get_version() != Some(Version::SortRand) {
        return Err(invalid(field, "must be UUIDv7"));
    }
    require_uuid(value, field)
}

fn require_uuid(value: Uuid, field: &'static str) -> Result<(), KnowledgeError> {
    if value.is_nil() {
        return Err(invalid(field, "must not be nil"));
    }
    Ok(())
}

const fn invalid(field: &'static str, message: &'static str) -> KnowledgeError {
    KnowledgeError::Invalid { field, message }
}

fn uuid_field(name: &str) -> Field {
    Field::new(name, DataType::FixedSizeBinary(16), false)
}

fn fixed<'a>(
    batch: &'a RecordBatch,
    name: &'static str,
) -> Result<&'a FixedSizeBinaryArray, KnowledgeError> {
    batch
        .column_by_name(name)
        .and_then(|value| value.as_any().downcast_ref())
        .ok_or_else(|| invalid(name, "column type mismatch"))
}

fn timestamp<'a>(
    batch: &'a RecordBatch,
    name: &'static str,
) -> Result<&'a TimestampMicrosecondArray, KnowledgeError> {
    batch
        .column_by_name(name)
        .and_then(|value| value.as_any().downcast_ref())
        .ok_or_else(|| invalid(name, "column type mismatch"))
}

fn uint32<'a>(
    batch: &'a RecordBatch,
    name: &'static str,
) -> Result<&'a UInt32Array, KnowledgeError> {
    batch
        .column_by_name(name)
        .and_then(|value| value.as_any().downcast_ref())
        .ok_or_else(|| invalid(name, "column type mismatch"))
}

fn uuid_at(
    values: &FixedSizeBinaryArray,
    row: usize,
    field: &'static str,
) -> Result<Uuid, KnowledgeError> {
    Uuid::from_slice(values.value(row)).map_err(|_| invalid(field, "invalid UUID bytes"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn uuid7(seed: u8) -> Uuid {
        let mut bytes = [seed; 16];
        bytes[6] = (bytes[6] & 0x0f) | 0x70;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        Uuid::from_bytes(bytes)
    }

    fn relation(id: u8, prior: u8, replacement: u8, time: i64) -> AssertionSupersession {
        AssertionSupersession::new(
            uuid7(id),
            uuid7(prior),
            uuid7(replacement),
            uuid7(id.wrapping_add(40)),
            uuid7(id.wrapping_add(80)),
            uuid7(id.wrapping_add(120)),
            time,
        )
        .unwrap()
    }

    #[test]
    fn chains_branches_order_round_trip_and_fingerprint_are_stable() {
        let ledger = AssertionSupersessionLedger::new(vec![
            relation(3, 10, 12, 2),
            relation(2, 10, 11, 1),
            relation(4, 11, 13, 2),
        ])
        .unwrap();
        assert_eq!(ledger.relations()[0].supersession_uuid, uuid7(2));
        let decoded =
            AssertionSupersessionLedger::from_batches(&[ledger.batch().unwrap()]).unwrap();
        assert_eq!(decoded, ledger);
        assert_eq!(
            decoded.relation_fingerprint(uuid7(3)).unwrap(),
            ledger.relation_fingerprint(uuid7(3)).unwrap()
        );
    }

    #[test]
    fn self_links_cycles_duplicate_statuses_and_conflicts_fail() {
        assert!(
            AssertionSupersession::new(
                uuid7(1),
                uuid7(2),
                uuid7(2),
                uuid7(3),
                uuid7(4),
                uuid7(5),
                1,
            )
            .is_err()
        );
        assert!(
            AssertionSupersessionLedger::new(vec![relation(1, 10, 11, 1), relation(2, 11, 10, 2),])
                .is_err()
        );
        let mut duplicate_status = relation(2, 11, 12, 2);
        duplicate_status.status_event_uuid = uuid7(41);
        assert!(
            AssertionSupersessionLedger::new(vec![relation(1, 10, 11, 1), duplicate_status,])
                .is_err()
        );
        let base = AssertionSupersessionLedger::new(vec![relation(1, 10, 11, 1)]).unwrap();
        assert_eq!(base.merge(&base).unwrap(), base);
        let conflict = AssertionSupersessionLedger::new(vec![relation(1, 10, 12, 1)]).unwrap();
        assert!(matches!(
            base.merge(&conflict),
            Err(KnowledgeError::Conflict("supersession_uuid"))
        ));
    }
}
