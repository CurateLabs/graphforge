//! Append-only M21 hypothesis groups, membership, and explicit selection.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock};

use arrow::array::{
    Array, FixedSizeBinaryArray, FixedSizeBinaryBuilder, StringArray, StringBuilder,
    TimestampMicrosecondArray, TimestampMicrosecondBuilder, UInt32Array, UInt32Builder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use graphforge_core::canonical::{
    CANONICAL_CONTRACT_VERSION, CanonicalDomain, CanonicalWriter, fingerprint,
};
use unicode_normalization::UnicodeNormalization;
use uuid::{Uuid, Version};

use crate::{
    EPISTEMIC_CAPABILITY_VERSION, KnowledgeError, MAX_KNOWLEDGE_ROWS, SchemaRegistryEntry,
};

/// Frozen hypothesis-group record contract.
pub const HYPOTHESIS_GROUP_CONTRACT_VERSION: u32 = 1;
/// Frozen membership-event record contract.
pub const HYPOTHESIS_MEMBERSHIP_CONTRACT_VERSION: u32 = 1;
/// Frozen selection-event record contract.
pub const HYPOTHESIS_SELECTION_CONTRACT_VERSION: u32 = 1;
/// Canonical group-key policy.
pub const HYPOTHESIS_KEY_POLICY_VERSION: u32 = 1;
/// Membership/selection state policy.
pub const HYPOTHESIS_STATE_POLICY_VERSION: u32 = 1;
/// Maximum canonical question-key bytes.
pub const MAX_HYPOTHESIS_QUESTION_KEY_BYTES: usize = 1_024;

/// Authoritative group schema.
pub static HYPOTHESIS_GROUP_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        uuid_field("group_uuid", false),
        Field::new("question_key", DataType::Utf8, false),
        uuid_field("provenance_uuid", false),
        timestamp_field("recorded_at"),
        Field::new("contract_version", DataType::UInt32, false),
    ]))
});

/// Authoritative membership-event schema.
pub static HYPOTHESIS_MEMBERSHIP_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        uuid_field("membership_event_uuid", false),
        uuid_field("operation_uuid", false),
        uuid_field("group_uuid", false),
        uuid_field("assertion_uuid", false),
        Field::new("action", DataType::Utf8, false),
        uuid_field("reasoning_uuid", false),
        uuid_field("provenance_uuid", false),
        timestamp_field("recorded_at"),
        Field::new("contract_version", DataType::UInt32, false),
    ]))
});

/// Authoritative selection-event schema.
pub static HYPOTHESIS_SELECTION_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        uuid_field("selection_event_uuid", false),
        uuid_field("operation_uuid", false),
        uuid_field("group_uuid", false),
        uuid_field("selected_assertion_uuid", true),
        uuid_field("reasoning_uuid", false),
        uuid_field("provenance_uuid", false),
        timestamp_field("recorded_at"),
        Field::new("contract_version", DataType::UInt32, false),
    ]))
});

static GROUP_SCHEMA_FINGERPRINT: LazyLock<[u8; 32]> = LazyLock::new(|| {
    fingerprint(
        CanonicalDomain::Schema,
        CANONICAL_CONTRACT_VERSION,
        b"hypothesis_group/1|group_uuid:fixed[16]:required|question_key:utf8:required|provenance_uuid:fixed[16]:required|recorded_at:timestamp_us_utc:required|contract_version:u32:required",
    )
    .expect("registered hypothesis-group schema is within canonical bounds")
});

static MEMBERSHIP_SCHEMA_FINGERPRINT: LazyLock<[u8; 32]> = LazyLock::new(|| {
    fingerprint(
        CanonicalDomain::Schema,
        CANONICAL_CONTRACT_VERSION,
        b"hypothesis_membership/1|membership_event_uuid:fixed[16]:required|operation_uuid:fixed[16]:required|group_uuid:fixed[16]:required|assertion_uuid:fixed[16]:required|action:utf8:required|reasoning_uuid:fixed[16]:required|provenance_uuid:fixed[16]:required|recorded_at:timestamp_us_utc:required|contract_version:u32:required",
    )
    .expect("registered hypothesis-membership schema is within canonical bounds")
});

static SELECTION_SCHEMA_FINGERPRINT: LazyLock<[u8; 32]> = LazyLock::new(|| {
    fingerprint(
        CanonicalDomain::Schema,
        CANONICAL_CONTRACT_VERSION,
        b"hypothesis_selection/1|selection_event_uuid:fixed[16]:required|operation_uuid:fixed[16]:required|group_uuid:fixed[16]:required|selected_assertion_uuid:fixed[16]:nullable|reasoning_uuid:fixed[16]:required|provenance_uuid:fixed[16]:required|recorded_at:timestamp_us_utc:required|contract_version:u32:required",
    )
    .expect("registered hypothesis-selection schema is within canonical bounds")
});

/// Closed membership action.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum HypothesisMembershipAction {
    /// Make an assertion a current group member.
    Added,
    /// Remove an assertion from current membership.
    Removed,
}

impl HypothesisMembershipAction {
    /// Stable persisted spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Added => "added",
            Self::Removed => "removed",
        }
    }

    fn parse(value: &str) -> Result<Self, KnowledgeError> {
        match value {
            "added" => Ok(Self::Added),
            "removed" => Ok(Self::Removed),
            _ => Err(invalid(
                "hypothesis_membership.action",
                "unknown registry value",
            )),
        }
    }
}

/// One immutable hypothesis group.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HypothesisGroup {
    /// Caller-supplied UUIDv7 group identity.
    pub group_uuid: Uuid,
    /// Canonical question key.
    pub question_key: String,
    /// Existing producing provenance event.
    pub provenance_uuid: Uuid,
    /// Mandatory transaction time.
    pub recorded_at_micros: i64,
    /// Frozen record contract.
    pub contract_version: u32,
}

impl HypothesisGroup {
    /// Validate and construct one canonical group.
    pub fn new(
        group_uuid: Uuid,
        question_key: String,
        provenance_uuid: Uuid,
        recorded_at_micros: i64,
    ) -> Result<Self, KnowledgeError> {
        require_v7(group_uuid, "group_uuid")?;
        require_uuid(provenance_uuid, "provenance_uuid")?;
        validate_question_key(&question_key)?;
        Ok(Self {
            group_uuid,
            question_key,
            provenance_uuid,
            recorded_at_micros,
            contract_version: HYPOTHESIS_GROUP_CONTRACT_VERSION,
        })
    }
}

/// One immutable membership transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HypothesisMembershipEvent {
    /// Caller-supplied UUIDv7 event identity.
    pub membership_event_uuid: Uuid,
    /// Publication operation identity.
    pub operation_uuid: Uuid,
    /// Existing hypothesis group.
    pub group_uuid: Uuid,
    /// Existing immutable assertion.
    pub assertion_uuid: Uuid,
    /// Explicit add/remove action.
    pub action: HypothesisMembershipAction,
    /// Existing decision rationale.
    pub reasoning_uuid: Uuid,
    /// Existing producing provenance event.
    pub provenance_uuid: Uuid,
    /// Mandatory transaction time.
    pub recorded_at_micros: i64,
    /// Frozen record contract.
    pub contract_version: u32,
}

impl HypothesisMembershipEvent {
    /// Validate and construct one membership event.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        membership_event_uuid: Uuid,
        operation_uuid: Uuid,
        group_uuid: Uuid,
        assertion_uuid: Uuid,
        action: HypothesisMembershipAction,
        reasoning_uuid: Uuid,
        provenance_uuid: Uuid,
        recorded_at_micros: i64,
    ) -> Result<Self, KnowledgeError> {
        for (uuid, field) in [
            (membership_event_uuid, "membership_event_uuid"),
            (operation_uuid, "operation_uuid"),
            (group_uuid, "group_uuid"),
            (assertion_uuid, "assertion_uuid"),
            (reasoning_uuid, "reasoning_uuid"),
        ] {
            require_v7(uuid, field)?;
        }
        require_uuid(provenance_uuid, "provenance_uuid")?;
        Ok(Self {
            membership_event_uuid,
            operation_uuid,
            group_uuid,
            assertion_uuid,
            action,
            reasoning_uuid,
            provenance_uuid,
            recorded_at_micros,
            contract_version: HYPOTHESIS_MEMBERSHIP_CONTRACT_VERSION,
        })
    }
}

/// One immutable explicit selection or clear event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HypothesisSelectionEvent {
    /// Caller-supplied UUIDv7 event identity.
    pub selection_event_uuid: Uuid,
    /// Publication operation identity.
    pub operation_uuid: Uuid,
    /// Existing hypothesis group.
    pub group_uuid: Uuid,
    /// Explicit selected member, or `None` to clear selection.
    pub selected_assertion_uuid: Option<Uuid>,
    /// Existing decision rationale.
    pub reasoning_uuid: Uuid,
    /// Existing producing provenance event.
    pub provenance_uuid: Uuid,
    /// Mandatory transaction time.
    pub recorded_at_micros: i64,
    /// Frozen record contract.
    pub contract_version: u32,
}

impl HypothesisSelectionEvent {
    /// Validate and construct one explicit selection event.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        selection_event_uuid: Uuid,
        operation_uuid: Uuid,
        group_uuid: Uuid,
        selected_assertion_uuid: Option<Uuid>,
        reasoning_uuid: Uuid,
        provenance_uuid: Uuid,
        recorded_at_micros: i64,
    ) -> Result<Self, KnowledgeError> {
        for (uuid, field) in [
            (selection_event_uuid, "selection_event_uuid"),
            (operation_uuid, "operation_uuid"),
            (group_uuid, "group_uuid"),
            (reasoning_uuid, "reasoning_uuid"),
        ] {
            require_v7(uuid, field)?;
        }
        if let Some(assertion_uuid) = selected_assertion_uuid {
            require_v7(assertion_uuid, "selected_assertion_uuid")?;
        }
        require_uuid(provenance_uuid, "provenance_uuid")?;
        Ok(Self {
            selection_event_uuid,
            operation_uuid,
            group_uuid,
            selected_assertion_uuid,
            reasoning_uuid,
            provenance_uuid,
            recorded_at_micros,
            contract_version: HYPOTHESIS_SELECTION_CONTRACT_VERSION,
        })
    }
}

/// Validated complete hypothesis state participants.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HypothesisLedger {
    groups: Vec<HypothesisGroup>,
    membership_events: Vec<HypothesisMembershipEvent>,
    selection_events: Vec<HypothesisSelectionEvent>,
}

impl HypothesisLedger {
    /// Validate, order, and construct all three record families together.
    pub fn new(
        mut groups: Vec<HypothesisGroup>,
        mut membership_events: Vec<HypothesisMembershipEvent>,
        mut selection_events: Vec<HypothesisSelectionEvent>,
    ) -> Result<Self, KnowledgeError> {
        for (participant, observed) in [
            ("hypothesis_groups", groups.len()),
            ("hypothesis_membership_events", membership_events.len()),
            ("hypothesis_selection_events", selection_events.len()),
        ] {
            if observed > MAX_KNOWLEDGE_ROWS {
                return Err(KnowledgeError::Limit {
                    participant,
                    observed,
                    limit: MAX_KNOWLEDGE_ROWS,
                });
            }
        }
        validate_groups(&groups)?;
        validate_event_ids(&membership_events, &selection_events)?;
        groups.sort_by_key(|row| (row.recorded_at_micros, row.group_uuid));
        membership_events.sort_by_key(|row| (row.recorded_at_micros, row.membership_event_uuid));
        selection_events.sort_by_key(|row| (row.recorded_at_micros, row.selection_event_uuid));
        validate_state(&groups, &membership_events, &selection_events)?;
        Ok(Self {
            groups,
            membership_events,
            selection_events,
        })
    }

    #[must_use]
    /// Return validated groups in canonical order.
    pub fn groups(&self) -> &[HypothesisGroup] {
        &self.groups
    }

    #[must_use]
    /// Return validated membership history in canonical order.
    pub fn membership_events(&self) -> &[HypothesisMembershipEvent] {
        &self.membership_events
    }

    #[must_use]
    /// Return validated selection history in canonical order.
    pub fn selection_events(&self) -> &[HypothesisSelectionEvent] {
        &self.selection_events
    }

    /// Canonical fingerprint over one exact immutable hypothesis group.
    pub fn group_fingerprint(&self, group_uuid: Uuid) -> Result<[u8; 32], KnowledgeError> {
        let row = self
            .groups
            .iter()
            .find(|row| row.group_uuid == group_uuid)
            .ok_or(KnowledgeError::Dangling("group_uuid"))?;
        let mut writer = CanonicalWriter::new();
        writer.raw(row.group_uuid.as_bytes())?;
        writer.text(&row.question_key)?;
        writer.raw(row.provenance_uuid.as_bytes())?;
        writer.i64(row.recorded_at_micros)?;
        writer.u32(row.contract_version)?;
        fingerprint(
            CanonicalDomain::HypothesisGroup,
            CANONICAL_CONTRACT_VERSION,
            &writer.finish(),
        )
        .map_err(Into::into)
    }

    /// Canonical fingerprint over one exact membership event.
    pub fn membership_fingerprint(
        &self,
        membership_event_uuid: Uuid,
    ) -> Result<[u8; 32], KnowledgeError> {
        let row = self
            .membership_events
            .iter()
            .find(|row| row.membership_event_uuid == membership_event_uuid)
            .ok_or(KnowledgeError::Dangling("membership_event_uuid"))?;
        let mut writer = CanonicalWriter::new();
        for value in [
            row.membership_event_uuid,
            row.operation_uuid,
            row.group_uuid,
            row.assertion_uuid,
        ] {
            writer.raw(value.as_bytes())?;
        }
        writer.text(row.action.as_str())?;
        writer.raw(row.reasoning_uuid.as_bytes())?;
        writer.raw(row.provenance_uuid.as_bytes())?;
        writer.i64(row.recorded_at_micros)?;
        writer.u32(row.contract_version)?;
        fingerprint(
            CanonicalDomain::HypothesisMembership,
            CANONICAL_CONTRACT_VERSION,
            &writer.finish(),
        )
        .map_err(Into::into)
    }

    /// Canonical fingerprint over one exact selection event.
    pub fn selection_fingerprint(
        &self,
        selection_event_uuid: Uuid,
    ) -> Result<[u8; 32], KnowledgeError> {
        let row = self
            .selection_events
            .iter()
            .find(|row| row.selection_event_uuid == selection_event_uuid)
            .ok_or(KnowledgeError::Dangling("selection_event_uuid"))?;
        let mut writer = CanonicalWriter::new();
        for value in [row.selection_event_uuid, row.operation_uuid, row.group_uuid] {
            writer.raw(value.as_bytes())?;
        }
        match row.selected_assertion_uuid {
            Some(value) => {
                writer.u8(1)?;
                writer.raw(value.as_bytes())?;
            }
            None => writer.u8(0)?,
        }
        writer.raw(row.reasoning_uuid.as_bytes())?;
        writer.raw(row.provenance_uuid.as_bytes())?;
        writer.i64(row.recorded_at_micros)?;
        writer.u32(row.contract_version)?;
        fingerprint(
            CanonicalDomain::HypothesisSelection,
            CANONICAL_CONTRACT_VERSION,
            &writer.finish(),
        )
        .map_err(Into::into)
    }

    /// Merge append-only participants with exact replay semantics.
    pub fn merge(&self, staged: &Self) -> Result<Self, KnowledgeError> {
        Self::new(
            merge_rows(
                &self.groups,
                &staged.groups,
                |row| row.group_uuid,
                "group_uuid",
            )?,
            merge_rows(
                &self.membership_events,
                &staged.membership_events,
                |row| row.membership_event_uuid,
                "membership_event_uuid",
            )?,
            merge_rows(
                &self.selection_events,
                &staged.selection_events,
                |row| row.selection_event_uuid,
                "selection_event_uuid",
            )?,
        )
    }

    /// Build the authoritative group batch.
    pub fn group_batch(&self) -> Result<RecordBatch, KnowledgeError> {
        let len = self.groups.len();
        let mut ids = FixedSizeBinaryBuilder::with_capacity(len, 16);
        let mut keys = StringBuilder::with_capacity(
            len,
            self.groups.iter().map(|r| r.question_key.len()).sum(),
        );
        let mut provenance = FixedSizeBinaryBuilder::with_capacity(len, 16);
        let mut times = TimestampMicrosecondBuilder::with_capacity(len).with_timezone("UTC");
        let mut versions = UInt32Builder::with_capacity(len);
        for row in &self.groups {
            append_uuid(&mut ids, row.group_uuid, "group_uuid")?;
            keys.append_value(&row.question_key);
            append_uuid(&mut provenance, row.provenance_uuid, "provenance_uuid")?;
            times.append_value(row.recorded_at_micros);
            versions.append_value(row.contract_version);
        }
        record_batch(
            Arc::clone(&HYPOTHESIS_GROUP_SCHEMA),
            vec![
                Arc::new(ids.finish()),
                Arc::new(keys.finish()),
                Arc::new(provenance.finish()),
                Arc::new(times.finish()),
                Arc::new(versions.finish()),
            ],
        )
    }

    /// Build the authoritative membership-history batch.
    pub fn membership_batch(&self) -> Result<RecordBatch, KnowledgeError> {
        let len = self.membership_events.len();
        let mut ids = FixedSizeBinaryBuilder::with_capacity(len, 16);
        let mut operations = FixedSizeBinaryBuilder::with_capacity(len, 16);
        let mut groups = FixedSizeBinaryBuilder::with_capacity(len, 16);
        let mut assertions = FixedSizeBinaryBuilder::with_capacity(len, 16);
        let mut actions = StringBuilder::with_capacity(len, len * 7);
        let mut reasoning = FixedSizeBinaryBuilder::with_capacity(len, 16);
        let mut provenance = FixedSizeBinaryBuilder::with_capacity(len, 16);
        let mut times = TimestampMicrosecondBuilder::with_capacity(len).with_timezone("UTC");
        let mut versions = UInt32Builder::with_capacity(len);
        for row in &self.membership_events {
            append_uuid(&mut ids, row.membership_event_uuid, "membership_event_uuid")?;
            append_uuid(&mut operations, row.operation_uuid, "operation_uuid")?;
            append_uuid(&mut groups, row.group_uuid, "group_uuid")?;
            append_uuid(&mut assertions, row.assertion_uuid, "assertion_uuid")?;
            actions.append_value(row.action.as_str());
            append_uuid(&mut reasoning, row.reasoning_uuid, "reasoning_uuid")?;
            append_uuid(&mut provenance, row.provenance_uuid, "provenance_uuid")?;
            times.append_value(row.recorded_at_micros);
            versions.append_value(row.contract_version);
        }
        record_batch(
            Arc::clone(&HYPOTHESIS_MEMBERSHIP_SCHEMA),
            vec![
                Arc::new(ids.finish()),
                Arc::new(operations.finish()),
                Arc::new(groups.finish()),
                Arc::new(assertions.finish()),
                Arc::new(actions.finish()),
                Arc::new(reasoning.finish()),
                Arc::new(provenance.finish()),
                Arc::new(times.finish()),
                Arc::new(versions.finish()),
            ],
        )
    }

    /// Build the authoritative selection-history batch.
    pub fn selection_batch(&self) -> Result<RecordBatch, KnowledgeError> {
        let len = self.selection_events.len();
        let mut ids = FixedSizeBinaryBuilder::with_capacity(len, 16);
        let mut operations = FixedSizeBinaryBuilder::with_capacity(len, 16);
        let mut groups = FixedSizeBinaryBuilder::with_capacity(len, 16);
        let mut selected = FixedSizeBinaryBuilder::with_capacity(len, 16);
        let mut reasoning = FixedSizeBinaryBuilder::with_capacity(len, 16);
        let mut provenance = FixedSizeBinaryBuilder::with_capacity(len, 16);
        let mut times = TimestampMicrosecondBuilder::with_capacity(len).with_timezone("UTC");
        let mut versions = UInt32Builder::with_capacity(len);
        for row in &self.selection_events {
            append_uuid(&mut ids, row.selection_event_uuid, "selection_event_uuid")?;
            append_uuid(&mut operations, row.operation_uuid, "operation_uuid")?;
            append_uuid(&mut groups, row.group_uuid, "group_uuid")?;
            if let Some(value) = row.selected_assertion_uuid {
                append_uuid(&mut selected, value, "selected_assertion_uuid")?;
            } else {
                selected.append_null();
            }
            append_uuid(&mut reasoning, row.reasoning_uuid, "reasoning_uuid")?;
            append_uuid(&mut provenance, row.provenance_uuid, "provenance_uuid")?;
            times.append_value(row.recorded_at_micros);
            versions.append_value(row.contract_version);
        }
        record_batch(
            Arc::clone(&HYPOTHESIS_SELECTION_SCHEMA),
            vec![
                Arc::new(ids.finish()),
                Arc::new(operations.finish()),
                Arc::new(groups.finish()),
                Arc::new(selected.finish()),
                Arc::new(reasoning.finish()),
                Arc::new(provenance.finish()),
                Arc::new(times.finish()),
                Arc::new(versions.finish()),
            ],
        )
    }

    /// Decode and validate the three authoritative record families.
    pub fn from_batches(
        group_batches: &[RecordBatch],
        membership_batches: &[RecordBatch],
        selection_batches: &[RecordBatch],
    ) -> Result<Self, KnowledgeError> {
        let mut groups = Vec::new();
        for batch in group_batches {
            require_schema(batch, &HYPOTHESIS_GROUP_SCHEMA, "hypothesis_group.schema")?;
            let ids = fixed(batch, "group_uuid")?;
            let keys = strings(batch, "question_key")?;
            let provenance = fixed(batch, "provenance_uuid")?;
            let times = timestamps(batch)?;
            let versions = versions(batch)?;
            for row in 0..batch.num_rows() {
                groups.push(HypothesisGroup {
                    group_uuid: uuid_at(ids, row, "group_uuid")?,
                    question_key: keys.value(row).to_owned(),
                    provenance_uuid: uuid_at(provenance, row, "provenance_uuid")?,
                    recorded_at_micros: times.value(row),
                    contract_version: versions.value(row),
                });
            }
        }
        let mut membership = Vec::new();
        for batch in membership_batches {
            require_schema(
                batch,
                &HYPOTHESIS_MEMBERSHIP_SCHEMA,
                "hypothesis_membership.schema",
            )?;
            let ids = fixed(batch, "membership_event_uuid")?;
            let operations = fixed(batch, "operation_uuid")?;
            let group_ids = fixed(batch, "group_uuid")?;
            let assertions = fixed(batch, "assertion_uuid")?;
            let actions = strings(batch, "action")?;
            let reasoning = fixed(batch, "reasoning_uuid")?;
            let provenance = fixed(batch, "provenance_uuid")?;
            let times = timestamps(batch)?;
            let versions = versions(batch)?;
            for row in 0..batch.num_rows() {
                membership.push(HypothesisMembershipEvent {
                    membership_event_uuid: uuid_at(ids, row, "membership_event_uuid")?,
                    operation_uuid: uuid_at(operations, row, "operation_uuid")?,
                    group_uuid: uuid_at(group_ids, row, "group_uuid")?,
                    assertion_uuid: uuid_at(assertions, row, "assertion_uuid")?,
                    action: HypothesisMembershipAction::parse(actions.value(row))?,
                    reasoning_uuid: uuid_at(reasoning, row, "reasoning_uuid")?,
                    provenance_uuid: uuid_at(provenance, row, "provenance_uuid")?,
                    recorded_at_micros: times.value(row),
                    contract_version: versions.value(row),
                });
            }
        }
        let mut selection = Vec::new();
        for batch in selection_batches {
            require_schema(
                batch,
                &HYPOTHESIS_SELECTION_SCHEMA,
                "hypothesis_selection.schema",
            )?;
            let ids = fixed(batch, "selection_event_uuid")?;
            let operations = fixed(batch, "operation_uuid")?;
            let group_ids = fixed(batch, "group_uuid")?;
            let selected = fixed(batch, "selected_assertion_uuid")?;
            let reasoning = fixed(batch, "reasoning_uuid")?;
            let provenance = fixed(batch, "provenance_uuid")?;
            let times = timestamps(batch)?;
            let versions = versions(batch)?;
            for row in 0..batch.num_rows() {
                selection.push(HypothesisSelectionEvent {
                    selection_event_uuid: uuid_at(ids, row, "selection_event_uuid")?,
                    operation_uuid: uuid_at(operations, row, "operation_uuid")?,
                    group_uuid: uuid_at(group_ids, row, "group_uuid")?,
                    selected_assertion_uuid: (!selected.is_null(row))
                        .then(|| uuid_at(selected, row, "selected_assertion_uuid"))
                        .transpose()?,
                    reasoning_uuid: uuid_at(reasoning, row, "reasoning_uuid")?,
                    provenance_uuid: uuid_at(provenance, row, "provenance_uuid")?,
                    recorded_at_micros: times.value(row),
                    contract_version: versions.value(row),
                });
            }
        }
        Self::new(groups, membership, selection)
    }

    /// Current membership for one group, ordered by assertion UUID.
    #[must_use]
    pub fn current_members(&self, group_uuid: Uuid) -> Vec<Uuid> {
        let mut state = HashSet::new();
        for event in self
            .membership_events
            .iter()
            .filter(|row| row.group_uuid == group_uuid)
        {
            match event.action {
                HypothesisMembershipAction::Added => {
                    state.insert(event.assertion_uuid);
                }
                HypothesisMembershipAction::Removed => {
                    state.remove(&event.assertion_uuid);
                }
            }
        }
        let mut members = state.into_iter().collect::<Vec<_>>();
        members.sort_unstable();
        members
    }

    /// Current explicit selection for one group.
    #[must_use]
    pub fn current_selection(&self, group_uuid: Uuid) -> Option<Uuid> {
        self.selection_events
            .iter()
            .rfind(|row| row.group_uuid == group_uuid)
            .and_then(|row| row.selected_assertion_uuid)
    }
}

fn validate_question_key(value: &str) -> Result<(), KnowledgeError> {
    if value.is_empty()
        || value.len() > MAX_HYPOTHESIS_QUESTION_KEY_BYTES
        || value.trim() != value
        || !value.nfc().eq(value.chars())
    {
        return Err(invalid(
            "hypothesis_group.question_key",
            "must be non-empty bounded NFC without surrounding whitespace",
        ));
    }
    Ok(())
}

fn validate_groups(groups: &[HypothesisGroup]) -> Result<(), KnowledgeError> {
    let mut ids = HashSet::new();
    let mut keys = HashSet::new();
    for row in groups {
        HypothesisGroup::new(
            row.group_uuid,
            row.question_key.clone(),
            row.provenance_uuid,
            row.recorded_at_micros,
        )?;
        if row.contract_version != HYPOTHESIS_GROUP_CONTRACT_VERSION {
            return Err(invalid(
                "hypothesis_group.contract_version",
                "unsupported version",
            ));
        }
        if !ids.insert(row.group_uuid) {
            return Err(KnowledgeError::Duplicate("group_uuid"));
        }
        if !keys.insert(row.question_key.as_str()) {
            return Err(KnowledgeError::Duplicate("question_key"));
        }
    }
    Ok(())
}

fn validate_event_ids(
    membership: &[HypothesisMembershipEvent],
    selection: &[HypothesisSelectionEvent],
) -> Result<(), KnowledgeError> {
    let mut ids = HashSet::new();
    for row in membership {
        HypothesisMembershipEvent::new(
            row.membership_event_uuid,
            row.operation_uuid,
            row.group_uuid,
            row.assertion_uuid,
            row.action,
            row.reasoning_uuid,
            row.provenance_uuid,
            row.recorded_at_micros,
        )?;
        if row.contract_version != HYPOTHESIS_MEMBERSHIP_CONTRACT_VERSION {
            return Err(invalid(
                "hypothesis_membership.contract_version",
                "unsupported version",
            ));
        }
        if !ids.insert(row.membership_event_uuid) {
            return Err(KnowledgeError::Duplicate("membership_event_uuid"));
        }
    }
    ids.clear();
    for row in selection {
        HypothesisSelectionEvent::new(
            row.selection_event_uuid,
            row.operation_uuid,
            row.group_uuid,
            row.selected_assertion_uuid,
            row.reasoning_uuid,
            row.provenance_uuid,
            row.recorded_at_micros,
        )?;
        if row.contract_version != HYPOTHESIS_SELECTION_CONTRACT_VERSION {
            return Err(invalid(
                "hypothesis_selection.contract_version",
                "unsupported version",
            ));
        }
        if !ids.insert(row.selection_event_uuid) {
            return Err(KnowledgeError::Duplicate("selection_event_uuid"));
        }
    }
    Ok(())
}

fn validate_state(
    groups: &[HypothesisGroup],
    membership: &[HypothesisMembershipEvent],
    selection: &[HypothesisSelectionEvent],
) -> Result<(), KnowledgeError> {
    let group_times = groups
        .iter()
        .map(|row| (row.group_uuid, row.recorded_at_micros))
        .collect::<HashMap<_, _>>();
    let mut members = HashMap::<Uuid, HashSet<Uuid>>::new();
    let mut selected = HashMap::<Uuid, Option<Uuid>>::new();
    for (time, _, operation_uuid) in operation_keys(membership, selection) {
        let selected_before = selected.clone();
        let membership_at_time = membership
            .iter()
            .filter(|row| row.recorded_at_micros == time && row.operation_uuid == operation_uuid)
            .collect::<Vec<_>>();
        let selection_at_time = selection
            .iter()
            .filter(|row| row.recorded_at_micros == time && row.operation_uuid == operation_uuid)
            .collect::<Vec<_>>();
        for event in &membership_at_time {
            let group_time = group_times
                .get(&event.group_uuid)
                .ok_or(KnowledgeError::Dangling("group_uuid"))?;
            if event.recorded_at_micros < *group_time {
                return Err(invalid(
                    "hypothesis_membership.recorded_at",
                    "cannot predate its hypothesis group",
                ));
            }
            let state = members.entry(event.group_uuid).or_default();
            match event.action {
                HypothesisMembershipAction::Added if !state.insert(event.assertion_uuid) => {
                    return Err(invalid(
                        "hypothesis_membership.action",
                        "cannot add a current member",
                    ));
                }
                HypothesisMembershipAction::Removed if !state.remove(&event.assertion_uuid) => {
                    return Err(invalid(
                        "hypothesis_membership.action",
                        "cannot remove a non-member",
                    ));
                }
                _ => {}
            }
        }
        for event in &selection_at_time {
            let group_time = group_times
                .get(&event.group_uuid)
                .ok_or(KnowledgeError::Dangling("group_uuid"))?;
            if event.recorded_at_micros < *group_time {
                return Err(invalid(
                    "hypothesis_selection.recorded_at",
                    "cannot predate its hypothesis group",
                ));
            }
            if let Some(assertion_uuid) = event.selected_assertion_uuid
                && !members
                    .get(&event.group_uuid)
                    .is_some_and(|state| state.contains(&assertion_uuid))
            {
                return Err(invalid(
                    "hypothesis_selection.selected_assertion_uuid",
                    "selected assertion must be a current member",
                ));
            }
            selected.insert(event.group_uuid, event.selected_assertion_uuid);
        }
        for removal in membership_at_time
            .iter()
            .filter(|row| row.action == HypothesisMembershipAction::Removed)
        {
            if selected_before
                .get(&removal.group_uuid)
                .copied()
                .flatten()
                .is_some_and(|id| id == removal.assertion_uuid)
            {
                let paired = selection_at_time.iter().any(|event| {
                    event.group_uuid == removal.group_uuid
                        && event.operation_uuid == removal.operation_uuid
                        && event.selected_assertion_uuid != Some(removal.assertion_uuid)
                });
                if !paired {
                    return Err(invalid(
                        "hypothesis_membership.action",
                        "selected-member removal requires a paired explicit selection event",
                    ));
                }
            }
        }
    }
    Ok(())
}

fn operation_keys(
    membership: &[HypothesisMembershipEvent],
    selection: &[HypothesisSelectionEvent],
) -> Vec<(i64, Uuid, Uuid)> {
    let mut operations = HashMap::<(i64, Uuid), Uuid>::new();
    for (time, operation, event) in membership
        .iter()
        .map(|row| {
            (
                row.recorded_at_micros,
                row.operation_uuid,
                row.membership_event_uuid,
            )
        })
        .chain(selection.iter().map(|row| {
            (
                row.recorded_at_micros,
                row.operation_uuid,
                row.selection_event_uuid,
            )
        }))
    {
        operations
            .entry((time, operation))
            .and_modify(|first| *first = (*first).min(event))
            .or_insert(event);
    }
    let mut keys = operations
        .into_iter()
        .map(|((time, operation), first_event)| (time, first_event, operation))
        .collect::<Vec<_>>();
    keys.sort_unstable();
    keys
}

fn merge_rows<T, F>(
    existing: &[T],
    staged: &[T],
    id: F,
    field: &'static str,
) -> Result<Vec<T>, KnowledgeError>
where
    T: Clone + Eq,
    F: Fn(&T) -> Uuid,
{
    let mut rows = existing.to_vec();
    let mut by_id = rows
        .iter()
        .cloned()
        .map(|row| (id(&row), row))
        .collect::<HashMap<_, _>>();
    for row in staged {
        if let Some(current) = by_id.get(&id(row)) {
            if current != row {
                return Err(KnowledgeError::Conflict(field));
            }
        } else {
            rows.push(row.clone());
            by_id.insert(id(row), row.clone());
        }
    }
    Ok(rows)
}

fn append_uuid(
    builder: &mut FixedSizeBinaryBuilder,
    value: Uuid,
    field: &'static str,
) -> Result<(), KnowledgeError> {
    builder
        .append_value(value.as_bytes())
        .map_err(|_| invalid(field, "invalid UUID width"))
}

fn record_batch(
    schema: SchemaRef,
    columns: Vec<Arc<dyn arrow::array::Array>>,
) -> Result<RecordBatch, KnowledgeError> {
    RecordBatch::try_new(schema, columns)
        .map_err(|_| invalid("hypothesis", "Arrow batch construction failed"))
}

fn require_schema(
    batch: &RecordBatch,
    schema: &SchemaRef,
    field: &'static str,
) -> Result<(), KnowledgeError> {
    if batch.schema().as_ref() != schema.as_ref() {
        return Err(invalid(field, "schema mismatch"));
    }
    Ok(())
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

fn strings<'a>(
    batch: &'a RecordBatch,
    name: &'static str,
) -> Result<&'a StringArray, KnowledgeError> {
    batch
        .column_by_name(name)
        .and_then(|value| value.as_any().downcast_ref())
        .ok_or_else(|| invalid(name, "column type mismatch"))
}

fn timestamps(batch: &RecordBatch) -> Result<&TimestampMicrosecondArray, KnowledgeError> {
    batch
        .column_by_name("recorded_at")
        .and_then(|value| value.as_any().downcast_ref())
        .ok_or_else(|| invalid("recorded_at", "column type mismatch"))
}

fn versions(batch: &RecordBatch) -> Result<&UInt32Array, KnowledgeError> {
    batch
        .column_by_name("contract_version")
        .and_then(|value| value.as_any().downcast_ref())
        .ok_or_else(|| invalid("contract_version", "column type mismatch"))
}

fn uuid_at(
    values: &FixedSizeBinaryArray,
    row: usize,
    field: &'static str,
) -> Result<Uuid, KnowledgeError> {
    if values.is_null(row) {
        return Err(invalid(field, "unexpected null UUID"));
    }
    Uuid::from_slice(values.value(row)).map_err(|_| invalid(field, "invalid UUID bytes"))
}

pub(crate) fn schema_registry_entries() -> [SchemaRegistryEntry; 3] {
    [
        entry(
            "hypothesis_groups",
            HYPOTHESIS_GROUP_CONTRACT_VERSION,
            Arc::clone(&HYPOTHESIS_GROUP_SCHEMA),
            *GROUP_SCHEMA_FINGERPRINT,
            &["recorded_at", "group_uuid"],
            (&["group_uuid"], "group_uuid"),
            &[("hypothesis_key_policy", HYPOTHESIS_KEY_POLICY_VERSION)],
        ),
        entry(
            "hypothesis_membership_events",
            HYPOTHESIS_MEMBERSHIP_CONTRACT_VERSION,
            Arc::clone(&HYPOTHESIS_MEMBERSHIP_SCHEMA),
            *MEMBERSHIP_SCHEMA_FINGERPRINT,
            &["recorded_at", "membership_event_uuid"],
            (&["membership_event_uuid"], "membership_event_uuid"),
            &[
                ("hypothesis_state_policy", HYPOTHESIS_STATE_POLICY_VERSION),
                ("membership_action", 1),
            ],
        ),
        entry(
            "hypothesis_selection_events",
            HYPOTHESIS_SELECTION_CONTRACT_VERSION,
            Arc::clone(&HYPOTHESIS_SELECTION_SCHEMA),
            *SELECTION_SCHEMA_FINGERPRINT,
            &["recorded_at", "selection_event_uuid"],
            (&["selection_event_uuid"], "selection_event_uuid"),
            &[("hypothesis_state_policy", HYPOTHESIS_STATE_POLICY_VERSION)],
        ),
    ]
}

fn entry(
    family: &'static str,
    version: u32,
    schema: SchemaRef,
    schema_fingerprint: [u8; 32],
    sort_key: &'static [&'static str],
    diff_identity: (&'static [&'static str], &'static str),
    enums: &'static [(&'static str, u32)],
) -> SchemaRegistryEntry {
    SchemaRegistryEntry {
        capability_id: "epistemic",
        capability_version: EPISTEMIC_CAPABILITY_VERSION,
        record_family: family,
        record_version: version,
        schema,
        schema_fingerprint,
        enum_registry_versions: enums,
        sort_key,
        diff_identity_fields: diff_identity.0,
        diff_record_uuid_field: Some(diff_identity.1),
        fingerprint_domain: CanonicalDomain::Schema,
        owner: "graphforge-knowledge",
        implementation_issue: 779,
        max_rows: MAX_KNOWLEDGE_ROWS,
    }
}

fn uuid_field(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::FixedSizeBinary(16), nullable)
}

fn timestamp_field(name: &str) -> Field {
    Field::new(
        name,
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        false,
    )
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

#[cfg(test)]
mod tests {
    use super::*;

    fn uuid7(seed: u8) -> Uuid {
        let mut bytes = [seed; 16];
        bytes[6] = (bytes[6] & 0x0f) | 0x70;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        Uuid::from_bytes(bytes)
    }

    fn group() -> HypothesisGroup {
        HypothesisGroup::new(uuid7(1), "cause.primary".into(), uuid7(2), 1).unwrap()
    }

    fn member(
        id: u8,
        operation: u8,
        assertion: u8,
        action: HypothesisMembershipAction,
        time: i64,
    ) -> HypothesisMembershipEvent {
        HypothesisMembershipEvent::new(
            uuid7(id),
            uuid7(operation),
            uuid7(1),
            uuid7(assertion),
            action,
            uuid7(id.wrapping_add(40)),
            uuid7(id.wrapping_add(80)),
            time,
        )
        .unwrap()
    }

    fn selection(
        id: u8,
        operation: u8,
        selected: Option<u8>,
        time: i64,
    ) -> HypothesisSelectionEvent {
        HypothesisSelectionEvent::new(
            uuid7(id),
            uuid7(operation),
            uuid7(1),
            selected.map(uuid7),
            uuid7(id.wrapping_add(40)),
            uuid7(id.wrapping_add(80)),
            time,
        )
        .unwrap()
    }

    #[test]
    fn keys_are_exact_nfc_bounded_and_unique() {
        assert!(HypothesisGroup::new(uuid7(1), String::new(), uuid7(2), 1).is_err());
        assert!(HypothesisGroup::new(uuid7(1), " key".into(), uuid7(2), 1).is_err());
        assert!(HypothesisGroup::new(uuid7(1), "e\u{301}".into(), uuid7(2), 1).is_err());
        let upper = HypothesisGroup::new(uuid7(3), "Cause".into(), uuid7(4), 1).unwrap();
        let lower = HypothesisGroup::new(uuid7(5), "cause".into(), uuid7(6), 1).unwrap();
        assert!(HypothesisLedger::new(vec![upper, lower], vec![], vec![]).is_ok());
        let duplicate =
            HypothesisGroup::new(uuid7(7), "cause.primary".into(), uuid7(8), 1).unwrap();
        assert!(HypothesisLedger::new(vec![group(), duplicate], vec![], vec![]).is_err());
    }

    #[test]
    fn add_select_change_clear_and_selected_removal_are_explicit() {
        let ledger = HypothesisLedger::new(
            vec![group()],
            vec![
                member(10, 20, 30, HypothesisMembershipAction::Added, 2),
                member(11, 21, 31, HypothesisMembershipAction::Added, 3),
                member(12, 22, 30, HypothesisMembershipAction::Removed, 5),
            ],
            vec![
                selection(13, 23, Some(30), 4),
                selection(14, 22, Some(31), 5),
                selection(15, 24, None, 6),
            ],
        )
        .unwrap();
        assert_eq!(ledger.current_members(uuid7(1)), vec![uuid7(31)]);
        assert_eq!(ledger.current_selection(uuid7(1)), None);
        assert_eq!(
            HypothesisLedger::from_batches(
                &[ledger.group_batch().unwrap()],
                &[ledger.membership_batch().unwrap()],
                &[ledger.selection_batch().unwrap()],
            )
            .unwrap(),
            ledger
        );
        assert_eq!(ledger.merge(&ledger).unwrap(), ledger);

        assert!(
            HypothesisLedger::new(
                vec![group()],
                vec![
                    member(10, 20, 30, HypothesisMembershipAction::Added, 2),
                    member(12, 22, 30, HypothesisMembershipAction::Removed, 4),
                ],
                vec![selection(13, 23, Some(30), 3)],
            )
            .is_err()
        );
    }

    #[test]
    fn canonical_fingerprints_are_exact_order_independent_and_round_trip_stable() {
        assert_eq!(
            CanonicalDomain::HypothesisGroup.as_str(),
            "graphforge/hypothesis-group"
        );
        assert_eq!(
            CanonicalDomain::HypothesisMembership.as_str(),
            "graphforge/hypothesis-membership"
        );
        assert_eq!(
            CanonicalDomain::HypothesisSelection.as_str(),
            "graphforge/hypothesis-selection"
        );
        let membership = member(10, 20, 30, HypothesisMembershipAction::Added, 2);
        let other = member(11, 21, 31, HypothesisMembershipAction::Added, 3);
        let selected = selection(12, 22, Some(30), 4);
        let cleared = selection(13, 23, None, 5);
        let ledger = HypothesisLedger::new(
            vec![group()],
            vec![membership.clone(), other.clone()],
            vec![selected.clone(), cleared.clone()],
        )
        .unwrap();
        let reordered = HypothesisLedger::new(
            vec![group()],
            vec![other, membership.clone()],
            vec![cleared.clone(), selected.clone()],
        )
        .unwrap();
        let decoded = HypothesisLedger::from_batches(
            &[ledger.group_batch().unwrap()],
            &[ledger.membership_batch().unwrap()],
            &[ledger.selection_batch().unwrap()],
        )
        .unwrap();
        let fingerprints = [
            ledger.group_fingerprint(uuid7(1)).unwrap(),
            ledger
                .membership_fingerprint(membership.membership_event_uuid)
                .unwrap(),
            ledger
                .selection_fingerprint(selected.selection_event_uuid)
                .unwrap(),
            ledger
                .selection_fingerprint(cleared.selection_event_uuid)
                .unwrap(),
        ];
        let hex = |value: [u8; 32]| {
            value
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        };
        assert_eq!(
            hex(fingerprints[0]),
            "336fe80f4e94c5758b5e77de06c9c5b6ebf51ff20355c5d6950ab3e648991265"
        );
        assert_eq!(
            hex(fingerprints[1]),
            "fc4fe4882c1469a3f4606c5352028a63d43d369301e26ac0216373889945e403"
        );
        assert_eq!(
            hex(fingerprints[2]),
            "ba0ec00b86950fd8b60afaa40327f240377f8fd1695a54dad1d51cbf45665615"
        );
        assert_eq!(
            hex(fingerprints[3]),
            "fc220def4f2a61fb134f5a414a257d50cb29a1aea364464831f89b7747e44f9b"
        );
        assert_eq!(
            reordered.group_fingerprint(uuid7(1)).unwrap(),
            fingerprints[0]
        );
        assert_eq!(
            reordered
                .membership_fingerprint(membership.membership_event_uuid)
                .unwrap(),
            fingerprints[1]
        );
        assert_eq!(
            reordered
                .selection_fingerprint(selected.selection_event_uuid)
                .unwrap(),
            fingerprints[2]
        );
        assert_eq!(
            reordered
                .selection_fingerprint(cleared.selection_event_uuid)
                .unwrap(),
            fingerprints[3]
        );
        assert_eq!(
            decoded.group_fingerprint(uuid7(1)).unwrap(),
            fingerprints[0]
        );
        assert_eq!(
            decoded
                .membership_fingerprint(membership.membership_event_uuid)
                .unwrap(),
            fingerprints[1]
        );
        assert_eq!(
            decoded
                .selection_fingerprint(selected.selection_event_uuid)
                .unwrap(),
            fingerprints[2]
        );
        assert_eq!(
            decoded
                .selection_fingerprint(cleared.selection_event_uuid)
                .unwrap(),
            fingerprints[3]
        );
    }

    #[test]
    fn selecting_nonmember_and_invalid_membership_transitions_fail() {
        assert!(
            HypothesisLedger::new(vec![group()], vec![], vec![selection(10, 20, Some(30), 2)],)
                .is_err()
        );
        assert!(
            HypothesisLedger::new(
                vec![group()],
                vec![member(10, 20, 30, HypothesisMembershipAction::Removed, 2)],
                vec![],
            )
            .is_err()
        );
        assert!(
            HypothesisLedger::new(
                vec![group()],
                vec![
                    member(10, 20, 30, HypothesisMembershipAction::Added, 2),
                    member(11, 21, 30, HypothesisMembershipAction::Added, 3),
                ],
                vec![],
            )
            .is_err()
        );
    }
}
