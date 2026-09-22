//! Arrow knowledge views explicitly join immutable claims with current scoped authority.
use super::{InspectResearchClaimsRequest, ResearchContext, authority, ledger};
use crate::{
    ExecutionResult, GfError, GraphForge,
    knowledge::{assertion_result, knowledge_error, ledger as k},
};
use arrow::{
    array::{ArrayRef, BooleanArray, FixedSizeBinaryBuilder, StringArray, UInt64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use graphforge_knowledge::research::{ResearchDecisionKind, ResearchSubjectKind};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use uuid::Uuid;

impl GraphForge {
    /// Inspect classified and legacy claims in an explicit scoped knowledge view.
    /// Suppression filters this view only; include it explicitly to inspect retained history.
    pub fn inspect_research_claims(
        &self,
        request: &InspectResearchClaimsRequest,
    ) -> Result<ExecutionResult, GfError> {
        self.graph_visibility.health.check()?;
        authority::require_owner(self)?;
        let owner = self.generation_for_read()?;
        let scope = authority::resolve(&owner, &request.context, request.community_uuid)?;
        let decisions = ledger::read_decisions(&owner)?;
        let mut canonical = HashMap::new();
        for row in decisions.events().iter().filter(|row| {
            row.authority == scope
                && row.subject_kind == ResearchSubjectKind::Assertion
                && row.kind != ResearchDecisionKind::Integrate
        }) {
            canonical.insert(row.subject_uuid, row.kind == ResearchDecisionKind::Promote);
        }
        let mut contexts = HashSet::from([scope.context_uuid]);
        if let ResearchContext::Branch { branch_uuid } = &request.context {
            let registry = graphforge_storage::research_versions::read_research_registry(&owner)?;
            let mut next = registry
                .branches
                .get(branch_uuid)
                .and_then(|b| b.parent_branch_uuid);
            while let Some(id) = next {
                if !contexts.insert(id) {
                    return Err(GfError::Validation("cyclic Branch ancestry".into()));
                }
                next = registry
                    .branches
                    .get(&id)
                    .and_then(|b| b.parent_branch_uuid);
            }
        }
        authority::with_context(self, &owner, &request.context, |graph, generation| {
            let state = if matches!(request.context, ResearchContext::Branch { .. }) {
                branch_state(&crate::branches::inspect_fields(graph)?)?
            } else {
                HashMap::new()
            };
            inspect(generation, request, &contexts, &canonical, &state)
        })
    }
}
fn inspect(
    generation: &graphforge_storage::ResolvedProjectGeneration,
    request: &InspectResearchClaimsRequest,
    contexts: &HashSet<Uuid>,
    canonical: &HashMap<Uuid, bool>,
    states: &HashMap<Uuid, &'static str>,
) -> Result<ExecutionResult, GfError> {
    let assertions = bounded_assertions(generation)?;
    let metadata = ledger::read_claims(generation)?;
    let by_id: HashMap<_, _> = metadata
        .claims()
        .iter()
        .map(|r| (r.assertion_uuid, r))
        .collect();
    let hidden: HashSet<_> = ledger::read_suppressions(generation)?
        .events()
        .iter()
        .filter(|r| contexts.contains(&r.context_uuid))
        .map(|r| r.assertion_uuid)
        .collect();
    let KnowledgeState {
        latest,
        history_count,
        evidence_count,
    } = knowledge_state(generation)?;
    let base = assertions.assertion_batch().map_err(knowledge_error)?;
    let rows = &assertions.assertions;
    let mut fields = base
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect::<Vec<_>>();
    let mut columns = base.columns().to_vec();
    let mut add = |name: &str, kind: DataType, nullable: bool, values: ArrayRef| {
        fields.push(Field::new(name, kind, nullable));
        columns.push(values);
    };
    classification_columns(rows, &by_id, &mut add)?;
    add(
        "status",
        DataType::Utf8,
        true,
        Arc::new(StringArray::from(
            rows.iter()
                .map(|r| latest.get(&r.assertion_uuid).copied())
                .collect::<Vec<_>>(),
        )),
    );
    add(
        "canonical",
        DataType::Boolean,
        false,
        Arc::new(BooleanArray::from(
            rows.iter()
                .map(|r| canonical.get(&r.assertion_uuid).copied().unwrap_or(false))
                .collect::<Vec<_>>(),
        )),
    );
    add(
        "suppressed",
        DataType::Boolean,
        false,
        Arc::new(BooleanArray::from(
            rows.iter()
                .map(|r| hidden.contains(&r.assertion_uuid))
                .collect::<Vec<_>>(),
        )),
    );
    add(
        "local_state",
        DataType::Utf8,
        false,
        Arc::new(StringArray::from_iter_values(rows.iter().map(|r| {
            states.get(&r.assertion_uuid).copied().unwrap_or("local")
        }))),
    );
    for (name, counts) in [
        ("status_event_count", &history_count),
        ("evidence_link_count", &evidence_count),
    ] {
        add(
            name,
            DataType::UInt64,
            false,
            Arc::new(UInt64Array::from_iter_values(
                rows.iter()
                    .map(|r| counts.get(&r.assertion_uuid).copied().unwrap_or(0)),
            )),
        );
    }
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|e| GfError::Execution(e.to_string()))?;
    let mask = BooleanArray::from(
        rows.iter()
            .map(|r| request.include_suppressed || !hidden.contains(&r.assertion_uuid))
            .collect::<Vec<_>>(),
    );
    Ok(assertion_result(
        arrow::compute::filter_record_batch(&batch, &mask)
            .map_err(|e| GfError::Execution(e.to_string()))?,
    ))
}
fn uuid_array(values: Vec<Option<Uuid>>) -> Result<ArrayRef, GfError> {
    let mut builder = FixedSizeBinaryBuilder::with_capacity(values.len(), 16);
    for value in values {
        if let Some(id) = value {
            builder
                .append_value(id.as_bytes())
                .map_err(|e| GfError::Execution(e.to_string()))?;
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()))
}
fn branch_state(result: &ExecutionResult) -> Result<HashMap<Uuid, &'static str>, GfError> {
    let mut objects = HashMap::new();
    let mut changed = HashSet::new();
    for batch in &result.batches {
        let column = |name| {
            batch
                .column_by_name(name)
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                .ok_or_else(|| GfError::Validation("invalid Branch field inspection".into()))
        };
        let kinds = column("object_kind")?;
        let ids = column("object_uuid")?;
        let fields = column("field")?;
        let status = column("status")?;
        for row in 0..batch.num_rows() {
            if kinds.value(row) != "assertion" {
                continue;
            }
            let id = Uuid::parse_str(ids.value(row))
                .map_err(|_| GfError::Validation("invalid Branch assertion identity".into()))?;
            if fields.value(row) == "$object" {
                objects.insert(
                    id,
                    if status.value(row) == "inherited" {
                        "inherited"
                    } else {
                        "local"
                    },
                );
            } else if status.value(row) != "inherited" {
                changed.insert(id);
            }
        }
    }
    for id in changed {
        if objects.get(&id) == Some(&"inherited") {
            objects.insert(id, "modified");
        }
    }
    Ok(objects)
}

fn classification_columns(
    rows: &[graphforge_knowledge::Assertion],
    by_id: &HashMap<Uuid, &graphforge_knowledge::research::ResearchClaimRecord>,
    add: &mut impl FnMut(&str, DataType, bool, ArrayRef),
) -> Result<(), GfError> {
    add(
        "category",
        DataType::Utf8,
        true,
        Arc::new(StringArray::from(
            rows.iter()
                .map(|r| by_id.get(&r.assertion_uuid).map(|c| c.category.as_str()))
                .collect::<Vec<_>>(),
        )),
    );
    for (name, values) in [
        (
            "conceptual_uuid",
            rows.iter()
                .map(|r| by_id.get(&r.assertion_uuid).map(|c| c.conceptual_uuid))
                .collect::<Vec<_>>(),
        ),
        (
            "creator_uuid",
            rows.iter()
                .map(|r| by_id.get(&r.assertion_uuid).map(|c| c.creator_uuid))
                .collect(),
        ),
        (
            "run_uuid",
            rows.iter()
                .map(|r| by_id.get(&r.assertion_uuid).and_then(|c| c.run_uuid))
                .collect(),
        ),
        (
            "origin_branch_uuid",
            rows.iter()
                .map(|r| {
                    by_id
                        .get(&r.assertion_uuid)
                        .and_then(|c| c.origin_branch_uuid)
                })
                .collect(),
        ),
        (
            "origin_version_uuid",
            rows.iter()
                .map(|r| {
                    by_id
                        .get(&r.assertion_uuid)
                        .and_then(|c| c.origin_version_uuid)
                })
                .collect(),
        ),
    ] {
        add(
            name,
            DataType::FixedSizeBinary(16),
            true,
            uuid_array(values)?,
        );
    }
    Ok(())
}

struct KnowledgeState {
    latest: HashMap<Uuid, &'static str>,
    history_count: HashMap<Uuid, u64>,
    evidence_count: HashMap<Uuid, u64>,
}
fn knowledge_state(
    generation: &graphforge_storage::ResolvedProjectGeneration,
) -> Result<KnowledgeState, GfError> {
    let status_ledger = if generation.capability("epistemic")?.is_some() {
        k::read_status_ledger(generation)?
    } else {
        graphforge_knowledge::AssertionStatusLedger::default()
    };
    let mut latest = HashMap::new();
    let mut history_count = HashMap::new();
    for row in &status_ledger.events {
        latest.insert(row.assertion_uuid, row.status.as_str());
        *history_count.entry(row.assertion_uuid).or_insert(0_u64) += 1;
    }
    let mut evidence_count = HashMap::new();
    for row in k::read_evidence_ledger(generation)?.links {
        *evidence_count.entry(row.assertion_uuid).or_insert(0_u64) += 1;
    }
    Ok(KnowledgeState {
        latest,
        history_count,
        evidence_count,
    })
}

fn bounded_assertions(
    generation: &graphforge_storage::ResolvedProjectGeneration,
) -> Result<graphforge_knowledge::AssertionLedger, GfError> {
    crate::branches::domain_bounds::preflight(generation)?;
    let assertions = k::read_ledger(generation)?;
    if assertions.assertions.len() > graphforge_knowledge::research::MAX_RESEARCH_ROWS {
        return Err(GfError::Project {
            code: graphforge_core::ProjectErrorCode::ResourceLimit,
            message: "research claim inspection exceeds row limit".into(),
        });
    }
    Ok(assertions)
}
