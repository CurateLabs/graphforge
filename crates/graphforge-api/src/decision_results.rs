//! Provider-neutral validation for externally produced analyst decisions.

use std::collections::{BTreeMap, BTreeSet};

use arrow::array::{ArrayRef, FixedSizeBinaryBuilder, Float64Builder, RecordBatch, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use graphforge_core::GfError;
use uuid::Uuid;

/// Maximum questions and result rows accepted in one decision batch.
pub const DECISION_BATCH_MAX_ROWS: usize = 256;
const MAX_SELECTED_ITEMS: usize = 100_000;
const MAX_TEXT_BYTES: usize = 4_096;
const MAX_LABEL_BYTES: usize = 256;
const PROBABILITY_SUM_TOLERANCE: f64 = 1.0e-9;
type ExpectedDecisionRows = BTreeSet<(Uuid, Option<Uuid>)>;
type DecisionQuestionMap<'a> = BTreeMap<Uuid, &'a DecisionQuestionV1>;
type DecisionResultMap<'a> = BTreeMap<(Uuid, Option<Uuid>), &'a DecisionResultV1>;

/// Exact, content-free identity for the selected input used by one producer.
///
/// A missing `version_uuid` denotes ephemeral state. Both digests describe the
/// caller's exact selected projection; neither field asserts a complete Version
/// payload. Item UUIDs are canonical graph UUIDs used for correlation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecisionInputIdentityV1 {
    /// Observed Project generation or immutable source generation.
    pub generation_uuid: Uuid,
    /// Immutable research Version, when this is retained historical state.
    pub version_uuid: Option<Uuid>,
    /// SHA-256 of the exact caller-selected projection bytes.
    pub projection_sha256: [u8; 32],
    /// SHA-256 of the selected object membership and selection rule.
    pub selection_sha256: [u8; 32],
    /// Canonical graph object UUIDs available to item-scoped questions.
    pub selected_item_uuids: BTreeSet<Uuid>,
}

/// Caller-supplied producer identity. Unknown facts remain absent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecisionProducerV1 {
    /// Producer/application name.
    pub name: String,
    /// Optional model or producer family identifier.
    pub model: Option<String>,
    /// Optional caller-known model or producer revision.
    pub revision: Option<String>,
}

/// One typed question or rubric applied to zero or more selected items.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecisionQuestionV1 {
    /// Stable caller-generated question identity.
    pub question_uuid: Uuid,
    /// Bounded question or proposition text.
    pub text: String,
    /// Selected object UUIDs expected to receive one result each. Empty means
    /// one project-level result with no item UUID.
    pub item_uuids: Vec<Uuid>,
    /// Finite allowed choices, ordered rubric labels, or a yes/no proposition.
    pub kind: DecisionQuestionKindV1,
}

/// Neutral question semantics; rubric order is caller supplied.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DecisionQuestionKindV1 {
    /// One exact value from a finite caller-supplied set.
    Choice {
        /// Unique allowed labels, in caller-defined display order.
        allowed_choices: Vec<String>,
    },
    /// One exact label from an ordered caller-supplied rubric.
    RubricScore {
        /// Unique rubric labels, from lowest to highest caller-defined level.
        ordered_levels: Vec<String>,
    },
    /// Probability that the question's proposition is true.
    YesNoProbability,
}

/// Caller-declared producer confidence. It remains separate from probability,
/// GraphForge assertion confidence, and canonical authority.
#[derive(Clone, Debug, PartialEq)]
pub struct DecisionConfidenceV1 {
    /// Producer-supplied confidence value.
    pub value: f64,
    /// Explicit inclusive lower bound for the declared scale.
    pub minimum: f64,
    /// Explicit inclusive upper bound for the declared scale.
    pub maximum: f64,
    /// Producer-declared confidence domain.
    pub domain: String,
    /// Producer-declared meaning of this confidence value.
    pub meaning: String,
}

/// Result status that keeps incomplete outcomes distinct from negative answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DecisionResultStatusV1 {
    /// A typed value was supplied.
    Answered,
    /// The producer is unsure; a tentative value may be present.
    Uncertain,
    /// The producer or transport could not provide a result.
    Unavailable,
    /// The producer completed a valid partial response without this item.
    Missing,
}

/// Typed decision value supplied for one question and optional item.
#[derive(Clone, Debug, PartialEq)]
pub enum DecisionValueV1 {
    /// Caller choice from the question's finite allowed set.
    Choice(String),
    /// Caller rubric level; its order is defined only by that question.
    RubricScore(String),
    /// Yes probability and optional explicit no probability.
    YesNoProbability {
        /// Probability assigned to yes.
        yes_probability: f64,
        /// Optional probability assigned to no.
        no_probability: Option<f64>,
    },
}

/// One producer result, correlated by IDs rather than array position.
#[derive(Clone, Debug, PartialEq)]
pub struct DecisionResultV1 {
    /// Question identity from the submitted batch.
    pub question_uuid: Uuid,
    /// Candidate identity, or `None` for a project-level question.
    pub item_uuid: Option<Uuid>,
    /// Explicit producer outcome.
    pub status: DecisionResultStatusV1,
    /// Typed value when the outcome supplies one.
    pub value: Option<DecisionValueV1>,
    /// Optional producer confidence with declared meaning and scale.
    pub confidence: Option<DecisionConfidenceV1>,
}

/// Complete ephemeral input to the provider-neutral decision validator.
#[derive(Clone, Debug, PartialEq)]
pub struct DecisionBatchV1 {
    /// Exact state and projection identity used by the producer.
    pub input: DecisionInputIdentityV1,
    /// Caller supplied producer identity.
    pub producer: DecisionProducerV1,
    /// Questions and expected question/item result identities.
    pub questions: Vec<DecisionQuestionV1>,
    /// Results, in any order, from the independent producer.
    pub results: Vec<DecisionResultV1>,
}

impl DecisionBatchV1 {
    /// Validate correlation and values, then return a deterministic Arrow table.
    ///
    /// Missing pairs in a valid partial response are materialized with the
    /// `missing` status. This operation is pure: it does not execute a producer,
    /// persist evidence, resolve caller policy, or mutate GraphForge state.
    pub fn validate(&self) -> Result<RecordBatch, GfError> {
        validate_batch(self)
    }
}

fn validate_batch(request: &DecisionBatchV1) -> Result<RecordBatch, GfError> {
    validate_input(request)?;
    let (questions, expected) = validate_questions(request)?;
    let results = validate_results(request, &questions, &expected)?;
    build_result_batch(request, &questions, &expected, &results)
}

fn validate_input(request: &DecisionBatchV1) -> Result<(), GfError> {
    validate_uuid(request.input.generation_uuid, "generation_uuid")?;
    if let Some(version_uuid) = request.input.version_uuid {
        validate_uuid(version_uuid, "version_uuid")?;
    }
    if request.input.selected_item_uuids.len() > MAX_SELECTED_ITEMS {
        return Err(invalid(
            "selected item count exceeds the decision input bound",
        ));
    }
    if request.input.selected_item_uuids.iter().any(Uuid::is_nil) {
        return Err(invalid("selected item identity must not be nil"));
    }
    validate_label(&request.producer.name, "producer name")?;
    validate_optional_label(request.producer.model.as_deref(), "producer model")?;
    validate_optional_label(request.producer.revision.as_deref(), "producer revision")?;
    if request.questions.is_empty() || request.questions.len() > DECISION_BATCH_MAX_ROWS {
        return Err(invalid(
            "question count is outside the decision batch bound",
        ));
    }

    Ok(())
}

fn validate_questions(
    request: &DecisionBatchV1,
) -> Result<(DecisionQuestionMap<'_>, ExpectedDecisionRows), GfError> {
    let mut questions = BTreeMap::new();
    let mut expected = BTreeSet::new();
    for question in &request.questions {
        validate_uuid(question.question_uuid, "question_uuid")?;
        validate_text(&question.text, "question text")?;
        if questions.insert(question.question_uuid, question).is_some() {
            return Err(invalid("duplicate question identity"));
        }
        validate_question_kind(&question.kind)?;
        if question.item_uuids.len() > DECISION_BATCH_MAX_ROWS {
            return Err(invalid(
                "question item count exceeds the decision batch bound",
            ));
        }
        if question
            .item_uuids
            .iter()
            .any(|item| !request.input.selected_item_uuids.contains(item))
        {
            return Err(invalid("question item is outside the selected input"));
        }
        let items = if question.item_uuids.is_empty() {
            vec![None]
        } else {
            if question.item_uuids.iter().collect::<BTreeSet<_>>().len()
                != question.item_uuids.len()
            {
                return Err(invalid("duplicate item identity in question"));
            }
            question.item_uuids.iter().copied().map(Some).collect()
        };
        for item_uuid in items {
            expected.insert((question.question_uuid, item_uuid));
            if expected.len() > DECISION_BATCH_MAX_ROWS {
                return Err(invalid("decision batch exceeds 256 expected result rows"));
            }
        }
    }
    Ok((questions, expected))
}

fn validate_results<'a>(
    request: &'a DecisionBatchV1,
    questions: &DecisionQuestionMap<'a>,
    expected: &ExpectedDecisionRows,
) -> Result<DecisionResultMap<'a>, GfError> {
    if request.results.len() > DECISION_BATCH_MAX_ROWS {
        return Err(invalid(
            "submitted result count exceeds the decision batch bound",
        ));
    }

    let mut results = BTreeMap::new();
    for result in &request.results {
        let key = (result.question_uuid, result.item_uuid);
        let question = questions
            .get(&result.question_uuid)
            .ok_or_else(|| invalid("result refers to an unknown question"))?;
        if !expected.contains(&key) {
            return Err(invalid(
                "result refers to an unknown question/item identity",
            ));
        }
        validate_result(question, result)?;
        if results.insert(key, result).is_some() {
            return Err(invalid("duplicate result identity"));
        }
    }
    Ok(results)
}

fn build_result_batch(
    request: &DecisionBatchV1,
    questions: &DecisionQuestionMap<'_>,
    expected: &ExpectedDecisionRows,
    results: &DecisionResultMap<'_>,
) -> Result<RecordBatch, GfError> {
    let schema = result_schema();
    let mut columns = DecisionResultColumns::new(expected.len());

    for &(question_uuid, item_uuid) in expected {
        let question = questions[&question_uuid];
        columns.append_row(
            request,
            question,
            question_uuid,
            item_uuid,
            results.get(&(question_uuid, item_uuid)).copied(),
        )?;
    }
    columns.finish(schema)
}

struct DecisionResultColumns {
    generation: FixedSizeBinaryBuilder,
    version: FixedSizeBinaryBuilder,
    projection: FixedSizeBinaryBuilder,
    selection: FixedSizeBinaryBuilder,
    producer_name: StringBuilder,
    producer_model: StringBuilder,
    producer_revision: StringBuilder,
    question_id: FixedSizeBinaryBuilder,
    item_id: FixedSizeBinaryBuilder,
    question_kind: StringBuilder,
    question_text: StringBuilder,
    question_options: StringBuilder,
    status: StringBuilder,
    choice: StringBuilder,
    rubric_score: StringBuilder,
    yes_probability: Float64Builder,
    no_probability: Float64Builder,
    confidence_value: Float64Builder,
    confidence_minimum: Float64Builder,
    confidence_maximum: Float64Builder,
    confidence_domain: StringBuilder,
    confidence_meaning: StringBuilder,
}

impl DecisionResultColumns {
    fn new(row_count: usize) -> Self {
        Self {
            generation: FixedSizeBinaryBuilder::with_capacity(row_count, 16),
            version: FixedSizeBinaryBuilder::with_capacity(row_count, 16),
            projection: FixedSizeBinaryBuilder::with_capacity(row_count, 32),
            selection: FixedSizeBinaryBuilder::with_capacity(row_count, 32),
            producer_name: StringBuilder::new(),
            producer_model: StringBuilder::new(),
            producer_revision: StringBuilder::new(),
            question_id: FixedSizeBinaryBuilder::with_capacity(row_count, 16),
            item_id: FixedSizeBinaryBuilder::with_capacity(row_count, 16),
            question_kind: StringBuilder::new(),
            question_text: StringBuilder::new(),
            question_options: StringBuilder::new(),
            status: StringBuilder::new(),
            choice: StringBuilder::new(),
            rubric_score: StringBuilder::new(),
            yes_probability: Float64Builder::new(),
            no_probability: Float64Builder::new(),
            confidence_value: Float64Builder::new(),
            confidence_minimum: Float64Builder::new(),
            confidence_maximum: Float64Builder::new(),
            confidence_domain: StringBuilder::new(),
            confidence_meaning: StringBuilder::new(),
        }
    }

    fn append_row(
        &mut self,
        request: &DecisionBatchV1,
        question: &DecisionQuestionV1,
        question_uuid: Uuid,
        item_uuid: Option<Uuid>,
        result: Option<&DecisionResultV1>,
    ) -> Result<(), GfError> {
        append_uuid(&mut self.generation, request.input.generation_uuid)?;
        append_optional_uuid(&mut self.version, request.input.version_uuid)?;
        self.projection
            .append_value(request.input.projection_sha256)
            .map_err(|error| arrow_error(&error))?;
        self.selection
            .append_value(request.input.selection_sha256)
            .map_err(|error| arrow_error(&error))?;
        self.producer_name.append_value(&request.producer.name);
        self.producer_model
            .append_option(request.producer.model.as_deref());
        self.producer_revision
            .append_option(request.producer.revision.as_deref());
        append_uuid(&mut self.question_id, question_uuid)?;
        append_optional_uuid(&mut self.item_id, item_uuid)?;
        self.question_kind
            .append_value(question_kind_name(&question.kind));
        self.question_text.append_value(&question.text);
        self.question_options
            .append_value(question_options_json(&question.kind)?);
        self.status
            .append_value(result.map_or("missing", |row| result_status_name(row.status)));
        self.append_value(result.and_then(|row| row.value.as_ref()));
        self.append_confidence(result.and_then(|row| row.confidence.as_ref()));
        Ok(())
    }

    fn append_value(&mut self, value: Option<&DecisionValueV1>) {
        match value {
            Some(DecisionValueV1::Choice(value)) => {
                self.choice.append_value(value);
                self.rubric_score.append_null();
                self.yes_probability.append_null();
                self.no_probability.append_null();
            }
            Some(DecisionValueV1::RubricScore(value)) => {
                self.choice.append_null();
                self.rubric_score.append_value(value);
                self.yes_probability.append_null();
                self.no_probability.append_null();
            }
            Some(DecisionValueV1::YesNoProbability {
                yes_probability,
                no_probability,
            }) => {
                self.choice.append_null();
                self.rubric_score.append_null();
                self.yes_probability.append_value(*yes_probability);
                self.no_probability.append_option(*no_probability);
            }
            None => {
                self.choice.append_null();
                self.rubric_score.append_null();
                self.yes_probability.append_null();
                self.no_probability.append_null();
            }
        }
    }

    fn append_confidence(&mut self, confidence: Option<&DecisionConfidenceV1>) {
        self.confidence_value
            .append_option(confidence.map(|row| row.value));
        self.confidence_minimum
            .append_option(confidence.map(|row| row.minimum));
        self.confidence_maximum
            .append_option(confidence.map(|row| row.maximum));
        self.confidence_domain
            .append_option(confidence.map(|row| row.domain.as_str()));
        self.confidence_meaning
            .append_option(confidence.map(|row| row.meaning.as_str()));
    }

    fn finish(mut self, schema: std::sync::Arc<Schema>) -> Result<RecordBatch, GfError> {
        let arrays: Vec<ArrayRef> = vec![
            std::sync::Arc::new(self.generation.finish()),
            std::sync::Arc::new(self.version.finish()),
            std::sync::Arc::new(self.projection.finish()),
            std::sync::Arc::new(self.selection.finish()),
            std::sync::Arc::new(self.producer_name.finish()),
            std::sync::Arc::new(self.producer_model.finish()),
            std::sync::Arc::new(self.producer_revision.finish()),
            std::sync::Arc::new(self.question_id.finish()),
            std::sync::Arc::new(self.item_id.finish()),
            std::sync::Arc::new(self.question_kind.finish()),
            std::sync::Arc::new(self.question_text.finish()),
            std::sync::Arc::new(self.question_options.finish()),
            std::sync::Arc::new(self.status.finish()),
            std::sync::Arc::new(self.choice.finish()),
            std::sync::Arc::new(self.rubric_score.finish()),
            std::sync::Arc::new(self.yes_probability.finish()),
            std::sync::Arc::new(self.no_probability.finish()),
            std::sync::Arc::new(self.confidence_value.finish()),
            std::sync::Arc::new(self.confidence_minimum.finish()),
            std::sync::Arc::new(self.confidence_maximum.finish()),
            std::sync::Arc::new(self.confidence_domain.finish()),
            std::sync::Arc::new(self.confidence_meaning.finish()),
        ];
        RecordBatch::try_new(schema, arrays).map_err(|error| arrow_error(&error))
    }
}

fn validate_question_kind(kind: &DecisionQuestionKindV1) -> Result<(), GfError> {
    match kind {
        DecisionQuestionKindV1::Choice { allowed_choices } => {
            validate_vocabulary(allowed_choices, "choice vocabulary")
        }
        DecisionQuestionKindV1::RubricScore { ordered_levels } => {
            validate_vocabulary(ordered_levels, "rubric levels")
        }
        DecisionQuestionKindV1::YesNoProbability => Ok(()),
    }
}

fn validate_vocabulary(values: &[String], field: &str) -> Result<(), GfError> {
    if values.is_empty() || values.len() > 64 {
        return Err(invalid(format!("{field} count is outside its bound")));
    }
    let mut unique = BTreeSet::new();
    for value in values {
        validate_label(value, field)?;
        if !unique.insert(value) {
            return Err(invalid(format!("{field} contains a duplicate")));
        }
    }
    Ok(())
}

fn validate_result(
    question: &DecisionQuestionV1,
    result: &DecisionResultV1,
) -> Result<(), GfError> {
    match (result.status, result.value.as_ref()) {
        (DecisionResultStatusV1::Answered, None)
        | (DecisionResultStatusV1::Unavailable | DecisionResultStatusV1::Missing, Some(_)) => {
            return Err(invalid("result status and value are incompatible"));
        }
        _ => {}
    }
    if matches!(
        result.status,
        DecisionResultStatusV1::Unavailable | DecisionResultStatusV1::Missing
    ) && result.confidence.is_some()
    {
        return Err(invalid(
            "missing or unavailable result cannot carry confidence",
        ));
    }
    if let Some(confidence) = &result.confidence {
        if !confidence.value.is_finite()
            || !confidence.minimum.is_finite()
            || !confidence.maximum.is_finite()
            || confidence.minimum >= confidence.maximum
            || confidence.value < confidence.minimum
            || confidence.value > confidence.maximum
        {
            return Err(invalid("confidence is outside its declared finite scale"));
        }
        validate_label(&confidence.domain, "confidence domain")?;
        validate_text(&confidence.meaning, "confidence meaning")?;
    }
    match (&question.kind, result.value.as_ref()) {
        (_, None) => Ok(()),
        (
            DecisionQuestionKindV1::Choice { allowed_choices },
            Some(DecisionValueV1::Choice(value)),
        ) => {
            if allowed_choices.contains(value) {
                Ok(())
            } else {
                Err(invalid("choice is outside the allowed vocabulary"))
            }
        }
        (
            DecisionQuestionKindV1::RubricScore { ordered_levels },
            Some(DecisionValueV1::RubricScore(value)),
        ) => {
            if ordered_levels.contains(value) {
                Ok(())
            } else {
                Err(invalid("rubric score is outside the declared rubric"))
            }
        }
        (
            DecisionQuestionKindV1::YesNoProbability,
            Some(DecisionValueV1::YesNoProbability {
                yes_probability,
                no_probability,
            }),
        ) => {
            validate_probability(*yes_probability)?;
            if let Some(no) = no_probability {
                validate_probability(*no)?;
                if (yes_probability + no - 1.0).abs() > PROBABILITY_SUM_TOLERANCE {
                    return Err(invalid("yes/no probabilities do not sum to one"));
                }
            }
            Ok(())
        }
        _ => Err(invalid("result value kind does not match its question")),
    }
}

fn validate_probability(value: f64) -> Result<(), GfError> {
    if value.is_finite() && (0.0..=1.0).contains(&value) {
        Ok(())
    } else {
        Err(invalid("probability must be finite and within [0, 1]"))
    }
}

fn validate_uuid(value: Uuid, field: &str) -> Result<(), GfError> {
    if value.is_nil() {
        Err(invalid(format!("{field} must not be nil")))
    } else {
        Ok(())
    }
}

fn validate_label(value: &str, field: &str) -> Result<(), GfError> {
    if value.trim().is_empty()
        || value.len() > MAX_LABEL_BYTES
        || value.chars().any(char::is_control)
    {
        Err(invalid(format!("{field} is empty or outside its bound")))
    } else {
        Ok(())
    }
}

fn validate_optional_label(value: Option<&str>, field: &str) -> Result<(), GfError> {
    if let Some(value) = value {
        validate_label(value, field)
    } else {
        Ok(())
    }
}

fn validate_text(value: &str, field: &str) -> Result<(), GfError> {
    if value.trim().is_empty()
        || value.len() > MAX_TEXT_BYTES
        || value.chars().any(char::is_control)
    {
        Err(invalid(format!("{field} is empty or outside its bound")))
    } else {
        Ok(())
    }
}

fn question_kind_name(kind: &DecisionQuestionKindV1) -> &'static str {
    match kind {
        DecisionQuestionKindV1::Choice { .. } => "choice",
        DecisionQuestionKindV1::RubricScore { .. } => "rubric_score",
        DecisionQuestionKindV1::YesNoProbability => "yes_no_probability",
    }
}

fn question_options_json(kind: &DecisionQuestionKindV1) -> Result<String, GfError> {
    let values = match kind {
        DecisionQuestionKindV1::Choice { allowed_choices } => Some(allowed_choices),
        DecisionQuestionKindV1::RubricScore { ordered_levels } => Some(ordered_levels),
        DecisionQuestionKindV1::YesNoProbability => None,
    };
    values.map_or_else(
        || Ok("[]".to_owned()),
        |values| serde_json::to_string(values).map_err(|error| invalid(error.to_string())),
    )
}

fn result_status_name(status: DecisionResultStatusV1) -> &'static str {
    match status {
        DecisionResultStatusV1::Answered => "answered",
        DecisionResultStatusV1::Uncertain => "uncertain",
        DecisionResultStatusV1::Unavailable => "unavailable",
        DecisionResultStatusV1::Missing => "missing",
    }
}

fn append_uuid(builder: &mut FixedSizeBinaryBuilder, value: Uuid) -> Result<(), GfError> {
    builder
        .append_value(value.as_bytes())
        .map_err(|error| arrow_error(&error))
}

fn append_optional_uuid(
    builder: &mut FixedSizeBinaryBuilder,
    value: Option<Uuid>,
) -> Result<(), GfError> {
    if let Some(value) = value {
        builder
            .append_value(value.as_bytes())
            .map_err(|error| arrow_error(&error))
    } else {
        builder.append_null();
        Ok(())
    }
}

fn result_schema() -> std::sync::Arc<Schema> {
    let mut metadata = std::collections::HashMap::new();
    metadata.insert(
        "graphforge.contract".to_owned(),
        "decision_result/1".to_owned(),
    );
    std::sync::Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("generation_uuid", DataType::FixedSizeBinary(16), false),
            Field::new("version_uuid", DataType::FixedSizeBinary(16), true),
            Field::new("projection_sha256", DataType::FixedSizeBinary(32), false),
            Field::new("selection_sha256", DataType::FixedSizeBinary(32), false),
            Field::new("producer_name", DataType::Utf8, false),
            Field::new("producer_model", DataType::Utf8, true),
            Field::new("producer_revision", DataType::Utf8, true),
            Field::new("question_uuid", DataType::FixedSizeBinary(16), false),
            Field::new("item_uuid", DataType::FixedSizeBinary(16), true),
            Field::new("question_kind", DataType::Utf8, false),
            Field::new("question_text", DataType::Utf8, false),
            Field::new("question_options_json", DataType::Utf8, false),
            Field::new("status", DataType::Utf8, false),
            Field::new("choice_value", DataType::Utf8, true),
            Field::new("rubric_score", DataType::Utf8, true),
            Field::new("yes_probability", DataType::Float64, true),
            Field::new("no_probability", DataType::Float64, true),
            Field::new("confidence_value", DataType::Float64, true),
            Field::new("confidence_minimum", DataType::Float64, true),
            Field::new("confidence_maximum", DataType::Float64, true),
            Field::new("confidence_domain", DataType::Utf8, true),
            Field::new("confidence_meaning", DataType::Utf8, true),
        ],
        metadata,
    ))
}

fn invalid(message: impl Into<String>) -> GfError {
    GfError::Validation(message.into())
}

fn arrow_error(error: &arrow::error::ArrowError) -> GfError {
    GfError::Validation(error.to_string())
}
