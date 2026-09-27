"""Typed inputs for caller supplied, provider neutral decisions.

The GraphForge native API validates these contracts and returns an Arrow table.
This module contains types only; it does not interpret a decision or choose an
action policy.
"""

from typing import Literal, TypedDict


class DecisionInputIdentityV1(TypedDict):
    """Content free identity and digests for the selected decision input."""

    generation_uuid: str
    version_uuid: str | None
    projection_sha256: list[int]
    selection_sha256: list[int]
    selected_item_uuids: list[str]


class _DecisionProducerRequired(TypedDict):
    """Required producer identity fields shared by the public TypedDict."""

    name: str


class DecisionProducerV1(_DecisionProducerRequired, total=False):
    """Caller supplied producer metadata; unknown model facts may be absent."""

    model: str | None
    revision: str | None


class ChoiceQuestionKindV1(TypedDict):
    """Question with one answer from a finite choice set."""

    kind: Literal["choice"]
    allowed_choices: list[str]


class RubricQuestionKindV1(TypedDict):
    """Question with one answer from caller ordered rubric levels."""

    kind: Literal["rubric_score"]
    ordered_levels: list[str]


class ProbabilityQuestionKindV1(TypedDict):
    """Question asking for a yes/no probability."""

    kind: Literal["yes_no_probability"]


class DecisionQuestionV1(TypedDict):
    """Stable question identity, text, item set, and typed meaning."""

    question_uuid: str
    text: str
    item_uuids: list[str]
    kind: ChoiceQuestionKindV1 | RubricQuestionKindV1 | ProbabilityQuestionKindV1


class ChoiceValueV1(TypedDict):
    """Choice result encoded with the adjacent kind/value representation."""

    kind: Literal["choice"]
    value: str


class RubricScoreValueV1(TypedDict):
    """Rubric result encoded with the adjacent kind/value representation."""

    kind: Literal["rubric_score"]
    value: str


class _YesNoProbabilityRequired(TypedDict):
    """Required yes probability shared by the public payload TypedDict."""

    yes_probability: float


class YesNoProbabilityPayloadV1(_YesNoProbabilityRequired, total=False):
    """Yes probability and optional explicit no probability."""

    no_probability: float | None


class YesNoProbabilityValueV1(TypedDict):
    """Yes/no result using an adjacent kind and nested value payload."""

    kind: Literal["yes_no_probability"]
    value: YesNoProbabilityPayloadV1


class DecisionConfidenceV1(TypedDict):
    """Producer confidence with an explicit numeric scale and meaning."""

    value: float
    minimum: float
    maximum: float
    domain: str
    meaning: str


class _DecisionResultRequired(TypedDict):
    """Required result identity and outcome status shared by its TypedDict."""

    question_uuid: str
    status: Literal["answered", "uncertain", "unavailable", "missing"]


class DecisionResultV1(_DecisionResultRequired, total=False):
    """Caller supplied answer, uncertainty, unavailability, or partial absence."""

    item_uuid: str | None
    value: ChoiceValueV1 | RubricScoreValueV1 | YesNoProbabilityValueV1 | None
    confidence: DecisionConfidenceV1 | None


class DecisionBatchV1(TypedDict):
    """Complete bounded decision input accepted by the native validator."""

    input: DecisionInputIdentityV1
    producer: DecisionProducerV1
    questions: list[DecisionQuestionV1]
    results: list[DecisionResultV1]
