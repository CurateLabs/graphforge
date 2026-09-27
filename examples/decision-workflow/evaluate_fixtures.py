"""Compare a labeled offline producer fixture with an explicit rule baseline."""

from __future__ import annotations

import json

REVIEW_THRESHOLD = 0.6
CASES = [
    {
        "id": "clear-evidence",
        "label": "research",
        "rule": "research",
        "status": "answered",
        "result": "research",
        "yes_probability": 0.25,
    },
    {
        "id": "conflicted-evidence",
        "label": "human_review",
        "rule": "human_review",
        "status": "uncertain",
        "result": "human_review",
        "yes_probability": 0.75,
    },
    {
        "id": "misleading-evidence",
        "label": "research",
        "rule": "research",
        "status": "answered",
        "result": "human_review",
        "yes_probability": 0.65,
    },
    {
        "id": "missing-context",
        "label": "human_review",
        "rule": "human_review",
        "status": "missing",
        "result": None,
        "yes_probability": None,
    },
]


def main() -> None:
    answer_rows = [case for case in CASES if case["status"] == "answered"]
    probability_rows = [case for case in CASES if case["yes_probability"] is not None]
    report = {
        "fixture_only": True,
        "rule_baseline_errors": sum(case["rule"] != case["label"] for case in CASES),
        "producer_answer_errors": sum(case["result"] != case["label"] for case in answer_rows),
        "answered": len(answer_rows),
        "missing": sum(case["status"] == "missing" for case in CASES),
        "unavailable": sum(case["status"] == "unavailable" for case in CASES),
        "uncertain": sum(case["status"] == "uncertain" for case in CASES),
        "policy_reviews": sum(
            case["status"] != "answered"
            or case["result"] == "human_review"
            or (case["yes_probability"] is not None and case["yes_probability"] >= REVIEW_THRESHOLD)
            for case in CASES
        ),
        "probability_brier_on_fixture": round(
            sum(
                (case["yes_probability"] - (case["label"] == "human_review")) ** 2
                for case in probability_rows
            )
            / len(probability_rows),
            4,
        ),
    }
    assert report == {
        "fixture_only": True,
        "rule_baseline_errors": 0,
        "producer_answer_errors": 1,
        "answered": 2,
        "missing": 1,
        "unavailable": 0,
        "uncertain": 1,
        "policy_reviews": 3,
        "probability_brier_on_fixture": 0.1825,
    }
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
