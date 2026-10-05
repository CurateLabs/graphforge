"""A synthetic survey/interview teaching example using the public Python API.

Run with the GraphForge version selected by docs/guide/installation.md:
    python first_project.py create study-project
    python first_project.py review study-project

No real participants, statistical inference, model service, or external downloads.
"""

import argparse
from pathlib import Path

from graphforge import GraphForge

# Attendance is self-reported for one week: 1 = attended, 0 = did not attend.
# "long" means a one-way commute of at least 40 minutes in this teaching example.
SURVEY = [
    ("P01", "long", 1),
    ("P02", "long", 0),
    ("P03", "long", 0),
    ("P04", "short", 1),
    ("P05", "short", 1),
    ("P06", "short", 1),
]
# These are authored fictional excerpts and human-assigned descriptive codes.
INTERVIEWS = [
    (
        "P01",
        "I can join when the session follows my lecture.",
        "schedule_fit",
        "The meeting fits an existing campus visit.",
    ),
    (
        "P02",
        "The last bus leaves before the group ends.",
        "travel_timing",
        "Transport timing conflicts with attendance.",
    ),
    (
        "P03",
        "My paid shift starts when the group meets.",
        "work_schedule",
        "Paid work overlaps the meeting.",
    ),
    (
        "P04",
        "I stayed because a friend invited me.",
        "peer_invitation",
        "An invitation influenced participation.",
    ),
]
SUMMARY = (
    "In these six fictional responses, 1 of 3 long-commute participants and "
    "3 of 3 short-commute participants reported attending. Interview excerpts "
    "suggest timing, work, and invitation as explanations to investigate. "
    "P01 shows that a long commute does not always prevent attendance."
)
LIMITATION = (
    "Six invented survey responses and four invented excerpts teach a method; "
    "they establish no population pattern or causal effect."
)


def create(path: Path) -> None:
    """Create once, retaining source rows and the bounded interpretation."""
    path.mkdir()  # Refuse an existing path so rerunning cannot duplicate records.
    forge = GraphForge(str(path))
    try:
        participants = {}
        for participant_id, commute, attended in SURVEY:
            participants[participant_id] = forge.add_node(
                "Participant",
                participant_id=participant_id,
                commute=commute,
                attended=attended,
                source="fictional survey, week 1",
            )
        excerpts = []
        for participant_id, text, code, reason in INTERVIEWS:
            excerpt = forge.add_node(
                "Excerpt",
                text=text,
                source=f"fictional interview {participant_id}, excerpt 1",
            )
            category = forge.add_node(
                "Code",
                name=code,
                assigned_by="teaching-example author",
                reason=reason,
            )
            forge.add_edge(participants[participant_id], "SAID", excerpt)
            forge.add_edge(excerpt, "CODED_AS", category)
            excerpts.append(excerpt)
        finding = forge.add_node(
            "Finding",
            subject="study-group-attendance",
            summary=SUMMARY,
            limitation=LIMITATION,
            scope="week 1; six survey respondents; four interviewed",
            next_question="Would a different meeting time help, and for whom?",
        )
        for record in [*participants.values(), *excerpts]:
            forge.add_edge(finding, "CONSIDERS", record)
    finally:
        forge.close()


def review(path: Path) -> None:
    """Read the saved graph, numerical summary, linked excerpts, and conclusion."""
    if not (path / "FORMAT").is_file():
        raise ValueError("Choose the existing study-project created by this example.")
    forge = GraphForge(str(path))
    try:
        print("Survey summary (people, not quotations):")
        for row in forge.execute("""
            MATCH (p:Participant)
            RETURN p.commute AS commute, count(*) AS respondents,
                   sum(p.attended) AS attended
            ORDER BY commute
        """).to_pylist():
            print(
                f"{row['commute']}: {row['attended']} attended / {row['respondents']} respondents"
            )
        print("\nInterview evidence and assigned codes:")
        for row in forge.execute("""
            MATCH (p:Participant)-[:SAID]->(e:Excerpt)-[:CODED_AS]->(c:Code)
            RETURN p.participant_id AS participant, e.text AS excerpt, c.name AS code,
                   e.source AS source, c.assigned_by AS assigned_by, c.reason AS reason
            ORDER BY participant, code
        """).to_pylist():
            print(f"{row['participant']} | {row['code']} | {row['excerpt']}")
            print(f"  Source: {row['source']}; coded by: {row['assigned_by']}")
            print(f"  Reason: {row['reason']}")
        print("\nSaved finding:")
        for row in forge.execute(
            """
            MATCH (f:Finding {subject: $subject})
            RETURN f.summary AS summary, f.scope AS scope,
                   f.limitation AS limitation, f.next_question AS next_question
        """,
            {"subject": "study-group-attendance"},
        ).to_pylist():
            for name in ("summary", "scope", "limitation", "next_question"):
                print(f"{name}: {row[name]}")
    finally:
        forge.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("create", "review"))
    parser.add_argument("project", type=Path)
    args = parser.parse_args()
    if args.action == "create":
        create(args.project)
    review(args.project)


if __name__ == "__main__":
    main()
