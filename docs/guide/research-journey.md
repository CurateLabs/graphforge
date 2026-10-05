# Keep research history

**Advanced · Assumes basic Python, database, and terminal skills.**
Start with the [Advanced introduction](advanced.md) for GraphForge terminology,
or [Basic](overview.md#basic) for a guided first graph.

This optional journey is for research that needs independent work, retained
history, or review. For a graph and a query, use the [quickstart](quickstart.md).
For a hypothesis you want to challenge and retrieve later, start with
[Record and revisit an inquiry](record-an-inquiry.md).

The research APIs add precise context and history to those tasks. They do not
require every user to follow an entire lifecycle. This guide explains the
choices; the [research workspace reference](../book/architecture/research-workspaces.md)
contains the exact requests and result contracts. A simpler collaboration command
layer is tracked in [#1772](https://github.com/CurateLabs/graphforge/issues/1772);
do not assume proposed command names are already available.

## Keep an exact state while you continue

Start with one task: retain a paper's original annotation, revise the working
graph, then retrieve both values in a later session. This uses a **Version**
without a Slice, Branch, or Proposal. Use a new empty directory on
[supported durable storage](installation.md#durable-storage).

```python
from pathlib import Path
from time import time_ns
from uuid import uuid4
from graphforge import GraphForge

Path("history-project").mkdir()
forge = GraphForge("history-project")
forge.add_node("Paper", title="Survey", note="Original annotation")
version_uuid = str(uuid4())
prepared = forge.prepare_research_version({
    "operation_uuid": str(uuid4()),
    "version_uuid": version_uuid,
    "context_uuid": str(uuid4()),
    "label": "Before annotation revision",
    "description": "Survey before changing the working annotation",
    "created_at": time_ns() // 1_000,  # UTC microseconds
    "required_versions": [],
})
forge.commit_research_version_operation(prepared)
forge.execute("MATCH (p:Paper {title: 'Survey'}) SET p.note = 'Revised annotation'")
forge.close()
```

The UUIDs identify this capture, its operation, and its research context; the
standard library generates them. Keep the exact `prepared` request unchanged
if retrying the commit. A new capture uses new identities. Repeat the whole
example only in a new empty directory.

In a later Python session, open the same path. Discover the retained Version
by its label, then query that exact state and the current graph:

```python
from graphforge import GraphForge

forge = GraphForge("history-project")
matches = [
    row for row in forge.list_research_versions().to_pylist()
    if row["label"] == "Before annotation revision"
]
if len(matches) != 1:
    raise ValueError("Choose exactly one retained Version before continuing.")
version_uuid = matches[0]["version_uuid"]
query = "MATCH (p:Paper {title: 'Survey'}) RETURN p.note AS note"
print("Retained:", forge.query_research_version(version_uuid, query).to_pylist())
print("Current:", forge.execute(query).to_pylist())
forge.close()
```

Expected output:

```text
Retained: [{'note': 'Original annotation'}]
Current: [{'note': 'Revised annotation'}]
```

You can stop here. The retained state remains distinct from live edits and can
anchor a conclusion to its original graph context. The same capture operation
can retain the graph records from [an inquiry](record-an-inquiry.md). It does
not make the reasoning correct or retain external source files merely named
in a property; register source material as native Artifacts when that is needed.
The remaining sections are **orientation**, not executable tutorials. They
explain when an additional tool is useful and link to its exact public request
contract. Those references assume you are ready to construct and inspect API
requests; the Version example above is a complete stopping point.

## Focus only when the full graph is too broad

Suppose Ada appears in two stories, Mystery and Voyage. You want to investigate
Mystery without including every fact about Voyage.

A **Slice** defines the selected graph and why each item belongs. Inspect the
included items, outside boundary, and evidence dependencies before freezing it.
Ada's inclusion does not silently include the second story. Source material
supporting a claim and graph membership remain separate questions.

You can stop here and analyze the selection. A Slice does not require a Branch.
The [implemented Slice facade](../book/architecture/research-workspaces.md#implemented-slice-facade-1351)
documents `preview_slice`, `freeze_slice`, and the inspection requests.

## Branch when work needs to evolve independently

A **Branch** is an independent research workspace with an origin and its own
current state. Create one when you want to change interpretations, properties,
or working knowledge without changing the source Project.

Ask which state is inherited and which changes are local. Working independently
is a state-management choice, not a privacy setting. Access to a private Project
is enforced by its host, not by creating a Branch.
Use the [implemented Branch facade](../book/architecture/research-workspaces.md#implemented-branch-facade-1352)
for creation, querying, independent edits, and restoration.

## Retain a Version when you need an exact reference

A **Version** retains a particular research state. Refer to it when recording
what evidence and graph context informed a conclusion. Live work can continue
without changing that reference. A Version is distinct from a saved query:
the query stores the analysis definition; the Version identifies retained state.

Retaining a Version does not make unavailable external bytes available. Inspect
whether evidence is retained locally, external-only, or released before relying
on it. Retrieval must report unavailable historical material rather than silently
substituting a newly fetched source.
The [native Version lifecycle](../book/architecture/research-workspaces.md#native-public-version-lifecycle)
describes capture, inspection, retention, and restoration requests.

## Compare before incorporating changes

Compare your work against the intended parent or upstream state. Review the
actual objects and fields that changed. Bring selected upstream changes into
your Branch deliberately; an unselected change remains unapplied.

The [comparison contract](../book/architecture/research-workspaces.md#evolution-and-comparison)
defines the compared states and reported changes. The
[upstream request contract](../book/architecture/research-workspaces.md#native-upstream-request-contract)
documents previewing and applying selected updates. Inspect the operation
receipt and resulting graph before relying on the change.

## Propose and accept only when review is needed

A **Proposal** selects contributions from a retained source Version. A reviewer
can accept some items and defer others. Accepting Ada's score need not accept
the proposed ending of the story. Later Branch edits do not change the frozen
contribution being reviewed.

Acceptance applies the selected contribution. It does not automatically make
an assertion canonical or establish that a conclusion is true. Inspect what
changed, the evidence supporting it, and the recorded review outcome.

When retrying a committed operation, reuse its original identity and request.
Changing a request is a new decision; do not disguise it as a retry.
The [selective Proposal contract](../book/architecture/research-workspaces.md#native-selective-proposal-contract-1356)
provides the preparation, submission, review, and acceptance requests.

## Continue or share

Continue working after review. Use the retained Version to revisit earlier
context; use the current Branch for ongoing work. A **Fork** creates a separate
Project with its own identity and history when independent ownership is needed.

Public projects support transparent participation with attributable authors and
committers, following Git practices. Private projects provide a non-public space
for known collaborators. Choose the appropriate project space when sharing;
privacy is not a prerequisite lesson for basic graph use. Hosting applications
own authentication and access enforcement.

Use [portable projects](portable-projects.md) for actual transfer and Hub clone.
History, fast-forward publication, rewrite, and prune behavior are specified in
[#1771](https://github.com/CurateLabs/graphforge/issues/1771); check the installed
CLI and current reference before using a developing collaboration surface.

## For integrators

The [two-story corpus](https://github.com/CurateLabs/graphforge/tree/main/tests/fixtures/analyst-journey-v1)
and [acceptance matrix](../engineering/TESTING.md#analyst-ux-acceptance) map these
steps to real Rust, Python, Node, and CLI execution, captured Arrow results,
receipts, and recovery tests. They establish Core behavior, not packaged editor
usability or observed human comprehension. Use
[#1209](https://github.com/CurateLabs/graphforge/issues/1209) for qualification of
those entry experiences.
