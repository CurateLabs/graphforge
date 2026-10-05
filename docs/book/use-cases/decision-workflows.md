# Provider-neutral decision workflows

**Advanced:** this guide assumes basic Python dictionaries, API calls, and
running a script from a terminal. For a first research assignment, start with
[Your first research project](../../guide/first-research-project.md). A decision
producer is not required for basic graph use or human interpretation.

Use this workflow when another part of your application proposes a choice,
an ordered rubric rating, or a yes/no probability. GraphForge checks whether
the response matches the question and selected data. Your application decides
what to do next. A structurally valid result is not necessarily a good judgment,
and it never authorizes a write by itself.

## Run the offline analyst example

Install the matching Python package through [Installation](../../guide/installation.md).
The final v0.6.0 command applies only after publication; choose an available
candidate explicitly while evaluating the release.

Download [analyst.py](../../../examples/decision-workflow/analyst.py), save it
in your working folder, and run it with the same environment's interpreter:

```bash
python analyst.py
```

From a source checkout, the equivalent command is
`python examples/decision-workflow/analyst.py`. You do not need to build Rust
when using an installed matching native package. The example runs in memory,
uses a local teaching fixture, and makes no model call.

It creates two synthetic research candidates, Mystery and Voyage. The local
producer supplies a route, an ordered rubric label, and a review probability.
The example prints the validated Arrow rows and the caller's selected action.
Expect a `research` route and `high` rubric for Mystery; Voyage has an uncertain
`human_review` route and `medium` rubric. The fixture's project-level review
probability is `0.25`. These values were chosen for teaching, not measured from
real researchers or a model.

Read the source in this order:

1. `caller_producer()` supplies proposed answers without changing the graph.
2. `main()` selects the graph records and states the allowed answers.
3. `validate_decision_batch()` checks the submitted answers against that request.
4. Caller code checks the actual status and current state before applying its
   separately permitted sample action.

The script prepares opaque item/question IDs and content digests for you.
IDs identify which answer belongs to which question or item; digests bind it to
selected content. You do not need to invent those contracts to run the example.

## Interpret uncertainty and validation correctly

| Result                     | What it means                                         | What it does not establish           |
| -------------------------- | ----------------------------------------------------- | ------------------------------------ |
| Choice                     | One of the options supplied by the caller             | The best option or permission to act |
| Rubric label               | A value in the caller's ordered scale                 | A universal quality score            |
| Yes/no probability         | A supplied value in the declared probability range    | Calibration or factual correctness   |
| `uncertain`                | The producer identifies uncertainty                   | A negative answer                    |
| `missing` or `unavailable` | An expected answer is absent or could not be obtained | Evidence against the proposition     |

The validator checks allowed values, finite numbers, stable correlation, and
request bounds. It does not rank candidates, choose a threshold, judge evidence,
or normalize malformed probabilities. A valid partial response keeps missing
items explicit. Decide a review or clarification path in caller code.

## Change the producer or the policy

You can replace the local fixture with your own function or independently
produced data. Preserve question/item identity when results arrive in a different
order. Record the producer revision if known; do not invent one.

Before acting on a result, check that the selected live state has not changed.
A stale result needs review or revalidation. Keep action failure distinct from
successful validation. For a retry of a native mutation, preserve its prepared
operation identity and exact request rather than constructing a new operation
and risking a duplicate effect.

The [Node example](../../../examples/decision-workflow/agent.mjs) reads an
independent Arrow artifact before validation. Its
[README](../../../examples/decision-workflow/README.md) documents setup and
expected output; Node needs `apache-arrow`. Python returns a PyArrow table;
Node returns Arrow IPC bytes.

## Evaluate and retain results when needed

The [evaluation fixture](../../../examples/decision-workflow/evaluate_fixtures.py)
compares a small labeled example with an explicit rule baseline. It separates
answer errors, missing results, uncertainty, and review decisions. Its four-row
Brier score illustrates the calculation; it is not evidence of real-model
quality or calibration. Evaluate representative, independently reviewed task
cases before using a threshold in a real workflow.

The examples are memory-only. Retaining an Arrow result file is different from
retaining all graph and source context behind it. For richer retention, use the
[research workspace contract](../architecture/research-workspaces.md#external-decision-results-1577)
and [knowledge API](../architecture/knowledge-public-api-v1.md). These expert
references explain explicit Artifact/Version registration and the exact batch
schema. [Native test evidence](../../engineering/TESTING.md#m12-decision-workflow-contract)
is separate from candidate-package and human-use qualification in
[#1209](https://github.com/CurateLabs/graphforge/issues/1209).
