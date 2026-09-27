# Provider neutral decision workflows

GraphForge can validate caller supplied choice, ordered rubric, and yes/no
probability results against bounded, explicitly identified graph context. The
producer, ranking rules, review thresholds, and any action remain ordinary
caller code. A valid result never authorizes a write.

## Python and Node

For a memory only first use, create GraphForge without a path, select a finite
set of objects, and build a DecisionBatchV1 from the observed generation, the
selected UUIDs, and digests of the exact projection and selection. Generate
question IDs with the language's standard UUID library. A deterministic local
fixture can return a choice such as research or human_review without network
access; callers can replace that function with an independent application
producer.

Python exports DecisionBatchV1 and the supporting TypedDicts from graphforge.
Pass the batch to GraphForge.validate_decision_batch(). It returns a
pyarrow.Table with UUID and digest byte arrays, nullable values, and Float64
probabilities intact. Node exposes validateDecisionBatch() and returns Arrow
IPC bytes for apache-arrow's tableFromIPC(). Its discriminated TypeScript
contracts are in @curatelabs/graphforge/lib/decision.

Runnable offline examples are in examples/decision-workflow/analyst.py and
examples/decision-workflow/agent.mjs. They create a memory only graph, select a
bounded projection, use labeled local fixtures, inspect uncertainty, and gate
a sample action in caller code. See the examples README for install and run
commands; the Node example also needs apache-arrow.

The Python example invokes a caller-owned function that returns route, rubric,
and project-level probability records. The Node example creates and reloads an
independent Arrow result artifact, then joins results by question and item UUID
before validation. Neither path adds a provider to Core. The checked-in
four-row evaluation fixture reports its rule baseline, answer errors, missing
and uncertain results, review decisions, and a fixture-only Brier score; it is
an executable measurement recipe, not model-quality or calibration evidence.

The composition test
`crates/graphforge-api/tests/decision_results.rs::composed_workflow_supports_independent_producers_explicit_action_and_replay`
uses a durable graph rooted on a supported filesystem. It validates both
producer paths through the Rust facade, confirms private fields are excluded,
keeps missing/unavailable/uncertain and action failures distinct, rejects a
stale action, and verifies exact Arrow payload identity plus exact receipt
replay after cleanup and reopen. Python and Node examples are memory-only;
their direct native validation tests are
`crates/graphforge-bindings-py/tests/decision_results.py` and
`crates/graphforge-bindings-node/tests/decision-results.test.mjs`.

At this evidence point, the binding package metadata remains `0.5.2`. The
coordinated `0.6` compatibility/version update belongs to #858; this M12 work
does not claim a released package version.

The CLI accepts the same JSON contract and writes the Arrow result without
invoking a producer: gf --project PROJECT research decision validate --file
BATCH.json --output DECISIONS.arrow. The project is opened under the normal
filesystem admission rules; the command does not mutate it.

The Rust validator enforces the 256 expected result row limit, exact
question/item correlation, finite choice/rubric domains, valid probabilities,
and declared confidence scales. Missing and unavailable outcomes remain
distinct from negative answers. It does not normalize probabilities, rank
candidates, infer confidence meaning, or select a policy threshold.

## Explicit policy and retry

Caller code may route a clear result only after checking that the input
generation is still current. Uncertain, missing, unavailable, or stale results
should take a caller chosen clarification or review path. For an optional
write, prepare its operation identity and expected state once, retain the exact
request and resulting receipt, and reuse that request for an exact retry.
Changed state requires explicit review or revalidation. Existing research
prepare/commit and Proposal APIs provide the durable mutation receipts and
replay contract; external decision results themselves create no authority.

The binding tests in
crates/graphforge-bindings-py/tests/decision_results.py and
crates/graphforge-bindings-node/tests/decision-results.test.mjs exercise the
real Rust validator. The Rust contract and reopened Artifact evidence are
documented in the [research workspace guide](../architecture/research-workspaces.md#external-decision-results-1577).

## Retaining results

The initial path can remain memory only. To retain a result, serialize the
validated Arrow table as Arrow IPC, explicitly register it as a local Artifact
with a Source and selected-item derivation references, then capture a research
Version. Reopen that Version before relying on retained evidence. Payload
availability follows ordinary Version retention; release can make it
unavailable. Durable project roots require ext4, xfs, or btrfs.
