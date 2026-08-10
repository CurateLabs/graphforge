# Agent skills package

The `@curatelabs/graphforge-agent-skills` package is the NPX distribution surface for
GraphForge agent workflows. The package foundation declares compatibility with
GraphForge v0.5.0 and contains a shared version-1 adapter with no graph,
knowledge, or workflow semantics. It discovers existing v0.5 projects without
mutation, opens them only through `@curatelabs/graphforge`, decodes native Arrow IPC
through `apache-arrow`, checks exact capability versions, and normalizes errors
for agents. It contains no alternate backend or runtime fallback.

Every adapter and workflow open selects native embedded-write options. The
validated choices are `single_writer` (default), `queued_writer`, and
`optimistic_multi_writer`, with bounded queue capacity and rebase attempts.
These options are forwarded unchanged to `@curatelabs/graphforge`; the package does
not infer operation, actor, provenance, graph, or knowledge identities and does
not provide MCP, HTTP, or any other server transport.

The adapter's deterministic interchange rules are lowercase hyphenated UUIDs,
Arrow schema-order rows, decimal strings for 64-bit integers, recursively
key-sorted JSON objects, and one trailing LF. Unsupported or future capability
versions fail with a structured version-1 adapter error.

Skill manifests and invocation envelopes use the checked-in JSON Schema
2020-12 contracts under `packages/agent-skills/schemas`. The package exports
offline validators from `@curatelabs/graphforge-agent-skills/schemas`; they fail closed on
unknown, missing, malformed, or incompatible-version inputs and return at most
eight deterministic diagnostics without echoing rejected values.

The shared security contract is fail-closed. Project discovery rejects lexical
parent traversal, control characters, directory symlinks, and marker symlinks
without returning private paths. Native exception messages and malformed codes
are not reflected. Adapter and schema payloads reject cycles and enforce a
depth-16, 4,096-entry, 4,096-character recursive budget. Subprocess execution is
explicitly unsupported, and the package contains no `child_process` adapter
import. These limits are published in `compatibility.json` and verified again
from the locally packed, offline-installed consumer artifact.

Run the package tests and deterministic offline installation smoke from the
repository root:

```bash
pnpm test:agent-skills
pnpm smoke:agent-skills
pnpm format:agent-skills
```

The smoke creates the package twice and requires identical SHA-256 hashes and
file manifests. It then installs the tarball into a clean temporary project
with `npm install --offline` and invokes:

```bash
npx --offline --no-install graphforge-agent-skills compatibility --json
```

No registry access or package publication occurs.

## Preservation-first workflows

The package exports `bootstrapProject`, `buildKnowledge`,
`resolveBeliefSubject`, `narrateBeliefRecords`,
`dispatchRecordedNeutralAnalysis`, `exploreGraph`, and `retrieveAnalyze` from
`@curatelabs/graphforge-agent-skills/workflows`, with checked manifests under
`packages/agent-skills/skills`. Bootstrap uses the public native constructor and
Arrow query surface to create or reopen a zero-server local project, then closes,
reopens, and queries one reserved idempotent marker. Exploratory mode is the
default; advisory mode requires an explicit ontology path, and any requested
mode mismatch fails as a structured conflict.

Build knowledge enables only the public provenance, knowledge, and optionally
epistemic capabilities. It adds graph records through UUID handles, publishes an
assertion and non-empty evidence bundle through the native composite API, then
appends the required explicit confidence assessment. Epistemic (**epistemic**)
reasoning or status is appended only when explicitly requested.
`confidence` graph properties are never converted to assessments, and a
knowledge-layer (**knowledge**) assertion remains statusless unless the caller
supplies an epistemic (**epistemic**) status block.
Separate public writes are not represented as one larger transaction; each
native failure preserves the previous complete generation, and the workflow
does not destructively compensate or overwrite records.

`resolveBeliefSubject` resolves one explicit assertion UUID or hypothesis
question key at a caller-supplied transaction cutoff. Optional valid time
remains a separate input. The caller must provide every field of the version-1
belief-projection policy. Native `resolveBeliefSubject` remains authoritative;
the package only decodes its canonical Arrow evidence and exposes the opaque
native projection. Results retain
statusless, unselected, competing, and superseded identities plus exact source
UUIDs and native projection fingerprints. The workflow never selects from
confidence, and unresolved native ambiguity stays a structured error.

`narrateBeliefRecords` reuses that subject resolution, then pages every public
assertion, graph-reference, status, validity, supersession, confidence, evidence,
reasoning, provenance, and hypothesis history for the scoped UUIDs. It fails
closed when the caller `record_budget` is exhausted and returns UUID-addressed
`projection_descriptors` for broader project-level collections instead of
silently truncating them.

`dispatchRecordedNeutralAnalysis` accepts the opaque resolved projection plus a
caller-prepared `InvocationDescriptor` and exact run/operation/attachment
identities. It invokes the public resolved recorded-analysis API, returns Arrow
result linkage with independent run and attachment lifecycles, and preserves a
completed knowledge-layer (**knowledge**) run when attachment is absent or fails.

`exploreGraph` accepts an explicit explore mode, start UUIDs, and finite
`result_limit` (plus depth for neighborhood/traversal, and a target UUID for
path mode). It opens only the graph capability, dispatches the public Node
paths/descriptor facade, returns UUID-addressed summaries with complete-result
linkage, and fails closed before native invocation when bounds are missing.

`retrieveAnalyze` dispatches caller-selected find/index (**search**) inputs and
live analyst-verb (**algorithm**) families (`rank`, `cluster`, `paths`, `analyze`,
`similar`) through public Node facades without inferred
algorithm/provider/freshness choices, keeps knowledge-layer (**knowledge**) and
epistemic (**epistemic**) tables unopened, and exposes truncation/empty/structured-error
outcomes.

## Release-candidate end-to-end verification

Analyst-agent and developer-agent scenarios are defined once in
`packages/agent-skills/rc/scenarios.js` and reused by CI goldens
(`packages/agent-skills/tests/rc-e2e.test.mjs` /
`packages/agent-skills/tests/goldens/`), executable examples
(`packages/agent-skills/examples/`), and the native pack-install runner
(`packages/agent-skills/scripts/run-native-rc-e2e.mjs`).

```bash
pnpm test:agent-skills
pnpm smoke:agent-skills
pnpm --filter @curatelabs/graphforge-agent-skills example:analyst
pnpm --filter @curatelabs/graphforge-agent-skills example:developer

GRAPHFORGE_NODE_MODULE=$PWD/crates/graphforge-bindings-node/index.js \
  pnpm --filter @curatelabs/graphforge-agent-skills test:rc-native -- \
    --commit-sha "$(git rev-parse HEAD)" \
    --evidence target/release-workflows/agent-skills/rc-e2e.json
```

The native runner installs the local `npm pack` artifact offline, exercises
bootstrap/build/explore/find/analyst-verb/belief/reopen plus developer
embed/error paths, redacts volatile paths/timestamps, and records
GraphForge/package/Node versions with the commit SHA. npm publication remains
part of the coordinated **v0.5.1** release close-out.
