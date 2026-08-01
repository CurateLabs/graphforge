# GraphForge agent skills

This package is the distributable NPX shell for GraphForge agent skills. Its
foundation includes the versioned, semantics-free adapter used by every skill.
The first preservation-first workflows bootstrap local projects and append
explicit graph and knowledge records.

## Shared adapter

Import the adapter from `@graphforge/agent-skills`. Pass the shipped
`GraphForge` constructor from `@graphforge/node` and `tableFromIPC` from
`apache-arrow` to `openProject`; the adapter does not bundle or substitute a
runtime:

```js
import { openProject } from "@graphforge/agent-skills";
import { tableFromIPC } from "apache-arrow";
import { createRequire } from "node:module";

const { GraphForge } = createRequire(import.meta.url)("@graphforge/node");
const opened = await openProject({
  GraphForge,
  path: "/absolute/path/to/project",
  requiredCapabilities: { graph: 1, workspace: 1 },
  tableFromIPC,
  writeOptions: {
    writeMode: "optimistic_multi_writer",
    writeQueueCapacity: 64,
    maxRebaseAttempts: 3,
  },
});
```

Discovery accepts only an existing, real directory with the exact v0.5
`FORMAT` marker. Missing, ambiguous, symlinked, malformed, and future-format
inputs fail before the native constructor is called. Capability checks require
exact versions and reject `unsupported_future` rows.

Every adapter/workflow open passes an explicit validated embedded-write options
object to the native binding. Callers may select `single_writer` (the default),
`queued_writer`, or `optimistic_multi_writer`; invalid names, unbounded queues,
and unbounded retry counts fail before native construction. The adapter does not
add coordination semantics, identity inference, a server, or a transport.

The adapter contract version is `1`. `uuidToString` emits lowercase hyphenated
UUIDs, `tableToJson` preserves Arrow schema column order while representing
64-bit integers as decimal strings, and `stableJson` recursively sorts object
keys and appends one LF. Structured failures serialize as
`{code, contract_version, details, message}`.

The shared boundary rejects parent-traversal and control-character paths,
project-directory or `FORMAT` symlinks, and unsupported subprocess requests
before any native constructor or command can run. Discovery errors report only
bounded counts, never candidate paths. Native error text is replaced with a
fixed message while a valid `GF_*` code is retained. Recursive values are
cycle-checked and bounded to depth 16, 4,096 entries, and 4,096 characters per
string; violations return a fixed structured error without reflecting content.
The adapter deliberately has no `node:child_process` import or execution path.

## Skill schemas

The package ships closed, versioned JSON schemas for skill manifests and
input/output envelopes. Import the dependency-free offline validators from
`@graphforge/agent-skills/schemas`. Rejected values produce at most eight
stably ordered diagnostics with fixed, non-value-bearing messages. See
[`schemas/README.md`](schemas/README.md) for the contract and identifiers.
Input, output, and error-detail payloads enforce the same recursive value
budgets in the dependency-free validator; the checked-in schemas additionally
bound every nested string, array, and object.

## Local offline smoke

From the repository root, create the local artifact and run the deterministic
offline install/invocation smoke:

```bash
pnpm --filter @graphforge/agent-skills pack:local
pnpm --filter @graphforge/agent-skills test:offline
```

The smoke test installs the packed tarball into a clean temporary project and
invokes it without registry access:

```bash
npx --offline --no-install graphforge-agent-skills compatibility --json
```

The compatibility response is machine-readable and currently declares support
for GraphForge `>=0.5.0 <0.6.0`.

## Bootstrap, build knowledge, and resolve belief

Import `bootstrapProject`, `buildKnowledge`, `resolveBeliefSubject`,
`narrateBeliefRecords`, `dispatchRecordedNeutralAnalysis`, `exploreGraph`, and
`retrieveAnalyze` from `@graphforge/agent-skills/workflows`. Both require the
shipped `GraphForge` and `tableFromIPC` surfaces shown above. Bootstrap creates
or reopens a local project, defaults to exploratory ontology mode, and verifies
one reserved marker through a real close/reopen/query cycle. Replays return the
same marker UUID; duplicates or ontology-mode mismatches return structured
conflicts.

Build knowledge accepts caller-keyed nodes and edges plus caller-supplied UUIDv7
operation and ledger identities. It enables the public provenance/knowledge
capabilities, uses native UUID handles for graph references, and uses the atomic
assertion-plus-nonempty-evidence API, then appends the required explicit M20
confidence assessment. M21 reasoning and first status are appended only when
their explicit input blocks are supplied. A domain property named `confidence`
remains an ordinary graph property.

Every native Arrow receipt is decoded into bounded deterministic JSON rows.
Native generation publication remains authoritative: a failed composite write
does not replace its previous complete generation. The workflow does not promise
a transaction spanning separate public API calls and never compensates by
deleting prior records.

`resolveBeliefSubject` accepts exactly one assertion UUID or hypothesis
question key, a transaction cutoff, an independently optional valid time, and
the complete version-1 belief-projection policy. Rust resolves the subject and
returns canonical Arrow evidence with its opaque graph projection. The thin
workflow decodes that evidence and returns the addressed
assertion identities, competing and superseded alternatives, statusless or
unselected state, exact source-record UUIDs, and native projection
fingerprints. Native ambiguity remains a structured error; confidence is never
used to choose an alternative. The returned opaque `projection` is reserved for
later recorded dispatch through the native resolved-projection API.

`narrateBeliefRecords` pages every public scoped M20/M21 history family for the
resolved assertion set within an explicit `record_budget`, keeps competing and
superseded records distinguishable, and returns project-level pagination
descriptors rather than truncating broader collections.

`dispatchRecordedNeutralAnalysis` forwards one caller-prepared descriptor to
`invokeResolvedRecorded`, returns the Arrow result with independent run and
attachment state, and leaves completed M20 runs queryable when attachment fails.

`exploreGraph` dispatches bounded neighborhood, traversal, path, and
reachability requests through the public paths/descriptor facade, fails closed
without finite bounds, and opens only the graph capability.

`retrieveAnalyze` reaches public M19 find modes and every live M18 family with
explicit caller-selected algorithms/inputs, finite `result_limit`, and
graph-only capability opens.

## Release-candidate end-to-end scenarios

Analyst-agent and developer-agent scenarios live in `rc/scenarios.js` and are
shared by:

- golden CI tests in `tests/rc-e2e.test.mjs`
- executable examples in `examples/`
- the native pack-install runner `scripts/run-native-rc-e2e.mjs`

```bash
pnpm --filter @graphforge/agent-skills test
pnpm --filter @graphforge/agent-skills example:analyst
pnpm --filter @graphforge/agent-skills example:developer

# Optional native evidence against a local @graphforge/node build
GRAPHFORGE_NODE_MODULE=$PWD/crates/graphforge-bindings-node/index.js \
  pnpm --filter @graphforge/agent-skills test:rc-native -- \
    --commit-sha "$(git rev-parse HEAD)" \
    --evidence target/release-workflows/agent-skills/rc-e2e.json
```

The native runner packs the package, offline-installs the tarball into a clean
temporary consumer, verifies public workflow exports and compatibility, then
runs both scenarios against the pinned Node binding. Evidence redacts volatile
paths/timestamps and records the commit SHA plus package/runtime versions.
Publication remains the v0.5.0 publication close-out issue.
