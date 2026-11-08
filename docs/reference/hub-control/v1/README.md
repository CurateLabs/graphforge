# Native Hub control v1

`graphforge-hub-control/1` is the Core-owned catalog, account and hosted object
management protocol. It is a prerequisite for a Hub implementation and native
clients. The client and fixture tests do **not** establish that a hosted Hub
implements this endpoint, its transactional policies, or an editor experience.
Remote ontology adoption prepare/apply is a separate native contract.

## Authority and types

The authoritative closed DTOs and validators are
[`graphforge-discovery::hub`](../../../../crates/graphforge-discovery/src/hub.rs).
The native client is
[`auth/control.rs`](../../../../crates/graphforge-cli/src/auth/control.rs); the
process adapter is
[`hub_control.rs`](../../../../crates/graphforge-cli/src/hub_control.rs).
Hub services must consume these Rust validators/types directly or the generated
TypeScript declarations and Rust-backed exports of
[`@curatelabs/graphforge-protocol-wasm`](../../../../packages/protocol-wasm/README.md):

- `validate_hub_control_request(request)` parses the bounded closed request.
- `validate_hub_control_response(request, response)` binds a bounded reply to its
  initiating request, exact selection, scope and operation family.
- `prepare_hub_management_plan(input)` constructs the native review commitment
  from fresh **server-authorized** facts. It does not confer authority.

The package generates `dist/graphforge_protocol_wasm.d.ts` from Rust/tsify during
its build. Do not maintain another schema, canonicalizer or semantics engine in
TypeScript. [Conformance examples](../../../contracts/hub-control/v1/conformance.json) contain accepted/refused
request/response pairs and a native-checked commitment vector; they are contract
inputs, not hosted execution receipts.

## HTTP and process interface

Send `POST https://<selected-hub>/api/hub/v1/control`, with JSON content and
accept headers. A bare explicit HTTPS origin selects the Hub; credentials,
queries, fragments and paths in that origin are refused. Production transport
requires public addresses, disables proxies, and never follows POST redirects.
Requests are at most **64 KiB**, replies at most **1 MiB**, and pages contain
**1–50** records/accounts. These are encoded UTF-8 byte limits. Parse duplicate
JSON members, unknown fields/variants, unsupported formats and noncanonical
identities as errors. The service must enforce the request bound before parsing
and use `HubControlResponse::to_json(request)` (or native response validation)
before writing a reply. Oversize detail returns a closed `incomplete` error;
it must not truncate an exact document into a misleading successful result.

Every request has `format`, a canonical nonnil lowercase `request_uuid`, and an
`operation` discriminated by `kind`. Every reply echoes format and request UUID
and contains a closed `result`. A successful protocol exchange uses HTTP 200;
non-200 replies must contain `result.kind = "failure"` with a closed code.
Unknown or malformed remote error bodies are never displayed as prose.

The supported process entry point is:

```sh
gf hub control --request-file public-search.json --hub https://graphforge.sh --anonymous
gf hub control --request-file accounts.json --hub https://graphforge.sh
```

The file is a finite regular JSON file; errors never print its native path or
contents. A validated response is emitted as one JSON document. Exit **0** means
a read/preparation succeeded or a mutation receipt is `applied`. With a validated
response, exit **1** means a failure/refused operation and exit **2** means a
`prepared`/`pending` receipt requiring reconciliation. Local errors instead use
the existing CLI diagnostic/exit classes (including exit 2 for validation) and
emit no successful response. Always inspect the validated result or diagnostic
code; exit 2 alone does not imply a pending operation. An output-write error is
failure, including when the server may already have applied the operation. Do
not infer success from process interruption, missing output or an exit without
a validated reply.

`--anonymous` never accesses the vault. `--protection-bypass-env NAME` supplies
the existing separate preview-deployment bypass; it grants no human/account
identity and never travels to data-plane object origins. Never put a credential
in the JSON or a command-line argument.

Saved human sessions reuse `gf auth login`, the existing OS vault and same-origin
refresh. Account/search-management calls require the actual attached native
process. Captured `runCli` does not read the vault for control; spawn the exact
`gf` process asynchronously on the host where it will run. In remote SSH,
containers or WSL, login and vault access belong to that execution host; a local
machine's session is not exported. Capabilities never consult credentials.
Public reads work signed out; an attached process may use its saved session for
inspect/public reads. A revoked/expired session is a denial, never an implicit
private-to-public downgrade. Repository-scoped automation credentials grant no
account authority. No provider organization chooser or personal-token prompt
is part of this contract.

## Discovery and privacy

| Operation             | Result                     | Semantics                                                                                                                                    |
| --------------------- | -------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| `capabilities`        | `capabilities`             | Unique supported families: `accounts`, `public_search`, `private_search`, `starred_search`, `inspect`, `manage`. These convey no permission. |
| `accounts {page}`     | `accounts {accounts,page}` | Fresh authorized namespaces, labels, personal/organization kind and current member/owner role; a handle may be unclaimed.                    |
| `search {query,page}` | `search {records,page}`    | Explicit public, account or starred scope.                                                                                                   |
| `inspect {target}`    | `inspect {record,detail}`  | One stable Project or exact ontology, without default/latest fallback for an ontology identity.                                              |

A search query contains `scope`, literal `text` (≤512 UTF-8 bytes), optional
`object_kind` (`project`/`ontology`), up to 16 unique exact `tags`, and optional
`visibility` (`public`/`private`). `scope` is `{kind:"public"}`,
`{kind:"account",account_id:...}`, or `{kind:"starred"}`. The latter two require
a saved human session. A public search with an auth header **remains public**.
Public records must be public and omit `account_id`; account records must bind
the selected authorized account. Starred records must have `starred:true` and
be reauthorized against current visibility. Denied/hidden objects are never
included merely because the caller once starred or published them.

`page` requests have `limit` and nullable opaque `cursor` (≤4096 bytes). Continue
with exactly the original query, limit and authorization context. The service
binds cursors to actor, query, scope, ordering and snapshot/authority state;
tampering, a scope change, expired membership or revoked authority must refuse
or restart explicitly. Core never invents offsets, sorts a page independently,
or manufactures a continuation. Result order is the server's explicit array
order. Duplicate records/accounts and repeated continuation tokens are invalid.

Response page information includes `next_cursor`, `coverage` (`complete`,
`partial`, `unknown`) and `total`: `exact`, `lower_bound`, `approximate` with a
JSON-safe integer `value`, or `unknown`. A missing cursor is not proof of scope
completeness. An unknown count is not zero. Any future facet or preview count
must use the **same server authorization and scope** as the records; never query
public catalog indexes and describe them as private inventory. This version
has no independent facet API.

A `HubRecord` binds `target` to stable `ItemRepositoryFact` identity, current
canonical owner/name and repository revision. Titles/descriptions/tags/license
are bounded untrusted display text. `actions` are unique current UI guidance,
not authority or reusable grants. Advisory `producing_core_version` must not
become a universal native reader-version veto. Native semantic metadata changes
remain edit-and-republish through Core's existing publish protocol.

A Project target is `{kind:"project",repository_id:...}`. `inspect.detail`, when
available, reuses native `DiscoveryManifest`, `RefSet`, and advertised
`ResearchLineage`, with all their existing validation and snapshot bindings.
They carry explicit ordered refs/default, immutable repository versions,
Branch/Version/tag selectors, Fork/Proposal lineage and manifest preview
metadata. No lineage may be omitted when the supplied manifest advertises it.
Absent `detail` means unavailable detail, not an empty Project. Use `gf hub list`
for verified named repository metadata. Exact clone selectors remain `gf clone`
`--ref`, `--version-uuid`, and `--tag`; their existing selection and package
verification rules remain authoritative. Preview descriptors are metadata;
fetch and validate image bytes separately with the existing native preview
validators. Do not execute markup, command links or URLs from display metadata.

An ontology target is `{kind:"ontology",id,version,digest}`: canonical permanent
bare ontology ID, opaque exact authored version, and canonical content digest.
Its record must include the matching native `ItemDescriptor`, bound to its
publishing repository and retained refs/manifest validators. Authored versions
are not inferred SemVer, and ontology identity differs from runtime catalog or
Project identity. Metadata never proves module/package bytes. Native
`gf ontology module fetch` negotiates the exact descriptor at the canonical
item URL, authorizes that same-origin request using the saved session when
needed, binds the verified descriptor's repository, reads its exact retained
snapshots, and verifies the advertised package. Named module reads and
`gf hub list` share the existing authenticated clone transport. Identity tokens
stay on selected Hub metadata endpoints; objects use separate repository-scoped
read grants only at Hub-approved object origins. Authenticated redirects are
refused; named repository move markers still require refs/manifest agreement.

## Review, apply and reconcile

Management operations are `prepare {operation_uuid,mutation}`, `apply {plan}`
and `status {operation_uuid,intent_digest}`. A request UUID identifies one
exchange; a caller-chosen operation UUID identifies one durable intended change.
Generate the latter once, before prepare, and retain it with the reviewed plan.
Never replace it just to retry an uncertain apply.

Supported closed mutations are:

- `star {target,witness,starred}` sets an explicit desired state, never toggles.
- `visit {target,witness}` explicitly advances seen state; reads never visit.
- `set_visibility {repository_id,visibility}` retains entitlement checks,
  audit effects and search refresh.
- `rename {repository_id,name}` preserves stable identity and existing aliases.
- `delete_empty {repository_id,confirm_name}` refuses content and busy state.
- `tombstone {account_id,id}` is ontology lifecycle, separate from Project
  deletion; native dependencies/public-head reuse and uncertain dependency proof
  must refuse it.

An ontology star/visit requires `witness {repository_id,immutable_version}` for
the exact public publication; Project star/visit uses null witness. Share Hub's
existing public-target and claimed-profile policy. Unsupported visit or other
policy is `unsupported_operation`; capabilities/actions must describe support
honestly. Do not introduce hosted semantic edits or reinterpret empty-only
Project deletion as arbitrary content deletion.

Prepare authenticates a validated human Actor and obtains fresh authority and
object/policy revisions from the responsible services. Construct the plan with
Rust from `hub_origin`, `operation_uuid`, the **full mutation**,
`precondition {authority_revision,object_revision}`, and host-fixed Unix
`expires_at_ms`. Each revision is opaque and must change when its protected
state changes, including revocation/ABA. The commitment is SHA-256 of the byte
domain `graphforge-hub-management-plan/1\0` followed by native canonical JSON of
`[hub_origin,operation_uuid,mutation,precondition,expires_at_ms]`. Neither a hash
nor a caller-provided revision grants authority. The native client refuses a
cross-Hub or expired plan before accessing credentials or the network.

Hub needs a **separate durable management ledger**, not publish receipts. Before
acknowledging prepare/apply it must transactionally bind the operation UUID to
the actor, selected account/object, full intent, reviewed revisions, commitment,
expiry and operation state. Same UUID/same intent reconciles; different intent
conflicts, even across process restarts. On apply, atomically reauthenticate and
reauthorize the current Actor/account, recheck expiry, both revisions and policy,
and commit/schedule exactly the prepared operation with its ledger transition.
A client must never submit fabricated web-auth context; share the Actor-aware
namespace, visibility, deletion, lifecycle and star services.

Receipts echo operation UUID and commitment. States are `prepared` (no effect),
`pending` (accepted/scheduled, unfinished), `applied` (completed), and `refused`
(no requested effect, mandatory closed refusal code). Other states have null
refusal. Optional apply receipt records remain currently authorized and must match
the reviewed target and any completed visible effect. Status receipts omit
records because their request carries replay identity alone; inspect an explicit
exact target to refresh metadata. Absence proves nothing about object existence. Scheduled
rename/deletion stays pending until its actual effect completes. Ledger workers
must recover idempotently after restart and serialize conflicting operations.
Retain tombstones/replay identities long enough to prevent reuse after expiry;
status must reconcile accepted operations even after a review plan expires.
Fresh authorization still applies to status, and revocation must not leak
private records. Hub delivery must independently prove these transactional and
crash/replay properties before advertising `manage`.

POST is sent once, with no automatic retry. Missing, malformed, redirected,
oversize, interrupted or contradictory apply replies produce
`hub.control_outcome_uncertain`. Preserve UUID/commitment and issue **status**;
never silently prepare a new plan, repeat an unreviewed mutation, or claim
success. `prepared` can be reapplied only as the exact reviewed plan while valid;
`pending` requires status; `applied` is terminal; `refused` requires the recovery
appropriate to its code. An unavailable status leaves the outcome uncertain.

## Closed errors and recovery

| Code                                       | Recovery                                                                                |
| ------------------------------------------ | --------------------------------------------------------------------------------------- |
| `authentication_required`, `access_denied` | Login on the execution host or restore current authority; disclose no hidden existence. |
| `not_found`, `exact_version_unavailable`   | Preserve the exact selection; reselect explicitly, never fall back to latest.           |
| `unsupported_operation`, `invalid_request` | Refresh capabilities/use a supported native request.                                    |
| `stale_plan`, `expired_plan`               | Obtain fresh authorized facts, prepare and review a new plan.                           |
| `idempotency_conflict`                     | Preserve the original operation identity and reconcile its original intent.             |
| `entitlement_required`                     | Resolve the current account policy before preparing again.                              |
| `rate_limited`, `busy`                     | Respect nullable `retry_after_ms` (≤1 hour); reconcile an uncertain mutation first.     |
| `not_empty`, `ontology_in_use`             | Preserve content/dependencies; do not broaden deletion.                                 |
| `incomplete`, `service_unavailable`        | Treat missing proof as unknown; retry the same read or status explicitly.               |

Errors have no remote prose, private payloads, native paths, credentials or
signed URLs. Local transport/parser diagnostics use fixed recovery text. Render
successful authorized data only in its intended result surface; do not log
session headers, raw remote bodies or request-file contents.
