# CLI JSON error contract for agents

Every `gf` command reports failures as one stable JSON document when you pass
the global `--json` flag. Agents and other programs branch on the JSON fields;
they never parse the human-readable message. This page is the authoritative
contract: the codes below come from the CLI source (`crates/graphforge-cli`).
Tests exercise every error class and keep the Hub publish wire-code rows in
sync with the native protocol vocabulary; local preparation codes are listed
separately.

## Which field is authoritative

Branch in this order:

1. `error.details.semantic_code` — the stable, fine-grained machine field
   (`hub.network`, `inventory.not_found`, …). Present exactly when the failure
   carries a stable dotted contract code. This is the field agents key on for
   Hub, clone, and publish behavior.
2. `error.diagnostics[].code` — multi-ontology failures only: read the
   per-diagnostic codes before falling back to the outer code, because they
   can reclass the failure. A concurrent portable change, for example, is the
   outer `GF_IDEMPOTENCY_CONFLICT` with the diagnostic
   `inventory.generation_conflict` — a retry after refreshing, not the
   operation-identity decision the outer code alone suggests. The complete
   vocabulary is tabulated under [Diagnostic codes](#diagnostic-codes-errordiagnosticscode) below.
3. `error.code` — the coarse, always-present `GF_*` outer code
   (`GF_VALIDATION`, `GF_IO`, …). Use it when `semantic_code` is absent and to
   derive the exit class.
4. Exit code — a coarse fallback documented below.

Never branch on `error.message`: it is diagnostic text, not a contract.

## Envelope shapes

With `--json`, a failed command writes exactly one JSON document to standard
error. Successful output (results, receipts) always goes to standard output.

Core runtime and argument errors:

```json
{"error":{"code":"GF_IO","message":"storage error: hub.network: request failed",
"details":{"source":"runtime","kind":"storage","semantic_code":"hub.network"}}}
```

- `error.code` — stable `GF_*` outer code.
- `error.message` — human-readable diagnostic; not a contract.
- `error.details.source` — `runtime`, or `argument_parser` when clap refused
  the arguments (`error.code` is then `GF_VALIDATION` and `semantic_code` is
  absent).
- `error.details.kind` — fault domain: `validation`, `parse`, `bind`, `plan`,
  `execution`, `provider`, `storage`, `project`, `api`, `lifecycle`,
  `ontology`, `not_implemented`, or the lowering/algorithm fault domain.
- `error.details.semantic_code` — optional; the stable dotted code this page
  tabulates.
- `warnings` — optional, a sibling of `error`, present only when the command
  raised diagnostics before it failed. Today these are Core version warnings
  (`{"code":"GF_CORE_VERSION_MISMATCH","producing_core_version":…,"running_core_version":…,"message":…}`):
  the project or package was written by another Core version. A warning never
  changes the error class; it names the runtime that matches the data. The
  same `warnings` array is added to a successful command's single stdout
  document.

Multi-ontology facade failures use a diagnostics list instead of `details`:

```json
{"error":{"code":"GF_ONTOLOGY_DIAGNOSTIC","message":"…","diagnostics":[
{"code":"dependency.in_use","phase":"…","message":"…","subjects":["…"],
"candidates":["…"],"remediation":"…","limit":64}]}}
```

Each `diagnostics[].code` is a stable dotted code, and `remediation` is
Rust-owned guidance mirroring the next action. The complete diagnostic
vocabulary is tabulated under [Diagnostic codes](#diagnostic-codes-errordiagnosticscode).

Without `--json`, failures print `CODE: message` plain text. Agents must pass
`--json`.

## Exit codes

| Exit | Meaning |
| --- | --- |
| `0` | Success. |
| `1` | Failure not otherwise classified (includes `GF_IDEMPOTENCY_CONFLICT` and `GF_WRITER_BUSY`). |
| `2` | Fix the input: argument refusals and every `GF_VALIDATION`, including all `hub.publish.*` codes except `internal` (exit 3) and `idempotency_conflict` (surfaced as `GF_IDEMPOTENCY_CONFLICT`, exit 1). |
| `3` | Transport or storage: `GF_IO` (including `hub.network`), `GF_UNSUPPORTED_FILESYSTEM`, `hub.publish.internal`, and the transient Hub statuses (`hub.rate_limited`, `hub.unavailable`). |
| `4` | `gf sync --check` found drift. Control-flow result, not an error; the JSON body is on standard output. |

## Error classes

Every code has exactly one class — what the caller should plan to do next:

- **retry** — the same command may succeed later without any change (the CLI
  already retries transient transport failures in process). Wait or rerun.
- **fix input** — change something the caller controls (arguments, project
  state, selection, or the credential available to the process), then rerun.
- **ask a human** — a person must decide or act outside this process
  (permissions, entitlements, operation identity, corrupted state, Hub
  internals).

The "next action" column is the concrete first step; several fix-input codes
escalate to a human when the obvious fix does not clear the failure.

## Semantic codes (`error.details.semantic_code`)

### Transport and availability (retry)

| Code | Class | Meaning | Next action | Exit |
| --- | --- | --- | --- | --- |
| `hub.network` | retry | Transport failed after the CLI's in-process retries | Rerun; check connectivity and the Hub's status | 3 |
| `hub.rate_limited` | retry | Hub returned 429 and asked to slow down (its `Retry-After` is honored in process) | Rerun after a wait | 3 |
| `hub.unavailable` | retry | Hub returned 408/502/503/504 | Rerun later; if it persists, check the Hub | 3 |
| `hub.concurrent_clone` | retry | Another clone into this destination is already running | Wait for it, then rerun | 2 |
| `hub.interrupted` | retry | This run was cancelled by a signal | Rerun if the work is still wanted | 2 |
| `hub.package.cancelled` | retry | Package import was cancelled | Rerun | 2 |
| `hub.package.concurrent_mutation` | retry | A local writer mutated the project during verification | Rerun after local writers finish | 2 |
| `lifecycle.cancelled` | retry | The operation was cancelled at a deterministic checkpoint | Rerun if the work is still wanted | 1 |
| `inventory.generation_conflict` | retry | The selected generation moved under this run (stale generation or fingerprint) | Rerun; if it repeats, coordinate with other writers | 1 |
| `hub.publish.ref_conflict` | retry | The repository ref moved during publication | Rerun; it lands on the newer revision without re-sending held bytes | 2 |

### Fix the input or local state

| Code | Class | Meaning | Next action | Exit |
| --- | --- | --- | --- | --- |
| `hub.publish.auth_denied` | fix input | Publish credential missing, expired, or not scoped to this repository | With no token: set `GRAPHFORGE_HUB_PUBLISH_TOKEN` (see [Credentials](#credentials)). With a token still denied: a human must mint a `publish:owner/repository`-scoped token | 2 |
| `hub.invalid_identity` | fix input | Repository argument is not `owner/repository` or a valid HTTPS URL | Fix the repository (or `--hub`) argument | 2 |
| `hub.invalid_input` | fix input | An outgoing request or header is invalid | Correct the request inputs or credential value; keep secret values out of logs | 2 |
| `hub.unsafe_location` | fix input | URL must be credential-free HTTPS on a public host | Pass the documented Hub URL form | 2 |
| `hub.auth_denied` | fix input | Credential missing, unreadable, empty, or not valid for this origin (clone `--token-env`; publish saved-session origin) | Set or correct the credential environment variable (see [Credentials](#credentials)); if a provided token is still denied, a human must mint a scoped token | 2 |
| `hub.not_found` | fix input | The repository or resource does not exist (404/410) | Check the owner/repository spelling and ref | 2 |
| `hub.missing_ref` | fix input | The requested ref is absent | Check `--ref` against the repository's advertised refs | 2 |
| `hub.ambiguous_identity` | fix input | The canonical item/version has multiple eligible content digests (409) | Ask the publisher to resolve the ambiguity; `--digest` is only a verification pin | 2 |
| `hub.missing_object` | fix input | A referenced object is absent from the Hub | Rerun; if it persists, ask the repository publisher | 2 |
| `hub.duplicate` | fix input | A canonical identity, ref, or object occurs more than once | Rerun; if it persists, ask the Hub operator | 2 |
| `hub.limit_exceeded` | fix input | A response or request exceeded a configured bound | Reduce the request size; check `DiscoveryLimits` | 2 |
| `hub.package.limit_exceeded` | fix input | The package exceeded an import bound | Free space or adjust the documented limits | 2 |
| `hub.insufficient_space` | fix input | The destination filesystem lacks space for the clone | Free space or choose another destination | 3 |
| `hub.destination_conflict` | fix input | The destination path exists and is not a safe staging area | Choose an empty destination | 2 |
| `hub.package.repository_mismatch` | fix input | Package content belongs to a different repository | Clone the intended repository | 2 |
| `hub.package.immutable_version_mismatch` | fix input | A retained Version disagrees with its immutable identity | Ask the repository publisher; do not retry blindly | 2 |
| `hub.package.package_digest_mismatch` | fix input | Package bytes disagree with the declared digest | Rerun to re-download; if it repeats, ask the publisher | 2 |
| `hub.package.research_version_mismatch` | fix input | Research lineage disagrees with the package | Ask the repository publisher | 2 |
| `hub.package.producing_core_version_mismatch` | fix input | The manifest and package disagree about the Core version that produced the bytes | Ask the publisher to regenerate matching controls and package bytes; do not relabel the package. This differs from a warning that the running Core version is different | 2 |
| `hub.module.identity_mismatch` | fix input | An ontology module's identity disagrees with the manifest | Ask the repository publisher | 2 |
| `hub.module.content_digest_mismatch` | fix input | A module's bytes disagree with its digest | Rerun to re-download; if it repeats, ask the publisher | 2 |
| `hub.package.invalid_participant` | fix input | The package names an invalid participant | Ask the publisher to republish | 2 |
| `hub.package.invalid_structure` | fix input | The package structure is invalid | Rerun to re-download; if it repeats, ask the publisher | 2 |
| `hub.package.invalid_path` | fix input | A package path is not safe | Ask the publisher to republish | 2 |
| `hub.package.duplicate_entry` | fix input | The package contains a duplicate entry | Ask the publisher to republish | 2 |
| `hub.package.digest_mismatch` | fix input | An entry's bytes disagree with its digest | Rerun to re-download; if it repeats, ask the publisher | 2 |
| `hub.package.incompatible` | fix input | The package is incompatible with this client's selection | Check the ref/Version selection and client version | 2 |
| `hub.package.unsupported_format` | fix input | The package format major is not supported | Upgrade the GraphForge CLI | 2 |
| `hub.package.unsupported_future` | fix input | The package needs a capability this client lacks | Upgrade the GraphForge CLI | 2 |
| `hub.package.io` | fix input | Local I/O failed while importing the package | Check disk and permissions, then rerun | 3 |
| `hub.integrity` | fix input | A strong validator disagreed | Rerun to re-download; if it repeats, ask the Hub operator | 2 |
| `hub.integrity_failure` | fix input | Stored or transported bytes disagree with digests | Rerun to re-download; if it repeats, ask the Hub operator | 2 |
| `hub.publish.invalid_input` | fix input | The publish request failed structural validation (400, 404 unknown resource, 413 over a limit, 416 upload offset) | Fix the selection, image set, or sizes per the message | 2 |
| `hub.publish.id_taken` | fix input | A permanent item ID belongs to another publisher (409) | Publish as the owner when authorized. For a refused module or bridge, review `gf ontology regenerate-id ID --operation-uuid UUID`; it requires confirmation and the exact local refusal receipt | 2 |
| `hub.publish.non_fast_forward` | fix input | The remote head is not an ancestor of the proposed head | Pull/clone the remote head first, or take the explicit `--rewrite` lease deliberately | 2 |
| `hub.publish.integrity_failure` | fix input | Stored or uploaded bytes disagree with the declared digests | Rerun to rebuild and re-upload; if it repeats, ask the Hub operator | 2 |
| `hub.unsupported_future` | fix input | Discovery needs a protocol capability this client lacks | Upgrade the GraphForge CLI | 2 |
| `hub.publish.unsupported_future` | fix input | The publish protocol needs a capability this client lacks | Upgrade the GraphForge CLI | 2 |
| `interchange.unsupported_future` | fix input | Input uses a newer interchange major | Upgrade the GraphForge CLI | 1 |
| `interchange.unsupported_format` | fix input | Input format is not recognized | Check the input file type | 1 |
| `interchange.integrity` | fix input | Interchange digests, duplicates, or structure failed verification | Re-export or re-download the input | 2 |
| `interchange.selection` | fix input | The requested selection is incompatible with the input | Adjust the selection | 2 |
| `interchange.io` | fix input | Local I/O or path validation failed | Check the path and permissions | 1 |
| `inventory.not_found` | fix input | A named object is absent | Check the name/UUID argument | 1 |
| `dependency.in_use` | fix input | The module has bounded dependants | Remove or rebind the dependants first | 1 |
| `resolution.ambiguous` | fix input | A selector matched more than one subject | Narrow the selector | 1 |
| `resource.bytes` | fix input | A bounded resource limit was exceeded | Reduce the request or raise the documented limit | 1 |

### Ask a human

| Code | Class | Meaning | Next action | Exit |
| --- | --- | --- | --- | --- |
| `hub.unexpected_status` | ask a human | HTTP status without a Hub error body (for example a proxy): a protocol or Hub failure, never a transient one | Check for an outdated client or a proxy rewriting responses; ask the Hub operator | 2 |
| `hub.publish.entitlement_denied` | ask a human | Quota or entitlement refused publication | Ask the account/Hub owner | 2 |
| `hub.access_denied` | ask a human | The repository is private or the credential lacks access | Ask the repository owner for access or a scoped token | 2 |
| `hub.malformed_response` | ask a human | The Hub response violated its contract | Possibly an outdated client: upgrade first; otherwise ask the Hub operator | 2 |
| `hub.publish.internal` | ask a human | Hub internal failure (500) | Report to the Hub operator | 3 |
| `hub.publish.idempotency_conflict` | ask a human | The same operation identity was reused with different content (`GF_IDEMPOTENCY_CONFLICT` is the outer `error.code`; no `semantic_code` is emitted) | A human decides: replay (same identity) or intentional new operation (new UUID) | 1 |

### Local publication preparation (fix input)

These CLI validation codes describe publication preparation before upload;
they are not additional `HubPublishErrorCode` wire values.

| Code | Class | Meaning | Next action | Exit |
| --- | --- | --- | --- | --- |
| `hub.publish.derivation` | fix input | The selected Version could not be exported or its publication documents could not be derived | Inspect the diagnostic and repair the selected Version or required project inputs; if valid retained content still fails, report it to the maintainer | 2 |
| `hub.publish.ontology_composition_mismatch` | fix input | The workspace's ontology modules, bridges or activations differ from the selected frozen Version | Restore the workspace for that Version, or commit the ontology changes as a new Version and publish the current Branch head | 2 |

### Diagnostic codes (`error.diagnostics[].code`)

Multi-ontology failures carry per-diagnostic codes. Read them before the outer
code: the exit is a property of the envelope (`error.code` per the exit-code
table — usually `GF_ONTOLOGY_DIAGNOSTIC`, exit 1), while the diagnostic names
the actionable cause. Each diagnostic also carries a Rust-owned `remediation`
string mirroring the next action below. Spellings are the stable
`DiagnosticCode::as_str` tokens plus the portable staging codes.

| Code | Class | Meaning | Next action |
| --- | --- | --- | --- |
| `inventory.generation_conflict` | retry | A concurrent portable change moved the authority under this operation (the outer code may be `GF_IDEMPOTENCY_CONFLICT`) | Refresh the authority state and rerun; not an identity decision |
| `lifecycle.cancelled` | retry | The operation was cancelled at a deterministic checkpoint | Rerun if the work is still wanted |
| `bridge.contradiction` | fix input | Bridge assertions contradict each other | Remove the contradictory bridge assertions |
| `bridge.endpoint_missing` | fix input | A bridge endpoint module is absent from the inventory | Adopt every exact bridge endpoint module first |
| `bridge.provenance_missing` | fix input | The authoritative mapping lacks required provenance | Add bounded authored provenance |
| `collision.metadata` | fix input | An identifier is not NFC-normalized or its digest is malformed | Normalize the identifier and repair its metadata |
| `collision.qualified_duplicate` | fix input | The same qualified identity is declared twice with conflicting content | Remove the conflicting qualified symbol |
| `dependency.cycle` | fix input | The module dependency graph contains a cycle | Remove the dependency cycle |
| `dependency.in_use` | fix input | A module or bridge cannot be removed while dependants reference it | Remove dependent module, bridge, and activation references first |
| `dependency.missing` | fix input | A required dependency identity is not present | Adopt every exact dependency first |
| `interchange.integrity` | fix input | A declared digest disagrees with the canonical document | Repair or regenerate the authenticated content |
| `interchange.io` | fix input | Portable staging I/O or path validation failed | Verify the staging authority and path, then retry |
| `interchange.selection` | fix input | The request is incompatible with the candidate | Select a compatible portable ontology candidate |
| `interchange.unsupported_format` | fix input | The package format is not a supported portable project | Export with a supported producer |
| `interchange.unsupported_future` | fix input | The package needs a newer format or capability | Upgrade GraphForge |
| `inventory.duplicate` | fix input | A module identity is duplicated with conflicting content | Select or author one unique exact identity |
| `inventory.malformed` | fix input | The module document failed independent validation | Repair and validate the authored document |
| `inventory.not_found` | fix input | The requested module is absent from the inventory | Select an existing exact inventory identity |
| `lifecycle.invalid_transition` | fix input | The lifecycle transition is not allowed from the current status | Perform the required prior lifecycle step |
| `resolution.ambiguous` | fix input | An unqualified name has more than one candidate | Supply an exact qualified selector |
| `resolution.kind_mismatch` | fix input | The requested kind does not match the candidate | Request the symbol under its actual kind |
| `resolution.not_found` | fix input | No candidate exists for the requested symbol | Check the requested symbol's name |
| `resource.bridges`, `resource.bytes`, `resource.diagnostics`, `resource.modules`, `resource.symbols` | fix input | A caller limit was exceeded | Reduce the request below the registered limit |
| `storage.insufficient_space` | fix input | The target filesystem lacks space | Free space beside the target or import onto a larger volume |
| `storage.unsupported_filesystem` | fix input | The filesystem cannot provide the required semantics | Import onto a local `ext4`/`xfs`/`btrfs` volume (see `docs/development/agent-environment.md`) |

## Outer codes without a `semantic_code`

These failures set `error.code` and `details.kind` only. The class follows the
`kind` and the table:

| `error.code` | Class | Next action | Exit |
| --- | --- | --- | --- |
| `GF_VALIDATION` | fix input | Fix the input named in the message (argument refusals: fix the command line) | 2 |
| `GF_PARSE`, `GF_PLAN` | fix input | Fix the query; check `docs/reference/opencypher-compatibility.md` | 1 |
| `GF_NOT_IMPLEMENTED` | fix input | Not available in this release; check the documented status | 1 |
| `GF_NOT_FOUND`, `GF_PAGE_INVALID`, `GF_PAGE_SNAPSHOT_GONE`, `GF_UNKNOWN_ARGUMENT`, `GF_SCHEMA_MISMATCH` | fix input | Fix the arguments or refresh the snapshot and rerun | 1 |
| `GF_CHECKPOINT_EXISTS`, `GF_CHECKPOINT_NOT_FOUND`, `GF_READ_ONLY_VIEW`, `GF_CAPABILITY_DISABLED`, `GF_UNSUPPORTED_CAPABILITY_VERSION`, `GF_UNSUPPORTED_PROJECT_FORMAT`, `GF_PROJECT_UNINITIALIZED` | fix input | Fix the target name, enable the capability, or point `--project` at a GraphForge project | 1 |
| `GF_IDEMPOTENCY_CONFLICT` | ask a human | Decide replay versus intentional change (see the table above) | 1 |
| `GF_WRITER_BUSY` | retry | Another process holds the project writer lock; wait for it and rerun | 1 |
| `GF_REBASE_EXHAUSTED` | retry | Contention exceeded the bounded rebase attempts; rerun when the other writer finishes | 1 |
| `GF_IO` | retry | Transport or local storage failed; for `hub.network` rerun, otherwise check disk and permissions | 3 |
| `GF_UNSUPPORTED_FILESYSTEM` | fix input | Durable projects need an `ext4`/`xfs`/`btrfs` volume; provision one (see `docs/development/agent-environment.md`) | 3 |
| `GF_PROJECT_CORRUPT`, `GF_CHECKPOINT_REGISTRY_CORRUPT`, `GF_PUBLICATION_FAILED` | ask a human | Project state is unsafe; restore from a backup before any retry | 1 |
| `GF_RESOURCE_LIMIT` | fix input | Reduce the request size or raise the documented limit | 1 |
| `GF_CANCELLED` | retry | The run was cancelled; rerun if the work is still wanted | 1 |
| `GF_LIFECYCLE`, `GF_ONTOLOGY`, `GF_EXECUTION`, `GF_IDENTITY_CONFLICT`, `GF_FINGERPRINT_COLLISION`, `GF_RESULT_NOT_RETAINED` | ask a human | Inspect the message; these need human judgement or a bug report | 1 |
| `GF_AMBIGUOUS_PROJECTION` | fix input | Make the projection explicit | 1 |

`GF_TRANSACTION_CONFLICT` is a legacy spelling of `GF_IDEMPOTENCY_CONFLICT`.

## Credentials for agents and CI

`gf publish` resolves its credential in this order:

1. `GRAPHFORGE_HUB_PUBLISH_TOKEN` — an explicit publish bearer token. A CI job
   or agent sets it to a non-empty token scoped `publish:owner/repository` for
   the target repository. This is the supported non-interactive path; it never
   prompts and never consults the local vault.
2. A saved `gf auth login` session for the selected Hub (OS credential vault),
   exchanged for a repository-scoped publish token. Works without a terminal.
3. The RFC 8628 device flow, only when standard input and standard error are
   terminals.

With no credential available, `gf publish` fails `hub.publish.auth_denied`
(exit 2) and names the environment variable in the message. A captured
invocation (the Python and Node CLI shims) refuses before any request; an
interactive `gf` process attempts the anonymous discovery read first and may
report a transport code instead when the Hub is unreachable.

The token value is never accepted as a command-line argument, never logged,
never serialized into the Project or receipts, and is sent only as the
`Authorization: Bearer …` header to the resolved repository's own origin —
never to OAuth endpoints, object reads, or upload URLs. Saved-session grants
are additionally checked against the Hub origin (`hub.auth_denied` on
mismatch).

Acquisition: a human with Hub access mints a publish token scoped to
`publish:owner/repository` and stores it as a secret in the CI/agent
environment (for example `GRAPHFORGE_HUB_PUBLISH_TOKEN`). Agents never mint
tokens themselves.

`gf clone --token-env <VAR>` is the read-side equivalent: it reads a Hub
bearer token from the named environment variable (opt-in, noninteractive) for
private repositories. The variable must exist, be non-empty, and contain only
visible ASCII without whitespace; otherwise the command fails `hub.auth_denied`
(exit 2) before any network request. Clone credentials are bound to the
configured Hub origin and are never sent to redirects or object hosts.

## Contract stability

This page is tested by `crates/graphforge-cli/tests/error_contract.rs`, which
produces at least one real code per class through the CLI and compares the
`hub.publish.*` rows above against the code-owned `HubPublishErrorCode::ALL`
list. The full `hub.*` clone vocabulary is raised as dotted message prefixes
across the clone modules and is not enumerable as a single constant; its rows
are covered by the failure classifier it is derived from
(`classify_failure` in `crates/graphforge-cli/src/hub_clone.rs`).
