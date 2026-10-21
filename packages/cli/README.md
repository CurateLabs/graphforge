# `@curatelabs/graphforge-cli`

Run the GraphForge repository lifecycle CLI without a global installation:

```bash
npx @curatelabs/graphforge-cli init
npx @curatelabs/graphforge-cli sync --check
npx @curatelabs/graphforge-cli sync \
  --idempotency-key 41414141-4141-4141-4141-414141414141
npx @curatelabs/graphforge-cli export --current \
  --output .graphforge/exports/project.gfpb
npx @curatelabs/graphforge-cli import \
  --input .graphforge/imports/project.gfpb \
  --idempotency-key 47474747-4747-4747-4747-474747474747
npx @curatelabs/graphforge-cli checkpoint create before-refactor \
  --idempotency-key 43434343-4343-4343-4343-434343434343
npx @curatelabs/graphforge-cli checkpoint list
npx @curatelabs/graphforge-cli checkpoint show before-refactor
npx @curatelabs/graphforge-cli checkpoint diff \
  --from before-refactor --to-current --scope all --detail summary
npx @curatelabs/graphforge-cli checkpoint delete before-refactor \
  --idempotency-key 44444444-4444-4444-4444-444444444444
npx @curatelabs/graphforge-cli revert before-refactor --reason "restore before refactor" \
  --idempotency-key 45454545-4545-4545-4545-454545454545 --yes
npx @curatelabs/graphforge-cli remove --yes
npx @curatelabs/graphforge-cli skills install
npx @curatelabs/graphforge-cli skills status
npx @curatelabs/graphforge-cli skills update
npx @curatelabs/graphforge-cli skills remove
npx @curatelabs/graphforge-cli config validate
npx @curatelabs/graphforge-cli config resolve --json
npx @curatelabs/graphforge-cli infra validate --target production --json
npx @curatelabs/graphforge-cli clone openalex/openalex
npx @curatelabs/graphforge-cli clone https://graphforge.sh/openalex/openalex openalex-copy
npx @curatelabs/graphforge-cli hub list curate/claims
npx @curatelabs/graphforge-cli clone curate/claims --ref feature/score claims-feature
npx @curatelabs/graphforge-cli ontology module fetch openalex/openalex \
  --ontology-id https://openalex.org/ontology/works --version 2026.01 \
  --digest f1f475dfcf01bdfe7a578afb0203bac26831dc0b48f4cd009bdc418b506981b4 \
  --output works-ontology.json
```

The package is a thin launcher for the Rust-owned CLI exposed by
`@curatelabs/graphforge`. It does not parse commands or implement GraphForge behavior
in JavaScript. Command names, flags, JSON output, errors, and exit codes are the
same as the native `gf` executable.

`clone` treats `owner/repository` as the canonical
`https://graphforge.sh/owner/repository` identity. The optional second argument
is a new destination directory and defaults to the repository name. Clone reads
only the versioned `/.gf/refs` and `/.gf/manifest` control documents from that
identity; package bytes come only from the content-addressed HTTPS location in
the validated manifest. Existing destinations are never overwritten.

For another Hub, use `--hub https://preview.example` with `owner/repository`,
or pass an explicit HTTPS repository URL (which cannot be combined with `--hub`).
Sign in once for private repositories:

```bash
gf auth login
gf auth status
gf clone my-org/private-repo
gf auth logout
```

`gf auth login` opens the WorkOS approval page and prints its verification link
and code. Use `--no-browser` to approve on another device, or
`--hub https://preview.example` on each auth command for another Hub. Protected
previews also accept `--protection-bypass-env PREVIEW_BYPASS_SECRET` on auth
commands; that secret is read only for same-origin Hub requests and never saved. Access and
refresh tokens are stored together only in macOS Keychain, Windows Credential
Manager, or Linux Secret Service. An unavailable or locked vault fails clearly;
there is no plaintext fallback. Status verifies the session with the Hub. Logout
revokes its provider session and deletes local credentials, including when
remote revocation fails. Expired access tokens refresh and rotate the saved pair.
`--json auth status` reports authentication and principal identity without tokens.

Public clones stay anonymous and never consult the credential vault. A private
repository may conceal its existence with `404`. After `401` or `404`, clone uses
the saved session to request a repository-scoped read token from the selected Hub.
Only that token reaches the data-plane origin the Hub authorizes. Authenticated
downloads reject redirects and retain range/resume and digest verification.
The WorkOS session never reaches manifest-provided object locations.

For headless use, select an environment variable containing a Hub bearer token
with `--token-env HUB_READ_TOKEN`. For a Vercel protected preview, independently
select the bypass secret with `--protection-bypass-env PREVIEW_BYPASS_SECRET`:

```bash
# Supply these variables through your shell or CI secret store.
gf clone openalex/openalex --hub https://preview.example \
  --token-env HUB_READ_TOKEN --protection-bypass-env PREVIEW_BYPASS_SECRET
```

The token becomes `Authorization: Bearer ...`; the preview secret becomes
`x-vercel-protection-bypass`. Initial discovery receives these headers only at
the configured Hub's exact origin. Neither header follows redirects. When private
discovery or objects reject access, clone can exchange the Hub token at the same
Hub for a separate repository read credential. The original environment value
never reaches object origins; the preview bypass never reaches objects.
Missing, empty, non-ASCII, or whitespace-containing secrets fail before discovery
with `hub.auth_denied`. Clone does not prompt or store secrets in resume state,
output, project files, or telemetry. Public clones without these flags read no
credential variables.

`gf publish` uses `GRAPHFORGE_HUB_PUBLISH_TOKEN` when supplied; otherwise it
exchanges the saved session for the selected repository's publish capability.
The existing interactive device-flow fallback remains available on compatible
publish servers. For a protected preview, pass
`gf publish https://preview.example/OWNER/REPOSITORY --ref main --protection-bypass-env PREVIEW_BYPASS_SECRET`.
The bypass is validated with the same rules as clone credentials and sent only
to initial control requests at that Hub origin, including saved-session refresh
and repository-token exchange. Redirects, off-origin OAuth endpoints, object
reads, and capability-upload `HEAD`/`PUT` requests receive no bypass. Uploads
never receive identity or publish credentials, even when hosted at the Hub origin.

Without `--ref` or `--version-uuid`, clone follows the repository's
`default_ref`. When the repository advertises a research lineage with Branches,
the default ref must name one of them: clone gets that Branch's head Version
through the research path, exactly as `--ref <default ref>` would, and a default
ref that names none fails with `hub.missing_ref` rather than downloading another
package. A repository without lineage, or whose lineage has no Branches, gets the
Project package. `--ref NAME` clones another Branch's head and
`--version-uuid UUID` one exact published Version. A research clone's JSON
result adds `resolved_ref` (the Branch ref it followed, absent for
`--version-uuid`), `research_branch_uuid`, `research_version_uuid`,
`research_version_kind`, and `research_version_identity` (the Version identity
digest the lineage advertises).

Downloads use finite response, object (1 TiB), redirect, and connection
limits, and an idle bound instead of a whole-transfer deadline: a slow transfer
continues as long as bytes keep arriving. Transient failures (a failed
connection, a cut body, or a `408`, `429`, `502`, `503`, or `504` status) are
retried in the same invocation with backoff; a `Retry-After` (seconds or an HTTP date) sets a
longer wait, up to 30 seconds, and a longer one ends the retries with the wait in
the message. A download cut short by a retryable status keeps that status's
code and class (with a rerun resuming it). Other statuses fail at once. A retry uses a strong ETag with
`Range`/`If-Range`, appends only an exact matching `206` response, and reads a
full `200` response as the whole object again. Before downloading, clone checks
that the destination filesystem is admissible (`GF_UNSUPPORTED_FILESYSTEM`) and
has room for the package plus the import (the package, plus a staged and a
published copy of its payload, plus a validation copy and a rebuilt-Version copy
of its research content when it has any; `hub.insufficient_space`). Download, import target, and resume state live in an
owner-private, symlink-safe staging directory beside the destination, protected
by an exclusive process lock. The destination is installed by one atomic
no-replace rename only after transport size and SHA-256 checks, full
portable verification, semantic package identity comparison, import, and
reopen through the GraphForge facade. After a kill, a failed import, or Ctrl-C,
rerunning the same command resumes or finishes the clone; import failures keep
their `hub.package.*` code. Byte progress goes to standard error. The
[portable projects guide](https://github.com/CurateLabs/graphforge/blob/main/docs/guide/portable-projects.md#large-projects)
covers disk needs, resume, limits, and errors for large projects.
Redirects and DNS answers are rechecked against the public-network-only policy;
HTTP, credential-bearing URLs, private/link-local/loopback addresses, corrupt
objects, and ambiguous package references fail closed. JSON mode returns the
`graphforge-hub-clone/1` result contract and stable `hub.*` semantic errors
(see [Hub error classes](#hub-error-classes)).

`hub list OWNER/REPOSITORY [--hub URL]` (alias `hub ls`) shows a published
repository without cloning it: its refs and default ref, and, when it advertises
a research lineage, its Branches (ref, head, base, and parent Branch), Versions
(kind, the source Version of a projection, and whether a package is published),
the Fork origin, and Proposals. It reads only `/.gf/refs`, `/.gf/manifest`, and
the lineage document, through the same reads and verifier as `clone`: a lineage
whose bytes disagree with their digest, that is bound to another snapshot, or
that requires an unsupported capability fails the listing with the same code it
fails `clone`. `--hub` replaces the default `https://graphforge.sh` base for an
`OWNER/REPOSITORY` name.

```text
$ gf hub list curate/claims-fork
curate/claims-fork (default ref main)
Refs: feature, main
Forked from curate/claims Version 01a10003-3e9b-7c71-9d6e-0c0f2e5a2b10
Branches:
  main  head 01a10003-4664-77df-8712-03523e788b5e  base 01a10003-45d2-7b4e-a3f1-6a5d0e3c9f21
  feature  head 01a10003-47af-723a-8696-f43d163d7494  base 01a10003-4701-7e88-b1c2-2f4a9d8e6c33  parent main
Versions:
  01a10003-4664-77df-8712-03523e788b5e  complete  main  package
  01a10003-47af-723a-8696-f43d163d7494  complete  feature  package
```

JSON mode returns the `graphforge-hub-listing/1` contract:

```json
{
  "contract": "graphforge-hub-listing/1",
  "repository": "curate/claims-fork",
  "default_ref": "main",
  "immutable_version": "sha256:...",
  "package_digest": "sha256:...",
  "refs": [{ "name": "main", "target": "sha256:..." }],
  "lineage": {
    "project_uuid": "...",
    "fork": {
      "repository": "curate/claims",
      "project_uuid": "...",
      "version_uuid": "...",
      "version_identity": "sha256:..."
    },
    "branches": [
      {
        "ref": "main",
        "branch_uuid": "...",
        "label": "main",
        "head_version_uuid": "...",
        "base_version_uuid": "...",
        "parent_branch_uuid": null
      }
    ],
    "versions": [
      {
        "version_uuid": "...",
        "kind": "complete",
        "branch_uuid": "...",
        "identity_digest": "sha256:...",
        "source_version_uuid": null,
        "package_digest": "sha256:..."
      }
    ],
    "proposals": [
      {
        "proposal_uuid": "...",
        "source_branch_uuid": "...",
        "source_version_uuid": "...",
        "payload_version_uuid": "...",
        "package_digest": "sha256:..."
      }
    ]
  }
}
```

A repository that moved reports `moved_from` and `moved_to`, as `clone` does.
`lineage` is `null` for a repository without research lineage; `fork`,
`parent_branch_uuid`, `source_version_uuid`, and a Version's `package_digest` are
`null` when absent. `gf publish` does not publish Proposals, so `proposals` is
empty unless another publisher added them.

`ontology module fetch OWNER/REPOSITORY --ontology-id ID --version VERSION
--digest HEX --output FILE [--hub URL]` retrieves one exact ontology module from
a Hub repository without cloning it: it makes exactly three requests (`/.gf/refs`,
`/.gf/manifest`, and the module's own package object) and never downloads the
Project package or any graph data. It needs no project or repository, and is
outside the four-surface multi-ontology contract, like `clone`. Pass the three
parts of the module identity as separate flags, because ids and versions may
contain `@` and `#`. `--digest` is the module's canonical content digest as 64
lowercase hexadecimal digits; a `sha256:` prefix is also accepted. It is neither
the package digest nor the SHA-256 of the document file. `--hub` replaces the
default `https://graphforge.sh` base for an `OWNER/REPOSITORY` name.

The fetch applies the same transport rules as `clone`: HTTPS only, no
credentials, public-network addresses only (rechecked on redirects and DNS),
and finite response, redirect, connection, and operation limits. The module
package is also bounded to 64 MiB. The downloaded package must match its
discovery object digest and length, pass full portable verification, carry the
advertised `package_digest`, and contain the requested module, whose canonical
content digest is recomputed from the document. Only then is the module document
written, byte for byte, to `--output`: through a private staging file next to it
and an atomic no-replace link, so an existing `--output` (including a dangling
symlink) is never overwritten. The link requires an `--output` filesystem that
supports hard links; on one that does not (for example FAT or some network
mounts) the fetch fails with `hub.destination_conflict` rather than risk
replacing a file. A failed fetch removes its staging files and publishes no
output. A run that is interrupted or killed can leave a private staging
directory next to `--output`; the next fetch to the same `--output` reuses it
safely, because every byte is re-verified against its digest. JSON mode returns
the `graphforge-hub-module-fetch/1` receipt:

```json
{
  "contract": "graphforge-hub-module-fetch/1",
  "repository": "openalex/openalex",
  "module": {
    "ontology_id": "https://openalex.org/ontology/works",
    "authored_version": "2026.01",
    "canonical_digest": "f1f475df...81b4"
  },
  "package_digest": "sha256:8528501d...77b0",
  "module_sha256": "<SHA-256 hex of the written file>",
  "output": "works-ontology.json"
}
```

`canonical_digest` and `module_sha256` are lowercase hexadecimal without a
prefix; `package_digest` identifies the carrying package, differs between
Projects that publish the same module, and keeps its `sha256:` prefix. Failures
carry a stable `hub.*` code: `hub.invalid_identity` (malformed repository, Hub,
or `--digest`), `hub.unsafe_location`, `hub.network`, the HTTP status codes
(`hub.not_found`, `hub.access_denied`, `hub.rate_limited`, `hub.unavailable`,
`hub.unexpected_status`), `hub.missing_ref`,
`hub.missing_object` (the repository does not advertise that exact module with a
package), `hub.limit_exceeded` (object larger than the bound), `hub.interrupted`
(short body), `hub.integrity` (bytes disagree with their digest),
`hub.package.*` and `hub.module.*` (the package or module failed verification),
and `hub.destination_conflict` (existing `--output`, missing output directory, or
an output filesystem without hard links). Also possible, with the same meanings
as for `clone`: `hub.malformed_response`, `hub.unsupported_future`,
`hub.duplicate` and `hub.integrity_failure` (invalid or future discovery
documents), and `hub.concurrent_clone` (another fetch is already using the
staging directory for the same `--output`).

### Hub error classes

`clone`, `hub list`, and `ontology module fetch` fail with a stable `hub.*`
semantic code (`details.semantic_code` in JSON mode). Each code belongs to one
class: **retry** (rerun the same command, later), **fix input** (change the
command or the local state), or **ask a human** (the repository or Hub must
change). Messages never repeat the Hub's response body. Exit status 3
(`GF_IO`) marks a failure a plain rerun may cure; exit status 2
(`GF_VALIDATION`) one it will not.

| Code                                                               | Class       | Exit                      | Meaning                                                                                   |
| ------------------------------------------------------------------ | ----------- | ------------------------- | ----------------------------------------------------------------------------------------- |
| `hub.network`                                                      | retry       | 3                         | The Hub could not be reached or the connection failed                                     |
| `hub.unavailable`                                                  | retry       | 3                         | `408`, `502`, `503`, or `504` after every in-process retry                                |
| `hub.rate_limited`                                                 | retry       | 3                         | `429`; the message gives the `Retry-After` wait when the Hub sent one                     |
| `hub.interrupted`                                                  | retry       | 2                         | The download stopped or was cancelled; a rerun resumes it                                 |
| `hub.integrity`                                                    | retry       | 2                         | Downloaded bytes disagree with their digest (removed), or a range response does not match |
| `hub.concurrent_clone`                                             | retry       | 2                         | Another run is using the same staging directory; wait for it                              |
| `hub.package.io`, `hub.package.cancelled`                          | retry       | 3 (2 during verification) | The import or verification hit an I/O failure or was cancelled                            |
| `hub.auth_denied`                                                  | fix input   | 2                         | A `--token-env` or `--protection-bypass-env` secret is missing, empty, or invalid         |
| `hub.not_found`                                                    | fix input   | 2                         | `404` or `410`: no such repository or object; check the name and `--hub`                  |
| `hub.invalid_identity`                                             | fix input   | 2                         | Malformed repository, Hub URL, `--digest`, or both `--ref` and `--version-uuid`           |
| `hub.unsafe_location`                                              | fix input   | 2                         | A URL is not credential-free public HTTPS (a Hub redirect there is refused too)           |
| `hub.missing_ref`                                                  | fix input   | 2                         | The `--ref` Branch is not published (or the Hub's refs omit a ref its documents name)     |
| `hub.missing_object`                                               | fix input   | 2                         | The `--version-uuid` Version, module, or package is not published                         |
| `hub.destination_conflict`                                         | fix input   | 2                         | The destination or `--output` exists, or its directory cannot hold the result             |
| `hub.insufficient_space`                                           | fix input   | 3                         | Not enough free space beside the destination                                              |
| `hub.access_denied`                                                | ask a human | 2                         | `401` or `403`: ask the repository owner for access                                       |
| `hub.unexpected_status`                                            | ask a human | 2                         | Any other unsuccessful status, named in the message                                       |
| `hub.malformed_response`, `hub.duplicate`, `hub.integrity_failure` | ask a human | 2                         | The Hub serves invalid, duplicated, or mutually inconsistent discovery documents          |
| `hub.unsupported_future`                                           | ask a human | 2                         | The repository requires a newer protocol or capability; upgrade `gf` or ask the publisher |
| `hub.limit_exceeded`                                               | ask a human | 2                         | An object exceeds its size bound                                                          |
| `hub.package.*`, `hub.module.*` (others)                           | ask a human | 2                         | The package or module failed verification                                                 |

`GF_UNSUPPORTED_FILESYSTEM` (exit 1) is a fix-input failure: clone onto a
supported local volume.

### Research history commands

Git-style commands act on the Project's research history; `gf research ...` stays
the namespace of the native JSON verbs they wrap ([ADR 0061](https://github.com/CurateLabs/graphforge/blob/main/docs/adr/0061-research-porcelain-and-ref-naming.md)).
`--ref` always names a Branch by its label, which is also its ref on the Hub.

| Command                                              | What it does                                                                                              |
| ---------------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| `gf status [--branch REF]`                           | Current Branch, head Version, upstream, `ahead N` of the last fetched head, tags on the head, warnings    |
| `gf log [VERSION] [-n N] [--all] [--remote [REPO]]`  | Newest-first history of the Branch, from a tag or Version, or the Hub's published lineage                  |
| `gf commit -m MESSAGE (--query CYPHER \| --file F)`  | Run the query against the Branch and record a Version; the message is its description                    |
| `gf branch [NAME] [--from VERSION]`                  | List Branches, or create one (the first Branch starts from the Project's current research)                 |
| `gf switch REF`                                      | Select the Branch the other commands act on                                                               |
| `gf tag [NAME [VERSION]] [-d NAME]`                  | List, create (default: the head) or delete tags; a tag never moves, delete and create again               |
| `gf push [REPO] [--ref REF] [--rewrite] [--prune]`   | `gf publish` of the Branch; the repository and ref default from its upstream                              |
| `gf pull [REPO] [--ref REF]`                         | Fetch the Hub's head of the ref and record a two-parent Version adopting every compatible change           |
| `gf clone REPO --tag NAME`                           | Clone the Version a published tag names                                                                   |
| `gf config user [--name N] [--email E] [--orcid O]`  | Set or show the credited author and committer (`$XDG_CONFIG_HOME/graphforge/config.toml`)                 |

A command that records a Version credits an author and committer from
`--author`/`--committer` (`Name <email>`), then `GF_AUTHOR`/`GF_COMMITTER`, then
`gf config user`; the committer defaults to the author. The Branch a command acts on
is `--branch`, then the one `gf switch` selected, then the Project's only Branch.
Commands that record a Version take `--operation-uuid` (a UUIDv7); rerunning a
command with it returns the original result. `gf push` and `gf pull` take the same
`--hub`, `--token-env` and `--protection-bypass-env` as `gf clone`.

| Code                                                                        | Class     | Meaning                                                                                   |
| --------------------------------------------------------------------------- | --------- | ----------------------------------------------------------------------------------------- |
| `GF_RESEARCH_BRANCH_LABEL_INVALID`, `GF_RESEARCH_BRANCH_LABEL_TAKEN`        | fix input | A Branch label must be a valid ref and unused in the Project                              |
| `GF_RESEARCH_BRANCH_UNSELECTED`                                             | fix input | Several Branches and none selected; run `gf switch REF`                                   |
| `GF_RESEARCH_SIGNATURE_MISSING`                                             | fix input | No author configured; pass `--author`, set `GF_AUTHOR` or run `gf config user`            |
| `GF_RESEARCH_TAG_EXISTS`                                                    | fix input | The tag names another Version; delete it first                                            |
| `GF_RESEARCH_TAG_CONFLICT`                                                  | fix input | The Hub serves a tag on another Version than the local tag; delete the local tag, pull again |
| `GF_HUB_TAG_CONFLICT`                                                       | fix input | The Hub already serves the tag on another Version; publish with `--rewrite` to move it   |
| `GF_RESEARCH_UPSTREAM_UNSET`                                                | fix input | The Branch has no upstream; name the repository                                           |

Clone does not initialize or contact a telemetry exporter. Neither does module fetch. The future opt-in
GraphForge OpenTelemetry lifecycle will remain Rust-owned and must not attach
repository names, URLs, local paths, credentials, manifests, or graph data to
clone signals.

Use `--project-dir` to select a repository explicitly and `--json` for
machine-readable results. Mutating commands accept caller-owned operation and
actor identities where required. CI can use `sync --check`; checkpoint restore
supports `revert --preview`; destructive commands require an explicit
confirmation such as `--yes` when no interactive terminal is available.

Repository `export` and `import` operate on a complete portable GraphForge
project generation. They are not ontology-document commands. Rust-owned
runtime-catalog inspection, ontology suggestion, non-mutating validation, and
YAML/JSON ontology-document export are the #236 API surface; #237 exposes that
same surface, plus durable ontology adoption and clearing, through thin Python
and Node bindings. This CLI preserves those APIs and does not infer, adopt,
clear, or export an ontology implicitly.

See the
[repository integration guide](https://docs.graphforge.sh/guide/repository-integration/)
for the tracked `.graphforge/` definition boundary, ignored data surfaces,
Git behavior, and complete lifecycle contract.

Node.js 20 or newer is required.
