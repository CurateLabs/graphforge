# `@curatelabs/graphforge-cli`

Run the GraphForge repository lifecycle CLI without a global installation:

```bash
npx @curatelabs/graphforge-cli init
npx @curatelabs/graphforge-cli sync --check
npx @curatelabs/graphforge-cli sync \
  --idempotency-key 41414141-4141-4141-4141-414141414141
npx @curatelabs/graphforge-cli export --current \
  --output .graphforge/exports/project.gfportable
npx @curatelabs/graphforge-cli import \
  --input .graphforge/imports/project.gfportable \
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

Downloads use finite response, object, redirect, connection, and operation
limits. An interrupted download remains in an owner-private, symlink-safe
staging directory protected by an exclusive process lock. A retry uses a
strong ETag with `Range`/`If-Range`, resumes only an exact matching `206`
response, and otherwise restarts safely. A complete destination is published only after transport
size and SHA-256 checks, full portable-v2 verification, semantic package
identity comparison, atomic import, and reopen through the GraphForge facade.
Redirects and DNS answers are rechecked against the public-network-only policy;
HTTP, credential-bearing URLs, private/link-local/loopback addresses, corrupt
objects, and ambiguous package references fail closed. JSON mode returns the
`graphforge-hub-clone/1` result contract and stable `hub.*` semantic errors.

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
discovery object digest and length, pass full portable-v2 verification, carry the
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
or `--digest`), `hub.unsafe_location`, `hub.network`, `hub.missing_ref`,
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
