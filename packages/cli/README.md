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

Clone does not initialize or contact a telemetry exporter. The future opt-in
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
[repository integration guide](https://docs.graphforge.sh/guides/repository-integration/)
for the tracked `.graphforge/` definition boundary, ignored data surfaces,
Git behavior, and complete lifecycle contract.

Node.js 20 or newer is required.
