# `@graphforge/cli`

Run the GraphForge repository lifecycle CLI without a global installation:

```bash
npx @graphforge/cli init
npx @graphforge/cli sync --check
npx @graphforge/cli sync \
  --idempotency-key 41414141-4141-4141-4141-414141414141
npx @graphforge/cli export --current \
  --output .graphforge/exports/project.gfportable
npx @graphforge/cli import \
  --input .graphforge/imports/project.gfportable \
  --idempotency-key 47474747-4747-4747-4747-474747474747
npx @graphforge/cli checkpoint create before-refactor \
  --idempotency-key 43434343-4343-4343-4343-434343434343
npx @graphforge/cli checkpoint list
npx @graphforge/cli checkpoint show before-refactor
npx @graphforge/cli checkpoint diff \
  --from before-refactor --to-current --scope all --detail summary
npx @graphforge/cli checkpoint delete before-refactor \
  --idempotency-key 44444444-4444-4444-4444-444444444444
npx @graphforge/cli revert before-refactor --reason "restore before refactor" \
  --idempotency-key 45454545-4545-4545-4545-454545454545 --yes
npx @graphforge/cli remove --yes
npx @graphforge/cli skills install
npx @graphforge/cli skills status
npx @graphforge/cli skills update
npx @graphforge/cli skills remove
npx @graphforge/cli config validate
npx @graphforge/cli config resolve --json
npx @graphforge/cli infra validate --target production --json
```

The package is a thin launcher for the Rust-owned CLI exposed by
`@graphforge/node`. It does not parse commands or implement GraphForge behavior
in JavaScript. Command names, flags, JSON output, errors, and exit codes are the
same as the native `gf` executable.

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
