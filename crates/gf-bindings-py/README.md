# GraphForge for Python

`graphforge` is the native Python binding and repository lifecycle CLI for
GraphForge's Rust-owned engine.

```bash
uvx graphforge init
uvx graphforge sync --check
uvx graphforge sync \
  --idempotency-key 41414141-4141-4141-4141-414141414141
uvx graphforge export --current \
  --output .graphforge/exports/project.gfportable
uvx graphforge import \
  --input .graphforge/imports/project.gfportable \
  --idempotency-key 47474747-4747-4747-4747-474747474747
uvx graphforge checkpoint create before-refactor \
  --idempotency-key 43434343-4343-4343-4343-434343434343
uvx graphforge checkpoint list
uvx graphforge checkpoint show before-refactor
uvx graphforge checkpoint diff \
  --from before-refactor --to-current --scope all --detail summary
uvx graphforge checkpoint delete before-refactor \
  --idempotency-key 44444444-4444-4444-4444-444444444444
uvx graphforge revert before-refactor --reason "restore before refactor" \
  --idempotency-key 45454545-4545-4545-4545-454545454545 --yes
uvx graphforge remove --yes
uvx graphforge skills install
uvx graphforge skills status
uvx graphforge skills update
uvx graphforge skills remove
uvx graphforge config validate
uvx graphforge config resolve --json
uvx graphforge infra validate --target production --json
```

The `graphforge` console entry point is a thin launcher over the same native
Rust CLI contract used by `gf` and `npx @graphforge/cli`. Use `--project-dir`
to select a repository explicitly, `--json` for machine-readable results,
`sync --check` for CI, `revert --preview` before restoring a checkpoint, and
`--yes` for non-interactive destructive operations.

Repository `export` and `import` operate on a complete portable GraphForge
project generation. They are not ontology-document commands. Rust owns the
#236 runtime-catalog inspection, ontology suggestion, non-mutating validation,
and YAML/JSON ontology-document export behavior. Python and Node expose that
contract, plus durable ontology adoption and clearing, as thin #237 bindings.
The repository CLI does not infer, adopt, clear, or export an ontology
implicitly.

Project-local GraphForge skills are packaged in the wheel and install directly;
Python does not invoke NPX or require Node.

See the [GraphForge documentation](https://curatelabs.github.io/graphforge/)
for the engine and repository-integration contracts.
