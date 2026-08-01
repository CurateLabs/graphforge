# Agent-skills executable examples

These examples import the shared RC scenario module under `../rc/scenarios.js`,
the same source used by `tests/rc-e2e.test.mjs` and
`scripts/run-native-rc-e2e.mjs`.

```bash
# Deterministic mock GraphForge (no native binding)
pnpm --filter @curatelabs/graphforge-agent-skills example:analyst
pnpm --filter @curatelabs/graphforge-agent-skills example:developer

# Native GraphForge from a local `@curatelabs/graphforge` build
GRAPHFORGE_NODE_MODULE=$PWD/crates/graphforge-bindings-node/index.js \
  node packages/agent-skills/examples/analyst-agent.mjs --native
```

Mock outputs match the checked-in goldens under `tests/goldens/`.
