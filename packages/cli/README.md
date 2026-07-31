# `@graphforge/cli`

Run the GraphForge repository lifecycle CLI without a global installation:

```bash
npx @graphforge/cli init
npx @graphforge/cli sync
npx @graphforge/cli config validate
```

The package is a thin launcher for the Rust-owned CLI exposed by
`@graphforge/node`. It does not parse commands or implement GraphForge behavior
in JavaScript. Command names, flags, JSON output, errors, and exit codes are the
same as the native `gf` executable.

Node.js 20 or newer is required.
