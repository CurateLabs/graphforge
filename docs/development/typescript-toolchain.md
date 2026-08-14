# TypeScript toolchain policy

Canonical GraphForge TypeScript toolchain for first-party packages that
directly declare or invoke the compiler.

## Compiler

**TypeScript 5.9.3** (exact) is the canonical compiler. First-party packages
that pin `typescript` directly:

- `tests/features/node`
- `iac/pulumi/typescript`

Do not bump the compiler major without a coordinated upgrade across all direct
pins. Verify JavaScript/JSDoc behavior, compiler-API consumers, framework
tooling, and peer ranges (for example `@pulumi/pulumi` peers `typescript < 7`)
before changing the pin.

## Runtime loader

**`tsx`** is the canonical TypeScript execution / runtime loader.
Feature BDD (Cucumber) uses `tsx/cjs`.

**`ts-node` is deprecated and prohibited** for new or first-party GraphForge
code — manifests, scripts, CI, and docs. Do not introduce it.

## Out of scope for direct pins

- **`docs-site`** does not directly pin or invoke `tsc`. Astro owns its
  embedded TypeScript integration.
- Transitive compiler or loader copies in lockfiles (including optional
  `ts-node` peer metadata from `@pulumi/pulumi`) are dependency implementation
  details — not the repository standard.
