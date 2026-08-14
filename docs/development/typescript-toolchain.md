# TypeScript toolchain policy

Canonical GraphForge TypeScript toolchain for first-party packages that
directly declare or invoke the compiler, and for workspace-resolved
`typescript` installs enforced via package-manager overrides.

## Compiler

**TypeScript 5.9.3** (exact) is the repository standard.

First-party packages that pin `typescript` directly:

- `tests/features/node`
- `iac/pulumi/typescript`

### Workspace / lockfile enforcement

The root pnpm workspace forces every resolved `typescript` dependency —
including transitive consumers such as `@napi-rs/cli` and
`@astrojs/starlight` / `i18next` — to **5.9.3** via `pnpm.overrides` in the
root `package.json`. After `pnpm install`, the root lockfile should contain
only `typescript@5.9.3` (no 6.x / 7.x copies).

`iac/pulumi/typescript` is outside the pnpm workspace and already pins
exact `5.9.3`. It also declares an npm `overrides` entry so transitive
resolutions stay on that pin if a future dependency tries to pull another
compiler major.

Prefer an override over documenting an exception. Record an exception only
when a dependency truly cannot run against TypeScript 5.9.3 after an
override attempt, with evidence in the coordinating issue.

Do not bump the compiler major (or the exact pin) without a coordinated
upgrade across all direct pins and overrides. Verify JavaScript/JSDoc
behavior, compiler-API consumers, framework tooling, and peer ranges (for
example `@pulumi/pulumi` peers `typescript < 7`) before changing the pin.

## Runtime loader

**`tsx`** is the canonical TypeScript execution / runtime loader.
Feature BDD (Cucumber) uses `tsx/cjs`.

**`ts-node` is deprecated and prohibited** for new or first-party GraphForge
code — manifests, scripts, CI, and docs. Do not introduce it.

## Out of scope for direct pins

- **`docs-site`** does not directly pin or invoke `tsc`. Astro owns its
  embedded TypeScript integration. Its transitive `typescript` peer still
  resolves to **5.9.3** through the root `pnpm.overrides`.
- Generated lockfile peer metadata that mentions optional loaders such as
  `ts-node` (for example from `@pulumi/pulumi`) is not first-party usage and
  is not the repository standard.
