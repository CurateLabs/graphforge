"""Single Bazel-visible source of the GraphForge release version (#1395).

`WORKSPACE_VERSION` mirrors the Cargo workspace version (`[workspace.package]
version` in `//Cargo.toml`), spelled exactly as Cargo spells it (including any
`-dev` or `-rc.N` suffix per ADR 0033/ADR 0034). Every Bazel target whose
compiled crate exposes `env!("CARGO_PKG_VERSION")` at runtime -- the Python
extension module, the Node native module, and the CLI -- must set its
`version` attribute from this constant instead of a hand-typed literal, so
there is exactly one Bazel-side place to keep in sync with Cargo.

`scripts/set_release_version.py` rewrites this constant on every version bump,
in lockstep with Cargo.toml/Cargo.lock/pyproject.toml/package.json. Do not
hand-edit it outside that tool; `python3 scripts/set_release_version.py
--check` and `scripts/ci/test-crate-publish-plan.py` both fail closed if it
drifts from the workspace version.

Why a checked-in constant rather than reading Cargo.toml directly: Bazel's
loading-phase Starlark macros (the ones `tools/bazel/gf_rust.bzl` BUILD files
call) have no primitive to read an arbitrary workspace file's contents.  Only
a repository_rule or module_extension can do that (the mechanism
`crate.from_cargo` in `//MODULE.bazel` already uses, via the vendored
`cargo-bazel` tool, to parse `Cargo.lock`). Standing up a new module_extension
solely to parse one string out of `Cargo.toml` would touch `//MODULE.bazel`
-- a shared root config file outside this fix's owned surface
(`BUILD.bazel` files, `tools/bazel/`, `scripts/set_release_version.py`, and
the policy test) -- for a benefit this single-file, tool-rewritten constant
already delivers: one write on every bump instead of three.
"""

WORKSPACE_VERSION = "0.5.2"
