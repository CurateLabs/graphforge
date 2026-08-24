# Rust-owned Hub fixture

This directory is the single GraphForge authority for the public Hub closeout
fixture. `openalex-source/` is the complete, synthetic, expanded portable-v2
source state. `generated/v1/` contains the deterministic bundle and canonical
discovery manifest and refs emitted by Rust.

Check drift:

```bash
cargo run -p graphforge-cli --example generate_hub_fixture
```

Regenerate after an intentional source or contract change:

```bash
cargo run -p graphforge-cli --example generate_hub_fixture -- --update
```

The generator fully verifies the source, imports and reopens it through the
public GraphForge facade with a fixed operation identity, exports and fully
verifies the canonical bundle, then constructs and validates manifest and refs
through `graphforge-discovery`. Its metadata binds the checked-in source tree,
the compiled generator source, and the package, transport, manifest,
and byte-length identities. It does not accept a caller-supplied Git commit.
The invoking checkout commit is the review and release provenance evidence.

Downstream TypeScript may copy or import the four files in `generated/v1/` as
opaque versioned artifacts. It must not reconstruct protocol fields, validators,
digests, or compatibility rules.
