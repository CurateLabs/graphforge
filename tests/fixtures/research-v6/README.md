# Research revision 6 fixture

A real Project and research interchange package written by the last research
revision 6 producer (graphforge 0.5.2 at commit `b2b703732`). Tests copy them
to a private directory and prove that this build reads them, upgrades the
Project on its first research write, and imports the package (ADR 0055).

- `project/`: a durable Project with a canonical research decision, a Project
  capture, and a Branch `main` created from current research and edited once.
- `package.gfpb`: `export_research` of that Branch head (a bundle).
- `imported/`: `package.gfpb` imported by the same code, a revision 6 Project
  holding only imported research provenance.
- `ids.json`: the identities the tests assert, including every Version identity
  digest the revision 6 registry committed.

Empty directories are not checked in; the Project recreates them. Lease and lock
files are part of the Project and are kept.

Git does not preserve the producer's read-only CAS file permissions. Test
helpers restore those permissions on copied `graph-objects/sha256/` payloads
before opening the Project; mutable control files stay writable. No fixture
bytes or identity digests change.

## Regenerate

The generator runs against the revision 6 code, never against this tree:

```bash
git worktree add --detach /path/to/v6 b2b703732
cp tests/fixtures/research-v6/generate_research_v6_fixture.rs \
  /path/to/v6/crates/graphforge-api/examples/
cd /path/to/v6
CARGO_TARGET_DIR=/path/to/isolated-target \
  cargo run -p graphforge-api --example generate_research_v6_fixture -- /path/to/new-output
```

`TMPDIR` and the output must be on an admitted filesystem (`ext4`, `xfs` or
`btrfs`). Copy `project/` and `imported/` (including their lease and
lock files), `package.gfpb` and `ids.json` over this directory, and remove the
temporary worktree. Identities are fresh UUIDs on every run, so a regenerated
fixture differs byte-for-byte; the tests read `ids.json` rather than pinning
values.
