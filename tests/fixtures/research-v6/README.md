# Research revision 6 fixture

A real Project and research interchange package written by the last research
revision 6 producer (graphforge 0.5.2 at commit `b2b703732`). Tests copy them
to a private directory. The plain Project tests prove revision 6 record reading
and upgrade on the first research write (ADR 0055). Interchange has a separate
exact producer-version check: the checked-in `0.5.2` bundle is rejected by the
current development version, and reading research history from `imported/`
reports the same incompatibility. Opening that Project alone does not validate
its archived research history. The rejection tests preserve the historical
fixture bytes and assert no publication.

Positive revision 6 interchange coverage uses a clearly synthetic temporary
package. A native expanded export of `project/` supplies the current producer
and unchanged legacy Version records. The test labels only the archive and
runtime research metadata as revision 6, then recalculates the package's file,
semantic, and BagIt hashes. Full verification, native import, close/reopen, and
re-export prove admission at revision 7 while preserving the imported archive's
revision 6 label and original Version identities. This is a layout-compatibility
test, not evidence that another pre-v1 producer version is supported.

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
