# openCypher TCK — vendored corpus

Vendored snapshot of the openCypher Technology Compatibility Kit (TCK) Gherkin feature files.
The canonical upstream corpus is `features/`; GraphForge-specific Cypher golden
features live under `features/graphforge/` and run only in the Rust TCK harness.

- **Source:** <https://github.com/opencypher/openCypher> (`tck/features/`)
- **Pinned revision:** tag **`2024.3`** (commit `677cbaf`)
- **License:** Apache License 2.0 — see [`LICENSE`](./LICENSE) and [`NOTICE`](./NOTICE) in this
  directory; every `.feature` file also retains the upstream Apache-2.0 + attribution header verbatim.

## Local modification

Every upstream feature retains the snapshot's **`@skip-rust @skip-node`** tags.
The Rust runner deliberately runs the full corpus and gates the passing set;
Node does not load the TCK corpus. Upstream files are otherwise unchanged,
except **line endings** normalized to LF (9 upstream files shipped as CRLF).

GraphForge-specific goldens use the same TCK-style Gherkin and expected result
tables, but live under `features/graphforge/` so they remain separate from the
upstream snapshot. They are included by the Rust TCK runner and its passing
scenario baseline; the Python and Node suites do not execute them.

> **Gherkin-parser note (#886).** The Rust `gherkin` 0.14 parser rejects a scenario whose *first*
> step uses the `And`/`But` continuation keyword (e.g. `Match5.feature`'s scenario-leading
> `And having executed:`, which continues the Background's `Given`); cucumber-js is lenient. Rather
> than edit the vendored files, the **BDD runner normalizes block-leading `And`/`But` → `Given` at
> load time into an ephemeral copy** (`crates/graphforge-api/tests/bdd/main.rs`), so these files stay
> byte-for-byte upstream and re-vendoring is a clean copy.

## Re-vendoring

Bump the pinned tag, re-extract upstream `tck/features/` into `features/`, then
re-apply the `@skip-rust @skip-node` feature tags. Do not replace or overwrite
the GraphForge-specific `features/graphforge/` directory:

```bash
gh api repos/opencypher/openCypher/tarball/<tag> | tar xz -C /tmp/oc
cp -R /tmp/oc/opencypher-openCypher-*/tck/features/. tests/tck/features/
find tests/tck/features -path 'tests/tck/features/graphforge' -prune -o -name '*.feature' -print0 | while IFS= read -r -d '' f; do
  awk 'BEGIN{d=0} /^Feature:/&&!d{print "@skip-rust @skip-node"; d=1} {print}' "$f" > "$f.tmp" && mv "$f.tmp" "$f"
done
```

No gherkin-parser fix-ups are needed at vendor time — the BDD runner normalizes block-leading
`And`/`But` steps at load time (see the note above). Verify with `cargo test -p graphforge-api --test bdd`
(green = the whole corpus parses and the un-skipped tiers pass).
