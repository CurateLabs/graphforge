# openCypher TCK — vendored corpus

Vendored snapshot of the openCypher Technology Compatibility Kit (TCK) Gherkin feature files.
`tests/features/tck/features` symlinks here (`../../tck/features`).

- **Source:** <https://github.com/opencypher/openCypher> (`tck/features/`)
- **Pinned revision:** tag **`2024.3`** (commit `677cbaf`)
- **License:** Apache License 2.0 — see [`LICENSE`](./LICENSE) and [`NOTICE`](./NOTICE) in this
  directory; every `.feature` file also retains the upstream Apache-2.0 + attribution header verbatim.

## Local modification

Every feature is tagged **`@skip-rust @skip-node`** at the feature level so the Rust
(`crates/gf-api/tests/bdd/`) and Node (`tests/features/node/`) BDD suites treat all TCK scenarios as
skipped until conformance tiers land (M17: #597–#601, #608). Tags are removed tier-by-tier as
scenarios start passing. Files are otherwise upstream, except **line endings** normalized to LF
(9 upstream files shipped as CRLF).

> **Gherkin-parser note (#886).** The Rust `gherkin` 0.14 parser rejects a scenario whose *first*
> step uses the `And`/`But` continuation keyword (e.g. `Match5.feature`'s scenario-leading
> `And having executed:`, which continues the Background's `Given`); cucumber-js is lenient. Rather
> than edit the vendored files, the **BDD runner normalizes block-leading `And`/`But` → `Given` at
> load time into an ephemeral copy** (`crates/gf-api/tests/bdd/main.rs`), so these files stay
> byte-for-byte upstream and re-vendoring is a clean copy.

## Re-vendoring

Bump the pinned tag, re-extract `tck/features/` from the upstream tarball into this directory, then
re-apply the `@skip-rust @skip-node` feature tags:

```bash
gh api repos/opencypher/openCypher/tarball/<tag> | tar xz -C /tmp/oc
cp -R /tmp/oc/opencypher-openCypher-*/tck/features/. tests/tck/features/
find tests/tck/features -name '*.feature' -print0 | while IFS= read -r -d '' f; do
  awk 'BEGIN{d=0} /^Feature:/&&!d{print "@skip-rust @skip-node"; d=1} {print}' "$f" > "$f.tmp" && mv "$f.tmp" "$f"
done
```

No gherkin-parser fix-ups are needed at vendor time — the BDD runner normalizes block-leading
`And`/`But` steps at load time (see the note above). Verify with `cargo test -p gf-api --test bdd`
(green = the whole corpus parses and the un-skipped tiers pass).
