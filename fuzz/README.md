# GraphForge fuzz harness

Coverage-guided fuzzing of the compiler front end and execution engine, via
[`cargo-fuzz`](https://github.com/rust-fuzz/cargo-fuzz) / libFuzzer. This is a
**standalone crate** (its own `[workspace]` table) so it stays out of the
pinned-stable root workspace — the targets need nightly and a sanitizer
runtime.

## Targets

| target | surface | invariant |
|---|---|---|
| `fuzz_parse` | `gf_cypher::parse` | arbitrary text never panics |
| `fuzz_bind` | `parse` → `gf_ir::Binder::bind` (exploratory) | parseable queries never panic when bound |
| `fuzz_ontology` | `gf_ontology::OntologyLoader::{load_yaml,load_json}` | arbitrary bytes never panic |
| `fuzz_exec` | `gf_api::GraphForge::execute` (parse→bind→lower→execute) | parseable queries never panic when executed |

## Running

Requires a nightly toolchain and `cargo-fuzz`:

```bash
rustup toolchain install nightly
cargo +nightly install cargo-fuzz
```

Seed inputs are committed under `seeds/`; the writable corpus
(`corpus/<target>/`) is gitignored and grown by libFuzzer. Run a target,
seeding from `seeds/`:

```bash
# parser / binder / exec share the Cypher-query seed set
cargo +nightly fuzz run fuzz_parse    corpus/fuzz_parse    seeds/queries
cargo +nightly fuzz run fuzz_bind     corpus/fuzz_bind     seeds/queries
cargo +nightly fuzz run fuzz_exec     corpus/fuzz_exec     seeds/queries
cargo +nightly fuzz run fuzz_ontology corpus/fuzz_ontology seeds/ontology
```

CI runs each target time-boxed (`-- -max_total_time=60`) as a smoke test — a
crash, OOM, or timeout fails the job. To reproduce a CI crash, run the target
against the reported artifact:

```bash
cargo +nightly fuzz run fuzz_parse artifacts/fuzz_parse/crash-<hash>
```
