# openCypher conformance

GraphForge tests its Rust query engine against the vendored openCypher 2024.3
Technology Compatibility Kit (TCK). A scenario checks a query's expected result
or error. This describes compatibility with that corpus, rather than every
extension offered by other graph databases.

## Corpus and regression baseline

The corpus contains **3,897 upstream scenarios and one GraphForge regression
scenario**. The committed [passing baseline](https://github.com/CurateLabs/graphforge/blob/main/tests/tck/passing_baseline.txt)
lists all **3,898 identities**. The [corpus metadata](https://github.com/CurateLabs/graphforge/blob/main/tests/tck/coverage_matrix.json)
records those counts; it is not a per-feature support matrix.

A full run must preserve every baseline case. A failure, skipped case, or
missing identity causes the baseline check to fail. An unrelated improvement
cannot cancel a regression. Baseline membership records the required passing
set; consult the test output for results from a particular source commit.

Newly passing cases produce `TCK XPASS` warnings for review before inclusion
in the baseline. Newly added cases outside the baseline are exploratory
coverage: their failure alone does not fail the baseline check. This is the
limited meaning of the runner's “advisory” label. Existing baseline scenarios
are required, and the runner does not use `@skip-rust` to exclude them.

The [Rust BDD runner](https://github.com/CurateLabs/graphforge/blob/main/crates/graphforge-api/tests/bdd/main.rs)
implements this set comparison. The [PR workflow](https://github.com/CurateLabs/graphforge/blob/main/.github/workflows/test.yml)
runs it in **Rust Harness, Doc, and Feature Tests** when the code-change filter
selects that lane; its result feeds `CI Gate`. Prose-only changes do not run the
whole corpus.

## Run the checks

From a source checkout with the [development prerequisites](../development/agent-environment.md):

```bash
make test-tck
```

For a focused investigation, select feature filenames by substring:

```bash
TCK_ONLY=Temporal cargo test -p graphforge-api --test bdd
```

A focused run bypasses the whole-corpus baseline check because a subset cannot
satisfy it. It is useful for debugging, but cannot establish full conformance.
The normal CI run does not set `TCK_ONLY`.

When a reviewed change adds passing scenarios, a maintainer can regenerate the
baseline and inspect the diff:

```bash
BLESS_TCK_BASELINE=1 cargo test -p graphforge-api --test bdd
```

Do not remove failing cases to make a run pass. Baseline edits need ordinary
review, and the final full run must preserve the intended scenario set.

## Correctness and performance are separate

Passing the TCK establishes results for those scenarios. It does not certify
maximum graph size, workload latency, or production maturity.

The BDD runner's per-scenario timing output is diagnostic. Performance
comparisons use the separate Divan/BenchExec procedure with matched hardware,
build, workload, and configuration. See [benchmarking](../development/benchmarking.md)
for the reproducible method and [scale guidance](scale-limits.md) for the
claim-to-evidence table. Run results belong to their issue or CI run rather
than becoming a current performance promise on this page.

For supported query syntax and examples, see the [Cypher guide](../guide/cypher-guide.md).
Analyst verbs such as `rank` and `cluster` are separate APIs; their availability
is not evidence that a Cypher scenario passes.
