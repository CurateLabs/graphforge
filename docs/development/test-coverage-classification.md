# Test coverage classification

Use this method when consolidating GraphForge's test suites. Keep the scenario
and test inventory results on the tracking issue; this page records the method
so another contributor can reproduce the inventory.

## Inventory

Use the issue's base commit and the current candidate commit as the inventory
boundary. Exclude `tests/release_workflows/`, whose features are publication
gates. Enumerate physical Gherkin files with:

```bash
find tests -type f -name '*.feature' -not -path 'tests/release_workflows/*' -print | sort
```

For each file, record every `Scenario` and `Scenario Outline` by repository
path, feature title, scenario title, and source line. Keep examples attached to
their outline. Inventory Python binding tests from
`crates/graphforge-bindings-py/tests/` and Node binding tests from
`crates/graphforge-bindings-node/tests/`; record each test case's file and name,
not only its containing file.

## Classification

Assign one category to every inventory row:

| Category | Meaning | Disposition |
| --- | --- | --- |
| (a) Covered engine behavior | Cypher or engine behavior with an equivalent Rust test or TCK scenario | Remove the duplicate and identify its exact replacement by path and test/scenario name. |
| (b) Uncovered engine behavior | Cypher or engine behavior without equivalent Rust or TCK evidence | Port it to the openCypher TCK or a Rust test before removing the source case. |
| (c) Binding surface | Python or Node type marshalling, error mapping, lifecycle, or Arrow handoff | Keep one representative assertion in that binding's smoke suite. |

An equivalence requires the same input condition and observable outcome; a
similar test name or a shared fixture is not enough. A test can cover multiple
outcomes, so retain a replacement for each distinct assertion. Keep exclusions
and skipped cases visible in the inventory and do not count them as passing
coverage.

The canonical engine oracle is the Rust TCK runner at
`cargo test -p graphforge-api --test bdd`. New Cypher behavior belongs in a
TCK-style feature with an expected result table, or in a Rust golden test. Do
not reproduce engine semantics in Python or Node.

## Evidence

Record on the issue:

- the exact base and candidate commits and the sorted inventory counts;
- one row per scenario or binding test, including its category and replacement
  path/name (or the new Rust/TCK test that replaces it);
- the Rust coverage-ledger result before and after consolidation;
- Python and Node smoke-suite wall times measured with the same commands and
  environment before and after;
- `test.yml` job count and line count, plus the total workflow line count,
  measured at the same base and candidate commits.

Use `make coverage-rust` for the coverage ledger. It runs the full diagnostic
inventory, including `coverage_diagnostics.py` for broad Python adapter line
coverage; the PR gate remains limited to the bounded smoke commands in
`.github/workflows/test.yml`. Measure those smoke
commands with `/usr/bin/time` and report the exact command and elapsed time so
the results can be repeated. Do not put per-issue result tables or raw run
output in `docs/development/`.
