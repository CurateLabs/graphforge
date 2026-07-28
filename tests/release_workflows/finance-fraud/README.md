# Finance and fraud-analysis release workflow

This synthetic workflow investigates transfer patterns without asserting that
any party committed fraud. It preserves mistaken entity, transaction, and scope
generations, then proves the corrected strict project and non-binary epistemic
state after reopen.

Run from the repository root after committing the scenario:

```bash
python3 tests/release_workflows/finance-fraud/run.py \
  --evidence-dir target/release-workflow-evidence
```

The command invokes an opt-in Rust example and a clean-installed same-commit
native Python wheel. It is deliberately excluded from ordinary workspace tests,
required PR checks, and the aggregate CI gate.
