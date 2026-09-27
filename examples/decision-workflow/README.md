# Provider neutral decision examples

Both examples run offline against local fixture results and the real native
validator. Install the Python GraphForge package before the first command and
install the Node GraphForge and apache-arrow packages before the second.

```bash
python3 examples/decision-workflow/analyst.py
node examples/decision-workflow/agent.mjs
python3 examples/decision-workflow/evaluate_fixtures.py
```

They use an in-memory project, so they run without a durable project
filesystem. For retained evidence, use a separate project rooted on ext4, xfs,
or btrfs and follow the Artifact/Version retention flow in the decision
workflow guide. Do not interpret the deterministic fixtures as model-quality
or calibration evidence.

The analyst example calls a caller-owned Python function that returns route,
rubric, and review-probability records. The agent example writes a separate
Arrow result artifact, reads it back, and joins its result identities to the
selected questions and objects before validation. Either producer can be
replaced in application code without changing Core.

Observed output from the fixtures:

- Analyst: Mystery → `research`, rubric `high`; Voyage → `human_review`
  with `uncertain` status, rubric `medium`; review probability `0.25`; the
  explicit caller policy routes Mystery to research.
- Agent: Summarize evidence → `continue`; Resolve source conflict → `clarify`
  with `uncertain` status; Review missing source citation → `review`; review
  probability `0.25`; the caller applies the allowed continue action.
- Evaluation fixture: rule-baseline errors `0`, producer answer errors `1`,
  one missing result, one uncertain result, three review decisions, and
  fixture-only Brier score `0.1825`.

The evaluation script compares a tiny labeled fixture with explicit rules and
separates answer errors, missing outputs, uncertainty/review, and probability
scoring. Use held-out task cases to calibrate probabilities and choose a review
threshold; this four-row fixture is only an executable method example.
