# Provider neutral decision examples

Both examples run offline against local fixture results and the real native
validator. Install the Python GraphForge package before the first command and
install the Node GraphForge and apache-arrow packages before the second.

```bash
python3 examples/decision-workflow/analyst.py
node examples/decision-workflow/agent.mjs
```

They use an in-memory project, so they run without a durable project
filesystem. For retained evidence, use a separate project rooted on ext4, xfs,
or btrfs and follow the Artifact/Version retention flow in the decision
workflow guide. Do not interpret the deterministic fixtures as model-quality
or calibration evidence.
