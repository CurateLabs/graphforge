---
name: graphforge-build-knowledge
description: Reconcile declared GraphForge definitions and external sources while preserving the Git and data boundary.
---

# Build knowledge with GraphForge

Use this skill when a user wants to synchronize declared definitions, validate
an infrastructure target, or move a portable GraphForge project envelope.

## Procedure

1. Read `.graphforge/graphforge.yaml` and validate it with
   `graphforge config validate`.
2. Resolve configuration with `graphforge config resolve --json`; treat secret
   references as references and never print or copy secret values.
3. Run `graphforge sync` only for definitions and digest-identified external
   sources explicitly declared by the configuration. Never scan the repository
   for data.
4. Before provisioning, run `graphforge infra validate --target <name>`.
   Static validity is not service readiness.
5. Use `graphforge export` and `graphforge import` only for versioned portable
   whole-project envelopes. Do not treat them as ontology-document export
   commands. Rust-owned runtime-catalog inspection, deterministic ontology
   suggestion, non-mutating ontology validation, and explicit YAML/JSON
   ontology-document export belong to #236; thin Python and Node parity,
   including durable adopt/clear behavior, belongs to #237.

## Safety

Actual graph or source data must remain outside the code repository. Never
stage `.graphforge/state/`, `.graphforge/imports/`, or
`.graphforge/exports/`. Preserve user-edited or unrelated project skills. Use a
checkpoint before destructive lifecycle work, and use `graphforge revert` when
the user requests restoration; revert publishes a new complete generation.
Never infer, adopt, clear, or change ontology authority as a side effect of
repository sync, project import/export, configuration validation, or IaC.
