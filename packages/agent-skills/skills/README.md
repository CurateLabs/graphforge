# Agent workflows

`bootstrap` creates or reopens a local project and verifies a real
create/reopen/query cycle. `build-knowledge` appends caller-specified graph and
M20 records, with optional explicit M21 reasoning and first status. Both use the
shared adapter and injected shipped Node/Arrow surfaces; neither contains a
runtime, fallback backend, inference, search, traversal, or algorithm behavior.

The checked manifest for each workflow is stored beside this file. Import the
implementations from `@graphforge/agent-skills/workflows`.
