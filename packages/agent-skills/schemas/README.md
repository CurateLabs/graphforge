# Agent skill schemas

GraphForge agent skills use three JSON Schema 2020-12 contracts. Each contract
is closed (`additionalProperties: false`) and carries the exact integer
`schema_version` it accepts:

- `skill-manifest-v1.json` describes a skill and its required GraphForge
  capabilities.
- `input-envelope-v1.json` carries one skill invocation.
- `output-envelope-v1.json` carries either a successful result or a structured
  error.

Import `validateSkillManifest`, `validateSkillInput`, or `validateSkillOutput`
from `@graphforge/agent-skills/schemas`. Validation is local and deterministic;
it does not open a GraphForge project, execute a skill, or access the network.
Each function returns `{ valid, diagnostics }`. Diagnostics contain only a
stable code, schema path, and fixed message. They never echo rejected values,
are sorted by path and code, and are capped at eight entries.

Envelope payloads are recursively bounded to depth 16, 4,096 visited entries,
and 4,096 characters per string. Cycles are rejected. The JSON schemas bound
nested strings, arrays, and objects; the dependency-free validator additionally
enforces the aggregate entry and depth budgets with fixed diagnostics.

Version 1 identifiers are lowercase kebab-case. Request IDs are lowercase
hyphenated UUIDs. Unknown fields, missing fields, malformed values, and any
schema version other than `1` fail closed.
