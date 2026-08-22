# Multi-ontology contract fixtures

`canonical.json` is the normative semantic oracle for ADR 0022. The validator
reverses its inventory and proves the composition fingerprint is invariant,
then checks exact qualified resolution and activation ownership.

`adversarial.json` enumerates required typed failure dispositions. Every case
must preserve the source generation and composition fingerprint. These are
contract fixtures for implementation issues #836-#843, not a second runtime.

`binding-parity-v1.json` is the shared Rust/Python/Node/CLI lifecycle oracle for
#842. `$base` and `$dependent` are symbolic references replaced with the exact
content-derived identities returned by Rust before constructing the bridge.
Its ten cases are the required cross-surface conformance matrix.

`certification-v1/` is the reproducible six-domain #843 project. It contains
authored ontology documents and deterministic expectations, never graph source
data, generated runtime identifiers, filesystem paths, or host values. The
genealogy v1 to v2 route deliberately renames both a retained entity label and
a retained property so certification cannot pass through a version-only or
empty-data shortcut.

Validate deterministically from the repository root:

```bash
python3 scripts/ci/multi-ontology-contract-check.py
```
