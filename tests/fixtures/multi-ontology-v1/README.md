# Multi-ontology contract fixtures

`canonical.json` is the normative semantic oracle for ADR 0022. The validator
reverses its inventory and proves the composition fingerprint is invariant,
then checks exact qualified resolution and activation ownership.

`adversarial.json` enumerates required typed failure dispositions. Every case
must preserve the source generation and composition fingerprint. These are
contract fixtures for implementation issues #836-#843, not a second runtime.

Validate deterministically from the repository root:

```bash
python3 scripts/ci/multi-ontology-contract-check.py
```
