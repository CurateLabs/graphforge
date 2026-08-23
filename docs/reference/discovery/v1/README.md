# Discovery v1 contract artifacts

These checked-in JSON Schemas describe the wire shape of GraphForge discovery
manifest and refs responses. `conformance.json` supplies valid and invalid
examples with stable Rust error results. Semantic rules that JSON Schema cannot
express—canonical ordering, cumulative bounds, safe URLs, capability
negotiation, and duplicate JSON members—remain authoritative in
`graphforge-discovery` and are exercised by the corpus.

Regenerate all three artifacts deterministically from the Rust test source:

```bash
GRAPHFORGE_UPDATE_DISCOVERY_ARTIFACTS=1 \
  cargo test -p graphforge-discovery --test contract_artifacts
```

Running the same test without that environment variable compares generated
bytes with the checked-in files and validates every corpus case through the
public Rust parser. Cargo and Bazel CI therefore fail when artifacts drift.

Downstream TypeScript may package these JSON files, use a JSON Schema validator
for early structural feedback, and run the conformance corpus against an HTTP
adapter. It must not transcribe the schema as hand-written TypeScript protocol
types or reimplement validation semantics. Rust remains the authority; generated
TypeScript declarations, if needed, must be treated as disposable build output
derived from these versioned artifacts.

See [portable-v2-integration.md](portable-v2-integration.md) for the normative
validation order and the boundary between discovery and package verification.
