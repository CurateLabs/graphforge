# Two-story native journey

`corpus.json` is shared input for real Rust, Python, Node, and same-build CLI
journey tests. `output/` contains one actual native-facade run. Its manifest
records format/reader qualification, generated identity roles, file sizes, and
SHA-256 digests. Arrow streams retain native schema metadata and values.

The two JSON assertion summaries are identified in the manifest; they are not
new response schemas. UUIDs and temporary discovery paths remain exactly as
observed and differ between runs. The scan/OCR content is synthetic supplied
input, not evidence of OCR execution. No human study or application deployment
is represented.

See [the research journey guide](../../../docs/guide/research-journey.md) for
stage questions and result fields, and [the acceptance evidence matrix](../../../docs/engineering/TESTING.md#analyst-ux-acceptance)
for owner regressions covering the richer domain, failure, and retention cases.

Regenerate into a new directory on an admitted durable filesystem:

```sh
GRAPHFORGE_JOURNEY_CAPTURE_DIR=/path/to/new-output \
CARGO_TARGET_DIR=/path/to/isolated-target \
  cargo test -p graphforge-api --test research_journey -- --nocapture
```

Replace a checked-in capture only after the test completes successfully and
reviewing its semantic observations and manifest digests. Byte-for-byte equality
across runs is not expected for generated identities or timing/path metadata.
