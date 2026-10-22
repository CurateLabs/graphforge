// Acceptance demo: the openalex Hub fixture validated through the WebAssembly
// module, with a publish request commitment equal to the native Rust value
// (`crates/graphforge-protocol-wasm/tests/corpus.rs` recomputes the same
// commitment with `PublishIntent`, the type `gf publish` uses), plus the
// error-shape, no-trap and footprint properties of the module.
import assert from "node:assert/strict";
import { test } from "node:test";
import { performance } from "node:perf_hooks";
import { protocol, readJson, readText, attempt, dist, wasm } from "./support.mjs";
import { statSync } from "node:fs";
import { join } from "node:path";

const fixtureDir = ["tests", "fixtures", "hub", "generated", "v1"];
const fixture = readJson(...fixtureDir, "fixture.json");
const manifestText = readText(...fixtureDir, "manifest.json");
const refsText = readText(...fixtureDir, "refs.json");
const summaryText = readText(...fixtureDir, fixture.summary_path);
const recorded = readJson("crates", "graphforge-protocol-wasm", "tests", "openalex-commitment.json");

test("the openalex manifest, refs and summary validate and bind", () => {
  const manifest = protocol.validate_manifest(manifestText);
  assert.equal(manifest.digest, fixture.manifest_digest);
  assert.equal(manifest.document.repository.owner, "openalex");

  const refs = protocol.validate_refs(refsText);
  assert.equal(refs.document.refs[0].validator, manifest.digest);

  const summary = protocol.validate_summary(summaryText);
  assert.equal(summary.digest, fixture.summary_digest);

  protocol.bind_summary(manifestText, summaryText);
  protocol.bind_refs(refsText, manifestText);
  protocol.bind_summary_lineage(summaryText, null);

  const [module] = fixture.module_objects;
  const selected = protocol.ontology_module_object(manifestText, {
    id: module.ontology_id,
    version: module.version,
    content_digest: module.content_digest,
  });
  assert.equal(selected.object.digest, module.object_digest);
  assert.deepEqual(protocol.manifest_preview_descriptors(manifestText), []);
});

test("the openalex publish commitment equals the native Rust commitment", () => {
  const manifest = protocol.validate_manifest(manifestText);
  const objects = manifest.document.objects
    .map(({ digest, length, media_type }) => ({ digest, length, media_type }))
    .sort((a, b) => (a.digest < b.digest ? -1 : 1));
  const commitment = protocol.request_commitment({
    repository: manifest.document.repository,
    intent_digest: fixture.package_digest,
    objects,
    manifest_validator: manifest.digest,
    refs: ["main"],
    expected_revision: null,
  });
  assert.equal(commitment, recorded.request_commitment);
});

test("a lineage document validates to its canonical form and digest", () => {
  const corpus = readJson("docs", "reference", "discovery", "v1", "conformance.json");
  const lineageCase = corpus.cases.find((c) => c.document === "lineage" && c.expected.result === "valid");
  const lineage = protocol.validate_lineage(lineageCase.json);
  assert.equal(lineage.canonical_json, lineageCase.expected.canonical_json);
  assert.match(lineage.digest, /^sha256:[0-9a-f]{64}$/);
});

test("failures are structured plain objects with the Rust code", () => {
  const malformed = attempt(() => protocol.validate_manifest("{"));
  assert.ok(!malformed.ok);
  assert.equal(malformed.error.constructor, Object);
  assert.equal(malformed.error.code, "malformed_response");
  assert.equal(typeof malformed.error.message, "string");

  const duplicate = attempt(() => protocol.validate_refs('{"format":"x","format":"y"}'));
  // A repeated object member is a malformed response, as in native Rust.
  assert.equal(duplicate.error.code, "malformed_response");

  const future = attempt(() => protocol.validate_session_request({ format: "graphforge-hub-publish/2" }));
  assert.equal(future.error.code, "unsupported_future");

  const limit = attempt(() => protocol.validate_manifest(manifestText, { max_response_bytes: 1 }));
  assert.equal(limit.error.code, "limit_exceeded");
  assert.equal(limit.error.path, "response");

  const badLimits = attempt(() => protocol.validate_manifest(manifestText, { unknown: 1 }));
  assert.equal(badLimits.error.code, "invalid_argument");
});

test("hostile arguments throw structured errors and never trap the module", () => {
  const exports = Object.entries(protocol).filter(
    ([name, value]) => typeof value === "function" && (name.startsWith("validate_") || name.startsWith("bind_")),
  );
  const nested = JSON.parse("[".repeat(100) + "]".repeat(100));
  const hostile = [undefined, null, 0, 1.5, true, "", "null", "[]", "{}", "\u0000", "x".repeat(1 << 20), [], {}, nested, () => 1];
  for (const [name, call] of exports) {
    for (const value of hostile) {
      const outcome = attempt(() => call(value, value, value));
      if (!outcome.ok) {
        assert.ok(!(outcome.error instanceof WebAssembly.RuntimeError), `${name} trapped`);
        // A wrong JavaScript type for a typed parameter is rejected by the glue.
        if (outcome.error instanceof TypeError) continue;
        assert.equal(typeof outcome.error.code, "string", `${name}: ${String(outcome.error)}`);
      }
    }
  }
  // The module is still usable after the abuse.
  assert.equal(protocol.validate_manifest(manifestText).digest, fixture.manifest_digest);
});

test("footprint: size, memory and per-call cost", (t) => {
  const wasmBytes = statSync(join(dist, "graphforge_protocol_wasm_bg.wasm")).size;
  const iterations = 2000;
  global.gc?.();
  const before = process.memoryUsage();
  const started = performance.now();
  for (let i = 0; i < iterations; i += 1) {
    protocol.validate_manifest(manifestText);
    protocol.validate_summary(summaryText);
  }
  const perPair = (performance.now() - started) / iterations;
  global.gc?.();
  const after = process.memoryUsage();
  t.diagnostic(
    JSON.stringify({
      wasm_bytes: wasmBytes,
      linear_memory_bytes: wasm.memory.buffer.byteLength,
      manifest_plus_summary_ms: Number(perPair.toFixed(3)),
      rss_growth_bytes: after.rss - before.rss,
      external_growth_bytes: after.external - before.external,
    }),
  );
  assert.ok(perPair < 25, `validate_manifest + validate_summary took ${perPair} ms`);
});
