// The JavaScript boundary: Rust never reads a caller's object, object and text
// input reach the validators identically, and results are exact plain data.
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { test } from "node:test";
import { everyTrapThrows } from "./hostile.mjs";
import { attempt, dist, protocol, readJson } from "./support.mjs";

const discovery = readJson(
  "docs",
  "reference",
  "discovery",
  "v1",
  "conformance.json",
);
const manifestCase = discovery.cases.find(
  ({ name }) => name === "manifest-minor-compatible-and-canonical",
);
const VALIDATORS = {
  manifest: protocol.validate_manifest,
  refs: protocol.validate_refs,
  summary: protocol.validate_summary,
  lineage: protocol.validate_lineage,
};

/** What a validator says, as comparable data. */
const verdict = (validate, input, limits) => {
  const outcome = attempt(() => validate(input, limits));
  return outcome.ok
    ? { ok: true, canonical: outcome.value.canonical_json }
    : { ok: false, code: outcome.error.code, path: outcome.error.path };
};

test("a throwing proxy yields invalid_argument, 2,000 times, then a valid call succeeds", () => {
  const proxy = everyTrapThrows();
  for (let i = 0; i < 2000; i += 1) {
    const outcome = attempt(() => protocol.validate_manifest(proxy));
    assert.ok(!outcome.ok);
    assert.equal(outcome.error.code, "invalid_argument");
  }
  assert.equal(protocol.validate_manifest(manifestCase.json).digest.length, 71);
});

test("an object and its JSON text get the identical verdict, corpus-wide", () => {
  let compared = 0;
  for (const { document, json, limits } of discovery.cases) {
    const validate = VALIDATORS[document];
    const object = JSON.parse(json);
    const text = JSON.stringify(object);
    assert.deepEqual(
      verdict(validate, object, limits),
      verdict(validate, text, limits),
      document,
    );
    // The original text is the same document unless the parse normalized it.
    if (text === json) {
      assert.deepEqual(
        verdict(validate, json, limits),
        verdict(validate, object, limits),
      );
    }
    compared += 1;
  }
  assert.equal(compared, 100);
});

test("object and text input agree on surrogates, -0, exponents, duplicate and __proto__ keys", () => {
  const base = JSON.parse(manifestCase.json);
  const cases = {
    "lone surrogate in a value": { "x-v": "\ud800" },
    "lone surrogate in a key": { "x-\udc00": 1 },
    "paired surrogates": { "x-v": "😀" },
    "negative zero": { "x-v": -0 },
    exponent: { "x-v": 1e2, "x-w": 1.5e-7, "x-x": 0.1 },
    "proto key": JSON.parse('{"x-a": {"__proto__": {"a": 1}}, "x-b": 2}'),
    "nested proto key": JSON.parse('{"x-a": [{"__proto__": [1]}]}'),
  };
  for (const [name, extensions] of Object.entries(cases)) {
    const object = { ...base, extensions };
    const text = JSON.stringify(object);
    const fromObject = verdict(protocol.validate_manifest, object);
    assert.deepEqual(
      fromObject,
      verdict(protocol.validate_manifest, text),
      name,
    );
    if (name.startsWith("lone")) {
      // serde_json refuses a lone surrogate escape; so must both forms.
      assert.equal(fromObject.code, "malformed_response", name);
    }
  }
  // Duplicate members exist only in text; the text form is refused.
  const duplicate = manifestCase.json.replace(
    '"format"',
    '"default_ref":"x","format"',
  );
  assert.equal(
    verdict(protocol.validate_manifest, duplicate).code,
    "malformed_response",
  );
  // A raw string holding a lone surrogate is refused, never reread as U+FFFD.
  const raw = manifestCase.json.replace("x-example", "x-\ud800");
  assert.equal(
    attempt(() => protocol.validate_manifest(raw)).error.code,
    "invalid_argument",
  );
});

test("results keep __proto__ and every other key as own properties", () => {
  const text = manifestCase.json.replace(
    '"extensions":{',
    '"extensions":{"x-a":{"__proto__":{"polluted":1},"constructor":"c","nested":[{"__proto__":2}]},',
  );
  const { document, canonical_json: canonical } =
    protocol.validate_manifest(text);
  const value = document.extensions["x-a"];
  assert.equal(Object.getPrototypeOf(value), Object.prototype);
  assert.ok(Object.hasOwn(value, "__proto__"));
  // Integers inside free-form extension values are 64-bit too, so `bigint`.
  assert.deepEqual(Object.getOwnPropertyDescriptor(value, "__proto__").value, {
    polluted: 1n,
  });
  assert.ok(Object.hasOwn(value, "constructor"));
  assert.ok(Object.hasOwn(value.nested[0], "__proto__"));
  assert.equal(value.polluted, undefined);
  assert.equal({}.polluted, undefined);
  assert.ok(canonical.includes('"__proto__":{"polluted":1}'));
  // The result is plain data: a Map never leaks out.
  assert.ok(!(document.extensions instanceof Map) && !(value instanceof Map));
  assert.deepEqual(
    Object.keys(document).sort(),
    Object.keys(JSON.parse(canonical)).sort(),
  );
});

test("null fields are null, and the declarations say so", () => {
  const summaryCase = discovery.cases.find(
    ({ document, expected }) =>
      document === "summary" && expected.result === "valid",
  );
  const object = JSON.parse(summaryCase.json);
  object.metadata.corpus_size = {};
  const { document } = protocol.validate_summary(object);
  assert.equal(document.metadata.corpus_size.node_count, null);
  assert.equal(
    document.metadata.title === null ||
      typeof document.metadata.title === "string",
    true,
  );

  const declarations = readFileSync(
    join(dist, "graphforge_protocol_wasm.d.ts"),
    "utf8",
  );
  const field = (name) => new RegExp(`^\\s+${name}: bigint \\| null;`, "m");
  for (const name of [
    "node_count",
    "relationship_count",
    "source_count",
    "artifact_count",
  ]) {
    assert.match(declarations, field(name));
  }
  // Optional-by-omission fields stay optional, not nullable.
  assert.match(declarations, /^\s+preferred_chunk_bytes\?: bigint;/m);
});

test("a caller string carrying the bigint mark is never rewritten", () => {
  const zeros = `gf-bigint-${"00".repeat(16)}:`;
  const real = crypto.getRandomValues.bind(crypto);
  let calls = 0;
  // The first mark is predictable (all zero bytes); later marks are random.
  crypto.getRandomValues = (bytes) =>
    calls++ === 0 ? bytes.fill(0) : real(bytes);
  try {
    // No bigint: the colliding string stays a string, as in the text form.
    const colliding = JSON.parse(manifestCase.json);
    colliding.objects[0].length = `${zeros}1`;
    assert.deepEqual(
      verdict(protocol.validate_manifest, colliding),
      verdict(protocol.validate_manifest, JSON.stringify(colliding)),
    );
    assert.equal(verdict(protocol.validate_manifest, colliding).ok, false);

    // A bigint beside a colliding string: the first mark collides, so the
    // conversion retries with a fresh mark and writes only the bigint exactly.
    calls = 0;
    const mixed = JSON.parse(manifestCase.json);
    mixed.extensions = { "x-mark": `${zeros}7`, "x-count": 5n };
    const text = JSON.stringify({
      ...mixed,
      extensions: { "x-mark": `${zeros}7`, "x-count": 5 },
    });
    assert.deepEqual(
      verdict(protocol.validate_manifest, mixed),
      verdict(protocol.validate_manifest, text),
    );
    assert.ok(calls >= 2, "the colliding first mark was retried");
  } finally {
    crypto.getRandomValues = real;
  }
});
