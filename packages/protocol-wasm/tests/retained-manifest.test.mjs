// Replay authentic retired controls through the real, packaged native WASM.
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { test } from "node:test";
import { attempt, dist, protocol, readJson } from "./support.mjs";

const corpus = readJson(
  "docs",
  "reference",
  "discovery",
  "v1",
  "retired-manifest-conformance.json",
);
const digest = (text) =>
  `sha256:${createHash("sha256").update(text).digest("hex")}`;

test("retained historical coverage preserves canonical bytes and identity without producer or refs", () => {
  assert.equal(
    corpus.source_commit,
    "9cbb410719d1d463bdb9c9f3b6d67b39bd27e391",
  );
  let covered = 0;
  for (const entry of corpus.cases) {
    assert.equal(digest(entry.json), `sha256:${entry.json_sha256}`, entry.name);
    const result = attempt(() => protocol.assess_retained_manifest(entry.json));
    assert.equal(
      attempt(() => protocol.validate_manifest(entry.json)).ok,
      false,
      entry.name,
    );
    assert.equal(
      attempt(() => protocol.item_admission_requirements(entry.json)).ok,
      false,
      entry.name,
    );
    if (entry.expected.revision === "retired_no_claims") {
      assert.ok(result.ok, `${entry.name}: ${JSON.stringify(result.error)}`);
      const original = JSON.parse(entry.json);
      assert.deepEqual(
        result.value,
        {
          assessment: {
            revision: "retired_no_claims",
            repository: original.repository,
            immutable_version: original.immutable_version,
          },
          canonical_json: entry.expected.canonical_json,
          digest: digest(entry.expected.canonical_json),
        },
        entry.name,
      );
      assert.equal(
        result.value.canonical_json.includes("producing_core_version"),
        false,
      );
      covered++;
    } else {
      assert.equal(result.ok, false, entry.name);
      assert.equal(typeof result.error.code, "string", entry.name);
    }
  }
  assert.equal(covered, 5);
});

test("every current manifest keeps the strict current verdict at default bounds", () => {
  const cases = [
    "conformance.json",
    "module-metadata-conformance.json",
    "module-definition-counts-conformance.json",
  ].flatMap(
    (file) => readJson("docs", "reference", "discovery", "v1", file).cases,
  );
  for (const entry of cases.filter((entry) => entry.document === "manifest")) {
    const validation = attempt(() => protocol.validate_manifest(entry.json));
    const assessment = attempt(() =>
      protocol.assess_retained_manifest(entry.json),
    );
    assert.equal(assessment.ok, validation.ok, entry.name);
    if (validation.ok) {
      assert.deepEqual(
        assessment.value,
        {
          assessment: { revision: "current" },
          canonical_json: validation.value.canonical_json,
          digest: validation.value.digest,
        },
        entry.name,
      );
    } else {
      assert.equal(
        assessment.error.code,
        attempt(() => protocol.item_admission_requirements(entry.json)).error
          .code,
        entry.name,
      );
    }
  }
});

test("retained assessment binds empty ontology controls and enforces the default byte bound", () => {
  const original = JSON.parse(
    corpus.cases.find(
      (entry) => entry.name === "manifest-with-research-lineage",
    ).json,
  );
  original.ontology = {
    composition_digest: original.immutable_version,
    modules: [],
    bridge_sets: [],
  };
  const first = protocol.assess_retained_manifest(JSON.stringify(original));
  original.immutable_version = `sha256:${"f".repeat(64)}`;
  const changed = protocol.assess_retained_manifest(JSON.stringify(original));
  assert.notEqual(changed.digest, first.digest);
  assert.equal(
    changed.assessment.immutable_version,
    original.immutable_version,
  );
  assert.equal(
    attempt(() =>
      protocol.assess_retained_manifest(
        JSON.stringify(original).padEnd(16 * 1024 * 1024 + 1, " "),
      ),
    ).ok,
    false,
  );
});

test("assessment ABI declarations expose only the coverage result and raw text input", () => {
  const declarations = readFileSync(
    join(dist, "graphforge_protocol_wasm.d.ts"),
    "utf8",
  );
  assert.match(
    declarations,
    /export function assess_retained_manifest\(manifest: string\): RetainedManifestInspection;/,
  );
  assert.match(declarations, /export interface RetainedManifestInspection \{/);
  assert.match(declarations, /export type RetainedManifestAssessment =/);
  assert.match(declarations, /revision: "retired_no_claims"/);
  assert.doesNotMatch(
    declarations,
    /export (?:interface|type) RetiredManifest\b/,
  );
});
