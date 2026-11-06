import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { test } from "node:test";
import { protocol, readJson } from "./support.mjs";

const fixture = ["tests", "fixtures", "hub", "generated", "v1"];

test("a declared module license is committed by WASM summary binding", () => {
  const summary = readJson(
    ...fixture,
    "objects",
    "opendata-openalex.summary.json",
  );
  const manifest = readJson(...fixture, "manifest.json");
  summary.facts.ontology_composition.modules[0].license = "MIT OR Apache-2.0";
  summary.requirements.push({
    capability: "ontology-module-metadata",
    major: 1,
  });
  summary.requirements.sort((a, b) => a.capability.localeCompare(b.capability));
  const validated = protocol.validate_summary(summary);
  const bytes = validated.canonical_json;
  const objectDigest = `sha256:${createHash("sha256").update(bytes).digest("hex")}`;
  const object = manifest.objects.find(
    ({ digest }) => digest === manifest.summary.object_digest,
  );
  object.digest = objectDigest;
  object.length = Buffer.byteLength(bytes);
  manifest.objects.sort((a, b) => a.digest.localeCompare(b.digest));
  manifest.summary.object_digest = objectDigest;
  manifest.summary.summary_digest = validated.digest;
  protocol.bind_summary(manifest, bytes);

  summary.facts.ontology_composition.modules[0].license = "BSD-3-Clause";
  const forged = protocol.validate_summary(summary);
  assert.notEqual(forged.digest, validated.digest);
  assert.throws(
    () => protocol.bind_summary(manifest, forged.canonical_json),
    ({ code }) => code === "integrity_failure",
  );
});
