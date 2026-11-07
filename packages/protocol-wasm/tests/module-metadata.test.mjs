import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { test } from "node:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { dist, protocol, readJson } from "./support.mjs";

const fixture = ["tests", "fixtures", "hub", "generated", "v1"];

test("module license and fork provenance are committed by WASM summary binding", () => {
  const summary = readJson(
    ...fixture,
    "objects",
    "opendata-openalex.summary.json",
  );
  const manifest = readJson(...fixture, "manifest.json");
  summary.facts.ontology_composition.modules[0].license = "MIT OR Apache-2.0";
  summary.facts.ontology_composition.modules[0].forked_from = {
    id: "https://graphforge.sh/ont_aaaaaaaaaaaaaaaa",
    version: "source-1",
    digest: `sha256:${"a".repeat(64)}`,
  };
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

  for (const [field, value] of [
    ["license", "BSD-3-Clause"],
    [
      "definition_counts",
      {
        ...summary.facts.ontology_composition.modules[0].definition_counts,
        properties: 3,
      },
    ],
    [
      "forked_from",
      {
        ...summary.facts.ontology_composition.modules[0].forked_from,
        version: "forged",
      },
    ],
  ]) {
    const tampered = structuredClone(summary);
    tampered.facts.ontology_composition.modules[0][field] = value;
    const forged = protocol.validate_summary(tampered);
    assert.notEqual(forged.digest, validated.digest);
    assert.throws(
      () => protocol.bind_summary(manifest, forged.canonical_json),
      ({ code }) => code === "integrity_failure",
    );
  }
});

test("module definition counts are optional u32 numbers in WASM and generated declarations", () => {
  const declarations = readFileSync(
    join(dist, "graphforge_protocol_wasm.d.ts"),
    "utf8",
  );
  const moduleBody = declarations.match(
    /export interface SummaryOntologyModule \{([^}]+)\}/,
  )[1];
  assert.match(
    moduleBody,
    /definition_counts\?: SummaryModuleDefinitionCounts;/,
  );
  const countsBody = declarations.match(
    /export interface SummaryModuleDefinitionCounts \{([^}]+)\}/,
  )[1];
  for (const field of ["entity_types", "properties", "relation_types"]) {
    assert.match(countsBody, new RegExp(`${field}: number;`));
    assert.doesNotMatch(countsBody, new RegExp(`${field}: bigint`));
  }
  const summary = readJson(
    ...fixture,
    "objects",
    "opendata-openalex.summary.json",
  );
  const validated = protocol.validate_summary(summary);
  const counts =
    validated.document.facts.ontology_composition.modules[0].definition_counts;
  assert.deepEqual(counts, {
    entity_types: 3,
    properties: 2,
    relation_types: 1,
  });
  for (const count of Object.values(counts))
    assert.equal(typeof count, "number");
  protocol.bind_summary(
    readJson(...fixture, "manifest.json"),
    validated.canonical_json,
  );
  const corpus = readJson(
    "docs",
    "reference",
    "discovery",
    "v1",
    "module-definition-counts-conformance.json",
  );
  const max = corpus.cases.find(
    (c) => c.name === "summary-module-definition-counts-max",
  );
  const output = protocol.validate_summary(max.json).document.facts
    .ontology_composition.modules[0].definition_counts;
  for (const count of Object.values(output)) {
    assert.equal(typeof count, "number");
    assert.equal(count, 4294967295);
  }
});
