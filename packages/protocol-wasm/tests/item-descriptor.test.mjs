import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { test } from "node:test";
import { attempt, dist, protocol, readJson } from "./support.mjs";

const corpus = readJson(
  "docs",
  "reference",
  "discovery",
  "v1",
  "item-conformance.json",
);

test("item descriptor corpus has the same verdict through the public WASM package", () => {
  for (const { name, json, limits, expected } of corpus.cases) {
    const outcome = attempt(() =>
      protocol.validate_item_descriptor(json, limits),
    );
    if (expected.result === "valid") {
      assert.equal(outcome.ok, true, name);
      assert.deepEqual(outcome.value.document, expected.document, name);
      assert.deepEqual(
        protocol.validate_item_descriptor(expected.document).document,
        expected.document,
        name,
      );
    } else {
      assert.equal(outcome.ok, false, name);
      assert.equal(outcome.error.code, expected.code, name);
      assert.equal(outcome.error.path, expected.field ?? undefined, name);
    }
  }
});

test("item descriptor declarations come from the Rust wire types", () => {
  const declarations = readFileSync(
    join(dist, "graphforge_protocol_wasm.d.ts"),
    "utf8",
  );
  assert.match(
    declarations,
    /export type ItemKind = "ontology_module" \| "bridge_set";/,
  );
  assert.match(
    declarations,
    /export interface ItemDescriptor \{[\s\S]*?format: string;[\s\S]*?id: string;[\s\S]*?kind: ItemKind;[\s\S]*?repository: RepositoryIdentity;[\s\S]*?module: ExactIdentity;[\s\S]*?discovery: ItemDiscoverySnapshot;/,
  );
  assert.match(
    declarations,
    /export interface ItemDiscoverySnapshot \{[\s\S]*?refs_revision: Sha256Digest;[\s\S]*?manifest_validator: Sha256Digest;/,
  );
  assert.match(
    declarations,
    /export function validate_item_descriptor\(descriptor: JsonInput<ItemDescriptor>, limits\?: DiscoveryLimitsOverride \| null\): ValidatedItemDescriptor;/,
  );
});
