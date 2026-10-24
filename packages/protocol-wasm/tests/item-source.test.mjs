import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { test } from "node:test";
import { attempt, dist, protocol, readJson } from "./support.mjs";

const corpus = readJson(
  "docs",
  "reference",
  "hub-publish",
  "v1",
  "item-source-conformance.json",
);
test("actual WASM replays native per-ID source transitions and CAS expectations", () => {
  for (const { name, json, expected } of corpus.cases) {
    for (const input of [
      json,
      ...(name === "duplicate-json-member" ? [] : [JSON.parse(json)]),
    ]) {
      const result = attempt(() => protocol.plan_item_source_transition(input));
      if (expected.plan) {
        assert.equal(result.ok, true, name);
        assert.deepEqual(result.value, expected.plan, name);
      } else {
        assert.equal(result.ok, false, name);
        assert.equal(result.error.code, expected.code, name);
      }
    }
  }
});
test("source transitions use bounded native JSON and generated types", () => {
  assert.equal(
    attempt(() =>
      protocol.plan_item_source_transition(" ".repeat(16 * 1024 * 1024 + 1)),
    ).error.code,
    "invalid_input",
  );
  const declarations = readFileSync(
    join(dist, "graphforge_protocol_wasm.d.ts"),
    "utf8",
  );
  assert.match(
    declarations,
    /export function plan_item_source_transition\(input: JsonInput<ItemSourceTransitionInput>\): ItemSourceTransitionPlan;/,
  );
  assert.match(
    declarations,
    /export interface ItemSourceDesignation \{[\s\S]*?repository_id: StableRepositoryId;/,
  );
  assert.match(declarations, /export type StableRepositoryId = string;/);
});
