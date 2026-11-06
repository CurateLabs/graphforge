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
  "module-version-order-conformance.json",
);

test("declared precedence corpus replays through the actual public WASM export", () => {
  for (const { name, a, b, limits, expected } of corpus.cases) {
    const outcome = attempt(() =>
      protocol.compare_module_versions(a, b, limits),
    );
    if (typeof expected === "string") {
      assert.equal(outcome.ok, true, name);
      assert.equal(outcome.value, expected, name);
      assert.equal(
        protocol.compare_module_versions(JSON.parse(a), JSON.parse(b), limits),
        expected,
        name,
      );
    } else {
      assert.equal(outcome.ok, false, name);
      assert.equal(outcome.error.code, expected.code, name);
      assert.equal(outcome.error.path, expected.field ?? undefined, name);
    }
  }
});

test("declared order is a u32 number in Rust-generated declarations and WASM values", () => {
  const declarations = readFileSync(
    join(dist, "graphforge_protocol_wasm.d.ts"),
    "utf8",
  );
  for (const type of [
    "DeclaredModuleVersion",
    "ItemDescriptor",
    "SummaryOntologyModule",
  ]) {
    const body = declarations.match(
      new RegExp(`export interface ${type} \\{([^}]+)\\}`),
    )[1];
    assert.match(body, /version_order\?: number;/);
    assert.doesNotMatch(body, /version_order\??: bigint/);
  }
  assert.match(
    declarations,
    /export type ModuleVersionOrdering = "less" \| "equal" \| "greater" \| "unordered";/,
  );
  assert.match(
    declarations,
    /export function compare_module_versions\(a: JsonInput<DeclaredModuleVersion>, b: JsonInput<DeclaredModuleVersion>, limits\?: DiscoveryLimitsOverride \| null\): ModuleVersionOrdering;/,
  );
  const items = readJson(
    "docs",
    "reference",
    "discovery",
    "v1",
    "item-conformance.json",
  );
  const item = items.cases.find((c) => c.name === "declared-order-4294967295")
    .expected.document;
  const output = protocol.validate_item_descriptor(item).document;
  assert.equal(typeof output.version_order, "number");
  assert.equal(output.version_order, 4294967295);
  const summaries = readJson(
    "docs",
    "reference",
    "discovery",
    "v1",
    "module-metadata-conformance.json",
  );
  const summary = summaries.cases.find(
    (c) => c.name === "summary-declared-order-4294967295",
  );
  const module = protocol.validate_summary(summary.json).document.facts
    .ontology_composition.modules[0];
  assert.equal(typeof module.version_order, "number");
  assert.equal(module.version_order, 4294967295);
});
