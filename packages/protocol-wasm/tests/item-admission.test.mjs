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
  "item-admission-conformance.json",
);

test("actual WASM admission matches the native decision and privacy oracle", () => {
  for (const { name, input, expected } of corpus.cases) {
    for (const value of [input, JSON.stringify(input)]) {
      const result = attempt(() => protocol.plan_item_admission(value));
      if (expected.plan) {
        assert.equal(result.ok, true, name);
        assert.deepEqual(result.value, expected.plan, name);
      } else {
        assert.equal(result.ok, false, name);
        assert.equal(
          Object.getPrototypeOf(result.error),
          Object.prototype,
          name,
        );
        assert.deepEqual(result.error, expected.error, name);
        assert.ok(
          !JSON.stringify(result.error).includes("opaque:foreign"),
          name,
        );
      }
    }
  }
});

test("native query projection canonicalizes aliases without host grammar", () => {
  const value = corpus.cases.find(
    ({ name }) => name === "canonical-alias-one-tuple",
  ).input;
  const query = protocol.item_admission_requirements(value.manifest);
  assert.deepEqual(query, [
    {
      id: "ont_aaaaaaaaaaaaaaaa",
      kind: "ontology_module",
      tuples: value.facts[0].tuples.map(({ version, content_digest }) => ({
        version,
        content_digest,
      })),
    },
  ]);
  const legacy = corpus.cases.find(
    ({ name }) => name === "legacy-iri-ignored",
  ).input;
  assert.deepEqual(protocol.item_admission_requirements(legacy.manifest), []);
});

export function largeAdmission() {
  const value = structuredClone(corpus.cases[0].input);
  const module = value.manifest.ontology.modules[0];
  const fact = value.facts[0];
  value.manifest.ontology.modules = [];
  value.manifest.ontology.bridge_sets = [];
  value.facts = [];
  for (const prefix of ["ont", "brg"]) {
    for (let n = 0; n < 4096; n += 1) {
      const id = `${prefix}_${n.toString(16).padStart(16, "0")}`;
      const entry = { ...module, id: `https://graphforge.sh/${id}` };
      if (prefix === "brg") delete entry.package;
      value.manifest.ontology[
        prefix === "ont" ? "modules" : "bridge_sets"
      ].push(entry);
      value.facts.push({ ...structuredClone(fact), id });
    }
  }
  return value;
}

test("actual WASM accepts 4096 modules and 4096 bridge sets with complete facts", () => {
  const value = largeAdmission();
  assert.equal(
    protocol.item_admission_requirements(value.manifest).length,
    8192,
  );
  const plan = protocol.plan_item_admission(value);
  assert.equal(plan.decisions.length, 8192);
  assert.equal(plan.decisions[0].kind, "bridge_set");
  value.facts.push(value.facts[0]);
  assert.equal(
    attempt(() => protocol.plan_item_admission(value)).error.code,
    "invalid_input",
  );
});

test("actual WASM query and planner enforce aggregate byte bounds on otherwise valid facts", () => {
  const value = largeAdmission();
  value.manifest.ontology.bridge_sets = [];
  value.facts = value.facts.filter(({ id }) => id.startsWith("ont_"));
  assert.equal(protocol.plan_item_admission(value).decisions.length, 4096);
  for (const module of value.manifest.ontology.modules)
    module.version = "v".repeat(4096);
  const query = attempt(() =>
    protocol.item_admission_requirements(value.manifest),
  );
  assert.equal(query.error.code, "invalid_input");
  assert.equal(
    query.error.message,
    "item admission manifest exceeds its byte bound",
  );
  const control = largeAdmission();
  control.manifest.ontology.bridge_sets = [];
  control.facts = control.facts.filter(({ id }) => id.startsWith("ont_"));
  control.publisher_account_id = "z".repeat(4096);
  for (const fact of control.facts)
    fact.claim = {
      state: "claimed",
      owner_account_id: control.publisher_account_id,
    };
  const plan = attempt(() => protocol.plan_item_admission(control));
  assert.equal(plan.error.code, "invalid_input");
  assert.equal(
    plan.error.message,
    "item admission input exceeds its byte bound",
  );
});

test("malformed Unicode and duplicate members produce sanitized errors without items", () => {
  for (const input of [
    '{"publisher_account_id":"\\ud800"}',
    '{"facts":[],"facts":[]}',
  ]) {
    const { error } = attempt(() => protocol.plan_item_admission(input));
    assert.equal(error.code, "invalid_input");
    assert.equal(error.items, undefined);
  }
});

test("admission declarations are generated from closed native types", () => {
  const declarations = readFileSync(
    join(dist, "graphforge_protocol_wasm.d.ts"),
    "utf8",
  );
  for (const name of [
    "ItemAdmissionInput",
    "ItemAdmissionPlan",
    "ItemClaimFact",
    "ItemTupleFact",
    "ItemClaimRequirement",
  ])
    assert.match(declarations, new RegExp(`export interface ${name} \\{`));
  assert.match(declarations, /export type StableAccountId = string;/);
  assert.match(declarations, /items\?: IdTakenItem\[\];/);
  assert.match(
    declarations,
    /export function plan_item_admission\(input: JsonInput<ItemAdmissionInput>\): ItemAdmissionPlan;/,
  );
});
