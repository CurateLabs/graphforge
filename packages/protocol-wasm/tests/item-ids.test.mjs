import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { test } from "node:test";
import { attempt, dist, protocol } from "./support.mjs";

test("both permanent item kinds normalize bare and URI references through WASM", () => {
  for (const [prefix, kind] of [
    ["ont", "ontology_module"],
    ["brg", "bridge_set"],
  ]) {
    const id = `${prefix}_7k2m9x4q8r3t5v1w`;
    const uri = `https://graphforge.sh/${id}`;
    for (const reference of [id, uri, id.toUpperCase(), uri.toUpperCase()]) {
      assert.deepEqual(protocol.parse_item_id(reference), { id, uri, kind });
    }
  }
});

test("malformed and Unicode references fail without trapping the module", () => {
  const id = "ont_7k2m9x4q8r3t5v1w";
  for (const reference of [
    "",
    ...["i", "l", "o", "u", "I", "L", "O", "U"].map(
      (alias) => `${id.slice(0, -1)}${alias}`,
    ),
    "abc_0000000000000000",
    id.slice(0, -1),
    `${id}0`,
    id.replace("_", "-"),
    `https://example.org/${id}`,
    `http://graphforge.sh/${id}`,
    `https://graphforge.sh:443/${id}`,
    `https://graphforge.sh/owner/${id}`,
    ...["/", "/2026.01", "?x=1", "#fragment"].map(
      (suffix) => `https://graphforge.sh/${id}${suffix}`,
    ),
    "https:",
    "https://graphforge.sé",
    "éx_0000000000000000",
    "😀_000000000000000",
    "ont_7k2m9x4q8r3t5vé",
    "ont_7k2m9x4q8r3t5v1é",
    "ont_000000000000000\0",
    "ont_00000000000000K",
    ` ${id}`,
    "\ud800",
    "\udc00",
  ]) {
    const outcome = attempt(() => protocol.parse_item_id(reference));
    assert.equal(outcome.ok, false, JSON.stringify(reference));
    assert.equal(outcome.error.code, "invalid_argument");
    assert.equal(Object.getPrototypeOf(outcome.error), Object.prototype);
    assert.equal(protocol.parse_item_id(id).id, id);
  }
});

test("shape reservation accepts unknown prefixes and only canonical lowercase names", () => {
  for (const name of [
    "ont_7k2m9x4q8r3t5v1w",
    "brg_zzzzzzzzzzzzzzzz",
    "abc_0000000000000000",
  ]) {
    assert.equal(protocol.has_item_id_shape(name), true, name);
  }
  assert.equal(
    attempt(() => protocol.parse_item_id("abc_0000000000000000")).ok,
    false,
  );
  for (const name of [
    "",
    "ONT_7K2M9X4Q8R3T5V1W",
    "ont_7K2m9x4q8r3t5v1w",
    "abc_000000000000000",
    "abc_00000000000000000",
    "abc_000000000000000o",
    "https://graphforge.sh/ont_7k2m9x4q8r3t5v1w",
    "éx_0000000000000000",
    "😀_000000000000000",
  ]) {
    assert.equal(protocol.has_item_id_shape(name), false, name);
  }
  for (const name of ["\ud800", "\udc00"]) {
    assert.equal(
      attempt(() => protocol.has_item_id_shape(name)).error.code,
      "invalid_argument",
    );
  }
});

test("item validation needs no JavaScript entropy", () => {
  const original = crypto.getRandomValues;
  crypto.getRandomValues = () => {
    throw new Error("entropy unavailable");
  };
  try {
    assert.equal(
      protocol.parse_item_id("ONT_7K2M9X4Q8R3T5V1W").kind,
      "ontology_module",
    );
    assert.equal(protocol.has_item_id_shape("abc_0000000000000000"), true);
  } finally {
    crypto.getRandomValues = original;
  }
});

test("generated declarations expose the typed parser and boolean shape export", () => {
  const declarations = readFileSync(
    join(dist, "graphforge_protocol_wasm.d.ts"),
    "utf8",
  );
  assert.match(
    declarations,
    /export type ItemIdKind = "ontology_module" \| "bridge_set";/,
  );
  assert.match(
    declarations,
    /export interface ParsedItemId \{[\s\S]*?id: string;[\s\S]*?uri: string;[\s\S]*?kind: ItemIdKind;/,
  );
  assert.match(
    declarations,
    /export function parse_item_id\(reference: string\): ParsedItemId;/,
  );
  assert.match(
    declarations,
    /export function has_item_id_shape\(name: string\): boolean;/,
  );
});
