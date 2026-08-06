import assert from "node:assert/strict";
import test from "node:test";
import { tableFromIPC } from "apache-arrow";

import { neighbourhood } from "../lib/recipes.mjs";

const { GraphForge } = await import("../index.js");

test("neighbourhood returns distinct name rows without duplicate columns", () => {
  const forge = new GraphForge();
  const alice = forge.addNode("Person", { name: "Alice" });
  const bob = forge.addNode("Person", { name: "Bob" });
  const charlie = forge.addNode("Person", { name: "Charlie" });
  forge.addEdge(alice, "KNOWS", bob);
  forge.addEdge(bob, "KNOWS", charlie);

  const table = tableFromIPC(
    neighbourhood(forge, "Alice", 2, {
      label: "Person",
      canonicalProp: "name",
    }),
  );
  assert.deepEqual(
    table.schema.fields.map((field) => field.name),
    ["name", "labels"],
  );
  const names = [...(table.getChild("name")?.toArray() ?? [])]
    .map(String)
    .sort();
  assert.deepEqual(names, ["Bob", "Charlie"]);
});

test("neighbourhood hops 0 returns typed empty Arrow table", () => {
  const forge = new GraphForge();
  forge.addNode("Person", { name: "Alice" });
  const table = tableFromIPC(
    neighbourhood(forge, "Alice", 0, {
      label: "Person",
      canonicalProp: "name",
    }),
  );
  assert.equal(table.numRows, 0);
  assert.deepEqual(
    table.schema.fields.map((field) => field.name),
    ["name", "labels"],
  );
});
