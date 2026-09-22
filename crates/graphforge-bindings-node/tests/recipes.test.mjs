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

test("neighbourhood defaults preserve canonical identity and typed zero-hop schema", () => {
  const forge = new GraphForge();
  try {
    const ada = forge.addNode("Entity", { canonical: "ada", name: "Ada" });
    const ben = forge.addNode("Entity", { canonical: "ben", name: "Ben" });
    const cy = forge.addNode("Entity", { canonical: "cy", name: "Cy" });
    forge.addNode("Entity", { canonical: "outside", name: "Outside" });
    forge.addEdge(ada, "KNOWS", ben);
    forge.addEdge(ben, "KNOWS", cy);

    const direct = tableFromIPC(neighbourhood(forge, "ada", 1));
    const twoHops = tableFromIPC(neighbourhood(forge, "ada"));
    const zeroHops = tableFromIPC(neighbourhood(forge, "ada", 0));
    assert.deepEqual(Array.from(direct.getChild("canonical")), ["ben"]);
    assert.deepEqual(
      twoHops
        .toArray()
        .map((row) => [row.canonical, row.name])
        .sort(),
      [
        ["ben", "Ben"],
        ["cy", "Cy"],
      ],
    );
    assert.deepEqual(
      twoHops.schema.fields.map((field) => field.name),
      ["canonical", "name", "labels"],
    );
    assert.equal(zeroHops.numRows, 0);
    const schema = (table) =>
      table.schema.fields.map((field) => ({
        name: field.name,
        type: field.type.toString(),
        nullable: field.nullable,
      }));
    assert.deepEqual(schema(zeroHops), schema(twoHops));
    assert.deepEqual(schema(direct), schema(twoHops));
  } finally {
    forge.close();
  }
});

test("neighbourhood refuses invalid identifiers and hops before native execution", () => {
  const forge = new GraphForge();
  try {
    forge.addNode("Entity", { canonical: "ada", name: "Ada" });
    const query =
      "MATCH(n:Entity) RETURN n.canonical AS canonical,n.name AS name";
    const before = tableFromIPC(forge.execute(query))
      .toArray()
      .map((row) => row.toJSON());
    let calls = 0;
    const observed = {
      execute(...args) {
        calls += 1;
        return forge.execute(...args);
      },
    };
    for (const [options, field] of [
      [{ label: "Entity) RETURN n" }, "label"],
      [{ label: null }, "label"],
      [{ canonicalProp: "name`" }, "canonical_prop"],
    ]) {
      assert.throws(
        () => neighbourhood(observed, "ada", 1, options),
        (error) =>
          error instanceof TypeError &&
          error.message.startsWith(`${field} must be a valid identifier`),
      );
    }
    for (const hops of [-1, 1.5, true, "2"]) {
      assert.throws(
        () => neighbourhood(observed, "ada", hops),
        (error) =>
          error instanceof TypeError &&
          error.message.startsWith("hops must be an integer >= 0"),
      );
    }
    assert.equal(calls, 0);
    assert.deepEqual(
      tableFromIPC(forge.execute(query))
        .toArray()
        .map((row) => row.toJSON()),
      before,
    );
  } finally {
    forge.close();
  }
});
