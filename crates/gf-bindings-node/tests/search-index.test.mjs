// Fresh-addon acceptance for typed graph-native search indexing.

import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { GraphForge } from "../index.js";

function expectValidation(fragment, call) {
  assert.throws(
    call,
    (error) =>
      error.code === "ValidationError" && error.message.includes(fragment),
  );
}

test("typed text and vector indexing persist through the Rust facade", () => {
  const project = mkdtempSync(join(tmpdir(), "gf-node-search-index-"));
  const forge = new GraphForge(project);
  try {
    const alice = forge.addNode("Person", {
      name: "Alice",
      summary: "Graph search",
      age: 30,
    });
    const animal = forge.addNode("Animal", { name: "Otter" });
    forge.addEdge(alice, "KNOWS", animal);
    forge.addNode("adjacency", { name: "Label, not capability" });

    forge.index("Person", { properties: null });
    forge.index("Person", { properties: ["summary", "name"] });
    forge.index("Person", { rebuild: true });
    const receipt = forge.index("Person", {
      properties: ["name"],
      rebuild: true,
    });
    assert.deepEqual(
      Object.keys(receipt).sort(),
      [
        "projectGenerationUuid",
        "properties",
        "sourceGeneration",
        "sourceFingerprint",
        "artifactGeneration",
        "artifactSourceGeneration",
        "artifactSourceFingerprint",
        "state",
        "reason",
      ].sort(),
    );
    assert.deepEqual(receipt.properties, ["name"]);
    assert.equal(receipt.state, "current");
    assert.equal(receipt.reason, null);
    assert.equal(receipt.artifactSourceGeneration, receipt.sourceGeneration);
    assert.equal(receipt.artifactSourceFingerprint, receipt.sourceFingerprint);

    forge.addNode("Person", { name: "Bob" });
    const stale = forge.inspectTextIndex("Person", ["name"]);
    assert.equal(stale.state, "stale");
    assert.equal(stale.reason, "source_generation_changed");
    const repaired = forge.index("Person", {
      properties: ["name"],
      rebuild: true,
    });
    assert.equal(repaired.state, "current");
    assert.notEqual(repaired.artifactGeneration, receipt.artifactGeneration);

    expectValidation("requires text fields", () => forge.index("Person"));
    expectValidation("cannot be combined", () =>
      forge.index("Person", {
        properties: ["name"],
        node: alice,
        vector: [1, 0],
        space: "semantic",
      }),
    );
    expectValidation("at least one property", () =>
      forge.index("Person", { properties: [] }),
    );
    expectValidation("duplicate", () =>
      forge.index("Person", { properties: ["name", "name"] }),
    );
    expectValidation("not observed as a string", () =>
      forge.index("Person", { properties: ["age"] }),
    );
    expectValidation("unknown", () =>
      forge.index("Missing", { properties: null }),
    );

    forge.index("Person", {
      node: alice,
      vector: [1, 0],
      space: "semantic",
    });
    forge.index("Person", {
      node: alice.uuid,
      vector: [1, 0],
      space: "semantic",
    });
    forge.index("Person", {
      node: { label: "Person", property: "name", value: "Alice" },
      vector: [0, 1],
      space: "semantic",
    });
    forge.index("Person", {
      node: alice.uuid,
      vector: [0, 1],
      space: "semantic",
    });

    expectValidation("requires space", () =>
      forge.index("Person", { node: alice, vector: [1, 0] }),
    );
    expectValidation("required label", () =>
      forge.index("Person", {
        node: animal,
        vector: [1, 0],
        space: "semantic",
      }),
    );
    expectValidation("non-zero", () =>
      forge.index("Person", {
        node: alice,
        vector: [0, 0],
        space: "other",
      }),
    );
    expectValidation("finite", () =>
      forge.index("Person", {
        node: alice,
        vector: [Number.NaN, 1],
        space: "other",
      }),
    );
    expectValidation("exceeds the finite f32 range", () =>
      forge.index("Person", {
        node: alice,
        vector: [Number.MAX_VALUE, 1],
        space: "other",
      }),
    );
    expectValidation("smaller than the finite f32 range", () =>
      forge.index("Person", {
        node: alice,
        vector: [Number.MIN_VALUE, 1],
        space: "other",
      }),
    );
    expectValidation("dimension", () =>
      forge.index("Person", {
        node: alice,
        vector: [1],
        space: "semantic",
      }),
    );
    expectValidation("property selector", () =>
      forge.index("Person", {
        node: { unsupported: true },
        vector: [1, 0],
        space: "other",
      }),
    );

    const adjacency = forge.indexAdjacency();
    assert.deepEqual(
      Object.keys(adjacency),
      [
        "projectGenerationUuid",
        "sourceTopologyGeneration",
        "sourceTopologyFingerprint",
        "artifactSourceGeneration",
        "artifactEffectiveGeneration",
        "artifactFingerprint",
        "state",
        "reason",
      ].sort(),
    );
    assert.equal(adjacency.state, "current");
    assert.equal(
      adjacency.artifactFingerprint,
      adjacency.sourceTopologyFingerprint,
    );
    assert.deepEqual(forge.inspectAdjacency(), adjacency);
    forge.addNode("Person", {
      name: "Topology generation without an edge delta",
    });
    assert.equal(forge.inspectAdjacency().state, "current");
    forge.execute("MATCH ()-[r:KNOWS]->() DELETE r");
    const staleAdjacency = forge.inspectAdjacency();
    assert.equal(staleAdjacency.state, "stale");
    assert.equal(staleAdjacency.reason, "incomplete_delta_chain");
    forge.index("adjacency");
    forge.index("adjacency", { properties: null });
    const expectedReopen = forge.inspectTextIndex("Person", ["name"]);
    const expectedAdjacency = forge.inspectAdjacency();
    forge.close();

    const reopened = new GraphForge(project);
    try {
      assert.deepEqual(
        reopened.inspectTextIndex("Person", ["name"]),
        expectedReopen,
      );
      assert.deepEqual(reopened.inspectAdjacency(), expectedAdjacency);
      reopened.index("Person", {
        node: alice.uuid,
        vector: [0, 1],
        space: "semantic",
      });
    } finally {
      reopened.close();
    }
  } finally {
    forge.close();
    rmSync(project, { recursive: true, force: true });
  }
});
