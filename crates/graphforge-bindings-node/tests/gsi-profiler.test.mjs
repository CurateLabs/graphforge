// GSI profiler binding parity for #398.
import assert from "node:assert/strict";
import { test } from "node:test";

import { GraphForge } from "../index.js";

test("empty workspace grades Gx-00-XS-D00", () => {
  const forge = new GraphForge();
  const empty = forge.profileGsi();
  assert.equal(empty.gsi, "Gx-00-XS-D00");
  assert.equal(empty.directedness, "unknown");
  assert.equal(empty.nodeCount, 0n);
  assert.equal(empty.edgeCount, 0n);
  assert.equal(empty.densityInteger, 0);
  assert.equal(forge.graphDirectedness(), null);
});

test("triangle grades GD/GU/Gx with configuration", () => {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), " +
      "(c:Person {name:'Carol'}), " +
      "(a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), (c)-[:KNOWS]->(a)",
  );
  const unknown = forge.profileGsi();
  assert.equal(unknown.gsi, "Gx-01-XS-D50");
  assert.equal(unknown.directedness, "unknown");
  assert.equal(unknown.nodeCount, 3n);
  assert.equal(unknown.edgeCount, 3n);

  forge.setGraphDirectedness(
    "00000000-0000-0000-0000-000000009b79",
    "undirected",
  );
  assert.equal(forge.graphDirectedness(), "undirected");
  assert.equal(forge.profileGsi().gsi, "GU-01-XS-D100");

  forge.setGraphDirectedness(
    "00000000-0000-0000-0000-000000009b7a",
    "directed",
  );
  assert.equal(forge.profileGsi().gsi, "GD-01-XS-D50");

  forge.setGraphDirectedness("00000000-0000-0000-0000-000000009b7b", null);
  assert.equal(forge.graphDirectedness(), null);
  assert.equal(forge.profileGsi().gsi, "Gx-01-XS-D50");
});

test("tiny graph and unknown directedness fail closed", () => {
  const forge = new GraphForge();
  forge.execute("CREATE (a:Person {name:'Alice'})");
  const tiny = forge.profileGsi();
  assert.equal(tiny.gsi, "Gx-01-XS-D00");
  assert.equal(tiny.densityInteger, 0);

  assert.throws(
    () =>
      forge.setGraphDirectedness(
        "00000000-0000-0000-0000-000000009b7c",
        "bidirectional",
      ),
    (error) =>
      error?.code === "GF_VALIDATION" || /directedness/i.test(String(error)),
  );
});
