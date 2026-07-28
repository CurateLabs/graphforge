import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

test("degree and components write back atomically and persist", () => {
  const dir = mkdtempSync(join(tmpdir(), "gf-node-writeback-"));
  try {
    const forge = new GraphForge(dir);
    forge.execute(
      "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), " +
        "(c:Person {name:'Carol'}), (a)-[:KNOWS]->(b)",
    );

    const degree = tableFromIPC(forge.rank("Person", "degree", "KNOWS", false));
    assert.deepEqual([...degree.getChild("score").toArray()], [0.5, 0.5, 0]);
    assert.equal(
      tableFromIPC(
        forge.execute(
          "MATCH (n:Person) WHERE n.degree_score IS NOT NULL RETURN n.degree_score",
        ),
      ).numRows,
      0,
    );
    const writtenDegree = tableFromIPC(
      forge.rank("Person", "degree", "KNOWS", false, "degree_score"),
    );
    assert.deepEqual(
      [...writtenDegree.getChild("score").toArray()],
      [...degree.getChild("score").toArray()],
    );

    const components = tableFromIPC(
      forge.cluster("Person", "components", "KNOWS", false),
    );
    assert.equal(
      tableFromIPC(
        forge.execute(
          "MATCH (n:Person) WHERE n.component IS NOT NULL RETURN n.component",
        ),
      ).numRows,
      0,
    );
    forge.execute(
      "MATCH (n:Person {name:'Alice'}) SET n.atomic_component = 'old'",
    );
    assert.throws(
      () =>
        forge.cluster(
          "Person",
          "components",
          "KNOWS",
          false,
          "atomic_component",
        ),
      (error) =>
        error.code === "ValidationError" &&
        error.message.includes("collides with existing Utf8 data"),
    );
    assert.deepEqual(
      [
        ...tableFromIPC(
          forge.execute(
            "MATCH (n:Person) WHERE n.atomic_component IS NOT NULL " +
              "RETURN n.atomic_component AS value",
          ),
        )
          .getChild("value")
          .toArray(),
      ],
      ["old"],
    );
    assert.throws(
      () => forge.cluster("Person", "components", "KNOWS", false, ""),
      (error) => error.code === "ValidationError",
    );
    assert.equal(
      tableFromIPC(
        forge.cluster(
          "Missing",
          "components",
          "KNOWS",
          false,
          "empty_component",
        ),
      ).numRows,
      0,
    );
    const writtenComponents = tableFromIPC(
      forge.cluster("Person", "components", "KNOWS", false, "component"),
    );
    assert.deepEqual(
      [...writtenComponents.getChild("community_id").toArray()],
      [...components.getChild("community_id").toArray()],
    );
    forge.close();

    const reopened = new GraphForge(dir);
    const persisted = tableFromIPC(
      reopened.execute(
        "MATCH (n:Person) RETURN n.degree_score AS degree_score, " +
          "n.component AS component ORDER BY n.name",
      ),
    );
    assert.deepEqual(
      [...persisted.getChild("degree_score").toArray()],
      [0.5, 0.5, 0],
    );
    assert.deepEqual(
      [...persisted.getChild("component").toArray()],
      [0n, 0n, 1n],
    );
    reopened.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
