// Real Rust-owned Slice parity; no selection logic lives in JavaScript.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { createRequire } from "node:module";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
const require = createRequire(import.meta.url);
const { GraphForge } = require("../index.js");

test("native Slice boundary, freeze, revision and cancellation", async () => {
  const graph = new GraphForge();
  try {
    await graph.execute(
      "CREATE (a:Story {name: 'First'}), (b:Story {name: 'Second'}), (c:Character {name: 'Shared'}), (a)-[:FEATURES]->(c), (b)-[:FEATURES]->(c)",
    );
    const request = {
      request_uuid: randomUUID(),
      source: { kind: "current" },
      selector: {
        kind: "filter",
        label: "Character",
        property: "name",
        equals: "Shared",
      },
    };
    assert.equal(
      tableFromIPC(await graph.previewSlice(request, "included")).numRows,
      1,
    );
    assert.equal(
      tableFromIPC(await graph.previewSlice(request, "boundary")).numRows,
      4,
    );
    const version = randomUUID();
    const prepared = graph.prepareResearchVersion({
      operation_uuid: randomUUID(),
      version_uuid: version,
      context_uuid: randomUUID(),
      label: null,
      description: null,
      created_at: 1,
      required_versions: [],
    });
    await graph.commitResearchVersionOperation(prepared);
    request.source = { kind: "version", version_uuid: version };
    const capsule = await graph.freezeSlice(request);
    await graph.execute("CREATE (:Character {name: 'Later'})");
    const included = tableFromIPC(
      await graph.inspectFrozenSlice(capsule, "included"),
    );
    assert.equal(included.numRows, 1);
    const revised = await graph.reviseFrozenSlice(capsule, {
      request_uuid: randomUUID(),
      exclude: { nodes: [included.getChild("object_uuid").get(0)] },
      source_version: null,
    });
    assert.equal(
      tableFromIPC(await graph.inspectFrozenSlice(revised, "included")).numRows,
      0,
    );
    const controller = new AbortController();
    controller.abort();
    await assert.rejects(
      graph.previewSlice(
        request,
        "included",
        undefined,
        undefined,
        controller.signal,
      ),
      (error) => error.code === "GF_CANCELLED",
    );
    assert.throws(
      () => graph.freezeSlice({ private_sentinel: "private-secret-value" }),
      (error) =>
        !error.message.includes("private_sentinel") &&
        !error.message.includes("private-secret-value"),
    );
  } finally {
    graph.close();
  }
});
