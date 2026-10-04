import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { randomUUID } from "node:crypto";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
const require = createRequire(import.meta.url);
const { GraphForge } = require("../index.js");
const total = (ipc) => Number(tableFromIPC(ipc).getChild("total").get(0));

test("saved query CRUD, parameter validation and historical aggregates survive reopen", async () => {
  const directory = mkdtempSync(join(tmpdir(), "gf-saved-queries-"));
  let graph;
  try {
    const root = join(directory, "project");
    graph = new GraphForge(root);
    await graph.execute("CREATE (:Item {score:1}), (:Item {score:3})");
    const saved = {
      query_uuid: randomUUID(),
      name: "Items above threshold",
      description: "Reusable aggregate",
      query:
        "MATCH (n:Item) WHERE n.score >= $minimum RETURN count(n) AS total",
      parameters: { minimum: "integer" },
    };
    assert.deepEqual(graph.createSavedQuery(saved), saved);
    assert.deepEqual(graph.savedQueries(), [saved]);
    assert.deepEqual(graph.savedQuery(saved.query_uuid), saved);
    assert.equal(
      total(await graph.executeSavedQuery(saved.query_uuid, { minimum: 2 })),
      1,
    );
    for (const params of [
      undefined,
      { minimum: "2" },
      { minimum: 2, extra: 1 },
    ]) {
      await assert.rejects(graph.executeSavedQuery(saved.query_uuid, params));
    }
    assert.throws(() => graph.createSavedQuery(saved));
    assert.throws(() =>
      graph.updateSavedQuery({ ...saved, query_uuid: randomUUID() }),
    );
    assert.throws(() =>
      graph.createSavedQuery({
        ...saved,
        query_uuid: randomUUID(),
        name: "Mutation",
        query: "CREATE (:Item)",
        parameters: {},
      }),
    );
    const version = randomUUID();
    await graph.commitResearchVersionOperation(
      graph.prepareResearchVersion({
        operation_uuid: randomUUID(),
        version_uuid: version,
        context_uuid: randomUUID(),
        created_at: 1,
        required_versions: [],
      }),
    );
    const historical = { kind: "version", version_uuid: version };
    const updated = { ...saved, name: "Revised threshold" };
    assert.deepEqual(graph.updateSavedQuery(updated), updated);
    await graph.execute("CREATE (:Item {score:4})");
    graph.close();
    graph = new GraphForge(root);
    assert.deepEqual(graph.savedQuery(saved.query_uuid), updated);
    assert.deepEqual(graph.savedQuery(saved.query_uuid, historical), saved);
    assert.deepEqual(graph.savedQueries(historical), [saved]);
    assert.equal(
      total(await graph.executeSavedQuery(saved.query_uuid, { minimum: 2 })),
      2,
    );
    assert.equal(
      total(
        await graph.executeSavedQuery(
          saved.query_uuid,
          { minimum: 2 },
          historical,
        ),
      ),
      1,
    );
    await assert.rejects(
      graph.executeSavedQuery(
        saved.query_uuid,
        { minimum: 2 },
        undefined,
        AbortSignal.abort(),
      ),
    );
    const uuidQuery = {
      ...saved,
      query_uuid: randomUUID(),
      name: "UUID parameter",
      query: "RETURN $identity AS identity",
      parameters: { identity: "uuid" },
    };
    graph.createSavedQuery(uuidQuery);
    const identity = randomUUID();
    const result = tableFromIPC(
      await graph.executeSavedQuery(uuidQuery.query_uuid, {
        identity: { $uuid: identity },
      }),
    );
    assert.equal(result.numRows, 1);
    const actual = result.getChild("identity").get(0);
    assert.ok(
      actual === identity ||
        Buffer.from(actual).toString("hex") === identity.replaceAll("-", ""),
    );
    graph.deleteSavedQuery(saved.query_uuid);
    assert.throws(() => graph.savedQuery(saved.query_uuid));
    assert.deepEqual(graph.savedQuery(saved.query_uuid, historical), saved);
  } finally {
    graph?.close();
    rmSync(directory, { recursive: true, force: true });
  }
});
