import assert from "node:assert/strict";
import { mkdtempSync, realpathSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { retrieveAnalyze } from "../workflows/index.js";

const UUIDS = Array.from(
  { length: 6 },
  (_, index) =>
    `018f0f4e-7b8c-7000-8000-${String(index + 1).padStart(12, "0")}`,
);

function table(rows) {
  const fields = [...new Set(rows.flatMap((row) => Object.keys(row)))];
  return {
    getChild: (name) => ({ get: (index) => rows[index]?.[name] }),
    numRows: rows.length,
    schema: { fields: fields.map((name) => ({ name })) },
  };
}

function project(handlers = {}) {
  const path = realpathSync(
    mkdtempSync(join(tmpdir(), "graphforge-retrieve-")),
  );
  writeFileSync(join(path, "FORMAT"), "graphforge-project/v1\n");
  class GraphForge {
    static instance;
    constructor(openedPath) {
      assert.equal(openedPath, path);
      this.calls = [];
      GraphForge.instance = this;
    }
    async projectCapabilities() {
      return table([
        { capability_id: "graph", capability_version: 1, status: "supported" },
        {
          capability_id: "knowledge",
          capability_version: 1,
          status: "supported",
        },
      ]);
    }
    find(...args) {
      this.calls.push(["find", args]);
      return handlers.find
        ? handlers.find(...args)
        : table([{ node_uuid: UUIDS[0] }]);
    }
    prepareRankInvocation(...args) {
      this.calls.push(["prepareRankInvocation", args]);
      return { algorithm: args[1], fingerprint: "11".repeat(32), verb: "rank" };
    }
    prepareClusterInvocation(...args) {
      this.calls.push(["prepareClusterInvocation", args]);
      return {
        algorithm: args[1],
        fingerprint: "22".repeat(32),
        verb: "cluster",
      };
    }
    preparePathsInvocation(...args) {
      this.calls.push(["preparePathsInvocation", args]);
      return {
        algorithm: args[2],
        fingerprint: "33".repeat(32),
        verb: "paths",
      };
    }
    prepareAnalyzeInvocation(...args) {
      this.calls.push(["prepareAnalyzeInvocation", args]);
      return {
        algorithm: args[0],
        fingerprint: "44".repeat(32),
        verb: "analyze",
      };
    }
    prepareSimilarInvocation(...args) {
      this.calls.push(["prepareSimilarInvocation", args]);
      return {
        algorithm: args[1],
        fingerprint: "55".repeat(32),
        verb: "similar",
      };
    }
    invokeDescriptor(descriptor) {
      this.calls.push(["invokeDescriptor", descriptor]);
      return handlers.rows
        ? table(handlers.rows)
        : table([{ node_uuid: UUIDS[1], score: 1 }]);
    }
    listAssertions() {
      this.calls.push(["listAssertions"]);
      throw new Error("knowledge must stay closed");
    }
    close() {
      this.closed = true;
    }
  }
  return { GraphForge, path };
}

test("find and every M18 family are reachable without knowledge opens", async () => {
  const { GraphForge, path } = project();
  const find = await retrieveAnalyze({
    GraphForge,
    tableFromIPC: (value) => value,
    path,
    input: {
      force_stale: true,
      query: "ada",
      result_limit: 5,
      space: "default",
      surface: "find",
    },
  });
  assert.equal(find.surface, "find");
  assert.equal(find.find.force_stale, true);
  assert.equal(find.find.space, "default");
  assert.deepEqual(GraphForge.instance.calls[0], [
    "find",
    ["ada", undefined, undefined, undefined, undefined, 5, "default", true],
  ]);

  for (const [surface, algorithm, label] of [
    ["rank", "degree", "Person"],
    ["cluster", "components", "Person"],
    ["paths", "bfs", undefined],
    ["analyze", "is_dag", undefined],
    ["similar", "node_similarity", "Person"],
  ]) {
    const result = await retrieveAnalyze({
      GraphForge,
      tableFromIPC: (value) => value,
      path,
      input: {
        algorithm,
        label,
        result_limit: 3,
        source: surface === "paths" ? UUIDS[0] : undefined,
        surface,
      },
    });
    assert.equal(result.surface, surface);
    assert.equal(result.m18.algorithm, algorithm);
  }
  assert.ok(
    GraphForge.instance.calls.every(([name]) => name !== "listAssertions"),
  );
  assert.equal(GraphForge.instance.closed, true);
});

test("retrieve truncates summaries and fails closed on missing bounds", async () => {
  const { GraphForge, path } = project({
    rows: [
      { node_uuid: UUIDS[0] },
      { node_uuid: UUIDS[1] },
      { node_uuid: UUIDS[2] },
    ],
  });
  const result = await retrieveAnalyze({
    GraphForge,
    tableFromIPC: (value) => value,
    path,
    input: {
      algorithm: "degree",
      label: "Person",
      result_limit: 2,
      surface: "rank",
    },
  });
  assert.equal(result.truncated, true);
  assert.equal(result.summary.length, 2);
  assert.equal(result.result.length, 3);

  await assert.rejects(
    retrieveAnalyze({
      GraphForge,
      tableFromIPC: (value) => value,
      path,
      input: { query: "x", surface: "find" },
    }),
    { code: "GF_AGENT_RETRIEVE_BOUNDS_REQUIRED" },
  );
  await assert.rejects(
    retrieveAnalyze({
      GraphForge,
      tableFromIPC: (value) => value,
      path,
      input: { result_limit: 1, surface: "find" },
    }),
    { code: "GF_AGENT_RETRIEVE_FIND_REQUIRED" },
  );
});
