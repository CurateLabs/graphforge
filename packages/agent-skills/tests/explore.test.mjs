import assert from "node:assert/strict";
import { mkdtempSync, realpathSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { exploreGraph } from "../workflows/index.js";

const UUIDS = Array.from(
  { length: 8 },
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

function project(rows = [], overrides = {}) {
  const path = realpathSync(mkdtempSync(join(tmpdir(), "graphforge-explore-")));
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

    paths(source, target, by, via, directed, k, weight, heuristic, walkLength) {
      this.calls.push([
        "paths",
        { source, target, by, via, directed, k, weight, heuristic, walkLength },
      ]);
      if (overrides.pathsError) throw overrides.pathsError;
      return table(rows);
    }

    preparePathsInvocation(
      source,
      target,
      by,
      via,
      directed,
      k,
      weight,
      heuristic,
      walkLength,
    ) {
      this.calls.push([
        "preparePathsInvocation",
        { source, target, by, via, directed, k, weight, heuristic, walkLength },
      ]);
      return {
        algorithm: by,
        fingerprint: "aa".repeat(32),
        verb: "paths",
        walkLength,
      };
    }

    invokeDescriptor(descriptor) {
      this.calls.push(["invokeDescriptor", descriptor]);
      return table(rows);
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

test("bounded neighborhood explores through public paths descriptors", async () => {
  const rows = [
    { source_uuid: UUIDS[0], target_uuid: UUIDS[1], cost: 1 },
    { source_uuid: UUIDS[0], target_uuid: UUIDS[2], cost: 1 },
    { source_uuid: UUIDS[0], target_uuid: UUIDS[3], cost: 2 },
  ];
  const { GraphForge, path } = project(rows);
  const result = await exploreGraph({
    GraphForge,
    tableFromIPC: (value) => value,
    path,
    input: {
      depth: 2,
      mode: "neighborhood",
      result_limit: 2,
      start_uuids: [UUIDS[0]],
      via: "KNOWS",
    },
  });

  assert.equal(result.mode, "neighborhood");
  assert.equal(result.algorithm, "bfs");
  assert.equal(result.walk_length, 1);
  assert.equal(result.truncated, true);
  assert.equal(result.summary.length, 2);
  assert.equal(result.result.length, 3);
  assert.deepEqual(result.start_uuids, [UUIDS[0]]);
  assert.ok(
    GraphForge.instance.calls.some(
      ([name]) => name === "preparePathsInvocation",
    ),
  );
  assert.ok(
    GraphForge.instance.calls.every(
      ([name]) => !["listAssertions", "listEvidenceLinks"].includes(name),
    ),
  );
  assert.equal(GraphForge.instance.closed, true);
});

test("unbounded or invalid explore requests fail before native invocation", async () => {
  const { GraphForge, path } = project();
  await assert.rejects(
    exploreGraph({
      GraphForge,
      tableFromIPC: (value) => value,
      path,
      input: {
        mode: "traversal",
        result_limit: 10,
        start_uuids: [UUIDS[0]],
      },
    }),
    { code: "GF_AGENT_EXPLORE_BOUNDS_REQUIRED" },
  );
  await assert.rejects(
    exploreGraph({
      GraphForge,
      tableFromIPC: (value) => value,
      path,
      input: {
        depth: 2,
        mode: "path",
        result_limit: 10,
        start_uuids: [UUIDS[0]],
      },
    }),
    { code: "GF_AGENT_EXPLORE_TARGET_REQUIRED" },
  );
  await assert.rejects(
    exploreGraph({
      GraphForge,
      tableFromIPC: (value) => value,
      path,
      input: {
        depth: 1,
        mode: "neighborhood",
        start_uuids: [UUIDS[0]],
      },
    }),
    { code: "GF_AGENT_EXPLORE_BOUNDS_REQUIRED" },
  );
  assert.equal(GraphForge.instance, undefined);
});

test("reachability and path modes dispatch the selected public algorithms", async () => {
  const { GraphForge, path } = project([
    { source_uuid: UUIDS[0], target_uuid: UUIDS[1] },
  ]);
  const reachability = await exploreGraph({
    GraphForge,
    tableFromIPC: (value) => value,
    path,
    input: {
      mode: "reachability",
      result_limit: 10,
      start_uuids: [UUIDS[0]],
    },
  });
  assert.equal(reachability.algorithm, "transitive_closure");
  assert.equal(reachability.truncated, false);

  const pathResult = await exploreGraph({
    GraphForge,
    tableFromIPC: (value) => value,
    path,
    input: {
      mode: "path",
      result_limit: 10,
      start_uuids: [UUIDS[0]],
      target_uuid: UUIDS[1],
    },
  });
  assert.equal(pathResult.algorithm, "dijkstra");
  assert.equal(pathResult.target_uuid, UUIDS[1]);
  const prepare = GraphForge.instance.calls.find(
    ([name]) => name === "preparePathsInvocation",
  );
  assert.equal(prepare[1].by, "dijkstra");
  assert.equal(prepare[1].target, UUIDS[1]);
});

test("explore opens only the graph capability surface", async () => {
  const path = realpathSync(
    mkdtempSync(join(tmpdir(), "graphforge-explore-cap-")),
  );
  writeFileSync(join(path, "FORMAT"), "graphforge-project/v1\n");
  const calls = [];
  class GraphForge {
    constructor(openedPath) {
      assert.equal(openedPath, path);
    }
    async projectCapabilities() {
      calls.push("projectCapabilities");
      return table([
        { capability_id: "graph", capability_version: 1, status: "supported" },
      ]);
    }
    preparePathsInvocation(...args) {
      calls.push(["prepare", args]);
      return {
        algorithm: args[2],
        fingerprint: "aa".repeat(32),
        verb: "paths",
      };
    }
    invokeDescriptor(descriptor) {
      calls.push(["invoke", descriptor.algorithm]);
      return table([]);
    }
    listAssertions() {
      calls.push("listAssertions");
      throw new Error("knowledge must stay closed");
    }
    close() {
      calls.push("close");
    }
  }
  const result = await exploreGraph({
    GraphForge,
    tableFromIPC: (value) => value,
    path,
    input: {
      depth: 1,
      mode: "traversal",
      result_limit: 5,
      start_uuids: [UUIDS[0]],
    },
  });
  assert.equal(result.mode, "traversal");
  assert.ok(calls.includes("projectCapabilities"));
  assert.ok(!calls.includes("listAssertions"));
  assert.ok(calls.includes("close"));
});
