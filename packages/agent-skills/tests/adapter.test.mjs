import assert from "node:assert/strict";
import {
  mkdtemp,
  mkdir,
  readFile,
  realpath,
  symlink,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import {
  AgentAdapterError,
  capabilitiesFromTable,
  discoverProject,
  normalizeGraphForgeError,
  openProject,
  requestSubprocess,
  requireCapabilities,
  stableJson,
  tableToJson,
  uuidToString,
} from "../adapter/index.js";

const vector = (values) => ({ get: (row) => values[row] });
const table = (columns) => ({
  getChild: (name) => vector(columns[name]),
  numRows: Object.values(columns)[0].length,
  schema: { fields: Object.keys(columns).map((name) => ({ name })) },
});

async function project(root, name) {
  const path = join(root, name);
  await mkdir(path);
  await writeFile(join(path, "FORMAT"), "graphforge-project/v1\n");
  return realpath(path);
}

test("discovers exactly one supported real project without mutation", async () => {
  const root = await mkdtemp(join(tmpdir(), "gf-adapter-"));
  const supported = await project(root, "supported");
  const unsupported = join(root, "unsupported");
  await mkdir(unsupported);
  await writeFile(join(unsupported, "FORMAT"), "graphforge-project/v2\n");
  assert.equal(
    await discoverProject({ candidates: [unsupported, supported] }),
    supported,
  );

  const link = join(root, "linked");
  await symlink(supported, link);
  await assert.rejects(
    discoverProject({ candidates: [link] }),
    ({ code }) => code === "GF_AGENT_PROJECT_NOT_FOUND",
  );
  assert.equal(
    await readFile(join(unsupported, "FORMAT"), "utf8"),
    "graphforge-project/v2\n",
  );
});

test("rejects missing and ambiguous discovery deterministically", async () => {
  const root = await mkdtemp(join(tmpdir(), "gf-adapter-"));
  await assert.rejects(
    discoverProject({ candidates: [root] }),
    ({ code }) => code === "GF_AGENT_PROJECT_NOT_FOUND",
  );
  const first = await project(root, "a");
  const second = await project(root, "b");
  await assert.rejects(
    discoverProject({ candidates: [second, first] }),
    (error) =>
      error.code === "GF_AGENT_PROJECT_AMBIGUOUS" &&
      error.details.candidate_count === 2 &&
      !JSON.stringify(error).includes(root),
  );
});

test("rejects traversal, control characters, and symlinks without path disclosure", async () => {
  const root = await mkdtemp(join(tmpdir(), "gf-adapter-secret-"));
  const outside = await project(root, "private-project");
  const discoveryRoot = join(root, "discovery");
  await mkdir(discoveryRoot);

  for (const candidate of ["../private-project", "bad\0path", "bad\npath"]) {
    await assert.rejects(
      discoverProject({ candidates: [candidate], cwd: discoveryRoot }),
      (error) =>
        error.code === "GF_AGENT_INVALID_PROJECT_PATH" &&
        error.message ===
          (candidate.startsWith("..")
            ? "project candidates must not contain parent traversal"
            : "project candidates must be bounded paths without control characters") &&
        !JSON.stringify(error).includes(root) &&
        !JSON.stringify(error).includes(JSON.stringify(candidate).slice(1, -1)),
    );
  }

  const linked = join(discoveryRoot, "linked");
  await symlink(outside, linked);
  await assert.rejects(
    discoverProject({ candidates: [linked] }),
    (error) =>
      error.code === "GF_AGENT_PROJECT_NOT_FOUND" &&
      !JSON.stringify(error).includes(root),
  );
});

test("opens only through shipped surfaces and checks exact capabilities", async () => {
  const root = await mkdtemp(join(tmpdir(), "gf-adapter-"));
  const path = await project(root, "project");
  const capabilityTable = table({
    capability_id: ["graph", "workspace"],
    capability_version: [1, 1],
    status: ["supported", "supported"],
  });
  class GraphForge {
    constructor(opened) {
      this.opened = opened;
    }
    async projectCapabilities() {
      return Buffer.from("ipc");
    }
  }
  const opened = await openProject({
    GraphForge,
    path,
    requiredCapabilities: { graph: 1, workspace: 1 },
    tableFromIPC: () => capabilityTable,
  });
  assert.equal(opened.graph.opened, path);
  assert.deepEqual(opened.capabilities, {
    graph: { status: "supported", version: 1 },
    workspace: { status: "supported", version: 1 },
  });
});

test("does not construct GraphForge for unsupported project formats", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "gf-adapter-")));
  await writeFile(join(root, "FORMAT"), "graphforge-project/v2\n");
  let constructed = false;
  class GraphForge {
    constructor() {
      constructed = true;
    }
  }
  await assert.rejects(
    openProject({ GraphForge, path: root, tableFromIPC: () => null }),
    ({ code }) => code === "GF_AGENT_PROJECT_UNSUPPORTED",
  );
  assert.equal(constructed, false);
});

test("fails closed for future capability versions and normalizes errors", () => {
  const actual = capabilitiesFromTable(
    table({
      capability_id: ["graph"],
      capability_version: [2],
      status: ["unsupported_future"],
    }),
  );
  assert.throws(
    () => requireCapabilities(actual, { graph: 1 }),
    (error) =>
      error.code === "GF_AGENT_CAPABILITY_UNSUPPORTED" &&
      error.details.actual_status === "unsupported_future",
  );
  assert.throws(
    () => requireCapabilities(actual, { graph: 0 }),
    ({ code }) => code === "GF_AGENT_ADAPTER_CONFIGURATION",
  );
  assert.throws(
    () =>
      capabilitiesFromTable(
        table({
          capability_id: ["graph", "graph"],
          capability_version: [1, 1],
          status: ["supported", "supported"],
        }),
      ),
    ({ code }) => code === "GF_AGENT_INVALID_CAPABILITY_TABLE",
  );
  const secret = `${process.cwd()}/SECRET_TOKEN_DO_NOT_ECHO`;
  assert.deepEqual(
    normalizeGraphForgeError({
      code: "GF_PROJECT_CORRUPT",
      message: secret,
    }).toJSON(),
    {
      code: "GF_PROJECT_CORRUPT",
      contract_version: 1,
      details: {},
      message: "GraphForge operation failed",
    },
  );
  assert.equal(
    JSON.stringify(
      normalizeGraphForgeError({ code: secret, message: secret }),
    ).includes(secret),
    false,
  );

  const cyclicDetails = {};
  cyclicDetails.self = cyclicDetails;
  assert.deepEqual(
    new AgentAdapterError("GF_AGENT_TEST", "fixed", cyclicDetails).details,
    {
      details_omitted: true,
    },
  );
});

test("serializes UUID, Arrow rows, bigint, and JSON deterministically", () => {
  const uuid = Uint8Array.from({ length: 16 }, (_, index) => index);
  assert.equal(uuidToString(uuid), "00010203-0405-0607-0809-0a0b0c0d0e0f");
  assert.deepEqual(
    tableToJson(
      table({ node_uuid: [uuid], count: [3n], path: [new Set([uuid])] }),
    ),
    [
      {
        node_uuid: "00010203-0405-0607-0809-0a0b0c0d0e0f",
        count: "3",
        path: ["00010203-0405-0607-0809-0a0b0c0d0e0f"],
      },
    ],
  );
  assert.equal(
    stableJson({ z: 1, a: { d: 2, c: 3 } }),
    '{"a":{"c":3,"d":2},"z":1}\n',
  );
  assert.equal(
    stableJson({ count: 3n, uuid }),
    '{"count":"3","uuid":"00010203-0405-0607-0809-0a0b0c0d0e0f"}\n',
  );
});

test("rejects cyclic and over-budget values without reflecting content", () => {
  const cyclic = { safe: true };
  cyclic.self = cyclic;
  assert.throws(
    () => stableJson(cyclic),
    (error) =>
      error.code === "GF_AGENT_CYCLIC_VALUE" &&
      !JSON.stringify(error).includes("safe"),
  );

  const secret = "SECRET_TOKEN_DO_NOT_ECHO";
  assert.throws(
    () => stableJson({ value: `${secret}${"x".repeat(4096)}` }),
    (error) =>
      error.code === "GF_AGENT_VALUE_BUDGET_EXCEEDED" &&
      !JSON.stringify(error).includes(secret),
  );
  assert.throws(
    () => stableJson(Array.from({ length: 4097 }, (_, index) => index)),
    ({ code }) => code === "GF_AGENT_VALUE_BUDGET_EXCEEDED",
  );
  let deep = {};
  for (let depth = 0; depth < 18; depth += 1) deep = { deep };
  assert.throws(
    () => stableJson(deep),
    ({ code }) => code === "GF_AGENT_VALUE_BUDGET_EXCEEDED",
  );
  assert.throws(
    () => stableJson(new Uint8Array(4097)),
    ({ code }) => code === "GF_AGENT_VALUE_BUDGET_EXCEEDED",
  );
  assert.equal(stableJson({ b: 1, _c: 3, a: 2 }), '{"_c":3,"a":2,"b":1}\n');
  assert.equal(
    stableJson(new Set(["b", "a"])),
    stableJson(new Set(["a", "b"])),
  );
  assert.equal(
    stableJson(
      new Map([
        ["b", 2],
        ["a", 1],
      ]),
    ),
    stableJson(
      new Map([
        ["a", 1],
        ["b", 2],
      ]),
    ),
  );
});

test("rejects subprocess requests without exposing an execution surface", async () => {
  assert.throws(
    () =>
      requestSubprocess({
        command: "printf",
        args: ["SECRET_TOKEN_DO_NOT_ECHO"],
      }),
    (error) =>
      error.code === "GF_AGENT_SUBPROCESS_UNSUPPORTED" &&
      !JSON.stringify(error).includes("SECRET_TOKEN_DO_NOT_ECHO"),
  );
  const source = await readFile(
    new URL("../adapter/index.js", import.meta.url),
    "utf8",
  );
  assert.equal(source.includes("node:child_process"), false);
});
