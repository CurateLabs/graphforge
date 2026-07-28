import assert from "node:assert/strict";
import { mkdirSync, writeFileSync } from "node:fs";
import { mkdtemp, realpath, symlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { bootstrapProject, buildKnowledge } from "../workflows/index.js";

const table = (rows) => {
  const fields = [...new Set(rows.flatMap((row) => Object.keys(row)))];
  return {
    getChild: (name) => ({ get: (index) => rows[index]?.[name] }),
    numRows: rows.length,
    schema: { fields: fields.map((name) => ({ name })) },
  };
};
const tableFromIPC = (value) => value;
const projects = new Map();
const uuids = Array.from(
  { length: 40 },
  (_, index) =>
    `018f0f4e-7b8c-7000-8000-${String(index + 1).padStart(12, "0")}`,
);

class GraphForge {
  constructor(path) {
    this.openedPath = path;
    if (!projects.has(path)) {
      mkdirSync(path, { recursive: true });
      writeFileSync(join(path, "FORMAT"), "graphforge-project/v1\n");
      projects.set(path, {
        capabilities: new Map([
          ["graph", 1],
          ["workspace", 1],
        ]),
        calls: [],
        edges: [],
        marker: undefined,
        nodes: [],
        ontologyMode: "exploratory",
      });
    }
    this.state = projects.get(path);
    this.ontologyMode = this.state.ontologyMode;
  }

  async projectCapabilities() {
    return table(
      [...this.state.capabilities].map(
        ([capability_id, capability_version]) => ({
          capability_id,
          capability_version,
          status: "supported",
        }),
      ),
    );
  }

  execute() {
    return table(this.state.marker ? [{ node_uuid: this.state.marker }] : []);
  }

  addNode(label, properties) {
    const uuid = uuids[this.state.nodes.length];
    this.state.nodes.push({ label, properties, uuid });
    if (label === "GraphForgeBootstrap") this.state.marker = uuid;
    return { uuid };
  }

  addEdge(source, type, target, properties) {
    const uuid = uuids[20 + this.state.edges.length];
    this.state.edges.push({
      properties,
      source: source.uuid,
      target: target.uuid,
      type,
      uuid,
    });
    return { uuid };
  }

  async enableCapability(request) {
    this.state.capabilities.set(
      request.capabilityId,
      request.capabilityVersion,
    );
    this.state.calls.push(["enable", request]);
    return table([
      { capability_id: request.capabilityId, capability_version: 1 },
    ]);
  }

  async createAssertionWithEvidence(request) {
    this.state.calls.push(["assertion_with_evidence", request]);
    if (this.state.failAssertion)
      throw Object.assign(new Error("private"), { code: "GF_WRITE" });
    this.state.assertion = request;
    return table([
      {
        assertion_uuid: request.assertionUuid,
        provenance_uuid: uuids[30],
      },
    ]);
  }

  async createAssertion(request) {
    this.state.calls.push(["assertion", request]);
    this.state.assertion = request;
    return table([
      { assertion_uuid: request.assertionUuid, provenance_uuid: uuids[30] },
    ]);
  }

  async assessConfidence(request) {
    this.state.calls.push(["confidence", request]);
    return table([
      { confidence_uuid: request.confidenceUuid, value: request.value },
    ]);
  }

  async recordReasoning(request) {
    this.state.calls.push(["reasoning", request]);
    return table([{ reasoning_uuid: request.reasoningUuid }]);
  }

  async recordAssertionStatus(request) {
    this.state.calls.push(["status", request]);
    return table([
      { assertion_uuid: request.assertionUuid, status: request.status },
    ]);
  }

  loadOntology(path) {
    this.state.ontologyPath = path;
    this.state.ontologyMode = "advisory";
    this.ontologyMode = "advisory";
  }

  close() {}
}

test("bootstrap creates, reopens, queries, and replays idempotently", async () => {
  const root = await realpath(
    await mkdtemp(join(tmpdir(), "gf-agent-bootstrap-")),
  );
  const path = join(root, "project");
  const first = await bootstrapProject({ GraphForge, path, tableFromIPC });
  const replay = await bootstrapProject({ GraphForge, path, tableFromIPC });

  assert.equal(first.created, true);
  assert.equal(replay.created, false);
  assert.equal(replay.marker_uuid, first.marker_uuid);
  assert.equal(replay.rows.length, 1);
  assert.equal(replay.ontology_mode, "exploratory");
  assert.equal(projects.get(path).nodes.length, 1);
});

test("bootstrap creates missing parent directories", async () => {
  const root = await realpath(
    await mkdtemp(join(tmpdir(), "gf-agent-bootstrap-")),
  );
  const path = join(root, "nested", "project");
  const result = await bootstrapProject({ GraphForge, path, tableFromIPC });

  assert.equal(result.created, true);
  assert.equal(projects.has(path), true);
});

test("bootstrap rejects a symlinked ontology before native loading", async () => {
  const root = await realpath(
    await mkdtemp(join(tmpdir(), "gf-agent-bootstrap-")),
  );
  const target = join(root, "ontology.yaml");
  writeFileSync(target, "classes: []\n");
  const ontologyPath = join(root, "ontology-link.yaml");
  await symlink(target, ontologyPath);
  const path = join(root, "project");

  await assert.rejects(
    bootstrapProject({
      GraphForge,
      ontologyMode: "advisory",
      ontologyPath,
      path,
      tableFromIPC,
    }),
    ({ code }) => code === "GF_AGENT_INVALID_PROJECT_PATH",
  );
  assert.equal(projects.get(path).ontologyPath, undefined);
});

test("bootstrap persists advisory ontology mode across reopen", async () => {
  const root = await realpath(
    await mkdtemp(join(tmpdir(), "gf-agent-bootstrap-")),
  );
  const ontologyPath = join(root, "ontology.yaml");
  writeFileSync(ontologyPath, "classes: []\n");
  const path = join(root, "project");
  const result = await bootstrapProject({
    GraphForge,
    ontologyMode: "advisory",
    ontologyPath,
    path,
    tableFromIPC,
  });

  assert.equal(result.ontology_mode, "advisory");
  assert.equal(projects.get(path).ontologyPath, ontologyPath);
});

test("bootstrap reports structured ontology conflicts without adding a marker", async () => {
  const root = await realpath(
    await mkdtemp(join(tmpdir(), "gf-agent-bootstrap-")),
  );
  const path = join(root, "project");
  await assert.rejects(
    bootstrapProject({
      GraphForge,
      ontologyMode: "strict",
      path,
      tableFromIPC,
    }),
    ({ code, message }) =>
      code === "GF_AGENT_ONTOLOGY_MODE_CONFLICT" && !message.includes(path),
  );
  assert.equal(projects.get(path).nodes.length, 0);
});

function buildInput({ m21 = false } = {}) {
  return {
    actor_uuid: uuids[1],
    assertion: {
      assertion_uuid: uuids[2],
      claim: "Ada knows Grace",
      graph_refs: [
        { graph_kind: "node", key: "ada", ordinal: 0, role: "subject" },
        { graph_kind: "node", key: "grace", ordinal: 0, role: "object" },
      ],
      operation_uuid: uuids[3],
    },
    capability_operation_uuids: {
      epistemic: uuids[4],
      knowledge: uuids[5],
      provenance: uuids[6],
    },
    confidence: {
      confidence_uuid: uuids[7],
      operation_uuid: uuids[8],
      policy: "explicit",
      value: 0.8,
    },
    edges: [
      {
        key: "knows",
        properties: { confidence: 0.25 },
        source_key: "ada",
        target_key: "grace",
        type: "KNOWS",
      },
    ],
    evidence: [
      {
        evidence_uuid: uuids[9],
        role: "supports",
        source_key: "ada",
        source_kind: "graph_node",
        weight: 0.9,
      },
    ],
    nodes: [
      {
        key: "ada",
        label: "Person",
        properties: { confidence: "domain", name: "Ada" },
      },
      { key: "grace", label: "Person", properties: { name: "Grace" } },
    ],
    reasoning: m21
      ? {
          content: "explicit evidence interpretation",
          content_format: "text/plain",
          kind: "evidence_interpretation",
          operation_uuid: uuids[10],
          reasoning_uuid: uuids[11],
        }
      : undefined,
    status: m21
      ? {
          operation_uuid: uuids[12],
          status: "hypothesis",
          status_event_uuid: uuids[13],
        }
      : undefined,
  };
}

test("build knowledge preserves domain confidence and leaves M20 statusless", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "gf-agent-build-")));
  const path = join(root, "project");
  await bootstrapProject({ GraphForge, path, tableFromIPC });
  const result = await buildKnowledge({
    GraphForge,
    input: buildInput(),
    path,
    tableFromIPC,
  });
  const state = projects.get(path);

  assert.equal(result.nodes.length, 2);
  assert.equal(result.edges.length, 1);
  assert.equal(result.evidence_count, 1);
  assert.deepEqual(
    result.capabilities.map(({ capability_id }) => capability_id),
    ["graph", "workspace", "provenance", "knowledge"],
  );
  assert.equal(result.confidence[0].value, 0.8);
  assert.equal(result.reasoning.length, 0);
  assert.equal(result.status.length, 0);
  assert.equal(
    state.nodes.find(({ label }) => label === "Person").properties.confidence,
    "domain",
  );
  assert.equal(
    state.calls.some(([name]) => name === "status"),
    false,
  );
  assert.equal(
    state.calls.filter(([name]) => name === "assertion_with_evidence").length,
    1,
  );
});

test("build knowledge rejects missing baseline M20 records", async () => {
  for (const mutate of [
    (input) => {
      input.evidence = [];
    },
    (input) => {
      input.confidence = undefined;
    },
    (input) => {
      input.assertion.graph_refs = undefined;
    },
  ]) {
    const input = buildInput();
    mutate(input);
    await assert.rejects(
      buildKnowledge({ GraphForge, input, path: "/unused", tableFromIPC }),
      ({ code }) => code === "GF_AGENT_BUILD_CONFIGURATION",
    );
  }
});

test("build knowledge rejects duplicate edge keys before opening the project", async () => {
  const input = buildInput();
  input.edges.push({ ...input.edges[0] });
  await assert.rejects(
    buildKnowledge({ GraphForge, input, path: "/unused", tableFromIPC }),
    ({ code, message }) =>
      code === "GF_AGENT_BUILD_CONFLICT" &&
      message === "edge keys must be unique",
  );
});

test("build knowledge rejects duplicate node keys before opening the project", async () => {
  const input = buildInput();
  input.nodes.push({ ...input.nodes[0] });
  await assert.rejects(
    buildKnowledge({ GraphForge, input, path: "/unused", tableFromIPC }),
    ({ code, message }) =>
      code === "GF_AGENT_BUILD_CONFLICT" &&
      message === "node keys must be unique",
  );
});

test("build knowledge appends required confidence and only explicit M21 records", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "gf-agent-build-")));
  const path = join(root, "project");
  await bootstrapProject({ GraphForge, path, tableFromIPC });
  const result = await buildKnowledge({
    GraphForge,
    input: buildInput({ m21: true }),
    path,
    tableFromIPC,
  });

  assert.equal(result.confidence[0].value, 0.8);
  assert.equal(result.reasoning.length, 1);
  assert.equal(result.status[0].status, "hypothesis");
  assert.deepEqual(
    projects
      .get(path)
      .calls.filter(([name]) =>
        ["confidence", "reasoning", "status"].includes(name),
      )
      .map(([name]) => name),
    ["confidence", "reasoning", "status"],
  );
});

test("build knowledge forwards conservative-min confidence inputs", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "gf-agent-build-")));
  const path = join(root, "project");
  await bootstrapProject({ GraphForge, path, tableFromIPC });
  const input = buildInput({ m21: true });
  input.confidence = {
    confidence_uuid: uuids[7],
    input_confidence_uuids: [uuids[14]],
    operation_uuid: uuids[8],
    policy: "conservative_min",
  };
  await buildKnowledge({ GraphForge, input, path, tableFromIPC });

  assert.deepEqual(
    projects.get(path).calls.find(([name]) => name === "confidence")[1]
      .inputConfidenceUuids,
    [uuids[14]],
  );
});

test("a failed atomic assertion bundle preserves its previous complete ledger", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "gf-agent-build-")));
  const path = join(root, "project");
  await bootstrapProject({ GraphForge, path, tableFromIPC });
  const state = projects.get(path);
  state.assertion = { preserved: true };
  state.failAssertion = true;

  await assert.rejects(
    buildKnowledge({ GraphForge, input: buildInput(), path, tableFromIPC }),
    ({ code, message }) =>
      code === "GF_WRITE" && message === "GraphForge operation failed",
  );
  assert.deepEqual(state.assertion, { preserved: true });
});
