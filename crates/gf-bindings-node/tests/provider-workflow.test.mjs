import assert from "node:assert/strict";
import { once } from "node:events";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { Worker } from "node:worker_threads";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

const workerSource = String.raw`
  const http = require("node:http");
  const { parentPort } = require("node:worker_threads");
  let calls = 0;
  const server = http.createServer((request, response) => {
    const chunks = [];
    request.on("data", (chunk) => chunks.push(chunk));
    request.on("end", () => {
      try {
        if (request.headers.authorization !== "Bearer test-secret") {
          throw new Error("missing credential");
        }
        const payload = JSON.parse(Buffer.concat(chunks));
        let body;
        if (calls === 0) {
          if (request.url !== "/api/v1/embeddings" || payload.input.length !== 2) {
            throw new Error("unexpected document request");
          }
          body = { model: "vendor/model", data: [
            { index: 0, embedding: [1, 0] },
            { index: 1, embedding: [0, 1] },
          ] };
        } else if (calls === 1 || calls === 2) {
          if (request.url !== "/api/v1/embeddings" || typeof payload.input !== "string") {
            throw new Error("unexpected query request");
          }
          body = { model: "vendor/model", data: [{ index: 0, embedding: [1, 0] }] };
        } else if (calls === 3) {
          if (request.url !== "/api/v1/rerank" || payload.documents.length !== 2) {
            throw new Error("unexpected rerank request");
          }
          body = { model: "vendor/model", results: [
            { index: 0, relevance_score: 0.1 },
            { index: 1, relevance_score: 0.9 },
          ] };
        } else {
          throw new Error("unexpected extra provider request");
        }
        calls += 1;
        const encoded = JSON.stringify(body);
        response.writeHead(200, { "content-type": "application/json", "content-length": Buffer.byteLength(encoded) });
        response.end(encoded);
      } catch (error) {
        parentPort.postMessage({ error: String(error) });
        response.destroy(error);
      }
    });
  });
  server.listen(0, "127.0.0.1", () => {
    parentPort.postMessage({ origin: "http://127.0.0.1:" + server.address().port });
  });
  parentPort.on("message", (message) => {
    if (message === "count") parentPort.postMessage({ calls });
    if (message === "close") server.close(() => process.exit(0));
  });
`;

function expectValidation(fragment, call) {
  assert.throws(
    call,
    (error) =>
      error.code === "ValidationError" &&
      error.message.includes(fragment) &&
      !error.message.includes("test-secret"),
  );
}

function expectError(code, fragment, call) {
  assert.throws(
    call,
    (error) =>
      error.code === code &&
      error.message.includes(fragment) &&
      !error.message.includes("test-secret"),
  );
}

function firstColumnValues(table) {
  return Array.from(table.getChild("node_uuid"), (value) =>
    Buffer.from(value).toString("hex"),
  );
}

test("configured provider indexing, semantic find, rerank, and warnings stay in parity", async () => {
  const worker = new Worker(workerSource, { eval: true });
  const [{ origin }] = await once(worker, "message");
  const project = mkdtempSync(join(tmpdir(), "gf-node-provider-workflow-"));
  const forge = new GraphForge(project);
  try {
    const first = forge.addNode("Paper", { title: "First" });
    const second = forge.addNode("Paper", { title: "Second" });
    expectValidation("origin", () =>
      forge.configureOpenrouter("test-secret", {
        origin: "https://example.com/path",
        model: "vendor/model",
      }),
    );
    expectError("ExecutionError", "authentication", () =>
      forge.configureOpenrouter("", { origin, model: "vendor/model" }),
    );
    expectValidation("provider model", () =>
      forge.configureOpenrouter("test-secret", { origin, model: " " }),
    );
    expectValidation("non-zero", () =>
      forge.configureOpenrouter("test-secret", {
        origin,
        model: "vendor/model",
        maxInputTokens: 0,
      }),
    );
    forge.configureOpenrouter("test-secret", {
      origin,
      model: "vendor/model",
      revision: "revision",
      capabilities: [
        "document_embeddings",
        "query_embeddings",
        "candidate_reranking",
      ],
      maxInputTokens: 10_000,
    });

    const request = {
      name: "semantic",
      label: "Paper",
      properties: ["title"],
      dimensions: 2,
    };
    const inspection = forge.inspectProviderEmbeddingPlan(request);
    assert.equal(inspection.provider, "openrouter");
    assert.equal(inspection.model, "vendor/model");
    assert.equal(inspection.tokenCountClass, "approximate");
    assert.equal(inspection.modelInputTokens, 10_000);
    assert.deepEqual(inspection.properties, ["title"]);
    assert.equal(inspection.selectedNodes, 2);
    assert.ok(inspection.inputTokens > 0);
    assert.ok(inspection.batches.length > 0);
    assert.deepEqual(inspection.requestLimits, {
      items: 1_024,
      inputBytes: 8 * 1024 * 1024,
      inputTokens: 1_000_000,
      outputValues: 16_777_216,
      providerCalls: 128,
    });
    assert.equal(inspection.executionLimits.providerCalls, 128);
    assert.equal(inspection.executionLimits.retries, 2);
    assert.equal(inspection.executionLimits.timeoutMillis, 30_000);
    assert.doesNotMatch(
      JSON.stringify(inspection, (_key, value) =>
        typeof value === "bigint" ? value.toString() : value,
      ),
      /First|Second|test-secret/,
    );

    const published = forge.publishProviderEmbeddings(request);
    assert.deepEqual(published.producer, {
      kind: "remote",
      model: "vendor/model",
      provider: "openrouter",
      responseContractVersion: "v1",
      revision: "revision",
    });

    const baseline = forge.find(
      undefined,
      "Paper",
      [1, 0],
      undefined,
      undefined,
      2,
      "semantic",
      false,
      undefined,
      true,
    );
    const advisoryPromise = once(process, "warning");
    const advisory = forge.find(
      undefined,
      "Paper",
      [1, 0],
      undefined,
      undefined,
      2,
      "semantic",
    );
    const [advisoryWarning] = await advisoryPromise;
    assert.match(advisoryWarning.message, /reranker/);
    assert.deepEqual(advisory, baseline);

    const baselineTable = tableFromIPC(baseline);
    const semantic = tableFromIPC(
      forge.find(
        undefined,
        "Paper",
        undefined,
        undefined,
        "meaning",
        2,
        "semantic",
        false,
        undefined,
        true,
      ),
    );
    assert.deepEqual(
      semantic.schema.fields.map((field) => field.name),
      baselineTable.schema.fields.map((field) => field.name),
    );
    assert.deepEqual(
      firstColumnValues(semantic),
      firstColumnValues(baselineTable),
    );

    const reranked = tableFromIPC(
      forge.find(
        undefined,
        "Paper",
        undefined,
        undefined,
        "meaning",
        2,
        "semantic",
        false,
        {
          query: "meaning",
          properties: ["title"],
          candidateDepth: 2,
          failurePolicy: "error",
        },
      ),
    );
    assert.deepEqual(
      reranked.schema.fields.map((field) => field.name),
      semantic.schema.fields.map((field) => field.name),
    );
    assert.deepEqual(
      new Set(firstColumnValues(reranked)),
      new Set([
        first.uuid.replaceAll("-", ""),
        second.uuid.replaceAll("-", ""),
      ]),
    );
    assert.equal(
      firstColumnValues(reranked)[0],
      firstColumnValues(semantic)[1],
    );

    expectValidation("failure policy", () =>
      forge.find(
        undefined,
        "Paper",
        undefined,
        undefined,
        "meaning",
        2,
        "semantic",
        false,
        {
          query: "meaning",
          properties: ["title"],
          candidateDepth: 2,
          failurePolicy: "unknown",
        },
      ),
    );
    expectValidation("candidate_depth", () =>
      forge.find(
        undefined,
        "Paper",
        undefined,
        undefined,
        "meaning",
        2,
        "semantic",
        false,
        {
          query: "meaning",
          properties: ["title"],
          candidateDepth: 1,
        },
      ),
    );

    forge.setEmbeddingRefreshProjectPolicy(false, 250, 2);
    forge.addNode("Paper", { title: "text that is too large" });
    const stalePromise = once(process, "warning");
    const stale = tableFromIPC(
      forge.find(
        undefined,
        "Paper",
        [1, 0],
        undefined,
        undefined,
        2,
        "semantic",
        true,
        undefined,
        true,
      ),
    );
    const [staleWarning] = await stalePromise;
    assert.equal(stale.numRows, 2);
    assert.match(staleWarning.message, /stale/i);

    forge.configureOpenrouter("test-secret", {
      origin,
      model: "vendor/model",
      revision: "revision",
      maxInputTokens: 2,
    });
    assert.throws(
      () =>
        forge.inspectProviderEmbeddingPlan({
          name: "too-small",
          label: "Paper",
          properties: ["title"],
          dimensions: 2,
        }),
      (error) =>
        error.code === "ExecutionError" &&
        /resource/.test(error.message) &&
        !error.message.includes("test-secret"),
    );
    expectValidation("unknown provider capability", () =>
      forge.configureOpenrouter("test-secret", {
        origin,
        model: "vendor/model",
        capabilities: ["unknown"],
      }),
    );

    worker.postMessage("count");
    const [{ calls }] = await once(worker, "message");
    assert.equal(calls, 4);
  } finally {
    forge.close();
    worker.postMessage("close");
    await once(worker, "exit");
    rmSync(project, { recursive: true, force: true });
  }
});
