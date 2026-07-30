// Deterministic Node native concurrency parity against the Rust contract (#2416).

import assert from "node:assert/strict";
import { existsSync, mkdtempSync, readdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { Worker } from "node:worker_threads";
import { tableFromIPC } from "apache-arrow";
import {
  GraphForge,
  testAcquireWriterHold,
  testReleaseWriterHold,
} from "../index.js";

const DEADLINE_MS = 10_000;
const QUERY = "MATCH (n:Person) RETURN n.name AS name ORDER BY name";
const CHECKPOINT_KEY = "018f0f4e-7b8c-7000-8000-000000002416";

function seed(path) {
  const forge = new GraphForge(path);
  forge.execute(
    "CREATE " +
      "(alice:Person {name:'Alice'}), " +
      "(bob:Person {name:'Bob'}), " +
      "(carol:Person {name:'Carol'}), " +
      "(alice)-[:KNOWS]->(bob), " +
      "(bob)-[:KNOWS]->(carol)",
  );
  forge.close();
}

function names(forge) {
  return [...tableFromIPC(forge.execute(QUERY)).getChild("name").toArray()];
}

function reopenedNames(path) {
  const forge = new GraphForge(path);
  try {
    return names(forge);
  } finally {
    forge.close();
  }
}

async function namesAsync(forge) {
  const ipc = await forge.plan(QUERY).collectIpc();
  return [...tableFromIPC(ipc).getChild("name").toArray()];
}

function transactionNames(root) {
  const directory = join(root, "transactions");
  if (!existsSync(directory)) {
    return [];
  }
  return readdirSync(directory).sort();
}

function optimisticWorker(project, operationUuid, nodeUuid, name) {
  return new Promise((resolve, reject) => {
    const worker = new Worker(
      new URL("./fixtures/write-mode-worker.mjs", import.meta.url),
      { workerData: { name, nodeUuid, operationUuid, project } },
    );
    let settled = false;
    const deadline = AbortSignal.timeout(DEADLINE_MS);
    const finish = (complete, value) => {
      if (settled) return;
      settled = true;
      deadline.removeEventListener("abort", onTimeout);
      void worker.terminate();
      complete(value);
    };
    const onTimeout = () =>
      finish(reject, new Error("optimistic worker timed out"));
    deadline.addEventListener("abort", onTimeout, { once: true });
    worker.once("message", (message) => {
      if (message.ok) finish(resolve, message);
      else finish(reject, Object.assign(new Error(message.message), message));
    });
    worker.once("error", (error) => finish(reject, error));
    worker.once("exit", (code) => {
      if (!settled) {
        finish(
          reject,
          new Error(`optimistic worker exited ${code} before responding`),
        );
      }
    });
  });
}

test("write-mode options validate and optimistic agents publish exactly once", async () => {
  for (const writeMode of [
    "single_writer",
    "queued_writer",
    "optimistic_multi_writer",
  ]) {
    const root = mkdtempSync(join(tmpdir(), "gf-node-write-mode-"));
    try {
      const forge = new GraphForge(root, {
        maxRebaseAttempts: 4,
        writeMode,
        writeQueueCapacity: 8,
      });
      forge.execute("CREATE (:Person {name:'mode'})");
      forge.close();
      assert.deepEqual(reopenedNames(root), ["mode"]);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  }

  for (const options of [
    { writeMode: "server" },
    { writeQueueCapacity: 0 },
    { writeQueueCapacity: -1 },
    { maxRebaseAttempts: -1 },
    { maxRebaseAttempts: 33 },
  ]) {
    assert.throws(
      () => new GraphForge(undefined, options),
      (error) => error.code === "ValidationError",
    );
  }

  const root = mkdtempSync(join(tmpdir(), "gf-node-optimistic-agents-"));
  const operations = [
    "018f0f4e-7b8c-7000-8000-000000002146",
    "018f0f4e-7b8c-7000-8000-000000002147",
  ];
  try {
    new GraphForge(root).close();
    const results = await Promise.all([
      optimisticWorker(
        root,
        operations[0],
        "018f0f4e-7b8c-7000-8000-000000002148",
        "agent-0",
      ),
      optimisticWorker(
        root,
        operations[1],
        "018f0f4e-7b8c-7000-8000-000000002149",
        "agent-1",
      ),
    ]);
    assert.deepEqual(
      results.map(({ operationUuid }) => operationUuid).sort(),
      [...operations].sort(),
    );
    assert.deepEqual(reopenedNames(root), ["agent-0", "agent-1"]);

    const before = reopenedNames(root);
    const conflicting = new GraphForge(root, {
      writeMode: "optimistic_multi_writer",
    });
    assert.throws(
      () =>
        conflicting.publishCompositeTransaction({
          contractVersion: 1,
          operationUuid: operations[0],
          graphMutations: [
            {
              kind: "create_node",
              label: "Person",
              nodeUuid: "018f0f4e-7b8c-7000-8000-000000002150",
              properties: { name: "conflict" },
            },
          ],
        }),
      (error) => error.code === "GF_IDEMPOTENCY_CONFLICT",
    );
    conflicting.close();
    assert.deepEqual(reopenedNames(root), before);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test(
  "independent and same-instance concurrent reads are ordered and equal",
  { timeout: DEADLINE_MS },
  async () => {
    const first = mkdtempSync(join(tmpdir(), "gf-node-conc-a-"));
    const second = mkdtempSync(join(tmpdir(), "gf-node-conc-b-"));
    try {
      seed(first);
      seed(second);
      const left = new GraphForge(first);
      const right = new GraphForge(second);
      const independent = await Promise.all([
        namesAsync(left),
        namesAsync(right),
      ]);
      assert.deepEqual(independent[0], ["Alice", "Bob", "Carol"]);
      assert.deepEqual(independent[1], ["Alice", "Bob", "Carol"]);

      const shared = new GraphForge(first);
      const sameInstance = await Promise.all(
        Array.from({ length: 4 }, () => namesAsync(shared)),
      );
      for (const row of sameInstance) {
        assert.deepEqual(row, ["Alice", "Bob", "Carol"]);
      }
    } finally {
      rmSync(first, { recursive: true, force: true });
      rmSync(second, { recursive: true, force: true });
    }
  },
);

test(
  "one cancelled async call cannot corrupt a concurrent peer",
  { timeout: DEADLINE_MS },
  async () => {
    const forge = new GraphForge();
    await forge.checkpoint({ name: "one", idempotencyKey: CHECKPOINT_KEY });

    // Cooperative AbortSignal may lose a race to a fast listCheckpoints worker
    // (same contract as checkpoints/provenance settlesOrCancels and
    // async-errors AbortSignal coverage). Peer isolation is the hard guarantee.
    const controller = new AbortController();
    const cancelled = forge.listCheckpoints({ signal: controller.signal });
    controller.abort();
    const peer = forge.listCheckpoints();

    const [cancelledOutcome, peerTable] = await Promise.all([
      cancelled.then(
        (ipc) => ({ kind: "ok", table: tableFromIPC(ipc) }),
        (error) => ({ kind: "error", code: error.code, name: error.name }),
      ),
      peer.then((ipc) => tableFromIPC(ipc)),
    ]);

    if (cancelledOutcome.kind === "error") {
      assert.equal(cancelledOutcome.code, "GF_CANCELLED");
      assert.notEqual(cancelledOutcome.name, "AbortError");
    } else {
      assert.deepEqual(
        [...cancelledOutcome.table.getChild("name").toArray()],
        ["one"],
      );
    }
    assert.deepEqual([...peerTable.getChild("name").toArray()], ["one"]);
  },
);

test(
  "simultaneous async reads preserve structured codes and ordered IPC",
  { timeout: DEADLINE_MS },
  async () => {
    const forge = new GraphForge();
    forge.execute(
      "CREATE (:Person {name:'Alice'}), (:Person {name:'Bob'}), (:Person {name:'Carol'})",
    );
    const expected = ["Alice", "Bob", "Carol"];
    const results = await Promise.all(
      Array.from({ length: 4 }, () => namesAsync(forge)),
    );
    for (const row of results) {
      assert.deepEqual(row, expected);
    }

    await assert.rejects(
      forge.enableCapability({
        operationUuid: "018f0f4e-7b8c-7000-8000-000000002417",
        capabilityId: "knowledge",
        capabilityVersion: 2,
      }),
      (error) => error.code === "GF_UNSUPPORTED_CAPABILITY_VERSION",
    );
  },
);

test(
  "same-directory unsupported writes reject before partial publication",
  { timeout: DEADLINE_MS },
  async () => {
    const root = mkdtempSync(join(tmpdir(), "gf-node-writer-busy-"));
    try {
      seed(root);
      const longReader = new GraphForge(root);
      assert.deepEqual(names(longReader), ["Alice", "Bob", "Carol"]);

      testAcquireWriterHold(root);
      try {
        const before = transactionNames(root);
        assert.throws(
          () => {
            const writer = new GraphForge(root);
            writer.execute("CREATE (:Person {name:'Delta'})");
          },
          (error) => error.code === "GF_WRITER_BUSY",
        );
        assert.deepEqual(transactionNames(root), before);
        assert.deepEqual(names(longReader), ["Alice", "Bob", "Carol"]);
      } finally {
        testReleaseWriterHold();
      }

      const writer = new GraphForge(root);
      writer.execute("CREATE (:Person {name:'Delta'})");
      writer.close();
      assert.deepEqual(names(longReader), ["Alice", "Bob", "Carol"]);
      const reopened = new GraphForge(root);
      assert.deepEqual(names(reopened), ["Alice", "Bob", "Carol", "Delta"]);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  },
);
