// Deterministic Node native concurrency parity against the Rust contract (#2416).

import assert from "node:assert/strict";
import { existsSync, mkdtempSync, readdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
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
