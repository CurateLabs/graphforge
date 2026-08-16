// Native acceptance for transaction and maintenance parity (#755).
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { GraphForge } from "../index.js";

const OP_COMMIT = "018f0f4e-7b8c-7000-8000-000000007501";
const OP_ROLLBACK = "018f0f4e-7b8c-7000-8000-000000007502";
const OP_DROP = "018f0f4e-7b8c-7000-8000-000000007503";
const OP_SEED = "018f0f4e-7b8c-7000-8000-000000007504";
const NODE_BULK = "018f0f4e-7b8c-7000-8000-000000007511";
const NODE_GHOST = "018f0f4e-7b8c-7000-8000-000000007512";

async function withProject(run) {
  const root = await mkdtemp(join(tmpdir(), "gf-755-"));
  try {
    await run(root);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

test("mixed transaction commit and explicit rollback have parity", async () => {
  await withProject(async (root) => {
    const forge = new GraphForge(root);
    const tx = forge.beginTransaction(OP_COMMIT);
    const status = tx.status();
    assert.equal(status.phase, "open");
    assert.equal(status.committed, false);
    tx.stageCypher("CREATE (:Person {name: 'Cypher'})");
    tx.stageAddNode(NODE_BULK, "Person", { name: "Bulk" });
    tx.validate();
    const generation = tx.commit();
    assert.match(generation, /^[0-9a-f-]{36}$/i);

    const rolled = forge.beginTransaction(OP_ROLLBACK);
    rolled.stageAddNode(NODE_GHOST, "Person", { name: "Ghost" });
    rolled.rollback();

    const recovery = forge.projectOpenRecovery();
    assert.ok(recovery.selectedGenerationUuid);
    assert.equal(typeof recovery.repairedJournals, "bigint");
  });
});

test("dropped wrapper handles roll back and never commit", async () => {
  await withProject(async (root) => {
    const forge = new GraphForge(root);
    {
      const tx = forge.beginTransaction(OP_DROP);
      tx.stageCypher("CREATE (:Person {name: 'Dropped'})");
    }
    const reopened = new GraphForge(root);
    const cleanup = reopened.previewProjectCleanup({ retainedAncestors: 2 });
    assert.equal(typeof cleanup.candidates, "bigint");
    assert.equal(typeof cleanup.remainingBytes, "bigint");
    // Values above Number.MAX_SAFE_INTEGER stay exact as bigint (not Number).
    const aboveSafe = cleanup.candidates + 9007199254740993n;
    assert.equal(aboveSafe > cleanup.candidates, true);
    assert.equal(Number.isSafeInteger(Number(aboveSafe)), false);
  });
});

test("maintenance preview and execution reconcile candidate identities", async () => {
  await withProject(async (root) => {
    const forge = new GraphForge(root);
    const seed = forge.beginTransaction(OP_SEED);
    seed.stageCypher("CREATE (:Person {name: 'Keep'})");
    seed.commit();
    const preview = forge.previewProjectCleanup({ retainedAncestors: 2 });
    const executed = forge.executeProjectCleanup({ retainedAncestors: 2 });
    assert.equal(preview.candidates, executed.candidates);
    assert.equal(preview.reachableCount, executed.reachableCount);
    assert.deepEqual(
      preview.entries.map((entry) => entry.generationUuid),
      executed.entries.map((entry) => entry.generationUuid),
    );
    const status = forge.graphDeltaCompactionStatus({});
    assert.equal(typeof status.runCount, "bigint");
    assert.equal(typeof status.runBytes, "bigint");
  });
});
