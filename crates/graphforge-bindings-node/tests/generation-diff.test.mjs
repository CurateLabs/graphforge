// Real napi execution for the Rust-owned semantic generation diff (#804).

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

test("generation diff forwards exact Rust IPC, identities, limits, and cancellation", async () => {
  const forge = new GraphForge();
  const first = forge.addNode("Person", { name: "Grace" });
  const source = forge.committedGenerationIdentity();
  const second = forge.addNode("Person", { name: "Ada" });
  forge.addEdge(first, "KNOWS", second, { since: 2026 });
  const target = forge.committedGenerationIdentity();
  const request = { source, target };

  const result = await forge.diffCommittedGenerations(request);
  const retry = await forge.diffCommittedGenerations(request);
  assert.equal(result.kind, "ready");
  assert.deepEqual(result.source, source);
  assert.deepEqual(result.target, target);
  assert.deepEqual(result.checkpointBinding, retry.checkpointBinding);
  for (const name of [
    "addedNodes",
    "removedNodes",
    "modifiedNodes",
    "addedEdges",
    "removedEdges",
    "modifiedEdges",
  ]) {
    assert.deepEqual(result[name].ipc, retry[name].ipc);
    assert.equal(
      tableFromIPC(result[name].ipc).numRows,
      Number(result[name].rowCount),
    );
  }
  assert.equal(result.addedNodes.rowCount, 1n);
  assert.equal(result.addedEdges.rowCount, 1n);

  forge.execute("MATCH (n) SET n.active = true");
  const finalTarget = forge.committedGenerationIdentity();
  const ladder = await forge.diffCommittedGenerations({
    source: target,
    target: finalTarget,
  });
  const direct = await forge.diffCommittedGenerations({
    source,
    target: finalTarget,
  });
  assert.equal(ladder.kind, "ready");
  assert.equal(direct.kind, "ready");
  assert.deepEqual(ladder.source, target);
  assert.deepEqual(ladder.target, finalTarget);
  assert.deepEqual(direct.source, source);
  assert.deepEqual(direct.target, finalTarget);
  assert.equal(ladder.modifiedNodes.rowCount, 2n);
  assert.equal(direct.addedNodes.rowCount, 1n);
  assert.equal(direct.modifiedNodes.rowCount, 1n);

  const wrongManifest = Buffer.from(source.manifestSha256);
  wrongManifest[0] ^= 0xff;
  assert.deepEqual(
    await forge.diffCommittedGenerations({
      ...request,
      source: { ...source, manifestSha256: wrongManifest },
    }),
    { kind: "reload_required", reason: "identity_mismatch" },
  );
  assert.deepEqual(
    await forge.diffCommittedGenerations({
      ...request,
      maxRecordsPerGeneration: 0n,
    }),
    { kind: "reload_required", reason: "resource_limit" },
  );
  assert.deepEqual(
    await forge.diffCommittedGenerations({
      ...request,
      maxOutputBytes: 1n,
    }),
    { kind: "reload_required", reason: "resource_limit" },
  );

  const controller = new AbortController();
  const pending = forge.diffCommittedGenerations({
    ...request,
    signal: controller.signal,
  });
  controller.abort();
  await assert.rejects(pending, (error) => error.code === "GF_CANCELLED");
});
