// Fresh-native live registry and descriptor facade acceptance (#2602).

import assert from "node:assert/strict";
import {
  existsSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  rmSync,
  statSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

function snapshotKnowledgeEpistemic(root) {
  const snapshot = new Map();
  const walk = (dir) => {
    if (!existsSync(dir)) return;
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      const path = join(dir, entry.name);
      if (entry.isDirectory()) {
        walk(path);
        continue;
      }
      if (
        !path.includes(`${join("participants", "knowledge")}`) &&
        !path.includes(`${join("participants", "epistemic")}`)
      ) {
        continue;
      }
      const stat = statSync(path);
      snapshot.set(path, {
        mtimeMs: stat.mtimeMs,
        size: stat.size,
        sha: readFileSync(path),
      });
    }
  };
  walk(root);
  return snapshot;
}

function assertUnchanged(before, after) {
  assert.deepEqual([...after.keys()].sort(), [...before.keys()].sort());
  for (const [path, prior] of before) {
    const next = after.get(path);
    assert.equal(next.size, prior.size, path);
    assert.equal(next.mtimeMs, prior.mtimeMs, path);
    assert.ok(Buffer.from(next.sha).equals(Buffer.from(prior.sha)), path);
  }
}

test("live registry contracts are catalog-exhaustive and descriptor reachable", () => {
  const forge = new GraphForge();
  const node = forge.addNode("Person", { name: "Ada" });
  const contracts = forge.algorithmDescriptorContracts();
  assert.equal(contracts.length, 94);
  assert.equal(
    new Set(contracts.map(({ verb, algorithm }) => `${verb}.${algorithm}`))
      .size,
    94,
  );

  const byVerb = Object.fromEntries(
    ["rank", "cluster", "paths", "analyze", "similar"].map((verb) => [
      verb,
      contracts.filter((row) => row.verb === verb),
    ]),
  );
  assert.ok(byVerb.rank.length > 0);
  assert.ok(byVerb.cluster.length > 0);
  assert.ok(byVerb.paths.length > 0);
  assert.ok(byVerb.analyze.length > 0);
  assert.ok(byVerb.similar.length > 0);

  const rank = forge.prepareRankInvocation("Person", "degree", undefined, true);
  assert.equal(rank.verb, "rank");
  assert.equal(rank.algorithm, "degree");
  const cluster = forge.prepareClusterInvocation("Person", "components");
  assert.equal(cluster.verb, "cluster");
  const paths = forge.preparePathsInvocation(node.uuid, undefined, "bfs");
  assert.equal(paths.verb, "paths");
  const analyze = forge.prepareAnalyzeInvocation("is_dag");
  assert.equal(analyze.verb, "analyze");
  const similar = forge.prepareSimilarInvocation("Person", "node_similarity");
  assert.equal(similar.verb, "similar");

  assert.equal(tableFromIPC(forge.invokeDescriptor(rank)).numRows, 1);
  assert.equal(
    tableFromIPC(forge.invokeDescriptorBytes(rank.canonicalBytes)).numRows,
    1,
  );
  forge.close();
});

test("registry discovery and descriptor preparation stay knowledge isolated", async () => {
  const project = mkdtempSync(join(tmpdir(), "gf-node-descriptor-isolation-"));
  try {
    const forge = new GraphForge(project);
    await forge.enableCapability({
      operationUuid: "018f0f4e-7b8c-7000-8000-000000000901",
      capabilityId: "provenance",
      capabilityVersion: 1,
    });
    const node = forge.addNode("Person", { name: "Ada" });
    await forge.enableCapability({
      operationUuid: "018f0f4e-7b8c-7000-8000-000000000902",
      capabilityId: "knowledge",
      capabilityVersion: 1,
    });
    await forge.enableCapability({
      operationUuid: "018f0f4e-7b8c-7000-8000-000000000903",
      capabilityId: "epistemic",
      capabilityVersion: 1,
    });
    await forge.createAssertionWithStatus({
      operationUuid: "018f0f4e-7b8c-7000-8000-000000000904",
      assertionUuid: "018f0f4e-7b8c-7000-8000-000000000905",
      claim: "Ada exists for isolation",
      graphRefs: [
        {
          graphUuid: node.uuid,
          graphKind: "node",
          role: "subject",
          ordinal: 0,
        },
      ],
      statusEventUuid: "018f0f4e-7b8c-7000-8000-000000000906",
      status: "supported",
    });
    forge.close();

    const reopened = new GraphForge(project);
    const before = snapshotKnowledgeEpistemic(project);
    assert.ok(before.size > 0);

    const contracts = reopened.algorithmDescriptorContracts();
    assert.equal(contracts.length, 94);
    const descriptor = reopened.prepareRankInvocation(
      "Person",
      "degree",
      undefined,
      true,
    );
    assert.equal(descriptor.verb, "rank");
    assert.equal(
      tableFromIPC(reopened.invokeDescriptor(descriptor)).numRows,
      1,
    );
    assertUnchanged(before, snapshotKnowledgeEpistemic(project));
    reopened.close();
  } finally {
    rmSync(project, { force: true, recursive: true });
  }
});
