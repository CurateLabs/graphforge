import assert from "node:assert/strict";
import { createHash, randomUUID as identity } from "node:crypto";
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

test("native research interchange preserves citations exports and independent Fork replay", async () => {
  const root = mkdtempSync(join(tmpdir(), "gf-research-interchange-"));
  const graph = new GraphForge();
  try {
    await graph.execute("CREATE (:Item {score:7})");
    const branch = identity(),
      version = identity();
    const generation = () =>
      graph.researchProjectSummary().identity.generationUuid;
    await graph.createResearchBranch({
      operation_uuid: identity(),
      expected_generation_uuid: generation(),
      branch_uuid: branch,
      version_uuid: version,
      source: {
        kind: "current",
        origin_version_uuid: identity(),
        context_uuid: identity(),
      },
      creator_uuid: identity(),
      created_at: 1,
      label: "Portable research",
    });
    const target = { kind: "version", version_uuid: version };
    const reference = await graph.researchReference(target);
    const live = await graph.researchReference({
      kind: "branch",
      branch_uuid: branch,
    });
    assert.deepEqual(live.version, reference.version);
    assert.equal(reference.genealogy[0].branch_uuid, branch);
    const input = join(root, "package");
    const exportRequest = {
      version_uuid: version,
      output: input,
      bundled: false,
      projection: null,
    };
    const exported = await graph.exportResearch(exportRequest);
    const verified = await GraphForge.verifyPortableV2({ input, mode: "full" });
    assert.equal(verified.packageDigest, exported.package_digest);
    assert.equal(verified.researchInterchange, true);
    assert.ok(verified.researchEntries.length > 0);
    const manifest = JSON.parse(
      readFileSync(join(input, "data/graphforge-project.json"), "utf8"),
    );
    const expectedEntries = manifest.components
      .filter((component) => component.kind === "research")
      .flatMap((component) =>
        component.files.map((file) => ({
          componentId: component.participant_id,
          path: file.path,
          length: BigInt(file.length),
          sha256: file.sha256,
        })),
      );
    assert.deepEqual(verified.researchEntries, expectedEntries);
    for (const entry of verified.researchEntries) {
      const bytes = readFileSync(join(input, entry.path));
      assert.equal(entry.length, BigInt(bytes.length));
      assert.equal(
        entry.sha256,
        createHash("sha256").update(bytes).digest("hex"),
      );
    }
    const projectRoot = join(root, "imported");
    await GraphForge.importPortableV2({
      projectRoot,
      input,
      operationId: identity(),
    });
    const imported = new GraphForge(projectRoot);
    try {
      const after = await imported.researchReference(target);
      assert.deepEqual(after.version, reference.version);
      assert.deepEqual(after.genealogy, reference.genealogy);
      assert.equal(
        tableFromIPC(await imported.execute("MATCH(n:Item) RETURN n.score"))
          .getChildAt(0)
          .get(0),
        7n,
      );
      await assert.rejects(
        imported.researchReference({ kind: "branch", branch_uuid: branch }),
      );
    } finally {
      imported.close();
    }
    const metadata = graph.researchProjectMetadata();
    metadata.title = "Independent research";
    metadata.access.visibility = "private";
    metadata.access.access_policy = "Independent local review";
    const fork = {
      operation_uuid: identity(),
      project_uuid: identity(),
      version_uuid: version,
      projection: null,
      target: join(root, "fork"),
      actor_uuid: identity(),
      governance: "Independent review",
      adopt_selected_ontology: true,
      metadata,
    };
    const first = await graph.forkResearch(fork);
    const replay = await graph.forkResearch(fork);
    assert.equal(replay.idempotent_replay, true);
    assert.equal(replay.generation_uuid, first.generation_uuid);
    const destination = new GraphForge(fork.target);
    try {
      assert.deepEqual(destination.researchProjectMetadata(), metadata);
      const citation = await destination.researchReference(target);
      assert.equal(citation.project_uuid, fork.project_uuid);
      assert.notEqual(citation.project_uuid, reference.project_uuid);
      assert.deepEqual(citation.version, reference.version);
      await assert.rejects(
        graph.forkResearch({ ...fork, governance: "Changed policy" }),
        (error) => error.code === "GF_IDEMPOTENCY_CONFLICT",
      );
    } finally {
      destination.close();
    }
    const before = generation();
    const controller = new AbortController();
    controller.abort();
    const cancelledExport = {
      ...exportRequest,
      output: join(root, "cancelled-export"),
    };
    const cancelledFork = {
      ...fork,
      operation_uuid: identity(),
      target: join(root, "cancelled-fork"),
    };
    for (const [method, request] of [
      ["researchReference", target],
      ["exportResearch", cancelledExport],
      ["forkResearch", cancelledFork],
    ]) {
      await assert.rejects(
        graph[method](request, controller.signal),
        (error) => error.code === "GF_CANCELLED",
      );
    }
    assert.equal(existsSync(cancelledExport.output), false);
    assert.equal(existsSync(cancelledFork.target), false);
    assert.equal(generation(), before);
  } finally {
    graph.close();
    rmSync(root, { recursive: true, force: true });
  }
});

test("interchange request diagnostics exclude private input", () => {
  const graph = new GraphForge();
  const sentinel = "PRIVATE_INTERCHANGE_SENTINEL";
  try {
    for (const method of [
      "researchReference",
      "exportResearch",
      "forkResearch",
    ]) {
      assert.throws(
        () => graph[method]({ [sentinel]: sentinel }),
        (error) =>
          !error.message.includes(sentinel) && error.message.length < 256,
      );
    }
  } finally {
    graph.close();
  }
});
