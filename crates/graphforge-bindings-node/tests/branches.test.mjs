import assert from "node:assert/strict";
import { randomUUID as identity } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createRequire } from "node:module";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
const { GraphForge } = createRequire(import.meta.url)("../index.js");
test("native Branch isolation, exact retry, scoped restore and durable reopen", async () => {
  const directory = mkdtempSync(join(tmpdir(), "gf-branch-"));
  const root = join(directory, "project");
  let graph = new GraphForge(root);
  try {
    await graph.execute("CREATE (:Character {name: 'Original'})");
    const generation = () =>
      graph.researchProjectSummary().identity.generationUuid;
    const branch = identity(),
      base = identity();
    const create = {
      operation_uuid: identity(),
      expected_generation_uuid: generation(),
      branch_uuid: branch,
      version_uuid: base,
      source: {
        kind: "current",
        origin_version_uuid: identity(),
        context_uuid: identity(),
      },
      creator_uuid: identity(),
      created_at: 1,
      label: "Story",
    };
    const receipt = await graph.createResearchBranch(create);
    assert.deepEqual(await graph.createResearchBranch(create), receipt);
    const selection = await graph.researchBranchSelection(branch);
    assert.equal(tableFromIPC(selection).numRows, 1);
    await graph.executeResearchBranch({
      operation_uuid: identity(),
      expected_generation_uuid: generation(),
      branch_uuid: branch,
      version_uuid: identity(),
      created_at: 2,
      query: "MATCH (n:Character) SET n.name = 'Local'",
    });
    assert.equal(
      tableFromIPC(
        await graph.queryResearchBranch(
          branch,
          "MATCH (n) RETURN n.name AS name",
        ),
      )
        .getChild("name")
        .get(0),
      "Local",
    );
    assert.equal(
      tableFromIPC(await graph.execute("MATCH (n) RETURN n.name AS name"))
        .getChild("name")
        .get(0),
      "Original",
    );
    assert.deepEqual(await graph.researchBranchSelection(branch), selection);
    assert.ok(
      [
        ...tableFromIPC(await graph.researchBranchFields(branch)).getChild(
          "status",
        ),
      ].includes("local"),
    );
    const restore = {
      operation_uuid: identity(),
      expected_generation_uuid: generation(),
      branch_uuid: branch,
      source_version_uuid: base,
      version_uuid: identity(),
      created_at: 3,
    };
    const abort = new AbortController();
    abort.abort();
    await assert.rejects(
      graph.restoreResearchBranch(restore, abort.signal),
      (e) => e.code === "GF_CANCELLED",
    );
    const restored = await graph.restoreResearchBranch(restore);
    graph.close();
    graph = new GraphForge(root);
    assert.deepEqual(await graph.restoreResearchBranch(restore), restored);
    assert.equal(
      (await graph.researchBranch(branch)).version_uuid,
      restore.version_uuid,
    );
    assert.equal(
      tableFromIPC(
        await graph.queryResearchBranch(
          branch,
          "MATCH (n) RETURN n.name AS name",
        ),
      )
        .getChild("name")
        .get(0),
      "Original",
    );
    assert.equal(
      tableFromIPC(await graph.researchBranchReferences(branch)).numRows,
      0,
    );
  } finally {
    graph.close();
    rmSync(directory, { recursive: true, force: true });
  }
});
