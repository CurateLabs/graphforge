import assert from "node:assert/strict";
import { randomUUID as identity } from "node:crypto";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

test("native comparison preserves local/upstream semantics and refuses stale pages", async () => {
  const graph = new GraphForge();
  try {
    const generation = () =>
      graph.researchProjectSummary().identity.generationUuid;
    await graph.execute("CREATE (:Item {x:0,y:0})");
    const branch = identity();
    await graph.createResearchBranch({
      operation_uuid: identity(),
      expected_generation_uuid: generation(),
      branch_uuid: branch,
      version_uuid: identity(),
      source: {
        kind: "current",
        origin_version_uuid: identity(),
        context_uuid: identity(),
      },
      creator_uuid: identity(),
      created_at: 1,
      label: "Comparison",
    });
    await graph.executeResearchBranch({
      operation_uuid: identity(),
      expected_generation_uuid: generation(),
      branch_uuid: branch,
      version_uuid: identity(),
      created_at: 2,
      query: "MATCH (n:Item) SET n.x = 1",
    });
    await graph.execute("MATCH (n:Item) SET n.y = 2");
    const request = {
      left: { kind: "branch", branch_uuid: branch },
      right: { kind: "project" },
      detail: "changes",
      max_fields: 40000,
      max_bytes: 67108864,
      page_size: 1000,
    };
    const before = generation();
    const result = tableFromIPC(await graph.compareResearch(request));
    assert.deepEqual(Array.from(result.getChild("change")), [
      "local",
      "upstream",
    ]);
    assert.equal(generation(), before);
    request.page_size = 1;
    const page = tableFromIPC(await graph.compareResearch(request));
    request.after = page.schema.metadata.get(
      "graphforge.comparison.next_cursor",
    );
    await graph.execute("MATCH (n:Item) SET n.y = 3");
    await assert.rejects(
      graph.compareResearch(request),
      (error) => error.code === "GF_PAGE_SNAPSHOT_GONE",
    );
    delete request.after;
    const controller = new AbortController();
    controller.abort();
    await assert.rejects(
      graph.compareResearch(request, controller.signal),
      (error) => error.code === "GF_CANCELLED",
    );
  } finally {
    graph.close();
  }
});
