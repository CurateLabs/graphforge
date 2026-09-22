import assert from "node:assert/strict";
import { randomUUID as identity } from "node:crypto";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

function uuid(bytes) {
  const hex = Buffer.from(bytes).toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

test("native upstream review advances only the selected baseline and replays its receipt", async () => {
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
      label: "Upstream review",
    });
    await graph.execute("MATCH (n:Item) SET n.x=1,n.y=2");
    const previewRequest = { branch_uuid: branch, scope: { kind: "branch" } };
    const before = generation();
    const preview = tableFromIPC(
      await graph.previewResearchUpstream(previewRequest),
    );
    assert.equal(generation(), before);
    const row = preview.toArray().find((row) => row.field === "property:x");
    const update = {
      operation_uuid: identity(),
      expected_generation_uuid: before,
      version_uuid: identity(),
      preview: previewRequest,
      preview_sha256: Array.from(
        Buffer.from(
          preview.schema.metadata.get("graphforge.upstream.preview_sha256"),
          "hex",
        ),
      ),
      selection: {
        kind: "selected",
        decisions: [
          {
            unit: {
              object_kind: row.object_kind,
              object_uuid: uuid(row.object_uuid),
              field: row.field,
            },
            resolution: { kind: "adopt_upstream" },
          },
        ],
      },
      acknowledge_evidence: [],
      actor_uuid: identity(),
      created_at: 2,
      explanation: "Adopt reviewed x only",
    };
    const receipt = await graph.updateResearchBranch(update);
    const values = tableFromIPC(
      await graph.queryResearchBranch(
        branch,
        "MATCH (n:Item) RETURN n.x AS x,n.y AS y",
      ),
    );
    assert.equal(Number(values.getChild("x").get(0)), 1);
    assert.equal(Number(values.getChild("y").get(0)), 0);
    const after = tableFromIPC(
      await graph.previewResearchUpstream(previewRequest),
    );
    assert.deepEqual(
      Object.fromEntries(
        after
          .toArray()
          .filter((row) => row.field.startsWith("property:"))
          .map((row) => [row.field, row.change]),
      ),
      { "property:y": "upstream" },
    );
    assert.deepEqual(await graph.updateResearchBranch(update), receipt);
    const history = tableFromIPC(
      await graph.researchUpstreamHistory({
        branch_uuid: branch,
        page_size: 10,
      }),
    );
    assert.equal(history.numRows, 1);
    assert.equal(
      history.getChild("operation_uuid").get(0),
      update.operation_uuid,
    );
    await assert.rejects(
      graph.updateResearchBranch({ ...update, explanation: "changed request" }),
      (error) => error.code === "GF_IDEMPOTENCY_CONFLICT",
    );
    const controller = new AbortController();
    controller.abort();
    await assert.rejects(
      graph.previewResearchUpstream(previewRequest, controller.signal),
      (error) => error.code === "GF_CANCELLED",
    );
  } finally {
    graph.close();
  }
});
