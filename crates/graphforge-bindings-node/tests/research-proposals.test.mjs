import assert from "node:assert/strict";
import { randomUUID as identity } from "node:crypto";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";
const uuid = (bytes) => {
  const hex = Buffer.from(bytes).toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
};
test("native proposals preserve selected review replay and history", async () => {
  const graph = new GraphForge();
  try {
    const generation = () =>
      graph.researchProjectSummary().identity.generationUuid;
    await graph.execute(
      "CREATE (:Character {score:0, private_note:'private'})",
    );
    const node = uuid(
      tableFromIPC(
        await graph.execute("MATCH (n:Character) RETURN n.node_uuid AS id"),
      )
        .getChild("id")
        .get(0),
    );
    const branch = identity(),
      version = identity(),
      proposal = identity();
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
      label: "Story",
    });
    await graph.executeResearchBranch({
      operation_uuid: identity(),
      expected_generation_uuid: generation(),
      branch_uuid: branch,
      version_uuid: version,
      created_at: 2,
      query: "MATCH (n:Character) SET n.score=1",
    });
    const frozen = await graph.freezeSlice({
      request_uuid: identity(),
      source: { kind: "version", version_uuid: version },
      selector: { kind: "direct", members: { nodes: [node] } },
    });
    await graph.submitResearchProposal({
      operation_uuid: identity(),
      expected_generation_uuid: generation(),
      proposal_uuid: proposal,
      source_branch_uuid: branch,
      source_version_uuid: version,
      frozen_ipc: Array.from(frozen),
      fields: [
        { object_kind: "node", object_uuid: node, field: "property:score" },
      ],
      actor_uuid: identity(),
      created_at: 3,
      motivation: "Selected score",
      policy: "",
    });
    const before = generation();
    const preview = tableFromIPC(
      await graph.previewResearchProposal({ proposal_uuid: proposal }),
    ).get(0);
    assert.equal(generation(), before);
    const review = {
      operation_uuid: identity(),
      expected_generation_uuid: preview.generation_uuid,
      proposal_uuid: proposal,
      preview_sha256: Array.from(Buffer.from(preview.preview_sha256, "hex")),
      decisions: { [preview.item_uuid]: "accept" },
      resolve_conflicts: [],
      acknowledge_evidence: [],
      promotions: [],
      community_uuid: null,
      actor_uuid: identity(),
      created_at: 4,
      explanation: "Accept score",
      policy: "",
    };
    const receipt = await graph.reviewResearchProposal(review);
    assert.deepEqual(await graph.reviewResearchProposal(review), receipt);
    const result = tableFromIPC(
      await graph.execute(
        "MATCH (n:Character) RETURN n.score AS score, n.private_note AS note",
      ),
    );
    assert.equal(result.getChild("score").get(0), 1n);
    assert.equal(result.getChild("note").get(0), "private");
    assert.equal(
      tableFromIPC(
        await graph.researchProposalHistory({
          proposal_uuid: proposal,
          detail: "accepted",
          page_size: 100,
        }),
      ).numRows,
      1,
    );
    await graph.releaseResearchProposal({
      operation_uuid: identity(),
      expected_generation_uuid: generation(),
      proposal_uuid: proposal,
    });
    assert.deepEqual(await graph.reviewResearchProposal(review), receipt);
    await assert.rejects(
      graph.reviewResearchProposal({ ...review, explanation: "changed" }),
      (e) => e.code === "GF_IDEMPOTENCY_CONFLICT",
    );
  } finally {
    graph.close();
  }
});
