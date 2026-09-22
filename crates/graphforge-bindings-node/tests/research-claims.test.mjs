import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";
const identity = () => randomUUID().replace(/^(.{14})./, "$17");
const uuid = (bytes) => {
  const hex = Buffer.from(bytes).toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
};
test("contextual claims preserve native authority and durable Branch isolation", async () => {
  const directory = mkdtempSync(join(tmpdir(), "gf-claims-"));
  const root = join(directory, "project");
  let graph = new GraphForge(root);
  try {
    await graph.execute("CREATE (:Person {name:'Ada'})");
    for (const capabilityId of ["provenance", "knowledge", "epistemic"]) {
      await graph.enableCapability({
        operationUuid: identity(),
        capabilityId,
        capabilityVersion: 1,
      });
    }
    const node = uuid(
      tableFromIPC(await graph.execute("MATCH (n) RETURN n.node_uuid AS id"))
        .getChild("id")
        .get(0),
    );
    const generation = () =>
      graph.researchProjectSummary().identity.generationUuid;
    const claim = async (text) => {
      const request = {
        operation_uuid: identity(),
        expected_generation_uuid: generation(),
        assertion_uuid: identity(),
        claim: text,
        graph_refs: [
          { graph_uuid: node, graph_kind: "node", role: "subject", ordinal: 0 },
        ],
        category: "interpretation",
        creator_uuid: identity(),
        run_uuid: null,
        created_at: 1,
      };
      const result = await graph.createResearchClaim(request);
      assert.equal(tableFromIPC(result).numRows, 1);
      assert.deepEqual(await graph.createResearchClaim(request), result);
      return request.assertion_uuid;
    };
    const first = await claim("Original interpretation"),
      second = await claim("Alternative interpretation");
    const provenance = uuid(
      tableFromIPC(await graph.assertion(first))
        .getChild("provenance_uuid")
        .get(0),
    );
    await graph.relateResearchClaims({
      operation_uuid: identity(),
      expected_generation_uuid: generation(),
      relation: {
        relation_uuid: identity(),
        source_assertion_uuid: second,
        target_assertion_uuid: first,
        kind: "alternative_to",
        creator_uuid: identity(),
        provenance_uuid: provenance,
        recorded_at: 2,
      },
    });
    const project = { kind: "project" },
      authority = { context: project, community_uuid: null };
    const view = { ...authority, include_suppressed: false };
    assert.deepEqual(
      [
        ...tableFromIPC(await graph.inspectResearchClaims(view)).getChild(
          "canonical",
        ),
      ],
      [false, false],
    );
    await graph.recordResearchDecisions({
      operation_uuid: identity(),
      expected_generation_uuid: generation(),
      ...authority,
      creator_uuid: identity(),
      recorded_at: 3,
      decisions: [
        {
          decision_uuid: identity(),
          subject_kind: "assertion",
          subject_uuid: first,
          kind: "promote",
          source_version_uuid: null,
        },
      ],
    });
    assert.equal(
      tableFromIPC(await graph.researchCanonicalChoices(authority)).numRows,
      1,
    );
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
      created_at: 4,
      label: "Alternative",
    });
    const context = { kind: "branch", branch_uuid: branch };
    const change = {
      operation_uuid: identity(),
      expected_generation_uuid: generation(),
      branch_uuid: branch,
      version_uuid: identity(),
      creator_uuid: identity(),
      created_at: 5,
      change: {
        kind: "suppress",
        suppression_uuid: identity(),
        assertion_uuid: first,
        provenance_uuid: provenance,
      },
    };
    const abort = new AbortController();
    abort.abort();
    await assert.rejects(
      graph.changeResearchBranchClaim(change, abort.signal),
      (e) => e.code === "GF_CANCELLED",
    );
    const receipt = await graph.changeResearchBranchClaim(change);
    assert.equal(
      tableFromIPC(
        await graph.inspectResearchClaims({
          context,
          community_uuid: null,
          include_suppressed: false,
        }),
      ).numRows,
      1,
    );
    assert.equal(
      tableFromIPC(await graph.inspectResearchClaims(view)).numRows,
      2,
    );
    assert.equal(
      tableFromIPC(
        await graph.researchClaimHistory({
          context,
          family: "suppressions",
          assertion_uuid: first,
        }),
      ).numRows,
      1,
    );
    assert.equal(
      tableFromIPC(
        await graph.researchClaimHistory({
          context: project,
          family: "relations",
          assertion_uuid: first,
        }),
      ).numRows,
      1,
    );
    assert.equal(
      Number(
        tableFromIPC(await graph.execute("MATCH (n) RETURN count(n) AS n"))
          .getChild("n")
          .get(0),
      ),
      1,
    );
    graph.close();
    graph = new GraphForge(root);
    assert.deepEqual(await graph.changeResearchBranchClaim(change), receipt);
    assert.equal(
      tableFromIPC(await graph.researchDecisionHistory(authority)).numRows,
      1,
    );
    assert.equal(
      tableFromIPC(
        await graph.researchCanonicalChoices({ context, community_uuid: null }),
      ).numRows,
      0,
    );
  } finally {
    graph.close();
    rmSync(directory, { recursive: true, force: true });
  }
});
