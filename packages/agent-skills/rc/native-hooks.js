/**
 * Native GraphForge hooks for RC E2E (same public APIs the skills package uses).
 */

import { openProject, uuidToString } from "../adapter/index.js";
import { QUESTION_KEY } from "./scenarios.js";

function decodeTable(tableFromIPC, ipc) {
  return Number.isInteger(ipc?.numRows) ? ipc : tableFromIPC(ipc);
}

export async function seedCompetingHypothesesNative({
  GraphForge,
  tableFromIPC,
  path,
  priorAssertionUuid,
  priorProvenanceUuid,
  priorReasoningUuid,
  uuids,
  nodeUuids,
}) {
  const opened = await openProject({
    GraphForge,
    path,
    requiredCapabilities: { epistemic: 1, graph: 1, knowledge: 1 },
    tableFromIPC,
  });
  const graph = opened.graph;
  try {
    const competingAssertionUuid = uuids[14];
    const replacementAssertionUuid = uuids[15];
    const groupUuid = uuids[16];

    const competing = decodeTable(
      tableFromIPC,
      await graph.createAssertion({
        assertionUuid: competingAssertionUuid,
        claim: "competing cause",
        graphRefs: [
          {
            graphKind: "node",
            graphUuid: nodeUuids.grace,
            ordinal: 0,
            role: "subject",
          },
        ],
        operationUuid: uuids[32],
      }),
    );
    const competingProvenanceUuid = uuidToString(competing.getChild("provenance_uuid").get(0));
    const competingReasoningUuid = uuids[43];
    await graph.recordReasoning({
      assertionUuid: competingAssertionUuid,
      content: Buffer.from("competing membership rationale", "utf8"),
      contentFormat: "text/plain",
      kind: "decision_rationale",
      operationUuid: uuids[44],
      provenanceUuid: competingProvenanceUuid,
      reasoningUuid: competingReasoningUuid,
    });

    const replacement = decodeTable(
      tableFromIPC,
      await graph.createAssertion({
        assertionUuid: replacementAssertionUuid,
        claim: "replacement cause",
        graphRefs: [
          {
            graphKind: "node",
            graphUuid: nodeUuids.ada,
            ordinal: 0,
            role: "subject",
          },
        ],
        operationUuid: uuids[33],
      }),
    );
    const replacementProvenanceUuid = uuidToString(replacement.getChild("provenance_uuid").get(0));
    const replacementReasoningUuid = uuids[45];
    await graph.recordReasoning({
      assertionUuid: replacementAssertionUuid,
      content: Buffer.from("replacement membership rationale", "utf8"),
      contentFormat: "text/plain",
      kind: "decision_rationale",
      operationUuid: uuids[46],
      provenanceUuid: replacementProvenanceUuid,
      reasoningUuid: replacementReasoningUuid,
    });

    await graph.createHypothesisGroup({
      groupUuid,
      operationUuid: uuids[34],
      provenanceUuid: replacementProvenanceUuid,
      questionKey: QUESTION_KEY,
    });
    await graph.recordHypothesisMembership({
      action: "added",
      assertionUuid: competingAssertionUuid,
      groupUuid,
      membershipEventUuid: uuids[35],
      operationUuid: uuids[36],
      provenanceUuid: competingProvenanceUuid,
      reasoningUuid: competingReasoningUuid,
    });
    await graph.recordHypothesisMembership({
      action: "added",
      assertionUuid: replacementAssertionUuid,
      groupUuid,
      membershipEventUuid: uuids[37],
      operationUuid: uuids[38],
      provenanceUuid: replacementProvenanceUuid,
      reasoningUuid: replacementReasoningUuid,
    });
    await graph.supersedeAssertion({
      operationUuid: uuids[39],
      priorAssertionUuid,
      provenanceUuid: priorProvenanceUuid,
      reasoningUuid: priorReasoningUuid,
      replacementAssertionUuid,
      statusEventUuid: uuids[42],
      supersessionUuid: uuids[17],
    });

    return {
      competing_assertion_uuid: competingAssertionUuid,
      group_uuid: groupUuid,
      question_key: QUESTION_KEY,
      replacement_assertion_uuid: replacementAssertionUuid,
    };
  } finally {
    graph.close();
  }
}

export async function prepareSearchIndexNative({ GraphForge, tableFromIPC, path }) {
  const opened = await openProject({
    GraphForge,
    path,
    requiredCapabilities: { graph: 1 },
    tableFromIPC,
  });
  try {
    opened.graph.index("Person", {
      properties: ["name", "summary"],
      rebuild: true,
    });
  } finally {
    opened.graph.close();
  }
}
