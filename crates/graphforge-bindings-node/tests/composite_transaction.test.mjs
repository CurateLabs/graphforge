// Native acceptance for Node composite transactions (#2591).

import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, readdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

const OPERATION = "018f0f4e-7b8c-7000-8000-00000000d001";
const NODE = "018f0f4e-7b8c-7000-8000-00000000d002";
const ASSERTION = "018f0f4e-7b8c-7000-8000-00000000d003";
const STATUS_EVENT = "018f0f4e-7b8c-7000-8000-00000000d004";
const CONFLICT_OPERATION = "018f0f4e-7b8c-7000-8000-00000000d010";
const CONFLICT_NODE = "018f0f4e-7b8c-7000-8000-00000000d012";
const CONFLICT_ASSERTION = "018f0f4e-7b8c-7000-8000-00000000d013";
const CONFLICT_STATUS = "018f0f4e-7b8c-7000-8000-00000000d014";
const RECORDED_AT = 10;
// ProvenanceEvent::new(operation, CreateNode, None, 10) deterministic UUIDv8.
const PROVENANCE = "6f5e982b-b43a-8a20-aa4d-481da9c05f90";
const CONFLICT_PROVENANCE = "a915ebd6-0117-8ac4-966c-7dda803ee14f";

const RECEIPT_COLUMNS = [
  "request_identity",
  "transaction_uuid",
  "generation_uuid",
  "content_fingerprint",
  "contract_version",
  "graph_mutation_count",
  "provenance_events_count",
  "lineage_count",
  "assertions_count",
  "assertion_graph_refs_count",
  "confidence_assessments_count",
  "confidence_inputs_count",
  "evidence_count",
  "reasoning_count",
  "assertion_status_count",
  "assertion_supersessions_count",
  "hypothesis_groups_count",
  "hypothesis_membership_count",
  "hypothesis_selection_count",
  "assertion_validity_count",
];

function projectDigest(root) {
  const hash = createHash("sha256");
  const walk = (path) => {
    for (const entry of readdirSync(path, { withFileTypes: true }).sort(
      (a, b) => a.name.localeCompare(b.name),
    )) {
      const child = join(path, entry.name);
      if (entry.isDirectory()) {
        walk(child);
        continue;
      }
      hash.update(child.slice(root.length));
      hash.update(readFileSync(child));
    }
  };
  walk(root);
  return hash.digest("hex");
}

async function enableCapabilities(forge) {
  await forge.enableCapability({
    operationUuid: "018f0f4e-7b8c-7000-8000-00000000d101",
    capabilityId: "provenance",
    capabilityVersion: 1,
  });
  await forge.enableCapability({
    operationUuid: "018f0f4e-7b8c-7000-8000-00000000d102",
    capabilityId: "knowledge",
    capabilityVersion: 1,
  });
  await forge.enableCapability({
    operationUuid: "018f0f4e-7b8c-7000-8000-00000000d103",
    capabilityId: "epistemic",
    capabilityVersion: 1,
  });
}

function compositeRequest({
  operationUuid = OPERATION,
  nodeUuid = NODE,
  assertionUuid = ASSERTION,
  statusEventUuid = STATUS_EVENT,
  provenanceUuid = PROVENANCE,
  claim = "composite publishes atomically",
  graphKind = "node",
} = {}) {
  return {
    contractVersion: 1,
    operationUuid,
    graphMutations: [
      {
        kind: "create_node",
        nodeUuid,
        label: "Person",
        properties: { name: "Ada" },
      },
    ],
    knowledge: {
      provenanceEvents: [
        {
          operationUuid,
          eventKind: "create_node",
          recordedAtMicros: RECORDED_AT,
        },
      ],
      lineage: [
        {
          provenanceUuid,
          subjectUuid: nodeUuid,
          subjectKind: "node",
          role: "output",
          ordinal: 0,
        },
      ],
      assertions: [
        {
          assertionUuid,
          claim,
          provenanceUuid,
          recordedAtMicros: RECORDED_AT,
        },
      ],
      assertionGraphRefs: [
        {
          assertionUuid,
          graphUuid: nodeUuid,
          graphKind,
          role: "subject",
          ordinal: 0,
        },
      ],
      assertionStatus: [
        {
          statusEventUuid,
          assertionUuid,
          status: "supported",
          provenanceUuid,
          recordedAtMicros: RECORDED_AT,
        },
      ],
    },
  };
}

test("publishCompositeTransaction receipts, reopen, invalid zero-mutation, exact retry", async () => {
  const project = mkdtempSync(join(tmpdir(), "gf-composite-node-"));
  try {
    let forge = new GraphForge(project);
    await enableCapabilities(forge);

    const request = compositeRequest();
    const receipt = tableFromIPC(forge.publishCompositeTransaction(request));
    assert.equal(receipt.numRows, 1);
    assert.deepEqual(
      receipt.schema.fields.map((field) => field.name),
      RECEIPT_COLUMNS,
    );
    assert.equal(
      receipt.schema.metadata.get("graphforge.composite_kind"),
      "receipt",
    );
    assert.equal(
      receipt.schema.metadata.get("graphforge.row_order"),
      "singleton",
    );
    assert.equal(Number(receipt.getChild("graph_mutation_count").get(0)), 1);
    assert.equal(Number(receipt.getChild("provenance_events_count").get(0)), 1);
    assert.equal(Number(receipt.getChild("lineage_count").get(0)), 1);
    assert.equal(Number(receipt.getChild("assertions_count").get(0)), 1);
    assert.equal(
      Number(receipt.getChild("assertion_graph_refs_count").get(0)),
      1,
    );
    assert.equal(Number(receipt.getChild("assertion_status_count").get(0)), 1);
    assert.equal(Number(receipt.getChild("evidence_count").get(0)), 0);

    const names = tableFromIPC(
      forge.execute("MATCH (n:Person) RETURN n.name AS name ORDER BY name"),
    );
    assert.deepEqual([...names.getChild("name")], ["Ada"]);
    assert.equal(tableFromIPC(await forge.listAssertions()).numRows, 1);
    assert.equal(tableFromIPC(await forge.listAssertionStatus()).numRows, 1);

    const before = projectDigest(project);
    const retry = tableFromIPC(forge.publishCompositeTransaction(request));
    assert.deepEqual(
      [...retry.getChild("generation_uuid")],
      [...receipt.getChild("generation_uuid")],
    );
    assert.deepEqual(
      [...retry.getChild("content_fingerprint")],
      [...receipt.getChild("content_fingerprint")],
    );
    assert.equal(projectDigest(project), before);

    let conflicted = false;
    try {
      forge.publishCompositeTransaction(
        compositeRequest({ claim: "different claim" }),
      );
    } catch (error) {
      conflicted =
        error?.code === "GF_IDEMPOTENCY_CONFLICT" ||
        String(error?.message ?? error).includes("IDEMPOTENCY") ||
        String(error?.message ?? error)
          .toLowerCase()
          .includes("conflict");
    }
    assert.equal(conflicted, true);
    assert.equal(projectDigest(project), before);

    forge = new GraphForge(project);
    const reopened = tableFromIPC(
      forge.execute("MATCH (n:Person) RETURN n.name AS name ORDER BY name"),
    );
    assert.deepEqual([...reopened.getChild("name")], ["Ada"]);
    assert.equal(tableFromIPC(await forge.listAssertions()).numRows, 1);
    assert.equal(tableFromIPC(await forge.listAssertionStatus()).numRows, 1);

    const beforeInvalid = projectDigest(project);
    let invalidCode = null;
    try {
      forge.publishCompositeTransaction(
        compositeRequest({
          operationUuid: CONFLICT_OPERATION,
          nodeUuid: CONFLICT_NODE,
          assertionUuid: CONFLICT_ASSERTION,
          statusEventUuid: CONFLICT_STATUS,
          provenanceUuid: CONFLICT_PROVENANCE,
          graphKind: "edge",
        }),
      );
    } catch (error) {
      invalidCode = error?.code ?? null;
      if (
        invalidCode == null &&
        String(error?.message ?? error).includes("NOT_FOUND")
      ) {
        invalidCode = "GF_NOT_FOUND";
      }
    }
    assert.equal(invalidCode, "GF_NOT_FOUND");
    assert.equal(projectDigest(project), beforeInvalid);
    const count = tableFromIPC(
      forge.execute("MATCH (n:Person) RETURN count(n) AS c"),
    );
    assert.equal(Number(count.getChild("c").get(0)), 1);
  } finally {
    rmSync(project, { recursive: true, force: true });
  }
});

test("Node exposes one composite publish entrypoint with no inference helpers", () => {
  const forge = new GraphForge();
  assert.equal(typeof forge.publishCompositeTransaction, "function");
  assert.equal(typeof forge.publishCompositeGraphTransaction, "undefined");
  assert.equal(typeof forge.publishCompositeKnowledgeTransaction, "undefined");
  assert.equal(typeof forge.inferCompositeParticipants, "undefined");
});

test("every explicit composite participant family crosses the Node adapter", async () => {
  const project = mkdtempSync(
    join(tmpdir(), "gf-composite-participants-node-"),
  );
  try {
    const forge = new GraphForge(project);
    await enableCapabilities(forge);
    const operationUuid = "018f0f4e-7b8c-7000-8000-00000000e001";
    const provenanceUuid = "018f0f4e-7b8c-7000-8000-00000000e002";
    const assertionUuid = "018f0f4e-7b8c-7000-8000-00000000e003";
    const confidenceUuid = "018f0f4e-7b8c-7000-8000-00000000e004";
    const reasoningUuid = "018f0f4e-7b8c-7000-8000-00000000e005";
    const groupUuid = "018f0f4e-7b8c-7000-8000-00000000e006";
    const before = projectDigest(project);
    assert.throws(
      () =>
        forge.publishCompositeTransaction({
          contractVersion: 1,
          operationUuid,
          graphMutations: [
            {
              kind: "create_node",
              nodeUuid: "018f0f4e-7b8c-7000-8000-00000000e010",
              label: "Person",
              properties: { name: "Grace" },
            },
          ],
          knowledge: {
            confidenceAssessments: [
              {
                confidenceUuid,
                assertionUuid,
                policy: "conservative_min",
                value: 0.5,
                provenanceUuid,
                recordedAtMicros: RECORDED_AT,
              },
            ],
            confidenceInputs: [
              {
                confidenceUuid,
                inputConfidenceUuid: "018f0f4e-7b8c-7000-8000-00000000e007",
                inputValue: 0.5,
                ordinal: 0,
              },
            ],
            evidence: [
              {
                evidenceUuid: "018f0f4e-7b8c-7000-8000-00000000e008",
                assertionUuid,
                sourceUuid: "018f0f4e-7b8c-7000-8000-00000000e009",
                sourceKind: "document",
                role: "supports",
                weight: 0.8,
                provenanceUuid,
                recordedAtMicros: RECORDED_AT,
              },
            ],
            reasoning: [
              {
                reasoningUuid,
                assertionUuid,
                kind: "methodological_note",
                contentFormat: "text/plain",
                content: Buffer.from("explicit participant conversion"),
                provenanceUuid,
                recordedAtMicros: RECORDED_AT,
              },
            ],
            assertionSupersessions: [
              {
                supersessionUuid: "018f0f4e-7b8c-7000-8000-00000000e00a",
                priorAssertionUuid: assertionUuid,
                replacementAssertionUuid:
                  "018f0f4e-7b8c-7000-8000-00000000e00b",
                statusEventUuid: "018f0f4e-7b8c-7000-8000-00000000e00c",
                reasoningUuid,
                provenanceUuid,
                recordedAtMicros: RECORDED_AT,
              },
            ],
            hypothesisGroups: [
              {
                groupUuid,
                questionKey: "which hypothesis",
                provenanceUuid,
                recordedAtMicros: RECORDED_AT,
              },
            ],
            hypothesisMembership: [
              {
                membershipEventUuid: "018f0f4e-7b8c-7000-8000-00000000e00d",
                operationUuid,
                groupUuid,
                assertionUuid,
                action: "added",
                reasoningUuid,
                provenanceUuid,
                recordedAtMicros: RECORDED_AT,
              },
            ],
            hypothesisSelection: [
              {
                selectionEventUuid: "018f0f4e-7b8c-7000-8000-00000000e00e",
                operationUuid,
                groupUuid,
                selectedAssertionUuid: assertionUuid,
                reasoningUuid,
                provenanceUuid,
                recordedAtMicros: RECORDED_AT,
              },
            ],
            assertionValidity: [
              {
                validityEventUuid: "018f0f4e-7b8c-7000-8000-00000000e00f",
                assertionUuid,
                validFromMicros: 1,
                validToMicros: 2,
                reasoningUuid,
                provenanceUuid,
                recordedAtMicros: RECORDED_AT,
              },
            ],
          },
        }),
      (error) => error?.code === "GF_NOT_FOUND",
    );
    assert.equal(projectDigest(project), before);
  } finally {
    rmSync(project, { recursive: true, force: true });
  }
});
