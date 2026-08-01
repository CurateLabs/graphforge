// Fresh-native pinned belief-subject Arrow contract (#2634).

import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

const id = (suffix) => `018f0f4e-7b8c-7000-8000-${suffix.padStart(12, "0")}`;
const uuid = (value) => {
  const hex = Buffer.from(value).toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
};
const hex = (value) => Buffer.from(value).toString("hex");
const micros = (column, row) =>
  Number(column.data[0].values[column.data[0].offset + row]);
const policy = (overrides = {}) => ({
  includedStatuses: ["supported"],
  statusless: "include",
  supersessionBranches: "include_all_leaves",
  hypotheses: "exclude_unselected_group",
  ...overrides,
});
const request = (subject, overrides = {}) => ({
  ...subject,
  transactionCutoffMicros: Number.MAX_SAFE_INTEGER,
  validTimeMicros: 150,
  policy: policy(),
  ...overrides,
});

async function fixture() {
  const forge = new GraphForge();
  for (const [suffix, capabilityId] of [
    [1, "provenance"],
    [2, "knowledge"],
    [3, "epistemic"],
    [4, "valid_time"],
  ]) {
    await forge.enableCapability({
      operationUuid: id(String(suffix)),
      capabilityId,
      capabilityVersion: 1,
    });
  }
  const ids = {
    prior: id("10"),
    selected: id("11"),
    alternative: id("12"),
    priorReason: id("30"),
    selectedReason: id("31"),
    alternativeReason: id("32"),
    primary: id("40"),
    unselected: id("41"),
    supersession: id("50"),
    supersededStatus: id("51"),
    selectedStatus: id("52"),
    primarySelected: id("60"),
    primaryAlternative: id("61"),
    unselectedMembership: id("62"),
    selection: id("63"),
    validity: id("70"),
  };
  const assertions = [];
  for (const [index, assertionUuid] of [
    ids.prior,
    ids.selected,
    ids.alternative,
  ].entries()) {
    const node = forge.addNode("BeliefSubject", { index });
    const table = tableFromIPC(
      await forge.createAssertion({
        operationUuid: id(String(20 + index)),
        assertionUuid,
        claim: `claim ${assertionUuid}`,
        graphRefs: [
          {
            graphUuid: node.uuid,
            graphKind: "node",
            role: "subject",
            ordinal: 0,
          },
        ],
      }),
    );
    assertions.push({
      node,
      provenance: uuid(table.getChild("provenance_uuid").get(0)),
    });
  }
  for (const [index, [assertionUuid, reasoningUuid]] of [
    [ids.prior, ids.priorReason],
    [ids.selected, ids.selectedReason],
    [ids.alternative, ids.alternativeReason],
  ].entries()) {
    await forge.recordReasoning({
      operationUuid: id(String(33 + index)),
      reasoningUuid,
      assertionUuid,
      kind: "decision_rationale",
      contentFormat: "text/plain",
      content: Buffer.from(`reason ${assertionUuid}`),
      provenanceUuid: assertions[index].provenance,
    });
  }
  await forge.recordAssertionStatus({
    operationUuid: id("53"),
    statusEventUuid: ids.selectedStatus,
    assertionUuid: ids.selected,
    status: "supported",
    reasoningUuid: ids.selectedReason,
    provenanceUuid: assertions[1].provenance,
  });
  await forge.assessConfidence({
    operationUuid: id("54"),
    confidenceUuid: id("55"),
    assertionUuid: ids.alternative,
    policy: "explicit",
    value: 1,
  });
  for (const [operation, groupUuid, questionKey, provenanceUuid] of [
    [42, ids.primary, "belief-subject.primary.v1", assertions[1].provenance],
    [
      43,
      ids.unselected,
      "belief-subject.unselected.v1",
      assertions[2].provenance,
    ],
  ])
    await forge.createHypothesisGroup({
      operationUuid: id(String(operation)),
      groupUuid,
      questionKey,
      provenanceUuid,
    });
  for (const [
    operation,
    membershipEventUuid,
    groupUuid,
    assertionUuid,
    reasoningUuid,
    provenanceUuid,
  ] of [
    [
      64,
      ids.primarySelected,
      ids.primary,
      ids.selected,
      ids.selectedReason,
      assertions[1].provenance,
    ],
    [
      65,
      ids.primaryAlternative,
      ids.primary,
      ids.alternative,
      ids.alternativeReason,
      assertions[2].provenance,
    ],
    [
      66,
      ids.unselectedMembership,
      ids.unselected,
      ids.alternative,
      ids.alternativeReason,
      assertions[2].provenance,
    ],
  ])
    await forge.recordHypothesisMembership({
      operationUuid: id(String(operation)),
      membershipEventUuid,
      groupUuid,
      assertionUuid,
      action: "added",
      reasoningUuid,
      provenanceUuid,
    });
  await forge.recordHypothesisSelection({
    operationUuid: id("67"),
    selectionEventUuid: ids.selection,
    groupUuid: ids.primary,
    selectedAssertionUuid: ids.selected,
    reasoningUuid: ids.selectedReason,
    provenanceUuid: assertions[1].provenance,
  });
  await forge.supersedeAssertion({
    operationUuid: id("56"),
    supersessionUuid: ids.supersession,
    priorAssertionUuid: ids.prior,
    replacementAssertionUuid: ids.selected,
    statusEventUuid: ids.supersededStatus,
    reasoningUuid: ids.priorReason,
    provenanceUuid: assertions[0].provenance,
  });
  await forge.recordAssertionValidity({
    operationUuid: id("71"),
    validityEventUuid: ids.validity,
    assertionUuid: ids.selected,
    validFromMicros: 100,
    validToMicros: 200,
    reasoningUuid: ids.selectedReason,
    provenanceUuid: assertions[1].provenance,
  });
  return { forge, ids };
}

test("belief subjects preserve Rust projection and canonical Arrow evidence", async () => {
  const declarations = readFileSync(
    new URL("../index.d.ts", import.meta.url),
    "utf8",
  );
  assert.match(
    declarations,
    /resolveBeliefSubject\(request: \(\{ assertionUuid: string; hypothesisQuestionKey\?: never \} \| \{ assertionUuid\?: never; hypothesisQuestionKey: string \}\) & \{ transactionCutoffMicros: number; validTimeMicros\?: number; policy: Required<BeliefSubjectPolicyInput>; signal\?: AbortSignal \}\): Promise<ResolvedBeliefSubjectOutput>/,
  );
  assert.doesNotMatch(
    declarations,
    /resolveBeliefSubject\(request: ResolveBeliefSubjectInput\)/,
  );

  const { forge, ids } = await fixture();
  const byQuestion = await forge.resolveBeliefSubject(
    request({
      hypothesisQuestionKey: "belief-subject.primary.v1",
    }),
  );
  const byAssertion = await forge.resolveBeliefSubject(
    request({ assertionUuid: ids.prior }),
  );
  assert.ok(
    Buffer.from(byQuestion.evidence).equals(Buffer.from(byAssertion.evidence)),
  );

  const evidence = tableFromIPC(byQuestion.evidence);
  assert.equal(evidence.numRows, 5);
  assert.deepEqual(
    [...evidence.getChild("entity_kind").toArray()],
    [
      "assertion",
      "assertion",
      "assertion",
      "hypothesis_group",
      "hypothesis_group",
    ],
  );
  assert.deepEqual(
    [0, 1, 2].map((row) => uuid(evidence.getChild("assertion_uuid").get(row))),
    [ids.prior, ids.selected, ids.alternative],
  );
  assert.deepEqual(
    [0, 1, 2].map((row) => evidence.getChild("status").get(row)),
    ["superseded", "supported", null],
  );
  assert.deepEqual(
    [3, 4].map((row) => uuid(evidence.getChild("group_uuid").get(row))),
    [ids.primary, ids.unselected],
  );
  assert.equal(
    uuid(evidence.getChild("selected_assertion_uuid").get(3)),
    ids.selected,
  );
  assert.equal(evidence.getChild("selected_assertion_uuid").get(4), null);
  assert.deepEqual(
    [...evidence.getChild("current_member_assertion_uuids").get(3)].map(uuid),
    [ids.selected, ids.alternative],
  );
  assert.deepEqual(
    [...evidence.getChild("superseded_by_assertion_uuids").get(0)].map(uuid),
    [ids.selected],
  );
  const expectedSources = [
    [ids.prior, ids.priorReason, ids.supersession, ids.supersededStatus],
    [ids.selected, ids.selectedReason, ids.supersession, ids.selectedStatus],
    [ids.alternative, ids.alternativeReason],
    [ids.primary, ids.primarySelected, ids.primaryAlternative, ids.selection],
    [ids.unselected, ids.unselectedMembership],
  ];
  for (let row = 0; row < evidence.numRows; row += 1) {
    assert.deepEqual(
      [...evidence.getChild("source_record_uuids").get(row)].map(uuid).sort(),
      expectedSources[row].toSorted(),
    );
  }

  const projection = byQuestion.projection;
  assert.equal(
    projection.sourceRecordUuids.includes(ids.primaryAlternative),
    true,
  );
  assert.equal(
    projection.sourceRecordUuids.includes(ids.unselectedMembership),
    true,
  );
  for (let row = 0; row < evidence.numRows; row += 1) {
    assert.equal(
      uuid(evidence.getChild("source_generation_uuid").get(row)),
      projection.sourceGenerationUuid,
    );
    assert.equal(
      micros(evidence.getChild("transaction_cutoff_micros"), row),
      Number.MAX_SAFE_INTEGER,
    );
    assert.equal(micros(evidence.getChild("valid_time_micros"), row), 150);
    assert.equal(
      hex(evidence.getChild("policy_fingerprint").get(row)),
      projection.policyFingerprint,
    );
    assert.equal(
      hex(evidence.getChild("snapshot_fingerprint").get(row)),
      projection.snapshotFingerprint,
    );
    assert.equal(
      hex(evidence.getChild("valid_time_fingerprint").get(row)),
      projection.validTimeFingerprint,
    );
    assert.equal(
      hex(evidence.getChild("graph_content_fingerprint").get(row)),
      projection.graphContentFingerprint,
    );
  }

  const withoutValidTime = await forge.resolveBeliefSubject(
    request({ assertionUuid: ids.prior }, { validTimeMicros: undefined }),
  );
  assert.equal(
    withoutValidTime.projection.snapshotFingerprint,
    projection.snapshotFingerprint,
  );
  assert.equal(withoutValidTime.projection.validTimeFingerprint, null);
  assert.notEqual(
    withoutValidTime.projection.validTimeFingerprint,
    projection.validTimeFingerprint,
  );
  const noValidTimeEvidence = tableFromIPC(withoutValidTime.evidence);
  assert.equal(noValidTimeEvidence.numRows, 5);
  for (let row = 0; row < noValidTimeEvidence.numRows; row += 1) {
    assert.equal(
      noValidTimeEvidence.getChild("valid_time_micros").get(row),
      null,
    );
    assert.equal(
      micros(noValidTimeEvidence.getChild("transaction_cutoff_micros"), row),
      Number.MAX_SAFE_INTEGER,
    );
  }

  const validation = (error) => error.code === "ValidationError";
  await assert.rejects(
    async () => forge.resolveBeliefSubject(request({})),
    validation,
  );
  await assert.rejects(
    async () =>
      forge.resolveBeliefSubject(
        request({
          assertionUuid: ids.prior,
          hypothesisQuestionKey: "belief-subject.primary.v1",
        }),
      ),
    validation,
  );
  await assert.rejects(
    async () =>
      forge.resolveBeliefSubject({
        assertionUuid: ids.prior,
        transactionCutoffMicros: Number.MAX_SAFE_INTEGER,
      }),
    validation,
  );
  await assert.rejects(
    async () =>
      forge.resolveBeliefSubject(
        request(
          { assertionUuid: ids.prior },
          { policy: { includedStatuses: ["supported"] } },
        ),
      ),
    validation,
  );
  await assert.rejects(
    forge.resolveBeliefSubject(
      request({
        hypothesisQuestionKey: "belief-subject.missing.v1",
      }),
    ),
    (error) => error.code === "GF_NOT_FOUND",
  );
  await assert.rejects(
    forge.resolveBeliefSubject(
      request(
        { hypothesisQuestionKey: "belief-subject.primary.v1" },
        { policy: policy({ statusless: "reject" }) },
      ),
    ),
    (error) => error.code === "GF_AMBIGUOUS_PROJECTION",
  );

  const controller = new AbortController();
  const cancelled = forge.resolveBeliefSubject(
    request({ assertionUuid: ids.prior }, { signal: controller.signal }),
  );
  controller.abort();
  try {
    assert.equal(tableFromIPC((await cancelled).evidence).numRows, 5);
  } catch (error) {
    assert.equal(error.code, "GF_CANCELLED");
    assert.notEqual(error.name, "AbortError");
  }
});
