import assert from "node:assert/strict";
import { mkdtempSync, realpathSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { resolveBeliefSubject } from "../workflows/index.js";

const UUIDS = Array.from(
  { length: 24 },
  (_, index) =>
    `018f0f4e-7b8c-7000-8000-${String(index + 1).padStart(12, "0")}`,
);

function table(rows) {
  const fields = [...new Set(rows.flatMap((row) => Object.keys(row)))];
  return {
    getChild: (name) => ({ get: (index) => rows[index]?.[name] }),
    numRows: rows.length,
    schema: { fields: fields.map((name) => ({ name })) },
  };
}

function project(evidence, overrides = {}) {
  const path = realpathSync(
    mkdtempSync(join(tmpdir(), "graphforge-belief-subject-")),
  );
  writeFileSync(join(path, "FORMAT"), "graphforge-project/v1\n");
  class GraphForge {
    static instance;

    constructor(openedPath) {
      assert.equal(openedPath, path);
      this.calls = [];
      GraphForge.instance = this;
    }

    async projectCapabilities() {
      return table(
        ["graph", "knowledge", "epistemic"].map((capability_id) => ({
          capability_id,
          capability_version: 1,
          status: "supported",
        })),
      );
    }

    async resolveBeliefSubject(request) {
      this.calls.push(["resolveBeliefSubject", request]);
      if (overrides.resolveError) throw overrides.resolveError;
      return {
        evidence: table(evidence),
        projection: {
          graphContentFingerprint: "11".repeat(32),
          policyFingerprint: "22".repeat(32),
          snapshotFingerprint: "33".repeat(32),
          sourceGenerationUuid: UUIDS[20],
          sourceRecordUuids: [UUIDS[9], UUIDS[2]],
          validTimeFingerprint:
            request.validTimeMicros === undefined ? undefined : "44".repeat(32),
        },
      };
    }

    close() {
      this.closed = true;
    }
  }
  return { GraphForge, path };
}

const assertion = ({
  uuid,
  status = null,
  statusEvent = null,
  supersededBy = [],
  sources = [uuid],
}) => ({
  assertion_uuid: uuid,
  current_member_assertion_uuids: [],
  entity_kind: "assertion",
  group_uuid: null,
  question_key: null,
  reasoning_history_uuids: [],
  reasoning_leaf_uuids: [],
  selected_assertion_uuid: null,
  source_record_uuids: sources,
  status,
  status_event_uuid: statusEvent,
  superseded_by_assertion_uuids: supersededBy,
});

const group = ({
  uuid,
  question,
  members,
  selected = null,
  sources = [uuid],
}) => ({
  assertion_uuid: null,
  current_member_assertion_uuids: members,
  entity_kind: "hypothesis_group",
  group_uuid: uuid,
  question_key: question,
  reasoning_history_uuids: [],
  reasoning_leaf_uuids: [],
  selected_assertion_uuid: selected,
  source_record_uuids: sources,
  status: null,
  status_event_uuid: null,
  superseded_by_assertion_uuids: [],
});

const policy = {
  hypotheses: "include_all_current_members",
  included_statuses: ["supported", "hypothesis"],
  statusless: "include",
  supersession_branches: "include_all_leaves",
  version: 1,
};

test("assertion subject preserves statusless, competing, and superseded identities", async () => {
  const evidence = [
    assertion({ uuid: UUIDS[0], supersededBy: [UUIDS[1]] }),
    assertion({
      uuid: UUIDS[1],
      status: "hypothesis",
      statusEvent: UUIDS[10],
      sources: [UUIDS[1], UUIDS[10]],
    }),
    assertion({ uuid: UUIDS[2], status: "supported" }),
    group({
      uuid: UUIDS[3],
      question: "cause.primary.v1",
      members: [UUIDS[1], UUIDS[2]],
      sources: [UUIDS[3], UUIDS[11]],
    }),
  ];
  const { GraphForge, path } = project(evidence);
  const result = await resolveBeliefSubject({
    GraphForge,
    tableFromIPC: (value) => value,
    path,
    input: {
      policy,
      subject: { assertion_uuid: UUIDS[0] },
      transaction_cutoff_micros: 900,
    },
  });

  assert.deepEqual(
    result.assertions.map((row) => row.assertion_uuid),
    [UUIDS[0], UUIDS[1], UUIDS[2]],
  );
  assert.equal(result.assertions[0].status, null);
  assert.deepEqual(result.hypothesis_groups[0].current_member_assertion_uuids, [
    UUIDS[1],
    UUIDS[2],
  ]);
  assert.equal(result.hypothesis_groups[0].selected_assertion_uuid, null);
  assert.deepEqual(result.subject, {
    assertion_uuid: UUIDS[0],
    kind: "assertion",
  });
  assert.deepEqual(result.subject_source_record_uuids, [
    UUIDS[0],
    UUIDS[1],
    UUIDS[2],
    UUIDS[3],
    UUIDS[10],
    UUIDS[11],
  ]);
  assert.equal(result.projection_evidence.source_generation_uuid, UUIDS[20]);
  assert.equal(result.projection_evidence.policy_fingerprint, "22".repeat(32));
  assert.equal(result.valid_time_micros, null);
  assert.deepEqual(GraphForge.instance.calls, [
    [
      "resolveBeliefSubject",
      {
        assertionUuid: UUIDS[0],
        policy: {
          hypotheses: "include_all_current_members",
          includedStatuses: ["supported", "hypothesis"],
          statusless: "include",
          supersessionBranches: "include_all_leaves",
        },
        transactionCutoffMicros: 900,
        validTimeMicros: undefined,
      },
    ],
  ]);
  assert.equal(GraphForge.instance.closed, true);
});

test("question subject keeps cutoff and valid time independent from explicit selection", async () => {
  const evidence = [
    assertion({ uuid: UUIDS[4], status: "hypothesis" }),
    assertion({ uuid: UUIDS[5], status: "hypothesis" }),
    group({
      uuid: UUIDS[6],
      question: "route.primary.v1",
      members: [UUIDS[4], UUIDS[5]],
      selected: UUIDS[5],
      sources: [UUIDS[6], UUIDS[12], UUIDS[13]],
    }),
  ];
  const { GraphForge, path } = project(evidence);
  const result = await resolveBeliefSubject({
    GraphForge,
    tableFromIPC: (value) => value,
    path,
    input: {
      policy,
      subject: { hypothesis_question_key: "route.primary.v1" },
      transaction_cutoff_micros: 1_000,
      valid_time_micros: 400,
    },
  });

  assert.deepEqual(result.subject, {
    group_uuid: UUIDS[6],
    hypothesis_question_key: "route.primary.v1",
    kind: "hypothesis_question",
  });
  assert.deepEqual(
    result.assertions.map((row) => row.assertion_uuid),
    [UUIDS[4], UUIDS[5]],
  );
  assert.equal(result.transaction_cutoff_micros, "1000");
  assert.equal(result.valid_time_micros, "400");
  assert.equal(
    result.projection_evidence.valid_time_fingerprint,
    "44".repeat(32),
  );
  assert.deepEqual(GraphForge.instance.calls[0][1], {
    hypothesisQuestionKey: "route.primary.v1",
    policy: {
      hypotheses: "include_all_current_members",
      includedStatuses: ["supported", "hypothesis"],
      statusless: "include",
      supersessionBranches: "include_all_leaves",
    },
    transactionCutoffMicros: 1_000,
    validTimeMicros: 400,
  });
});

test("missing policy or invalid subject returns a stable structured error", async () => {
  const { GraphForge, path } = project([]);
  await assert.rejects(
    resolveBeliefSubject({
      GraphForge,
      tableFromIPC: (value) => value,
      path,
      input: {
        subject: { assertion_uuid: UUIDS[0] },
        transaction_cutoff_micros: 1,
      },
    }),
    (error) => {
      assert.equal(error.code, "GF_AGENT_BELIEF_POLICY_REQUIRED");
      assert.deepEqual(error.toJSON().details, { required_policy_version: 1 });
      return true;
    },
  );
  await assert.rejects(
    resolveBeliefSubject({
      GraphForge,
      tableFromIPC: (value) => value,
      path,
      input: {
        policy: { ...policy, hypotheses: undefined },
        subject: { assertion_uuid: UUIDS[0] },
        transaction_cutoff_micros: 1,
      },
    }),
    { code: "GF_AGENT_BELIEF_POLICY_REQUIRED" },
  );
  await assert.rejects(
    resolveBeliefSubject({
      GraphForge,
      tableFromIPC: (value) => value,
      path,
      input: {
        policy,
        subject: {
          assertion_uuid: UUIDS[0],
          hypothesis_question_key: "ambiguous.v1",
        },
        transaction_cutoff_micros: 1,
      },
    }),
    { code: "GF_AGENT_BELIEF_SUBJECT_REQUIRED" },
  );
});

test("native ambiguity remains structured and does not trigger confidence selection", async () => {
  const native = Object.assign(new Error("private native detail"), {
    code: "GF_AMBIGUOUS",
  });
  const { GraphForge, path } = project(
    [assertion({ uuid: UUIDS[7], status: "hypothesis" })],
    { resolveError: native },
  );
  await assert.rejects(
    resolveBeliefSubject({
      GraphForge,
      tableFromIPC: (value) => value,
      path,
      input: {
        policy: { ...policy, hypotheses: "require_selected" },
        subject: { assertion_uuid: UUIDS[7] },
        transaction_cutoff_micros: 2,
      },
    }),
    (error) => {
      assert.equal(error.code, "GF_AMBIGUOUS");
      assert.equal(error.message, "GraphForge operation failed");
      assert.equal(JSON.stringify(error), JSON.stringify(error.toJSON()));
      assert.doesNotMatch(JSON.stringify(error), /private native detail/);
      return true;
    },
  );
});

test("question subject fails closed when native evidence omits the addressed group", async () => {
  const { GraphForge, path } = project([
    group({
      uuid: UUIDS[8],
      question: "different.question.v1",
      members: [UUIDS[7]],
    }),
  ]);
  await assert.rejects(
    resolveBeliefSubject({
      GraphForge,
      tableFromIPC: (value) => value,
      path,
      input: {
        policy,
        subject: { hypothesis_question_key: "requested.question.v1" },
        transaction_cutoff_micros: 3,
      },
    }),
    {
      code: "GF_AGENT_BELIEF_EVIDENCE_INVALID",
      message:
        "native belief-subject evidence must contain exactly one addressed hypothesis group",
    },
  );
  assert.equal(GraphForge.instance.closed, true);
});
