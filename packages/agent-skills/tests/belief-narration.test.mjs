import assert from "node:assert/strict";
import { mkdtempSync, realpathSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { narrateBeliefRecords } from "../workflows/index.js";

const UUIDS = Array.from(
  { length: 40 },
  (_, index) =>
    `018f0f4e-7b8c-7000-8000-${String(index + 1).padStart(12, "0")}`,
);

function table(rows, next = null) {
  const fields = [...new Set(rows.flatMap((row) => Object.keys(row)))];
  const metadata = new Map();
  if (next) metadata.set("graphforge.next_page_token", next);
  return {
    getChild: (name) => ({ get: (index) => rows[index]?.[name] }),
    numRows: rows.length,
    schema: {
      fields: fields.map((name) => ({ name })),
      metadata,
    },
  };
}

function project(evidence, pages, overrides = {}) {
  const path = realpathSync(
    mkdtempSync(join(tmpdir(), "graphforge-belief-narration-")),
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
      return {
        evidence: table(evidence),
        projection: {
          fingerprint: "projection",
          graphContentFingerprint: "11".repeat(32),
          policyFingerprint: "22".repeat(32),
          snapshotFingerprint: "33".repeat(32),
          sourceGenerationUuid: UUIDS[20],
          sourceRecordUuids: [UUIDS[9], UUIDS[2]],
          prepareRankInvocation() {
            return {
              algorithm: "degree",
              fingerprint: "aa".repeat(32),
              verb: "rank",
            };
          },
        },
      };
    }

    async assertion(uuid) {
      this.calls.push(["assertion", uuid]);
      return table(
        pages.assertions.filter((row) => row.assertion_uuid === uuid),
      );
    }

    async assertionGraphRefs(uuid, request = {}) {
      this.calls.push(["assertionGraphRefs", uuid, request]);
      return paged(pages.graph_refs, request);
    }

    async listAssertionStatus(request = {}) {
      this.calls.push(["listAssertionStatus", request]);
      return paged(
        pages.status.filter(
          (row) => row.assertion_uuid === request.assertionUuid,
        ),
        request,
      );
    }

    async listAssertionValidity(request = {}) {
      this.calls.push(["listAssertionValidity", request]);
      return paged(
        pages.validity.filter(
          (row) => row.assertion_uuid === request.assertionUuid,
        ),
        request,
      );
    }

    async listAssertionSupersessions(request = {}) {
      this.calls.push(["listAssertionSupersessions", request]);
      const rows = pages.supersessions.filter(
        (row) =>
          row.prior_assertion_uuid === request.priorAssertionUuid ||
          row.replacement_assertion_uuid === request.replacementAssertionUuid,
      );
      return paged(rows, request);
    }

    async listConfidenceAssessments(request = {}) {
      this.calls.push(["listConfidenceAssessments", request]);
      return paged(
        pages.confidence.filter(
          (row) => row.assertion_uuid === request.assertionUuid,
        ),
        request,
      );
    }

    async confidenceInputs(uuid, request = {}) {
      this.calls.push(["confidenceInputs", uuid, request]);
      return paged(
        pages.confidence_inputs.filter((row) => row.confidence_uuid === uuid),
        request,
      );
    }

    async listEvidenceLinks(request = {}) {
      this.calls.push(["listEvidenceLinks", request]);
      return paged(
        pages.evidence.filter(
          (row) => row.assertion_uuid === request.assertionUuid,
        ),
        request,
      );
    }

    async listReasoning(request = {}) {
      this.calls.push(["listReasoning", request]);
      return paged(
        pages.reasoning.filter(
          (row) => row.assertion_uuid === request.assertionUuid,
        ),
        request,
      );
    }

    async listProvenanceHistory(request = {}) {
      this.calls.push(["listProvenanceHistory", request]);
      return paged(
        pages.provenance.filter(
          (row) => row.subject_uuid === request.subjectUuid,
        ),
        request,
      );
    }

    async listHypothesisGroups(request = {}) {
      this.calls.push(["listHypothesisGroups", request]);
      return paged(
        pages.groups.filter((row) => row.question_key === request.questionKey),
        request,
      );
    }

    async listHypothesisMembership(request = {}) {
      this.calls.push(["listHypothesisMembership", request]);
      return paged(
        pages.membership.filter((row) => row.group_uuid === request.groupUuid),
        request,
      );
    }

    async listHypothesisSelection(request = {}) {
      this.calls.push(["listHypothesisSelection", request]);
      return paged(
        pages.selection.filter((row) => row.group_uuid === request.groupUuid),
        request,
      );
    }

    async invokeResolvedRecorded(projection, request) {
      this.calls.push(["invokeResolvedRecorded", projection, request]);
      if (overrides.recordedError) throw overrides.recordedError;
      return {
        attachment: overrides.attachmentFailure
          ? undefined
          : table([{ attachment_uuid: request.attachmentUuid }]),
        attachmentErrorCode: overrides.attachmentFailure
          ? "GF_ATTACHMENT_FAILED"
          : undefined,
        attachmentState: overrides.attachmentFailure
          ? "attachment_failed"
          : "attached",
        attachmentUuid: request.attachmentUuid,
        result: table([{ node_uuid: UUIDS[30], score: 1 }]),
        runUuid: request.runUuid,
      };
    }

    async algorithmRun(runUuid) {
      this.calls.push(["algorithmRun", runUuid]);
      return table([
        { algorithm: "rank.degree", run_uuid: runUuid, state: "completed" },
      ]);
    }

    async algorithmRunEvents(runUuid) {
      this.calls.push(["algorithmRunEvents", runUuid]);
      return table([
        { run_uuid: runUuid, state: "started" },
        { run_uuid: runUuid, state: "completed" },
      ]);
    }

    close() {
      this.closed = true;
    }
  }
  return { GraphForge, path };
}

function paged(rows, request) {
  const limit = request.limit ?? 100;
  const start = request.after
    ? rows.findIndex((row) => JSON.stringify(row) === request.after) + 1
    : 0;
  const slice = rows.slice(start, start + limit);
  const next =
    start + limit < rows.length
      ? JSON.stringify(slice[slice.length - 1])
      : null;
  return table(slice, next);
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

const pages = {
  assertions: [
    { assertion_uuid: UUIDS[0], claim: "prior" },
    { assertion_uuid: UUIDS[1], claim: "current" },
  ],
  confidence: [
    {
      assertion_uuid: UUIDS[1],
      confidence_uuid: UUIDS[15],
      value: 0.4,
    },
  ],
  confidence_inputs: [{ confidence_uuid: UUIDS[15], ordinal: 0 }],
  evidence: [{ assertion_uuid: UUIDS[1], evidence_uuid: UUIDS[16] }],
  graph_refs: [
    { assertion_uuid: UUIDS[0], graph_uuid: UUIDS[21], role: "subject" },
    { assertion_uuid: UUIDS[1], graph_uuid: UUIDS[22], role: "subject" },
  ],
  groups: [
    {
      group_uuid: UUIDS[3],
      question_key: "cause.primary.v1",
    },
  ],
  membership: [
    {
      assertion_uuid: UUIDS[1],
      group_uuid: UUIDS[3],
      membership_event_uuid: UUIDS[17],
    },
    {
      assertion_uuid: UUIDS[2],
      group_uuid: UUIDS[3],
      membership_event_uuid: UUIDS[18],
    },
  ],
  provenance: [{ provenance_uuid: UUIDS[19], subject_uuid: UUIDS[1] }],
  reasoning: [{ assertion_uuid: UUIDS[1], reasoning_uuid: UUIDS[14] }],
  selection: [
    {
      group_uuid: UUIDS[3],
      selected_assertion_uuid: null,
      selection_event_uuid: UUIDS[23],
    },
  ],
  status: [
    {
      assertion_uuid: UUIDS[1],
      status: "hypothesis",
      status_event_uuid: UUIDS[10],
    },
  ],
  supersessions: [
    {
      prior_assertion_uuid: UUIDS[0],
      replacement_assertion_uuid: UUIDS[1],
      supersession_event_uuid: UUIDS[24],
    },
  ],
  validity: [
    {
      assertion_uuid: UUIDS[1],
      valid_from_micros: 1,
      validity_event_uuid: UUIDS[25],
    },
  ],
};

test("narration returns scoped histories and project descriptors", async () => {
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
  const { GraphForge, path } = project(evidence, pages);
  const result = await narrateBeliefRecords({
    GraphForge,
    tableFromIPC: (value) => value,
    path,
    input: {
      page_limit: 1,
      policy,
      subject: { assertion_uuid: UUIDS[0] },
      transaction_cutoff_micros: 900,
    },
  });

  assert.equal(result.contract_version, 1);
  assert.deepEqual(result.scoped_assertion_uuids, [
    UUIDS[0],
    UUIDS[1],
    UUIDS[2],
  ]);
  assert.equal(result.records.assertion_status[0].status, "hypothesis");
  assert.equal(result.records.assertion_supersessions.length, 1);
  assert.equal(result.records.hypothesis_membership.length, 2);
  assert.equal(
    result.records.hypothesis_selection[0].selected_assertion_uuid,
    null,
  );
  assert.ok(
    result.projection_descriptors.some(
      (row) => row.collection === "assertions" && row.api === "listAssertions",
    ),
  );
  assert.equal(GraphForge.instance.closed, true);
  assert.ok(
    GraphForge.instance.calls.some(([name]) => name === "listAssertionStatus"),
  );
});

test("narration fails closed when the caller record budget is exceeded", async () => {
  const evidence = [
    assertion({ uuid: UUIDS[0] }),
    group({
      uuid: UUIDS[3],
      question: "cause.primary.v1",
      members: [UUIDS[0]],
    }),
  ];
  const { GraphForge, path } = project(evidence, pages);
  await assert.rejects(
    narrateBeliefRecords({
      GraphForge,
      tableFromIPC: (value) => value,
      path,
      input: {
        policy,
        record_budget: 1,
        subject: { assertion_uuid: UUIDS[0] },
        transaction_cutoff_micros: 1,
      },
    }),
    (error) => {
      assert.equal(error.code, "GF_AGENT_BELIEF_RECORD_BUDGET_EXCEEDED");
      assert.deepEqual(error.details, { record_budget: 1 });
      return true;
    },
  );
});
