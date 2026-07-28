import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";

import { QUESTION_KEY, scenarioUuids } from "./scenarios.js";

function table(rows, next = null) {
  const fields = [...new Set(rows.flatMap((row) => Object.keys(row)))];
  return {
    getChild: (name) => ({ get: (index) => rows[index]?.[name] }),
    numRows: rows.length,
    schema: {
      fields: fields.map((name) => ({ name })),
      metadata: next
        ? { get: (key) => (key === "graphforge.next_page_token" ? next : null) }
        : { get: () => null },
    },
  };
}

function paged(rows, request = {}) {
  const limit = request.limit ?? 100;
  const start = request.after
    ? rows.findIndex((row) => JSON.stringify(row) === request.after) + 1
    : 0;
  const slice = rows.slice(start, start + limit);
  const next = start + limit < rows.length ? JSON.stringify(slice[slice.length - 1]) : null;
  return table(slice, next);
}

/**
 * Deterministic GraphForge stand-in for RC golden tests (PR CI).
 * Native RC evidence uses the real `@graphforge/node` binding instead.
 */
export function createMockProject(projectPath) {
  mkdirSync(projectPath, { recursive: true });
  writeFileSync(join(projectPath, "FORMAT"), "graphforge-project/v1\n");
  const uuids = scenarioUuids();
  const state = {
    capabilities: new Map([
      ["graph", 1],
      ["workspace", 1],
    ]),
    edges: [],
    marker: undefined,
    nodes: [],
    ontologyMode: "exploratory",
    belief: null,
    pages: {
      assertions: [],
      confidence: [],
      confidence_inputs: [],
      evidence: [],
      graph_refs: [],
      groups: [],
      membership: [],
      provenance: [],
      reasoning: [],
      selection: [],
      status: [],
      supersessions: [],
      validity: [],
    },
  };

  class GraphForge {
    constructor(openedPath) {
      if (openedPath !== projectPath) {
        throw Object.assign(new Error("path mismatch"), { code: "GF_STORAGE" });
      }
      this.openedPath = openedPath;
      this.ontologyMode = state.ontologyMode;
      this.calls = [];
    }

    async projectCapabilities() {
      return table(
        [...state.capabilities].map(([capability_id, capability_version]) => ({
          capability_id,
          capability_version,
          status: "supported",
        })),
      );
    }

    execute(query) {
      if (String(query).includes("GraphForgeBootstrap")) {
        return table(state.marker ? [{ node_uuid: state.marker }] : []);
      }
      return table([]);
    }

    addNode(label, properties) {
      const uuid = uuids[20 + state.nodes.length];
      state.nodes.push({ label, properties, uuid });
      if (label === "GraphForgeBootstrap") state.marker = uuid;
      return { uuid };
    }

    addEdge(source, type, target, properties = {}) {
      const uuid = uuids[30 + state.edges.length];
      state.edges.push({
        properties,
        source: source.uuid,
        target: target.uuid,
        type,
        uuid,
      });
      return { uuid };
    }

    async enableCapability(request) {
      state.capabilities.set(request.capabilityId, request.capabilityVersion);
      return table([
        {
          capability_id: request.capabilityId,
          capability_version: request.capabilityVersion,
        },
      ]);
    }

    async createAssertionWithEvidence(request) {
      state.pages.assertions.push({
        assertion_uuid: request.assertionUuid,
        claim: request.claim,
      });
      return table([
        {
          assertion_uuid: request.assertionUuid,
          provenance_uuid: uuids[40],
        },
      ]);
    }

    async assessConfidence(request) {
      state.pages.confidence.push({
        assertion_uuid: request.assertionUuid,
        confidence_uuid: request.confidenceUuid,
        value: request.value,
      });
      return table([
        {
          confidence_uuid: request.confidenceUuid,
          value: request.value,
        },
      ]);
    }

    async recordReasoning(request) {
      state.pages.reasoning.push({
        assertion_uuid: request.assertionUuid,
        reasoning_uuid: request.reasoningUuid,
      });
      return table([{ reasoning_uuid: request.reasoningUuid }]);
    }

    async recordAssertionStatus(request) {
      state.pages.status.push({
        assertion_uuid: request.assertionUuid,
        status: request.status,
        status_event_uuid: request.statusEventUuid,
      });
      return table([
        {
          assertion_uuid: request.assertionUuid,
          status: request.status,
          status_event_uuid: request.statusEventUuid,
        },
      ]);
    }

    preparePathsInvocation(source, target, by, via, directed, k, weight, heuristic, walkLength) {
      this.calls.push([
        "preparePathsInvocation",
        { by, directed, source, target, via, walkLength },
      ]);
      return {
        algorithm: by,
        fingerprint: "aa".repeat(32),
        verb: "paths",
        walkLength,
      };
    }

    invokeDescriptor(descriptor) {
      this.calls.push(["invokeDescriptor", descriptor]);
      if (descriptor.verb === "rank") {
        return table([
          { node_uuid: state.nodes[0]?.uuid ?? uuids[20], score: 1 },
          { node_uuid: state.nodes[1]?.uuid ?? uuids[21], score: 0.5 },
        ]);
      }
      const source = state.nodes.find((node) => node.uuid)?.uuid;
      return table([
        {
          cost: 1,
          source_uuid: source ?? uuids[20],
          target_uuid: state.nodes[1]?.uuid ?? uuids[21],
        },
      ]);
    }

    prepareRankInvocation(label, algorithm, via, directed) {
      this.calls.push(["prepareRankInvocation", { algorithm, directed, label, via }]);
      return {
        algorithm,
        fingerprint: "bb".repeat(32),
        verb: "rank",
      };
    }

    find(query, label) {
      this.calls.push(["find", { label, query }]);
      const hit = state.nodes.find(
        (node) =>
          node.label === label && String(node.properties?.name ?? "").includes(String(query ?? "")),
      );
      return table(hit ? [{ node_uuid: hit.uuid, score: 1 }] : []);
    }

    async resolveBeliefSubject(request) {
      this.calls.push(["resolveBeliefSubject", request]);
      if (!state.belief) {
        throw Object.assign(new Error("belief not seeded"), {
          code: "GF_BELIEF_MISSING",
        });
      }
      return {
        evidence: table(state.belief.evidence),
        projection: {
          graphContentFingerprint: "11".repeat(32),
          policyFingerprint: "22".repeat(32),
          snapshotFingerprint: "33".repeat(32),
          sourceGenerationUuid: uuids[41],
          sourceRecordUuids: state.belief.evidence.flatMap((row) => row.source_record_uuids),
        },
      };
    }

    async assertion(uuid) {
      return table(state.pages.assertions.filter((row) => row.assertion_uuid === uuid));
    }

    async assertionGraphRefs(uuid, request = {}) {
      return paged(
        state.pages.graph_refs.filter((row) => row.assertion_uuid === uuid),
        request,
      );
    }

    async listAssertionStatus(request = {}) {
      return paged(
        state.pages.status.filter((row) => row.assertion_uuid === request.assertionUuid),
        request,
      );
    }

    async listAssertionValidity(request = {}) {
      return paged(
        state.pages.validity.filter((row) => row.assertion_uuid === request.assertionUuid),
        request,
      );
    }

    async listAssertionSupersessions(request = {}) {
      return paged(
        state.pages.supersessions.filter(
          (row) =>
            row.prior_assertion_uuid === request.priorAssertionUuid ||
            row.replacement_assertion_uuid === request.replacementAssertionUuid,
        ),
        request,
      );
    }

    async listConfidenceAssessments(request = {}) {
      return paged(
        state.pages.confidence.filter((row) => row.assertion_uuid === request.assertionUuid),
        request,
      );
    }

    async confidenceInputs(uuid, request = {}) {
      return paged(
        state.pages.confidence_inputs.filter((row) => row.confidence_uuid === uuid),
        request,
      );
    }

    async listEvidenceLinks(request = {}) {
      return paged(
        state.pages.evidence.filter((row) => row.assertion_uuid === request.assertionUuid),
        request,
      );
    }

    async listReasoning(request = {}) {
      return paged(
        state.pages.reasoning.filter((row) => row.assertion_uuid === request.assertionUuid),
        request,
      );
    }

    async listProvenanceHistory(request = {}) {
      return paged(
        state.pages.provenance.filter((row) => row.subject_uuid === request.subjectUuid),
        request,
      );
    }

    async listHypothesisGroups(request = {}) {
      return paged(
        state.pages.groups.filter(
          (row) => request.questionKey === undefined || row.question_key === request.questionKey,
        ),
        request,
      );
    }

    async listHypothesisMembership(request = {}) {
      return paged(
        state.pages.membership.filter((row) => row.group_uuid === request.groupUuid),
        request,
      );
    }

    async listHypothesisSelection(request = {}) {
      return paged(
        state.pages.selection.filter((row) => row.group_uuid === request.groupUuid),
        request,
      );
    }

    close() {
      this.closed = true;
    }
  }

  async function seedCompetingHypotheses({
    priorAssertionUuid,
    priorProvenanceUuid: _priorProvenanceUuid,
    priorReasoningUuid: _priorReasoningUuid,
    uuids: ids,
    nodeUuids,
  }) {
    const competingAssertionUuid = ids[14];
    const replacementAssertionUuid = ids[15];
    const groupUuid = ids[16];
    state.capabilities.set("knowledge", 1);
    state.capabilities.set("epistemic", 1);
    state.capabilities.set("provenance", 1);
    state.pages.assertions.push(
      { assertion_uuid: competingAssertionUuid, claim: "competing cause" },
      { assertion_uuid: replacementAssertionUuid, claim: "replacement cause" },
    );
    state.pages.graph_refs.push(
      {
        assertion_uuid: priorAssertionUuid,
        graph_uuid: nodeUuids.ada,
        role: "subject",
      },
      {
        assertion_uuid: competingAssertionUuid,
        graph_uuid: nodeUuids.grace,
        role: "subject",
      },
      {
        assertion_uuid: replacementAssertionUuid,
        graph_uuid: nodeUuids.ada,
        role: "subject",
      },
    );
    state.pages.status.push({
      assertion_uuid: priorAssertionUuid,
      status: "hypothesis",
      status_event_uuid: ids[13],
    });
    state.pages.supersessions.push({
      prior_assertion_uuid: priorAssertionUuid,
      replacement_assertion_uuid: replacementAssertionUuid,
      supersession_uuid: ids[17],
    });
    state.pages.groups.push({
      group_uuid: groupUuid,
      question_key: QUESTION_KEY,
    });
    state.pages.membership.push(
      {
        assertion_uuid: competingAssertionUuid,
        group_uuid: groupUuid,
        membership_event_uuid: ids[18],
      },
      {
        assertion_uuid: replacementAssertionUuid,
        group_uuid: groupUuid,
        membership_event_uuid: ids[19],
      },
    );
    state.pages.evidence.push({
      assertion_uuid: priorAssertionUuid,
      evidence_uuid: ids[9],
    });
    state.pages.provenance.push({
      provenance_uuid: ids[40],
      subject_uuid: priorAssertionUuid,
    });
    state.belief = {
      evidence: [
        {
          assertion_uuid: priorAssertionUuid,
          current_member_assertion_uuids: [],
          entity_kind: "assertion",
          group_uuid: null,
          question_key: null,
          reasoning_history_uuids: [ids[11]],
          reasoning_leaf_uuids: [ids[11]],
          selected_assertion_uuid: null,
          source_record_uuids: [priorAssertionUuid, ids[13]],
          status: "hypothesis",
          status_event_uuid: ids[13],
          superseded_by_assertion_uuids: [replacementAssertionUuid],
        },
        {
          assertion_uuid: competingAssertionUuid,
          current_member_assertion_uuids: [],
          entity_kind: "assertion",
          group_uuid: null,
          question_key: null,
          reasoning_history_uuids: [],
          reasoning_leaf_uuids: [],
          selected_assertion_uuid: null,
          source_record_uuids: [competingAssertionUuid],
          status: "hypothesis",
          status_event_uuid: null,
          superseded_by_assertion_uuids: [],
        },
        {
          assertion_uuid: replacementAssertionUuid,
          current_member_assertion_uuids: [],
          entity_kind: "assertion",
          group_uuid: null,
          question_key: null,
          reasoning_history_uuids: [],
          reasoning_leaf_uuids: [],
          selected_assertion_uuid: null,
          source_record_uuids: [replacementAssertionUuid],
          status: "hypothesis",
          status_event_uuid: null,
          superseded_by_assertion_uuids: [],
        },
        {
          assertion_uuid: null,
          current_member_assertion_uuids: [competingAssertionUuid, replacementAssertionUuid],
          entity_kind: "hypothesis_group",
          group_uuid: groupUuid,
          question_key: QUESTION_KEY,
          reasoning_history_uuids: [],
          reasoning_leaf_uuids: [],
          selected_assertion_uuid: null,
          source_record_uuids: [groupUuid],
          status: null,
          status_event_uuid: null,
          superseded_by_assertion_uuids: [],
        },
      ],
    };
    return {
      competing_assertion_uuid: competingAssertionUuid,
      group_uuid: groupUuid,
      question_key: QUESTION_KEY,
      replacement_assertion_uuid: replacementAssertionUuid,
    };
  }

  return {
    GraphForge,
    seedCompetingHypotheses,
    state,
    tableFromIPC: (value) => value,
  };
}
