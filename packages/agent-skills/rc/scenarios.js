/**
 * Shared release-candidate agent-skills scenarios.
 *
 * Docs examples and CI tests import this module so fixtures cannot drift.
 */

import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import {
  AgentAdapterError,
  openProject,
  requestSubprocess,
  stableJson,
  tableToJson,
} from "../adapter/index.js";
import {
  bootstrapProject,
  buildKnowledge,
  exploreGraph,
  narrateBeliefRecords,
  resolveBeliefSubject,
  retrieveAnalyze,
} from "../workflows/index.js";
import { readBootstrapMarker } from "./query.js";
import { redactEvidence } from "./redact.js";

const PACKAGE_ROOT = dirname(fileURLToPath(new URL("../package.json", import.meta.url)));
const WORKFLOWS_SOURCE = readFileSync(join(PACKAGE_ROOT, "workflows/index.js"), "utf8");
const ADAPTER_SOURCE = readFileSync(join(PACKAGE_ROOT, "adapter/index.js"), "utf8");

export const BELIEF_POLICY = {
  hypotheses: "include_all_current_members",
  included_statuses: ["supported", "hypothesis"],
  statusless: "include",
  supersession_branches: "include_all_leaves",
  version: 1,
};

export const DESIGNED_ONLY_SURFACE = "CheckpointView.add_node";
export const QUESTION_KEY = "agent-skills.rc.cause.v1";

/** Stable UUIDv7-shaped identities for deterministic scenario inputs. */
export function scenarioUuids(count = 48) {
  return Array.from(
    { length: count },
    (_, index) => `018f0f4e-7b8c-7000-8000-${String(index + 1).padStart(12, "0")}`,
  );
}

export function buildKnowledgeInput(uuids, { m21 = true } = {}) {
  return {
    actor_uuid: uuids[1],
    assertion: {
      assertion_uuid: uuids[2],
      claim: "Ada knows Grace",
      graph_refs: [
        { graph_kind: "node", key: "ada", ordinal: 0, role: "subject" },
        { graph_kind: "node", key: "grace", ordinal: 0, role: "object" },
      ],
      operation_uuid: uuids[3],
    },
    capability_operation_uuids: {
      epistemic: uuids[4],
      knowledge: uuids[5],
      provenance: uuids[6],
    },
    confidence: {
      confidence_uuid: uuids[7],
      operation_uuid: uuids[8],
      policy: "explicit",
      value: 0.8,
    },
    edges: [
      {
        key: "knows",
        properties: { confidence: 0.25 },
        source_key: "ada",
        target_key: "grace",
        type: "KNOWS",
      },
    ],
    evidence: [
      {
        evidence_uuid: uuids[9],
        role: "supports",
        source_key: "ada",
        source_kind: "graph_node",
        weight: 0.9,
      },
    ],
    nodes: [
      {
        key: "ada",
        label: "Person",
        properties: { confidence: "domain", name: "Ada", summary: "graph systems" },
      },
      {
        key: "grace",
        label: "Person",
        properties: { name: "Grace", summary: "native bindings" },
      },
    ],
    reasoning: m21
      ? {
          content: "explicit evidence interpretation",
          content_format: "text/plain",
          kind: "evidence_interpretation",
          operation_uuid: uuids[10],
          reasoning_uuid: uuids[11],
        }
      : undefined,
    status: m21
      ? {
          operation_uuid: uuids[12],
          status: "hypothesis",
          status_event_uuid: uuids[13],
        }
      : undefined,
  };
}

/** Fail closed when skill sources reference designed-only product surfaces. */
export function assertNoDesignedOnlyReferences() {
  for (const [name, source] of [
    ["workflows", WORKFLOWS_SOURCE],
    ["adapter", ADAPTER_SOURCE],
  ]) {
    if (source.includes("CheckpointView.") || source.includes("designed-only")) {
      throw new Error(`${name} references a Designed-only surface`);
    }
    if (source.includes(DESIGNED_ONLY_SURFACE)) {
      throw new Error(`${name} references ${DESIGNED_ONLY_SURFACE}`);
    }
  }
}

/**
 * Analyst-agent scenario: bootstrap → build → explore → find → M18 → beliefs → reopen.
 */
export async function runAnalystScenario({
  GraphForge,
  tableFromIPC,
  projectPath,
  seedCompetingHypotheses,
  prepareSearchIndex,
}) {
  assertNoDesignedOnlyReferences();
  const uuids = scenarioUuids();
  const bootstrap = await bootstrapProject({
    GraphForge,
    path: projectPath,
    tableFromIPC,
  });
  const knowledge = await buildKnowledge({
    GraphForge,
    path: projectPath,
    tableFromIPC,
    input: buildKnowledgeInput(uuids),
  });
  const adaUuid = knowledge.nodes.find(({ key }) => key === "ada").uuid;
  const graceUuid = knowledge.nodes.find(({ key }) => key === "grace").uuid;

  const competing = await seedCompetingHypotheses({
    GraphForge,
    tableFromIPC,
    path: projectPath,
    priorAssertionUuid: uuids[2],
    priorProvenanceUuid: knowledge.assertion[0].provenance_uuid,
    priorReasoningUuid: uuids[11],
    uuids,
    nodeUuids: { ada: adaUuid, grace: graceUuid },
  });

  if (typeof prepareSearchIndex === "function") {
    await prepareSearchIndex({ GraphForge, tableFromIPC, path: projectPath });
  }

  const explore = await exploreGraph({
    GraphForge,
    path: projectPath,
    tableFromIPC,
    input: {
      depth: 1,
      mode: "neighborhood",
      result_limit: 16,
      start_uuids: [adaUuid],
      via: "KNOWS",
    },
  });

  const find = await retrieveAnalyze({
    GraphForge,
    path: projectPath,
    tableFromIPC,
    input: {
      label: "Person",
      query: "Ada",
      result_limit: 8,
      surface: "find",
    },
  });

  const rank = await retrieveAnalyze({
    GraphForge,
    path: projectPath,
    tableFromIPC,
    input: {
      algorithm: "pagerank",
      label: "Person",
      result_limit: 8,
      surface: "rank",
      via: "KNOWS",
    },
  });

  const belief = await resolveBeliefSubject({
    GraphForge,
    path: projectPath,
    tableFromIPC,
    input: {
      policy: BELIEF_POLICY,
      subject: { hypothesis_question_key: competing.question_key },
      transaction_cutoff_micros: Number.MAX_SAFE_INTEGER,
    },
  });

  const narration = await narrateBeliefRecords({
    GraphForge,
    path: projectPath,
    tableFromIPC,
    input: {
      page_limit: 50,
      policy: BELIEF_POLICY,
      record_budget: 256,
      subject: { hypothesis_question_key: competing.question_key },
      transaction_cutoff_micros: Number.MAX_SAFE_INTEGER,
    },
  });

  const reopened = await openProject({
    GraphForge,
    path: projectPath,
    requiredCapabilities: { graph: 1 },
    tableFromIPC,
  });
  const markerRows = await readBootstrapMarker(reopened.graph, tableFromIPC);
  reopened.graph.close();

  return redactEvidence({
    scenario: "analyst-agent",
    apis: [
      "bootstrapProject",
      "buildKnowledge",
      "exploreGraph",
      "retrieveAnalyze",
      "resolveBeliefSubject",
      "narrateBeliefRecords",
      "openProject",
    ],
    bootstrap: {
      created: bootstrap.created,
      marker_uuid: bootstrap.marker_uuid,
      ontology_mode: bootstrap.ontology_mode,
    },
    knowledge: {
      assertion_uuid: uuids[2],
      edge_uuids: knowledge.edges.map(({ uuid }) => uuid).sort(),
      node_uuids: knowledge.nodes.map(({ uuid }) => uuid).sort(),
      status_count: knowledge.status.length,
    },
    explore: {
      mode: explore.mode,
      result_limit: explore.result_limit,
      summary_count: explore.summary.length,
      truncated: explore.truncated,
    },
    find: {
      empty: find.empty,
      surface: find.surface,
      summary_count: find.summary.length,
      truncated: find.truncated,
    },
    rank: {
      empty: rank.empty,
      surface: rank.surface,
      summary_count: rank.summary.length,
      truncated: rank.truncated,
    },
    belief: {
      competing_member_count:
        belief.hypothesis_groups[0]?.current_member_assertion_uuids?.length ?? 0,
      selected_assertion_uuid: belief.hypothesis_groups[0]?.selected_assertion_uuid ?? null,
      subject_kind: belief.subject.kind,
      superseded_present: belief.assertions.some(
        (row) => (row.superseded_by_assertion_uuids?.length ?? 0) > 0,
      ),
    },
    narration: {
      assertion_count: narration.records.assertions.length,
      contract_version: narration.contract_version,
      has_projection_descriptors: Array.isArray(narration.projection_descriptors),
    },
    reopen: {
      marker_matches: (markerRows[0]?.node_uuid ?? null) === bootstrap.marker_uuid,
      marker_uuid: markerRows[0]?.node_uuid ?? null,
    },
    competing: {
      competing_assertion_uuid: competing.competing_assertion_uuid,
      group_uuid: competing.group_uuid,
      question_key: competing.question_key,
      replacement_assertion_uuid: competing.replacement_assertion_uuid,
    },
  });
}

/** Developer-agent scenario: embed package surfaces, Arrow/JSON, errors, reopen. */
export async function runDeveloperScenario({ GraphForge, tableFromIPC, projectPath }) {
  assertNoDesignedOnlyReferences();
  const bootstrap = await bootstrapProject({
    GraphForge,
    path: projectPath,
    tableFromIPC,
  });

  let missingCapabilityCode = null;
  try {
    await openProject({
      GraphForge,
      path: projectPath,
      requiredCapabilities: { graph: 1, knowledge: 1 },
      tableFromIPC,
    });
  } catch (error) {
    missingCapabilityCode = error.code;
  }

  let subprocessCode = null;
  try {
    requestSubprocess({ command: "SECRET_TOKEN_DO_NOT_ECHO" });
  } catch (error) {
    subprocessCode = error.code;
  }

  let designedOnlyCode = null;
  try {
    throw new AgentAdapterError(
      "GF_AGENT_CAPABILITY_UNSUPPORTED",
      `unsupported GraphForge capability version: ${DESIGNED_ONLY_SURFACE}`,
      {
        actual_status: "unsupported_future",
        capability_id: DESIGNED_ONLY_SURFACE,
        required_version: 1,
      },
    );
  } catch (error) {
    designedOnlyCode = error.code;
  }

  const opened = await openProject({
    GraphForge,
    path: projectPath,
    requiredCapabilities: { graph: 1 },
    tableFromIPC,
  });
  const rows = await readBootstrapMarker(opened.graph, tableFromIPC);
  const encoded = stableJson({
    marker_uuid: rows[0]?.node_uuid ?? null,
    row_count: rows.length,
  });
  opened.graph.close();

  const reopened = await openProject({
    GraphForge,
    path: projectPath,
    requiredCapabilities: { graph: 1 },
    tableFromIPC,
  });
  const again = await readBootstrapMarker(reopened.graph, tableFromIPC);
  reopened.graph.close();

  return redactEvidence({
    scenario: "developer-agent",
    apis: ["bootstrapProject", "openProject", "tableToJson", "stableJson", "requestSubprocess"],
    bootstrap: {
      created: bootstrap.created,
      marker_uuid: bootstrap.marker_uuid,
    },
    arrow_json: {
      encoded,
      row_count: rows.length,
    },
    errors: {
      designed_only: designedOnlyCode,
      missing_capability: missingCapabilityCode,
      subprocess: subprocessCode,
    },
    reopen: {
      marker_matches: (again[0]?.node_uuid ?? null) === bootstrap.marker_uuid,
      marker_uuid: again[0]?.node_uuid ?? null,
    },
  });
}

export function evidenceEnvelope({
  commitSha,
  packageVersion,
  nodeVersion,
  graphforgeVersion,
  analyst,
  developer,
  pack,
}) {
  return redactEvidence({
    schema_version: 1,
    kind: "graphforge-agent-skills-rc-e2e-v1",
    commit_sha: commitSha,
    versions: {
      agent_skills: packageVersion,
      graphforge: graphforgeVersion,
      node: nodeVersion,
    },
    pack,
    scenarios: {
      analyst,
      developer,
    },
  });
}
