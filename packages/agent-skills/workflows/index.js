import { mkdir } from "node:fs/promises";

import {
  AgentAdapterError,
  normalizeGraphForgeError,
  openProject,
  requireCapabilities,
  tableToJson,
  uuidToString,
  validateProjectPath,
} from "../adapter/index.js";

const BOOTSTRAP_QUERY =
  "MATCH (n:GraphForgeBootstrap {key: 'agent-skills/v1'}) RETURN n.node_uuid AS node_uuid";

export async function bootstrapProject({
  GraphForge,
  tableFromIPC,
  path,
  cwd,
  ontologyMode = "exploratory",
  ontologyPath,
}) {
  configuredSurfaces(GraphForge, tableFromIPC);
  if (!["exploratory", "advisory", "strict"].includes(ontologyMode)) {
    throw new AgentAdapterError(
      "GF_AGENT_BOOTSTRAP_CONFIGURATION",
      "ontology mode must be exploratory, advisory, or strict",
    );
  }
  const projectPath = await validateProjectPath({ path, cwd });
  let graph;
  try {
    await mkdir(projectPath, { recursive: true }).catch((error) => {
      if (error?.code !== "EEXIST") throw error;
    });
    await validateProjectPath({ path: projectPath });
    graph = new GraphForge(projectPath);
    if (ontologyMode === "advisory" && graph.ontologyMode === "exploratory") {
      if (typeof ontologyPath !== "string" || ontologyPath.length === 0) {
        throw new AgentAdapterError(
          "GF_AGENT_BOOTSTRAP_CONFIGURATION",
          "advisory bootstrap requires an explicit ontology path",
        );
      }
      const safeOntologyPath = await validateProjectPath({
        path: ontologyPath,
        cwd,
      });
      graph.loadOntology(safeOntologyPath);
    }
    if (graph.ontologyMode !== ontologyMode) {
      throw new AgentAdapterError(
        "GF_AGENT_ONTOLOGY_MODE_CONFLICT",
        "the project ontology mode does not match the requested mode",
        { actual_mode: graph.ontologyMode, requested_mode: ontologyMode },
      );
    }
    const capabilities = decode(tableFromIPC, await graph.projectCapabilities());
    requireCapabilities(capabilityMap(capabilities), { graph: 1 });
    const before = decode(tableFromIPC, await graph.execute(BOOTSTRAP_QUERY));
    if (before.length > 1) {
      throw new AgentAdapterError(
        "GF_AGENT_BOOTSTRAP_CONFLICT",
        "multiple bootstrap markers exist",
      );
    }
    const created = before.length === 0;
    const markerUuid = created
      ? uuidToString(graph.addNode("GraphForgeBootstrap", { key: "agent-skills/v1" }).uuid)
      : uuidToString(before[0].node_uuid);
    graph.close();
    graph = undefined;

    const reopened = await openProject({
      GraphForge,
      path: projectPath,
      requiredCapabilities: { graph: 1 },
      tableFromIPC,
    });
    graph = reopened.graph;
    const verified = decode(tableFromIPC, await graph.execute(BOOTSTRAP_QUERY));
    if (verified.length !== 1 || uuidToString(verified[0].node_uuid) !== markerUuid) {
      throw new AgentAdapterError(
        "GF_AGENT_BOOTSTRAP_VERIFY_FAILED",
        "the reopened project did not return the bootstrap marker",
      );
    }
    return {
      capabilities: reopened.capabilities,
      created,
      marker_uuid: markerUuid,
      ontology_mode: graph.ontologyMode,
      rows: verified,
    };
  } catch (error) {
    throw normalizeGraphForgeError(error);
  } finally {
    graph?.close?.();
  }
}

export async function buildKnowledge({ GraphForge, tableFromIPC, path, input }) {
  configuredSurfaces(GraphForge, tableFromIPC);
  validateBuildInput(input);
  let graph;
  try {
    const opened = await openProject({
      GraphForge,
      path,
      requiredCapabilities: { graph: 1 },
      tableFromIPC,
    });
    graph = opened.graph;
    for (const capabilityId of requiredCapabilitiesFor(input)) {
      await graph.enableCapability({
        actorUuid: input.actor_uuid,
        capabilityId,
        capabilityVersion: 1,
        operationUuid: input.capability_operation_uuids[capabilityId],
      });
    }
    const capabilityRows = decode(tableFromIPC, await graph.projectCapabilities());

    const handles = new Map();
    const nodes = input.nodes.map((node) => {
      const handle = graph.addNode(node.label, node.properties ?? {});
      handles.set(node.key, handle);
      return { key: node.key, uuid: uuidToString(handle.uuid) };
    });
    const edges = input.edges.map((edge) => {
      const source = handles.get(edge.source_key);
      const target = handles.get(edge.target_key);
      if (!source || !target) {
        throw new AgentAdapterError(
          "GF_AGENT_BUILD_REFERENCE_MISSING",
          "edge endpoints must reference nodes in the same request",
        );
      }
      const handle = graph.addEdge(source, edge.type, target, edge.properties ?? {});
      return { key: edge.key, uuid: uuidToString(handle.uuid) };
    });

    const graphRefs = input.assertion.graph_refs.map((reference) => ({
      graphKind: reference.graph_kind,
      graphUuid: graphUuid(reference, nodes, edges),
      ordinal: reference.ordinal,
      role: reference.role,
    }));
    const assertionRequest = {
      actorUuid: input.actor_uuid,
      assertionUuid: input.assertion.assertion_uuid,
      claim: input.assertion.claim,
      graphRefs,
      operationUuid: input.assertion.operation_uuid,
    };
    const assertionRows = decode(
      tableFromIPC,
      await graph.createAssertionWithEvidence({
        ...assertionRequest,
        evidence: input.evidence.map((evidence) => ({
          evidenceUuid: evidence.evidence_uuid,
          role: evidence.role,
          sourceKind: evidence.source_kind,
          sourceUuid: graphSourceUuid(evidence, nodes, edges),
          weight: evidence.weight,
        })),
      }),
    );
    const assertionProvenance = uuidToString(assertionRows[0].provenance_uuid);

    const confidenceRows = decode(
      tableFromIPC,
      await graph.assessConfidence({
        actorUuid: input.actor_uuid,
        assertionUuid: input.assertion.assertion_uuid,
        confidenceUuid: input.confidence.confidence_uuid,
        operationUuid: input.confidence.operation_uuid,
        policy: input.confidence.policy,
        value: input.confidence.value,
        inputConfidenceUuids: input.confidence.input_confidence_uuids,
      }),
    );
    const reasoningRows = input.reasoning
      ? decode(
          tableFromIPC,
          await graph.recordReasoning({
            actorUuid: input.actor_uuid,
            assertionUuid: input.assertion.assertion_uuid,
            content: Buffer.from(input.reasoning.content, "utf8"),
            contentFormat: input.reasoning.content_format,
            kind: input.reasoning.kind,
            operationUuid: input.reasoning.operation_uuid,
            provenanceUuid: assertionProvenance,
            reasoningUuid: input.reasoning.reasoning_uuid,
          }),
        )
      : [];
    const statusRows = input.status
      ? decode(
          tableFromIPC,
          await graph.recordAssertionStatus({
            actorUuid: input.actor_uuid,
            assertionUuid: input.assertion.assertion_uuid,
            confidenceUuid: input.confidence.confidence_uuid,
            operationUuid: input.status.operation_uuid,
            provenanceUuid: assertionProvenance,
            reasoningUuid: input.reasoning?.reasoning_uuid,
            status: input.status.status,
            statusEventUuid: input.status.status_event_uuid,
          }),
        )
      : [];
    return {
      assertion: assertionRows,
      capabilities: capabilityRows,
      confidence: confidenceRows,
      edges,
      evidence_count: input.evidence.length,
      nodes,
      reasoning: reasoningRows,
      status: statusRows,
    };
  } catch (error) {
    throw normalizeGraphForgeError(error);
  } finally {
    graph?.close?.();
  }
}

/**
 * Resolve one caller-addressed belief subject without inventing a selection.
 *
 * Rust remains authoritative for subject selection, the transaction snapshot,
 * valid-time intersection, ambiguity policy, and graph projection. This
 * workflow only decodes the canonical native evidence and exposes the opaque
 * projection for a later recorded invocation.
 */
export async function resolveBeliefSubject({ GraphForge, tableFromIPC, path, input }) {
  configuredSurfaces(GraphForge, tableFromIPC);
  const request = validateBeliefSubjectInput(input);
  let graph;
  try {
    const opened = await openProject({
      GraphForge,
      path,
      requiredCapabilities: { epistemic: 1, graph: 1, knowledge: 1 },
      tableFromIPC,
    });
    graph = opened.graph;
    const resolved = await graph.resolveBeliefSubject({
      ...request.subject,
      transactionCutoffMicros: request.transactionCutoffMicros,
      validTimeMicros: request.validTimeMicros,
      policy: request.nativePolicy,
    });
    const evidence = decode(tableFromIPC, resolved.evidence);
    const assertions = evidence
      .filter((row) => row.entity_kind === "assertion")
      .map(assertionRecord);
    const hypothesisGroups = evidence
      .filter((row) => row.entity_kind === "hypothesis_group")
      .map(hypothesisRecord);
    const subjectSources = [...new Set(evidence.flatMap((row) => row.source_record_uuids))].sort();
    const projection = resolved.projection;
    let subject;
    if (request.subject.assertionUuid) {
      subject = { assertion_uuid: request.subject.assertionUuid, kind: "assertion" };
    } else {
      const addressedGroups = hypothesisGroups.filter(
        (row) => row.question_key === request.subject.hypothesisQuestionKey,
      );
      if (addressedGroups.length !== 1) {
        throw new AgentAdapterError(
          "GF_AGENT_BELIEF_EVIDENCE_INVALID",
          "native belief-subject evidence must contain exactly one addressed hypothesis group",
        );
      }
      subject = {
        group_uuid: addressedGroups[0].group_uuid,
        hypothesis_question_key: request.subject.hypothesisQuestionKey,
        kind: "hypothesis_question",
      };
    }
    return {
      assertions,
      contract_version: 1,
      hypothesis_groups: hypothesisGroups,
      policy: request.outputPolicy,
      projection,
      projection_evidence: {
        graph_content_fingerprint: projection.graphContentFingerprint,
        policy_fingerprint: projection.policyFingerprint,
        snapshot_fingerprint: projection.snapshotFingerprint,
        source_generation_uuid: uuidToString(projection.sourceGenerationUuid),
        source_record_uuids: [...projection.sourceRecordUuids].map(uuidToString).sort(),
        valid_time_fingerprint: projection.validTimeFingerprint ?? null,
      },
      subject,
      subject_source_record_uuids: subjectSources,
      transaction_cutoff_micros: String(request.transactionCutoffMicros),
      valid_time_micros:
        request.validTimeMicros === undefined ? null : String(request.validTimeMicros),
    };
  } catch (error) {
    throw normalizeGraphForgeError(error);
  } finally {
    graph?.close?.();
  }
}

const DEFAULT_NARRATION_RECORD_BUDGET = 1024;
const DEFAULT_NARRATION_PAGE_LIMIT = 100;
const NEXT_PAGE_TOKEN_KEY = "graphforge.next_page_token";

/**
 * Narrate every public record relevant to one resolved belief subject.
 *
 * Rust remains authoritative for history content and pagination. This workflow
 * only walks the shipped Node list surfaces for the resolved assertion set,
 * fails closed when the caller budget is exhausted, and returns UUID-addressed
 * descriptors for broader project-level collections instead of truncating them.
 */
export async function narrateBeliefRecords({ GraphForge, tableFromIPC, path, input }) {
  configuredSurfaces(GraphForge, tableFromIPC);
  const resolved = await resolveBeliefSubject({ GraphForge, tableFromIPC, path, input });
  const budget = narrationBudget(input?.record_budget);
  const pageLimit = narrationPageLimit(input?.page_limit);
  const counter = { budget, remaining: budget };
  let graph;
  try {
    const opened = await openProject({
      GraphForge,
      path,
      requiredCapabilities: { epistemic: 1, graph: 1, knowledge: 1 },
      tableFromIPC,
    });
    graph = opened.graph;
    const assertionUuids = resolved.assertions.map((row) => row.assertion_uuid);
    const groupUuids = resolved.hypothesis_groups.map((row) => row.group_uuid);
    const validTimeEnabled =
      opened.capabilities?.valid_time?.status === "supported" &&
      opened.capabilities?.valid_time?.version === 1;
    const records = {
      assertion_graph_refs: [],
      assertion_status: [],
      assertion_supersessions: [],
      assertion_validity: [],
      assertions: [],
      confidence_assessments: [],
      confidence_inputs: [],
      evidence_links: [],
      hypothesis_groups: [],
      hypothesis_membership: [],
      hypothesis_selection: [],
      provenance: [],
      reasoning: [],
    };

    for (const assertionUuid of assertionUuids) {
      const assertionRows = decode(tableFromIPC, await graph.assertion(assertionUuid));
      appendUnique(records.assertions, assertionRows, "assertion_uuid", counter);
      await collectPaged(records.assertion_graph_refs, counter, async (after) =>
        pageDecode(
          tableFromIPC,
          await graph.assertionGraphRefs(assertionUuid, { after, limit: pageLimit }),
        ),
      );
      await collectPaged(records.assertion_status, counter, async (after) =>
        pageDecode(
          tableFromIPC,
          await graph.listAssertionStatus({ after, assertionUuid, limit: pageLimit }),
        ),
      );
      // valid_time@1 is optional; skip when the project has not enabled it.
      if (validTimeEnabled) {
        await collectPaged(records.assertion_validity, counter, async (after) =>
          pageDecode(
            tableFromIPC,
            await graph.listAssertionValidity({ after, assertionUuid, limit: pageLimit }),
          ),
        );
      }
      await collectPaged(records.assertion_supersessions, counter, async (after) =>
        pageDecode(
          tableFromIPC,
          await graph.listAssertionSupersessions({
            after,
            limit: pageLimit,
            priorAssertionUuid: assertionUuid,
          }),
        ),
      );
      await collectPaged(records.assertion_supersessions, counter, async (after) =>
        pageDecode(
          tableFromIPC,
          await graph.listAssertionSupersessions({
            after,
            limit: pageLimit,
            replacementAssertionUuid: assertionUuid,
          }),
        ),
      );
      const confidenceBefore = records.confidence_assessments.length;
      await collectPaged(
        records.confidence_assessments,
        counter,
        async (after) =>
          pageDecode(
            tableFromIPC,
            await graph.listConfidenceAssessments({
              after,
              assertionUuid,
              limit: pageLimit,
            }),
          ),
        "confidence_uuid",
      );
      const confidenceUuids = [
        ...new Set(
          records.confidence_assessments.slice(confidenceBefore).map((row) => row.confidence_uuid),
        ),
      ].sort();
      for (const confidenceUuid of confidenceUuids) {
        await collectPaged(records.confidence_inputs, counter, async (after) =>
          pageDecode(
            tableFromIPC,
            await graph.confidenceInputs(confidenceUuid, {
              after,
              limit: pageLimit,
            }),
          ),
        );
      }
      await collectPaged(records.evidence_links, counter, async (after) =>
        pageDecode(
          tableFromIPC,
          await graph.listEvidenceLinks({ after, assertionUuid, limit: pageLimit }),
        ),
      );
      await collectPaged(records.reasoning, counter, async (after) =>
        pageDecode(
          tableFromIPC,
          await graph.listReasoning({ after, assertionUuid, limit: pageLimit }),
        ),
      );
      await collectPaged(records.provenance, counter, async (after) =>
        pageDecode(
          tableFromIPC,
          await graph.listProvenanceHistory({
            after,
            limit: pageLimit,
            subjectUuid: assertionUuid,
          }),
        ),
      );
    }

    for (const group of resolved.hypothesis_groups) {
      await collectPaged(records.hypothesis_groups, counter, async (after) =>
        pageDecode(
          tableFromIPC,
          await graph.listHypothesisGroups({
            after,
            limit: pageLimit,
            questionKey: group.question_key,
          }),
        ),
      );
      await collectPaged(records.hypothesis_membership, counter, async (after) =>
        pageDecode(
          tableFromIPC,
          await graph.listHypothesisMembership({
            after,
            groupUuid: group.group_uuid,
            limit: pageLimit,
          }),
        ),
      );
      await collectPaged(records.hypothesis_selection, counter, async (after) =>
        pageDecode(
          tableFromIPC,
          await graph.listHypothesisSelection({
            after,
            groupUuid: group.group_uuid,
            limit: pageLimit,
          }),
        ),
      );
    }

    for (const family of Object.values(records)) {
      family.sort(compareCanonicalRows);
    }

    return {
      contract_version: 1,
      page_limit: pageLimit,
      policy: resolved.policy,
      projection: resolved.projection,
      projection_descriptors: projectDescriptors(pageLimit),
      projection_evidence: resolved.projection_evidence,
      record_budget: budget,
      records,
      scoped_assertion_uuids: [...assertionUuids].sort(),
      scoped_hypothesis_group_uuids: [...groupUuids].sort(),
      subject: resolved.subject,
      subject_source_record_uuids: resolved.subject_source_record_uuids,
      transaction_cutoff_micros: resolved.transaction_cutoff_micros,
      valid_time_micros: resolved.valid_time_micros,
    };
  } catch (error) {
    throw normalizeGraphForgeError(error);
  } finally {
    graph?.close?.();
  }
}

/**
 * Optionally dispatch one caller-prepared neutral M18 analysis on a resolved
 * belief projection, preserving completed M20 runs when M21 attachment fails.
 */
export async function dispatchRecordedNeutralAnalysis({ GraphForge, tableFromIPC, path, input }) {
  configuredSurfaces(GraphForge, tableFromIPC);
  const request = validateRecordedAnalysisInput(input);
  let graph;
  try {
    const opened = await openProject({
      GraphForge,
      path,
      requiredCapabilities: { epistemic: 1, graph: 1, knowledge: 1 },
      tableFromIPC,
    });
    graph = opened.graph;
    const recorded = await graph.invokeResolvedRecorded(request.projection, {
      actorUuid: request.actorUuid,
      attachmentUuid: request.attachmentUuid,
      descriptor: request.descriptor,
      operationUuid: request.operationUuid,
      runUuid: request.runUuid,
      signal: request.signal,
    });
    const runRows = decode(tableFromIPC, await graph.algorithmRun(recorded.runUuid));
    const eventRows = decode(tableFromIPC, await graph.algorithmRunEvents(recorded.runUuid));
    return {
      attachment: recorded.attachment ? decode(tableFromIPC, recorded.attachment) : [],
      attachment_error_code: recorded.attachmentErrorCode ?? null,
      attachment_state: recorded.attachmentState,
      attachment_uuid: recorded.attachmentUuid,
      contract_version: 1,
      descriptor_algorithm: request.descriptor.algorithm,
      descriptor_fingerprint: request.descriptor.fingerprint,
      descriptor_verb: request.descriptor.verb,
      result: decode(tableFromIPC, recorded.result),
      run: runRows,
      run_events: eventRows,
      run_uuid: recorded.runUuid,
    };
  } catch (error) {
    throw normalizeGraphForgeError(error);
  } finally {
    graph?.close?.();
  }
}

const EXPLORE_MODES = new Set(["neighborhood", "traversal", "path", "reachability"]);
const RANDOM_WALK_ALGORITHMS = new Set(["random_walk"]);
const MAX_EXPLORE_RESULT_LIMIT = 10_000;
const MAX_EXPLORE_DEPTH = 10_000;

/**
 * Bounded graph exploration through the public Node paths facade.
 *
 * Rust owns traversal/path/reachability semantics. This workflow only validates
 * finite agent bounds, dispatches the selected public paths algorithm, and
 * returns UUID-addressed summaries with complete Arrow/JSON linkage.
 */
export async function exploreGraph({ GraphForge, tableFromIPC, path, input }) {
  configuredSurfaces(GraphForge, tableFromIPC);
  const request = validateExploreInput(input);
  let graph;
  try {
    const opened = await openProject({
      GraphForge,
      path,
      requiredCapabilities: { graph: 1 },
      tableFromIPC,
    });
    graph = opened.graph;
    if (request.signal?.aborted) {
      throw new AgentAdapterError("GF_CANCELLED", "explore request was cancelled");
    }
    const ipc = invokeExplore(graph, request);
    if (request.signal?.aborted) {
      throw new AgentAdapterError("GF_CANCELLED", "explore request was cancelled");
    }
    const table = Number.isInteger(ipc?.numRows) ? ipc : tableFromIPC(ipc);
    const rows = tableToJson(table);
    const truncated = rows.length > request.resultLimit;
    const summary = truncated ? rows.slice(0, request.resultLimit) : rows;
    return {
      algorithm: request.algorithm,
      contract_version: 1,
      directed: request.directed,
      mode: request.mode,
      result: rows,
      result_limit: request.resultLimit,
      start_uuids: request.startUuids,
      summary,
      target_uuid: request.targetUuid,
      truncated,
      via: request.via,
      walk_length: request.walkLength,
    };
  } catch (error) {
    throw normalizeGraphForgeError(error);
  } finally {
    graph?.close?.();
  }
}

function validateExploreInput(input) {
  if (!input || typeof input !== "object") {
    throw new AgentAdapterError(
      "GF_AGENT_EXPLORE_CONFIGURATION",
      "explore requires a bounded input object",
    );
  }
  const mode = input.mode;
  if (!EXPLORE_MODES.has(mode)) {
    throw new AgentAdapterError(
      "GF_AGENT_EXPLORE_MODE_REQUIRED",
      "mode must be neighborhood, traversal, path, or reachability",
    );
  }
  const resultLimit = input.result_limit;
  if (
    !Number.isSafeInteger(resultLimit) ||
    resultLimit < 1 ||
    resultLimit > MAX_EXPLORE_RESULT_LIMIT
  ) {
    throw new AgentAdapterError(
      "GF_AGENT_EXPLORE_BOUNDS_REQUIRED",
      "result_limit must be a safe integer in 1..=10000",
    );
  }
  const startUuids = normalizeExploreUuids(input.start_uuids, "start_uuids");
  if (startUuids.length === 0) {
    throw new AgentAdapterError(
      "GF_AGENT_EXPLORE_START_REQUIRED",
      "explore requires one or more start UUIDs",
    );
  }
  let targetUuid = null;
  if (mode === "path") {
    if (input.target_uuid === undefined || input.target_uuid === null) {
      throw new AgentAdapterError(
        "GF_AGENT_EXPLORE_TARGET_REQUIRED",
        "path mode requires an explicit target UUID",
      );
    }
    targetUuid = uuidToString(input.target_uuid);
  } else if (input.target_uuid !== undefined && input.target_uuid !== null) {
    targetUuid = uuidToString(input.target_uuid);
  }
  let walkLength;
  if (mode === "neighborhood" || mode === "traversal") {
    const depth = input.depth ?? input.walk_length;
    if (!Number.isSafeInteger(depth) || depth < 1 || depth > MAX_EXPLORE_DEPTH) {
      throw new AgentAdapterError(
        "GF_AGENT_EXPLORE_BOUNDS_REQUIRED",
        "neighborhood and traversal require a finite depth in 1..=10000",
      );
    }
    walkLength = mode === "neighborhood" ? Math.min(depth, 1) : depth;
  } else if (input.walk_length !== undefined && input.walk_length !== null) {
    if (
      !Number.isSafeInteger(input.walk_length) ||
      input.walk_length < 1 ||
      input.walk_length > MAX_EXPLORE_DEPTH
    ) {
      throw new AgentAdapterError(
        "GF_AGENT_EXPLORE_BOUNDS_REQUIRED",
        "walk_length must be a safe integer in 1..=10000 when provided",
      );
    }
    walkLength = input.walk_length;
  }
  const algorithm =
    mode === "neighborhood" || mode === "traversal"
      ? (input.algorithm ?? "bfs")
      : mode === "path"
        ? (input.algorithm ?? "dijkstra")
        : (input.algorithm ?? "transitive_closure");
  if (typeof algorithm !== "string" || algorithm.length === 0 || algorithm.length > 4096) {
    throw new AgentAdapterError(
      "GF_AGENT_EXPLORE_ALGORITHM_REQUIRED",
      "algorithm must be a non-empty public paths catalog value",
    );
  }
  const via =
    input.via === undefined || input.via === null
      ? undefined
      : typeof input.via === "string" && input.via.length > 0 && input.via.length <= 4096
        ? input.via
        : (() => {
            throw new AgentAdapterError(
              "GF_AGENT_EXPLORE_CONFIGURATION",
              "via must be a bounded non-empty string when provided",
            );
          })();
  const directed = input.directed === undefined ? true : Boolean(input.directed);
  return {
    algorithm,
    directed,
    mode,
    resultLimit,
    signal: input.signal,
    startUuids,
    targetUuid,
    via,
    walkLength,
  };
}

function normalizeExploreUuids(value, field) {
  if (!Array.isArray(value)) {
    throw new AgentAdapterError(
      "GF_AGENT_EXPLORE_START_REQUIRED",
      `${field} must be an array of UUIDs`,
    );
  }
  if (value.length > 1024) {
    throw new AgentAdapterError(
      "GF_AGENT_EXPLORE_BOUNDS_REQUIRED",
      `${field} exceeds the 1024-entry explore budget`,
    );
  }
  return [...new Set(value.map((item) => uuidToString(item)))].sort();
}

function invokeExplore(graph, request) {
  const source = request.startUuids[0];
  const target = request.targetUuid ?? undefined;
  // Only random-walk catalog algorithms accept walkLength. Neighborhood/traversal
  // keep walk_length in the skill response for agent bounds, but must not pass it
  // into bfs/dijkstra/etc. (native validation rejects random-walk options there).
  const walkLength = RANDOM_WALK_ALGORITHMS.has(request.algorithm) ? request.walkLength : undefined;
  if (typeof graph.preparePathsInvocation === "function") {
    const descriptor = graph.preparePathsInvocation(
      source,
      target,
      request.algorithm,
      request.via,
      request.directed,
      undefined,
      undefined,
      undefined,
      walkLength,
    );
    return graph.invokeDescriptor(descriptor);
  }
  return graph.paths(
    source,
    target,
    request.algorithm,
    request.via,
    request.directed,
    undefined,
    undefined,
    undefined,
    walkLength,
  );
}

const RETRIEVE_SURFACES = new Set(["find", "rank", "cluster", "paths", "analyze", "similar"]);
const MAX_RETRIEVE_RESULT_LIMIT = 10_000;

/**
 * Bounded retrieve/analyze over public M19 find and live M18 descriptor families.
 *
 * Caller-selected modes and descriptor fields pass through unchanged. Rust owns
 * algorithm/search semantics; this workflow only enforces finite bounds and
 * agent-legible truncation while opening the graph capability alone.
 */
export async function retrieveAnalyze({ GraphForge, tableFromIPC, path, input }) {
  configuredSurfaces(GraphForge, tableFromIPC);
  const request = validateRetrieveInput(input);
  let graph;
  try {
    const opened = await openProject({
      GraphForge,
      path,
      requiredCapabilities: { graph: 1 },
      tableFromIPC,
    });
    graph = opened.graph;
    if (request.signal?.aborted) {
      throw new AgentAdapterError("GF_CANCELLED", "retrieve request was cancelled");
    }
    const ipc = invokeRetrieve(graph, request);
    if (request.signal?.aborted) {
      throw new AgentAdapterError("GF_CANCELLED", "retrieve request was cancelled");
    }
    const table = Number.isInteger(ipc?.numRows) ? ipc : tableFromIPC(ipc);
    const rows = tableToJson(table);
    const truncated = rows.length > request.resultLimit;
    const summary = truncated ? rows.slice(0, request.resultLimit) : rows;
    return {
      contract_version: 1,
      empty: rows.length === 0,
      result: rows,
      result_limit: request.resultLimit,
      summary,
      surface: request.surface,
      truncated,
      ...request.echo,
    };
  } catch (error) {
    throw normalizeGraphForgeError(error);
  } finally {
    graph?.close?.();
  }
}

function validateRetrieveInput(input) {
  if (!input || typeof input !== "object") {
    throw new AgentAdapterError(
      "GF_AGENT_RETRIEVE_CONFIGURATION",
      "retrieve/analyze requires a bounded input object",
    );
  }
  const surface = input.surface;
  if (!RETRIEVE_SURFACES.has(surface)) {
    throw new AgentAdapterError(
      "GF_AGENT_RETRIEVE_SURFACE_REQUIRED",
      "surface must be find, rank, cluster, paths, analyze, or similar",
    );
  }
  const resultLimit = input.result_limit;
  if (
    !Number.isSafeInteger(resultLimit) ||
    resultLimit < 1 ||
    resultLimit > MAX_RETRIEVE_RESULT_LIMIT
  ) {
    throw new AgentAdapterError(
      "GF_AGENT_RETRIEVE_BOUNDS_REQUIRED",
      "result_limit must be a safe integer in 1..=10000",
    );
  }
  if (surface === "find") {
    const hasText = typeof input.query === "string" && input.query.length > 0;
    const hasVector = Array.isArray(input.vector) && input.vector.length > 0;
    const hasSemantic = typeof input.semantic_query === "string" && input.semantic_query.length > 0;
    const hasSimilar = input.similar_to !== undefined && input.similar_to !== null;
    if (!hasText && !hasVector && !hasSemantic && !hasSimilar) {
      throw new AgentAdapterError(
        "GF_AGENT_RETRIEVE_FIND_REQUIRED",
        "find requires query, vector, semantic_query, and/or similar_to",
      );
    }
    return {
      echo: {
        find: {
          force_stale: Boolean(input.force_stale),
          label: input.label ?? null,
          query: input.query ?? null,
          semantic_query: input.semantic_query ?? null,
          similar_to: input.similar_to ?? null,
          space: input.space ?? null,
          vector: hasVector ? [...input.vector] : null,
        },
      },
      find: {
        forceStale: Boolean(input.force_stale),
        label: input.label,
        query: input.query,
        semanticQuery: input.semantic_query,
        similarTo: input.similar_to,
        space: input.space,
        vector: hasVector ? input.vector : undefined,
      },
      resultLimit,
      signal: input.signal,
      surface,
    };
  }
  if (typeof input.algorithm !== "string" || input.algorithm.length === 0) {
    throw new AgentAdapterError(
      "GF_AGENT_RETRIEVE_ALGORITHM_REQUIRED",
      "M18 surfaces require an explicit algorithm catalog value",
    );
  }
  if (surface !== "analyze" && (typeof input.label !== "string" || input.label.length === 0)) {
    if (surface !== "paths") {
      throw new AgentAdapterError(
        "GF_AGENT_RETRIEVE_LABEL_REQUIRED",
        "rank, cluster, and similar require an explicit label",
      );
    }
  }
  return {
    echo: {
      m18: {
        algorithm: input.algorithm,
        directed: input.directed === undefined ? null : Boolean(input.directed),
        label: input.label ?? null,
        source: input.source ?? null,
        target: input.target ?? null,
        vector_property: input.vector_property ?? null,
        via: input.via ?? null,
      },
    },
    m18: {
      algorithm: input.algorithm,
      directed: input.directed,
      label: input.label,
      source: input.source,
      target: input.target,
      vectorProperty: input.vector_property,
      via: input.via,
    },
    resultLimit,
    signal: input.signal,
    surface,
  };
}

function invokeRetrieve(graph, request) {
  if (request.surface === "find") {
    return graph.find(
      request.find.query,
      request.find.label,
      request.find.vector,
      request.find.similarTo,
      request.find.semanticQuery,
      request.resultLimit,
      request.find.space,
      request.find.forceStale,
    );
  }
  const { m18, surface } = request;
  if (surface === "rank") {
    if (typeof graph.prepareRankInvocation === "function") {
      return graph.invokeDescriptor(
        graph.prepareRankInvocation(m18.label, m18.algorithm, m18.via, m18.directed),
      );
    }
    return graph.rank(m18.label, m18.algorithm, m18.via, m18.directed);
  }
  if (surface === "cluster") {
    if (typeof graph.prepareClusterInvocation === "function") {
      return graph.invokeDescriptor(
        graph.prepareClusterInvocation(
          m18.label,
          m18.algorithm,
          m18.via,
          m18.directed,
          m18.vectorProperty,
        ),
      );
    }
    return graph.cluster(m18.label, m18.algorithm, m18.via, m18.directed, m18.vectorProperty);
  }
  if (surface === "paths") {
    if (typeof graph.preparePathsInvocation === "function") {
      return graph.invokeDescriptor(
        graph.preparePathsInvocation(m18.source, m18.target, m18.algorithm, m18.via, m18.directed),
      );
    }
    return graph.paths(m18.source, m18.target, m18.algorithm, m18.via, m18.directed);
  }
  if (surface === "analyze") {
    if (typeof graph.prepareAnalyzeInvocation === "function") {
      return graph.invokeDescriptor(
        graph.prepareAnalyzeInvocation(m18.algorithm, m18.label, m18.via, m18.directed),
      );
    }
    return graph.analyze(m18.algorithm, m18.label, m18.via, m18.directed);
  }
  if (typeof graph.prepareSimilarInvocation === "function") {
    return graph.invokeDescriptor(
      graph.prepareSimilarInvocation(
        m18.label,
        m18.algorithm,
        request.resultLimit,
        m18.vectorProperty,
        m18.via,
      ),
    );
  }
  return graph.similar(m18.label, m18.algorithm, request.resultLimit, m18.vectorProperty, m18.via);
}

function configuredSurfaces(GraphForge, tableFromIPC) {
  if (typeof GraphForge !== "function" || typeof tableFromIPC !== "function") {
    throw new AgentAdapterError(
      "GF_AGENT_ADAPTER_CONFIGURATION",
      "GraphForge and tableFromIPC shipped surfaces are required",
    );
  }
}

function narrationBudget(value) {
  if (value === undefined || value === null) return DEFAULT_NARRATION_RECORD_BUDGET;
  if (!Number.isSafeInteger(value) || value < 1) {
    throw beliefInputError(
      "GF_AGENT_BELIEF_BUDGET_REQUIRED",
      "record_budget must be a positive safe integer",
    );
  }
  return value;
}

function narrationPageLimit(value) {
  if (value === undefined || value === null) return DEFAULT_NARRATION_PAGE_LIMIT;
  if (!Number.isSafeInteger(value) || value < 1 || value > 10_000) {
    throw beliefInputError(
      "GF_AGENT_BELIEF_PAGE_LIMIT_REQUIRED",
      "page_limit must be a safe integer in 1..=10000",
    );
  }
  return value;
}

function validateRecordedAnalysisInput(input) {
  if (!input || typeof input !== "object") {
    throw beliefInputError(
      "GF_AGENT_ANALYSIS_CONFIGURATION",
      "recorded analysis requires a bounded input object",
    );
  }
  if (!input.projection || typeof input.projection !== "object") {
    throw beliefInputError(
      "GF_AGENT_ANALYSIS_PROJECTION_REQUIRED",
      "recorded analysis requires the opaque resolved projection",
    );
  }
  const descriptor = input.descriptor;
  if (
    !descriptor ||
    typeof descriptor !== "object" ||
    typeof descriptor.algorithm !== "string" ||
    typeof descriptor.fingerprint !== "string" ||
    typeof descriptor.verb !== "string"
  ) {
    throw beliefInputError(
      "GF_AGENT_ANALYSIS_DESCRIPTOR_REQUIRED",
      "recorded analysis requires the caller-prepared InvocationDescriptor",
    );
  }
  return {
    actorUuid:
      input.actor_uuid === undefined || input.actor_uuid === null
        ? undefined
        : uuidToString(input.actor_uuid),
    attachmentUuid: uuidToString(input.attachment_uuid),
    descriptor,
    operationUuid: uuidToString(input.operation_uuid),
    projection: input.projection,
    runUuid: uuidToString(input.run_uuid),
    signal: input.signal,
  };
}

function pageDecode(tableFromIPC, ipc) {
  const table = Number.isInteger(ipc?.numRows) ? ipc : tableFromIPC(ipc);
  return {
    next: nextPageToken(table),
    rows: tableToJson(table),
  };
}

function nextPageToken(table) {
  const metadata = table?.schema?.metadata;
  if (!metadata) return null;
  if (typeof metadata.get === "function") {
    return metadata.get(NEXT_PAGE_TOKEN_KEY) ?? null;
  }
  return metadata[NEXT_PAGE_TOKEN_KEY] ?? null;
}

async function collectPaged(target, counter, fetchPage, identityKey) {
  let after;
  for (;;) {
    const page = await fetchPage(after);
    appendUnique(target, page.rows, identityKey, counter);
    if (!page.next) return;
    after = page.next;
  }
}

function appendUnique(target, rows, identityKey, counter, alreadyCounted = false) {
  const seen = new Set(target.map((row) => rowIdentity(row, identityKey)));
  for (const row of rows) {
    const identity = rowIdentity(row, identityKey);
    if (seen.has(identity)) continue;
    if (!alreadyCounted) {
      if (counter.remaining <= 0) {
        throw beliefInputError(
          "GF_AGENT_BELIEF_RECORD_BUDGET_EXCEEDED",
          "scoped belief narration exceeded the caller record budget",
          { record_budget: counter.budget },
        );
      }
      counter.remaining -= 1;
    }
    seen.add(identity);
    target.push(row);
  }
}

function rowIdentity(row, identityKey) {
  if (identityKey && Object.hasOwn(row, identityKey)) {
    return `${identityKey}:${String(row[identityKey])}`;
  }
  return JSON.stringify(row);
}

function compareCanonicalRows(left, right) {
  const leftJson = JSON.stringify(left);
  const rightJson = JSON.stringify(right);
  return leftJson < rightJson ? -1 : leftJson > rightJson ? 1 : 0;
}

function projectDescriptors(pageLimit) {
  return [
    { api: "listAssertions", collection: "assertions", page_limit: pageLimit },
    {
      api: "listConfidenceAssessments",
      collection: "confidence_assessments",
      page_limit: pageLimit,
    },
    { api: "listEvidenceLinks", collection: "evidence_links", page_limit: pageLimit },
    { api: "listReasoning", collection: "reasoning", page_limit: pageLimit },
    {
      api: "listAssertionStatus",
      collection: "assertion_status",
      page_limit: pageLimit,
    },
    {
      api: "listAssertionValidity",
      collection: "assertion_validity",
      page_limit: pageLimit,
    },
    {
      api: "listAssertionSupersessions",
      collection: "assertion_supersessions",
      page_limit: pageLimit,
    },
    {
      api: "listHypothesisGroups",
      collection: "hypothesis_groups",
      page_limit: pageLimit,
    },
    {
      api: "listHypothesisMembership",
      collection: "hypothesis_membership",
      page_limit: pageLimit,
    },
    {
      api: "listHypothesisSelection",
      collection: "hypothesis_selection",
      page_limit: pageLimit,
    },
    {
      api: "listProvenanceHistory",
      collection: "provenance",
      page_limit: pageLimit,
    },
  ];
}

function validateBeliefSubjectInput(input) {
  if (!input || typeof input !== "object") {
    throw beliefInputError(
      "GF_AGENT_BELIEF_CONFIGURATION",
      "belief resolution requires a bounded input object",
    );
  }
  const assertionUuid = input.subject?.assertion_uuid;
  const questionKey = input.subject?.hypothesis_question_key;
  if ((assertionUuid === undefined) === (questionKey === undefined)) {
    throw beliefInputError(
      "GF_AGENT_BELIEF_SUBJECT_REQUIRED",
      "provide exactly one assertion UUID or hypothesis question key",
    );
  }
  let subject;
  if (assertionUuid !== undefined) {
    subject = { assertionUuid: uuidToString(assertionUuid) };
  } else if (
    typeof questionKey === "string" &&
    questionKey.length > 0 &&
    questionKey.length <= 4096
  ) {
    subject = { hypothesisQuestionKey: questionKey };
  } else {
    throw beliefInputError(
      "GF_AGENT_BELIEF_SUBJECT_REQUIRED",
      "provide exactly one assertion UUID or hypothesis question key",
    );
  }
  const cutoff = input.transaction_cutoff_micros;
  const validTime = input.valid_time_micros;
  if (
    !Number.isSafeInteger(cutoff) ||
    (![undefined, null].includes(validTime) && !Number.isSafeInteger(validTime))
  ) {
    throw beliefInputError(
      "GF_AGENT_BELIEF_TIME_REQUIRED",
      "transaction cutoff and optional valid time must be safe integer microseconds",
    );
  }
  const policy = input.policy;
  if (
    !policy ||
    policy.version !== 1 ||
    !Array.isArray(policy.included_statuses) ||
    typeof policy.statusless !== "string" ||
    typeof policy.supersession_branches !== "string" ||
    typeof policy.hypotheses !== "string"
  ) {
    throw beliefInputError(
      "GF_AGENT_BELIEF_POLICY_REQUIRED",
      "a complete graphforge-belief-projection/1 policy is required",
      { required_policy_version: 1 },
    );
  }
  const includedStatuses = policy.included_statuses.map((status) => {
    if (typeof status !== "string" || status.length === 0 || status.length > 4096) {
      throw beliefInputError(
        "GF_AGENT_BELIEF_POLICY_REQUIRED",
        "a complete graphforge-belief-projection/1 policy is required",
        { required_policy_version: 1 },
      );
    }
    return status;
  });
  return {
    nativePolicy: {
      hypotheses: policy.hypotheses,
      includedStatuses,
      statusless: policy.statusless,
      supersessionBranches: policy.supersession_branches,
    },
    outputPolicy: {
      hypotheses: policy.hypotheses,
      included_statuses: [...includedStatuses].sort(),
      statusless: policy.statusless,
      supersession_branches: policy.supersession_branches,
      version: 1,
    },
    subject,
    transactionCutoffMicros: cutoff,
    validTimeMicros: validTime ?? undefined,
  };
}

function beliefInputError(code, message, details) {
  return new AgentAdapterError(code, message, details);
}

function assertionRecord(row) {
  return {
    assertion_uuid: row.assertion_uuid,
    reasoning_history_uuids: row.reasoning_history_uuids,
    reasoning_leaf_uuids: row.reasoning_leaf_uuids,
    source_record_uuids: row.source_record_uuids,
    status: row.status,
    status_event_uuid: row.status_event_uuid,
    superseded_by_assertion_uuids: row.superseded_by_assertion_uuids,
  };
}

function hypothesisRecord(row) {
  return {
    current_member_assertion_uuids: row.current_member_assertion_uuids,
    group_uuid: row.group_uuid,
    question_key: row.question_key,
    selected_assertion_uuid: row.selected_assertion_uuid,
    source_record_uuids: row.source_record_uuids,
  };
}

function decode(tableFromIPC, ipc) {
  return tableToJson(tableFromIPC(ipc));
}

function capabilityMap(rows) {
  return Object.fromEntries(
    rows.map((row) => [
      row.capability_id,
      { status: row.status ?? "supported", version: Number(row.capability_version) },
    ]),
  );
}

function validateBuildInput(input) {
  if (
    !input ||
    !Array.isArray(input.nodes) ||
    !Array.isArray(input.edges) ||
    !Array.isArray(input.evidence) ||
    !input.assertion ||
    !Array.isArray(input.assertion.graph_refs) ||
    !input.confidence ||
    input.evidence.length === 0 ||
    !input.capability_operation_uuids
  ) {
    throw new AgentAdapterError(
      "GF_AGENT_BUILD_CONFIGURATION",
      "build-knowledge requires nodes, edges, nonempty evidence, assertion, confidence, and capability operation IDs",
    );
  }
  const keys = input.nodes.map(({ key }) => key);
  if (new Set(keys).size !== keys.length) {
    throw new AgentAdapterError("GF_AGENT_BUILD_CONFLICT", "node keys must be unique");
  }
  const edgeKeys = input.edges.map(({ key }) => key);
  if (new Set(edgeKeys).size !== edgeKeys.length) {
    throw new AgentAdapterError("GF_AGENT_BUILD_CONFLICT", "edge keys must be unique");
  }
  for (const capability of requiredCapabilitiesFor(input)) {
    if (typeof input.capability_operation_uuids[capability] !== "string") {
      throw new AgentAdapterError(
        "GF_AGENT_BUILD_CONFIGURATION",
        "every requested capability requires an operation UUID",
      );
    }
  }
}

function requiredCapabilitiesFor(input) {
  return ["provenance", "knowledge", ...(input.status || input.reasoning ? ["epistemic"] : [])];
}

function graphUuid(reference, nodes, edges) {
  const rows = reference.graph_kind === "node" ? nodes : edges;
  const match = rows.find(({ key }) => key === reference.key);
  if (!match) {
    throw new AgentAdapterError(
      "GF_AGENT_BUILD_REFERENCE_MISSING",
      "assertion references must identify records in the same request",
    );
  }
  return match.uuid;
}

function graphSourceUuid(evidence, nodes, edges) {
  if (evidence.source_kind === "graph_node") {
    return graphUuid({ graph_kind: "node", key: evidence.source_key }, nodes, edges);
  }
  if (evidence.source_kind === "graph_edge") {
    return graphUuid({ graph_kind: "edge", key: evidence.source_key }, nodes, edges);
  }
  return evidence.source_uuid;
}
