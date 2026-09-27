// Offline agent next-step example using the real Node binding.
import { createHash, randomUUID } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  Table,
  Utf8,
  tableFromIPC,
  tableToIPC,
  vectorFromArray,
} from "apache-arrow";
import { GraphForge } from "@curatelabs/graphforge";

const uuidText = (value) => {
  const hex = Buffer.from(value).toString("hex");
  return [
    hex.slice(0, 8),
    hex.slice(8, 12),
    hex.slice(12, 16),
    hex.slice(16, 20),
    hex.slice(20),
  ].join("-");
};
const digest = (value) => [...createHash("sha256").update(value).digest()];
const findOutputRow = (table, questionUuid, itemUuid) => {
  for (let row = 0; row < table.numRows; row += 1) {
    const rowQuestion = uuidText(table.getChild("question_uuid").get(row));
    const rowItemBytes = table.getChild("item_uuid").get(row);
    const rowItem = rowItemBytes === null ? null : uuidText(rowItemBytes);
    if (rowQuestion === questionUuid && rowItem === itemUuid) return row;
  }
  throw new Error("validated decision row is missing");
};

const producerArtifactRoot = mkdtempSync(
  join(tmpdir(), "graphforge-decision-producer-"),
);
const graph = new GraphForge();
try {
  graph.execute(
    "CREATE (:Task {title: 'Summarize evidence'}), " +
      "(:Task {title: 'Resolve source conflict'}), " +
      "(:Task {title: 'Review missing source citation'})",
  );
  const context = tableFromIPC(
    graph.execute(
      "MATCH (t:Task) RETURN t.node_uuid AS item_uuid, t.title AS title ORDER BY t.node_uuid LIMIT 20",
    ),
  );
  const items = Array.from({ length: context.numRows }, (_, row) =>
    uuidText(context.getChild("item_uuid").get(row)),
  );
  const projection = items.map((item, row) => ({
    item_uuid: item,
    title: context.getChild("title").get(row),
  }));
  const inputBytes = Buffer.from(JSON.stringify(items));
  const projectionBytes = Buffer.from(JSON.stringify(projection));
  const question = randomUUID();
  const reviewQuestion = randomUUID();
  const producerArtifact = join(producerArtifactRoot, "results.arrow");
  const resultRows = [
    {
      question_uuid: question,
      item_uuid: items[0],
      status: "answered",
      value: { kind: "choice", value: "continue" },
    },
    {
      question_uuid: question,
      item_uuid: items[1],
      status: "uncertain",
      value: { kind: "choice", value: "clarify" },
    },
    {
      question_uuid: question,
      item_uuid: items[2],
      status: "answered",
      value: { kind: "choice", value: "review" },
    },
    {
      question_uuid: reviewQuestion,
      item_uuid: null,
      status: "answered",
      value: {
        kind: "yes_no_probability",
        value: { yes_probability: 0.25, no_probability: 0.75 },
      },
    },
  ];
  const producedArrow = new Table({
    question_uuid: vectorFromArray(
      resultRows.map((row) => row.question_uuid),
      new Utf8(),
    ),
    item_uuid: vectorFromArray(
      resultRows.map((row) => row.item_uuid),
      new Utf8(),
    ),
    status: vectorFromArray(
      resultRows.map((row) => row.status),
      new Utf8(),
    ),
    value_json: vectorFromArray(
      resultRows.map((row) => JSON.stringify(row.value)),
      new Utf8(),
    ),
  });
  // A caller-owned producer writes its results as Arrow; the workflow loads
  // that artifact independently before native validation.
  writeFileSync(producerArtifact, tableToIPC(producedArrow));
  const loadedProducerArrow = tableFromIPC(readFileSync(producerArtifact));
  const producerResults = Array.from(
    { length: loadedProducerArrow.numRows },
    (_, row) => ({
      question_uuid: loadedProducerArrow.getChild("question_uuid").get(row),
      item_uuid: loadedProducerArrow.getChild("item_uuid").get(row),
      status: loadedProducerArrow.getChild("status").get(row),
      value: JSON.parse(loadedProducerArrow.getChild("value_json").get(row)),
    }),
  );
  const batch = {
    input: {
      generation_uuid: graph.researchProjectSummary().identity.generationUuid,
      version_uuid: null,
      projection_sha256: digest(projectionBytes),
      selection_sha256: digest(inputBytes),
      selected_item_uuids: items,
    },
    producer: { name: "offline agent fixture", model: "fixture-v1" },
    questions: [
      {
        question_uuid: question,
        text: "What is the next permitted step?",
        item_uuids: items,
        kind: {
          kind: "choice",
          allowed_choices: ["continue", "clarify", "review"],
        },
      },
      {
        question_uuid: reviewQuestion,
        text: "Does this task need human review?",
        item_uuids: [],
        kind: { kind: "yes_no_probability" },
      },
    ],
    results: producerResults,
  };
  const output = tableFromIPC(graph.validateDecisionBatch(batch));
  for (let row = 0; row < output.numRows; row += 1) {
    const itemBytes = output.getChild("item_uuid").get(row);
    const item = itemBytes === null ? "project" : uuidText(itemBytes);
    const title =
      itemBytes === null
        ? "review policy"
        : context.getChild("title").get(items.indexOf(item));
    console.log(
      title,
      output.getChild("status").get(row),
      output.getChild("choice_value").get(row),
      output.getChild("yes_probability").get(row),
    );
  }
  const reviewRow = findOutputRow(output, reviewQuestion, null);
  const reviewProbability = output.getChild("yes_probability").get(reviewRow);
  const continueRow = findOutputRow(output, question, items[0]);
  const continueStatus = output.getChild("status").get(continueRow);
  const nextStep = output.getChild("choice_value").get(continueRow);
  console.log("review probability", reviewProbability);

  // Only an answered continue result is eligible for this caller's action.
  // Preserve this request and operation UUID if retrying after a failure.
  const prepared = {
    operation_uuid: randomUUID(),
    expected_generation_uuid: batch.input.generation_uuid,
    item_uuid: items[0],
    action: "continue",
  };
  const existing = tableFromIPC(
    graph.execute(
      "MATCH (r:AgentAction {operation_uuid: $operation_uuid}) " +
        "RETURN r.item_uuid AS item_uuid, r.action AS action",
      prepared,
    ),
  );
  const current = graph.researchProjectSummary().identity.generationUuid;
  if (existing.numRows > 0) {
    const receiptItem = uuidText(existing.getChild("item_uuid").get(0));
    const receiptAction = existing.getChild("action").get(0);
    if (
      receiptItem !== prepared.item_uuid ||
      receiptAction !== prepared.action
    ) {
      throw new Error("operation UUID already has a different action receipt");
    }
    console.log("exact action retry", existing.toArray());
  } else if (
    current === prepared.expected_generation_uuid &&
    continueStatus === "answered" &&
    nextStep === "continue" &&
    reviewProbability < 0.6
  ) {
    graph.execute(
      "MERGE (r:AgentAction {operation_uuid: $operation_uuid}) " +
        "SET r.item_uuid = $item_uuid, r.action = $action",
      prepared,
    );
    console.log("continue on", items[0]);
  } else {
    console.log("clarify or review; no graph action was applied");
  }
} finally {
  graph.close();
  rmSync(producerArtifactRoot, { recursive: true, force: true });
}
