import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

function request(value = "research") {
  const question = randomUUID();
  const item = randomUUID();
  return {
    input: {
      generation_uuid: randomUUID(),
      version_uuid: null,
      projection_sha256: Array(32).fill(7),
      selection_sha256: Array(32).fill(9),
      selected_item_uuids: [item],
    },
    producer: { name: "offline fixture" },
    questions: [
      {
        question_uuid: question,
        text: "Where should this item go?",
        item_uuids: [item],
        kind: { kind: "choice", allowed_choices: ["research", "human_review"] },
      },
    ],
    results: [
      {
        question_uuid: question,
        item_uuid: item,
        status: "answered",
        value: { kind: "choice", value },
      },
    ],
  };
}

function uuidText(value) {
  const hex = Buffer.from(value).toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

test("decision batches validate through Rust and retain Arrow identity columns", () => {
  const graph = new GraphForge();
  try {
    const batch = request();
    const before = graph.researchProjectSummary().identity.generationUuid;
    const table = tableFromIPC(graph.validateDecisionBatch(batch));
    const after = graph.researchProjectSummary().identity.generationUuid;
    assert.equal(table.numRows, 1);
    assert.equal(before, after);
    assert.equal(
      uuidText(table.getChild("generation_uuid").get(0)),
      batch.input.generation_uuid,
    );
    assert.deepEqual(
      [...table.getChild("projection_sha256").get(0)],
      batch.input.projection_sha256,
    );
    assert.equal(table.getChild("question_kind").get(0), "choice");
    assert.equal(table.getChild("choice_value").get(0), "research");
    assert.equal(
      uuidText(table.getChild("item_uuid").get(0)),
      batch.input.selected_item_uuids[0],
    );
  } finally {
    graph.close();
  }
});

test("decision batch validation rejects values outside the caller's choices", () => {
  const graph = new GraphForge();
  try {
    assert.throws(() => graph.validateDecisionBatch(request("write")), {
      code: "ValidationError",
    });
  } finally {
    graph.close();
  }
});

test("decision batch accepts yes/no values with nested probabilities", () => {
  const graph = new GraphForge();
  try {
    const batch = request();
    batch.questions[0].item_uuids = [];
    batch.questions[0].kind = { kind: "yes_no_probability" };
    batch.results[0].item_uuid = null;
    batch.results[0].value = {
      kind: "yes_no_probability",
      value: { yes_probability: 0.82, no_probability: 0.18 },
    };
    const table = tableFromIPC(graph.validateDecisionBatch(batch));
    assert.equal(table.getChild("yes_probability").get(0), 0.82);
    assert.equal(table.getChild("no_probability").get(0), 0.18);
  } finally {
    graph.close();
  }
});
