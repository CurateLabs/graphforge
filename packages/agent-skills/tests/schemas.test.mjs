import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

import {
  validateSkillInput,
  validateSkillManifest,
  validateSkillOutput,
} from "../schemas/validator.js";

const requestId = "00010203-0405-0607-0809-0a0b0c0d0e0f";
const manifest = {
  schema_version: 1,
  id: "explore-graph",
  title: "Explore graph",
  description: "Explore a GraphForge project without mutation.",
  input_schema: "graphforge-agent-skill-input/v1",
  output_schema: "graphforge-agent-skill-output/v1",
  required_capabilities: { graph: 1, "workspace.read": 1 },
};
const input = {
  schema_version: 1,
  skill_id: manifest.id,
  request_id: requestId,
  input: { limit: 10 },
};

test("accepts version-1 manifests and input/output envelopes", () => {
  assert.deepEqual(validateSkillManifest(manifest), {
    diagnostics: [],
    valid: true,
  });
  assert.deepEqual(validateSkillInput(input), { diagnostics: [], valid: true });
  assert.deepEqual(
    validateSkillOutput({
      schema_version: 1,
      skill_id: manifest.id,
      request_id: requestId,
      status: "ok",
      output: { rows: [] },
    }),
    { diagnostics: [], valid: true },
  );
  assert.deepEqual(
    validateSkillOutput({
      schema_version: 1,
      skill_id: manifest.id,
      request_id: requestId,
      status: "error",
      error: {
        code: "GF_AGENT_INVALID_INPUT",
        message: "input is invalid",
        details: {},
      },
    }),
    { diagnostics: [], valid: true },
  );
});

test("fails closed for missing, unknown, and malformed fields", () => {
  const value = { ...manifest, id: "Not Valid", title: 7, mystery: true };
  delete value.description;
  assert.deepEqual(validateSkillManifest(value).diagnostics, [
    {
      code: "unknown_field",
      message: "field is not allowed",
      path: "$.<field>",
    },
    {
      code: "missing_field",
      message: "required field is missing",
      path: "$.description",
    },
    {
      code: "invalid_value",
      message: "value does not match the required format",
      path: "$.id",
    },
    { code: "invalid_type", message: "must be a string", path: "$.title" },
  ]);
  assert.deepEqual(validateSkillInput(null), {
    diagnostics: [
      { code: "invalid_type", message: "must be an object", path: "$" },
    ],
    valid: false,
  });
});

test("rejects incompatible versions and malformed envelope values", () => {
  const result = validateSkillInput({
    ...input,
    schema_version: 2,
    request_id: requestId.toUpperCase(),
    input: [],
  });
  assert.deepEqual(result.diagnostics, [
    { code: "invalid_type", message: "must be an object", path: "$.input" },
    {
      code: "invalid_value",
      message: "value does not match the required format",
      path: "$.request_id",
    },
    {
      code: "incompatible_version",
      message: "value is not supported by this schema version",
      path: "$.schema_version",
    },
  ]);
});

test("enforces the output status union", () => {
  const base = {
    schema_version: 1,
    skill_id: manifest.id,
    request_id: requestId,
  };
  assert.deepEqual(
    validateSkillOutput({ ...base, status: "ok", error: {} }).diagnostics,
    [
      {
        code: "conflicting_field",
        message: "field is not allowed for this status",
        path: "$.error",
      },
      {
        code: "missing_field",
        message: "required field is missing",
        path: "$.output",
      },
    ],
  );
  assert.deepEqual(
    validateSkillOutput({
      ...base,
      status: "error",
      error: { code: "bad", message: "" },
    }).diagnostics,
    [
      {
        code: "invalid_value",
        message: "value does not match the required format",
        path: "$.error.code",
      },
      {
        code: "invalid_length",
        message: "string length is outside the schema bounds",
        path: "$.error.message",
      },
    ],
  );
});

test("bounds, sanitizes, and stably orders diagnostics", () => {
  const hostile = "SECRET_TOKEN_DO_NOT_ECHO";
  const value = Object.fromEntries(
    Array.from({ length: 20 }, (_, index) => [`${hostile}_${index}`, hostile]),
  );
  const first = validateSkillManifest(value);
  const second = validateSkillManifest(
    Object.fromEntries(Object.entries(value).reverse()),
  );
  assert.equal(first.valid, false);
  assert.equal(first.diagnostics.length, 8);
  assert.deepEqual(first, second);
  assert.equal(JSON.stringify(first).includes(hostile), false);
});

test("bounds recursive payload depth, entries, strings, and cycles", () => {
  const base = {
    schema_version: 1,
    skill_id: manifest.id,
    request_id: requestId,
  };
  const nested = {};
  let cursor = nested;
  for (let depth = 0; depth < 18; depth += 1) {
    cursor.next = {};
    cursor = cursor.next;
  }
  const cyclic = {};
  cyclic.self = cyclic;

  for (const payload of [
    nested,
    cyclic,
    { secret: "SECRET_TOKEN_DO_NOT_ECHO".repeat(200) },
    {
      wide: Object.fromEntries(
        Array.from({ length: 129 }, (_, index) => [`property${index}`, true]),
      ),
    },
    Object.fromEntries(
      Array.from({ length: 128 }, (_, index) => [
        `k${index}`,
        Array.from({ length: 40 }, (_, item) => item),
      ]),
    ),
  ]) {
    const result = validateSkillInput({ ...base, input: payload });
    assert.equal(result.valid, false);
    assert.equal(
      JSON.stringify(result).includes("SECRET_TOKEN_DO_NOT_ECHO"),
      false,
    );
    assert.equal(result.diagnostics.length <= 8, true);
  }
});

test("checked-in schemas are closed, versioned, and aligned with the validator", async () => {
  for (const name of [
    "skill-manifest-v1",
    "input-envelope-v1",
    "output-envelope-v1",
  ]) {
    const schema = JSON.parse(
      await readFile(
        new URL(`../schemas/${name}.json`, import.meta.url),
        "utf8",
      ),
    );
    assert.equal(
      schema.$schema,
      "https://json-schema.org/draft/2020-12/schema",
    );
    assert.match(schema.$id, new RegExp(`/${name}\\.json$`));
    assert.equal(schema.additionalProperties, false);
    assert.equal(schema.properties.schema_version.const, 1);
  }
});

test("shipped workflow manifests satisfy the shared contract", async () => {
  const expectedCapabilities = {
    bootstrap: { graph: 1 },
    "build-knowledge": { graph: 1, knowledge: 1, provenance: 1 },
  };
  for (const id of ["bootstrap", "build-knowledge"]) {
    const value = JSON.parse(
      await readFile(
        new URL(`../skills/${id}/manifest.json`, import.meta.url),
        "utf8",
      ),
    );
    assert.deepEqual(validateSkillManifest(value), {
      diagnostics: [],
      valid: true,
    });
    assert.equal(value.id, id);
    assert.deepEqual(value.required_capabilities, expectedCapabilities[id]);
  }
});
