const MAX_DIAGNOSTICS = 8;
const MAX_PAYLOAD_DEPTH = 16;
const MAX_PAYLOAD_ENTRIES = 4096;
const MAX_PAYLOAD_STRING_LENGTH = 4096;
const MAX_PAYLOAD_OBJECT_PROPERTIES = 128;
const MAX_PAYLOAD_ARRAY_ITEMS = 4096;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const SKILL_ID = /^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$/;
const CAPABILITY_ID = /^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$/;
const ERROR_CODE = /^GF_AGENT_[A-Z0-9_]+$/;

const manifestFields = [
  "schema_version",
  "id",
  "title",
  "description",
  "input_schema",
  "output_schema",
  "required_capabilities",
];
const inputFields = ["schema_version", "skill_id", "request_id", "input"];
const outputFields = ["schema_version", "skill_id", "request_id", "status", "output", "error"];

export function validateSkillManifest(value) {
  const diagnostics = [];
  validateClosedObject(value, "$", manifestFields, manifestFields, diagnostics);
  if (isObject(value)) {
    constant(value.schema_version, 1, "$.schema_version", diagnostics);
    stringPattern(value.id, SKILL_ID, 1, 64, "$.id", diagnostics);
    boundedString(value.title, 1, 120, "$.title", diagnostics);
    boundedString(value.description, 1, 500, "$.description", diagnostics);
    constant(value.input_schema, "graphforge-agent-skill-input/v1", "$.input_schema", diagnostics);
    constant(
      value.output_schema,
      "graphforge-agent-skill-output/v1",
      "$.output_schema",
      diagnostics,
    );
    validateCapabilities(value.required_capabilities, diagnostics);
  }
  return result(diagnostics);
}

export function validateSkillInput(value) {
  const diagnostics = [];
  validateClosedObject(value, "$", inputFields, inputFields, diagnostics);
  if (isObject(value)) {
    validateEnvelopeIdentity(value, diagnostics);
    payloadObject(value.input, 128, "$.input", diagnostics);
  }
  return result(diagnostics);
}

export function validateSkillOutput(value) {
  const diagnostics = [];
  validateClosedObject(value, "$", outputFields, outputFields.slice(0, 4), diagnostics);
  if (isObject(value)) {
    validateEnvelopeIdentity(value, diagnostics);
    if (value.status !== "ok" && value.status !== "error") {
      add(diagnostics, "invalid_value", "$.status", "must be one of the supported values");
    } else if (value.status === "ok") {
      required(value, "output", "$", diagnostics);
      forbidden(value, "error", "$", diagnostics);
      if (Object.hasOwn(value, "output")) payloadObject(value.output, 128, "$.output", diagnostics);
    } else {
      required(value, "error", "$", diagnostics);
      forbidden(value, "output", "$", diagnostics);
      if (Object.hasOwn(value, "error")) validateError(value.error, diagnostics);
    }
  }
  return result(diagnostics);
}

function validateEnvelopeIdentity(value, diagnostics) {
  constant(value.schema_version, 1, "$.schema_version", diagnostics);
  stringPattern(value.skill_id, SKILL_ID, 1, 64, "$.skill_id", diagnostics);
  stringPattern(value.request_id, UUID, 36, 36, "$.request_id", diagnostics);
}

function validateCapabilities(value, diagnostics) {
  if (!objectWithLimit(value, 64, "$.required_capabilities", diagnostics)) return;
  for (const [name, version] of Object.entries(value)) {
    const path = CAPABILITY_ID.test(name)
      ? `$.required_capabilities.${name}`
      : "$.required_capabilities.<field>";
    if (!CAPABILITY_ID.test(name) || name.length > 80) {
      add(diagnostics, "invalid_field", path, "field name does not match the schema");
    }
    if (!Number.isSafeInteger(version) || version < 1 || version > 2147483647) {
      add(diagnostics, "invalid_value", path, "must be a supported positive integer version");
    }
  }
}

function validateError(value, diagnostics) {
  const fields = ["code", "message", "details"];
  if (!validateClosedObject(value, "$.error", fields, ["code", "message"], diagnostics)) return;
  stringPattern(value.code, ERROR_CODE, 1, 80, "$.error.code", diagnostics);
  boundedString(value.message, 1, 500, "$.error.message", diagnostics);
  if (Object.hasOwn(value, "details"))
    payloadObject(value.details, 64, "$.error.details", diagnostics);
}

function payloadObject(value, maximum, path, diagnostics) {
  if (!objectWithLimit(value, maximum, path, diagnostics)) return false;
  const seen = new WeakSet();
  let entries = 0;
  let budgetReported = false;

  function visit(item, depth) {
    entries += 1;
    if (depth > MAX_PAYLOAD_DEPTH || entries > MAX_PAYLOAD_ENTRIES) {
      if (!budgetReported) {
        add(diagnostics, "invalid_size", path, "payload exceeds the recursive schema budget");
        budgetReported = true;
      }
      return;
    }
    if (typeof item === "string" && item.length > MAX_PAYLOAD_STRING_LENGTH) {
      if (!budgetReported) {
        add(diagnostics, "invalid_size", path, "payload exceeds the recursive schema budget");
        budgetReported = true;
      }
      return;
    }
    if (!item || typeof item !== "object") return;
    if (seen.has(item)) {
      if (!budgetReported) {
        add(diagnostics, "invalid_value", path, "payload must not contain cycles");
        budgetReported = true;
      }
      return;
    }
    const nodeSize = Array.isArray(item) ? item.length : Object.keys(item).length;
    const nodeLimit = Array.isArray(item) ? MAX_PAYLOAD_ARRAY_ITEMS : MAX_PAYLOAD_OBJECT_PROPERTIES;
    if (nodeSize > nodeLimit) {
      if (!budgetReported) {
        add(diagnostics, "invalid_size", path, "payload exceeds the recursive schema budget");
        budgetReported = true;
      }
      return;
    }
    seen.add(item);
    for (const [key, child] of Object.entries(item)) {
      visit(key, depth + 1);
      visit(child, depth + 1);
      if (budgetReported) break;
    }
    seen.delete(item);
  }

  visit(value, 0);
  return !budgetReported;
}

function validateClosedObject(value, path, allowed, requiredFields, diagnostics) {
  if (!isObject(value)) {
    add(diagnostics, "invalid_type", path, "must be an object");
    return false;
  }
  for (const field of requiredFields) required(value, field, path, diagnostics);
  for (const field of Object.keys(value)) {
    if (!allowed.includes(field))
      add(diagnostics, "unknown_field", `${path}.<field>`, "field is not allowed");
  }
  return true;
}

function required(value, field, path, diagnostics) {
  if (!Object.hasOwn(value, field))
    add(diagnostics, "missing_field", `${path}.${field}`, "required field is missing");
}

function forbidden(value, field, path, diagnostics) {
  if (Object.hasOwn(value, field))
    add(
      diagnostics,
      "conflicting_field",
      `${path}.${field}`,
      "field is not allowed for this status",
    );
}

function constant(value, expected, path, diagnostics) {
  if (value !== undefined && value !== expected)
    add(diagnostics, "incompatible_version", path, "value is not supported by this schema version");
}

function boundedString(value, minimum, maximum, path, diagnostics) {
  if (value === undefined) return false;
  if (typeof value !== "string") {
    add(diagnostics, "invalid_type", path, "must be a string");
    return false;
  }
  if (value.length < minimum || value.length > maximum) {
    add(diagnostics, "invalid_length", path, "string length is outside the schema bounds");
    return false;
  }
  return true;
}

function stringPattern(value, pattern, minimum, maximum, path, diagnostics) {
  if (boundedString(value, minimum, maximum, path, diagnostics) && !pattern.test(value)) {
    add(diagnostics, "invalid_value", path, "value does not match the required format");
  }
}

function objectWithLimit(value, maximum, path, diagnostics) {
  if (!isObject(value)) {
    if (value !== undefined) add(diagnostics, "invalid_type", path, "must be an object");
    return false;
  }
  if (Object.keys(value).length > maximum)
    add(diagnostics, "invalid_size", path, "object exceeds the schema field limit");
  return true;
}

function isObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function add(diagnostics, code, path, message) {
  diagnostics.push({ code, message, path });
}

function result(diagnostics) {
  const stable = diagnostics
    .sort(
      (left, right) => left.path.localeCompare(right.path) || left.code.localeCompare(right.code),
    )
    .slice(0, MAX_DIAGNOSTICS);
  return { diagnostics: stable, valid: stable.length === 0 };
}
