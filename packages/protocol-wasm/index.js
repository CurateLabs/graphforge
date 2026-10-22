// Glue between JavaScript values and the WebAssembly exports. It converts
// arguments to the primitives the exports take and results to plain objects; it
// holds no protocol logic. Rust never reads a JavaScript object: a plain-object
// argument becomes JSON text here, with `JSON.stringify`, so an object and its
// JSON text always reach Rust as the same string.
import * as raw from "./dist/graphforge_protocol_wasm.js";

export { default, initSync } from "./dist/graphforge_protocol_wasm.js";

const refuse = (message) => ({ code: "invalid_argument", message });

/** An unpredictable marker for one conversion; never derived from the input. */
function marker() {
  const bytes = crypto.getRandomValues(new Uint8Array(16));
  return `gf-bigint-${Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("")}:`;
}

/**
 * JSON text of an argument. A string is used as is (it must be well-formed
 * UTF-16, because the engine would otherwise see U+FFFD for a lone surrogate).
 * Anything else goes through `JSON.stringify`, with two differences: a `bigint`
 * is written exactly, and an integer above 2^53 (already rounded) or a
 * non-finite number is refused instead of being rounded or turned into `null`.
 *
 * A `bigint` is stringified as a marked string and the marks are then replaced
 * by its digits. The replacement happens only when the number of marks in the
 * text equals the number of bigints converted, so a caller string that contains
 * the mark can never be rewritten; on such a collision the conversion retries
 * with a fresh mark.
 */
function toJson(value, name) {
  if (typeof value === "string") {
    if (!value.isWellFormed()) {
      throw refuse(`${name} is not well-formed UTF-16 text`);
    }
    return value;
  }
  const refusal = refuse(
    `${name} holds an integer above 2^53 or a number that is not finite; send a bigint, or a JSON string`,
  );
  for (let attempt = 0; attempt < 4; attempt += 1) {
    const mark = marker();
    let bigints = 0;
    let text;
    try {
      text = JSON.stringify(value, (_key, member) => {
        if (typeof member === "bigint") {
          bigints += 1;
          return `${mark}${member}`;
        }
        if (
          typeof member === "number" &&
          (!Number.isFinite(member) || (Number.isInteger(member) && !Number.isSafeInteger(member)))
        ) {
          throw refusal;
        }
        return member;
      });
    } catch (error) {
      // Anything else was thrown by the caller's own getters, `toJSON` or proxy.
      throw error === refusal ? refusal : refuse(`${name} could not be converted to JSON`);
    }
    if (typeof text !== "string") {
      throw refuse(`${name} is not a JSON value`);
    }
    if (bigints === 0) return text;
    // Each bigint left exactly one mark; any other mark came from the caller.
    if (text.split(mark).length - 1 === bigints) {
      return text.replace(new RegExp(`"${mark}(-?\\d+)"`, "g"), "$1");
    }
  }
  throw refuse(`${name} could not be converted to JSON`);
}

function toBytes(value, name) {
  try {
    if (Object.prototype.toString.call(value) === "[object Uint8Array]") {
      return new Uint8Array(value);
    }
  } catch {
    // fall through to the refusal
  }
  throw refuse(`${name} must be a Uint8Array`);
}

const ARGUMENT = {
  // a JSON document: text, or any value `JSON.stringify` accepts
  json: (value, name) => toJson(value, name),
  // an optional JSON document; `undefined` and `null` are absent
  optional: (value, name) =>
    value === undefined || value === null ? undefined : toJson(value, name),
  string: (value, name) => {
    if (typeof value !== "string" || !value.isWellFormed()) {
      throw refuse(`${name} must be a well-formed string`);
    }
    return value;
  },
  bytes: toBytes,
};

/**
 * Results are built by Rust with `Map`s for every map. `Object.fromEntries`
 * creates own data properties, so a member named `__proto__` stays a member.
 */
function plain(value) {
  if (value instanceof Map) {
    return Object.fromEntries([...value].map(([key, member]) => [key, plain(member)]));
  }
  if (Array.isArray(value)) return value.map(plain);
  if (typeof value === "object" && value !== null) {
    for (const key of Object.keys(value)) value[key] = plain(value[key]);
  }
  return value;
}

function wrap(name, parameters) {
  const call = raw[name];
  return (...values) =>
    plain(call(...parameters.map(([kind, label], index) => ARGUMENT[kind](values[index], label))));
}

const doc = (label) => ["json", label];
const limits = ["optional", "limits"];

export const parse_item_id = wrap("parse_item_id", [["string", "reference"]]);
export const has_item_id_shape = wrap("has_item_id_shape", [["string", "name"]]);
export const validate_manifest = wrap("validate_manifest", [doc("manifest"), limits]);
export const validate_refs = wrap("validate_refs", [doc("refs"), limits]);
export const validate_summary = wrap("validate_summary", [doc("summary"), limits]);
export const validate_lineage = wrap("validate_lineage", [doc("lineage"), limits]);
export const validate_repository_moved = wrap("validate_repository_moved", [doc("moved"), limits]);
export const bind_summary = wrap("bind_summary", [doc("manifest"), doc("summary"), limits]);
export const bind_lineage = wrap("bind_lineage", [
  doc("manifest"),
  doc("refs"),
  doc("lineage"),
  limits,
]);
export const bind_summary_lineage = wrap("bind_summary_lineage", [
  doc("summary"),
  ["optional", "lineage"],
  limits,
]);
export const bind_refs = wrap("bind_refs", [doc("refs"), doc("manifest"), limits]);
export const ontology_module_object = wrap("ontology_module_object", [
  doc("manifest"),
  doc("identity"),
  limits,
]);
export const research_version_object = wrap("research_version_object", [
  doc("manifest"),
  doc("lineage"),
  ["string", "version_uuid"],
  limits,
]);
export const rehome_repository = wrap("rehome_repository", [
  doc("refs"),
  doc("snapshots"),
  doc("new_identity"),
  ["string", "control_locations"],
  limits,
]);
export const validate_image = wrap("validate_image", [
  ["string", "media_type"],
  ["bytes", "bytes"],
]);
export const validate_preview = wrap("validate_preview", [doc("descriptor"), ["bytes", "bytes"]]);
export const validate_preview_descriptors = wrap("validate_preview_descriptors", [
  doc("descriptors"),
]);
export const manifest_preview_descriptors = wrap("manifest_preview_descriptors", [
  doc("manifest"),
  limits,
]);
export const validate_capabilities = wrap("validate_capabilities", [doc("capabilities")]);
export const validate_session_request = wrap("validate_session_request", [doc("request")]);
export const validate_commit_request = wrap("validate_commit_request", [doc("request")]);
export const validate_receipt = wrap("validate_receipt", [doc("receipt")]);
export const validate_operation_status = wrap("validate_operation_status", [doc("status")]);
export const request_commitment = wrap("request_commitment", [doc("intent")]);
export const check_commit_manifest = wrap("check_commit_manifest", [doc("input")]);
