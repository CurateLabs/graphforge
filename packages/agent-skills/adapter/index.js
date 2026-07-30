import { constants } from "node:fs";
import { lstat, open, realpath } from "node:fs/promises";
import { isAbsolute, parse, relative, resolve, sep } from "node:path";

export const ADAPTER_CONTRACT_VERSION = 1;
export const PROJECT_FORMAT = "graphforge-project/v1\n";
export const VALUE_BUDGETS = Object.freeze({
  maxDepth: 16,
  maxEntries: 4096,
  maxStringLength: 4096,
});

const SAFE_NATIVE_CODE = /^GF_[A-Z0-9_]{1,72}$/;
const SENSITIVE_CODE = /(?:SECRET|TOKEN|PASSWORD|CREDENTIAL|API_KEY|PRIVATE_KEY)/;
const CONTROL_CHARACTER = /[\u0000-\u001f\u007f]/;

export class AgentAdapterError extends Error {
  constructor(code, message, details = {}) {
    super(message);
    this.name = "AgentAdapterError";
    this.code = code;
    this.contractVersion = ADAPTER_CONTRACT_VERSION;
    try {
      this.details = canonicalize(details);
    } catch {
      this.details = { details_omitted: true };
    }
  }

  toJSON() {
    return {
      code: this.code,
      contract_version: this.contractVersion,
      details: this.details,
      message: this.message,
    };
  }
}

export function normalizeGraphForgeError(error) {
  if (error instanceof AgentAdapterError) return error;
  const nativeCode =
    typeof error?.code === "string" &&
    SAFE_NATIVE_CODE.test(error.code) &&
    !SENSITIVE_CODE.test(error.code)
      ? error.code
      : "GF_AGENT_GRAPHFORGE_ERROR";
  return new AgentAdapterError(nativeCode, "GraphForge operation failed");
}

export function requestSubprocess() {
  throw new AgentAdapterError(
    "GF_AGENT_SUBPROCESS_UNSUPPORTED",
    "subprocess execution is not supported by the shared adapter",
  );
}

export async function validateProjectPath({ path, cwd = process.cwd() }) {
  if (
    typeof path !== "string" ||
    path.length === 0 ||
    path.length > 4096 ||
    CONTROL_CHARACTER.test(path) ||
    path.split(/[\\/]+/).includes("..")
  ) {
    throw new AgentAdapterError(
      "GF_AGENT_INVALID_PROJECT_PATH",
      "project path is not a safe bounded path",
    );
  }
  const candidate = resolve(cwd, path);
  if (!isAbsolute(path)) {
    const fromRoot = relative(resolve(cwd), candidate);
    if (fromRoot === ".." || fromRoot.startsWith(`..${sep}`) || isAbsolute(fromRoot)) {
      throw new AgentAdapterError(
        "GF_AGENT_INVALID_PROJECT_PATH",
        "project path must remain within the discovery root",
      );
    }
  }
  if (await containsSymlink(candidate)) {
    throw new AgentAdapterError(
      "GF_AGENT_INVALID_PROJECT_PATH",
      "project path must not contain symlinks",
    );
  }
  return candidate;
}

export async function discoverProject({ candidates, cwd = process.cwd() } = {}) {
  const inputs = candidates ?? [cwd];
  if (!Array.isArray(inputs) || inputs.length === 0) {
    throw new AgentAdapterError(
      "GF_AGENT_PROJECT_NOT_FOUND",
      "no GraphForge project candidates were provided",
    );
  }

  const matches = [];
  let unsupportedCount = 0;
  for (const input of inputs) {
    if (
      typeof input !== "string" ||
      input.length === 0 ||
      input.length > 4096 ||
      CONTROL_CHARACTER.test(input)
    ) {
      throw new AgentAdapterError(
        "GF_AGENT_INVALID_PROJECT_PATH",
        "project candidates must be bounded paths without control characters",
      );
    }
    const segments = input.split(/[\\/]+/);
    if (segments.includes("..")) {
      throw new AgentAdapterError(
        "GF_AGENT_INVALID_PROJECT_PATH",
        "project candidates must not contain parent traversal",
      );
    }
    const candidate = resolve(cwd, input);
    if (!isAbsolute(input)) {
      const fromRoot = relative(resolve(cwd), candidate);
      if (fromRoot === ".." || fromRoot.startsWith(`..${sep}`) || isAbsolute(fromRoot)) {
        throw new AgentAdapterError(
          "GF_AGENT_INVALID_PROJECT_PATH",
          "project candidates must remain within the discovery root",
        );
      }
    }
    if (await containsSymlink(candidate)) continue;
    const stat = await lstat(candidate).catch(() => null);
    if (!stat?.isDirectory() || stat.isSymbolicLink()) continue;
    const canonical = await realpath(candidate);
    const markerPath = resolve(canonical, "FORMAT");
    const handle = await open(markerPath, constants.O_RDONLY | constants.O_NOFOLLOW).catch(
      () => null,
    );
    if (!handle) continue;
    let marker = null;
    try {
      const markerStat = await handle.stat();
      if (!markerStat.isFile()) continue;
      if (markerStat.size > 64) {
        unsupportedCount += 1;
        continue;
      }
      marker = await handle.readFile("utf8");
    } catch {
      continue;
    } finally {
      await handle.close();
    }
    if (marker === PROJECT_FORMAT) matches.push(canonical);
    else if (marker !== null) unsupportedCount += 1;
  }

  const unique = [...new Set(matches)].sort();
  if (unique.length === 0) {
    if (unsupportedCount > 0) {
      throw new AgentAdapterError(
        "GF_AGENT_PROJECT_UNSUPPORTED",
        "only unsupported GraphForge project formats were discovered",
        { candidate_count: unsupportedCount },
      );
    }
    throw new AgentAdapterError(
      "GF_AGENT_PROJECT_NOT_FOUND",
      "no supported GraphForge project was discovered",
    );
  }
  if (unique.length !== 1) {
    throw new AgentAdapterError(
      "GF_AGENT_PROJECT_AMBIGUOUS",
      "multiple supported GraphForge projects were discovered",
      { candidate_count: unique.length },
    );
  }
  return unique[0];
}

async function containsSymlink(candidate) {
  const { root } = parse(candidate);
  let current = root;
  for (const segment of candidate.slice(root.length).split(sep).filter(Boolean)) {
    current = resolve(current, segment);
    const stat = await lstat(current).catch(() => null);
    if (!stat) return false;
    if (stat.isSymbolicLink()) return true;
  }
  return false;
}

const WRITE_MODES = new Set(["single_writer", "queued_writer", "optimistic_multi_writer"]);

export function normalizeWriteOptions(options = {}) {
  if (options === null || typeof options !== "object" || Array.isArray(options)) {
    throw new AgentAdapterError(
      "GF_AGENT_ADAPTER_CONFIGURATION",
      "write options must be an object",
    );
  }
  const { writeMode = "single_writer", writeQueueCapacity = 64, maxRebaseAttempts = 3 } = options;
  if (!WRITE_MODES.has(writeMode)) {
    throw new AgentAdapterError(
      "GF_AGENT_ADAPTER_CONFIGURATION",
      "write mode must be single_writer, queued_writer, or optimistic_multi_writer",
    );
  }
  if (
    !Number.isInteger(writeQueueCapacity) ||
    writeQueueCapacity < 1 ||
    writeQueueCapacity > 65_536
  ) {
    throw new AgentAdapterError(
      "GF_AGENT_ADAPTER_CONFIGURATION",
      "write queue capacity must be an integer between 1 and 65536",
    );
  }
  if (!Number.isInteger(maxRebaseAttempts) || maxRebaseAttempts < 0 || maxRebaseAttempts > 32) {
    throw new AgentAdapterError(
      "GF_AGENT_ADAPTER_CONFIGURATION",
      "max rebase attempts must be an integer between 0 and 32",
    );
  }
  return { maxRebaseAttempts, writeMode, writeQueueCapacity };
}

export async function openProject({
  path,
  GraphForge,
  tableFromIPC,
  requiredCapabilities = {},
  writeOptions,
}) {
  if (typeof GraphForge !== "function" || typeof tableFromIPC !== "function") {
    throw new AgentAdapterError(
      "GF_AGENT_ADAPTER_CONFIGURATION",
      "GraphForge and tableFromIPC shipped surfaces are required",
    );
  }
  const projectPath = await discoverProject({ candidates: [path] });
  const normalizedWriteOptions = normalizeWriteOptions(writeOptions);
  let graph;
  try {
    await validateProjectPath({ path: projectPath });
    graph = new GraphForge(projectPath, normalizedWriteOptions);
    const capabilities = capabilitiesFromTable(tableFromIPC(await graph.projectCapabilities()));
    requireCapabilities(capabilities, requiredCapabilities);
    return { capabilities, graph, path: projectPath };
  } catch (error) {
    try {
      graph?.close?.();
    } catch {
      // Cleanup must not replace the structured open/capability failure.
    }
    throw normalizeGraphForgeError(error);
  }
}

export function capabilitiesFromTable(table) {
  const ids = table?.getChild?.("capability_id");
  const versions = table?.getChild?.("capability_version");
  const statuses = table?.getChild?.("status");
  if (!Number.isInteger(table?.numRows) || !ids || !versions) {
    throw new AgentAdapterError(
      "GF_AGENT_INVALID_CAPABILITY_TABLE",
      "GraphForge returned an invalid capability table",
    );
  }
  const result = {};
  for (let row = 0; row < table.numRows; row += 1) {
    const id = ids.get(row);
    const version = Number(versions.get(row));
    const status = statuses?.get(row) ?? "supported";
    if (typeof id !== "string" || !Number.isSafeInteger(version)) {
      throw new AgentAdapterError(
        "GF_AGENT_INVALID_CAPABILITY_TABLE",
        "GraphForge returned an invalid capability row",
      );
    }
    if (Object.hasOwn(result, id)) {
      throw new AgentAdapterError(
        "GF_AGENT_INVALID_CAPABILITY_TABLE",
        "GraphForge returned duplicate capability rows",
      );
    }
    result[id] = { status, version };
  }
  return canonicalize(result);
}

export function requireCapabilities(actual, required) {
  for (const [id, version] of Object.entries(required).sort()) {
    if (
      typeof id !== "string" ||
      id.length === 0 ||
      !Number.isSafeInteger(version) ||
      version < 1
    ) {
      throw new AgentAdapterError(
        "GF_AGENT_ADAPTER_CONFIGURATION",
        "required capabilities must use non-empty IDs and positive integer versions",
      );
    }
    const capability = actual[id];
    if (!capability) {
      throw new AgentAdapterError(
        "GF_AGENT_CAPABILITY_MISSING",
        `required GraphForge capability is missing: ${id}`,
        { capability_id: id, required_version: version },
      );
    }
    if (capability.status !== "supported" || capability.version !== version) {
      throw new AgentAdapterError(
        "GF_AGENT_CAPABILITY_UNSUPPORTED",
        `unsupported GraphForge capability version: ${id}@${capability.version}`,
        {
          actual_status: capability.status,
          actual_version: capability.version,
          capability_id: id,
          required_version: version,
        },
      );
    }
  }
}

export function uuidToString(value) {
  if (typeof value === "string") {
    const normalized = value.toLowerCase();
    if (/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(normalized)) {
      return normalized;
    }
  }
  if (ArrayBuffer.isView(value) && value.byteLength === 16) {
    const bytes = Buffer.from(value.buffer, value.byteOffset, value.byteLength);
    const hex = bytes.toString("hex");
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
  }
  throw new AgentAdapterError("GF_AGENT_INVALID_UUID", "expected a canonical UUID");
}

export function tableToJson(table) {
  if (!Number.isInteger(table?.numRows) || !Array.isArray(table?.schema?.fields)) {
    throw new AgentAdapterError("GF_AGENT_INVALID_ARROW_TABLE", "expected an Apache Arrow table");
  }
  const fields = table.schema.fields.map(({ name }) => name);
  return Array.from({ length: table.numRows }, (_, row) =>
    Object.fromEntries(fields.map((name) => [name, jsonValue(table.getChild(name).get(row))])),
  );
}

export function stableJson(value) {
  return `${JSON.stringify(canonicalize(value))}\n`;
}

function jsonValue(value) {
  if (typeof value === "bigint") return value.toString();
  if (ArrayBuffer.isView(value) && value.byteLength === 16) return uuidToString(value);
  if (Array.isArray(value)) return canonicalize(value);
  if (value && typeof value === "object") return canonicalize(value);
  return value;
}

function canonicalize(value) {
  const seen = new WeakSet();
  let entries = 0;

  function visit(item, depth) {
    entries += 1;
    if (entries > VALUE_BUDGETS.maxEntries || depth > VALUE_BUDGETS.maxDepth) {
      throw new AgentAdapterError(
        "GF_AGENT_VALUE_BUDGET_EXCEEDED",
        "value exceeds the shared adapter budget",
      );
    }
    if (typeof item === "string" && item.length > VALUE_BUDGETS.maxStringLength) {
      throw new AgentAdapterError(
        "GF_AGENT_VALUE_BUDGET_EXCEEDED",
        "value exceeds the shared adapter budget",
      );
    }
    if (typeof item === "bigint") return item.toString();
    if (ArrayBuffer.isView(item)) {
      if (item.byteLength > VALUE_BUDGETS.maxEntries) {
        throw new AgentAdapterError(
          "GF_AGENT_VALUE_BUDGET_EXCEEDED",
          "value exceeds the shared adapter budget",
        );
      }
      return item.byteLength === 16 ? uuidToString(item) : Array.from(item);
    }
    if (!item || typeof item !== "object") return item;
    if (seen.has(item)) {
      throw new AgentAdapterError("GF_AGENT_CYCLIC_VALUE", "cyclic values are not supported");
    }
    seen.add(item);
    try {
      if (Array.isArray(item)) return item.map((child) => visit(child, depth + 1));
      if (typeof item[Symbol.iterator] === "function") {
        return Array.from(item, (child) => visit(child, depth + 1)).sort(compareCanonical);
      }
      return Object.fromEntries(
        Object.entries(item)
          .sort(([left], [right]) => compareCodeUnits(left, right))
          .map(([key, child]) => [visit(key, depth + 1), visit(child, depth + 1)]),
      );
    } finally {
      seen.delete(item);
    }
  }

  return visit(value, 0);
}

function compareCanonical(left, right) {
  return compareCodeUnits(JSON.stringify(left), JSON.stringify(right));
}

function compareCodeUnits(left, right) {
  return left < right ? -1 : left > right ? 1 : 0;
}
