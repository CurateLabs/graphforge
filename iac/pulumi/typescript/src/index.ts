import { createHash } from "node:crypto";

import * as pulumi from "@pulumi/pulumi";

type JsonPrimitive = boolean | number | string | null;
export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };
type JsonObject = { [key: string]: JsonValue };

export interface TargetValidationArgs {
  /** Canonical, secret-free `graphforge-resolved-config/1` JSON. */
  resolvedConfig: JsonObject;
  /** Stable ID of the target to validate. */
  targetId: string;
}

export interface DeploymentSpecArgs extends TargetValidationArgs {
  /** Immutable, credential-free location of the caller-owned artifact. */
  artifactLocator: string;
}

export interface InfraValidationReceipt extends JsonObject {
  contract: "graphforge-infra-validation/1";
  resolved_config_sha256: string;
  target: JsonObject;
  static_validity: { status: "valid" };
  planned_infrastructure: {
    status: "validated";
    mutation: "none";
    ownership: "embedded" | "local" | "external";
    kind: "embedded" | "service" | "worker" | "job" | "host";
    execution: "process" | "container" | "host";
    scheduling: "long_running" | "on_demand";
    replicas: number;
    artifact: JsonObject;
  };
  connectivity: { status: "not_checked" };
  readiness: { status: "not_checked" };
  capability_compatibility: {
    status: "requirements_declared";
    requirements: JsonObject[];
  };
}

export interface DeploymentSpecDocument extends JsonObject {
  contract: "graphforge-deployment-spec/1";
  resolved_config_sha256: string;
  target_id: string;
  artifact: {
    kind: "python_wheel" | "node_package" | "native_binary" | "oci_image";
    locator: string;
    sha256: string;
    version: string;
  };
  topology: {
    execution: "process" | "container" | "host";
    kind: "embedded" | "service" | "worker" | "job" | "host";
    ownership: "embedded" | "local" | "external";
    replicas: number;
    scheduling: "long_running" | "on_demand";
  };
  requirements: {
    backup: JsonObject;
    health: JsonObject;
    network: JsonObject;
    observability: JsonObject;
    resources: JsonObject;
    storage: JsonObject;
    write: JsonObject;
  };
  bindings: {
    secret_ids: string[];
    source_ids: string[];
  };
  ownership: {
    data: "external";
    infrastructure: "caller_owned";
    runtime: "caller_owned";
    specification: "graphforge";
  };
  infrastructure: { mutation: "none"; status: "caller_owned" };
  connectivity: { status: "not_checked" };
  readiness: { status: "not_checked" };
  capability_compatibility: {
    requirements: JsonObject[];
    status: "requirements_declared";
  };
}

const STABLE_ID = /^[a-z](?:[a-z0-9]|[-_](?=[a-z0-9])){0,63}$/;
const SHA256 = /^[0-9a-f]{64}$/;
const CONTROL_OR_SPACE = /[\u0000-\u0020\u007f]/u;
const WINDOWS_PATH = /^[A-Za-z]:[\\/]/;
const OCI_LOCATOR = /^(?<repository>[^@]+)@sha256:(?<digest>[0-9a-f]{64})$/;
const OCI_REPOSITORY =
  /^[a-z0-9]+(?:[.-][a-z0-9]+)*(?::[1-9][0-9]{0,4})?\/[a-z0-9]+(?:[._-][a-z0-9]+)*(?:\/[a-z0-9]+(?:[._-][a-z0-9]+)*)*$/;

function fail(message: string): never {
  throw new Error(`invalid graphforge-resolved-config/1: ${message}`);
}

function object(value: JsonValue | undefined, label: string): JsonObject {
  if (value === null || Array.isArray(value) || typeof value !== "object") {
    fail(`${label} must be an object`);
  }
  return value as JsonObject;
}

function array(value: JsonValue | undefined, label: string): JsonValue[] {
  if (!Array.isArray(value)) {
    fail(`${label} must be an array`);
  }
  return value;
}

function text(value: JsonValue | undefined, label: string): string {
  if (typeof value !== "string") {
    fail(`${label} must be a string`);
  }
  return value;
}

function integer(value: JsonValue | undefined, label: string, max: number): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 1 || value > max) {
    fail(`${label} must be an integer from 1 through ${max}`);
  }
  return value;
}

function integerFrom(
  value: JsonValue | undefined,
  label: string,
  minimum: number,
  maximum: number,
): number {
  if (
    typeof value !== "number" ||
    !Number.isSafeInteger(value) ||
    value < minimum ||
    value > maximum
  ) {
    fail(`${label} must be an integer from ${minimum} through ${maximum}`);
  }
  return value;
}

function boolean(value: JsonValue | undefined, label: string): boolean {
  if (typeof value !== "boolean") {
    fail(`${label} must be a boolean`);
  }
  return value;
}

function exactKeys(value: JsonObject, required: string[], optional: string[], label: string): void {
  const allowed = new Set([...required, ...optional]);
  for (const key of required) {
    if (!(key in value)) {
      fail(`${label} is missing ${key}`);
    }
  }
  for (const key of Object.keys(value)) {
    if (!allowed.has(key)) {
      fail(`${label} contains unknown field ${key}`);
    }
  }
}

function oneOf<T extends string>(
  value: JsonValue | undefined,
  choices: readonly T[],
  label: string,
): T {
  const candidate = text(value, label);
  if (!choices.includes(candidate as T)) {
    fail(`${label} has an unsupported value`);
  }
  return candidate as T;
}

function validateStableId(value: JsonValue | undefined, label: string): string {
  const candidate = text(value, label);
  if (!STABLE_ID.test(candidate)) {
    fail(`${label} is not a stable ID`);
  }
  return candidate;
}

function validateArtifact(value: JsonValue | undefined): JsonObject {
  const artifact = object(value, "target.artifact");
  exactKeys(artifact, ["kind", "version", "sha256"], [], "target.artifact");
  oneOf(
    artifact.kind,
    ["python_wheel", "node_package", "native_binary", "oci_image"] as const,
    "target.artifact.kind",
  );
  const version = text(artifact.version, "target.artifact.version");
  if (version.length < 1 || version.length > 128) {
    fail("target.artifact.version exceeds contract bounds");
  }
  if (!SHA256.test(text(artifact.sha256, "target.artifact.sha256"))) {
    fail("target.artifact.sha256 must be lowercase SHA-256");
  }
  return artifact;
}

function validateCapabilities(value: JsonValue | undefined): JsonObject[] {
  const capabilities = array(value, "target.capabilities");
  if (capabilities.length > 64) {
    fail("target.capabilities exceeds contract bounds");
  }
  const seen = new Set<string>();
  return capabilities.map((item, index) => {
    const requirement = object(item, `target.capabilities[${index}]`);
    exactKeys(requirement, ["id", "version"], [], `target.capabilities[${index}]`);
    const id = validateStableId(requirement.id, `target.capabilities[${index}].id`);
    if (seen.has(id)) {
      fail("target capability IDs must be unique");
    }
    seen.add(id);
    integer(requirement.version, `target.capabilities[${index}].version`, 65535);
    return requirement;
  });
}

function validateResolvedTarget(value: JsonValue, targetId: string): JsonObject {
  const target = object(value, `target ${targetId}`);
  const required = [
    "id",
    "kind",
    "ownership",
    "artifact",
    "topology",
    "capabilities",
    "write",
    "storage",
    "resources",
    "network",
    "health",
    "observability",
    "backup",
    "source_ids",
    "secret_ids",
  ];
  exactKeys(target, required, [], `target ${targetId}`);
  if (validateStableId(target.id, "target.id") !== targetId) {
    fail("selected target ID does not match target.id");
  }
  const kind = oneOf(
    target.kind,
    ["embedded", "service", "worker", "job", "host"] as const,
    "target.kind",
  );
  const ownership = oneOf(
    target.ownership,
    ["embedded", "local", "external"] as const,
    "target.ownership",
  );
  validateArtifact(target.artifact);
  validateCapabilities(target.capabilities);
  const topology = object(target.topology, "target.topology");
  exactKeys(topology, ["execution", "scheduling", "replicas"], [], "target.topology");
  const execution = oneOf(
    topology.execution,
    ["process", "container", "host"] as const,
    "target.topology.execution",
  );
  const scheduling = oneOf(
    topology.scheduling,
    ["long_running", "on_demand"] as const,
    "target.topology.scheduling",
  );
  const replicas = integer(topology.replicas, "target.topology.replicas", 1024);
  const write = object(target.write, "target.write");
  exactKeys(write, ["mode"], ["queue_capacity", "max_rebase_attempts"], "target.write");
  const writeMode = oneOf(
    write.mode,
    ["single_writer", "queued_writer", "optimistic_multi_writer"] as const,
    "target.write.mode",
  );
  if ("queue_capacity" in write) {
    integer(write.queue_capacity, "target.write.queue_capacity", 65536);
  }
  if ("max_rebase_attempts" in write) {
    integerFrom(write.max_rebase_attempts, "target.write.max_rebase_attempts", 0, 64);
  }
  if (
    (writeMode === "single_writer" &&
      ("queue_capacity" in write || "max_rebase_attempts" in write)) ||
    (writeMode === "queued_writer" && !("queue_capacity" in write)) ||
    (writeMode === "optimistic_multi_writer" && !("max_rebase_attempts" in write))
  ) {
    fail("target.write settings do not match its mode");
  }
  const storage = object(target.storage, "target.storage");
  exactKeys(storage, ["kind"], ["persistent", "class", "capacity_bytes"], "target.storage");
  const storageKind = oneOf(
    storage.kind,
    ["local", "volume", "object"] as const,
    "target.storage.kind",
  );
  if ("persistent" in storage) boolean(storage.persistent, "target.storage.persistent");
  if ("class" in storage) {
    const storageClass = text(storage.class, "target.storage.class");
    if (storageClass.length < 1 || storageClass.length > 128) {
      fail("target.storage.class exceeds contract bounds");
    }
  }
  if ("capacity_bytes" in storage) {
    integer(storage.capacity_bytes, "target.storage.capacity_bytes", Number.MAX_SAFE_INTEGER);
  }
  const resources = object(target.resources, "target.resources");
  exactKeys(resources, [], ["cpu_millis", "memory_bytes"], "target.resources");
  for (const key of ["cpu_millis", "memory_bytes"]) {
    if (key in resources)
      integer(resources[key], `target.resources.${key}`, Number.MAX_SAFE_INTEGER);
  }
  const network = object(target.network, "target.network");
  exactKeys(network, [], ["exposure", "port", "tls_required"], "target.network");
  const exposure =
    "exposure" in network
      ? oneOf(network.exposure, ["none", "private", "public"] as const, "target.network.exposure")
      : undefined;
  if ("port" in network) integer(network.port, "target.network.port", 65535);
  if ("tls_required" in network) boolean(network.tls_required, "target.network.tls_required");
  const health = object(target.health, "target.health");
  exactKeys(health, ["timeout_seconds"], [], "target.health");
  integer(health.timeout_seconds, "target.health.timeout_seconds", 300);
  const observability = object(target.observability, "target.observability");
  exactKeys(observability, [], ["logs", "metrics", "traces"], "target.observability");
  for (const key of ["logs", "metrics", "traces"]) {
    if (key in observability) boolean(observability[key], `target.observability.${key}`);
  }
  const backup = object(target.backup, "target.backup");
  exactKeys(backup, [], ["checkpoints", "retention_count"], "target.backup");
  if ("checkpoints" in backup) boolean(backup.checkpoints, "target.backup.checkpoints");
  if ("retention_count" in backup) {
    integer(backup.retention_count, "target.backup.retention_count", 1024);
    if (backup.checkpoints !== true) {
      fail("backup retention requires checkpoint backups");
    }
  }
  for (const key of ["source_ids", "secret_ids"]) {
    const values = array(target[key], `target.${key}`);
    const maximum = key === "source_ids" ? 256 : 128;
    if (values.length > maximum) fail(`target.${key} exceeds contract bounds`);
    const ids = values.map((id, index) => validateStableId(id, `target.${key}[${index}]`));
    if (new Set(ids).size !== ids.length) {
      fail(`target.${key} must contain unique IDs`);
    }
  }
  if ((kind === "embedded") !== (ownership === "embedded")) {
    fail("embedded ownership is valid only for an embedded target");
  }
  if (
    kind === "embedded" &&
    (execution !== "process" ||
      scheduling !== "long_running" ||
      replicas !== 1 ||
      storageKind !== "local" ||
      (exposure !== undefined && exposure !== "none"))
  ) {
    fail("embedded target requirements are invalid");
  }
  if ((kind === "host") !== (execution === "host")) {
    fail("host target and host execution must be used together");
  }
  if ((kind === "job") !== (scheduling === "on_demand")) {
    fail("job targets are on-demand and other targets are long-running");
  }
  if (kind === "service" && !("port" in network)) {
    fail("service targets require a network port");
  }
  if (exposure === "public" && network.tls_required !== true) {
    fail("public targets require TLS");
  }
  return target;
}

function validateResolvedConfig(resolvedConfig: JsonObject): JsonObject[] {
  exactKeys(
    resolvedConfig,
    ["contract", "project", "sources", "secrets", "targets"],
    [],
    "resolved config",
  );
  if (resolvedConfig.contract !== "graphforge-resolved-config/1") {
    fail("unsupported contract");
  }
  const project = object(resolvedConfig.project, "project");
  const projectKeys = [
    "integration_root",
    "state",
    "imports",
    "exports",
    "ontology",
    "schemas",
    "seeds",
    "migrations",
  ];
  exactKeys(project, projectKeys, [], "project");
  const fixedPaths: Record<string, string> = {
    integration_root: ".graphforge",
    state: ".graphforge/state",
    imports: ".graphforge/imports",
    exports: ".graphforge/exports",
  };
  for (const key of projectKeys) {
    const path = text(project[key], `project.${key}`);
    if (path.length < 1 || path.length > 1024) fail(`project.${key} exceeds contract bounds`);
    if (
      path.startsWith("/") ||
      path.includes("\\") ||
      path.split("/").includes("..") ||
      /[\u0000-\u001f\u007f]/u.test(path)
    ) {
      fail(`project.${key} is not a safe relative path`);
    }
    if (key in fixedPaths && path !== fixedPaths[key]) fail(`project.${key} is not canonical`);
  }
  const sourceIds = new Set<string>();
  const sources = array(resolvedConfig.sources, "sources");
  if (sources.length > 256) fail("sources exceeds contract bounds");
  sources.forEach((item, index) => {
    const source = object(item, `sources[${index}]`);
    exactKeys(source, ["id", "uri", "sha256"], ["media_type"], `sources[${index}]`);
    const id = validateStableId(source.id, `sources[${index}].id`);
    if (sourceIds.has(id)) fail("source IDs must be unique");
    sourceIds.add(id);
    const uri = text(source.uri, `sources[${index}].uri`);
    if (uri.length < 1 || uri.length > 2048 || /^[a-z][a-z0-9+.-]*:\/\/[^/]*@/i.test(uri)) {
      fail(`sources[${index}].uri is invalid`);
    }
    if (!SHA256.test(text(source.sha256, `sources[${index}].sha256`))) {
      fail(`sources[${index}].sha256 must be lowercase SHA-256`);
    }
    if ("media_type" in source) {
      const mediaType = text(source.media_type, `sources[${index}].media_type`);
      if (mediaType.length < 1 || mediaType.length > 128) {
        fail(`sources[${index}].media_type exceeds contract bounds`);
      }
    }
  });
  const secretIds = new Set<string>();
  const secrets = array(resolvedConfig.secrets, "secrets");
  if (secrets.length > 128) fail("secrets exceeds contract bounds");
  secrets.forEach((item, index) => {
    const secret = object(item, `secrets[${index}]`);
    exactKeys(secret, ["id", "source"], [], `secrets[${index}]`);
    const id = validateStableId(secret.id, `secrets[${index}].id`);
    if (secretIds.has(id)) fail("secret IDs must be unique");
    secretIds.add(id);
    oneOf(
      secret.source,
      ["environment", "pulumi", "terraform", "secret_manager"] as const,
      `secrets[${index}].source`,
    );
  });
  const rawTargets = array(resolvedConfig.targets, "targets");
  if (rawTargets.length < 1 || rawTargets.length > 64) fail("targets exceeds contract bounds");
  const targetIds = new Set<string>();
  const targets = rawTargets.map((item, index) => {
    const candidate = object(item, `targets[${index}]`);
    const id = validateStableId(candidate.id, `targets[${index}].id`);
    if (targetIds.has(id)) fail("target IDs must be unique");
    targetIds.add(id);
    const target = validateResolvedTarget(candidate, id);
    for (const sourceId of target.source_ids as JsonValue[]) {
      if (!sourceIds.has(sourceId as string)) fail("target references an unknown source");
    }
    for (const secretId of target.secret_ids as JsonValue[]) {
      if (!secretIds.has(secretId as string)) fail("target references an unknown secret");
    }
    return target;
  });
  return targets;
}

function compareCodeUnits(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0;
}

function encodeJsonScalar(value: JsonPrimitive): string {
  return JSON.stringify(value).replace(
    /[<>&\u2028\u2029]/gu,
    (character) => `\\u${character.charCodeAt(0).toString(16).padStart(4, "0")}`,
  );
}

export function canonicalJson(value: JsonValue): string {
  if (value === null || typeof value !== "object") {
    return encodeJsonScalar(value);
  }
  if (Array.isArray(value)) {
    return `[${value.map(canonicalJson).join(",")}]`;
  }
  const entries = Object.entries(value).sort(([left], [right]) => compareCodeUnits(left, right));
  return `{${entries
    .map(([key, item]) => `${encodeJsonScalar(key)}:${canonicalJson(item)}`)
    .join(",")}}`;
}

/** Deterministically reproduce the Rust-owned static validation receipt. */
export function validateTarget(
  resolvedConfig: JsonObject,
  targetId: string,
): InfraValidationReceipt {
  if (!STABLE_ID.test(targetId)) {
    fail("targetId is not a stable ID");
  }
  const targets = validateResolvedConfig(resolvedConfig);
  const matches = targets.filter((item) => {
    const candidate = object(item, "target");
    return candidate.id === targetId;
  });
  if (matches.length !== 1) {
    fail(`expected exactly one target named ${targetId}`);
  }
  const target = validateResolvedTarget(matches[0], targetId);
  const topology = object(target.topology, "target.topology");
  const capabilities = validateCapabilities(target.capabilities);
  const encoded = canonicalJson(resolvedConfig);

  return {
    contract: "graphforge-infra-validation/1",
    resolved_config_sha256: createHash("sha256").update(encoded, "utf8").digest("hex"),
    target,
    static_validity: { status: "valid" },
    planned_infrastructure: {
      status: "validated",
      mutation: "none",
      ownership: target.ownership as "embedded" | "local" | "external",
      kind: target.kind as "embedded" | "service" | "worker" | "job" | "host",
      execution: topology.execution as "process" | "container" | "host",
      scheduling: topology.scheduling as "long_running" | "on_demand",
      replicas: topology.replicas as number,
      artifact: object(target.artifact, "target.artifact"),
    },
    connectivity: { status: "not_checked" },
    readiness: { status: "not_checked" },
    capability_compatibility: {
      status: "requirements_declared",
      requirements: capabilities,
    },
  };
}

function validateArtifactLocator(value: unknown, kind: string, expectedSha256: string): string {
  if (typeof value !== "string" || value.length < 1 || value.length > 2048) {
    throw new Error(
      "invalid graphforge-deployment-spec/1: artifactLocator must be a bounded string",
    );
  }
  if (CONTROL_OR_SPACE.test(value)) {
    throw new Error(
      "invalid graphforge-deployment-spec/1: artifactLocator contains whitespace or control characters",
    );
  }
  const lowered = value.toLowerCase();
  if (
    value.startsWith("/") ||
    value.startsWith("./") ||
    value.startsWith("../") ||
    value.startsWith("~") ||
    WINDOWS_PATH.test(value) ||
    value.includes("\\") ||
    lowered.startsWith("file:")
  ) {
    throw new Error(
      "invalid graphforge-deployment-spec/1: artifactLocator must not be a local path",
    );
  }

  if (kind === "oci_image") {
    const match = OCI_LOCATOR.exec(value);
    if (match?.groups === undefined) {
      throw new Error(
        "invalid graphforge-deployment-spec/1: OCI artifactLocator must be pinned by sha256 digest",
      );
    }
    const repository = match.groups.repository;
    if (!OCI_REPOSITORY.test(repository)) {
      throw new Error(
        "invalid graphforge-deployment-spec/1: OCI artifactLocator must be registry/repository without a mutable tag",
      );
    }
    if (match.groups.digest !== expectedSha256) {
      throw new Error(
        "invalid graphforge-deployment-spec/1: OCI artifactLocator digest does not match target.artifact.sha256",
      );
    }
    return value;
  }

  let parsed: URL;
  try {
    parsed = new URL(value);
  } catch {
    throw new Error(
      "invalid graphforge-deployment-spec/1: non-OCI artifactLocator must be an https URL",
    );
  }
  if (
    parsed.protocol !== "https:" ||
    parsed.username !== "" ||
    parsed.password !== "" ||
    parsed.search !== "" ||
    parsed.hash !== "" ||
    parsed.hostname === ""
  ) {
    throw new Error(
      "invalid graphforge-deployment-spec/1: non-OCI artifactLocator must be credential-free https without query or fragment",
    );
  }
  return value;
}

/** Render one closed deployment projection without reading project or source data. */
export function renderDeploymentSpec(
  resolvedConfig: JsonObject,
  targetId: string,
  artifactLocator: string,
): DeploymentSpecDocument {
  const validation = validateTarget(resolvedConfig, targetId);
  const target = validation.target;
  const artifact = object(target.artifact, "target.artifact");
  const topology = object(target.topology, "target.topology");
  const capabilities = validateCapabilities(target.capabilities);
  const locator = validateArtifactLocator(
    artifactLocator,
    artifact.kind as string,
    artifact.sha256 as string,
  );

  return {
    contract: "graphforge-deployment-spec/1",
    resolved_config_sha256: validation.resolved_config_sha256,
    target_id: targetId,
    artifact: {
      kind: artifact.kind as DeploymentSpecDocument["artifact"]["kind"],
      locator,
      sha256: artifact.sha256 as string,
      version: artifact.version as string,
    },
    topology: {
      execution: topology.execution as DeploymentSpecDocument["topology"]["execution"],
      kind: target.kind as DeploymentSpecDocument["topology"]["kind"],
      ownership: target.ownership as DeploymentSpecDocument["topology"]["ownership"],
      replicas: topology.replicas as number,
      scheduling: topology.scheduling as DeploymentSpecDocument["topology"]["scheduling"],
    },
    requirements: {
      backup: object(target.backup, "target.backup"),
      health: object(target.health, "target.health"),
      network: object(target.network, "target.network"),
      observability: object(target.observability, "target.observability"),
      resources: object(target.resources, "target.resources"),
      storage: object(target.storage, "target.storage"),
      write: object(target.write, "target.write"),
    },
    bindings: {
      secret_ids: [...(target.secret_ids as string[])],
      source_ids: [...(target.source_ids as string[])],
    },
    ownership: {
      data: "external",
      infrastructure: "caller_owned",
      runtime: "caller_owned",
      specification: "graphforge",
    },
    infrastructure: { mutation: "none", status: "caller_owned" },
    connectivity: { status: "not_checked" },
    readiness: { status: "not_checked" },
    capability_compatibility: {
      requirements: capabilities,
      status: "requirements_declared",
    },
  };
}

/** Emit canonical UTF-8 deployment JSON with one trailing LF. */
export function renderDeploymentSpecJson(
  resolvedConfig: JsonObject,
  targetId: string,
  artifactLocator: string,
): string {
  return `${canonicalJson(renderDeploymentSpec(resolvedConfig, targetId, artifactLocator))}\n`;
}

/**
 * A state-safe, provider-free static validation component.
 *
 * The resolved configuration is intentionally not passed to `super`, so it is
 * neither a component input nor a state value. Only the secret-free receipt is
 * registered as an output.
 */
export class TargetValidation extends pulumi.ComponentResource {
  public readonly receipt: pulumi.Output<InfraValidationReceipt>;

  public constructor(
    name: string,
    args: TargetValidationArgs,
    opts?: pulumi.ComponentResourceOptions,
  ) {
    const receipt = validateTarget(args.resolvedConfig, args.targetId);
    super("graphforge:static:TargetValidation", name, {}, opts);
    this.receipt = pulumi.output(receipt);
    this.registerOutputs({ receipt: this.receipt });
  }
}

/** Provider-free state projection; caller IaC owns every deployed resource. */
export class DeploymentSpec extends pulumi.ComponentResource {
  public readonly spec: pulumi.Output<DeploymentSpecDocument>;
  public readonly canonicalJson: pulumi.Output<string>;

  public constructor(
    name: string,
    args: DeploymentSpecArgs,
    opts?: pulumi.ComponentResourceOptions,
  ) {
    const allowed = new Set(["resolvedConfig", "targetId", "artifactLocator"]);
    for (const key of Object.keys(args)) {
      if (!allowed.has(key)) {
        throw new Error(`invalid graphforge-deployment-spec/1: unknown input ${key}`);
      }
    }
    const spec = renderDeploymentSpec(args.resolvedConfig, args.targetId, args.artifactLocator);
    const encoded = `${canonicalJson(spec)}\n`;
    // Full resolved configuration and any secret values are deliberately not
    // component inputs. Only the bounded reference-only projection enters state.
    super("graphforge:deployment:DeploymentSpec", name, {}, opts);
    this.spec = pulumi.output(spec);
    this.canonicalJson = pulumi.output(encoded);
    this.registerOutputs({ spec: this.spec, canonicalJson: this.canonicalJson });
  }
}
