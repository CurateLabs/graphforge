import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import test from "node:test";

import * as pulumi from "@pulumi/pulumi";

import {
  DeploymentSpec,
  DeploymentSpecDocument,
  JsonValue,
  canonicalJson,
  renderDeploymentSpec,
  renderDeploymentSpecJson,
} from "../src/index";

const fixturePath = resolve(
  __dirname,
  "../../../../../docs/contracts/examples/graphforge-resolved-v1.json",
);
const fixture = JSON.parse(readFileSync(fixturePath, "utf8")) as {
  [key: string]: JsonValue;
};
const goldenPath = resolve(
  __dirname,
  "../../../../../docs/contracts/examples/graphforge-deployment-spec-production-v1.json",
);
const golden = JSON.parse(readFileSync(goldenPath, "utf8")) as DeploymentSpecDocument;
const productionLocator = "registry.example.com/graphforge/core@sha256:" + "c".repeat(64);

function cloneFixture(): { [key: string]: JsonValue } {
  return JSON.parse(JSON.stringify(fixture)) as { [key: string]: JsonValue };
}

function target(config: { [key: string]: JsonValue }, id: string): Record<string, JsonValue> {
  const targets = config.targets as Record<string, JsonValue>[];
  const selected = targets.find((item) => item.id === id);
  assert.ok(selected);
  return selected;
}

function canonical(value: JsonValue): string {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value).replace(
      /[<>&\u2028\u2029]/gu,
      (character) => `\\u${character.charCodeAt(0).toString(16).padStart(4, "0")}`,
    );
  }
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  return `{${Object.entries(value)
    .sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))
    .map(([key, item]) => `${canonical(key)}:${canonical(item)}`)
    .join(",")}}`;
}

test("renderer emits the frozen provider-neutral deployment shape", () => {
  const spec = renderDeploymentSpec(fixture, "production", productionLocator);

  assert.deepEqual(spec, {
    contract: "graphforge-deployment-spec/1",
    resolved_config_sha256: "eb9bd7c49ae277c62892b028d2c93e8328c24dd3e3fd5ecbabb4f2c94637e9e7",
    target_id: "production",
    artifact: {
      kind: "oci_image",
      locator: productionLocator,
      sha256: "c".repeat(64),
      version: "0.5.1",
    },
    topology: {
      execution: "container",
      kind: "service",
      ownership: "external",
      replicas: 2,
      scheduling: "long_running",
    },
    requirements: {
      backup: { checkpoints: true, retention_count: 14 },
      health: { timeout_seconds: 30 },
      network: { exposure: "private", port: 8443, tls_required: true },
      observability: { logs: true, metrics: true, traces: false },
      resources: { cpu_millis: 1000, memory_bytes: 2147483648 },
      storage: {
        capacity_bytes: 10737418240,
        kind: "volume",
        persistent: true,
      },
      write: { mode: "queued_writer", queue_capacity: 64 },
    },
    bindings: {
      secret_ids: ["service-token"],
      source_ids: ["example-data"],
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
      requirements: [
        { id: "graph", version: 1 },
        { id: "workspace", version: 1 },
      ],
      status: "requirements_declared",
    },
  });
  assert.deepEqual(spec, golden);
  assert.equal(
    renderDeploymentSpecJson(fixture, "production", productionLocator),
    `${canonical(spec)}\n`,
  );
  assert.doesNotMatch(JSON.stringify(spec), /example\.invalid\/graphforge\/example\.parquet/);
  assert.doesNotMatch(JSON.stringify(spec), /\.graphforge\/state/);
});

test("canonical JSON orders punctuation and case by UTF-16 code units", () => {
  const mixed: JsonValue = {
    a: 1,
    A: 2,
    _: 3,
    "-": 4,
    é: 5,
    Z: 6,
    aa: 7,
    a_: 8,
    $: 9,
  };
  const expected = '{"$":9,"-":4,"A":2,"Z":6,"_":3,"a":1,"a_":8,"aa":7,"é":5}';
  assert.equal(canonicalJson(mixed), expected);
  assert.equal(canonical(mixed), expected);
});

test("canonical JSON uses Terraform-compatible escaping for permitted text", () => {
  const escaped = cloneFixture();
  const artifact = target(escaped, "production").artifact as Record<string, JsonValue>;
  artifact.version = "v<>&\u2028\u2029";
  const encoded = renderDeploymentSpecJson(escaped, "production", productionLocator);

  assert.match(encoded, /"version":"v\\u003c\\u003e\\u0026\\u2028\\u2029"/);
  assert.equal(encoded.includes("<"), false);
  assert.equal(encoded.includes(">"), false);
  assert.equal(encoded.includes("&"), false);
  assert.equal(encoded.includes("\u2028"), false);
  assert.equal(encoded.includes("\u2029"), false);
  assert.equal(encoded, `${canonical(JSON.parse(encoded) as JsonValue)}\n`);
});

test("renderer supports every configured artifact kind without choosing a provider", () => {
  const cases: [string, string, string][] = [
    ["local", "python_wheel", "https://artifacts.example.invalid/graphforge-0.5.0.whl"],
    ["local-service", "native_binary", "https://artifacts.example.invalid/graphforge-0.5.0"],
    ["production", "oci_image", productionLocator],
  ];
  for (const [targetId, kind, locator] of cases) {
    const spec = renderDeploymentSpec(fixture, targetId, locator);
    assert.equal(spec.artifact.kind, kind);
    assert.equal(spec.artifact.locator, locator);
    assert.equal(spec.infrastructure.status, "caller_owned");
  }

  const nodeConfig = cloneFixture();
  const artifact = target(nodeConfig, "local").artifact as Record<string, JsonValue>;
  artifact.kind = "node_package";
  const node = renderDeploymentSpec(
    nodeConfig,
    "local",
    "https://artifacts.example.invalid/graphforge-node-0.5.0.tgz",
  );
  assert.equal(node.artifact.kind, "node_package");
});

test("renderer preserves every configured role without inventing runtime topology", () => {
  const locators: Record<string, string> = {
    "external-host": "https://artifacts.example.invalid/graphforge-host-0.5.1",
    "external-job": `registry.example.invalid/graphforge/job@sha256:${"f".repeat(64)}`,
    "external-worker": `registry.example.invalid/graphforge/worker@sha256:${"e".repeat(64)}`,
    local: "https://artifacts.example.invalid/graphforge-0.5.0.whl",
    "local-service": "https://artifacts.example.invalid/graphforge-0.5.0",
    production: productionLocator,
  };
  for (const [targetId, locator] of Object.entries(locators)) {
    const spec = renderDeploymentSpec(fixture, targetId, locator);
    const configured = target(fixture, targetId);
    const topology = configured.topology as Record<string, JsonValue>;
    assert.deepEqual(spec.topology, {
      execution: topology.execution,
      kind: configured.kind,
      ownership: configured.ownership,
      replicas: topology.replicas,
      scheduling: topology.scheduling,
    });
  }
});

test("artifact locators fail closed for mutable, mismatched, credential-bearing, and local input", () => {
  const invalid = [
    "registry.example.invalid/graphforge/authority:latest",
    `registry.example.invalid/graphforge/authority@sha256:${"d".repeat(64)}`,
    `https://registry.example.invalid/graphforge/authority@sha256:${"c".repeat(64)}`,
    `/tmp/authority@sha256:${"c".repeat(64)}`,
    `registry.example.invalid/GraphForge/authority@sha256:${"c".repeat(64)}`,
  ];
  for (const locator of invalid) {
    assert.throws(
      () => renderDeploymentSpec(fixture, "production", locator),
      /invalid graphforge-deployment-spec\/1/,
    );
  }

  for (const locator of [
    "http://artifacts.example.invalid/graphforge.whl",
    "https://user:password@artifacts.example.invalid/graphforge.whl",
    "https://artifacts.example.invalid/graphforge.whl?token=secret",
    "https://artifacts.example.invalid/graphforge.whl#sha256=abc",
    "file:///tmp/graphforge.whl",
    "../graphforge.whl",
    "C:\\graphforge.whl",
    "https://artifacts.example.invalid/graph forge.whl",
    `https://artifacts.example.invalid/${"a".repeat(2049)}`,
  ]) {
    assert.throws(
      () => renderDeploymentSpec(fixture, "local", locator),
      /invalid graphforge-deployment-spec\/1/,
    );
  }
  assert.equal(
    renderDeploymentSpec(fixture, "local", "https://127.0.0.1/graphforge.whl").artifact.locator,
    "https://127.0.0.1/graphforge.whl",
  );
});

test("configuration, artifact version, and digest drift produce a new deterministic spec", () => {
  const original = renderDeploymentSpecJson(fixture, "production", productionLocator);
  const changed = cloneFixture();
  const artifact = target(changed, "production").artifact as Record<string, JsonValue>;
  artifact.version = "0.5.2";
  artifact.sha256 = "d".repeat(64);
  const changedLocator = "registry.example.invalid/graphforge/authority@sha256:" + "d".repeat(64);
  const first = renderDeploymentSpecJson(changed, "production", changedLocator);
  const second = renderDeploymentSpecJson(changed, "production", changedLocator);

  assert.equal(second, first);
  assert.notEqual(first, original);
  const parsed = JSON.parse(first) as DeploymentSpecDocument;
  assert.equal(parsed.artifact.version, "0.5.2");
  assert.equal(parsed.artifact.sha256, "d".repeat(64));
  assert.notEqual(
    parsed.resolved_config_sha256,
    "eb9bd7c49ae277c62892b028d2c93e8328c24dd3e3fd5ecbabb4f2c94637e9e7",
  );
});

test("renderer rejects unknown config, target, artifact, and component input", () => {
  const unknown = cloneFixture();
  unknown.provider = "kubernetes";
  assert.throws(
    () => renderDeploymentSpec(unknown, "production", productionLocator),
    /unknown field provider/,
  );
  assert.throws(
    () => renderDeploymentSpec(fixture, "missing", productionLocator),
    /expected exactly one target/,
  );
  const invalidArtifact = cloneFixture();
  const artifact = target(invalidArtifact, "production").artifact as Record<string, JsonValue>;
  artifact.kind = "server_build";
  assert.throws(
    () => renderDeploymentSpec(invalidArtifact, "production", productionLocator),
    /target\.artifact\.kind has an unsupported value/,
  );
  const inlineSecret = cloneFixture();
  const secrets = inlineSecret.secrets as Record<string, JsonValue>[];
  secrets[0].value = "SECRET_VALUE_SENTINEL";
  assert.throws(
    () => renderDeploymentSpec(inlineSecret, "production", productionLocator),
    /secrets\[0\] contains unknown field value/,
  );
  assert.throws(
    () =>
      new DeploymentSpec("unknown-input", {
        resolvedConfig: fixture,
        targetId: "production",
        artifactLocator: productionLocator,
        provider: "kubernetes",
      } as never),
    /unknown input provider/,
  );
});

test("component preview registers only a state-safe projection and no provider children", async () => {
  const registrations: { type: string; inputs: Record<string, unknown> }[] = [];
  pulumi.runtime.setMocks(
    {
      newResource(args: pulumi.runtime.MockResourceArgs) {
        registrations.push({ type: args.type, inputs: args.inputs });
        return { id: `${args.name}-id`, state: args.inputs };
      },
      call(args: pulumi.runtime.MockCallArgs) {
        throw new Error(`unexpected provider call ${args.token}`);
      },
    },
    "graphforge",
    "deployment-spec",
    true,
  );

  const component = new DeploymentSpec("production", {
    resolvedConfig: fixture,
    targetId: "production",
    artifactLocator: productionLocator,
  });
  const [spec, encoded] = await new Promise<[DeploymentSpecDocument, string]>((done) => {
    pulumi.all([component.spec, component.canonicalJson, component.urn]).apply(([value, json]) => {
      done([value as DeploymentSpecDocument, json as string]);
      return value;
    });
  });

  assert.deepEqual(registrations, [{ type: "graphforge:deployment:DeploymentSpec", inputs: {} }]);
  assert.equal(spec.contract, "graphforge-deployment-spec/1");
  assert.equal(encoded, `${canonical(spec)}\n`);
  assert.equal(await pulumi.isSecret(component.spec), false);
  const state = JSON.stringify({ registrations, spec });
  assert.doesNotMatch(state, /example\.invalid\/graphforge\/example\.parquet/);
  assert.doesNotMatch(state, /\.graphforge\/state/);
  assert.doesNotMatch(state, /SECRET_VALUE_SENTINEL/);
});
