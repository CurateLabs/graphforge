import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import test from "node:test";

import * as pulumi from "@pulumi/pulumi";

import { InfraValidationReceipt, JsonValue, TargetValidation, validateTarget } from "../src/index";

const fixturePath = resolve(
  __dirname,
  "../../../../../docs/contracts/examples/graphforge-resolved-v1.json",
);
const fixture = JSON.parse(readFileSync(fixturePath, "utf8")) as {
  [key: string]: JsonValue;
};

test("pure validator emits the frozen receipt deterministically", () => {
  const first = validateTarget(fixture, "production");
  const second = validateTarget(
    JSON.parse(JSON.stringify(fixture)) as { [key: string]: JsonValue },
    "production",
  );
  assert.deepEqual(second, first);
  assert.equal(first.contract, "graphforge-infra-validation/1");
  assert.equal(
    first.resolved_config_sha256,
    "eb9bd7c49ae277c62892b028d2c93e8328c24dd3e3fd5ecbabb4f2c94637e9e7",
  );
  assert.deepEqual(first.static_validity, { status: "valid" });
  assert.deepEqual(first.connectivity, { status: "not_checked" });
  assert.deepEqual(first.readiness, { status: "not_checked" });
  assert.equal(first.planned_infrastructure.mutation, "none");
  assert.equal(first.planned_infrastructure.kind, "service");
  assert.equal(first.planned_infrastructure.execution, "container");
  assert.equal(first.planned_infrastructure.replicas, 2);
  assert.deepEqual(first.capability_compatibility.requirements, [
    { id: "graph", version: 1 },
    { id: "workspace", version: 1 },
  ]);
});

test("pure validator covers every ownership and target topology", () => {
  const expected = [
    ["external-host", "external", "host", "host", "long_running", 2],
    ["external-job", "external", "job", "container", "on_demand", 1],
    ["external-worker", "external", "worker", "container", "long_running", 3],
    ["local", "embedded", "embedded", "process", "long_running", 1],
    ["local-service", "local", "service", "process", "long_running", 1],
    ["production", "external", "service", "container", "long_running", 2],
  ] as const;
  for (const [id, ownership, kind, execution, scheduling, replicas] of expected) {
    const plan = validateTarget(fixture, id).planned_infrastructure;
    assert.deepEqual(
      [plan.ownership, plan.kind, plan.execution, plan.scheduling, plan.replicas],
      [ownership, kind, execution, scheduling, replicas],
    );
  }
});

test("pure validator rejects unknown fields that could carry secret values", () => {
  const poisoned = JSON.parse(JSON.stringify(fixture)) as {
    [key: string]: JsonValue;
  };
  const targets = poisoned.targets as { [key: string]: JsonValue }[];
  targets[1].credential = ["GRAPHFORGE_SECRET", "SENTINEL"].join("_");
  assert.throws(() => validateTarget(poisoned, "production"), /unknown field credential/);
});

test("pure validator rejects inline source credentials", () => {
  const poisoned = JSON.parse(JSON.stringify(fixture)) as {
    [key: string]: JsonValue;
  };
  const sources = poisoned.sources as { [key: string]: JsonValue }[];
  sources[0].uri = "https://user:password@example.invalid/data.parquet";
  assert.throws(() => validateTarget(poisoned, "production"), /sources\[0\]\.uri is invalid/);
});

test("pure validator rejects integers above the portable JSON limit", () => {
  const poisoned = JSON.parse(JSON.stringify(fixture)) as {
    [key: string]: JsonValue;
  };
  const targets = poisoned.targets as { [key: string]: JsonValue }[];
  const production = targets.find(({ id }) => id === "production");
  assert.ok(production);
  const resources = production.resources as { [key: string]: JsonValue };
  resources.memory_bytes = 9_007_199_254_740_992;
  assert.throws(
    () => validateTarget(poisoned, "production"),
    /target\.resources\.memory_bytes must be an integer/,
  );
});

test("component registers no provider resources or component inputs", async () => {
  const registrations: {
    type: string;
    inputs: Record<string, unknown>;
  }[] = [];
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
    "static-validation",
    false,
  );

  const component = new TargetValidation("production", {
    resolvedConfig: fixture,
    targetId: "production",
  });
  const receipt = await new Promise<InfraValidationReceipt>((done) => {
    pulumi.all([component.receipt, component.urn]).apply(([value]) => {
      done(value as InfraValidationReceipt);
      return value;
    });
  });

  assert.equal(receipt.connectivity.status, "not_checked");
  assert.equal(receipt.readiness.status, "not_checked");
  assert.equal(await pulumi.isSecret(component.receipt), false);
  assert.deepEqual(registrations, [{ type: "graphforge:static:TargetValidation", inputs: {} }]);
  assert.doesNotMatch(JSON.stringify(registrations), /service-token|SECRET_SENTINEL/);
});
