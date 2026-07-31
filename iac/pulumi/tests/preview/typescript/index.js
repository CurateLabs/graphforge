"use strict";

const fs = require("node:fs");
const assert = require("node:assert/strict");
const path = require("node:path");

const pulumi = require("@pulumi/pulumi");

const root = process.env.GRAPHFORGE_REPO_ROOT;
if (!root) {
  throw new Error("GRAPHFORGE_REPO_ROOT is required");
}

const { DeploymentSpec, TargetValidation, renderDeploymentSpec, validateTarget } = require(
  path.join(root, "iac/pulumi/typescript/dist/src/index.js"),
);
const resolvedConfig = JSON.parse(
  fs.readFileSync(path.join(root, "docs/contracts/examples/graphforge-resolved-v1.json"), "utf8"),
);
const goldenReceipt = JSON.parse(
  fs.readFileSync(
    path.join(root, "docs/contracts/examples/graphforge-infra-validation-production-v1.json"),
    "utf8",
  ),
);
const goldenDeployment = JSON.parse(
  fs.readFileSync(
    path.join(root, "docs/contracts/examples/graphforge-deployment-spec-production-v1.json"),
    "utf8",
  ),
);
const artifactLocator = goldenDeployment.artifact.locator;

if (process.env.GRAPHFORGE_INJECT_FORBIDDEN === "1") {
  resolvedConfig.targets.find(({ id }) => id === "production").credential =
    process.env.GRAPHFORGE_TEST_SECRET;
}

assert.deepStrictEqual(validateTarget(resolvedConfig, "production"), goldenReceipt);
assert.deepStrictEqual(
  renderDeploymentSpec(resolvedConfig, "production", artifactLocator),
  goldenDeployment,
);
pulumi.log.info(`GraphForge golden receipt ${goldenReceipt.resolved_config_sha256} verified`);
pulumi.log.info(
  `GraphForge golden deployment spec ${goldenDeployment.resolved_config_sha256} verified`,
);

const validation = new TargetValidation("production", {
  resolvedConfig,
  targetId: "production",
});
const deployment = new DeploymentSpec("production", {
  resolvedConfig,
  targetId: "production",
  artifactLocator,
});

exports.receipt = validation.receipt;
exports.deploymentSpec = deployment.spec;
