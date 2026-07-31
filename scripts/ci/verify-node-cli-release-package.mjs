#!/usr/bin/env node
/**
 * Pack @graphforge/cli, install it with the already-published native package in
 * a clean consumer directory, then execute through npx without registry access.
 *
 * This runs after @graphforge/node publication. It deliberately does not use
 * the workspace-linked native addon, so a missing or unusable public dependency
 * blocks publishing the CLI.
 */

import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import {
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { isAbsolute, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(fileURLToPath(new URL("../..", import.meta.url)));
const packageRoot = join(root, "packages", "cli");
const metadata = JSON.parse(
  readFileSync(join(packageRoot, "package.json"), "utf8"),
);
const fixture = mkdtempSync(join(tmpdir(), "graphforge-cli-release-"));
const consumer = join(fixture, "consumer");
mkdirSync(consumer);

const run = (command, args, cwd) =>
  execFileSync(command, args, {
    cwd,
    encoding: "utf8",
    env: { ...process.env, npm_config_audit: "false", npm_config_fund: "false" },
  });

try {
  writeFileSync(
    join(consumer, "package.json"),
    `${JSON.stringify({ name: "graphforge-cli-release-probe", private: true })}\n`,
  );
  const packed = JSON.parse(
    run(
      "pnpm",
      ["pack", "--json", "--pack-destination", fixture],
      packageRoot,
    ),
  );
  const result = Array.isArray(packed) ? packed[0] : packed;
  const tarball = isAbsolute(result.filename)
    ? result.filename
    : join(fixture, result.filename);

  run(
    "npm",
    [
      "install",
      "--no-audit",
      "--no-fund",
      `@graphforge/node@${metadata.version}`,
      tarball,
    ],
    consumer,
  );
  const output = run(
    "npx",
    ["--offline", "--no-install", "graphforge", "--version"],
    consumer,
  ).trim();
  assert.match(output, new RegExp(metadata.version.replaceAll(".", "\\.")));
  process.stdout.write(`clean @graphforge/cli package ok: ${output}\n`);
} finally {
  rmSync(fixture, { recursive: true, force: true });
}
