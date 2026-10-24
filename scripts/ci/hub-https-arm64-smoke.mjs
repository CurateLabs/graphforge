import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

assert.equal(process.platform, "linux");
assert.equal(process.arch, "arm64");
const core = createRequire(import.meta.url)(resolve(process.argv[2]));
const parent = mkdtempSync(join(tmpdir(), "graphforge-hub-https-"));
const destination = join(parent, "project");
try {
  // A fresh, absent public identity tests verified HTTPS without depending on
  // the migrating fixture namespace or downloading a project's graph data.
  const repository = `transport-smoke-${randomUUID()}/absent`;
  const result = core.runCli(["clone", repository, destination, "--json"]);
  const stderr = result.stderr.toString("utf8");
  assert.equal(result.exitCode, 2, stderr);
  const error = JSON.parse(stderr.trim().split(/\r?\n/).at(-1)).error;
  assert.equal(error.details.semantic_code, "hub.not_found", stderr);
  assert.equal(existsSync(destination), false);
  console.log(`Core ${core.version()}: verified Hub HTTPS and HTTP 404 on linux/arm64`);
} finally {
  rmSync(parent, { recursive: true, force: true });
}
