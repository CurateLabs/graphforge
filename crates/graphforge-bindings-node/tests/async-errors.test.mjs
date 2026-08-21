// Structured native-addon Promise rejection acceptance (#2499).

import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { tmpdir } from "node:os";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { GraphForge } from "../index.js";

const here = dirname(fileURLToPath(import.meta.url));

const operation = (suffix) =>
  `018f0f4e-7b8c-7000-8000-${suffix.toString().padStart(12, "0")}`;

async function rejectsWithCode(promise, code) {
  await assert.rejects(promise, (error) => {
    assert.equal(error.code, code);
    assert.notEqual(error.name, "AbortError");
    return true;
  });
}

test("every native task uses structured cooperative error transport", () => {
  const srcDir = join(here, "../src");
  const source = readdirSync(srcDir)
    .filter((name) => name.endsWith(".rs"))
    .map((name) => readFileSync(join(srcDir, name), "utf8"))
    .join("\n");
  const errors = readFileSync(join(srcDir, "error.rs"), "utf8");
  const taskCount = source.match(/impl Task for /g)?.length ?? 0;
  assert.equal(taskCount, 50);
  assert.equal(
    source.match(/type Output =\s*(?:\n\s*)?std::result::Result</g)?.length,
    taskCount,
  );
  assert.equal(
    source.match(/to_(?:napi|portable|multi)_deferred_err\(env,/g)?.length,
    taskCount,
  );
  assert.match(
    source,
    /impl Task for ResolveBeliefSubjectTask \{[\s\S]*?type Output = std::result::Result<ResolvedBeliefSubject, GfError>;[\s\S]*?if self\.cancellation\.is_cancelled\(\)[\s\S]*?to_napi_deferred_err\(env,/,
  );
  assert.doesNotMatch(source, /with_optional_signal/);
  assert.doesNotMatch(`${source}\n${errors}`, /to_napi_status_err/);
});

test("PlanHandle async failures preserve parse and lifecycle codes", async () => {
  const parseForge = new GraphForge();
  await rejectsWithCode(parseForge.plan("MATCH (").collectIpc(), "ParseError");
  await rejectsWithCode(
    parseForge
      .plan("MATCH (")
      .sinkParquet(join(tmpdir(), "graphforge-invalid-plan.parquet")),
    "ParseError",
  );

  const closedForge = new GraphForge();
  const collect = closedForge.plan("MATCH (n) RETURN n");
  const sink = closedForge.plan("MATCH (n) RETURN n");
  closedForge.close();
  await rejectsWithCode(collect.collectIpc(), "LifecycleError");
  await rejectsWithCode(
    sink.sinkParquet(join(tmpdir(), "graphforge-closed-plan.parquet")),
    "LifecycleError",
  );
});

test("async validation and project errors retain semantic codes", async () => {
  const forge = new GraphForge();
  await forge.checkpoint({ name: "One", idempotencyKey: operation(1) });

  await rejectsWithCode(forge.listCheckpoints({ limit: 0 }), "ValidationError");
  await rejectsWithCode(
    forge.enableCapability({
      operationUuid: operation(2),
      capabilityId: "knowledge",
      capabilityVersion: 2,
    }),
    "GF_UNSUPPORTED_CAPABILITY_VERSION",
  );
});

test("AbortSignal never bypasses GraphForge with AbortError", async () => {
  const forge = new GraphForge();
  await forge.checkpoint({ name: "One", idempotencyKey: operation(10) });

  for (let attempt = 0; attempt < 16; attempt += 1) {
    const controller = new AbortController();
    const listing = forge.listCheckpoints({ signal: controller.signal });
    controller.abort();
    try {
      await listing;
    } catch (error) {
      assert.equal(error.code, "GF_CANCELLED");
      assert.notEqual(error.name, "AbortError");
    }
  }
});
