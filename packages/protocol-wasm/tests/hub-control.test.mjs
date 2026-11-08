import assert from "node:assert/strict";
import { test } from "node:test";
import { attempt, protocol, readJson } from "./support.mjs";
const corpus = readJson(
  "docs",
  "contracts",
  "hub-control",
  "v1",
  "conformance.json",
);

test("actual WASM preserves native control conformance and review commitment", () => {
  const plan = protocol.prepare_hub_management_plan(corpus.plan_input);
  assert.deepEqual(plan, {
    ...corpus.plan,
    expires_at_ms: BigInt(corpus.plan.expires_at_ms),
  });
  // Object and byte-for-byte text inputs use the same Rust constructor.
  assert.deepEqual(
    protocol.prepare_hub_management_plan(JSON.stringify(corpus.plan_input)),
    plan,
  );
  for (const entry of corpus.cases) {
    const result = attempt(() =>
      entry.response === undefined
        ? protocol.validate_hub_control_request(entry.request)
        : protocol.validate_hub_control_response(entry.request, entry.response),
    );
    assert.equal(result.ok, entry.accept, entry.name);
    if (!result.ok) {
      assert.equal(typeof result.error.code, "string", entry.name);
      assert.ok(!JSON.stringify(result.error).includes("publisher/project"));
    }
  }
});

test("actual WASM refuses changed authority, review fields, duplicate JSON and bounds", () => {
  const { request } = corpus.cases.find(({ name }) => name === "apply-applied");
  for (const change of [
    (p) => (p.mutation.name = "other"),
    (p) => (p.precondition.authority_revision = "revoked"),
    (p) => (p.precondition.object_revision = "new"),
    (p) => (p.expires_at_ms += 1),
    (p) => (p.hub_origin = "https://other.example"),
  ]) {
    const changed = JSON.parse(request);
    change(changed.operation.plan);
    assert.equal(
      attempt(() => protocol.validate_hub_control_request(changed)).ok,
      false,
    );
  }
  assert.equal(
    attempt(() => protocol.validate_hub_control_request(" ".repeat(65537))).ok,
    false,
  );
  assert.equal(
    attempt(() =>
      protocol.validate_hub_control_response(
        corpus.cases[0].request,
        " ".repeat(1048577),
      ),
    ).ok,
    false,
  );
  const duplicate = corpus.cases[0].request.replace(
    '"format":',
    '"format":"secret","format":',
  );
  const result = attempt(() =>
    protocol.validate_hub_control_request(duplicate),
  );
  assert.equal(result.ok, false);
  assert.ok(!JSON.stringify(result.error).includes("secret"));
});
