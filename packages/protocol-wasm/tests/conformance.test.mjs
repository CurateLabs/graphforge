// Replays the checked-in discovery v1 and hub-publish v1 corpora through the
// WebAssembly build. Each case is compared with the outcome the corpus records
// from native Rust; the native contract tests assert the same corpora against
// the Rust validators, so equal results here are equal results to native Rust.
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { test } from "node:test";
import { attempt, protocol, readJson } from "./support.mjs";

const discovery = readJson(
  "docs",
  "reference",
  "discovery",
  "v1",
  "conformance.json",
);
const moved = readJson(
  "docs",
  "reference",
  "discovery",
  "v1",
  "moved-conformance.json",
);
const previews = readJson(
  "docs",
  "reference",
  "hub-publish",
  "v1",
  "previews.json",
);
const hubPublish = readJson(
  "docs",
  "reference",
  "hub-publish",
  "v1",
  "conformance.json",
);

const VALIDATORS = {
  manifest: protocol.validate_manifest,
  refs: protocol.validate_refs,
  summary: protocol.validate_summary,
  lineage: protocol.validate_lineage,
};

/** The corpus spells a failure `{result, code, field, version?}`; the module throws `{code, path?, version?, message}`. */
function recordedFailure(expected) {
  const failure = { code: expected.code, path: expected.field ?? undefined };
  if (expected.version !== undefined) failure.version = expected.version;
  return failure;
}

function thrownFailure(error) {
  const { code, path, version } = error;
  return version === undefined ? { code, path } : { code, path, version };
}

test("discovery v1 corpus matches native Rust for every case", () => {
  assert.equal(discovery.cases.length, 99);
  for (const { name, document, json, limits, expected } of discovery.cases) {
    const outcome = attempt(() => VALIDATORS[document](json, limits));
    if (expected.result === "valid") {
      assert.ok(
        outcome.ok,
        `${name}: ${outcome.ok ? "" : JSON.stringify(outcome.error)}`,
      );
      assert.equal(outcome.value.canonical_json, expected.canonical_json, name);
      // The same document as a plain object validates to the same canonical form.
      const viaObject = VALIDATORS[document](JSON.parse(json), limits);
      assert.equal(
        viaObject.canonical_json,
        expected.canonical_json,
        `${name} (object input)`,
      );
    } else {
      assert.ok(!outcome.ok, `${name}: expected ${expected.code}`);
      assert.deepEqual(
        thrownFailure(outcome.error),
        recordedFailure(expected),
        name,
      );
    }
  }
});

test("repository move corpus matches native Rust for every case", () => {
  for (const { name, document, valid } of moved.cases) {
    const outcome = attempt(() => protocol.validate_repository_moved(document));
    assert.equal(outcome.ok, valid, name);
  }
});

const hex = (text) =>
  Uint8Array.from(text.match(/../g), (byte) => Number.parseInt(byte, 16));

test("preview corpus matches native Rust for every case", () => {
  for (const {
    name,
    media_type: mediaType,
    hex: encoded,
    utf8,
    error,
  } of previews) {
    const bytes =
      encoded === undefined ? new TextEncoder().encode(utf8) : hex(encoded);
    const outcome = attempt(() => protocol.validate_image(mediaType, bytes));
    if (error === null) {
      assert.ok(outcome.ok, `${name}: ${outcome.ok ? "" : outcome.error.code}`);
    } else {
      assert.ok(!outcome.ok, name);
      assert.equal(outcome.error.code, `preview.${error}`, name);
    }
  }
});

// ---------------------------------------------------------------- hub publish
//
// The hub-publish corpus drives a stateful reference Hub over HTTP, which does
// not exist in the WebAssembly build. What does exist is every control document
// the exchange carries. For each one that a step can show to be valid or
// invalid on its own, the module must agree with the recorded response:
//   - a document sent in a request that the Hub accepted must validate, and
//     the commitment recomputed from the open and commit requests must equal
//     the one the client declared;
//   - a document the module rejects must have been refused by the Hub with the
//     same code;
//   - documents the Hub returned (capabilities, receipts, operation statuses,
//     refs and their ETag revision) must validate.

const OWNER_REPOSITORY = /^https:\/\/hub\.example\/([^/]+)\/([^/]+)\/\.gf\//;
const SUCCESS = (status) => status >= 200 && status < 300;
const hasVariable = (value) => JSON.stringify(value).includes("${");

const baseCapabilities = (() => {
  for (const { steps } of hubPublish.cases) {
    for (const { request, expect } of steps) {
      if (request?.url.endsWith("/.gf/publish") && expect.json)
        return expect.json;
    }
  }
  throw new Error("the corpus carries no capabilities response");
})();

function capabilitiesFor(owner, repository) {
  return {
    ...baseCapabilities,
    repository: { owner, repository },
    authorization: {
      ...baseCapabilities.authorization,
      scope: `publish:${owner}/${repository}`,
    },
    object_location_template: `https://hub.example/${owner}/${repository}/.gf/objects/{digest}`,
  };
}

const tally = { accepted: 0, refused: 0, returned: 0, commitments: 0 };

/** The module's verdict on a request document, compared with the recorded response. */
function checkRequestDocument(label, validate, document, expect) {
  const outcome = attempt(validate);
  if (SUCCESS(expect.status)) {
    assert.ok(
      outcome.ok,
      `${label}: ${outcome.ok ? "" : JSON.stringify(outcome.error)}`,
    );
    tally.accepted += 1;
  } else if (!outcome.ok) {
    assert.equal(
      outcome.error.code,
      expect.error,
      `${label}: ${JSON.stringify(outcome.error)}`,
    );
    tally.refused += 1;
  }
  return outcome;
}

test("hub-publish v1 corpus documents match native Rust for every case", () => {
  assert.equal(hubPublish.cases.length, 30);
  for (const { name, steps } of hubPublish.cases) {
    const sessions = new Map(); // captured session variable -> open request document
    for (const [index, { request, expect }] of steps.entries()) {
      if (!request) continue;
      const label = `${name}#${index}`;
      const location = OWNER_REPOSITORY.exec(request.url);
      const body = request.json;
      const target = request.url.replace(
        /^https:\/\/hub\.example\/[^/]+\/[^/]+/,
        "",
      );

      if (
        request.method === "GET" &&
        target === "/.gf/publish" &&
        expect.json
      ) {
        protocol.validate_capabilities(expect.json);
        tally.returned += 1;
      } else if (
        request.method === "GET" &&
        target === "/.gf/refs" &&
        expect.body_text
      ) {
        const { revision } = protocol.validate_refs(expect.body_text);
        assert.equal(
          `"${revision}"`,
          expect.headers.etag,
          `${label}: ETag is the refs revision`,
        );
        tally.returned += 1;
      } else if (
        request.method === "GET" &&
        target.startsWith("/.gf/publish/operations/") &&
        expect.json
      ) {
        protocol.validate_operation_status(expect.json);
        tally.returned += 1;
      } else if (
        request.method === "POST" &&
        target === "/.gf/publish/sessions" &&
        body &&
        !hasVariable(body)
      ) {
        const outcome = checkRequestDocument(
          label,
          () => protocol.validate_session_request(body),
          body,
          expect,
        );
        if (outcome.ok && SUCCESS(expect.status)) {
          for (const variable of Object.keys(expect.capture ?? {})) {
            if (expect.capture[variable] === "/session_id")
              sessions.set(variable, body);
          }
        }
      } else if (
        request.method === "POST" &&
        target.endsWith("/commit") &&
        body &&
        !hasVariable(body)
      ) {
        const [, owner, repository] = location;
        const identity = { owner, repository };
        const variable = /\/sessions\/\$\{([^}]+)\}\/commit$/.exec(target)?.[1];
        const open = sessions.get(variable);
        const outcome = checkRequestDocument(
          label,
          () => protocol.validate_commit_request(body),
          body,
          expect,
        );
        if (outcome.ok && open) {
          const manifest = protocol.validate_manifest(body.manifest);
          const check = attempt(() =>
            protocol.check_commit_manifest({
              manifest: manifest.document,
              repository: identity,
              capabilities: capabilitiesFor(owner, repository),
              inventory: open.objects,
              refs: body.refs,
            }),
          );
          if (SUCCESS(expect.status)) {
            assert.ok(
              check.ok,
              `${label}: ${check.ok ? "" : JSON.stringify(check.error)}`,
            );
            const commitment = protocol.request_commitment({
              rewrite: body.rewrite ?? false,
              prune: body.prune ?? false,
              repository: identity,
              intent_digest: open.intent_digest,
              objects: open.objects,
              manifest_validator: manifest.digest,
              refs: body.refs,
              expected_revision: body.expected_revision,
            });
            assert.equal(
              commitment,
              open.request_commitment,
              `${label}: request commitment`,
            );
            tally.commitments += 1;
          } else if (!check.ok) {
            assert.equal(
              check.error.code,
              expect.error,
              `${label}: ${JSON.stringify(check.error)}`,
            );
          }
        }
        if (SUCCESS(expect.status) && expect.json?.receipt) {
          protocol.validate_receipt(expect.json.receipt);
          tally.returned += 1;
        }
      }
    }
  }
  console.log("hub-publish documents:", JSON.stringify(tally));
  // The replay must exercise every kind of document, not pass vacuously.
  assert.ok(tally.accepted > 60, JSON.stringify(tally));
  assert.ok(tally.commitments > 20, JSON.stringify(tally));
  assert.ok(tally.returned > 20, JSON.stringify(tally));
  assert.ok(tally.refused > 0, JSON.stringify(tally));
});

test("a published research lineage binds its manifest and resolves each Version package", () => {
  let bound = 0;
  for (const { name, steps } of hubPublish.cases) {
    const uploads = new Map(); // object digest -> uploaded text
    for (const [index, { request, expect }] of steps.entries()) {
      if (request?.body_text !== undefined) {
        uploads.set(
          `sha256:${createHash("sha256").update(request.body_text).digest("hex")}`,
          request.body_text,
        );
      }
      const manifest = request?.json?.manifest;
      if (
        !manifest?.lineage ||
        !SUCCESS(expect.status) ||
        !request.url.endsWith("/commit")
      )
        continue;
      const lineageText = uploads.get(manifest.lineage.object_digest);
      const refs = steps
        .slice(index + 1)
        .find(
          (step) =>
            step.expect?.body_text && step.request?.url.endsWith("/.gf/refs"),
        );
      if (lineageText === undefined || refs === undefined) continue;
      const lineage = protocol.validate_lineage(lineageText);
      assert.equal(lineage.digest, manifest.lineage.lineage_digest, name);
      protocol.bind_lineage(manifest, refs.expect.body_text, lineageText);
      for (const version of lineage.document.versions) {
        const selected = protocol.research_version_object(
          manifest,
          lineageText,
          version.version_uuid,
        );
        assert.equal(selected.version.version_uuid, version.version_uuid, name);
      }
      bound += 1;
    }
  }
  assert.ok(bound > 0, "the corpus publishes at least one research lineage");
});
