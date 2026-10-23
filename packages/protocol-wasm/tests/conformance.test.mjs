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
// the exchange carries. `crates/graphforge-protocol-wasm/tests/hub_documents.rs`
// runs each of them (and a mutated copy of one of each kind) through the native
// validators and records the verdict, `accept` or the error code, in
// `hub-publish-documents.json`. The module must reproduce every verdict, so it
// can neither accept what native Rust refuses nor refuse what it accepts.

const documents = readJson(
  "crates",
  "graphforge-protocol-wasm",
  "tests",
  "hub-publish-documents.json",
);

const DOCUMENT_VALIDATORS = {
  capabilities: protocol.validate_capabilities,
  session_request: protocol.validate_session_request,
  commit_request: protocol.validate_commit_request,
  operation_status: protocol.validate_operation_status,
  receipt: protocol.validate_receipt,
  refs: protocol.validate_refs,
};

const at = (value, pointer) =>
  pointer
    .split("/")
    .slice(1)
    .reduce((node, key) => node[key], value);

function mutated(document, { set = {}, remove = [] }) {
  const copy = structuredClone(document);
  Object.assign(copy, set);
  for (const name of remove) delete copy[name];
  return copy;
}

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

/** Run the export `entry.kind` names on the corpus document it points at. */
function replay(entry) {
  const { steps } = hubPublish.cases.find(({ name }) => name === entry.case);
  const step = steps[entry.step];
  const document = at(step, entry.pointer);
  if (entry.kind === "commit_manifest") {
    const [, owner, repository] =
      /^https:\/\/hub\.example\/([^/]+)\/([^/]+)\//.exec(step.request.url);
    const open = steps[entry.open_step].request.json;
    return attempt(() =>
      protocol.check_commit_manifest({
        manifest: protocol.validate_manifest(document.manifest).document,
        repository: { owner, repository },
        capabilities: capabilitiesFor(owner, repository),
        inventory: open.objects,
        refs: document.refs,
      }),
    );
  }
  const input = entry.mutation ? mutated(document, entry.mutation) : document;
  return attempt(() => DOCUMENT_VALIDATORS[entry.kind](input));
}

test("hub-publish v1 corpus documents have the recorded native verdict, accepted or refused", () => {
  assert.equal(hubPublish.cases.length, 31);
  assert.equal(documents.format, "graphforge-protocol-wasm-hub-documents/1");
  const seen = { accept: 0, refused: 0, mutated: 0 };
  const kinds = new Set();
  for (const entry of documents.documents) {
    const label = `${entry.case}#${entry.step} ${entry.kind}${
      entry.mutation ? ` ${JSON.stringify(entry.mutation)}` : ""
    }`;
    const outcome = replay(entry);
    if (entry.outcome === "accept") {
      assert.ok(
        outcome.ok,
        `${label}: native accepts, module refused ${outcome.ok ? "" : JSON.stringify(outcome.error)}`,
      );
      seen.accept += 1;
    } else {
      assert.ok(
        !outcome.ok,
        `${label}: native refuses with ${entry.outcome}, module accepted`,
      );
      assert.equal(outcome.error.code, entry.outcome, label);
      seen.refused += 1;
    }
    if (entry.mutation) seen.mutated += 1;
    kinds.add(entry.kind);
  }
  // Every kind of document is replayed, and refusals are among them.
  assert.equal(kinds.size, 7, [...kinds].join());
  assert.ok(
    seen.accept > 150 && seen.refused >= 10 && seen.mutated >= 10,
    JSON.stringify(seen),
  );
  console.log("hub-publish documents:", JSON.stringify(seen));
});

test("hub-publish v1 request commitments and refs revisions equal the declared ones", () => {
  let commitments = 0;
  for (const { name, steps } of hubPublish.cases) {
    const sessions = new Map(); // captured session variable -> open request document
    for (const [index, { request, expect }] of steps.entries()) {
      if (!request) continue;
      const target = request.url.replace(
        /^https:\/\/hub\.example\/[^/]+\/[^/]+/,
        "",
      );
      const body = request.json;
      if (
        request.method === "GET" &&
        target === "/.gf/refs" &&
        expect.body_text
      ) {
        const { revision } = protocol.validate_refs(expect.body_text);
        assert.equal(`"${revision}"`, expect.headers.etag, `${name}#${index}`);
      } else if (
        request.method === "POST" &&
        target === "/.gf/publish/sessions" &&
        body &&
        !JSON.stringify(body).includes("${") &&
        expect.status >= 200 &&
        expect.status < 300
      ) {
        for (const [variable, pointer] of Object.entries(
          expect.capture ?? {},
        )) {
          if (pointer === "/session_id") sessions.set(variable, body);
        }
      } else if (
        request.method === "POST" &&
        target.endsWith("/commit") &&
        body &&
        !JSON.stringify(body).includes("${") &&
        expect.status >= 200 &&
        expect.status < 300
      ) {
        const [, owner, repository] =
          /^https:\/\/hub\.example\/([^/]+)\/([^/]+)\//.exec(request.url);
        const variable = /\/sessions\/\$\{([^}]+)\}\/commit$/.exec(target)?.[1];
        const open = sessions.get(variable);
        if (!open) continue;
        const commitment = protocol.request_commitment({
          rewrite: body.rewrite ?? false,
          prune: body.prune ?? false,
          repository: { owner, repository },
          intent_digest: open.intent_digest,
          objects: open.objects,
          manifest_validator: protocol.validate_manifest(body.manifest).digest,
          refs: body.refs,
          expected_revision: body.expected_revision,
        });
        assert.equal(commitment, open.request_commitment, `${name}#${index}`);
        commitments += 1;
      }
    }
  }
  assert.ok(commitments > 20, `${commitments} commitments`);
});

const SUCCESS = (status) => status >= 200 && status < 300;

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
