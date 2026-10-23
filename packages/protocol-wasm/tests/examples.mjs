// Valid arguments for every export, built from the checked-in fixtures and
// corpora, so a test that damages one argument knows the others are good.
import { createHash } from "node:crypto";
import { protocol, readJson, readText } from "./support.mjs";

const fixtureDir = ["tests", "fixtures", "hub", "generated", "v1"];
const fixture = readJson(...fixtureDir, "fixture.json");
const hub = readJson(
  "docs",
  "reference",
  "hub-publish",
  "v1",
  "conformance.json",
);
const verdicts = readJson(
  "crates",
  "graphforge-protocol-wasm",
  "tests",
  "hub-publish-documents.json",
);
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

const at = (value, pointer) =>
  pointer
    .split("/")
    .slice(1)
    .reduce((node, key) => node[key], value);

const entryStep = (entry) =>
  hub.cases.find(({ name }) => name === entry.case).steps[entry.step];

/** The first corpus document of `kind` that native Rust accepts. */
function accepted(kind) {
  const entry = verdicts.documents.find(
    (e) => e.kind === kind && e.outcome === "accept" && !e.mutation,
  );
  return at(entryStep(entry), entry.pointer);
}

/** `check_commit_manifest` input from the first accepted corpus commit. */
function commitManifestInput() {
  const entry = verdicts.documents.find(
    (e) => e.kind === "commit_manifest" && e.outcome === "accept",
  );
  const { request } = entryStep(entry);
  const [, owner, repository] =
    /^https:\/\/hub\.example\/([^/]+)\/([^/]+)\//.exec(request.url);
  const capabilities = hub.cases
    .flatMap(({ steps }) => steps)
    .find(
      (step) => step.request?.url.endsWith("/.gf/publish") && step.expect.json,
    ).expect.json;
  const open = hub.cases.find(({ name }) => name === entry.case).steps[
    entry.open_step
  ];
  return {
    manifest: protocol.validate_manifest(request.json.manifest).document,
    repository: { owner, repository },
    capabilities: {
      ...capabilities,
      repository: { owner, repository },
      authorization: {
        ...capabilities.authorization,
        scope: `publish:${owner}/${repository}`,
      },
      object_location_template: `https://hub.example/${owner}/${repository}/.gf/objects/{digest}`,
    },
    inventory: open.request.json.objects,
    refs: request.json.refs,
  };
}

/** A published research lineage with its manifest and refs. */
function publishedLineage() {
  for (const { steps } of hub.cases) {
    const uploads = new Map();
    for (const [index, { request, expect }] of steps.entries()) {
      if (request?.body_text !== undefined) {
        uploads.set(
          `sha256:${createHash("sha256").update(request.body_text).digest("hex")}`,
          request.body_text,
        );
      }
      const manifest = request?.json?.manifest;
      if (!manifest?.lineage || !(expect.status >= 200 && expect.status < 300))
        continue;
      const lineage = uploads.get(manifest.lineage.object_digest);
      const refs = steps
        .slice(index + 1)
        .find(
          (s) => s.expect?.body_text && s.request?.url.endsWith("/.gf/refs"),
        );
      if (lineage === undefined || refs === undefined) continue;
      return {
        manifest,
        refs: refs.expect.body_text,
        lineage,
        versionUuid:
          protocol.validate_lineage(lineage).document.versions[0].version_uuid,
      };
    }
  }
  throw new Error("the corpus publishes no research lineage");
}

const svg = new TextEncoder().encode(
  '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16"><path d="M0 0L1 1"/></svg>',
);
const descriptor = {
  version_uuid: "01990000-0000-7000-8000-000000000001",
  digest: `sha256:${createHash("sha256").update(svg).digest("hex")}`,
  media_type: "image/svg+xml",
  length: svg.length,
  width: 16,
  height: 16,
};

const manifest = readText(...fixtureDir, "manifest.json");
const refs = readText(...fixtureDir, "refs.json");
const summary = readText(...fixtureDir, fixture.summary_path);
const module = fixture.module_objects[0];
const lineage = publishedLineage();

const validText = (document) =>
  discovery.cases.find(
    (c) => c.document === document && c.expected.result === "valid",
  ).json;
const manifestObject = JSON.parse(manifest);

/**
 * Every export with a working argument list. `optional` lists the argument
 * positions that accept `undefined`/`null`.
 */
const admission = readJson(
  "docs",
  "reference",
  "hub-publish",
  "v1",
  "item-admission-conformance.json",
).cases[0].input;
const itemDescriptor = {
  format: "graphforge-item/1",
  id: protocol.parse_item_id(module.ontology_id).id,
  kind: "ontology_module",
  repository: manifestObject.repository,
  module: {
    id: module.ontology_id,
    version: module.version,
    content_digest: module.content_digest,
  },
  discovery: {
    refs_revision: protocol.validate_refs(refs).revision,
    manifest_validator: protocol.validate_manifest(manifest).digest,
  },
};
const itemInventory = {
  id: itemDescriptor.id,
  version: itemDescriptor.module.version,
  complete: true,
  candidates: [itemDescriptor],
};

export const EXPORTS = {
  item_admission_requirements: { args: [admission.manifest], optional: [] },
  plan_item_admission: { args: [admission], optional: [] },
  resolve_item_version: { args: [itemInventory, undefined], optional: [1] },
  bind_item_version_source: {
    args: [itemInventory, itemDescriptor, refs, manifest, undefined],
    optional: [4],
  },
  item_resolution_error_response: {
    args: [
      {
        code: "ambiguous_identity",
        path: "item_version_inventory.candidates.content_digest",
        message: "item version identity is ambiguous",
      },
    ],
    optional: [],
    totalProjection: true,
  },
  parse_item_id: { args: ["ont_7k2m9x4q8r3t5v1w"], optional: [] },
  has_item_id_shape: {
    args: ["abc_0000000000000000"],
    optional: [],
    predicate: true,
  },
  validate_item_descriptor: {
    args: [
      {
        format: "graphforge-item/1",
        id: protocol.parse_item_id(module.ontology_id).id,
        kind: "ontology_module",
        repository: manifestObject.repository,
        module: {
          id: module.ontology_id,
          version: module.version,
          content_digest: module.content_digest,
        },
        discovery: {
          refs_revision: protocol.validate_refs(refs).revision,
          manifest_validator: protocol.validate_manifest(manifest).digest,
        },
      },
      undefined,
    ],
    optional: [1],
  },
  validate_manifest: { args: [manifest, undefined], optional: [1] },
  validate_refs: { args: [refs, undefined], optional: [1] },
  validate_summary: { args: [summary, undefined], optional: [1] },
  validate_lineage: { args: [validText("lineage"), undefined], optional: [1] },
  validate_repository_moved: {
    args: [moved.cases[0].document, undefined],
    optional: [1],
  },
  bind_summary: { args: [manifest, summary, undefined], optional: [2] },
  bind_lineage: {
    args: [lineage.manifest, lineage.refs, lineage.lineage, undefined],
    optional: [3],
  },
  bind_summary_lineage: { args: [summary, null, undefined], optional: [1, 2] },
  bind_refs: { args: [refs, manifest, undefined], optional: [2] },
  ontology_module_object: {
    args: [
      manifest,
      {
        id: module.ontology_id,
        version: module.version,
        content_digest: module.content_digest,
      },
      undefined,
    ],
    optional: [2],
  },
  research_version_object: {
    args: [lineage.manifest, lineage.lineage, lineage.versionUuid, undefined],
    optional: [3],
  },
  rehome_repository: {
    args: [
      refs,
      [{ manifest: manifestObject, summary }],
      { owner: "opendata", repository: "renamed" },
      "https://control.example/opendata/renamed/sha256/{hex}",
      undefined,
    ],
    optional: [4],
  },
  validate_image: { args: ["image/svg+xml", svg], optional: [] },
  validate_preview: { args: [descriptor, svg], optional: [] },
  validate_preview_descriptors: { args: [[descriptor]], optional: [] },
  manifest_preview_descriptors: { args: [manifest, undefined], optional: [1] },
  validate_capabilities: { args: [accepted("capabilities")], optional: [] },
  validate_session_request: {
    args: [accepted("session_request")],
    optional: [],
  },
  validate_commit_request: { args: [accepted("commit_request")], optional: [] },
  validate_receipt: { args: [accepted("receipt")], optional: [] },
  validate_operation_status: {
    args: [accepted("operation_status")],
    optional: [],
  },
  request_commitment: {
    args: [
      {
        repository: manifestObject.repository,
        intent_digest: fixture.package_digest,
        objects: manifestObject.objects.map(
          ({ digest, length, media_type }) => ({
            digest,
            length,
            media_type,
          }),
        ),
        manifest_validator: fixture.manifest_digest,
        refs: ["main"],
        expected_revision: null,
      },
    ],
    optional: [],
  },
  check_commit_manifest: { args: [commitManifestInput()], optional: [] },
};
