import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { test } from "node:test";
import { attempt, dist, protocol, readJson } from "./support.mjs";

const corpus = readJson(
  "docs",
  "reference",
  "discovery",
  "v1",
  "item-resolution-conformance.json",
);
const fixturePath = ["tests", "fixtures", "hub", "generated", "v1"];
const unavailable = { status: 503, body: "" };
const project = (error) => protocol.item_resolution_error_response(error);
const refuse = (call) => {
  const outcome = attempt(call);
  assert.equal(outcome.ok, false);
  return outcome.error;
};
function fixture() {
  const refs = readJson(...fixturePath, "refs.json");
  const manifest = readJson(...fixturePath, "manifest.json");
  const { id, version, content_digest } = manifest.ontology.modules[0];
  const parsed = protocol.parse_item_id(id);
  const descriptor = {
    format: "graphforge-item/1",
    id: parsed.id,
    kind: parsed.kind,
    repository: manifest.repository,
    module: { id, version, content_digest },
    discovery: {
      refs_revision: protocol.validate_refs(refs).revision,
      manifest_validator: protocol.validate_manifest(manifest).digest,
    },
  };
  return {
    descriptor,
    refs,
    manifest,
    inventory: {
      id: parsed.id,
      version,
      complete: true,
      candidates: [descriptor],
    },
  };
}

test("actual WASM replays the native resolution and HTTP conformance corpus", () => {
  for (const { name, json, limits, expected } of corpus.cases) {
    const outcome = attempt(() => protocol.resolve_item_version(json, limits));
    if (expected.identity) {
      assert.equal(outcome.ok, true, name);
      assert.deepEqual(outcome.value, expected.identity, name);
      assert.deepEqual(
        protocol.resolve_item_version(JSON.parse(json), limits),
        expected.identity,
        name,
      );
    } else {
      assert.equal(outcome.ok, false, name);
      assert.equal(outcome.error.code, expected.code, name);
      assert.equal(outcome.error.path, expected.field ?? undefined, name);
      assert.deepEqual(
        project(outcome.error),
        { status: expected.status, body: expected.body },
        name,
      );
    }
  }
});

test("all input permutations resolve the same identity or fixed ambiguity", () => {
  const { inventory, descriptor } = fixture();
  const b = structuredClone(descriptor);
  b.repository.repository = "equivalent";
  const c = structuredClone(descriptor);
  c.discovery.refs_revision = `sha256:${"e".repeat(64)}`;
  for (const different of [false, true]) {
    if (different) c.module.content_digest = `sha256:${"f".repeat(64)}`;
    for (const candidates of [
      [descriptor, b, c],
      [descriptor, c, b],
      [b, descriptor, c],
      [b, c, descriptor],
      [c, descriptor, b],
      [c, b, descriptor],
    ]) {
      const input = { ...inventory, candidates };
      if (different)
        assert.deepEqual(
          project(refuse(() => protocol.resolve_item_version(input))),
          { status: 409, body: '{"code":"ambiguous_identity"}' },
        );
      else
        assert.deepEqual(
          protocol.resolve_item_version(input),
          descriptor.module,
        );
    }
  }
});

test("host eligibility changes cannot expose hidden publications through Core", () => {
  const { inventory, descriptor } = fixture();
  const other = structuredClone(descriptor);
  other.module.content_digest = `sha256:${"f".repeat(64)}`;
  // Readability and retention are supplied host facts, not inferred by WASM.
  const eligible = (readA, retainedA, readB) => ({
    ...inventory,
    candidates: [
      [descriptor, readA && retainedA],
      [other, readB],
    ]
      .filter(([, yes]) => yes)
      .map(([d]) => d),
  });
  assert.deepEqual(
    protocol.resolve_item_version(eligible(true, true, false)),
    descriptor.module,
  );
  assert.equal(
    project(
      refuse(() => protocol.resolve_item_version(eligible(true, true, true))),
    ).status,
    409,
  );
  assert.deepEqual(
    protocol.resolve_item_version(eligible(false, true, true)),
    other.module,
  );
  const absent = project(
    refuse(() =>
      protocol.resolve_item_version({ ...inventory, candidates: [] }),
    ),
  );
  assert.deepEqual(
    project(
      refuse(() => protocol.resolve_item_version(eligible(false, true, false))),
    ),
    absent,
  );
  assert.deepEqual(
    project(
      refuse(() => protocol.resolve_item_version(eligible(true, false, false))),
    ),
    absent,
  );
});

test("every nominated equivalent witness binds its own retained snapshots", () => {
  const { inventory, descriptor: a, refs: ra, manifest: ma } = fixture();
  const b = structuredClone(a),
    rb = structuredClone(ra),
    mb = structuredClone(ma);
  b.repository.repository = "equivalent";
  rb.repository = b.repository;
  mb.repository = b.repository;
  mb.immutable_version = `sha256:${"e".repeat(64)}`;
  mb.ontology.modules[0].package.package_digest = `sha256:${"f".repeat(64)}`;
  b.discovery.manifest_validator = protocol.validate_manifest(mb).digest;
  for (const ref of rb.refs) {
    ref.target = mb.immutable_version;
    ref.validator = b.discovery.manifest_validator;
  }
  b.discovery.refs_revision = protocol.validate_refs(rb).revision;
  const input = { ...inventory, candidates: [a, b, a] };
  for (const candidates of [
    [a, b, a],
    [b, a, a],
  ]) {
    for (const [d, r, m] of [
      [a, ra, ma],
      [b, rb, mb],
    ])
      assert.deepEqual(
        protocol.bind_item_version_source({ ...input, candidates }, d, r, m)
          .document,
        d,
      );
  }
  for (const [d, r, m] of [
    [a, rb, ma],
    [b, rb, ma],
    [a, ra, mb],
  ])
    assert.deepEqual(
      project(refuse(() => protocol.bind_item_version_source(input, d, r, m))),
      unavailable,
    );
  assert.deepEqual(
    project(
      refuse(() => protocol.bind_item_version_source(inventory, b, rb, mb)),
    ),
    unavailable,
  );
  const spliced = structuredClone(b);
  spliced.discovery.refs_revision = a.discovery.refs_revision;
  assert.deepEqual(
    project(
      refuse(() => protocol.bind_item_version_source(input, spliced, rb, mb)),
    ),
    unavailable,
  );
  // Advance all refs in the same repository; retained originals remain usable.
  const current = structuredClone(ra);
  for (const ref of current.refs) {
    ref.target = mb.immutable_version;
    ref.validator = b.discovery.manifest_validator;
  }
  assert.deepEqual(
    project(
      refuse(() => protocol.bind_item_version_source(input, a, current, ma)),
    ),
    unavailable,
  );
  assert.deepEqual(
    protocol.bind_item_version_source(input, a, ra, ma).document,
    a,
  );
});

test("nested MissingObject maps to empty 503 while proven empty inventory maps to 404", () => {
  const { inventory, descriptor, refs, manifest } = fixture();
  for (const empty of [true, false]) {
    const broken = structuredClone(manifest);
    broken.objects = empty
      ? []
      : broken.objects.filter(
          (object) =>
            object.digest !== broken.ontology.modules[0].package.object_digest,
        );
    const error = refuse(() =>
      protocol.bind_item_version_source(inventory, descriptor, refs, broken),
    );
    assert.equal(error.code, "missing_object");
    assert.notEqual(error.path, "item_version_inventory.candidates");
    assert.deepEqual(project(error), unavailable);
  }
  assert.deepEqual(
    project(
      refuse(() =>
        protocol.resolve_item_version({ ...inventory, candidates: [] }),
      ),
    ),
    { status: 404, body: '{"code":"missing_object"}' },
  );
});

test("candidate, aggregate, string and closed JSON boundaries fail before output", () => {
  const { inventory, descriptor, refs, manifest } = fixture();
  protocol.resolve_item_version({
    ...inventory,
    candidates: Array(4096).fill(descriptor),
  });
  assert.equal(
    refuse(() =>
      protocol.resolve_item_version({
        ...inventory,
        candidates: Array(4097).fill(descriptor),
      }),
    ).code,
    "limit_exceeded",
  );
  const unicode = structuredClone(inventory);
  unicode.version = "β".repeat(2048);
  unicode.candidates[0].module.version = unicode.version;
  protocol.resolve_item_version(unicode);
  unicode.version += "a";
  assert.equal(
    refuse(() => protocol.resolve_item_version(unicode)).code,
    "limit_exceeded",
  );
  const bytes = new TextEncoder().encode(JSON.stringify(inventory)).length;
  assert.equal(
    refuse(() =>
      protocol.bind_item_version_source(inventory, descriptor, refs, manifest, {
        max_response_bytes: bytes,
      }),
    ).code,
    "limit_exceeded",
  );
  const args = [inventory, descriptor, refs, manifest];
  for (let i = 0; i < args.length; i++) {
    const text = JSON.stringify(args[i]);
    const [key, value] = Object.entries(args[i])[0];
    const duplicate = `${text.slice(0, -1)},${JSON.stringify(key)}:${JSON.stringify(value)}}`;
    const changed = args.with(i, duplicate);
    assert.deepEqual(
      project(refuse(() => protocol.bind_item_version_source(...changed))),
      unavailable,
    );
  }
  for (const limits of [
    '{"max_response_bytes":1,"max_response_bytes":2}',
    { unknown: 1 },
    " ".repeat(1025),
  ])
    assert.deepEqual(
      project(refuse(() => protocol.resolve_item_version(inventory, limits))),
      unavailable,
    );
});

test("error projection refuses unknown, missing, malformed or duplicate provenance", () => {
  for (const error of [
    "{",
    null,
    [],
    {},
    { code: "missing_object" },
    { code: "ambiguous_identity" },
    { code: "missing_object", path: "objects" },
    { code: "unknown", path: "item_version_inventory.candidates" },
    { code: false, path: "item_version_inventory.candidates" },
    { code: "missing_object", path: [] },
    {
      code: "missing_object",
      path: "item_version_inventory.candidates",
      version: {},
    },
    { code: "ambiguous_identity", path: "item_version_inventory.candidates" },
    {
      code: "missing_object",
      path: "item_version_inventory.candidates",
      extra: true,
    },
    '{"code":"missing_object","code":"missing_object","path":"item_version_inventory.candidates"}',
    " ".repeat(1025),
  ])
    assert.deepEqual(project(error), unavailable);
  assert.deepEqual(
    project({
      code: "ambiguous_identity",
      path: "item_version_inventory.candidates.content_digest",
      message: "SECRET",
    }),
    { status: 409, body: '{"code":"ambiguous_identity"}' },
  );
});

test("resolution declarations come from the native wire and result types", () => {
  const declarations = readFileSync(
    join(dist, "graphforge_protocol_wasm.d.ts"),
    "utf8",
  );
  assert.match(
    declarations,
    /export interface ItemVersionInventory \{[\s\S]*?id: string;[\s\S]*?version: string;[\s\S]*?complete: boolean;[\s\S]*?candidates: ItemDescriptor\[\];/,
  );
  assert.match(
    declarations,
    /export interface ItemResolutionHttpResponse \{[\s\S]*?status: number;[\s\S]*?body: string;/,
  );
  assert.match(
    declarations,
    /export function resolve_item_version\(inventory: JsonInput<ItemVersionInventory>, limits\?: DiscoveryLimitsOverride \| null\): ExactIdentity;/,
  );
  assert.match(
    declarations,
    /export function bind_item_version_source\(inventory: JsonInput<ItemVersionInventory>, nominated: JsonInput<ItemDescriptor>, refs: JsonInput<RefSet>, manifest: JsonInput<DiscoveryManifest>, limits\?: DiscoveryLimitsOverride \| null\): ValidatedItemDescriptor;/,
  );
  assert.match(
    declarations,
    /export function item_resolution_error_response\(error: JsonInput<ProtocolError>\): ItemResolutionHttpResponse;/,
  );
});
