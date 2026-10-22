// Runs the module inside a real Worker: Miniflare starts Cloudflare's workerd
// runtime locally (no account, no network). The Worker imports the package by
// its published specifiers, as the README documents, and validates the openalex
// fixture; its results must equal the native Rust values.
import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { after, before, test } from "node:test";
import { fileURLToPath } from "node:url";
import { Miniflare } from "miniflare";
import { dist, readJson, readText } from "./support.mjs";

const here = dirname(fileURLToPath(import.meta.url));
const fixtureDir = ["tests", "fixtures", "hub", "generated", "v1"];
const fixture = readJson(...fixtureDir, "fixture.json");
const recorded = readJson(
  "crates",
  "graphforge-protocol-wasm",
  "tests",
  "openalex-commitment.json",
);
const manifest = readText(...fixtureDir, "manifest.json");
const manifestObject = JSON.parse(manifest);
const discovery = readJson(
  "docs",
  "reference",
  "discovery",
  "v1",
  "conformance.json",
);
const lineage = discovery.cases.find(
  (c) => c.document === "lineage" && c.expected.result === "valid",
);

const body = {
  manifest,
  refs: readText(...fixtureDir, "refs.json"),
  summary: readText(...fixtureDir, fixture.summary_path),
  lineage: lineage.json,
  intent: {
    repository: manifestObject.repository,
    intent_digest: fixture.package_digest,
    objects: manifestObject.objects
      .map(({ digest, length, media_type }) => ({ digest, length, media_type }))
      .sort((a, b) => (a.digest < b.digest ? -1 : 1)),
    manifest_validator: fixture.manifest_digest,
    refs: ["main"],
    expected_revision: null,
  },
  rename: {
    identity: { owner: "openalex", repository: "renamed" },
    template: "https://control.example/openalex/renamed/sha256/{hex}",
  },
};

// The package's `exports` map, served to the Worker under the same specifiers.
// `index.js` imports `./dist/...` relative to its own module name.
const SPECIFIER = "@curatelabs/graphforge-protocol-wasm";
const modules = [
  {
    type: "ESModule",
    path: "worker.mjs",
    contents: readFileSync(join(here, "worker", "worker.mjs"), "utf8"),
  },
  {
    type: "ESModule",
    path: SPECIFIER,
    contents: readFileSync(join(here, "..", "index.js"), "utf8"),
  },
  {
    type: "ESModule",
    path: "@curatelabs/dist/graphforge_protocol_wasm.js",
    contents: readFileSync(join(dist, "graphforge_protocol_wasm.js"), "utf8"),
  },
  {
    type: "CompiledWasm",
    path: `${SPECIFIER}/wasm`,
    contents: readFileSync(join(dist, "graphforge_protocol_wasm_bg.wasm")),
  },
];

let worker;
before(async () => {
  worker = new Miniflare({ modules, compatibilityDate: "2025-07-01" });
  await worker.ready;
});
after(async () => {
  await worker?.dispose();
});

const post = async (payload) => {
  const response = await worker.dispatchFetch("http://worker.test/", {
    method: "POST",
    body: JSON.stringify(payload),
  });
  return { status: response.status, body: await response.json() };
};

/** Resident memory of the workerd child process, where /proc is available. */
function workerdRss() {
  try {
    for (const pid of readdirSync("/proc").filter((name) =>
      /^\d+$/.test(name),
    )) {
      const stat = readFileSync(`/proc/${pid}/stat`, "utf8");
      const name = /\((.*)\)/.exec(stat)?.[1];
      const parent = Number(
        stat.slice(stat.lastIndexOf(")") + 2).split(" ")[1],
      );
      if (name === "workerd" && parent === process.pid) {
        return (
          Number(
            /VmRSS:\s+(\d+) kB/.exec(
              readFileSync(`/proc/${pid}/status`, "utf8"),
            )[1],
          ) * 1024
        );
      }
    }
  } catch {
    // not Linux
  }
  return undefined;
}

test("a Worker validates the openalex fixture and equals the native values", async () => {
  const { status, body: result } = await post(body);
  assert.equal(status, 200, JSON.stringify(result));
  assert.equal(result.manifest_digest, fixture.manifest_digest);
  assert.equal(result.summary_digest, fixture.summary_digest);
  assert.equal(result.refs_revision.length, 71);
  assert.match(result.lineage_digest, /^sha256:[0-9a-f]{64}$/);
  // The 64-bit lengths come back exact, as bigint, rendered by the Worker.
  assert.deepEqual(
    result.object_lengths,
    manifestObject.objects.map(({ length }) => `${length}n`),
  );
  assert.equal(result.request_commitment, recorded.request_commitment);
  assert.equal(result.rehomed_revision, recorded.rehomed_revision);
  assert.equal(result.rehomed_manifest, recorded.rehomed_manifest);
});

test("a Worker returns the structured Rust error for a bad document", async () => {
  const { status, body: error } = await post({ ...body, manifest: "{" });
  assert.equal(status, 422);
  assert.equal(error.code, "malformed_response");
  const future = await post({
    ...body,
    refs: body.refs.replace("graphforge-discovery/1", "graphforge-discovery/2"),
  });
  assert.equal(future.body.code, "unsupported_future");
});

test("Worker footprint: linear memory, workerd RSS and request latency", async (t) => {
  const iterations = 200;
  const rssBefore = workerdRss();
  const started = performance.now();
  let result;
  for (let i = 0; i < iterations; i += 1) {
    result = (await post(body)).body;
  }
  const perRequest = (performance.now() - started) / iterations;
  t.diagnostic(
    JSON.stringify({
      linear_memory_bytes: result.linear_memory_bytes,
      workerd_rss_bytes_before: rssBefore,
      workerd_rss_bytes_after: workerdRss(),
      request_ms_including_http_and_six_validations: Number(
        perRequest.toFixed(2),
      ),
    }),
  );
  // Workers allow 128 MB per isolate; the module's linear memory is a few MB.
  assert.ok(result.linear_memory_bytes < 32 * 1024 * 1024);
});
