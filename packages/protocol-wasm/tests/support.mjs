// Shared test setup: load the built module synchronously and locate the
// checked-in conformance corpora and the openalex fixture.
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
export const root = join(here, "..", "..", "..");
// Release consumers run these same tests against their clean npm installation.
// Keep fixtures anchored to this checkout, but resolve every package byte there.
const consumer = process.env.GRAPHFORGE_PROTOCOL_PACKAGE_ROOT;
const require = createRequire(
  join(consumer ?? join(here, ".."), "package.json"),
);
export const entry = consumer
  ? require.resolve("@curatelabs/graphforge-protocol-wasm")
  : join(here, "..", "index.js");
const wasmPath = consumer
  ? require.resolve("@curatelabs/graphforge-protocol-wasm/wasm")
  : join(here, "..", "dist", "graphforge_protocol_wasm_bg.wasm");
export const dist = dirname(wasmPath);

export const protocol = await import(pathToFileURL(entry));
export const wasm = protocol.initSync({
  module: readFileSync(wasmPath),
});

export const readJson = (...segments) =>
  JSON.parse(readFileSync(join(root, ...segments), "utf8"));
export const readText = (...segments) =>
  readFileSync(join(root, ...segments), "utf8");

/** Run `call`; return `{ ok: true, value }` or `{ ok: false, error }`. */
export function attempt(call) {
  try {
    return { ok: true, value: call() };
  } catch (error) {
    return { ok: false, error };
  }
}
