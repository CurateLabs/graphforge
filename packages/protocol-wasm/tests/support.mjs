// Shared test setup: load the built module synchronously and locate the
// checked-in conformance corpora and the openalex fixture.
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
export const root = join(here, "..", "..", "..");
export const dist = join(here, "..", "dist");

export const protocol = await import(join(dist, "graphforge_protocol_wasm.js"));
export const wasm = protocol.initSync({
  module: readFileSync(join(dist, "graphforge_protocol_wasm_bg.wasm")),
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
