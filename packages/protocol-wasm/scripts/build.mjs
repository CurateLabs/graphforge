// Build the WebAssembly module and its ES wrapper into dist/.
//
// Needs the wasm32-unknown-unknown Rust target and a `wasm-bindgen` CLI whose
// version equals the `wasm-bindgen` crate in Cargo.lock (see README.md). The
// module is then shrunk with Binaryen's `wasm-opt` (the pinned `binaryen` dev
// dependency); `memory.copy`, which Rust emits, needs bulk-memory enabled.
import { execFileSync } from "node:child_process";
import { mkdirSync, rmSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..", "..", "..");
const dist = join(root, "packages", "protocol-wasm", "dist");
const targetDir = process.env.CARGO_TARGET_DIR ?? join(root, "target");

const run = (command, args) =>
  execFileSync(command, args, { cwd: root, stdio: "inherit" });

run("cargo", [
  "build",
  "--locked",
  "-p",
  "graphforge-protocol-wasm",
  "--target",
  "wasm32-unknown-unknown",
  "--profile",
  "wasm-release",
]);
rmSync(dist, { recursive: true, force: true });
mkdirSync(dist, { recursive: true });
run("wasm-bindgen", [
  "--target",
  "web",
  "--out-dir",
  dist,
  join(
    targetDir,
    "wasm32-unknown-unknown",
    "wasm-release",
    "graphforge_protocol_wasm.wasm",
  ),
]);

const binaryen = dirname(
  createRequire(import.meta.url).resolve("binaryen/package.json"),
);
const module = join(dist, "graphforge_protocol_wasm_bg.wasm");
run(process.execPath, [
  join(binaryen, "bin", "wasm-opt"),
  "-Oz",
  "--enable-bulk-memory",
  module,
  "-o",
  module,
]);
