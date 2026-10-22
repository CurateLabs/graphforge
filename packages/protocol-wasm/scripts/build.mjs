// Build the WebAssembly module and its ES wrapper into dist/.
//
// Needs the wasm32-unknown-unknown Rust target and a `wasm-bindgen` CLI whose
// version equals the `wasm-bindgen` crate in Cargo.lock (see README.md).
import { execFileSync } from "node:child_process";
import { mkdirSync, rmSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..", "..", "..");
const dist = join(root, "packages", "protocol-wasm", "dist");
const targetDir = process.env.CARGO_TARGET_DIR ?? join(root, "target");

const run = (command, args) => execFileSync(command, args, { cwd: root, stdio: "inherit" });

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
  join(targetDir, "wasm32-unknown-unknown", "wasm-release", "graphforge_protocol_wasm.wasm"),
]);
