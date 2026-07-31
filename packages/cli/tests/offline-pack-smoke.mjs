import assert from "node:assert/strict";
import { execFileSync, spawnSync } from "node:child_process";
import {
  mkdtempSync,
  mkdirSync,
  readFileSync,
  realpathSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const packageRoot = fileURLToPath(new URL("..", import.meta.url));
const metadata = JSON.parse(
  readFileSync(new URL("../package.json", import.meta.url), "utf8"),
);
const fixture = mkdtempSync(join(tmpdir(), "graphforge-cli-offline-"));
const nativeRoot = join(fixture, "native");
const installRoot = join(fixture, "consumer");
mkdirSync(nativeRoot);
mkdirSync(installRoot);

try {
  writeFileSync(
    join(nativeRoot, "package.json"),
    JSON.stringify({
      name: "@graphforge/node",
      version: metadata.version,
      type: "module",
      main: "index.js",
    }),
  );
  writeFileSync(
    join(nativeRoot, "index.js"),
    `export function runCli(args) {
    return {
      exitCode: args.includes("--fail") ? 3 : 0,
      stdout: JSON.stringify({ args, cwd: process.cwd() }) + "\\n",
      stderr: args.includes("--fail") ? "native failure\\n" : "",
    };
  }\n`,
  );

  const npmPack = (directory) =>
    execFileSync("npm", ["pack", "--ignore-scripts", "--json"], {
      cwd: directory,
      encoding: "utf8",
    });
  const nativeTarball = join(
    nativeRoot,
    JSON.parse(npmPack(nativeRoot))[0].filename,
  );
  const cliTarball = JSON.parse(
    execFileSync("pnpm", ["pack", "--json", "--pack-destination", fixture], {
      cwd: packageRoot,
      encoding: "utf8",
    }),
  ).filename;

  execFileSync(
    "npm",
    [
      "install",
      "--offline",
      "--ignore-scripts",
      "--no-audit",
      "--no-fund",
      nativeTarball,
      cliTarball,
    ],
    { cwd: installRoot, stdio: "pipe" },
  );

  const executable = join(installRoot, "node_modules", ".bin", "graphforge");
  const success = spawnSync(executable, ["--json", "config", "validate"], {
    cwd: installRoot,
    encoding: "utf8",
  });
  assert.equal(success.status, 0, success.stderr);
  assert.deepEqual(JSON.parse(success.stdout).args, [
    "--json",
    "config",
    "validate",
  ]);
  assert.equal(
    realpathSync(JSON.parse(success.stdout).cwd),
    realpathSync(installRoot),
  );

  const npx = spawnSync(
    "npx",
    ["--offline", "--no-install", "@graphforge/cli", "--info"],
    { cwd: installRoot, encoding: "utf8" },
  );
  assert.equal(npx.status, 0, npx.stderr);
  assert.deepEqual(JSON.parse(npx.stdout).args, ["--info"]);

  const failure = spawnSync(executable, ["--fail"], {
    cwd: installRoot,
    encoding: "utf8",
  });
  assert.equal(failure.status, 3);
  assert.equal(failure.stderr, "native failure\n");
} finally {
  rmSync(fixture, { recursive: true, force: true });
}
