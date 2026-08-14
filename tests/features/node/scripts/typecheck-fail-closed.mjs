#!/usr/bin/env node
/**
 * Contract: Node BDD `tsc --noEmit` fails closed on type-invalid sources.
 *
 * Uses a hermetic temp project with the package tsconfig shape so the invalid
 * fixture never pollutes the real `typecheck` include set. Does not invoke
 * Cucumber or tsx — those are transpile-only and must not mask this gate.
 */
import { spawnSync } from "node:child_process";
import { createRequire } from "node:module";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const pkgRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const require = createRequire(join(pkgRoot, "package.json"));
const tscPath = require.resolve("typescript/bin/tsc");
const baseConfig = JSON.parse(
  readFileSync(join(pkgRoot, "tsconfig.json"), "utf8"),
);

function runTsc(projectDir) {
  return spawnSync(process.execPath, [tscPath, "--noEmit", "-p", projectDir], {
    encoding: "utf8",
    cwd: pkgRoot,
  });
}

const dir = mkdtempSync(join(tmpdir(), "graphforge-bdd-typecheck-"));
try {
  const config = {
    compilerOptions: { ...baseConfig.compilerOptions, rootDir: "." },
    include: ["fixture.ts"],
    exclude: ["node_modules", "dist"],
  };
  writeFileSync(join(dir, "tsconfig.json"), `${JSON.stringify(config, null, 2)}\n`);

  writeFileSync(join(dir, "fixture.ts"), "const value: string = 1;\n");
  const invalid = runTsc(dir);
  if (invalid.status === 0) {
    if (invalid.stdout) process.stderr.write(invalid.stdout);
    if (invalid.stderr) process.stderr.write(invalid.stderr);
    throw new Error(
      "typecheck-fail-closed: tsc --noEmit accepted a type-invalid fixture",
    );
  }

  writeFileSync(join(dir, "fixture.ts"), 'const value: string = "ok";\n');
  const valid = runTsc(dir);
  if (valid.status !== 0) {
    if (valid.stdout) process.stderr.write(valid.stdout);
    if (valid.stderr) process.stderr.write(valid.stderr);
    throw new Error(
      "typecheck-fail-closed: tsc --noEmit rejected a type-valid control fixture",
    );
  }

  console.log(
    "typecheck-fail-closed: invalid fixture rejected; valid control accepted",
  );
} finally {
  rmSync(dir, { recursive: true, force: true });
}
