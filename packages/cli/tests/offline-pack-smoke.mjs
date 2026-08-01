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
      name: "@curatelabs/graphforge",
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
  const cliPack = JSON.parse(
    execFileSync("pnpm", ["pack", "--json", "--pack-destination", fixture], {
      cwd: packageRoot,
      encoding: "utf8",
    }),
  );
  const cliTarball = cliPack.filename;
  for (const path of [
    "project-skills/manifest.json",
    "project-skills/graphforge-bootstrap/SKILL.md",
    "project-skills/graphforge-build-knowledge/SKILL.md",
  ]) {
    assert.equal(
      cliPack.files.some((entry) => entry.path === path),
      true,
      `packed CLI is missing ${path}`,
    );
  }

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
  const installedSkills = join(
    installRoot,
    "node_modules",
    "@curatelabs",
    "graphforge-cli",
    "project-skills",
  );
  const installedManifest = JSON.parse(
    readFileSync(join(installedSkills, "manifest.json"), "utf8"),
  );
  assert.deepEqual(installedManifest.skills, [
    "graphforge-bootstrap",
    "graphforge-build-knowledge",
  ]);
  for (const skill of installedManifest.skills) {
    assert.equal(
      readFileSync(join(installedSkills, skill, "SKILL.md"), "utf8").includes(
        `name: ${skill}`,
      ),
      true,
    );
  }

  const executable = join(installRoot, "node_modules", ".bin", "graphforge");
  const success = spawnSync(executable, ["--json", "config", "validate"], {
    cwd: installRoot,
    encoding: "utf8",
  });
  assert.equal(success.status, 0, success.stderr);
  const successArgs = JSON.parse(success.stdout).args;
  assert.equal(successArgs[0], "--skills-bundle-dir");
  assert.equal(
    realpathSync(successArgs[1]),
    realpathSync(
      join(
        installRoot,
        "node_modules",
        "@curatelabs",
        "graphforge-cli",
        "project-skills",
      ),
    ),
  );
  assert.deepEqual(successArgs.slice(2), ["--json", "config", "validate"]);
  assert.equal(
    realpathSync(JSON.parse(success.stdout).cwd),
    realpathSync(installRoot),
  );

  const npx = spawnSync(
    "npx",
    ["--offline", "--no-install", "@curatelabs/graphforge-cli", "--info"],
    { cwd: installRoot, encoding: "utf8" },
  );
  assert.equal(npx.status, 0, npx.stderr);
  const npxArgs = JSON.parse(npx.stdout).args;
  assert.equal(npxArgs[0], "--skills-bundle-dir");
  assert.equal(realpathSync(npxArgs[1]), realpathSync(installedSkills));
  assert.deepEqual(npxArgs.slice(2), ["--info"]);

  const failure = spawnSync(executable, ["--fail"], {
    cwd: installRoot,
    encoding: "utf8",
  });
  assert.equal(failure.status, 3);
  assert.equal(failure.stderr, "native failure\n");
} finally {
  rmSync(fixture, { recursive: true, force: true });
}
