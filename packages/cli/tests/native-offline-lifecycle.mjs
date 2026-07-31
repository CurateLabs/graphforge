import assert from "node:assert/strict";
import { execFileSync, spawnSync } from "node:child_process";
import {
  appendFileSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  renameSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { isAbsolute, join } from "node:path";
import { fileURLToPath } from "node:url";

const packageRoot = fileURLToPath(new URL("..", import.meta.url));
const nativeRoot = fileURLToPath(
  new URL("../../../crates/gf-bindings-node/", import.meta.url),
);
const fixture = mkdtempSync(join(tmpdir(), "graphforge-native-skills-"));
const installRoot = join(fixture, "consumer");
const repository = join(fixture, "repository");
mkdirSync(installRoot);
mkdirSync(repository);

const pack = (directory, pnpm = false) => {
  const command = pnpm ? "pnpm" : "npm";
  const args = pnpm
    ? ["pack", "--json", "--pack-destination", fixture]
    : ["pack", "--ignore-scripts", "--json", "--pack-destination", fixture];
  const payload = JSON.parse(
    execFileSync(command, args, { cwd: directory, encoding: "utf8" }),
  );
  const result = Array.isArray(payload) ? payload[0] : payload;
  return isAbsolute(result.filename)
    ? result.filename
    : join(fixture, result.filename);
};

try {
  assert.equal(
    readdirSync(nativeRoot).some((name) => name.endsWith(".node")),
    true,
    "build @graphforge/node before running the native lifecycle smoke",
  );
  const nativeTarball = pack(nativeRoot);
  const cliTarball = pack(packageRoot, true);
  writeFileSync(
    join(installRoot, "package.json"),
    `${JSON.stringify({ name: "graphforge-skills-smoke", private: true })}\n`,
  );
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
  execFileSync("git", ["init", "-q", repository]);

  const executable = join(installRoot, "node_modules", ".bin", "graphforge");
  const run = (...args) =>
    spawnSync(executable, ["--project-dir", repository, "--json", ...args], {
      cwd: repository,
      encoding: "utf8",
    });
  const success = (...args) => {
    const result = run(...args);
    assert.equal(result.status, 0, result.stderr);
    return JSON.parse(result.stdout);
  };

  const initialized = success("init");
  assert.equal(initialized.skills.changed, true);
  const packaged = join(
    installRoot,
    "node_modules",
    "@graphforge",
    "cli",
    "project-skills",
  );
  const managed = join(repository, ".agents", "skills");
  for (const skill of ["graphforge-bootstrap", "graphforge-build-knowledge"]) {
    assert.deepEqual(
      readFileSync(join(managed, skill, "SKILL.md")),
      readFileSync(join(packaged, skill, "SKILL.md")),
    );
  }
  assert.equal(success("skills", "status").status, "current");
  assert.equal(success("skills", "install").changed, false);

  // Simulate interruption after the prior generation was moved to backup.
  const lifecycle = join(
    repository,
    ".graphforge",
    "state",
    "skills-lifecycle",
  );
  const backup = join(lifecycle, "backup");
  mkdirSync(backup);
  for (const skill of ["graphforge-bootstrap", "graphforge-build-knowledge"]) {
    renameSync(join(managed, skill), join(backup, skill));
    mkdirSync(join(managed, skill), { recursive: true });
    writeFileSync(join(managed, skill, "SKILL.md"), "interrupted\n");
  }
  renameSync(
    join(managed, ".graphforge-managed.json"),
    join(backup, ".graphforge-managed.json"),
  );
  writeFileSync(join(lifecycle, "transaction"), "graphforge-skills/1\n");
  assert.equal(success("skills", "status").status, "current");
  assert.equal(existsSync(join(lifecycle, "transaction")), false);

  const edited = join(managed, "graphforge-bootstrap", "SKILL.md");
  appendFileSync(edited, "\nuser edit\n");
  const beforeConflict = readFileSync(edited);
  assert.equal(success("skills", "status").status, "conflict");
  const updateConflict = run("skills", "update");
  assert.notEqual(updateConflict.status, 0);
  assert.deepEqual(readFileSync(edited), beforeConflict);
  assert.equal(success("skills", "update", "--force").changed, true);

  // Corruption of the installed npm asset must fail bundle validation. This
  // proves the package copy, rather than a separately embedded copy, is used.
  const packagedManifest = join(packaged, "manifest.json");
  const manifestBytes = readFileSync(packagedManifest);
  writeFileSync(packagedManifest, "{}\n");
  const invalidBundle = run("skills", "status");
  assert.notEqual(invalidBundle.status, 0);
  writeFileSync(packagedManifest, manifestBytes);

  appendFileSync(edited, "\nsecond user edit\n");
  const removeConflict = run("skills", "remove");
  assert.notEqual(removeConflict.status, 0);
  assert.equal(existsSync(edited), true);
  assert.equal(success("skills", "remove", "--force").changed, true);
  assert.equal(existsSync(join(managed, "graphforge-bootstrap")), false);
  assert.equal(success("skills", "install").changed, true);

  process.stdout.write("native npm project skill lifecycle: verified\n");
} finally {
  rmSync(fixture, { recursive: true, force: true });
}
