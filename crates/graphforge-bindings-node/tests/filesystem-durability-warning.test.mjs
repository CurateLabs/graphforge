import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { spawnSync } from "node:child_process";
import { setImmediate } from "node:timers/promises";
import { test } from "node:test";
import { GraphForge } from "../index.js";

if (process.platform === "linux")
  test("tmpfs emits one structured warning per actual open and none per write", async () => {
    const root = mkdtempSync("/dev/shm/gf-filesystem-warning-");
    const project = join(root, "project");
    const caught = [];
    const listener = (warning) => {
      if (warning.code === "GF_UNVERIFIED_FILESYSTEM") caught.push(warning);
    };
    process.on("warning", listener);
    try {
      let forge = new GraphForge(project);
      await setImmediate();
      assert.equal(caught.length, 1);
      assert.equal(caught[0].name, "FilesystemDurabilityWarning");
      const detail = JSON.parse(caught[0].detail);
      assert.equal(detail.phase, "CLASSIFY");
      assert.equal(detail.outcome, "unverified");
      assert.equal(detail.cause, "filesystem_class_unproven");
      assert.equal(detail.filesystem_type, "tmpfs");
      assert.equal(caught[0].message.includes(project), false);
      assert.equal(forge.openWarnings[0].code, "GF_UNVERIFIED_FILESYSTEM");
      forge.execute("CREATE (:Item {value: 42})");
      forge.close();
      await setImmediate();
      assert.equal(caught.length, 1);
      forge = new GraphForge(project);
      assert.equal(forge.nodeCount("Item"), 1);
      const pkg = join(root, "package.gfpb");
      const exported = await forge.exportPortable({
        outputPath: pkg,
        representation: "bundle",
        profile: "complete",
      });
      assert.equal(exported.sourceUnverifiedDurability.filesystemType, "tmpfs");
      const verified = await GraphForge.verifyPortable({
        input: pkg,
        mode: "full",
      });
      assert.deepEqual(
        verified.sourceUnverifiedDurability,
        exported.sourceUnverifiedDurability,
      );
      forge.close();
      const memory = new GraphForge();
      memory.close();
      await setImmediate();
      assert.equal(caught.length, 2);
      for (const [policy, code] of [
        ["strict", "GF_UNSUPPORTED_FILESYSTEM"],
        ["invalid", "GF_VALIDATION"],
      ]) {
        const child = spawnSync(
          process.execPath,
          [
            "--input-type=module",
            "-e",
            `import { GraphForge } from ${JSON.stringify(new URL("../index.js", import.meta.url).href)}; try { new GraphForge(${JSON.stringify(project)}); process.exit(1); } catch (e) { if (e.code !== ${JSON.stringify(code)}) throw e; }`,
          ],
          {
            env: { ...process.env, GRAPHFORGE_FILESYSTEM_DURABILITY: policy },
            encoding: "utf8",
          },
        );
        assert.equal(child.status, 0, child.stdout + child.stderr);
        const imported = spawnSync(
          process.execPath,
          [
            "--input-type=module",
            "-e",
            `import { GraphForge } from ${JSON.stringify(new URL("../index.js", import.meta.url).href)}; import {randomUUID} from "node:crypto"; try { await GraphForge.importPortable({projectRoot:${JSON.stringify(join(root, "refused-import"))}, input:${JSON.stringify(pkg)}, operationId:randomUUID()}); process.exit(1); } catch (e) { if (e.code !== ${JSON.stringify(code)}) throw e; }`,
          ],
          {
            env: { ...process.env, GRAPHFORGE_FILESYSTEM_DURABILITY: policy },
            encoding: "utf8",
          },
        );
        assert.equal(imported.status, 0, imported.stdout + imported.stderr);
      }
    } finally {
      process.off("warning", listener);
      rmSync(root, { recursive: true, force: true });
    }
  });
