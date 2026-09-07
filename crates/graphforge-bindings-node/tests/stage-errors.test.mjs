import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import { GraphForge } from "../index.js";

const matrix = JSON.parse(
  readFileSync(
    new URL(
      "../../graphforge-api/tests/stage_error_matrix.json",
      import.meta.url,
    ),
    "utf8",
  ),
);
for (const row of matrix) {
  const modes =
    row.operation === "analyze"
      ? ["analyze"]
      : ["execute", "params", "plan", "explain"];
  for (const mode of modes) {
    test(`stage matrix ${row.id} via ${mode}`, async () => {
      const forge = new GraphForge();
      const check = (error) => {
        const prefix = mode === "explain" ? "explain_" : "";
        assert.equal(error.code, row[`${prefix}node`] ?? row.node);
        const span = row.span ? `[span:${row.span[0]}:${row.span[1]}] ` : "";
        assert.equal(
          error.message,
          span + (row[`${prefix}message`] ?? row.message),
        );
        return true;
      };
      if (mode === "plan") {
        await assert.rejects(forge.plan(row.query).collectIpc(), check);
      } else {
        assert.throws(() => {
          if (mode === "analyze") {
            forge.execute(row.query);
            forge.analyze("euler_circuit", undefined, undefined, false);
          } else if (mode === "explain") forge.explain(row.query);
          else if (mode === "params") forge.execute(row.query, {});
          else forge.execute(row.query);
        }, check);
      }
    });
  }
}
